package influxdb_v2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type Config struct {
	URL           string
	Org           string
	Bucket        string
	Token         string
	Precision     string
	Measurement   string
	RetentionDays int
}

type Writer struct {
	cfg    Config
	logger *slog.Logger
	client *http.Client
	done   chan struct{}
}

func NewWriter(cfg Config, logger *slog.Logger) *Writer {
	return &Writer{
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: 10 * time.Second},
		done:   make(chan struct{}),
	}
}

func (w *Writer) Name() string {
	return "influxdb_v2"
}

func (w *Writer) Migrate() error {
	w.logger.Info("influxdb_v2: auto-migrate (bucket creation not supported via API)",
		"bucket", w.cfg.Bucket,
		"org", w.cfg.Org,
		"measurement", w.measurement(),
	)

	if err := w.applyBucketRetention(); err != nil {
		w.logger.Warn("influxdb_v2: bucket retention update failed (ignored)", "error", err)
	}

	return nil
}

func (w *Writer) applyBucketRetention() error {
	// Find bucket ID
	u := fmt.Sprintf("%s/api/v2/buckets?org=%s&name=%s",
		w.cfg.URL,
		url.QueryEscape(w.cfg.Org),
		url.QueryEscape(w.cfg.Bucket),
	)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+w.cfg.Token)

	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var list struct {
		Buckets []struct {
			ID string `json:"id"`
		} `json:"buckets"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return err
	}

	if len(list.Buckets) == 0 {
		return fmt.Errorf("bucket %q not found", w.cfg.Bucket)
	}
	bucketID := list.Buckets[0].ID

	// Build retention rules
	var rules []map[string]interface{}
	if w.cfg.RetentionDays > 0 {
		rules = []map[string]interface{}{{
			"type":          "expire",
			"everySeconds":  w.cfg.RetentionDays * 86400,
		}}
	}

	payload := map[string]interface{}{
		"retentionRules": rules,
	}
	data, _ := json.Marshal(payload)

	u = fmt.Sprintf("%s/api/v2/buckets/%s", w.cfg.URL, bucketID)
	req, err = http.NewRequest("PATCH", u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+w.cfg.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err = w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PATCH bucket failed: %d %s", resp.StatusCode, string(respBody))
	}

	w.logger.Info("influxdb_v2: bucket retention updated",
		"bucket", w.cfg.Bucket,
		"retention_days", w.cfg.RetentionDays,
	)
	return nil
}

func (w *Writer) Write(event domain.DNSRawEvent) error {
	line := w.toLineProtocol(event)
	if line == "" {
		return nil
	}

	precision := w.cfg.Precision
	if precision == "" {
		precision = "ns"
	}

	u := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s&precision=%s",
		w.cfg.URL,
		url.QueryEscape(w.cfg.Org),
		url.QueryEscape(w.cfg.Bucket),
		url.QueryEscape(precision),
	)

	req, err := http.NewRequest("POST", u, bytes.NewReader([]byte(line)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Authorization", "Token "+w.cfg.Token)

	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		w.logger.Warn("influxdb_v2: write failed",
			"status", resp.StatusCode,
			"body", string(body),
		)
	}

	return nil
}

func boolToInflux(b bool) string {
	if b {
		return "t"
	}
	return "f"
}

func (w *Writer) measurement() string {
	if w.cfg.Measurement != "" {
		return w.cfg.Measurement
	}
	return "dns_query"
}

func (w *Writer) toLineProtocol(event domain.DNSRawEvent) string {
	ts := event.DNSTap.Timestamp.UnixNano()
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	fields := fmt.Sprintf(
		"qname=%s,qtype=%s,qclass=%s,rcode=%s,"+
			"query_ip=%s,query_port=%d,response_ip=%s,response_port=%d,"+
			"family=%s,protocol=%s,"+
			"latency=%f,latency_ms=%d,"+
			"opcode=%d,length=%d,dns_id=%d,"+
			"qr=%s,tc=%s,aa=%s,rd=%s,ra=%s,ad=%s,cd=%s,"+
			"qdcount=%d,ancount=%d,nscount=%d,arcount=%d,"+
			"malformed=%s,"+
			"edns_version=%d,edns_udp_size=%d,edns_rcode=%d,edns_dnssec_ok=%s,"+
			"edns_client_subnet=%s,"+
			"socket_ip=%s,socket_port=%d,"+
			"peer_name=%s,query_zone=%s,"+
			"client_ip=%s",
		escapeField(event.DNS.QName),
		escapeField(event.DNS.QType),
		escapeField(event.DNS.QClass),
		escapeField(event.DNS.RCode),
		escapeField(event.Network.QueryIP),
		event.Network.QueryPort,
		escapeField(event.Network.ResponseIP),
		event.Network.ResponsePort,
		escapeField(event.Network.Family),
		escapeField(event.Network.Protocol),
		event.DNSTap.Latency,
		event.DNSTap.LatencyMs,
		event.DNS.Opcode,
		event.DNS.Length,
		event.DNS.ID,
		boolToInflux(event.DNS.Flags.QR),
		boolToInflux(event.DNS.Flags.TC),
		boolToInflux(event.DNS.Flags.AA),
		boolToInflux(event.DNS.Flags.RD),
		boolToInflux(event.DNS.Flags.RA),
		boolToInflux(event.DNS.Flags.AD),
		boolToInflux(event.DNS.Flags.CD),
		event.DNS.Qdcount,
		event.DNS.Ancount,
		event.DNS.Nscount,
		event.DNS.Arcount,
		boolToInflux(event.DNS.Malformed),
		event.EDNS.Version,
		event.EDNS.UDPSize,
		event.EDNS.RCode,
		boolToInflux(event.EDNS.DNSSECOK),
		escapeField(event.EDNS.ClientSubnet),
		escapeField(event.DNSTap.SocketIP),
		event.DNSTap.SocketPort,
		escapeField(event.DNSTap.PeerName),
		escapeField(event.DNSTap.QueryZone),
		escapeField(event.GeoIP.ClientIP),
	)

	threatFields := fmt.Sprintf(",is_malicious=%s,threat_category=%s",
		boolToInflux(event.Threat.Malicious),
		escapeField(event.Threat.Category),
	)

	anomalyFields := fmt.Sprintf(",is_anomaly=%s,anomaly_score=%f,entropy_score=%f",
		boolToInflux(event.Anomaly.Detected),
		event.Anomaly.Score,
		event.Anomaly.EntropyScore,
	)

	anomalyTypeTag := ""
	if event.Anomaly.Detected && len(event.Anomaly.Types) > 0 {
		anomalyTypeTag = fmt.Sprintf(",is_anomaly=true,anomaly_type=%s", escapeTag(strings.Join(event.Anomaly.Types, "|")))
	} else {
		anomalyTypeTag = ",is_anomaly=false"
	}

	threatTag := ""
	if event.Threat.Malicious {
		threatTag = fmt.Sprintf(",is_malicious=true,threat_category=%s", escapeTag(event.Threat.Category))
	} else {
		threatTag = ",is_malicious=false"
	}

	tags := fmt.Sprintf(
		"identity=%s,operation=%s,"+
			"dnstap_version=%s,dnstap_type=%s,"+
			"client_country=%s,client_city=%s,client_asn=%s%s%s",
		escapeTag(event.DNSTap.Identity),
		escapeTag(event.DNSTap.Operation),
		escapeTag(event.DNSTap.Version),
		escapeTag(event.DNSTap.Type),
		escapeTag(event.GeoIP.ClientCountry),
		escapeTag(event.GeoIP.ClientCity),
		escapeTag(event.GeoIP.ClientASN),
		anomalyTypeTag,
		threatTag,
	)

	policyFields := ""
	if event.DNSTap.PolicyRule != "" || event.DNSTap.PolicyType != "" ||
		event.DNSTap.PolicyMatch != "" || event.DNSTap.PolicyValue != "" ||
		event.DNSTap.HTTPProtocol != "" || event.DNSTap.Extra != "" {
		policyFields = fmt.Sprintf(
			",policy_rule=%s,policy_type=%s,policy_match=%s,policy_value=%s,"+
				"http_protocol=%s,extra=%s",
			escapeField(event.DNSTap.PolicyRule),
			escapeField(event.DNSTap.PolicyType),
			escapeField(event.DNSTap.PolicyMatch),
			escapeField(event.DNSTap.PolicyValue),
			escapeField(event.DNSTap.HTTPProtocol),
			escapeField(event.DNSTap.Extra),
		)
	}

	return fmt.Sprintf("%s,%s %s%s%s%s %d", w.measurement(), tags, fields, anomalyFields, threatFields, policyFields, ts)
}

func escapeField(s string) string {
	if s == "" {
		return `""`
	}
	return fmt.Sprintf("%q", s)
}

func escapeTag(s string) string {
	if s == "" {
		return "unknown"
	}
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, ` `, `\ `)
	s = strings.ReplaceAll(s, `,`, `\,`)
	s = strings.ReplaceAll(s, `=`, `\=`)
	return s
}

func (w *Writer) Close() error {
	w.client.CloseIdleConnections()
	w.logger.Info("influxdb_v2: closed")
	close(w.done)
	return nil
}
