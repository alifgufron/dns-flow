package influxdb

import (
	"bytes"
	"encoding/base64"
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
	URL              string
	Database         string
	Username         string
	Password         string
	RetentionPolicy  string
	RetentionDays    int
	Measurement      string
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
	return "influxdb"
}

func (w *Writer) authHeader() string {
	if w.cfg.Username == "" && w.cfg.Password == "" {
		return ""
	}
	auth := base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:%s", w.cfg.Username, w.cfg.Password)),
	)
	return fmt.Sprintf("Basic %s", auth)
}

func (w *Writer) dbExists() (bool, error) {
	u := fmt.Sprintf("%s/query?q=SHOW+DATABASES",
		w.cfg.URL)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return false, err
	}

	if auth := w.authHeader(); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	// Check if database name appears in the response
	return bytes.Contains(body, []byte(w.cfg.Database)), nil
}

func (w *Writer) Migrate() error {
	exists, err := w.dbExists()
	if err == nil && exists {
		w.logger.Info("influxdb: ready",
			"database", w.cfg.Database,
			"measurement", w.measurement(),
		)
		return nil
	}

	w.logger.Info("influxdb: creating database",
		"database", w.cfg.Database,
		"measurement", w.measurement(),
		"url", w.cfg.URL,
	)

	u := fmt.Sprintf("%s/query?q=CREATE+DATABASE+%%22%s%%22",
		w.cfg.URL, url.QueryEscape(w.cfg.Database))

	req, err := http.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	if auth := w.authHeader(); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		w.logger.Warn("influxdb: migrate failed (ignored)", "error", err)
		return nil
	}
	defer resp.Body.Close()

	w.logger.Info("influxdb: ready",
		"database", w.cfg.Database,
		"measurement", w.measurement(),
		"status", resp.StatusCode,
	)

	if w.cfg.RetentionDays > 0 {
		w.applyRetentionPolicy()
	}

	return nil
}

func (w *Writer) applyRetentionPolicy() {
	rpName := w.cfg.RetentionPolicy
	if rpName == "" {
		rpName = "autogen"
	}
	duration := fmt.Sprintf("%dd", w.cfg.RetentionDays)

	u := fmt.Sprintf("%s/query?q=CREATE+RETENTION+POLICY+%%22%s%%22+ON+%%22%s%%22+DURATION+%s+REPLICATION+1+DEFAULT",
		w.cfg.URL,
		url.QueryEscape(rpName),
		url.QueryEscape(w.cfg.Database),
		url.QueryEscape(duration),
	)

	req, err := http.NewRequest("POST", u, nil)
	if err != nil {
		w.logger.Warn("influxdb: failed to create retention policy request", "error", err)
		return
	}

	if auth := w.authHeader(); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		w.logger.Warn("influxdb: retention policy request failed", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		// RP may already exist — try ALTER instead
		u := fmt.Sprintf("%s/query?q=ALTER+RETENTION+POLICY+%%22%s%%22+ON+%%22%s%%22+DURATION+%s+REPLICATION+1+DEFAULT",
			w.cfg.URL,
			url.QueryEscape(rpName),
			url.QueryEscape(w.cfg.Database),
			url.QueryEscape(duration),
		)
		req, _ := http.NewRequest("POST", u, nil)
		if auth := w.authHeader(); auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := w.client.Do(req)
		if err != nil {
			w.logger.Warn("influxdb: alter retention policy failed", "error", err)
			return
		}
		defer resp.Body.Close()
	}

	w.logger.Info("influxdb: retention policy applied",
		"policy", rpName,
		"duration", duration,
	)
}

func (w *Writer) Write(event domain.DNSRawEvent) error {
	line := w.toLineProtocol(event)
	if line == "" {
		return nil
	}

	u := fmt.Sprintf("%s/write?db=%s", w.cfg.URL, url.QueryEscape(w.cfg.Database))
	if w.cfg.RetentionPolicy != "" {
		u += "&rp=" + url.QueryEscape(w.cfg.RetentionPolicy)
	}

	req, err := http.NewRequest("POST", u, bytes.NewReader([]byte(line)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	if auth := w.authHeader(); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		w.logger.Warn("influxdb: write failed",
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

	tags := fmt.Sprintf(
		"identity=%s,operation=%s,"+
			"dnstap_version=%s,dnstap_type=%s,"+
			"client_country=%s,client_city=%s,client_asn=%s",
		escapeTag(event.DNSTap.Identity),
		escapeTag(event.DNSTap.Operation),
		escapeTag(event.DNSTap.Version),
		escapeTag(event.DNSTap.Type),
		escapeTag(event.GeoIP.ClientCountry),
		escapeTag(event.GeoIP.ClientCity),
		escapeTag(event.GeoIP.ClientASN),
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

	return fmt.Sprintf("%s,%s %s%s %d", w.measurement(), tags, fields, policyFields, ts)
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
	w.logger.Info("influxdb: closed")
	close(w.done)
	return nil
}
