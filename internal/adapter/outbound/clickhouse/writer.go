package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type Config struct {
	Hosts       []string
	Database    string
	Username    string
	Password    string
	Compression bool
	PoolSize    int
	TTLDays     int
}

type Writer struct {
	cfg    Config
	logger *slog.Logger
	conn   driver.Conn
	queue  chan domain.DNSRawEvent
	done   chan struct{}
	wg     sync.WaitGroup
}

func NewWriter(cfg Config, logger *slog.Logger) *Writer {
	return &Writer{
		cfg:    cfg,
		logger: logger,
		queue:  make(chan domain.DNSRawEvent, 100000),
		done:   make(chan struct{}),
	}
}

func (w *Writer) Name() string {
	return "clickhouse"
}

func (w *Writer) migrateNeeded(ctx context.Context, conn driver.Conn) bool {
	var count uint64
	err := conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT count() FROM system.tables WHERE database = '%s' AND table = 'dns_raw'`,
		w.cfg.Database,
	)).Scan(&count)
	if err != nil || count == 0 {
		return true
	}
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT count() FROM system.tables WHERE database = '%s' AND table = 'dns_answers'`,
		w.cfg.Database,
	)).Scan(&count)
	return err != nil || count == 0
}

func (w *Writer) Migrate() error {
	connectOpts := func(db string) *clickhouse.Options {
		opts := &clickhouse.Options{
			Addr: w.cfg.Hosts,
			Auth: clickhouse.Auth{
				Database: db,
				Username: w.cfg.Username,
				Password: w.cfg.Password,
			},
			DialTimeout: 5 * time.Second,
		}
		if w.cfg.Compression {
			opts.Compression = &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			}
		}
		return opts
	}

	ctx := context.Background()

	// Connect to 'default' first
	conn, err := clickhouse.Open(connectOpts("default"))
	if err != nil {
		return fmt.Errorf("clickhouse: connect failed: %w", err)
	}
	defer conn.Close()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse: ping failed: %w", err)
	}

	// Check if database exists
	var dbCount uint64
	conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT count() FROM system.databases WHERE name = '%s'`, w.cfg.Database,
	)).Scan(&dbCount)

	if dbCount == 0 {
		w.logger.Info("clickhouse: creating database", "database", w.cfg.Database)
		if err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, w.cfg.Database)); err != nil {
			return fmt.Errorf("clickhouse: create database failed: %w", err)
		}
	}

	// Reconnect to the target database
	targetConn, err := clickhouse.Open(connectOpts(w.cfg.Database))
	if err != nil {
		return fmt.Errorf("clickhouse: reconnect failed: %w", err)
	}
	w.conn = targetConn

	db := w.cfg.Database

	ttlClause := ""
	if w.cfg.TTLDays > 0 {
		ttlClause = fmt.Sprintf("TTL toDateTime(timestamp) + INTERVAL %d DAY", w.cfg.TTLDays)
	}

	if w.migrateNeeded(ctx, w.conn) {
		w.logger.Info("clickhouse: creating tables", "database", db)
		queries := []string{
			fmt.Sprintf(`
				CREATE TABLE "%s".dns_raw (
					timestamp DateTime64(9),
					query_ip String,
					query_port UInt16,
					response_ip String,
					response_port UInt16,
					family String,
					protocol String,
					qname String,
					qtype String,
					qclass String,
					rcode String,
					opcode UInt8,
					length UInt16,
					dns_id UInt16,
					qr UInt8,
					tc UInt8,
					aa UInt8,
					ra UInt8,
					ad UInt8,
					rd UInt8,
					cd UInt8,
					qdcount UInt16,
					ancount UInt16,
					nscount UInt16,
					arcount UInt16,
					malformed UInt8,
					edns_version UInt8,
					edns_udp_size UInt16,
					edns_rcode UInt8,
					edns_dnssec_ok UInt8,
					edns_client_subnet String,
					client_country String,
					client_city String,
					client_asn String,
					latency Float64,
					latency_ms Int64,
					dnstap_identity String,
					dnstap_version String,
					dnstap_type String,
					dnstap_operation String,
					socket_ip String,
					socket_port UInt16,
				policy_rule String,
				policy_type String,
				policy_match String,
				policy_value String,
				http_protocol String,
				peer_name String,
				query_zone String,
				extra String,
				edns_options String,
				is_malicious UInt8,
				threat_category String,
				is_anomaly UInt8,
				anomaly_types Array(String),
				anomaly_score Float32,
				entropy_score Float32
				) ENGINE = MergeTree
				ORDER BY (timestamp, query_ip)
				%s
			`, db, ttlClause),
			fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS "%s".dns_answers (
				timestamp DateTime64(9),
				query_ip String,
				qname String,
					answer_name String,
					rdatatype String,
					rdata String,
					ttl UInt32,
					section String,
					answer_country String,
					answer_city String,
					answer_asn String
				) ENGINE = MergeTree
				ORDER BY (timestamp, qname)
				%s
			`, db, ttlClause),
		}

		for _, q := range queries {
			q := strings.TrimSpace(q)
			if q == "" {
				continue
			}
			if err := w.conn.Exec(ctx, q); err != nil {
				w.logger.Warn("clickhouse: migration query failed", "error", err)
			}
		}
	}

	// Always run ALTER TABLE for schema upgrades and TTL changes
	alterQueries := []string{
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS malformed UInt8`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS dnstap_type String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS socket_ip String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS socket_port UInt16`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS edns_rcode UInt8`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS latency_ms Int64`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS policy_rule String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS policy_type String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS policy_match String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS policy_value String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS peer_name String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS http_protocol String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS query_zone String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS extra String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_answers ADD COLUMN IF NOT EXISTS answer_country String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_answers ADD COLUMN IF NOT EXISTS answer_asn String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_answers ADD COLUMN IF NOT EXISTS query_ip String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_answers ADD COLUMN IF NOT EXISTS answer_city String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS edns_options String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS is_malicious UInt8`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS threat_category String`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS is_anomaly UInt8`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS anomaly_types Array(String)`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS anomaly_score Float32`, db),
		fmt.Sprintf(`ALTER TABLE "%s".dns_raw ADD COLUMN IF NOT EXISTS entropy_score Float32`, db),
	}
	if w.cfg.TTLDays > 0 {
		modTTL := fmt.Sprintf(`ALTER TABLE "%s".dns_raw MODIFY TTL toDateTime(timestamp) + INTERVAL %d DAY`, db, w.cfg.TTLDays)
		alterQueries = append(alterQueries, modTTL)
		modTTL = fmt.Sprintf(`ALTER TABLE "%s".dns_answers MODIFY TTL toDateTime(timestamp) + INTERVAL %d DAY`, db, w.cfg.TTLDays)
		alterQueries = append(alterQueries, modTTL)
	} else {
		alterQueries = append(alterQueries,
			fmt.Sprintf(`ALTER TABLE "%s".dns_raw REMOVE TTL`, db),
			fmt.Sprintf(`ALTER TABLE "%s".dns_answers REMOVE TTL`, db),
		)
	}
	for _, q := range alterQueries {
		if err := w.conn.Exec(ctx, q); err != nil {
			w.logger.Warn("clickhouse: alter table failed", "error", err)
		}
	}

	// Auto-create Materialized Views for Top Analytics & Anomalies if missing
	mvQueries := []string{
		fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS "%s".mv_top_domains_hourly
			ENGINE = SummingMergeTree ORDER BY (hour, qname)
			AS SELECT
			    toStartOfHour(timestamp) AS hour,
			    qname,
			    count() AS queries,
			    countIf(qr = 1) AS responses,
			    countIf(rcode = 'NXDOMAIN') AS nxdomains,
			    countIf(is_anomaly = 1) AS anomalies
			FROM "%s".dns_raw
			GROUP BY hour, qname
		`, db, db),
		fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS "%s".mv_dns_anomalies_hourly
			ENGINE = SummingMergeTree ORDER BY (hour, query_ip)
			AS SELECT
			    toStartOfHour(timestamp) AS hour,
			    query_ip,
			    client_country,
			    count() AS total_anomalies,
			    countIf(has(anomaly_types, 'DNS_TUNNELING')) AS tunneling_count,
			    countIf(has(anomaly_types, 'DGA_DOMAINS')) AS dga_count,
			    countIf(has(anomaly_types, 'NXDOMAIN_FLOOD')) AS nxdomain_flood_count,
			    countIf(has(anomaly_types, 'REBINDING_ATTACK_RISK')) AS rebinding_count
			FROM "%s".dns_raw
			WHERE is_anomaly = 1
			GROUP BY hour, query_ip, client_country
		`, db, db),
		fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS "%s".mv_top_clients_hourly
			ENGINE = SummingMergeTree ORDER BY (hour, query_ip)
			AS SELECT
			    toStartOfHour(timestamp) AS hour,
			    query_ip,
			    client_country,
			    client_city,
			    client_asn,
			    count() AS queries,
			    countIf(is_anomaly = 1) AS anomalies,
			    countIf(is_malicious = 1) AS threats
			FROM "%s".dns_raw
			GROUP BY hour, query_ip, client_country, client_city, client_asn
		`, db, db),
		fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS "%s".mv_dns_abuse_hourly
			ENGINE = SummingMergeTree ORDER BY (hour, query_ip, threat_category)
			AS SELECT
			    toStartOfHour(timestamp) AS hour,
			    query_ip,
			    threat_category,
			    countIf(is_malicious = 1) AS threat_count,
			    countIf(is_anomaly = 1) AS anomaly_count,
			    countIf(has(anomaly_types, 'DNS_TUNNELING')) AS tunneling_count,
			    countIf(has(anomaly_types, 'HIGH_QUERY_RATE_FLOOD')) AS flood_count,
			    countIf(has(anomaly_types, 'NXDOMAIN_FLOOD')) AS nxdomain_count
			FROM "%s".dns_raw
			WHERE is_malicious = 1 OR is_anomaly = 1
			GROUP BY hour, query_ip, threat_category
		`, db, db),
		fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS "%s".mv_qtype_distribution_hourly
			ENGINE = SummingMergeTree ORDER BY (hour, qtype)
			AS SELECT
			    toStartOfHour(timestamp) AS hour,
			    qtype,
			    count() AS count
			FROM "%s".dns_raw
			GROUP BY hour, qtype
		`, db, db),
	}
	for _, q := range mvQueries {
		q := strings.TrimSpace(q)
		if err := w.conn.Exec(ctx, q); err != nil {
			w.logger.Warn("clickhouse: create materialized view failed", "error", err)
		}
	}

	w.startFlusher()

	w.logger.Info("clickhouse: schema up-to-date", "database", db)
	return nil
}

func (w *Writer) Write(event domain.DNSRawEvent) error {
	select {
	case w.queue <- event:
		return nil
	default:
		w.logger.Warn("clickhouse: queue full, dropping event")
		return nil
	}
}

func (w *Writer) startFlusher() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		events := make([]domain.DNSRawEvent, 0, 1000)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		flush := func() {
			if len(events) == 0 {
				return
			}
			if err := w.flushBatch(events); err != nil {
				w.logger.Error("clickhouse: flush failed", "error", err)
			}
			events = events[:0]
		}

		for {
			select {
			case <-w.done:
				flush()
				return
			case evt := <-w.queue:
				events = append(events, evt)
				if len(events) >= 1000 {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()
}

func (w *Writer) flushBatch(events []domain.DNSRawEvent) error {
	if w.conn == nil || len(events) == 0 {
		return nil
	}

	ctx := context.Background()

	rawBatch, err := w.conn.PrepareBatch(ctx, fmt.Sprintf(`
		INSERT INTO "%s".dns_raw (
			timestamp, query_ip, query_port, response_ip, response_port,
			family, protocol, qname, qtype, qclass, rcode, opcode, length,
			dns_id, qr, tc, aa, ra, ad, rd, cd,
			qdcount, ancount, nscount, arcount, malformed,
			edns_version, edns_udp_size, edns_rcode, edns_dnssec_ok, edns_client_subnet,
			client_country, client_city, client_asn,
			latency, latency_ms,
			dnstap_identity, dnstap_version, dnstap_type, dnstap_operation,
			socket_ip, socket_port,
			policy_rule, policy_type, policy_match, policy_value,
			http_protocol, peer_name, query_zone, extra, edns_options,
			is_malicious, threat_category,
			is_anomaly, anomaly_types, anomaly_score, entropy_score
		)`, w.cfg.Database,
	))
	if err != nil {
		return fmt.Errorf("prepare raw batch: %w", err)
	}

	ansBatch, err := w.conn.PrepareBatch(ctx, fmt.Sprintf(`
		INSERT INTO "%s".dns_answers (timestamp, query_ip, qname, answer_name, rdatatype, rdata, ttl, section, answer_country, answer_city, answer_asn)
		`, w.cfg.Database,
	))
	if err != nil {
		return fmt.Errorf("prepare answers batch: %w", err)
	}

	for _, evt := range events {
		ts := evt.DNSTap.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}

		if err := rawBatch.Append(
			ts,
			evt.Network.QueryIP,
			uint16(evt.Network.QueryPort),
			evt.Network.ResponseIP,
			uint16(evt.Network.ResponsePort),
			evt.Network.Family,
			evt.Network.Protocol,
			evt.DNS.QName,
			evt.DNS.QType,
			evt.DNS.QClass,
			evt.DNS.RCode,
			uint8(evt.DNS.Opcode),
			uint16(evt.DNS.Length),
			uint16(evt.DNS.ID),
			boolToUint8(evt.DNS.Flags.QR),
			boolToUint8(evt.DNS.Flags.TC),
			boolToUint8(evt.DNS.Flags.AA),
			boolToUint8(evt.DNS.Flags.RA),
			boolToUint8(evt.DNS.Flags.AD),
			boolToUint8(evt.DNS.Flags.RD),
			boolToUint8(evt.DNS.Flags.CD),
			uint16(evt.DNS.Qdcount),
			uint16(evt.DNS.Ancount),
			uint16(evt.DNS.Nscount),
			uint16(evt.DNS.Arcount),
			boolToUint8(evt.DNS.Malformed),
			uint8(evt.EDNS.Version),
			uint16(evt.EDNS.UDPSize),
			uint8(evt.EDNS.RCode),
			boolToUint8(evt.EDNS.DNSSECOK),
			evt.EDNS.ClientSubnet,
			evt.GeoIP.ClientCountry,
			evt.GeoIP.ClientCity,
			evt.GeoIP.ClientASN,
			evt.DNSTap.Latency,
			evt.DNSTap.LatencyMs,
			evt.DNSTap.Identity,
			evt.DNSTap.Version,
			evt.DNSTap.Type,
			evt.DNSTap.Operation,
			evt.DNSTap.SocketIP,
			uint16(evt.DNSTap.SocketPort),
			evt.DNSTap.PolicyRule,
			evt.DNSTap.PolicyType,
			evt.DNSTap.PolicyMatch,
			evt.DNSTap.PolicyValue,
			evt.DNSTap.HTTPProtocol,
			evt.DNSTap.PeerName,
			evt.DNSTap.QueryZone,
			evt.DNSTap.Extra,
			ednsOptionsJSON(evt.EDNS.Options),
			boolToUint8(evt.Threat.Malicious),
			evt.Threat.Category,
			boolToUint8(evt.Anomaly.Detected),
			evt.Anomaly.Types,
			float32(evt.Anomaly.Score),
			float32(evt.Anomaly.EntropyScore),
		); err != nil {
			return fmt.Errorf("raw batch append: %w", err)
		}

		for section, rrs := range evt.DNS.Resource {
			for _, rr := range rrs {
				country := ""
				city := ""
				asn := ""
				if rr.Geo != nil {
					country = rr.Geo.Country
					city = rr.Geo.City
					asn = rr.Geo.ASN
				}
				if err := ansBatch.Append(
					ts,
					evt.Network.QueryIP,
					evt.DNS.QName,
					rr.Name,
					rr.RDataType,
					rr.RData,
					uint32(rr.TTL),
					section,
					country,
					city,
					asn,
				); err != nil {
					return fmt.Errorf("answers batch append: %w", err)
				}
			}
		}
	}

	if err := rawBatch.Send(); err != nil {
		return fmt.Errorf("raw batch send: %w", err)
	}
	if err := ansBatch.Send(); err != nil {
		return fmt.Errorf("answers batch send: %w", err)
	}
	return nil
}

func ednsOptionsJSON(opts []domain.EDNSOption) string {
	if len(opts) == 0 {
		return ""
	}
	b, err := json.Marshal(opts)
	if err != nil {
		return ""
	}
	return string(b)
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func (w *Writer) Close() error {
	close(w.done)
	w.wg.Wait()

	if w.conn != nil {
		w.conn.Close()
	}

	w.logger.Info("clickhouse: closed")
	return nil
}
