# DNS-Flow Usage Guide

## Prerequisites

- Go 1.25+ (to build from source)
- [Kafka cluster](kafka-setup.md) (required — all events pass through Kafka)
- At least one [output storage](outputs.md): ClickHouse, InfluxDB v1/v2, or File
- (Optional) MaxMind GeoLite2 databases for GeoIP enrichment
- A DNS server with [DNSTAP output](dnstap-sources.md) (DNSDist, BIND, PowerDNS, Unbound, etc.)

## Build

```bash
git clone <repo-url> dns-flow
cd dns-flow
make build
# or: go build -o bin/dns-flow ./cmd/dns-flow/
```

Cross-compile for FreeBSD amd64:

```bash
make build-freebsd
# or: GOOS=freebsd GOARCH=amd64 go build -o bin/dns-flow-freebsd ./cmd/dns-flow/
```

## Configuration

All configuration is in `configs/config.yaml`. Enable an output by including its section; disable by removing it. At least one output is required.

### Minimal (Kafka + file only)

```yaml
server:
  name: "dns-flow"
  log_level: "info"

kafka:
  brokers:
    - "localhost:9092"
  topic:
    raw: "dns.raw"
  producer:
    batch_size: 100
    flush_interval: 1s
    compression: "zstd"        # none | gzip | lz4 | zstd

outputs:
  file:
    path: "./data/dns-flow.json"

pipeline:
  worker_count: 4
  queue_size: 100000
```

### Full (Kafka + ClickHouse + InfluxDB v1 + InfluxDB v2 + File)

```yaml
server:
  name: "dns-flow"
  log_level: "info"            # info | debug | warn | error

dnstap:
  listen: "0.0.0.0:6000"      # Bind address for DNSTAP connections

kafka:
  brokers:
    - "localhost:9092"
  topic:
    raw: "dns.raw"             # Single topic for all DNS events
    retention_ms: "604800000"  # Topic retention in ms (7d), -1 = infinite
  producer:
    batch_size: 100
    flush_interval: 1s
    compression: "zstd"        # none | gzip | lz4 | zstd
  consumer:
    group_id: "dns-flow"       # Consumer group ID for offset tracking

outputs:
  clickhouse:
    hosts:
      - "localhost:9000"
    database: "dns_flow"
    username: "default"
    password: ""
    compression: true
    ttl_days: 30               # Data TTL in days, 0 = infinite

  influxdb:
    url: "http://localhost:8086"
    database: "dns_flow"
    username: ""
    password: ""
    measurement: "dns_query"   # Measurement name, default "dns_query"
    retention_days: 0          # Retention duration in days, 0 = infinite

  influxdb_v2:
    url: "http://localhost:8086"
    org: "my-org"
    bucket: "dns_flow"
    token: "my-token"
    precision: "ns"
    measurement: "dns_query"
    retention_days: 0          # Bucket retention in days, 0 = infinite

  file:
    path: "./data/dns-flow.json"
    max_size_mb: 100
    max_age_days: 7
    max_backups: 3
    compress: true              # gzip rotated files

pipeline:
  worker_count: 4
  queue_size: 100000
  enrichment:
    geoip_enabled: true

geoip:
  maxmind_db_path: "/data/geoip/GeoLite2-City.mmdb"
  asn_db_path: "/data/geoip/GeoLite2-ASN.mmdb"
```

### Configuration Reference

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `server` | `name` | `dns-flow` | Service name (appears in logs) |
| `server` | `log_level` | `info` | Log level: `info`, `debug`, `warn`, `error` |
| `dnstap` | `listen` | `0.0.0.0:6000` | TCP address for DNSTAP framestream |
| `kafka` | `brokers` | — | List of Kafka broker addresses |
| `kafka.topic` | `raw` | `dns.raw` | Topic name for all DNS events |
| `kafka.topic` | `retention_ms` | `""` | Topic retention in ms (`"604800000"` = 7d), empty = broker default |
| `kafka.producer` | `batch_size` | `100` | Max messages per batch write |
| `kafka.producer` | `flush_interval` | `1s` | Max interval between flushes |
| `kafka.producer` | `compression` | `none` | Codec: `none`, `gzip`, `lz4`, `zstd` |
| `kafka.consumer` | `group_id` | `dns-flow` | Consumer group for offset tracking |
| `outputs.clickhouse` | `hosts` | — | ClickHouse native TCP hosts (`host:port`) |
| `outputs.clickhouse` | `database` | `dns_flow` | Database name (auto-created) |
| `outputs.clickhouse` | `compression` | `true` | Enable native protocol compression |
| `outputs.clickhouse` | `ttl_days` | `30` | Data TTL in days (0 = infinite) |
| `outputs.influxdb` | `url` | — | InfluxDB v1 HTTP URL |
| `outputs.influxdb` | `database` | `dns_flow` | Database name (auto-created) |
| `outputs.influxdb` | `measurement` | `dns_query` | Measurement name |
| `outputs.influxdb` | `retention_days` | `0` | Retention duration in days (0 = infinite) |
| `outputs.influxdb_v2` | `url` | — | InfluxDB v2 HTTP URL |
| `outputs.influxdb_v2` | `org` | — | InfluxDB v2 organization |
| `outputs.influxdb_v2` | `bucket` | — | InfluxDB v2 bucket |
| `outputs.influxdb_v2` | `token` | — | InfluxDB v2 API token |
| `outputs.influxdb_v2` | `precision` | `ns` | Write precision (`ns`, `us`, `ms`, `s`) |
| `outputs.influxdb_v2` | `measurement` | `dns_query` | Measurement name |
| `outputs.influxdb_v2` | `retention_days` | `0` | Bucket retention in days (0 = infinite) |
| `outputs.file` | `path` | — | Output file path (directory auto-created) |
| `outputs.file` | `max_size_mb` | `100` | Max file size before rotation (MB) |
| `outputs.file` | `max_age_days` | `0` | Max age before rotation (0 = no age limit) |
| `outputs.file` | `max_backups` | `0` | Max rotated files to keep (0 = keep all) |
| `outputs.file` | `compress` | `false` | Compress rotated files with gzip |
| `pipeline` | `worker_count` | `4` | Number of worker goroutines |
| `pipeline` | `queue_size` | `100000` | Max in-flight events (drops when full) |
| `pipeline.enrichment` | `geoip_enabled` | `true` | Enable MaxMind GeoIP enrichment |
| `geoip` | `maxmind_db_path` | — | Path to GeoLite2-City.mmdb |
| `geoip` | `asn_db_path` | — | Path to GeoLite2-ASN.mmdb |

## Running

```bash
./bin/dns-flow                              # uses default config path
./bin/dns-flow -config /etc/dns-flow.yaml   # custom config path
```

The binary auto-searches for config in: `./config.yaml`, `./configs/config.yaml`, `/usr/local/etc/dns-flow.yaml`, `/etc/dns-flow.yaml`.

### Config Reload (SIGHUP)

```bash
kill -HUP <pid>   # reload without restart
```

SIGHUP re-reads the config file, validates it, and logs detected changes. Critical changes (Kafka, outputs, pipeline, dnstap) require a manual restart.

## Graceful Shutdown

Send SIGTERM, SIGINT, or press Ctrl+C:

```bash
kill -TERM <pid>
# or
kill -INT <pid>
```

Shutdown sequence:
1. Stop Kafka consumer → flush pending storage batches
2. Stop accepting new DNSTAP connections
3. Drain pipeline queue → close workers and correlator → flush Kafka producer
4. Close all storage connections

## Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| `connection refused` to ClickHouse | ClickHouse not running | Start ClickHouse or remove `outputs.clickhouse` section |
| `geoip unavailable` | MMDB file not found | Download GeoLite2 or set `geoip_enabled: false` |
| `pipeline queue full, dropping event` | Throughput exceeds capacity | Increase `worker_count` or `queue_size` |
| `orphaned response` | RESPONSE without matching QUERY | Normal on cold start; investigate if persistent |
| `unanswered query evicted` | QUERY >30s without RESPONSE | Possible packet loss or slow resolver |
| Kafka broker not available | Kafka cluster down | Check Kafka connection; topic auto-created at startup |
| `-1 Unknown` error with snappy | Kafka 3.9.2 does not support snappy | Change compression to `zstd`, `lz4`, `gzip`, or `none` |
