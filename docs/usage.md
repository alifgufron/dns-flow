# DNS-Flow Usage Guide

## How it fits together

`mode: collect` — dns-flow listens, the DNS server connects to it, and every
event is buffered through Kafka before it reaches storage:

```
DNS Server ──DNSTAP (TCP :6000 or unix socket)──▶ dns-flow collect
                                                       │ decode + GeoIP + correlate
                                                       ▼
                                              Kafka  topic: dns.raw
                                                       ▼
                                              dns-flow consumer
                                                ├── ClickHouse
                                                ├── InfluxDB v1 / v2
                                                └── File (JSON Lines)
```

`mode: relay` — for a DNS host that cannot reach the collector directly. It
forwards raw FSTRM frames, with no decode, no Kafka and no storage:

```
DNS Server ──unix socket──▶ dns-flow relay ──TCP FSTRM──▶ dns-flow collect (remote)
```

See [architecture.md](architecture.md) for the detailed data flow.

## Prerequisites

- Go 1.25+ (to build from source)
- [Kafka cluster](kafka-setup.md) — required in collect mode; all events pass through Kafka (not needed for `mode: relay`)
- At least one [output storage](outputs.md): ClickHouse, InfluxDB v1/v2, or File
- (Optional) MaxMind GeoLite2 databases for GeoIP enrichment
- A DNS server with [DNSTAP output](dnstap-sources.md) (DNSDist, BIND, PowerDNS, Unbound, etc.)

## Build

`make build` detects the host OS and architecture and produces `bin/dns-flow-<os>-<arch>`:

```bash
git clone <repo-url> dns-flow
cd dns-flow
make build
```

Cross-compile:

```bash
make build-linux      # bin/dns-flow-linux-amd64
make build-freebsd    # bin/dns-flow-freebsd-amd64
make build-all        # both
make build GOOS=linux GOARCH=arm64   # any other target
```

## Install

`make install` detects the host OS and installs the matching layout. It creates
the `dnsflow` service user, installs the binary and service unit, and installs
the config **only if it does not already exist** (the current template is always
written to `<config>.sample`, so an upgrade never overwrites a live config).

```bash
sudo make install                     # host OS layout
sudo make install GOOS=freebsd        # force the FreeBSD layout
make install DESTDIR=/tmp/stage       # staging for packaging (skips user creation)
```

| Path | Linux | FreeBSD |
|------|-------|---------|
| Binary | `/usr/local/sbin/dns-flow` | `/usr/local/sbin/dns-flow` |
| Config | `/etc/dns-flow.yaml` | `/usr/local/etc/dns-flow.yaml` |
| Service unit | `/etc/systemd/system/dns-flow.service` | `/usr/local/etc/rc.d/dns-flow` |
| Data | `/var/lib/dns-flow` | `/var/db/dns-flow` |
| Runtime (unix socket) | `/run/dns-flow` | `/var/run/dns-flow` |
| Logs | `/var/log/dns-flow` | `/var/log/dns-flow` |

The config is installed mode `640` owned by group `dnsflow` because it contains
storage passwords and API tokens.

Enable the service:

```bash
# Linux
sudo systemctl daemon-reload && sudo systemctl enable --now dns-flow
sudo systemctl reload dns-flow        # SIGHUP config reload

# FreeBSD
sudo sysrc dns_flow_enable=YES
sudo service dns-flow start
sudo service dns-flow reload          # SIGHUP config reload
```

`make uninstall` removes the binary and service unit but keeps the config, data
directory, and service user.

### Unix socket permissions

When `dnstap.type: unix` (or `relay.input.type: unix`), dns-flow creates the
socket with mode `0660` owned by the service user and group. The DNS server
connecting to it must be able to write to the socket, so add its user to the
`dnsflow` group (or run both under the same group):

```bash
sudo usermod -aG dnsflow bind      # Linux (BIND runs as 'bind' or 'named')
sudo pw groupmod dnsflow -m bind   # FreeBSD
```

Place the socket in the runtime directory (`/run/dns-flow/dnstap.sock` on Linux,
`/var/run/dns-flow/dnstap.sock` on FreeBSD). Avoid `/tmp`: the systemd unit sets
`PrivateTmp=true`, so a socket created there is invisible to other services.

## Kafka topic

Collect mode requires a reachable Kafka cluster and the `dns.raw` topic. Relay
mode does not use Kafka at all.

**Development** — dns-flow auto-creates the topic at startup (1 partition,
replication factor 1) if it does not exist, so no action is needed.

**Production** — create the topic *before* starting dns-flow so you control the
partition count and replication factor:

```bash
kafka-topics.sh --create \
  --topic dns.raw \
  --partitions 6 \
  --replication-factor 3 \
  --bootstrap-server localhost:9092
```

Verify it:

```bash
kafka-topics.sh --describe --topic dns.raw --bootstrap-server localhost:9092
```

Events are partitioned by hash of `qname`, so ordering is guaranteed per-domain
and the partition count sets the maximum consumer parallelism. Retention is
controlled by `kafka.topic.retention_ms` in the config and applied on startup.

Full broker setup, partition sizing, replay and offset management:
[kafka-setup.md](kafka-setup.md).

## Configuration

All configuration is in `configs/config.yaml`. Enable an output by including its section; disable by removing it. At least one output is required.

The binary runs in one of two modes, selected by the `mode` key:

- `mode: collect` (default) — read DNSTAP input, decode, buffer via Kafka, store to outputs.
- `mode: relay` — pass FSTRM frames through untouched from `relay.input` to `relay.output`; no Kafka, no decode, no storage.

In collect mode the DNSTAP input can be either a TCP listener (a DNS source dials in, e.g. `dnstap` in DNSDist) or a unix socket (dns-flow creates the socket and listens; the source dials in, e.g. BIND's `dnstap.sock`). In both cases dns-flow is the server/listener and the DNS source is the client. Both variants are shown below.

### Collect mode — TCP listener (default, full example)

```yaml
server:
  name: "dns-flow"
  log_level: "info"            # info | debug | warn | error

mode: collect                  # collect (default) | relay

dnstap:
  type: tcp                    # tcp (default) | unix
  listen: "0.0.0.0:6000"       # Bind address for TCP FSTRM (source dials here)
  # unix_socket: "/path/to/dnstap.sock"   # Used when type: unix (dns-flow listens)

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
    path: "/var/lib/dns-flow/dns-flow.json"
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

### Collect mode — Unix socket (listen)

Use this when the DNS source (e.g. BIND) connects to a unix socket. dns-flow creates the socket and listens on it; the source dials in. The rest of the config is identical to the TCP listener example.

```yaml
server:
  name: "dns-flow"
  log_level: "info"            # info | debug | warn | error

mode: collect

dnstap:
  type: unix
  unix_socket: "/path/to/dnstap.sock"   # dns-flow creates & listens here

kafka:
  brokers:
    - "localhost:9092"
  topic:
    raw: "dns.raw"             # Single topic for all DNS events
    retention_ms: "604800000"  # Topic retention in ms (7d), -1 = infinite
  producer:
    batch_size: 100
    flush_interval: 1s
    compression: "none"        # none | gzip | lz4 | zstd
  consumer:
    group_id: "dns-flow"

outputs:
  # Remove any section you don't need. At least one output is required.
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
    measurement: "dns_query"
    retention_days: 0

  influxdb_v2:
    url: "http://localhost:8086"
    org: "my-org"
    bucket: "dns_flow"
    token: "my-token"
    precision: "ns"
    measurement: "dns_query"
    retention_days: 0

  file:
    path: "/var/lib/dns-flow/dns-flow.json"
    max_size_mb: 100
    max_age_days: 7
    max_backups: 3
    compress: true

pipeline:
  worker_count: 4              # Number of worker goroutines
  queue_size: 100000           # Max in-flight events (drops when full)
  enrichment:
    geoip_enabled: true

geoip:
  maxmind_db_path: "/data/geoip/GeoLite2-City.mmdb"
  asn_db_path: "/data/geoip/GeoLite2-ASN.mmdb"
```

### Relay mode

```yaml
mode: relay

relay:
  input:
    type: unix             # unix | tcp
    address: "/path/to/dnstap.sock"
  output:
    type: tcp              # unix | tcp (relay dials into this address)
    address: "10.11.12.13:6000"
  queue_size: 100000       # Max in-memory frames (drops when full)
  reconnect_interval: 5s   # Retry interval when input/output is unreachable
```

Connection direction: a unix input is **listened on** by the relay (the source, e.g. BIND, dials in — same role as `fstrm_capture`); a tcp input and both output types are **dialed** by the relay (it connects to a remote listener).

When the output (remote collector) is unreachable, frames accumulate in the in-memory queue. If the queue fills, new frames are dropped (logged periodically) so the input producer is never blocked. Both ends reconnect automatically.

### Minimal (Kafka + file only)

```yaml
server:
  name: "dns-flow"
  log_level: "info"

mode: collect

dnstap:
  type: tcp
  listen: "0.0.0.0:6000"

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
    path: "/var/lib/dns-flow/dns-flow.json"

pipeline:
  worker_count: 4
  queue_size: 100000
```

### Configuration Reference

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `server` | `name` | `dns-flow` | Service name (appears in logs) |
| `server` | `log_level` | `info` | Log level: `info`, `debug`, `warn`, `error` |
| (root) | `mode` | `collect` | Mode: `collect` or `relay` |
| `dnstap` | `type` | `tcp` | Input type: `tcp` (listen) or `unix` (listen) |
| `dnstap` | `listen` | `0.0.0.0:6000` | TCP address for DNSTAP framestream (type `tcp`) |
| `dnstap` | `unix_socket` | — | Unix socket path dns-flow creates and listens on (type `unix`) |
| `relay` | `input.type` | — | Relay input type: `unix` (relay listens) or `tcp` (relay dials) |
| `relay` | `input.address` | — | Relay input address: socket to listen on (unix) or remote address to dial (tcp) |
| `relay` | `output.type` | — | Relay output type: `unix` or `tcp` (relay dials both) |
| `relay` | `output.address` | — | Relay output address to dial |
| `relay` | `queue_size` | `100000` | Max in-memory frames (drops when full) |
| `relay` | `reconnect_interval` | `5s` | Retry interval when input/output is unreachable |
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
