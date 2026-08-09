# DNS-Flow

A DNS traffic flow collector. Receives DNSTAP from any DNS server (BIND, PowerDNS, Unbound, DNSDist, etc.) over TCP or a Unix socket, enriches with GeoIP, buffers through Kafka, and stores to ClickHouse, InfluxDB (v1/v2), or file. Can also run as a lightweight FSTRM frame relay (`mode: relay`).

## Architecture

`mode: collect` — the full pipeline. dns-flow listens (TCP or Unix socket), the DNS server connects to it:

```
DNS Server (BIND/PowerDNS/Unbound/DNSDist/...)
        │  DNSTAP / FSTRM  (dials in)
        ▼
  dns-flow collect ── listens on TCP :6000 or a Unix socket
        │  decode + enrich (GeoIP) + correlate (query ↔ response)
        ▼
  Kafka  (mandatory buffer, topic: dns.raw)
        │
        ▼
  dns-flow consumer
        ├── ClickHouse
        ├── InfluxDB v1 / v2
        └── File (JSON Lines)
```

`mode: relay` — stateless FSTRM passthrough for hosts that cannot reach the collector directly:

```
DNS Server ──unix socket──▶ dns-flow relay ──TCP FSTRM──▶ dns-flow collect (remote)
                            (no decode, no Kafka, no storage)
```

Kafka is a **mandatory buffer** in collect mode — every DNS event passes through Kafka before reaching storage, ensuring zero data loss and replay capability. Relay mode bypasses Kafka entirely; it only forwards frames.

Connection direction: in every case the DNS server is the **client**. dns-flow (collect, or relay on a unix input) creates the listener/socket and accepts connections — the same role as `fstrm_capture`.

## Features

- **DNSTAP framestream receiver** over TCP (listen) or Unix socket (listen) — dns-flow creates the socket and accepts connections from any DNSTAP source (BIND, PowerDNS, Unbound, DNSDist, etc.) with full DNS wire parsing (26 RR types, EDNS options, DNSDist policy metadata)
- **Relay mode** (`mode: relay`) — stateless FSTRM frame passthrough (e.g. BIND Unix socket → remote collector), no payload decoding, in-memory queue + auto-reconnect
- **GeoIP enrichment** per client IP and per A/AAAA RDATA (country, city, ASN) via MaxMind
- **Kafka mandatory buffer** (kafka-go, sync producer, compression: gzip/lz4/zstd)
- **Query-response correlation** — matches CLIENT_QUERY ↔ CLIENT_RESPONSE by client IP + DNS ID; detects orphaned responses and unanswered queries
- **Multiple outputs** — ClickHouse (native TCP batch), InfluxDB v1/v2 (HTTP line protocol), File (rotating JSON lines)
- **Configurable retention** — ClickHouse TTL, InfluxDB retention policy/bucket, Kafka topic retention
- **Graceful shutdown** — zero data loss on SIGINT/SIGTERM
- **Live config reload** — SIGHUP re-reads config, logs changes
- **Single binary** — `dns-flow -config config.yaml`

## Quick Start

```bash
# Build
make build
# or: go build -o bin/dns-flow ./cmd/dns-flow/

# Run (requires Kafka at localhost:9092)
./bin/dns-flow -config config.yaml
```

## Documentation

| File | Description |
|------|-------------|
| [Usage Guide](docs/usage.md) | Build, configuration, run, graceful shutdown, troubleshooting |
| [Architecture](docs/architecture.md) | Data model, pipeline, query-response correlation, storage design |
| [Kafka Setup](docs/kafka-setup.md) | Install & configure Kafka (FreeBSD + Linux), KRaft, topic management |
| [DNSTAP Sources](docs/dnstap-sources.md) | Configure DNS servers (DNSDist, BIND, PowerDNS, Unbound) |
| [Output Formats](docs/outputs.md) | ClickHouse schema + MV, InfluxDB measurement, File JSON, query examples |

## License

MIT
