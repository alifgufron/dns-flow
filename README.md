# DNS-Flow

A production-grade DNS traffic flow collector. Receives DNSTAP from any DNS server (BIND, PowerDNS, Unbound, DNSDist, etc.), enriches with GeoIP, buffers through Kafka, and stores to ClickHouse, InfluxDB (v1/v2), or file.

## Architecture

```
DNS Server (BIND/PowerDNS/Unbound/DNSDist/...) → DNSTAP → Collector (decode + enrich + correlate)
                       ↓
                   Kafka (mandatory buffer)
                       ↓
                   Consumer
                     ├── ClickHouse
                     ├── InfluxDB v1 / v2
                     └── File (JSON Lines)
```

Kafka is a **mandatory buffer** — every DNS event passes through Kafka before reaching storage, ensuring zero data loss and replay capability.

## Features

- **DNSTAP framestream receiver** (compatible with any DNSTAP source: BIND, PowerDNS, Unbound, DNSDist, etc.) with full DNS wire parsing (26 RR types, EDNS options, DNSDist policy metadata)
- **GeoIP enrichment** per client IP and per A/AAAA RDATA (country, city, ASN) via MaxMind
- **Kafka mandatory buffer** (kafka-go, sync producer, compression: gzip/lz4/zstd)
- **Query-response correlation** — matches CLIENT_QUERY ↔ CLIENT_RESPONSE by client IP + DNS ID; detects orphaned responses and unanswered queries
- **Multiple outputs** — ClickHouse (native TCP batch), InfluxDB v1/v2 (HTTP line protocol), File (rotating JSON lines)
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
