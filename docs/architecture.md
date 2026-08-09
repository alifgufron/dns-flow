# DNS-Flow Architecture

## Overview

dns-flow runs in one of two modes, selected by the `mode` key in the config.

### `mode: collect` — full pipeline

```
DNS Client → DNS Server (BIND/PowerDNS/Unbound/DNSDist/...)
                           │
                           │  DNSTAP framestream [RFC 8618]
                           │  server dials in ─── TCP :6000 or Unix socket
                           ▼
                  dns-flow collect  (listener)
                           │  decode (26 RR types, EDNS, policy metadata)
                           │  enrich (MaxMind GeoIP: client IP + A/AAAA RDATA)
                           │  correlate (CLIENT_QUERY ↔ CLIENT_RESPONSE, latency)
                           ▼
                  Kafka (mandatory buffer)
                   topic: dns.raw
                           ▼
                 dns-flow consumer
                    ├── ClickHouse (dns_raw + dns_answers)
                    ├── InfluxDB v1 (dns_query)
                    ├── InfluxDB v2 (dns_query)
                    └── File (JSON Lines)
```

Both the collector and the consumer run inside the same process; Kafka decouples
them so storage outages never block DNSTAP ingestion.

### `mode: relay` — stateless passthrough

```
DNS Server ──unix socket──▶ dns-flow relay ──TCP FSTRM──▶ dns-flow collect (remote)
             (dials in)      (listens)        (dials out)     (listens)
```

No decode, no Kafka, no storage — see [Relay Mode](#relay-mode) below.

### Connection direction

In every case the DNS server acts as the **client**. dns-flow creates the
listener (TCP port or Unix socket) and accepts connections, the same role as
`fstrm_capture`. The only outbound connection dns-flow makes is the relay
output, which dials the remote collector.

Config reload via SIGHUP — critical changes log "restart required".

## Relay Mode

`mode: relay` runs dns-flow as a stateless FSTRM frame relay: it reads frames
from `relay.input` and forwards them, **payload untouched**, to `relay.output`
(typically a remote dns-flow collector).

- No DNSTAP decode, no Kafka, no GeoIP, no storage — the frame bytes are never inspected.
- The content type negotiated with the input (e.g. `protobuf:dnstap.Dnstap`) is reused for the output handshake.
- A unix input is **listened on** by the relay (the source dials in, like `fstrm_capture`); a tcp input and both output types are **dialed** by the relay.
- Frames are buffered in an in-memory queue; when the queue is full, new frames are dropped (logged) so the producer is never blocked.
- Both input and output reconnect automatically on failure.

Because relay mode holds frames only in memory, a long output outage combined
with a full queue drops frames. Use it as a transport hop, not as a buffer —
the durability guarantee comes from Kafka on the collect side.

## References

| Protocol | RFC / Specification |
|----------|-------------------|
| DNSTAP Framestream | [RFC 8618 — DNSTAP: A Protocol for DNS Traffic Transport](https://www.rfc-editor.org/info/rfc8618) |
| DNS Message Format | [RFC 1035 — Domain Names - Implementation and Specification](https://www.rfc-editor.org/info/rfc1035) |
| EDNS(0) | [RFC 6891 — Extension Mechanisms for DNS (EDNS(0))](https://www.rfc-editor.org/info/rfc6891) |
| EDNS Client Subnet | [RFC 7871 — Client Subnet in DNS Queries](https://www.rfc-editor.org/info/rfc7871) |
| DNS Classification | [RFC 1035 §3.2.2](https://www.rfc-editor.org/info/rfc1035), [IANA DNS Parameters](https://www.iana.org/assignments/dns-parameters/) |
| RR Types (26 parsed) | [IANA Resource Record Types](https://www.iana.org/assignments/dns-parameters/) |

## Directory Structure

```
dns-flow/
├── cmd/dns-flow/
│   └── main.go              # Entry point: DNSTAP → Kafka → Storage + SIGHUP reload
├── configs/
│   └── config.yaml           # Default configuration
├── docs/
│   ├── architecture.md       # This file
│   ├── usage.md              # Build, configuration, run, troubleshooting
│   ├── kafka-setup.md        # Kafka installation & configuration
│   ├── dnstap-sources.md     # DNSTAP source setup (DNSDist, BIND, PowerDNS, Unbound)
│   └── outputs.md            # Output format details & query examples
├── internal/
│   ├── domain/               # Entities + interfaces (ports)
│   │   ├── dns.go            # DNSInfo, Flags, DNSRR (with RRGeoInfo)
│   │   ├── network.go        # NetworkInfo
│   │   ├── dnstap.go         # DNSTapInfo (socket-ip, policy fields, latency-ms)
│   │   ├── event.go          # DNSRawEvent, EDNSInfo (Options[]), GeoIPInfo
│   │   └── ports.go          # Storage, Pipeline, GeoIPResolver interfaces
│   ├── usecase/
│   │   ├── pipeline.go       # Bounded channel pipeline, worker pool
│   │   └── correlator.go     # Query-response correlation (clientIP:dnsID state map)
│   ├── relay/
│   │   └── relay.go          # Stateless FSTRM frame relay (mode: relay)
│   ├── adapter/
│   │   ├── inbound/
│   │   │   ├── dnstap/
│   │   │   │   └── server.go # Framestream receiver (TCP or Unix socket listener; source dials in) + miekg/dns parse (26 RR types)
│   │   │   └── kafka/
│   │   │       └── consumer.go # Kafka consumer (topic → storage)
│   │   └── outbound/
│   │       ├── kafka/        # kafka-go batch producer (sync, compression: none/gzip/lz4/zstd)
│   │       ├── clickhouse/   # clickhouse-go/v2 native batch writer (edns_options JSON)
│   │       ├── influxdb/     # InfluxDB v1 HTTP line protocol
│   │       ├── influxdb_v2/  # InfluxDB v2 HTTP line protocol (token auth)
│   │       ├── file/         # Rotating JSON lines writer
│   │       └── geoip/        # MaxMind City + ASN lookup (per-client + per-RDATA A/AAAA)
│   └── infrastructure/
│       ├── config/           # YAML config loader + validation
│       └── logger/           # slog structured logger
└── go.mod
```

## Data Model

### `DNSRawEvent` — Main event sent to all storages

```
DNSRawEvent
├── Network    (query_ip, query_port, response_ip, response_port, family, protocol)
├── DNS        (qname, qtype, qclass, rcode, opcode, length, id, flags, resource records)
├── EDNS       (version, udp_size, rcode, dnssec_ok, client_subnet, options[])
├── DNSTap     (socket_ip, socket_port, identity, version, type, operation,
│               timestamp, latency, latency_ms, extra, peer_name, query_zone,
│               policy_rule/type/match/value, http_protocol)
└── GeoIP      (client_ip, client_country, client_city, client_asn) — enriched by pipeline
```

### `DNSInfo` — Full DNS message preservation

| Field | Type | Description |
|-------|------|-------------|
| `qname` | string | Query domain name |
| `qtype` | string | Query type (A, AAAA, MX, HTTPS, etc.) |
| `qclass` | string | Query class (IN, etc.) |
| `rcode` | string | Response code (NOERROR, NXDOMAIN, etc.) |
| `opcode` | int | DNS opcode |
| `length` | int | Wire length in bytes |
| `id` | int | DNS transaction ID |
| `qdcount` | int | Question count |
| `ancount` | int | Answer count |
| `nscount` | int | Authority count |
| `arcount` | int | Additional count |
| `flags` | Flags | All DNS header flags (QR, TC, AA, RA, AD, RD, CD) |
| `resource` | map[string][]DNSRR | Resource records per section (an, ns, ar) |
| `malformed` | bool | True if DNS wire parse failed |

### `DNSRR` — Resource Record

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Owner name |
| `rdatatype` | string | RR type (A, AAAA, CNAME, MX, HTTPS, SVCB, DS, RRSIG, etc.) |
| `class` | string | Class (IN) |
| `ttl` | int | Time to live |
| `rdata` | string | RDATA in human-readable format |
| `geoip` | *RRGeoInfo | Per-record GeoIP (A/AAAA only): country, city, asn |

26 RR types parsed explicitly: A, AAAA, CNAME, MX, NS, PTR, SOA, TXT, SRV, HTTPS, SVCB, DS, DNSKEY, RRSIG, NSEC, NSEC3, CAA, LOC, SSHFP, TLSA. Fallback to `rr.String()` for others (ref: [IANA RR Types](https://www.iana.org/assignments/dns-parameters/)).

### `GeoIPInfo` — Enrichment result

| Field | Type | Description |
|-------|------|-------------|
| `client-ip` | string | Original client IP |
| `client-country` | string | ISO country code |
| `client-city` | string | City name |
| `client-asn` | string | AS number (e.g. "AS15169") |

Per-record GeoIP in `resource-records.an[].geoip` for A/AAAA RDATA.

## Kafka Buffer (Mandatory)

All events from DNSTAP must pass through Kafka (`dns.raw` topic) before reaching storage. This ensures:
- **Zero data loss** — if ClickHouse/InfluxDB goes down, data is safe in Kafka
- **Replay** — replay from any offset at any time
- **Scaling** — collector and consumer can scale independently

### Retention Policy

Each storage output supports configurable data retention:

| Output | Config Key | Default | Description |
|--------|-----------|---------|-------------|
| ClickHouse | `outputs.clickhouse.ttl_days` | `30` | Data TTL in days (0 = infinite). Applied via `TTL` on `dns_raw` and `dns_answers` tables, updated via `MODIFY TTL` on config reload. |
| InfluxDB v1 | `outputs.influxdb.retention_days` | `0` | Retention duration in days (0 = infinite). Auto-creates/alters retention policy via InfluxDB query API. |
| InfluxDB v2 | `outputs.influxdb_v2.retention_days` | `0` | Bucket retention in days (0 = infinite). Updated via InfluxDB v2 `PATCH /api/v2/buckets/{id}`. |
| Kafka | `kafka.topic.retention_ms` | `""` | Topic retention in milliseconds (e.g. `"604800000"` for 7d, `"-1"` for infinite). Set via `IncrementalAlterConfigs` on startup. Empty string = broker default. |

### Kafka Producer (Collector)
- **Library**: segmentio/kafka-go
- **Format**: JSON-encoded `DNSRawEvent`
- **Compression**: none / gzip / lz4 / zstd (snappy removed due to incompatibility with Kafka 3.9.2)
- **Partitioner**: Hash by qname (`kafka.Hash{}`) — ordering per-domain guaranteed
- **Batch**: configurable size + flush interval
- **Write mode**: synchronous (`Async: false`), `RequiredAcks: RequireOne`
- **Retry**: 3 attempts on transient errors (`UnknownTopicOrPartition`, `LeaderNotAvailable`, `RequestTimedOut`)

### Kafka Consumer
- **GroupID**: `dns-flow` (default, via config)
- **Offset**: Start from earliest (`FirstOffset`) on first run (no committed offset); resumes from committed offset on restart
- **Replay**: `kafka-consumer-groups.sh --group dns-flow --topic dns.raw --reset-offsets --to-earliest --execute`
- **Deserialize**: JSON → `DNSRawEvent`
- **Dispatch**: Write to all configured storages; log every 1000 events

### Topic Creation
- **Auto-create**: dns-flow creates topic `dns.raw` (1 partition, replication 1) via Kafka admin API at startup if missing
- **Override**: Create topic manually with desired partition count before running dns-flow

## Query-Response Correlator

Runs in the pipeline worker pool:
- **Store**: QUERY events (QR=false) stored in map keyed by `clientIP:dnsID`
- **Lookup**: RESPONSE events (QR=true) matched against stored query, entry deleted
- **Orphaned response**: RESPONSE without matching QUERY → debug log
- **Unanswered query**: QUERY without RESPONSE >30s → evicted + warn log
- **Mismatch**: qname/qtype different between stored and response → warn log

## Storage Outputs

See [Output Formats](outputs.md) for full schema details, query examples, and field coverage per storage.

### ClickHouse
- **Library**: clickhouse-go/v2 native TCP
- **Tables** (2 tables, relational — supports JOIN):
  - `dns_raw` — 1 row per DNS event
  - `dns_answers` — 1 row per resource record (normalized: 1 event → N rows)
- **Rationale**: JOIN enables queries like "find all domains returning Cloudflare IPs (AS13335)" — impossible in a flat timeseries DB
- **Batch**: 1000 events or 1 second flush
- **Migration**: Auto-create database + tables (checks system.tables first) + ALTER TABLE upgrades always run
- **TTL**: Configurable via `ttl_days` (default 30, 0 = infinite)

### InfluxDB v1
- **Protocol**: HTTP line protocol with Basic Auth
- **Measurement**: 1 flat measurement (`dns_query`) — InfluxDB is a timeseries database, no JOIN
- **Tags**: identity, operation, dnstap_version, dnstap_type, client_country, client_city, client_asn
- **Migration**: Auto-creates database (checks SHOW DATABASES first)

### InfluxDB v2
- **Protocol**: HTTP `/api/v2/write` with Token auth
- **Same measurement model** as v1

### File
- **Format**: JSON lines (append)
- **Rotation**: Configurable max_size, max_age, max_backups
- **Compression**: Rotated files gzip'd when `compress: true`

## Pipeline (Collector → Kafka → Consumer)

- **Type**: Bounded channel + worker pool
- **Workers**: Configurable (default 4)
- **Queue**: Configurable (default 100,000 events)
- **Backpressure**: Non-blocking send → drop on full queue
- **Ordering**: No ordering guarantee (parallel workers)
- **Enrichment**: GeoIP client + per-RDATA A/AAAA (city, country, asn)
- **Shutdown**: close die channel → close queue → wg.Wait() → close storages → panic-safe

## GeoIP

- **Source**: MaxMind GeoLite2 City + ASN databases
- **Library**: oschwald/maxminddb-golang
- **Lookup**: By query IP (client-country/city/asn) + per-RDATA A/AAAA IP (country/city/asn)
- **Failure mode**: Warn + skip enrichment (non-fatal)

## Graceful Shutdown

1. SIGINT/SIGTERM received
2. Stop Kafka consumer → flush pending storage batch
3. Stop DNSTAP server
4. Drain pipeline queue → close workers + correlator → flush Kafka producer
5. Close all storage connections

**Zero data loss** as long as the Kafka cluster remains available.

## Config Reload (SIGHUP)

- SIGHUP → re-read config file, validate
- Log detected changes
- Critical changes (Kafka, outputs, pipeline, dnstap) → log "restart required"
- Non-critical (log level) → log change

## Target Performance

- **500K QPS** with 4-8 collector instances
- Kafka as shock absorber
- Configurable worker count per instance
