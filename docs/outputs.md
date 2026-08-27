# Output Formats

## File (JSON Lines)

```json
{
  "network": {
    "family": "IPv4",
    "protocol": "UDP",
    "query-ip": "10.0.0.2",
    "query-port": 53060,
    "response-ip": "203.0.113.1",
    "response-port": 53
  },
  "dns": {
    "length": 264,
    "id": 62251,
    "opcode": 0,
    "rcode": "NOERROR",
    "qname": "ipv6.msftconnecttest.com.",
    "qtype": "AAAA",
    "qclass": "IN",
    "qdcount": 1,
    "ancount": 6,
    "nscount": 0,
    "arcount": 0,
    "malformed-packet": false,
    "flags": {
      "qr": true,
      "tc": false,
      "aa": false,
      "ra": true,
      "ad": false,
      "rd": true,
      "cd": false
    },
    "resource-records": {
      "an": [
        {
          "name": "ipv6.msftconnecttest.com.",
          "rdatatype": "CNAME",
          "class": "IN",
          "ttl": 3600,
          "rdata": "ipv6.msftconnecttest.com.edgesuite.net."
        },
        {
          "name": "ipv6.msftconnecttest.com.edgesuite.net.",
          "rdatatype": "CNAME",
          "class": "IN",
          "ttl": 4824,
          "rdata": "a1968.i6g1.akamai.net."
        },
        {
          "name": "a1968.i6g1.akamai.net.",
          "rdatatype": "CNAME",
          "class": "IN",
          "ttl": 20,
          "rdata": "a1968.i6g1.akamai.net.0.1.cn.akamaitech.net."
        },
        {
          "name": "a1968.i6g1.akamai.net.0.1.cn.akamaitech.net.",
          "rdatatype": "AAAA",
          "class": "IN",
          "ttl": 20,
          "rdata": "2403:e800:e80b::2a63:8cb3",
          "geoip": {
            "country": "HK",
            "asn": "AS4637"
          }
        },
        {
          "name": "a1968.i6g1.akamai.net.0.1.cn.akamaitech.net.",
          "rdatatype": "AAAA",
          "class": "IN",
          "ttl": 20,
          "rdata": "2403:e800:e80b::2a63:8cb2",
          "geoip": {
            "country": "HK",
            "asn": "AS4637"
          }
        },
        {
          "name": "a1968.i6g1.akamai.net.0.1.cn.akamaitech.net.",
          "rdatatype": "AAAA",
          "class": "IN",
          "ttl": 20,
          "rdata": "2403:e800:e80b::2a63:8caa",
          "geoip": {
            "country": "HK",
            "asn": "AS4637"
          }
        }
      ]
    }
  },
  "edns": {
    "version": 0,
    "udp-size": 0,
    "rcode": 0,
    "dnssec-ok": false,
    "client-subnet": ""
  },
  "dnstap": {
    "version": "dnsdist 2.0.6",
    "type": "MESSAGE",
    "identity": "dnsdist-01",
    "operation": "CLIENT_RESPONSE",
    "socket-ip": "10.0.0.1",
    "socket-port": 63496,
    "timestamp": "2026-07-02T20:17:48.906762961+07:00",
    "latency": 0.041836722,
    "latency-ms": 41,
    "peer-name": "",
    "query-zone": "",
    "extra": "",
    "policy-rule": "",
    "policy-type": "",
    "policy-match": "",
    "policy-value": "",
    "http-protocol": ""
  },
  "geoip": {
    "client-ip": "10.0.0.2",
    "client-country": "US",
    "client-city": "Seattle",
    "client-asn": "AS65000"
  },
  "threat": {
    "malicious": true,
    "category": "domain_blocklist",
    "sources": ["domain_blocklist"]
  },
  "anomaly": {
    "detected": true,
    "types": [
      "DNS_TUNNELING",
      "SUSPICIOUS_TLD"
    ],
    "score": 6,
    "entropy_score": 4.62
  }
}
```

## ClickHouse

### Tables

Only **2 tables** auto-created by dns-flow:
- `dns_raw` — 1 row per DNS event (includes real-time anomaly analysis columns)
- `dns_answers` — 1 row per resource record (normalized)

```sql
-- dns_raw: 1 row per DNS event
CREATE TABLE dns_flow.dns_raw (
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
    qr UInt8, tc UInt8, aa UInt8, ra UInt8,
    ad UInt8, rd UInt8, cd UInt8,
    qdcount UInt16, ancount UInt16,
    nscount UInt16, arcount UInt16,
    malformed UInt8,
    edns_version UInt8, edns_udp_size UInt16,
    edns_rcode UInt8, edns_dnssec_ok UInt8,
    edns_client_subnet String,
    edns_options String,
    client_country String, client_city String, client_asn String,
    latency Float64, latency_ms Int64,
    dnstap_identity String, dnstap_version String,
    dnstap_type String, dnstap_operation String,
    socket_ip String, socket_port UInt16,
    policy_rule String, policy_type String,
    policy_match String, policy_value String,
    http_protocol String,
    peer_name String, query_zone String,
    extra String,
    is_anomaly UInt8,
    anomaly_types Array(String),
    anomaly_score Float32,
    entropy_score Float32
) ENGINE = MergeTree
ORDER BY (timestamp, query_ip)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;   -- configurable via ttl_days

-- dns_answers: 1 row per resource record
CREATE TABLE dns_flow.dns_answers (
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
TTL toDateTime(timestamp) + INTERVAL 30 DAY;   -- configurable via ttl_days
```

New columns are added automatically via ALTER TABLE at startup (non-fatal).

### Auto-Created Materialized Views for Analytics

dns-flow automatically creates and manages Materialized Views for high-performance real-time analytics:

```sql
-- 1) MV: top domains per hour (Auto-created)
CREATE MATERIALIZED VIEW dns_flow.mv_top_domains_hourly
ENGINE = SummingMergeTree ORDER BY (hour, qname)
AS SELECT
    toStartOfHour(timestamp) AS hour,
    qname,
    count() AS queries,
    countIf(qr = 1) AS responses,
    countIf(rcode = 'NXDOMAIN') AS nxdomains,
    countIf(is_anomaly = 1) AS anomalies
FROM dns_flow.dns_raw
GROUP BY hour, qname;

-- 2) MV: dns anomalies per hour (Auto-created)
CREATE MATERIALIZED VIEW dns_flow.mv_dns_anomalies_hourly
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
FROM dns_flow.dns_raw
WHERE is_anomaly = 1
GROUP BY hour, query_ip, client_country;
```

Query from MVs directly:

```sql
SELECT qname, queries, anomalies
FROM dns_flow.mv_top_domains_hourly
WHERE hour >= now() - INTERVAL 24 HOUR
ORDER BY queries DESC
LIMIT 10;
```

## InfluxDB v1 / v2

### Measurement

```
Measurement: dns_query
Tags:   identity, operation, client_country, client_city, client_asn
Fields: qname, qtype, rcode, query_ip, response_ip,
        family, protocol, latency, latency_ms,
        opcode, length, dns_id,
        qr, tc, aa, rd, ra, ad, cd,
        qdcount, ancount, nscount, arcount,
        edns_version, edns_udp_size, edns_rcode, edns_dnssec_ok
```

Boolean fields use `t`/`f`.

## Fields Covered per Storage

| Data | ClickHouse | InfluxDB v1/v2 | File |
|---|---|---|---|
| query/response IP + port | ✅ dns_raw | ✅ fields | ✅ |
| qname, qtype, qclass, rcode | ✅ dns_raw | ✅ fields | ✅ |
| flags (qr,tc,aa,ra,ad,rd,cd) | ✅ dns_raw | ✅ fields | ✅ |
| counts (qdcount,ancount,nscount,arcount) | ✅ dns_raw | ✅ fields | ✅ |
| malformed | ✅ dns_raw | ✅ fields | ✅ |
| EDNS version, udp_size, rcode, dnssec_ok | ✅ dns_raw | ✅ fields | ✅ |
| EDNS client-subnet | ✅ dns_raw | ✅ fields | ✅ |
| EDNS options (JSON) | ✅ dns_raw (JSON) | ❌ | ✅ |
| latency, latency_ms | ✅ dns_raw | ✅ fields | ✅ |
| socket_ip, socket_port | ✅ dns_raw | ✅ fields | ✅ |
| dnstap_version, dnstap_type | ✅ dns_raw | ✅ tags | ✅ |
| dnstap_identity, dnstap_operation | ✅ dns_raw | ✅ tags | ✅ |
| policy fields + http_protocol + extra | ✅ dns_raw | ✅ (if non-empty) | ✅ |
| peer_name, query_zone | ✅ dns_raw | ✅ fields | ✅ |
| client GeoIP (country/city/asn) | ✅ dns_raw | ✅ tags | ✅ |
| per-RR answers (an/ns/ar) | ✅ dns_answers | ❌ | ✅ (resource-records) |
| per-RR GeoIP (A/AAAA) | ✅ dns_answers | ❌ | ✅ (resource-records[].geoip) |

Both CLIENT_QUERY (`qr=0`) and CLIENT_RESPONSE (`qr=1`) are stored in all outputs, distinguished by the `qr` field and `dnstap_operation`.

## Query Examples

### ClickHouse — Top 10 queried domains (last hour)

```sql
SELECT qname, count() AS hits
FROM dns_flow.dns_raw
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY qname
ORDER BY hits DESC
LIMIT 10;
```

### ClickHouse — NXDOMAIN rate by client country

```sql
SELECT client_country, count() AS total,
       countIf(rcode = 'NXDOMAIN') AS nx,
       round(nx / total * 100, 2) AS pct
FROM dns_flow.dns_raw
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY client_country
ORDER BY total DESC;
```

### ClickHouse — EDNS options breakdown

```sql
SELECT edns_options, count() AS cnt
FROM dns_flow.dns_raw
WHERE timestamp >= now() - INTERVAL 1 HOUR
  AND edns_options != ''
GROUP BY edns_options
ORDER BY cnt DESC;
```

### InfluxDB — Query latency p99

```sql
SELECT percentile(latency, 99) AS p99_latency
FROM dns_query
WHERE time > now() - 1h
GROUP BY time(1m);
```
