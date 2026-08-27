# Grafana Dashboards & Prometheus Integration Guide

`dns-flow` provides pre-built Grafana dashboards for ClickHouse, InfluxDB, and native Prometheus metrics.

## Available Dashboards

| File | Datasource | Key Features |
|---|---|---|
| [`dashboards/grafana-clickhouse.json`](../dashboards/grafana-clickhouse.json) | ClickHouse | Real-time QPS, Latency, Top 10 Domains (Hourly MV), Top 10 Clients (Hourly MV), DNS Abuse & Threat Summary (Abuse MV), QType Breakdown, Anomaly Summary Table |
| [`dashboards/grafana-influxdb.json`](../dashboards/grafana-influxdb.json) | InfluxDB (v1/v2) | Time-series QPS, Average Latency, Threat Intelligence Flags, Anomaly Spikes by Type |

## Importing Dashboards to Grafana

1. Open your **Grafana UI** in a browser (`http://localhost:3000`).
2. Navigate to **Dashboards** $\rightarrow$ **New** $\rightarrow$ **Import**.
3. Click **Upload dashboard JSON file** and select either:
   - `dashboards/grafana-clickhouse.json`
   - `dashboards/grafana-influxdb.json`
4. Select your configured ClickHouse or InfluxDB data source.
5. Click **Import**.

## Prometheus Metrics Scraping

`dns-flow` exposes native Prometheus metrics at `http://localhost:9153/metrics` when enabled in `config.yaml`:

```yaml
monitoring:
  metrics_enabled: true
  prometheus_port: 9153
  metrics_path: "/metrics"
```

Add the following job to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'dns_flow'
    static_configs:
      - targets: ['localhost:9153']
```

### Metrics List

- `dnsflow_queries_total` — Total count of DNS queries by `qtype`, `family`, `protocol`
- `dnsflow_responses_total` — Total count of DNS responses by `rcode`, `qtype`
- `dnsflow_anomalies_total` — Total count of detected DNS anomalies by `anomaly_type`
- `dnsflow_latency_seconds` — Histogram of query-response latency
- `dnsflow_dropped_events_total` — Count of dropped events due to full pipeline queue
