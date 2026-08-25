package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type MetricsExporter struct {
	server           *http.Server
	logger           *slog.Logger
	port             int
	path             string
	queriesTotal     *prometheus.CounterVec
	responsesTotal   *prometheus.CounterVec
	anomaliesTotal   *prometheus.CounterVec
	latencyHistogram *prometheus.HistogramVec
	droppedEvents    prometheus.Counter
	once             sync.Once
}

var (
	globalExporter *MetricsExporter
	exporterMu     sync.Mutex
)

func InitMetrics(port int, path string, logger *slog.Logger) *MetricsExporter {
	exporterMu.Lock()
	defer exporterMu.Unlock()

	if globalExporter != nil {
		return globalExporter
	}

	if port <= 0 {
		port = 9153
	}
	if path == "" {
		path = "/metrics"
	}

	m := &MetricsExporter{
		logger: logger,
		port:   port,
		path:   path,
		queriesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dnsflow_queries_total",
				Help: "Total number of DNS query packets processed by dns-flow",
			},
			[]string{"qtype", "family", "protocol"},
		),
		responsesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dnsflow_responses_total",
				Help: "Total number of DNS response packets processed by dns-flow",
			},
			[]string{"rcode", "qtype"},
		),
		anomaliesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dnsflow_anomalies_total",
				Help: "Total number of DNS anomalies detected by type",
			},
			[]string{"anomaly_type"},
		),
		latencyHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "dnsflow_latency_seconds",
				Help:    "DNS query-response latency in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
			},
			[]string{"qtype"},
		),
		droppedEvents: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dnsflow_dropped_events_total",
				Help: "Total number of DNS events dropped due to full pipeline queue",
			},
		),
	}

	prometheus.MustRegister(m.queriesTotal)
	prometheus.MustRegister(m.responsesTotal)
	prometheus.MustRegister(m.anomaliesTotal)
	prometheus.MustRegister(m.latencyHistogram)
	prometheus.MustRegister(m.droppedEvents)

	globalExporter = m
	return m
}

func GetExporter() *MetricsExporter {
	exporterMu.Lock()
	defer exporterMu.Unlock()
	return globalExporter
}

func (m *MetricsExporter) Start() {
	if m == nil {
		return
	}
	mux := http.NewServeMux()
	mux.Handle(m.path, promhttp.Handler())

	m.server = &http.Server{
		Addr:    ":" + strconv.Itoa(m.port),
		Handler: mux,
	}

	go func() {
		m.logger.Info("prometheus metrics exporter listening", "port", m.port, "path", m.path)
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error("prometheus metrics server failed", "error", err)
		}
	}()
}

func (m *MetricsExporter) Stop() {
	if m == nil || m.server == nil {
		return
	}
	m.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.server.Shutdown(ctx); err != nil {
			m.logger.Warn("prometheus metrics server shutdown error", "error", err)
		}
		m.logger.Info("prometheus metrics server stopped")
	})
}

func (m *MetricsExporter) RecordEvent(event *domain.DNSRawEvent) {
	if m == nil || event == nil {
		return
	}

	if !event.DNS.Flags.QR {
		m.queriesTotal.WithLabelValues(event.DNS.QType, event.Network.Family, event.Network.Protocol).Inc()
	} else {
		m.responsesTotal.WithLabelValues(event.DNS.RCode, event.DNS.QType).Inc()
		if event.DNSTap.Latency > 0 {
			m.latencyHistogram.WithLabelValues(event.DNS.QType).Observe(event.DNSTap.Latency)
		}
	}

	if event.Anomaly.Detected {
		for _, at := range event.Anomaly.Types {
			m.anomaliesTotal.WithLabelValues(at).Inc()
		}
	}
}

func (m *MetricsExporter) RecordDropped() {
	if m == nil {
		return
	}
	m.droppedEvents.Inc()
}
