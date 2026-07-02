package usecase

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type queryEntry struct {
	QName     string
	QType     string
	ClientIP  string
	DNSID     int
	StoredAt  time.Time
}

type QueryCorrelator struct {
	entries map[string]*queryEntry
	mu      sync.Mutex
	ttl     time.Duration
	logger  *slog.Logger
	done    chan struct{}
	wg      sync.WaitGroup
}

func NewQueryCorrelator(logger *slog.Logger) *QueryCorrelator {
	return &QueryCorrelator{
		entries: make(map[string]*queryEntry),
		ttl:     30 * time.Second,
		logger:  logger,
		done:    make(chan struct{}),
	}
}

func (qc *QueryCorrelator) Start() {
	qc.wg.Add(1)
	go qc.evictLoop()
}

func (qc *QueryCorrelator) Stop() {
	close(qc.done)
	qc.wg.Wait()
}

func (qc *QueryCorrelator) evictLoop() {
	defer qc.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-qc.done:
			return
		case <-ticker.C:
			qc.evict()
		}
	}
}

func (qc *QueryCorrelator) evict() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	now := time.Now()
	for key, entry := range qc.entries {
		if now.Sub(entry.StoredAt) > qc.ttl {
			qc.logger.Warn("unanswered query evicted",
				"client_ip", entry.ClientIP,
				"dns_id", entry.DNSID,
				"qname", entry.QName,
				"qtype", entry.QType,
				"age_ms", now.Sub(entry.StoredAt).Milliseconds(),
			)
			delete(qc.entries, key)
		}
	}
}

func (qc *QueryCorrelator) Store(event domain.DNSRawEvent) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := fmt.Sprintf("%s:%d", event.Network.QueryIP, event.DNS.ID)
	qc.entries[key] = &queryEntry{
		QName:    event.DNS.QName,
		QType:    event.DNS.QType,
		ClientIP: event.Network.QueryIP,
		DNSID:    event.DNS.ID,
		StoredAt: time.Now(),
	}
}

func (qc *QueryCorrelator) Lookup(event domain.DNSRawEvent) bool {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := fmt.Sprintf("%s:%d", event.Network.QueryIP, event.DNS.ID)
	entry, ok := qc.entries[key]
	if !ok {
		return false
	}
	delete(qc.entries, key)

	if entry.QName != event.DNS.QName || entry.QType != event.DNS.QType {
		qc.logger.Warn("query mismatch on correlation",
			"client_ip", event.Network.QueryIP,
			"dns_id", event.DNS.ID,
			"stored_qname", entry.QName,
			"response_qname", event.DNS.QName,
			"stored_qtype", entry.QType,
			"response_qtype", event.DNS.QType,
		)
	}
	return true
}
