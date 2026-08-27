package usecase

import (
	"log/slog"
	"os"
	"testing"

	"github.com/alifgufron/dns-flow/internal/domain"
)

func TestThreatIntelEngine(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	engine := NewThreatIntelEngine(
		nil,
		[]string{"bad-domain.com", "phishing.test"},
		[]string{"1.2.3.4", "10.99.0.0/16"},
		logger,
	)

	// Test bad domain
	evt1 := &domain.DNSRawEvent{
		DNS: domain.DNSInfo{QName: "sub.bad-domain.com."},
	}
	engine.Check(evt1)
	if !evt1.Threat.Malicious {
		t.Errorf("expected malicious=true for sub.bad-domain.com")
	}

	// Test bad IP
	evt2 := &domain.DNSRawEvent{
		Network: domain.NetworkInfo{QueryIP: "1.2.3.4"},
	}
	engine.Check(evt2)
	if !evt2.Threat.Malicious {
		t.Errorf("expected malicious=true for IP 1.2.3.4")
	}

	// Test bad CIDR
	evt3 := &domain.DNSRawEvent{
		Network: domain.NetworkInfo{QueryIP: "10.99.5.20"},
	}
	engine.Check(evt3)
	if !evt3.Threat.Malicious {
		t.Errorf("expected malicious=true for CIDR 10.99.5.20")
	}

	// Test clean domain & IP
	evt4 := &domain.DNSRawEvent{
		DNS:     domain.DNSInfo{QName: "google.com."},
		Network: domain.NetworkInfo{QueryIP: "8.8.8.8"},
	}
	engine.Check(evt4)
	if evt4.Threat.Malicious {
		t.Errorf("expected malicious=false for google.com / 8.8.8.8")
	}
}
