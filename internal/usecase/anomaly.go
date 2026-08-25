package usecase

import (
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/alifgufron/dns-flow/internal/domain"
)

var suspiciousTLDs = map[string]bool{
	"zip":   true,
	"mov":   true,
	"top":   true,
	"tk":    true,
	"ml":    true,
	"ga":    true,
	"cf":    true,
	"gq":    true,
	"work":  true,
	"click": true,
	"cfd":   true,
	"fit":   true,
}

type clientWindow struct {
	nxdomainCount int
	queryCount    int
	lastReset     time.Time
}

type AnomalyDetector struct {
	mu          sync.Mutex
	clientStats map[string]*clientWindow
}

func NewAnomalyDetector() *AnomalyDetector {
	ad := &AnomalyDetector{
		clientStats: make(map[string]*clientWindow),
	}
	go ad.cleanupLoop()
	return ad
}

func (ad *AnomalyDetector) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		ad.mu.Lock()
		now := time.Now()
		for ip, window := range ad.clientStats {
			if now.Sub(window.lastReset) > 1*time.Minute {
				delete(ad.clientStats, ip)
			}
		}
		ad.mu.Unlock()
	}
}

func (ad *AnomalyDetector) Detect(event *domain.DNSRawEvent) {
	if event == nil {
		return
	}

	var types []string
	var totalScore float64

	qname := strings.TrimSuffix(event.DNS.QName, ".")
	labels := strings.Split(qname, ".")

	// 1. DNS Tunneling & High Entropy Detection
	entropy := calculateEntropy(qname)
	if len(qname) > 40 && entropy > 4.1 {
		types = append(types, "DNS_TUNNELING")
		totalScore += 4.0
	} else if entropy > 4.5 {
		types = append(types, "DNS_TUNNELING")
		totalScore += 3.0
	}

	// 2. DGA Domains Detection
	if len(labels) > 0 {
		subdomain := labels[0]
		if len(subdomain) > 12 && isDGAPattern(subdomain) {
			types = append(types, "DGA_DOMAINS")
			totalScore += 3.5
		}
	}

	// 3. Suspicious TLD Detection
	if len(labels) > 1 {
		tld := strings.ToLower(labels[len(labels)-1])
		if suspiciousTLDs[tld] {
			types = append(types, "SUSPICIOUS_TLD")
			totalScore += 2.0
		}
	}

	// 4. DNS Amplification Risk (ANY / TXT / Large Response)
	if event.DNS.QType == "ANY" || (event.DNS.Length > 1024 && (event.DNS.QType == "TXT" || event.DNS.QType == "ANY")) {
		types = append(types, "DNS_AMPLIFICATION_RISK")
		totalScore += 2.5
	}

	// 5. DNS Rebinding Detection (Private IP returned for Public Domain)
	if event.DNS.Flags.QR && isPublicDomain(qname) {
		for _, rrs := range event.DNS.Resource {
			for _, rr := range rrs {
				if (rr.RDataType == "A" || rr.RDataType == "AAAA") && isPrivateIP(rr.RData) {
					types = append(types, "REBINDING_ATTACK_RISK")
					totalScore += 4.5
					break
				}
			}
		}
	}

	// 6. Sliding Window Client Stats (NXDOMAIN Flood & High Query Rate)
	clientIP := event.Network.QueryIP
	if clientIP != "" {
		ad.mu.Lock()
		w, ok := ad.clientStats[clientIP]
		now := time.Now()
		if !ok || now.Sub(w.lastReset) > 10*time.Second {
			w = &clientWindow{lastReset: now}
			ad.clientStats[clientIP] = w
		}
		w.queryCount++
		if event.DNS.Flags.QR && event.DNS.RCode == "NXDOMAIN" {
			w.nxdomainCount++
		}
		nxCount := w.nxdomainCount
		qCount := w.queryCount
		ad.mu.Unlock()

		if nxCount > 20 {
			types = append(types, "NXDOMAIN_FLOOD")
			totalScore += 3.0
		}

		if qCount > 200 {
			types = append(types, "HIGH_QUERY_RATE_FLOOD")
			totalScore += 2.5
		}
	}

	if len(types) > 0 {
		event.Anomaly.Detected = true
		event.Anomaly.Types = types
		event.Anomaly.Score = math.Min(totalScore, 10.0)
		event.Anomaly.EntropyScore = math.Round(entropy*100) / 100
	}
}

func calculateEntropy(s string) float64 {
	if len(s) == 0 {
		return 0
	}
	counts := make(map[rune]float64)
	for _, char := range s {
		counts[char]++
	}
	var entropy float64
	length := float64(len(s))
	for _, count := range counts {
		p := count / length
		entropy -= p * math.Log2(p)
	}
	return entropy
}

func isDGAPattern(s string) bool {
	var digits, consonants, vowels int
	vowelMap := map[rune]bool{'a': true, 'e': true, 'i': true, 'o': true, 'u': true}
	for _, char := range strings.ToLower(s) {
		if char >= '0' && char <= '9' {
			digits++
		} else if char >= 'a' && char <= 'z' {
			if vowelMap[char] {
				vowels++
			} else {
				consonants++
			}
		}
	}
	if vowels == 0 && len(s) > 8 {
		return true
	}
	if float64(consonants)/float64(len(s)) > 0.75 {
		return true
	}
	return false
}

func isPublicDomain(qname string) bool {
	if qname == "" || strings.HasSuffix(qname, ".local") || strings.HasSuffix(qname, ".internal") || strings.HasSuffix(qname, ".lan") {
		return false
	}
	return true
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	return false
}
