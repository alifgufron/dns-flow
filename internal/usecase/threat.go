package usecase

import (
	"bufio"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type ThreatIntelEngine struct {
	mu           sync.RWMutex
	blockedDomains map[string]string // domain -> category
	blockedIPs     map[string]string // IP -> category
	blockedCIDRs   []*net.IPNet
	cidrCategories []string
	logger         *slog.Logger
}

func NewThreatIntelEngine(blocklistPaths []string, customDomains []string, customIPs []string, logger *slog.Logger) *ThreatIntelEngine {
	e := &ThreatIntelEngine{
		blockedDomains: make(map[string]string),
		blockedIPs:     make(map[string]string),
		logger:         logger,
	}

	for _, d := range customDomains {
		clean := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(d), "."))
		if clean != "" {
			e.blockedDomains[clean] = "custom_blocklist"
		}
	}

	for _, ipStr := range customIPs {
		cleanStr := strings.TrimSpace(ipStr)
		if cleanStr == "" {
			continue
		}
		if _, cidr, err := net.ParseCIDR(cleanStr); err == nil {
			e.blockedCIDRs = append(e.blockedCIDRs, cidr)
			e.cidrCategories = append(e.cidrCategories, "custom_cidr")
		} else if parsedIP := net.ParseIP(cleanStr); parsedIP != nil {
			e.blockedIPs[cleanStr] = "custom_ip_blocklist"
		}
	}

	for _, path := range blocklistPaths {
		e.loadBlocklistFile(path)
	}

	logger.Info("threat_intel: engine initialized",
		"domains_count", len(e.blockedDomains),
		"ips_count", len(e.blockedIPs),
		"cidrs_count", len(e.blockedCIDRs),
	)

	return e
}

func (e *ThreatIntelEngine) loadBlocklistFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		e.logger.Warn("threat_intel: failed to open blocklist file", "path", path, "error", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		domainOrIP := ""
		if len(parts) >= 2 && (parts[0] == "127.0.0.1" || parts[0] == "0.0.0.0") {
			domainOrIP = parts[1]
		} else if len(parts) >= 1 {
			domainOrIP = parts[0]
		}

		domainOrIP = strings.ToLower(strings.TrimSuffix(domainOrIP, "."))
		if domainOrIP != "" {
			e.blockedDomains[domainOrIP] = "blocklist_feed"
			count++
		}
	}
	e.logger.Info("threat_intel: loaded blocklist file", "path", path, "entries", count)
}

func (e *ThreatIntelEngine) Check(event *domain.DNSRawEvent) {
	if event == nil {
		return
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	qname := strings.ToLower(strings.TrimSuffix(event.DNS.QName, "."))
	if cat, ok := e.blockedDomains[qname]; ok {
		event.Threat.Malicious = true
		event.Threat.Category = cat
		event.Threat.Sources = append(event.Threat.Sources, "domain_blocklist")
		return
	}

	// Suffix parent domain check
	parts := strings.Split(qname, ".")
	if len(parts) > 2 {
		parent := strings.Join(parts[len(parts)-2:], ".")
		if cat, ok := e.blockedDomains[parent]; ok {
			event.Threat.Malicious = true
			event.Threat.Category = cat
			event.Threat.Sources = append(event.Threat.Sources, "domain_suffix_blocklist")
			return
		}
	}

	clientIP := event.Network.QueryIP
	if clientIP != "" {
		if cat, ok := e.blockedIPs[clientIP]; ok {
			event.Threat.Malicious = true
			event.Threat.Category = cat
			event.Threat.Sources = append(event.Threat.Sources, "ip_blocklist")
			return
		}

		parsedIP := net.ParseIP(clientIP)
		if parsedIP != nil {
			for i, cidr := range e.blockedCIDRs {
				if cidr.Contains(parsedIP) {
					event.Threat.Malicious = true
					event.Threat.Category = e.cidrCategories[i]
					event.Threat.Sources = append(event.Threat.Sources, "ip_cidr_blocklist")
					return
				}
			}
		}
	}
}
