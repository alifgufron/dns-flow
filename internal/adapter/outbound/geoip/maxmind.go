package geoip

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/oschwald/maxminddb-golang"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type MaxMindResolver struct {
	cityDB *maxminddb.Reader
	asnDB  *maxminddb.Reader
	logger *slog.Logger
}

type geoCityRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Location struct {
		Latitude  float64 `maxminddb:"latitude"`
		Longitude float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
}

type geoASNRecord struct {
	AutonomousSystemNumber       int    `maxminddb:"autonomous_system_number"`
	AutonomousSystemOrganization string `maxminddb:"autonomous_system_organization"`
}

func NewMaxMindResolver(cityDBPath, asnDBPath string, logger *slog.Logger) (*MaxMindResolver, error) {
	if cityDBPath == "" {
		return &MaxMindResolver{logger: logger}, nil
	}

	r := &MaxMindResolver{logger: logger}

	var err error
	r.cityDB, err = maxminddb.Open(cityDBPath)
	if err != nil {
		return nil, fmt.Errorf("geoip: failed to open city db %s: %w", cityDBPath, err)
	}
	logger.Info("geoip: city database loaded", "path", cityDBPath)

	if asnDBPath != "" {
		r.asnDB, err = maxminddb.Open(asnDBPath)
		if err != nil {
			return nil, fmt.Errorf("geoip: failed to open asn db %s: %w", asnDBPath, err)
		}
		logger.Info("geoip: asn database loaded", "path", asnDBPath)
	}

	return r, nil
}

func (m *MaxMindResolver) Lookup(ip string) (*domain.GeoIPInfo, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("geoip: invalid ip: %s", ip)
	}

	info := &domain.GeoIPInfo{
		ClientIP: ip,
	}

	if m.cityDB != nil {
		var rec geoCityRecord
		if err := m.cityDB.Lookup(parsed, &rec); err == nil {
			info.ClientCountry = rec.Country.ISOCode
			if name, ok := rec.City.Names["en"]; ok {
				info.ClientCity = name
			}
		}
	}

	if m.asnDB != nil {
		var rec geoASNRecord
		if err := m.asnDB.Lookup(parsed, &rec); err == nil {
			info.ClientASN = fmt.Sprintf("AS%d", rec.AutonomousSystemNumber)
		}
	}

	return info, nil
}

func (m *MaxMindResolver) LookupIP(ip string) (string, string, string) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return "", "", ""
	}

	country := ""
	city := ""
	asn := ""

	if m.cityDB != nil {
		var rec geoCityRecord
		if err := m.cityDB.Lookup(parsed, &rec); err == nil {
			country = rec.Country.ISOCode
			if name, ok := rec.City.Names["en"]; ok {
				city = name
			}
		}
	}

	if m.asnDB != nil {
		var rec geoASNRecord
		if err := m.asnDB.Lookup(parsed, &rec); err == nil {
			asn = fmt.Sprintf("AS%d", rec.AutonomousSystemNumber)
		}
	}

	return country, city, asn
}

func (m *MaxMindResolver) Health() map[string]string {
	return map[string]string{
		"geoip": "loaded",
	}
}

func (m *MaxMindResolver) Close() {
	if m.cityDB != nil {
		m.cityDB.Close()
	}
	if m.asnDB != nil {
		m.asnDB.Close()
	}
}
