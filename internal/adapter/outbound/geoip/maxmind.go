package geoip

import (
	"container/list"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/oschwald/maxminddb-golang"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type geoCachedResult struct {
	country string
	city    string
	asn     string
}

type lruEntry struct {
	key   string
	value geoCachedResult
}

type lruCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]*list.Element
	evictList *list.List
}

func newLRUCache(capacity int) *lruCache {
	if capacity <= 0 {
		capacity = 10000
	}
	return &lruCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		evictList: list.New(),
	}
}

func (c *lruCache) Get(key string) (geoCachedResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		return elem.Value.(*lruEntry).value, true
	}
	return geoCachedResult{}, false
}

func (c *lruCache) Add(key string, value geoCachedResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		elem.Value.(*lruEntry).value = value
		return
	}

	elem := c.evictList.PushFront(&lruEntry{key: key, value: value})
	c.items[key] = elem

	if c.evictList.Len() > c.capacity {
		oldest := c.evictList.Back()
		if oldest != nil {
			c.evictList.Remove(oldest)
			delete(c.items, oldest.Value.(*lruEntry).key)
		}
	}
}

type MaxMindResolver struct {
	cityDB *maxminddb.Reader
	asnDB  *maxminddb.Reader
	cache  *lruCache
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

func NewMaxMindResolver(cityDBPath, asnDBPath string, cacheSize int, logger *slog.Logger) (*MaxMindResolver, error) {
	r := &MaxMindResolver{
		cache:  newLRUCache(cacheSize),
		logger: logger,
	}

	if cityDBPath == "" {
		return r, nil
	}

	var err error
	r.cityDB, err = maxminddb.Open(cityDBPath)
	if err != nil {
		return nil, fmt.Errorf("geoip: failed to open city db %s: %w", cityDBPath, err)
	}
	logger.Info("geoip: city database loaded", "path", cityDBPath, "cache_capacity", r.cache.capacity)

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
	if m.cache != nil {
		if res, ok := m.cache.Get(ip); ok {
			return &domain.GeoIPInfo{
				ClientIP:      ip,
				ClientCountry: res.country,
				ClientCity:    res.city,
				ClientASN:     res.asn,
			}, nil
		}
	}

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

	if m.cache != nil {
		m.cache.Add(ip, geoCachedResult{
			country: info.ClientCountry,
			city:    info.ClientCity,
			asn:     info.ClientASN,
		})
	}

	return info, nil
}

func (m *MaxMindResolver) LookupIP(ip string) (string, string, string) {
	if m.cache != nil {
		if res, ok := m.cache.Get(ip); ok {
			return res.country, res.city, res.asn
		}
	}

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

	if m.cache != nil {
		m.cache.Add(ip, geoCachedResult{
			country: country,
			city:    city,
			asn:     asn,
		})
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
