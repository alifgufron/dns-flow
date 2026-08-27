package geoip

import (
	"testing"
)

func TestLRUCache(t *testing.T) {
	cache := newLRUCache(2)

	cache.Add("1.1.1.1", geoCachedResult{country: "US", city: "LA", asn: "AS13335"})
	cache.Add("8.8.8.8", geoCachedResult{country: "US", city: "MV", asn: "AS15169"})

	res, ok := cache.Get("1.1.1.1")
	if !ok || res.country != "US" {
		t.Errorf("expected 1.1.1.1 in cache")
	}

	// Add 3rd item to trigger eviction
	cache.Add("9.9.9.9", geoCachedResult{country: "US", city: "Berkeley", asn: "AS19281"})

	// 8.8.8.8 should have been evicted because 1.1.1.1 was recently accessed
	_, ok = cache.Get("8.8.8.8")
	if ok {
		t.Errorf("expected 8.8.8.8 to be evicted from LRU cache")
	}
}
