package domain

type DNSRawEvent struct {
	Network NetworkInfo `json:"network" yaml:"network"`
	DNS     DNSInfo     `json:"dns" yaml:"dns"`
	EDNS    EDNSInfo    `json:"edns" yaml:"edns"`
	DNSTap  DNSTapInfo  `json:"dnstap" yaml:"dnstap"`
	GeoIP   GeoIPInfo   `json:"geoip,omitempty" yaml:"geoip,omitempty"`
	Anomaly AnomalyInfo `json:"anomaly,omitempty" yaml:"anomaly,omitempty"`
}

type AnomalyInfo struct {
	Detected     bool     `json:"detected" yaml:"detected"`
	Types        []string `json:"types,omitempty" yaml:"types,omitempty"`
	Score        float64  `json:"score" yaml:"score"`
	EntropyScore float64  `json:"entropy_score,omitempty" yaml:"entropy_score,omitempty"`
}

type EDNSOption struct {
	Code  int    `json:"code" yaml:"code"`
	Name  string `json:"name" yaml:"name"`
	Value string `json:"value" yaml:"value"`
}

type EDNSInfo struct {
	Version      int           `json:"version" yaml:"version"`
	UDPSize      int           `json:"udp-size" yaml:"udp-size"`
	RCode        int           `json:"rcode" yaml:"rcode"`
	DNSSECOK     bool          `json:"dnssec-ok" yaml:"dnssec-ok"`
	ClientSubnet string        `json:"client-subnet" yaml:"client-subnet"`
	Options      []EDNSOption  `json:"options,omitempty" yaml:"options,omitempty"`
}

type EnrichedEvent struct {
	DNSRawEvent
	GeoIP   GeoIPInfo   `json:"geoip" yaml:"geoip"`
	Threat  ThreatInfo  `json:"threat,omitempty" yaml:"threat,omitempty"`
	Policy  PolicyInfo  `json:"policy,omitempty" yaml:"policy,omitempty"`
}

type GeoIPInfo struct {
	ClientIP      string `json:"client-ip" yaml:"client-ip"`
	ClientCountry string `json:"client-country" yaml:"client-country"`
	ClientCity    string `json:"client-city" yaml:"client-city"`
	ClientASN     string `json:"client-asn" yaml:"client-asn"`
}

type ThreatInfo struct {
	Malicious bool     `json:"malicious" yaml:"malicious"`
	Category  string   `json:"category" yaml:"category"`
	Sources   []string `json:"sources" yaml:"sources"`
}

type PolicyInfo struct {
	Action      string `json:"action" yaml:"action"`
	PolicyName  string `json:"policy-name" yaml:"policy-name"`
	Category    string `json:"category" yaml:"category"`
}
