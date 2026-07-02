package domain

type DNSInfo struct {
	Length    int                `json:"length" yaml:"length"`
	ID        int                `json:"id" yaml:"id"`
	Opcode    int                `json:"opcode" yaml:"opcode"`
	RCode     string             `json:"rcode" yaml:"rcode"`
	QName     string             `json:"qname" yaml:"qname"`
	QType     string             `json:"qtype" yaml:"qtype"`
	QClass    string             `json:"qclass" yaml:"qclass"`
	Qdcount   int                `json:"qdcount" yaml:"qdcount"`
	Ancount   int                `json:"ancount" yaml:"ancount"`
	Nscount   int                `json:"nscount" yaml:"nscount"`
	Arcount   int                `json:"arcount" yaml:"arcount"`
	Malformed bool               `json:"malformed-packet" yaml:"malformed-packet"`
	Flags     Flags              `json:"flags" yaml:"flags"`
	Resource  map[string][]DNSRR `json:"resource-records" yaml:"resource-records"`
}

type Flags struct {
	QR bool `json:"qr" yaml:"qr"`
	TC bool `json:"tc" yaml:"tc"`
	AA bool `json:"aa" yaml:"aa"`
	RA bool `json:"ra" yaml:"ra"`
	AD bool `json:"ad" yaml:"ad"`
	RD bool `json:"rd" yaml:"rd"`
	CD bool `json:"cd" yaml:"cd"`
}

type RRGeoInfo struct {
	Country string `json:"country,omitempty" yaml:"country,omitempty"`
	City    string `json:"city,omitempty" yaml:"city,omitempty"`
	ASN     string `json:"asn,omitempty" yaml:"asn,omitempty"`
}

type DNSRR struct {
	Name      string     `json:"name" yaml:"name"`
	RDataType string     `json:"rdatatype" yaml:"rdatatype"`
	Class     string     `json:"class" yaml:"class"`
	TTL       int        `json:"ttl" yaml:"ttl"`
	RData     string     `json:"rdata" yaml:"rdata"`
	Geo       *RRGeoInfo `json:"geoip,omitempty" yaml:"geoip,omitempty"`
}
