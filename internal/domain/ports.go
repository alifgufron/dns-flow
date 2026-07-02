package domain

// --- Inbound Ports ---

type DNSTapReceiver interface {
	Start() error
	Stop() error
}

// --- Outbound Ports ---

type EventProducer interface {
	Produce(event DNSRawEvent) error
	Close() error
}

type GeoIPResolver interface {
	Lookup(ip string) (*GeoIPInfo, error)
	LookupIP(ip string) (string, string, string)
}

type Storage interface {
	Name() string
	Write(event DNSRawEvent) error
	Migrate() error
	Close() error
}

// --- Usecase Ports ---

type Pipeline interface {
	Process(event DNSRawEvent) error
	Run() error
	Shutdown() error
}
