package domain

import "time"

type DNSTapInfo struct {
	Version     string    `json:"version" yaml:"version"`
	Type        string    `json:"type" yaml:"type"`
	Identity    string    `json:"identity" yaml:"identity"`
	Operation   string    `json:"operation" yaml:"operation"`
	SocketIP    string    `json:"socket-ip" yaml:"socket-ip"`
	SocketPort  int       `json:"socket-port" yaml:"socket-port"`
	Timestamp   time.Time `json:"timestamp" yaml:"timestamp"`
	Latency     float64   `json:"latency" yaml:"latency"`
	LatencyMs   int64     `json:"latency-ms" yaml:"latency-ms"`
	PeerName    string    `json:"peer-name" yaml:"peer-name"`
	QueryZone   string    `json:"query-zone" yaml:"query-zone"`
	Extra       string    `json:"extra" yaml:"extra"`
	PolicyRule  string    `json:"policy-rule" yaml:"policy-rule"`
	PolicyType  string    `json:"policy-type" yaml:"policy-type"`
	PolicyMatch string    `json:"policy-match" yaml:"policy-match"`
	PolicyValue string    `json:"policy-value" yaml:"policy-value"`
	HTTPProtocol string   `json:"http-protocol" yaml:"http-protocol"`
}
