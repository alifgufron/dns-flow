package domain

type NetworkInfo struct {
	Family        string `json:"family" yaml:"family"`
	Protocol      string `json:"protocol" yaml:"protocol"`
	QueryIP       string `json:"query-ip" yaml:"query-ip"`
	QueryPort     int    `json:"query-port" yaml:"query-port"`
	ResponseIP    string `json:"response-ip" yaml:"response-ip"`
	ResponsePort  int    `json:"response-port" yaml:"response-port"`
}
