package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Mode       string           `yaml:"mode"`
	DNSTap     DNSTapConfig     `yaml:"dnstap"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Relay      RelayConfig      `yaml:"relay"`
	Outputs    OutputsConfig    `yaml:"outputs"`
	Pipeline   PipelineConfig   `yaml:"pipeline"`
	GeoIP      GeoIPConfig      `yaml:"geoip"`
	Monitoring MonitoringConfig `yaml:"monitoring"`
}

type ServerConfig struct {
	Name     string `yaml:"name"`
	LogLevel string `yaml:"log_level"`
}

type DNSTapConfig struct {
	Type        string            `yaml:"type"`
	Listen      string            `yaml:"listen"`
	UnixSocket  string            `yaml:"unix_socket"`
	Framestream FramestreamConfig `yaml:"framestream"`
	TLS         TLSConfig         `yaml:"tls"`
}

type RelayConfig struct {
	Input             RelayEndpoint `yaml:"input"`
	Output            RelayEndpoint `yaml:"output"`
	QueueSize         int           `yaml:"queue_size"`
	ReconnectInterval time.Duration `yaml:"reconnect_interval"`
}

type RelayEndpoint struct {
	Type    string `yaml:"type"`
	Address string `yaml:"address"`
}

type FramestreamConfig struct {
	BufferSize int           `yaml:"buffer_size"`
	Timeout    time.Duration `yaml:"timeout"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type KafkaConfig struct {
	Brokers  []string      `yaml:"brokers"`
	Topic    KafkaTopic    `yaml:"topic"`
	Producer KafkaProducer `yaml:"producer"`
	Consumer KafkaConsumer `yaml:"consumer"`
}

type KafkaTopic struct {
	Raw         string `yaml:"raw"`
	Enriched    string `yaml:"enriched"`
	Alert       string `yaml:"alert"`
	Metrics     string `yaml:"metrics"`
	RetentionMS string `yaml:"retention_ms"`
}

type KafkaProducer struct {
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	Compression   string        `yaml:"compression"`
}

type KafkaConsumer struct {
	GroupID       string `yaml:"group_id"`
	MaxFetchBytes int    `yaml:"max_fetch_bytes"`
}

type OutputsConfig struct {
	ClickHouse *ClickHouseOutputConfig `yaml:"clickhouse"`
	InfluxDB   *InfluxDBOutputConfig   `yaml:"influxdb"`
	InfluxDBV2 *InfluxDBV2OutputConfig `yaml:"influxdb_v2"`
	File       *FileOutputConfig       `yaml:"file"`
}

type ClickHouseOutputConfig struct {
	Hosts       []string `yaml:"hosts"`
	Database    string   `yaml:"database"`
	Username    string   `yaml:"username"`
	Password    string   `yaml:"password"`
	Compression bool     `yaml:"compression"`
	PoolSize    int      `yaml:"pool_size"`
	TTLDays     int      `yaml:"ttl_days"`
}

type InfluxDBOutputConfig struct {
	URL             string `yaml:"url"`
	Database        string `yaml:"database"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	RetentionPolicy string `yaml:"retention_policy"`
	RetentionDays   int    `yaml:"retention_days"`
	Measurement     string `yaml:"measurement"`
}

type InfluxDBV2OutputConfig struct {
	URL           string `yaml:"url"`
	Org           string `yaml:"org"`
	Bucket        string `yaml:"bucket"`
	Token         string `yaml:"token"`
	Precision     string `yaml:"precision"`
	Measurement   string `yaml:"measurement"`
	RetentionDays int    `yaml:"retention_days"`
}

type FileOutputConfig struct {
	Path       string `yaml:"path"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxAgeDays int    `yaml:"max_age_days"`
	MaxBackups int    `yaml:"max_backups"`
	Compress   bool   `yaml:"compress"`
}

type PipelineConfig struct {
	WorkerCount int              `yaml:"worker_count"`
	QueueSize   int              `yaml:"queue_size"`
	Enrichment  EnrichmentConfig `yaml:"enrichment"`
	MaxRetries  int              `yaml:"max_retries"`
	RetryDelay  time.Duration    `yaml:"retry_delay"`
}

type EnrichmentConfig struct {
	GeoIPEnabled       bool `yaml:"geoip_enabled"`
	ASNEnabled         bool `yaml:"asn_enabled"`
	ThreatIntelEnabled bool `yaml:"threat_intel_enabled"`
}

type GeoIPConfig struct {
	MaxmindDBPath string `yaml:"maxmind_db_path"`
	ASNDBPath     string `yaml:"asn_db_path"`
}

type MonitoringConfig struct {
	MetricsEnabled  bool   `yaml:"metrics_enabled"`
	PrometheusPort  int    `yaml:"prometheus_port"`
	MetricsPath     string `yaml:"metrics_path"`
	TracingEnabled  bool   `yaml:"tracing_enabled"`
	TracingEndpoint string `yaml:"tracing_endpoint"`
	PprofEnabled    bool   `yaml:"pprof_enabled"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read file %s: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse yaml: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	mode := c.Mode
	if mode == "" {
		mode = "collect"
	}

	if mode == "relay" {
		return c.validateRelay()
	}
	return c.validateCollect()
}

func (c *Config) validateRelay() error {
	if c.Relay.Input.Type == "" || c.Relay.Input.Address == "" {
		return fmt.Errorf("config: relay.input.type and relay.input.address are required")
	}
	if c.Relay.Input.Type != "tcp" && c.Relay.Input.Type != "unix" {
		return fmt.Errorf("config: relay.input.type must be tcp or unix")
	}
	if c.Relay.Output.Type == "" || c.Relay.Output.Address == "" {
		return fmt.Errorf("config: relay.output.type and relay.output.address are required")
	}
	if c.Relay.Output.Type != "tcp" && c.Relay.Output.Type != "unix" {
		return fmt.Errorf("config: relay.output.type must be tcp or unix")
	}
	if c.Relay.ReconnectInterval <= 0 {
		c.Relay.ReconnectInterval = 5 * time.Second
	}
	return nil
}

func (c *Config) validateCollect() error {
	if c.Server.Name == "" {
		return fmt.Errorf("config: server.name is required")
	}
	switch c.DNSTap.Type {
	case "unix":
		if c.DNSTap.UnixSocket == "" {
			return fmt.Errorf("config: dnstap.unix_socket is required when dnstap.type is unix")
		}
	case "", "tcp":
		if c.DNSTap.Listen == "" {
			return fmt.Errorf("config: dnstap.listen is required")
		}
	default:
		return fmt.Errorf("config: dnstap.type must be tcp or unix")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("config: kafka.brokers is required")
	}
	if c.Kafka.Topic.Raw == "" {
		return fmt.Errorf("config: kafka.topic.raw is required")
	}
	if c.Outputs.ClickHouse == nil && c.Outputs.InfluxDB == nil && c.Outputs.InfluxDBV2 == nil && c.Outputs.File == nil {
		return fmt.Errorf("config: at least one output (clickhouse/influxdb/influxdb_v2/file) required")
	}
	return nil
}
