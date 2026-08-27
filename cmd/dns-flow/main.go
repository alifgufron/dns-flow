package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alifgufron/dns-flow/internal/adapter/inbound/dnstap"
	kafkain "github.com/alifgufron/dns-flow/internal/adapter/inbound/kafka"
	clickhouseout "github.com/alifgufron/dns-flow/internal/adapter/outbound/clickhouse"
	"github.com/alifgufron/dns-flow/internal/adapter/outbound/file"
	"github.com/alifgufron/dns-flow/internal/adapter/outbound/geoip"
	influxdbout "github.com/alifgufron/dns-flow/internal/adapter/outbound/influxdb"
	influxdbv2 "github.com/alifgufron/dns-flow/internal/adapter/outbound/influxdb_v2"
	kafkaout "github.com/alifgufron/dns-flow/internal/adapter/outbound/kafka"
	"github.com/alifgufron/dns-flow/internal/domain"
	"github.com/alifgufron/dns-flow/internal/infrastructure/config"
	"github.com/alifgufron/dns-flow/internal/infrastructure/logger"
	"github.com/alifgufron/dns-flow/internal/infrastructure/metrics"
	"github.com/alifgufron/dns-flow/internal/relay"
	"github.com/alifgufron/dns-flow/internal/usecase"
)

func main() {
	cfgPath := flag.String("config", "", "path to configuration file")
	configTest := flag.Bool("config-test", false, "validate configuration file and exit")

	// Override default flag.Usage to show a friendly help message.
	flag.Usage = printUsage
	flag.Parse()

	// Show usage if no arguments are given at all.
	if flag.NFlag() == 0 && flag.NArg() == 0 {
		printUsage()
		os.Exit(1)
	}

	cfgPath = resolveConfig(*cfgPath)
	if cfgPath == nil {
		fmt.Fprintf(os.Stderr, "error: -config path is required\n\n")
		printUsage()
		os.Exit(1)
	}

	if *configTest {
		runConfigTest(*cfgPath)
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		os.Exit(1)
	}

	log := logger.Init(cfg.Server.LogLevel, cfg.Server.Name)

	mode := cfg.Mode
	if mode == "" {
		mode = "collect"
	}

	switch mode {
	case "relay":
		runRelay(cfg, log)
	default:
		runCollect(cfg, *cfgPath, log)
	}
}

// printUsage prints the help/usage message to stderr.
func printUsage() {
	fmt.Fprintf(os.Stderr, "dns-flow — DNS telemetry pipeline (DNSTAP → Kafka → Storage)\n\n")
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  dns-flow -config <config.yaml> [flags]\n\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	fmt.Fprintf(os.Stderr, "  -config <path>    Path to configuration file (required)\n")
	fmt.Fprintf(os.Stderr, "  -config-test      Validate config file and exit without starting service\n\n")
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  dns-flow -config /usr/local/etc/dns-flow.yaml\n")
	fmt.Fprintf(os.Stderr, "  dns-flow -config /usr/local/etc/dns-flow.yaml -config-test\n\n")
	fmt.Fprintf(os.Stderr, "Config is auto-discovered from: ./config.yaml, ./configs/config.yaml,\n")
	fmt.Fprintf(os.Stderr, "  /usr/local/etc/dns-flow.yaml, /etc/dns-flow.yaml\n")
}

// runConfigTest validates the config file and prints a summary, then exits.
// Exit code 0 = config is valid. Exit code 1 = config is invalid.
func runConfigTest(cfgPath string) {
	fmt.Printf("Validating config: %s\n", cfgPath)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[FAIL] YAML parse error: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "[FAIL] Config validation error: %v\n", err)
		os.Exit(1)
	}

	// Print config summary
	mode := cfg.Mode
	if mode == "" {
		mode = "collect"
	}
	fmt.Printf("[OK]   Config loaded successfully\n")
	fmt.Printf("       mode       : %s\n", mode)
	fmt.Printf("       server     : %s\n", cfg.Server.Name)
	fmt.Printf("       log_level  : %s\n", cfg.Server.LogLevel)

	switch mode {
	case "relay":
		fmt.Printf("       relay.input: %s (%s)\n", cfg.Relay.Input.Address, cfg.Relay.Input.Type)
		fmt.Printf("       relay.output: %s (%s)\n", cfg.Relay.Output.Address, cfg.Relay.Output.Type)
	default:
		fmt.Printf("       dnstap     : %s (%s)\n", cfg.DNSTap.Listen, cfg.DNSTap.Type)
		fmt.Printf("       kafka      : %v\n", cfg.Kafka.Brokers)
		fmt.Printf("       outputs    : %s\n", outputSummary(cfg))
	}

	fmt.Printf("[OK]   Config test passed — configuration is valid\n")
}

func runRelay(cfg *config.Config, log *slog.Logger) {
	r := relay.New(relay.Config{
		Input: relay.Endpoint{
			Type:    cfg.Relay.Input.Type,
			Address: cfg.Relay.Input.Address,
		},
		Output: relay.Endpoint{
			Type:    cfg.Relay.Output.Type,
			Address: cfg.Relay.Output.Address,
		},
		QueueSize:         cfg.Relay.QueueSize,
		ReconnectInterval: cfg.Relay.ReconnectInterval,
	}, log)

	r.Start()
	log.Info("dns-flow relay started")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		sig := <-quit
		switch sig {
		case syscall.SIGHUP:
			log.Info("config reload triggered by SIGHUP")
			log.Info("relay config changes require restart to apply")
		default:
			log.Info("shutting down", "signal", sig.String())
			r.Stop()
			log.Info("shutdown complete")
			return
		}
	}
}

func runCollect(cfg *config.Config, cfgPath string, log *slog.Logger) {
	geoipResolver := initGeoIP(cfg, log)

	var threatEngine *usecase.ThreatIntelEngine
	if cfg.ThreatIntel.Enabled {
		threatEngine = usecase.NewThreatIntelEngine(
			cfg.ThreatIntel.BlocklistPaths,
			cfg.ThreatIntel.CustomDomains,
			cfg.ThreatIntel.CustomIPs,
			log,
		)
	}

	// --- Storage (outputs) ---
	storages := initStorages(cfg, log)

	var pipelineOutputs []domain.Storage
	var kafkaConsumer *kafkain.Consumer

	if cfg.Kafka.IsEnabled() {
		log.Info("kafka buffering enabled", "brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.Topic.Raw)
		// --- Kafka producer (collector side) ---
		kafkaProducer := kafkaout.NewProducer(kafkaout.Config{
			Brokers:       cfg.Kafka.Brokers,
			Topic:         cfg.Kafka.Topic.Raw,
			BatchSize:     cfg.Kafka.Producer.BatchSize,
			FlushInterval: cfg.Kafka.Producer.FlushInterval,
			Compression:   cfg.Kafka.Producer.Compression,
			RetentionMS:   cfg.Kafka.Topic.RetentionMS,
		}, log)

		if err := kafkaProducer.Migrate(); err != nil {
			logger.Fatal(log, "kafka connection failed", "error", err)
		}

		pipelineOutputs = []domain.Storage{kafkaProducer}

		// --- Kafka consumer (reads Kafka → storage) ---
		kafkaConsumer = kafkain.NewConsumer(kafkain.Config{
			Brokers:  cfg.Kafka.Brokers,
			Topic:    cfg.Kafka.Topic.Raw,
			GroupID:  cfg.Kafka.Consumer.GroupID,
			Storages: storages,
		}, log)
	} else {
		log.Info("direct storage mode active (Kafka bypassed)")
		pipelineOutputs = storages
	}

	// --- Collector pipeline ---
	collectorPipeline := usecase.NewPipeline(
		pipelineOutputs,
		geoipResolver,
		threatEngine,
		cfg.Pipeline.WorkerCount,
		cfg.Pipeline.QueueSize,
		log,
	)

	// --- Prometheus Metrics ---
	var metricsExporter *metrics.MetricsExporter
	if cfg.Monitoring.MetricsEnabled {
		metricsExporter = metrics.InitMetrics(cfg.Monitoring.PrometheusPort, cfg.Monitoring.MetricsPath, cfg.Monitoring.AuthToken, log)
		metricsExporter.Start()
	}

	// --- Start ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := collectorPipeline.Run(); err != nil {
		logger.Fatal(log, "failed to start pipeline", "error", err)
	}

	dnstapServer := dnstap.NewServer(dnstap.Config{
		Type:       cfg.DNSTap.Type,
		Listen:     cfg.DNSTap.Listen,
		UnixSocket: cfg.DNSTap.UnixSocket,
		TLSEnabled: cfg.DNSTap.TLS.Enabled,
		CertFile:   cfg.DNSTap.TLS.CertFile,
		KeyFile:    cfg.DNSTap.TLS.KeyFile,
	}, collectorPipeline, log)
	if err := dnstapServer.Start(); err != nil {
		logger.Fatal(log, "failed to start dnstap server", "error", err)
	}

	if kafkaConsumer != nil {
		go kafkaConsumer.Start(ctx)
	}

	dnstapDesc := cfg.DNSTap.Listen
	if cfg.DNSTap.Type == "unix" {
		dnstapDesc = cfg.DNSTap.UnixSocket
	}
	kafkaDesc := "disabled (direct storage mode)"
	if cfg.Kafka.IsEnabled() {
		kafkaDesc = fmt.Sprintf("%v", cfg.Kafka.Brokers)
	}
	log.Info("dns-flow started",
		"dnstap", dnstapDesc,
		"tls_enabled", cfg.DNSTap.TLS.Enabled,
		"kafka", kafkaDesc,
	)

	// --- Signals ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		sig := <-quit
		switch sig {
		case syscall.SIGHUP:
			handleReload(cfgPath, log)
		default:
			log.Info("shutting down", "signal", sig.String())
			if metricsExporter != nil {
				metricsExporter.Stop()
			}
			if kafkaConsumer != nil {
				kafkaConsumer.Stop()
			}
			dnstapServer.Stop()
			collectorPipeline.Shutdown()
			log.Info("shutdown complete")
			return
		}
	}
}

func resolveConfig(cfgPath string) *string {
	if cfgPath != "" {
		return &cfgPath
	}

	candidates := []string{
		"./config.yaml",
		"./configs/config.yaml",
		"/usr/local/etc/dns-flow.yaml",
		"/etc/dns-flow.yaml",
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return &p
		}
	}
	return nil
}

func initGeoIP(cfg *config.Config, log *slog.Logger) domain.GeoIPResolver {
	if !cfg.Pipeline.Enrichment.GeoIPEnabled {
		return nil
	}
	resolver, err := geoip.NewMaxMindResolver(
		cfg.GeoIP.MaxmindDBPath,
		cfg.GeoIP.ASNDBPath,
		cfg.GeoIP.CacheSize,
		log,
	)
	if err != nil {
		log.Warn("geoip unavailable, enrichment disabled", "error", err)
	}
	return resolver
}

func initStorages(cfg *config.Config, log *slog.Logger) []domain.Storage {
	var storages []domain.Storage

	if cfg.Outputs.ClickHouse != nil {
		ch := clickhouseout.NewWriter(clickhouseout.Config{
			Hosts:       cfg.Outputs.ClickHouse.Hosts,
			Database:    cfg.Outputs.ClickHouse.Database,
			Username:    cfg.Outputs.ClickHouse.Username,
			Password:    cfg.Outputs.ClickHouse.Password,
			Compression: cfg.Outputs.ClickHouse.Compression,
			PoolSize:    cfg.Outputs.ClickHouse.PoolSize,
			TTLDays:     cfg.Outputs.ClickHouse.TTLDays,
		}, log)
		storages = append(storages, ch)
	}

	if cfg.Outputs.InfluxDB != nil {
		inf := influxdbout.NewWriter(influxdbout.Config{
			URL:             cfg.Outputs.InfluxDB.URL,
			Database:        cfg.Outputs.InfluxDB.Database,
			Username:        cfg.Outputs.InfluxDB.Username,
			Password:        cfg.Outputs.InfluxDB.Password,
			RetentionPolicy: cfg.Outputs.InfluxDB.RetentionPolicy,
			RetentionDays:   cfg.Outputs.InfluxDB.RetentionDays,
			Measurement:     cfg.Outputs.InfluxDB.Measurement,
		}, log)
		storages = append(storages, inf)
	}

	if cfg.Outputs.InfluxDBV2 != nil {
		inf := influxdbv2.NewWriter(influxdbv2.Config{
			URL:           cfg.Outputs.InfluxDBV2.URL,
			Org:           cfg.Outputs.InfluxDBV2.Org,
			Bucket:        cfg.Outputs.InfluxDBV2.Bucket,
			Token:         cfg.Outputs.InfluxDBV2.Token,
			Precision:     cfg.Outputs.InfluxDBV2.Precision,
			Measurement:   cfg.Outputs.InfluxDBV2.Measurement,
			RetentionDays: cfg.Outputs.InfluxDBV2.RetentionDays,
		}, log)
		storages = append(storages, inf)
	}

	if cfg.Outputs.File != nil {
		f := file.NewWriter(file.Config{
			Path:       cfg.Outputs.File.Path,
			MaxSizeMB:  cfg.Outputs.File.MaxSizeMB,
			MaxAgeDays: cfg.Outputs.File.MaxAgeDays,
			MaxBackups: cfg.Outputs.File.MaxBackups,
			Compress:   cfg.Outputs.File.Compress,
		}, log)
		storages = append(storages, f)
	}

	// Migrate storage
	for _, s := range storages {
		if err := s.Migrate(); err != nil {
			log.Warn("storage setup failed", "name", s.Name(), "error", err)
		} else {
			log.Info("storage ready", "name", s.Name())
		}
	}

	return storages
}

func handleReload(cfgPath string, log *slog.Logger) {
	log.Info("config reload triggered by SIGHUP")

	newCfg, err := config.Load(cfgPath)
	if err != nil {
		log.Warn("config reload failed to read file", "error", err)
		return
	}

	if err := newCfg.Validate(); err != nil {
		log.Warn("config reload failed validation", "error", err)
		return
	}

	if newCfg.Server.LogLevel != "" && !strings.EqualFold(newCfg.Server.LogLevel, "info") {
		log.Info("log level change detected", "new", newCfg.Server.LogLevel)
		log.Warn("restart required to apply new log level")
	}

	dnstapDesc := newCfg.DNSTap.Listen
	if newCfg.DNSTap.Type == "unix" {
		dnstapDesc = newCfg.DNSTap.UnixSocket
	}
	log.Info("config reload complete",
		"mode", modeOf(newCfg),
		"dnstap", dnstapDesc,
		"kafka", newCfg.Kafka.Brokers,
		"outputs", outputSummary(newCfg),
	)
}

func modeOf(cfg *config.Config) string {
	if cfg.Mode == "relay" {
		return "relay"
	}
	return "collect"
}

func outputSummary(cfg *config.Config) string {
	var parts []string
	if cfg.Outputs.ClickHouse != nil {
		parts = append(parts, "clickhouse")
	}
	if cfg.Outputs.InfluxDB != nil {
		parts = append(parts, "influxdb_v1")
	}
	if cfg.Outputs.InfluxDBV2 != nil {
		parts = append(parts, "influxdb_v2")
	}
	if cfg.Outputs.File != nil {
		parts = append(parts, "file")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}
