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
	"github.com/alifgufron/dns-flow/internal/usecase"
)

func main() {
	cfgPath := flag.String("config", "", "path to configuration file")
	flag.Parse()

	cfgPath = resolveConfig(*cfgPath)
	if cfgPath == nil {
		fmt.Fprintf(os.Stderr, "Usage: dns-flow -config <config.yaml>\n\n")
		fmt.Fprintf(os.Stderr, "Example:\n")
		fmt.Fprintf(os.Stderr, "  dns-flow -config /usr/local/etc/dns-flow.yaml\n")
		fmt.Fprintf(os.Stderr, "  dns-flow -config ./config.yaml\n")
		os.Exit(1)
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

	geoipResolver := initGeoIP(cfg, log)

	// --- Kafka producer (collector side) ---
	kafkaProducer := kafkaout.NewProducer(kafkaout.Config{
		Brokers:       cfg.Kafka.Brokers,
		Topic:         cfg.Kafka.Topic.Raw,
		BatchSize:     cfg.Kafka.Producer.BatchSize,
		FlushInterval: cfg.Kafka.Producer.FlushInterval,
		Compression:   cfg.Kafka.Producer.Compression,
	}, log)

	if err := kafkaProducer.Migrate(); err != nil {
		logger.Fatal(log, "kafka connection failed", "error", err)
	}

	// --- Collector pipeline (DNSTAP → Kafka) ---
	collectorPipeline := usecase.NewPipeline(
		[]domain.Storage{kafkaProducer},
		geoipResolver,
		cfg.Pipeline.WorkerCount,
		cfg.Pipeline.QueueSize,
		log,
	)

	// --- Storage (consumer side) ---
	storages := initStorages(cfg, log)

	// --- Kafka consumer (reads Kafka → storage) ---
	kafkaConsumer := kafkain.NewConsumer(kafkain.Config{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic.Raw,
		GroupID:  cfg.Kafka.Consumer.GroupID,
		Storages: storages,
	}, log)

	// --- Start ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := collectorPipeline.Run(); err != nil {
		logger.Fatal(log, "failed to start pipeline", "error", err)
	}

	dnstapServer := dnstap.NewServer(cfg.DNSTap.Listen, collectorPipeline, log)
	if err := dnstapServer.Start(); err != nil {
		logger.Fatal(log, "failed to start dnstap server", "error", err)
	}

	go kafkaConsumer.Start(ctx)

	log.Info("dns-flow started",
		"dnstap", cfg.DNSTap.Listen,
		"kafka", cfg.Kafka.Brokers,
	)

	// --- Signals ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		sig := <-quit
		switch sig {
		case syscall.SIGHUP:
			handleReload(*cfgPath, log)
		default:
			log.Info("shutting down", "signal", sig.String())
			kafkaConsumer.Stop()
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
		}, log)
		storages = append(storages, ch)
	}

	if cfg.Outputs.InfluxDB != nil {
		inf := influxdbout.NewWriter(influxdbout.Config{
			URL:              cfg.Outputs.InfluxDB.URL,
			Database:         cfg.Outputs.InfluxDB.Database,
			Username:         cfg.Outputs.InfluxDB.Username,
			Password:         cfg.Outputs.InfluxDB.Password,
			RetentionPolicy:  cfg.Outputs.InfluxDB.RetentionPolicy,
			Measurement:      cfg.Outputs.InfluxDB.Measurement,
		}, log)
		storages = append(storages, inf)
	}

	if cfg.Outputs.InfluxDBV2 != nil {
		inf := influxdbv2.NewWriter(influxdbv2.Config{
			URL:         cfg.Outputs.InfluxDBV2.URL,
			Org:         cfg.Outputs.InfluxDBV2.Org,
			Bucket:      cfg.Outputs.InfluxDBV2.Bucket,
			Token:       cfg.Outputs.InfluxDBV2.Token,
			Precision:   cfg.Outputs.InfluxDBV2.Precision,
			Measurement: cfg.Outputs.InfluxDBV2.Measurement,
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

	log.Info("config reload complete",
		"dnstap", newCfg.DNSTap.Listen,
		"kafka", newCfg.Kafka.Brokers,
		"outputs", outputSummary(newCfg),
	)
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
