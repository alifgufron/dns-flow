package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type Config struct {
	Brokers       []string
	Topic         string
	BatchSize     int
	FlushInterval time.Duration
	Compression   string
	RetentionMS   string
}

type Producer struct {
	cfg    Config
	writer *kafka.Writer
	logger *slog.Logger
	queue  chan domain.DNSRawEvent
	done   chan struct{}
	wg     sync.WaitGroup
}

func NewProducer(cfg Config, logger *slog.Logger) *Producer {
	return &Producer{
		cfg:    cfg,
		logger: logger,
		queue:  make(chan domain.DNSRawEvent, 100000),
		done:   make(chan struct{}),
	}
}

func (p *Producer) Name() string {
	return "kafka"
}

func (p *Producer) Migrate() error {
	p.logger.Info("kafka: connecting",
		"brokers", p.cfg.Brokers,
		"topic", p.cfg.Topic,
	)

	client := &kafka.Client{
		Addr:    kafka.TCP(p.cfg.Brokers[0]),
		Timeout: 5 * time.Second,
	}

	// Check topic existence, create if missing
	found := false
	resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{
		Topics: []string{p.cfg.Topic},
	})
	if err != nil {
		p.logger.Warn("kafka: unable to verify topic", "topic", p.cfg.Topic, "error", err)
	} else {
		for _, t := range resp.Topics {
			if t.Name == p.cfg.Topic {
				if t.Error != nil {
					p.logger.Info("kafka: topic not found, creating", "topic", p.cfg.Topic, "error", t.Error)
				} else {
					found = true
					p.logger.Info("kafka: topic found", "topic", p.cfg.Topic)
				}
				break
			}
		}
	}

	if !found {
		p.logger.Info("kafka: topic not found, creating", "topic", p.cfg.Topic)
		if _, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{{
				Topic:             p.cfg.Topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			}},
		}); err != nil {
			return fmt.Errorf("kafka: create topic: %w", err)
		}
		p.logger.Info("kafka: topic created", "topic", p.cfg.Topic)

		for i := 0; i < 10; i++ {
			resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{
				Topics: []string{p.cfg.Topic},
			})
			if err == nil {
				for _, t := range resp.Topics {
					if t.Name == p.cfg.Topic && t.Error == nil {
						p.logger.Info("kafka: topic ready", "topic", p.cfg.Topic)
						goto ready
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		return fmt.Errorf("kafka: topic not ready after creation")
	}
ready:

	if p.cfg.RetentionMS != "" {
		_, err := client.IncrementalAlterConfigs(context.Background(), &kafka.IncrementalAlterConfigsRequest{
			Addr: kafka.TCP(p.cfg.Brokers[0]),
			Resources: []kafka.IncrementalAlterConfigsRequestResource{{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: p.cfg.Topic,
				Configs: []kafka.IncrementalAlterConfigsRequestConfig{{
					Name:            "retention.ms",
					Value:           p.cfg.RetentionMS,
					ConfigOperation: kafka.ConfigOperationSet,
				}},
			}},
		})
		if err != nil {
			p.logger.Warn("kafka: set retention.ms failed (ignored)", "error", err)
		} else {
			p.logger.Info("kafka: retention.ms set", "topic", p.cfg.Topic, "retention_ms", p.cfg.RetentionMS)
		}
	}

	compression := compress.None
	switch p.cfg.Compression {
	case "gzip":
		compression = compress.Gzip
	case "lz4":
		compression = compress.Lz4
	case "zstd":
		compression = compress.Zstd
	}

	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.cfg.Brokers...),
		Topic:        p.cfg.Topic,
		Compression:  compression,
		BatchSize:    p.cfg.BatchSize,
		BatchTimeout: p.cfg.FlushInterval,
		RequiredAcks: kafka.RequireOne,
		Balancer:     &kafka.Hash{},
	}

	p.startFlusher()

	p.logger.Info("kafka: producer ready",
		"topic", p.cfg.Topic,
		"compression", p.cfg.Compression,
	)
	return nil
}

func (p *Producer) Write(event domain.DNSRawEvent) error {
	select {
	case p.queue <- event:
		return nil
	default:
		p.logger.Warn("kafka: queue full, dropping event")
		return nil
	}
}

func (p *Producer) Produce(event domain.DNSRawEvent) error {
	return p.Write(event)
}

func (p *Producer) startFlusher() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		messages := make([]kafka.Message, 0, p.cfg.BatchSize)
		ticker := time.NewTicker(p.cfg.FlushInterval)
		defer ticker.Stop()

		flush := func() {
			if len(messages) == 0 {
				return
			}
			p.flushBatch(messages)
			messages = messages[:0]
		}

		for {
			select {
			case <-p.done:
				flush()
				return
			case evt := <-p.queue:
				data, err := json.Marshal(evt)
				if err != nil {
					p.logger.Error("kafka: marshal error", "error", err)
					continue
				}
				messages = append(messages, kafka.Message{
					Key:   []byte(evt.DNS.QName),
					Value: data,
				})
				if len(messages) >= p.cfg.BatchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()
}

func isRetriable(err error) bool {
	if err == nil {
		return false
	}
	// Check direct kafka error
	if errors.Is(err, kafka.UnknownTopicOrPartition) ||
		errors.Is(err, kafka.LeaderNotAvailable) ||
		errors.Is(err, kafka.NotLeaderForPartition) ||
		errors.Is(err, kafka.RequestTimedOut) {
		return true
	}
	// Check WriteErrors (per-message errors)
	var writeErr kafka.WriteErrors
	if errors.As(err, &writeErr) {
		for _, me := range writeErr {
			if isRetriable(me) {
				return true
			}
		}
	}
	return false
}

func (p *Producer) flushBatch(messages []kafka.Message) {
	var err error
	for attempt := range 3 {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
			p.logger.Info("kafka: retrying write", "attempt", attempt+1, "count", len(messages))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = p.writer.WriteMessages(ctx, messages...)
		cancel()
		if err == nil {
			p.logger.Debug("kafka: flushed", "count", len(messages))
			return
		}
		if !isRetriable(err) {
			break
		}
	}
	if err != nil {
		p.logger.Error("kafka: write error",
			"error", truncateError(err),
			"count", len(messages),
		)
	} else {
		p.logger.Debug("kafka: flushed", "count", len(messages))
	}
}

func truncateError(err error) string {
	s := err.Error()
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

func (p *Producer) Close() error {
	close(p.done)
	p.wg.Wait()

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			return err
		}
	}

	p.logger.Info("kafka: producer closed", "topic", p.cfg.Topic)
	return nil
}

func (p *Producer) Health() map[string]string {
	return map[string]string{
		"kafka_topic": p.cfg.Topic,
	}
}
