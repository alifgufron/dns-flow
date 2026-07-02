package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type Config struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Storages []domain.Storage
}

type Consumer struct {
	cfg    Config
	logger *slog.Logger
	reader *kafka.Reader
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewConsumer(cfg Config, logger *slog.Logger) *Consumer {
	return &Consumer{
		cfg:    cfg,
		logger: logger,
	}
}

func (c *Consumer) waitForTopic(ctx context.Context) {
	client := &kafka.Client{
		Addr:    kafka.TCP(c.cfg.Brokers[0]),
		Timeout: 5 * time.Second,
	}
	for {
		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{
			Topics: []string{c.cfg.Topic},
		})
		if err == nil {
			for _, t := range resp.Topics {
				if t.Name == c.cfg.Topic && (t.Error == nil) {
					c.logger.Info("kafka consumer: topic found", "topic", c.cfg.Topic)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (c *Consumer) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.waitForTopic(ctx)

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.cfg.Brokers,
		Topic:       c.cfg.Topic,
		GroupID:     c.cfg.GroupID,
		MinBytes:    1,
		MaxBytes:    10 * 1024 * 1024,
		StartOffset: kafka.FirstOffset,
	})

	names := make([]string, len(c.cfg.Storages))
	for i, s := range c.cfg.Storages {
		names[i] = s.Name()
	}
	c.logger.Info("kafka consumer: started",
		"brokers", c.cfg.Brokers,
		"topic", c.cfg.Topic,
		"group_id", c.cfg.GroupID,
		"storages", names,
	)

	c.wg.Add(1)
	go c.loop(ctx)
}

func (c *Consumer) loop(ctx context.Context) {
	defer c.wg.Done()

	var total int64

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				c.logger.Error("kafka consumer: read error", "error", err)
				continue
			}
		}

		var event domain.DNSRawEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Warn("kafka consumer: unmarshal error", "error", err)
			continue
		}

		for _, s := range c.cfg.Storages {
			if err := s.Write(event); err != nil {
				c.logger.Error("kafka consumer: storage write error",
					"name", s.Name(), "error", err)
			}
		}

		total++
		if total%1000 == 0 {
			c.logger.Info("kafka consumer: processed",
				"total", total,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
	}
}

func (c *Consumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.reader != nil {
		c.reader.Close()
	}
	c.wg.Wait()
	c.logger.Info("kafka consumer: stopped")
}
