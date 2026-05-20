// Package kafka provides a Kafka consumer for integration events from the
// Event Streaming & Audit backbone. The consumer uses the segmentio/kafka-go
// reader with consumer-group semantics and at-least-once delivery semantics:
// offsets are only committed after the handler returns nil.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/SheykoWk/workflow-engine/internal/domain"
)

// Handler processes a single integration event. Return a non-nil error to
// prevent offset commit (the message will be redelivered).
type Handler func(ctx context.Context, event *domain.IntegrationEvent) error

// Consumer wraps a kafka-go Reader and dispatches messages to a Handler.
type Consumer struct {
	reader  *kafkago.Reader
	handler Handler
	log     *slog.Logger
}

// Config holds Kafka consumer configuration.
type Config struct {
	Brokers       []string
	Topic         string
	GroupID       string
	MinBytes      int
	MaxBytes      int
}

// NewConsumer builds a Consumer. groupID should be unique per deployment so
// multiple Workflow Engine instances act as competing consumers.
func NewConsumer(cfg Config, handler Handler, log *slog.Logger) *Consumer {
	minBytes := cfg.MinBytes
	if minBytes == 0 {
		minBytes = 1
	}
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = 10 * 1024 * 1024 // 10 MiB
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: minBytes,
		MaxBytes: maxBytes,
	})
	return &Consumer{reader: reader, handler: handler, log: log}
}

// Run blocks and processes messages until ctx is cancelled.
// It returns nil on clean shutdown (ctx.Err() != nil).
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // normal shutdown
			}
			return fmt.Errorf("kafka consumer: fetch: %w", err)
		}

		var event domain.IntegrationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// Poison pill: log and skip — do not block the consumer.
			c.log.Warn("kafka consumer: malformed message, skipping",
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				c.log.Error("kafka consumer: commit poison pill failed", "error", commitErr)
			}
			continue
		}

		if err := c.handler(ctx, &event); err != nil {
			// Handler failure: do not commit. The message will be redelivered.
			// Persistent failures will stall the partition; consider a DLQ strategy.
			c.log.Error("kafka consumer: handler error — offset not committed",
				"event_type", event.Type,
				"tenant_id", event.TenantID,
				"event_id", event.ID,
				"error", err,
			)
			// Back-pressure: return the error so the caller can restart / circuit-break.
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				continue // retry without crashing the loop
			}
			return err
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Error("kafka consumer: commit failed", "error", err)
		}
	}
}

// Close shuts down the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
