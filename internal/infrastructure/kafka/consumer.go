// Package kafka provides a Kafka consumer for integration events from the
// Event Streaming & Audit backbone. The consumer uses the segmentio/kafka-go
// reader with consumer-group semantics and at-least-once delivery semantics:
// offsets are only committed after the handler returns nil.
//
// Dead-letter strategy:
//
// Persistent handler failures (same message failing > maxHandlerRetries times)
// are "dead-lettered" by logging a CRITICAL-level structured error and committing
// the offset. The source event is always durable in Event Streaming's PostgreSQL
// store and can be recovered via the replay API. A separate alerting pipeline
// should watch for "kafka consumer: message dead-lettered" log lines and page
// on-call when they appear.
//
// This approach avoids partition stall — a message that keeps failing would
// block ALL subsequent messages in the same partition indefinitely. By committing
// after maxHandlerRetries, later messages continue to flow. The cost is at-most-once
// processing for the failed message, which is acceptable because the source event
// is already durable in Event Streaming.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/SheykoWk/workflow-engine/internal/domain"
	wftrace "github.com/SheykoWk/workflow-engine/internal/pkg/trace"
)

// maxHandlerRetries is the number of in-memory retry attempts before a message
// is dead-lettered. 5 attempts with exponential backoff gives a total retry
// window of ~3.1s, which covers most transient DB and network errors.
const maxHandlerRetries = 5

// handlerRetryDelay returns the wait time before attempt number `attempt` (1-based).
// Base is 100ms, doubles each attempt, capped at 2s, with ±20% jitter.
// Total window for 5 attempts: ~100+200+400+800+1600ms ≈ 3.1s.
func handlerRetryDelay(attempt int) time.Duration {
	base := 100.0 // ms
	delay := base * math.Pow(2, float64(attempt-1))
	if delay > 2000 {
		delay = 2000
	}
	jitter := 0.8 + rand.Float64()*0.4 // uniform [0.8, 1.2]
	return time.Duration(delay*jitter) * time.Millisecond
}

// Handler processes a single integration event. Return a non-nil error to
// signal that the event should be retried (up to maxHandlerRetries).
type Handler func(ctx context.Context, event *domain.IntegrationEvent) error

// Consumer wraps a kafka-go Reader and dispatches messages to a Handler.
type Consumer struct {
	reader  *kafkago.Reader
	handler Handler
	log     *slog.Logger
}

// Config holds Kafka consumer configuration.
type Config struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
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

		// Extract W3C TraceContext from Kafka headers and attach to the message
		// context so all handler log statements are correlated with the originating
		// HTTP request (ADR-014). correlation_id is extracted as a fallback when
		// the JSON body's field is empty.
		msgCtx := ctx
		var msgTraceID, msgCorrelationID string
		for _, h := range msg.Headers {
			switch h.Key {
			case "traceparent":
				if tid := wftrace.ParseTraceID(string(h.Value)); tid != "" {
					msgTraceID = tid
					msgCtx = wftrace.WithTraceID(msgCtx, tid)
				}
			case "correlation_id":
				if v := string(h.Value); v != "" {
					msgCorrelationID = v
				}
			}
		}

		var event domain.IntegrationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// Poison pill: the message cannot be deserialised regardless of retries.
			// Commit the offset so downstream messages are not blocked.
			c.log.Warn("kafka consumer: undeserializable message — dead-lettering",
				"partition", msg.Partition,
				"offset", msg.Offset,
				"trace_id", msgTraceID,
				"correlation_id", msgCorrelationID,
				"error", err,
			)
			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				c.log.Error("kafka consumer: commit of poison pill failed", "error", commitErr)
			}
			continue
		}

		// Prefer the struct field values; fall back to header-extracted values.
		traceID := event.TraceID
		if traceID == "" {
			traceID = msgTraceID
		}
		correlationID := event.CorrelationID
		if correlationID == "" {
			correlationID = msgCorrelationID
		}

		// Attempt the handler up to maxHandlerRetries times with exponential backoff.
		// Backoff covers transient errors (DB connection pool exhaustion, network blips)
		// that typically resolve within 100ms-2s. Without backoff, all 3 retries fire
		// in <10ms and the event is dead-lettered before the error even clears.
		var handlerErr error
		for attempt := 1; attempt <= maxHandlerRetries; attempt++ {
			handlerErr = c.handler(msgCtx, &event)
			if handlerErr == nil {
				break
			}
			if errors.Is(handlerErr, context.Canceled) || errors.Is(handlerErr, context.DeadlineExceeded) {
				// Shutdown in progress — do not retry, do not commit.
				return handlerErr
			}
			c.log.Warn("kafka consumer: handler error (will retry)",
				"event_type", event.Type,
				"event_id", event.ID,
				"tenant_id", event.TenantID,
				"trace_id", traceID,
				"correlation_id", correlationID,
				"attempt", attempt,
				"max_attempts", maxHandlerRetries,
				"error", handlerErr,
			)
			if attempt < maxHandlerRetries {
				select {
				case <-time.After(handlerRetryDelay(attempt)):
				case <-msgCtx.Done():
					return msgCtx.Err()
				}
			}
		}

		if handlerErr != nil {
			// All retries exhausted. Dead-letter the message by committing the offset.
			// Recovery: use the Event Streaming replay API to re-ingest and re-publish,
			// or manually re-trigger via POST /workflows/:id/runs.
			// Alert: monitor for this log line — count > 0 in 5m → page on-call.
			c.log.Error("kafka consumer: message dead-lettered after max retries — offset committed",
				"event_type", event.Type,
				"event_id", event.ID,
				"tenant_id", event.TenantID,
				"trace_id", traceID,
				"correlation_id", correlationID,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"max_attempts", maxHandlerRetries,
				"error", handlerErr,
			)
		}

		// Commit regardless of handler outcome to prevent partition stall.
		// See package-level doc for the design rationale.
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.log.Error("kafka consumer: commit failed",
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", commitErr,
			)
		}
	}
}

// Close shuts down the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
