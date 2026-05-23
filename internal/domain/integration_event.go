package domain

import (
	"encoding/json"
	"time"
)

// IntegrationEvent is the canonical wire format for events received from the
// Event Streaming & Audit backbone via Kafka. It mirrors the Event entity in
// that project so we can unmarshal Kafka messages without a schema registry.
//
// Fields must stay in sync with the Event Streaming domain event struct.
// All fields use omitempty for backward compatibility with events produced
// before the field was introduced.
type IntegrationEvent struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	StreamID string `json:"stream_id"`
	Type     string `json:"type"`
	Source   string `json:"source"`
	// Version is the stream sequence number assigned by Event Streaming.
	// See EventVersion for the schema/contract version.
	Version int64 `json:"version"`
	// EventVersion is the schema/contract version of this event type (default 1).
	EventVersion  int       `json:"event_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id,omitempty"`
	ActorID       string    `json:"actor_id,omitempty"`
	// TraceID is the W3C trace ID (32 lowercase hex chars). Extracted from
	// the Kafka `traceparent` header when available (ADR-014).
	TraceID       string `json:"trace_id,omitempty"`
	SourceVersion string `json:"source_version,omitempty"`

	// Replay fields — populated only when this event was created by POST /replay (ADR-015).
	// Consumers that have non-idempotent side effects (e.g. sending emails) MUST check
	// IsReplay before processing and decide whether to skip or process the event.
	IsReplay            bool       `json:"is_replay,omitempty"`
	ReplayID            string     `json:"replay_id,omitempty"`
	ReplayedAt          *time.Time `json:"replayed_at,omitempty"`
	ReplayReason        string     `json:"replay_reason,omitempty"`
	ReplaySourceEventID string     `json:"replay_source_event_id,omitempty"`

	Payload  json.RawMessage   `json:"payload"`
	Metadata map[string]string `json:"metadata"`
}
