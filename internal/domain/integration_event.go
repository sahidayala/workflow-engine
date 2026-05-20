package domain

import (
	"encoding/json"
	"time"
)

// IntegrationEvent is the canonical wire format for events received from the
// Event Streaming & Audit backbone via Kafka. It mirrors the Event entity in
// that project so we can unmarshal Kafka messages without a schema registry.
type IntegrationEvent struct {
	ID            string            `json:"id"`
	TenantID      string            `json:"tenant_id"`
	StreamID      string            `json:"stream_id"`
	Type          string            `json:"type"`
	Source        string            `json:"source"`
	Version       int64             `json:"version"`
	OccurredAt    time.Time         `json:"occurred_at"`
	CorrelationID string            `json:"correlation_id"`
	Payload       json.RawMessage   `json:"payload"`
	Metadata      map[string]string `json:"metadata"`
}
