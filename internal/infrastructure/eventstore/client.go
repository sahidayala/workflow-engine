// Package eventstore provides an HTTP client adapter for publishing workflow
// lifecycle events to the Event Streaming & Audit backbone.
package eventstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/SheykoWk/workflow-engine/internal/app/ports"
)

// Compile-time contract: HTTPClient must satisfy ports.EventPublisher.
var _ ports.EventPublisher = (*HTTPClient)(nil)

// HTTPClient implements ports.EventPublisher by POSTing to the Event Streaming
// ingest API (POST /events). Publish failures are returned to the caller but
// must not crash the executor — the caller decides whether to log-and-continue.
type HTTPClient struct {
	baseURL  string
	apiToken string
	source   string
	http     *http.Client
	log      *slog.Logger
}

// NewHTTPClient returns a configured Event Streaming HTTP client.
func NewHTTPClient(baseURL, apiToken string, log *slog.Logger) *HTTPClient {
	return &HTTPClient{
		baseURL:  baseURL,
		apiToken: apiToken,
		source:   "workflow-engine",
		http: &http.Client{
			Timeout: 5 * time.Second,
		},
		log: log,
	}
}

type ingestRequest struct {
	StreamID      string            `json:"stream_id"`
	Type          string            `json:"type"`
	Source        string            `json:"source"`
	EventVersion  int               `json:"event_version,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	ActorID       string            `json:"actor_id,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	Payload       map[string]any    `json:"payload,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// Publish sends a workflow lifecycle event to the Event Streaming ingest API.
// The stream_id is scoped by tenant when an external_tenant_id is available,
// otherwise it falls back to the project-scoped stream.
//
// The full canonical envelope (correlation_id, causation_id, actor_id, trace_id)
// is forwarded so Event Streaming can reconstruct the causation chain from the
// original platform event through the workflow execution.
func (c *HTTPClient) Publish(ctx context.Context, event ports.WorkflowEvent) error {
	if c.baseURL == "" {
		return fmt.Errorf("eventstore: EVENT_STREAMING_BASE_URL is not configured")
	}

	streamID := buildStreamID(event)
	metadata := map[string]string{
		"project_id": event.ProjectID,
		"run_id":     event.RunID,
	}
	if event.StepRunID != "" {
		metadata["step_run_id"] = event.StepRunID
	}

	eventVersion := event.EventVersion
	if eventVersion == 0 {
		eventVersion = 1
	}
	actorID := event.ActorID
	if actorID == "" {
		actorID = "workflow-engine"
	}

	body, err := json.Marshal(ingestRequest{
		StreamID:     streamID,
		Type:         string(event.Type),
		Source:       c.source,
		EventVersion: eventVersion,
		CausationID:  event.CausationID,
		ActorID:      actorID,
		TraceID:      event.TraceID,
		Payload:      event.Payload,
		Metadata:     metadata,
	})
	if err != nil {
		return fmt.Errorf("eventstore: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/events", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("eventstore: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	if event.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", event.CorrelationID)
	}
	if event.CausationID != "" {
		req.Header.Set("X-Causation-ID", event.CausationID)
	}
	if event.TraceID != "" {
		req.Header.Set("X-Trace-ID", event.TraceID)
		// Also set W3C traceparent so the Event Streaming middleware can extract the
		// full trace context (ADR-014). Use the trace ID as both traceId and a synthetic
		// spanId (first 16 chars) to satisfy the header format — the spanId only needs
		// to be unique within the trace, not cryptographically strong here.
		if len(event.TraceID) == 32 {
			req.Header.Set("traceparent", "00-"+event.TraceID+"-"+event.TraceID[:16]+"-01")
		}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("eventstore: http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("eventstore: ingest returned %d", resp.StatusCode)
	}
	return nil
}

// buildStreamID returns a tenant-scoped stream ID when possible, otherwise
// falls back to a project-scoped ID. Tenant-scoped IDs allow the SaaS platform
// to query its own event history from a single tenant stream.
func buildStreamID(event ports.WorkflowEvent) string {
	if event.ExternalTenantID != "" {
		return fmt.Sprintf("platform.%s.workflow", event.ExternalTenantID)
	}
	return fmt.Sprintf("workflow.project.%s", event.ProjectID)
}
