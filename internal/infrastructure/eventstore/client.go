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
	StreamID  string            `json:"stream_id"`
	Type      string            `json:"type"`
	Source    string            `json:"source"`
	Payload   map[string]any    `json:"payload,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Publish sends a workflow lifecycle event to the Event Streaming ingest API.
// The stream_id is scoped by tenant when an external_tenant_id is available,
// otherwise it falls back to the project-scoped stream.
func (c *HTTPClient) Publish(ctx context.Context, event ports.WorkflowEvent) error {
	if c.baseURL == "" {
		return fmt.Errorf("eventstore: EVENT_STREAMING_BASE_URL is not configured")
	}

	streamID := buildStreamID(event)
	metadata := map[string]string{
		"project_id": event.ProjectID,
		"run_id":     event.RunID,
	}
	if event.CorrelationID != "" {
		metadata["correlation_id"] = event.CorrelationID
	}
	if event.ExternalTenantID != "" {
		metadata["tenant_id"] = event.ExternalTenantID
	}

	body, err := json.Marshal(ingestRequest{
		StreamID: streamID,
		Type:     string(event.Type),
		Source:   c.source,
		Payload:  event.Payload,
		Metadata: metadata,
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
