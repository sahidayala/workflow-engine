// Package ports defines secondary ports (driven adapters) used by the application layer.
package ports

import (
	"context"
	"time"
)

// WorkflowEventType enumerates the lifecycle events the executor emits.
type WorkflowEventType string

const (
	WorkflowRunStarted   WorkflowEventType = "workflow.run.started"
	WorkflowRunCompleted WorkflowEventType = "workflow.run.completed"
	WorkflowRunFailed    WorkflowEventType = "workflow.run.failed"
	StepRunStarted       WorkflowEventType = "workflow.step.started"
	StepRunSucceeded     WorkflowEventType = "workflow.step.succeeded"
	StepRunFailed        WorkflowEventType = "workflow.step.failed"
)

// WorkflowEvent carries the data for a single lifecycle event emitted by the
// executor. All canonical envelope fields must be populated before calling Publish.
type WorkflowEvent struct {
	// EventID is a stable UUID for this lifecycle event (idempotency key).
	EventID          string
	Type             WorkflowEventType
	// EventVersion is the schema version of this event type (always 1 until shape changes).
	EventVersion     int
	ProjectID        string
	// ExternalTenantID maps to the SaaS platform tenantId; empty when not configured.
	ExternalTenantID string
	RunID            string
	WorkflowID       string
	StepRunID        string // empty for run-level events
	StepIndex        int    // -1 for run-level events
	CorrelationID    string
	// CausationID is the ID of the integration event that triggered this workflow,
	// or the step event that caused the next step. Preserves the causation chain
	// so Event Streaming can reconstruct the full lineage of a workflow execution.
	CausationID      string
	// ActorID is always "workflow-engine" for lifecycle events.
	ActorID          string
	// TraceID propagated from the triggering integration event.
	TraceID          string
	OccurredAt       time.Time
	Payload          map[string]any
}

// EventPublisher is the port for emitting workflow lifecycle events to an
// external event backbone. Implementations must be non-blocking best-effort:
// a publish failure must NOT fail the executor step — log and continue.
type EventPublisher interface {
	Publish(ctx context.Context, event WorkflowEvent) error
}
