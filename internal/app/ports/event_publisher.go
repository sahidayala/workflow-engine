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

// WorkflowEvent carries the data for a single lifecycle event.
type WorkflowEvent struct {
	Type          WorkflowEventType
	ProjectID     string
	ExternalTenantID string // empty when project has no external_tenant_id
	RunID         string
	WorkflowID    string
	StepRunID     string // empty for run-level events
	StepIndex     int    // -1 for run-level events
	CorrelationID string
	OccurredAt    time.Time
	Payload       map[string]any
}

// EventPublisher is the port for emitting workflow lifecycle events to an
// external event backbone. Implementations must be non-blocking best-effort:
// a publish failure must NOT fail the executor step — log and continue.
type EventPublisher interface {
	Publish(ctx context.Context, event WorkflowEvent) error
}
