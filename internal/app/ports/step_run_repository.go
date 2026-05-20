package ports

import (
	"context"
	"time"
)

// PendingStepRun is the next step the executor should run (pending, eligible by order).
type PendingStepRun struct {
	ID             string
	WorkflowRunID  string
	WorkflowID     string
	WorkflowStepID string
	StepIndex      int
	Attempt        int
	StepType       string
	Config         []byte
}

// StepIndexOutput is stored JSON output for a succeeded step (workflow_steps.step_index).
type StepIndexOutput struct {
	StepIndex int
	Output    []byte // JSON object; never nil from the repository
}

// StepRunRepository is the persistence port required by the executor.
type StepRunRepository interface {
	GetNextPendingStepRun(ctx context.Context) (*PendingStepRun, error)
	GetPreviousStepOutputs(ctx context.Context, workflowRunID string) ([]StepIndexOutput, error)
	IsWorkflowRunPending(ctx context.Context, workflowRunID string) (bool, error)
	WorkflowRunStepCounts(ctx context.Context, workflowRunID string) (total int, succeeded int, err error)
	MarkStepRunRunning(ctx context.Context, id string) error
	MarkStepRunSucceeded(ctx context.Context, id string, outputJSON []byte) error
	MarkStepRunFailed(ctx context.Context, id string, outputJSON []byte) error
	MarkWorkflowRunRunning(ctx context.Context, workflowRunID string) error
	MarkWorkflowRunSucceeded(ctx context.Context, workflowRunID string) error
	MarkWorkflowRunFailed(ctx context.Context, workflowRunID string) error
	RetryStepRun(ctx context.Context, id string, nextRunAt time.Time, attempt int) error
	// GetProjectContext returns the project_id and external_tenant_id for a workflow run.
	GetProjectContext(ctx context.Context, workflowRunID string) (projectID, externalTenantID string, err error)
}
