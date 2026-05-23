package executor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/SheykoWk/workflow-engine/internal/app/ports"
)

const pollInterval = 1 * time.Second

// newEventID returns a random UUID v4 string for use as a lifecycle event ID.
// Uses crypto/rand so IDs are unpredictable and safe to expose externally.
func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback should never happen on a healthy OS; use time-based ID.
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Start runs the executor loop in a new goroutine until ctx is cancelled.
// publisher is optional — pass nil to disable event publishing.
func Start(ctx context.Context, repo ports.StepRunRepository, publisher ports.EventPublisher, log *slog.Logger) {
	go runLoop(ctx, repo, publisher, log)
}

func runLoop(ctx context.Context, repo ports.StepRunRepository, publisher ports.EventPublisher, logger *slog.Logger) {
	for {
		processOne(ctx, repo, publisher, logger)
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func processOne(ctx context.Context, repo ports.StepRunRepository, publisher ports.EventPublisher, logger *slog.Logger) {
	next, err := repo.GetNextPendingStepRun(ctx)
	if err != nil {
		logger.Error("executor: get next pending step run",
			"error", err,
		)
		return
	}
	if next == nil {
		return
	}

	persist := context.Background()

	projectID, externalTenantID, err := repo.GetProjectContext(persist, next.WorkflowRunID)
	if err != nil {
		// Non-fatal: proceed without tenant context; lifecycle events will lack
		// tenant scoping but the workflow execution must not be blocked.
		logger.Warn("executor: get project context — continuing without tenant",
			"workflow_run_id", next.WorkflowRunID,
			"error", err,
		)
	}

	runPending, err := repo.IsWorkflowRunPending(ctx, next.WorkflowRunID)
	if err != nil {
		logger.Error("executor: check workflow run pending",
			"workflow_run_id", next.WorkflowRunID,
			"error", err,
		)
		return
	}
	firstStep := runPending

	prevOuts, err := repo.GetPreviousStepOutputs(ctx, next.WorkflowRunID)
	if err != nil {
		logger.Error("executor: get previous step outputs",
			"workflow_run_id", next.WorkflowRunID,
			"error", err,
		)
		return
	}
	resolvedConfig, err := interpolateStepConfig(next.Config, prevOuts, next.StepIndex)
	if err != nil {
		logger.Error("executor: interpolate step config",
			"step_run_id", next.ID,
			"step_index", next.StepIndex,
			"error", err,
		)
		return
	}
	stepToRun := *next
	stepToRun.Config = resolvedConfig

	if err := repo.MarkStepRunRunning(ctx, next.ID); err != nil {
		logger.Error("executor: mark step run running",
			"step_run_id", next.ID,
			"error", err,
		)
		return
	}
	publishEvent(ctx, publisher, ports.WorkflowEvent{
		EventID:          newEventID(),
		EventVersion:     1,
		Type:             ports.StepRunStarted,
		ActorID:          "workflow-engine",
		ProjectID:        projectID,
		ExternalTenantID: externalTenantID,
		RunID:            next.WorkflowRunID,
		WorkflowID:       next.WorkflowID,
		StepRunID:        next.ID,
		StepIndex:        next.StepIndex,
		OccurredAt:       time.Now().UTC(),
		Payload: map[string]any{
			"step_type": next.StepType,
			"attempt":   next.Attempt,
		},
	}, logger)

	if firstStep {
		if err := repo.MarkWorkflowRunRunning(persist, next.WorkflowRunID); err != nil {
			logger.Error("executor: mark workflow run running",
				"workflow_run_id", next.WorkflowRunID,
				"error", err,
			)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			EventID:          newEventID(),
			EventVersion:     1,
			Type:             ports.WorkflowRunStarted,
			ActorID:          "workflow-engine",
			ProjectID:        projectID,
			ExternalTenantID: externalTenantID,
			RunID:            next.WorkflowRunID,
			WorkflowID:       next.WorkflowID,
			StepIndex:        -1,
			OccurredAt:       time.Now().UTC(),
			Payload:          map[string]any{},
		}, logger)
	}

	out, err := executeStep(ctx, &stepToRun)
	if err != nil {
		logger.Error("executor: step execution failed",
			"step_run_id", next.ID,
			"step_type", next.StepType,
			"step_index", next.StepIndex,
			"workflow_run_id", next.WorkflowRunID,
			"attempt", next.Attempt,
			"error", err,
		)
		maxAttempts, baseBackoffSec := parseRetryPolicy(next.Config)
		if next.Attempt < maxAttempts && !isNonRetriable(err) {
			delay := retryDelayWithJitter(baseBackoffSec, next.Attempt)
			nextAt := time.Now().UTC().Add(delay)
			if err := repo.RetryStepRun(persist, next.ID, nextAt, next.Attempt+1); err != nil {
				logger.Error("executor: schedule step retry",
					"step_run_id", next.ID,
					"next_attempt", next.Attempt+1,
					"error", err,
				)
			} else {
				logger.Info("executor: step scheduled for retry",
					"step_run_id", next.ID,
					"next_attempt", next.Attempt+1,
					"retry_delay", delay,
				)
			}
			return
		}
		if err := repo.MarkStepRunFailed(persist, next.ID, out); err != nil {
			logger.Error("executor: mark step run failed",
				"step_run_id", next.ID,
				"error", err,
			)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			EventID:          newEventID(),
			EventVersion:     1,
			Type:             ports.StepRunFailed,
			ActorID:          "workflow-engine",
			ProjectID:        projectID,
			ExternalTenantID: externalTenantID,
			RunID:            next.WorkflowRunID,
			WorkflowID:       next.WorkflowID,
			StepRunID:        next.ID,
			StepIndex:        next.StepIndex,
			OccurredAt:       time.Now().UTC(),
			Payload:          map[string]any{"error": err.Error(), "step_type": next.StepType},
		}, logger)

		if err := repo.MarkWorkflowRunFailed(persist, next.WorkflowRunID); err != nil {
			logger.Error("executor: mark workflow run failed",
				"workflow_run_id", next.WorkflowRunID,
				"error", err,
			)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			EventID:          newEventID(),
			EventVersion:     1,
			Type:             ports.WorkflowRunFailed,
			ActorID:          "workflow-engine",
			ProjectID:        projectID,
			ExternalTenantID: externalTenantID,
			RunID:            next.WorkflowRunID,
			WorkflowID:       next.WorkflowID,
			StepIndex:        -1,
			OccurredAt:       time.Now().UTC(),
			Payload:          map[string]any{"failed_step_run_id": next.ID},
		}, logger)
		return
	}

	if err := repo.MarkStepRunSucceeded(persist, next.ID, out); err != nil {
		logger.Error("executor: mark step run succeeded",
			"step_run_id", next.ID,
			"error", err,
		)
		return
	}
	publishEvent(persist, publisher, ports.WorkflowEvent{
		EventID:          newEventID(),
		EventVersion:     1,
		Type:             ports.StepRunSucceeded,
		ActorID:          "workflow-engine",
		ProjectID:        projectID,
		ExternalTenantID: externalTenantID,
		RunID:            next.WorkflowRunID,
		WorkflowID:       next.WorkflowID,
		StepRunID:        next.ID,
		StepIndex:        next.StepIndex,
		OccurredAt:       time.Now().UTC(),
		Payload:          map[string]any{"step_type": next.StepType},
	}, logger)

	total, succeededAfter, err := repo.WorkflowRunStepCounts(persist, next.WorkflowRunID)
	if err != nil {
		logger.Error("executor: get step counts after success",
			"workflow_run_id", next.WorkflowRunID,
			"error", err,
		)
		return
	}
	if total > 0 && succeededAfter == total {
		if err := repo.MarkWorkflowRunSucceeded(persist, next.WorkflowRunID); err != nil {
			logger.Error("executor: mark workflow run succeeded",
				"workflow_run_id", next.WorkflowRunID,
				"error", err,
			)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			EventID:          newEventID(),
			EventVersion:     1,
			Type:             ports.WorkflowRunCompleted,
			ActorID:          "workflow-engine",
			ProjectID:        projectID,
			ExternalTenantID: externalTenantID,
			RunID:            next.WorkflowRunID,
			WorkflowID:       next.WorkflowID,
			StepIndex:        -1,
			OccurredAt:       time.Now().UTC(),
			Payload:          map[string]any{"total_steps": total},
		}, logger)
	}
}

// publishEvent publishes a workflow lifecycle event best-effort.
// Failures are logged and never propagate — the executor must not fail because
// the event backbone is temporarily unavailable.
func publishEvent(ctx context.Context, publisher ports.EventPublisher, event ports.WorkflowEvent, logger *slog.Logger) {
	if publisher == nil {
		return
	}
	if err := publisher.Publish(ctx, event); err != nil {
		logger.Warn("executor: publish lifecycle event failed (best-effort, continuing)",
			"event_type", event.Type,
			"event_id", event.EventID,
			"run_id", event.RunID,
			"workflow_id", event.WorkflowID,
			"error", err,
		)
	}
}

func executeStep(ctx context.Context, step *ports.PendingStepRun) (output []byte, err error) {
	switch step.StepType {
	case "delay":
		return nil, runDelay(ctx, step.Config)
	case "http_request":
		return runHTTPRequest(ctx, step.Config)
	default:
		return nil, errUnknownStepType{typ: step.StepType}
	}
}

type errUnknownStepType struct {
	typ string
}

func (e errUnknownStepType) Error() string {
	return "unknown step type: " + e.typ
}

type delayConfig struct {
	Seconds int `json:"seconds"`
}

type stepExecutionConfig struct {
	Retry *struct {
		MaxAttempts    int `json:"max_attempts"`
		BackoffSeconds int `json:"backoff_seconds"`
	} `json:"retry"`
}

// parseRetryPolicy returns max attempts (minimum 1) and base backoff seconds (minimum 0)
// for exponential delay: base * 2^(attempt-1) with jitter before each retry.
// Omitted "retry" means a single try (no retries after the first failure).
func parseRetryPolicy(config []byte) (maxAttempts int, backoffSeconds int) {
	maxAttempts = 1
	backoffSeconds = 0
	if len(config) == 0 {
		return maxAttempts, backoffSeconds
	}
	var cfg stepExecutionConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return maxAttempts, backoffSeconds
	}
	if cfg.Retry == nil {
		return maxAttempts, backoffSeconds
	}
	if cfg.Retry.MaxAttempts > 0 {
		maxAttempts = cfg.Retry.MaxAttempts
	}
	backoffSeconds = max(cfg.Retry.BackoffSeconds, 0)
	return maxAttempts, backoffSeconds
}

func runDelay(ctx context.Context, config []byte) error {
	var cfg delayConfig
	if len(config) == 0 {
		config = []byte("{}")
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return err
	}
	if cfg.Seconds < 0 {
		return errInvalidDelaySeconds{}
	}
	d := time.Duration(cfg.Seconds) * time.Second
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type errInvalidDelaySeconds struct{}

func (e errInvalidDelaySeconds) Error() string {
	return "delay seconds must be >= 0"
}
