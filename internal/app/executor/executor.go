package executor

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"time"

	"github.com/SheykoWk/workflow-engine/internal/app/ports"
)

const pollInterval = 1 * time.Second

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
		log.Printf("executor: get next pending: %v", err)
		return
	}
	if next == nil {
		return
	}

	persist := context.Background()

	projectID, externalTenantID, err := repo.GetProjectContext(persist, next.WorkflowRunID)
	if err != nil {
		log.Printf("executor: get project context %s: %v", next.WorkflowRunID, err)
		// Non-fatal: proceed without tenant context
	}

	runPending, err := repo.IsWorkflowRunPending(ctx, next.WorkflowRunID)
	if err != nil {
		log.Printf("executor: workflow run pending check %s: %v", next.WorkflowRunID, err)
		return
	}
	firstStep := runPending

	prevOuts, err := repo.GetPreviousStepOutputs(ctx, next.WorkflowRunID)
	if err != nil {
		log.Printf("executor: previous outputs %s: %v", next.WorkflowRunID, err)
		return
	}
	resolvedConfig, err := interpolateStepConfig(next.Config, prevOuts, next.StepIndex)
	if err != nil {
		log.Printf("executor: interpolate %s: %v", next.ID, err)
		return
	}
	stepToRun := *next
	stepToRun.Config = resolvedConfig

	if err := repo.MarkStepRunRunning(ctx, next.ID); err != nil {
		log.Printf("executor: mark running %s: %v", next.ID, err)
		return
	}
	publishEvent(ctx, publisher, ports.WorkflowEvent{
		Type:             ports.StepRunStarted,
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
			log.Printf("executor: mark workflow run running %s: %v", next.WorkflowRunID, err)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			Type:             ports.WorkflowRunStarted,
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
		log.Printf("executor: step %s (%s): %v", next.ID, next.StepType, err)
		maxAttempts, baseBackoffSec := parseRetryPolicy(next.Config)
		if next.Attempt < maxAttempts && !isNonRetriable(err) {
			delay := retryDelayWithJitter(baseBackoffSec, next.Attempt)
			nextAt := time.Now().UTC().Add(delay)
			if err := repo.RetryStepRun(persist, next.ID, nextAt, next.Attempt+1); err != nil {
				log.Printf("executor: retry schedule %s: %v", next.ID, err)
			}
			return
		}
		if err := repo.MarkStepRunFailed(persist, next.ID, out); err != nil {
			log.Printf("executor: mark failed %s: %v", next.ID, err)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			Type:             ports.StepRunFailed,
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
			log.Printf("executor: mark workflow run failed %s: %v", next.WorkflowRunID, err)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			Type:             ports.WorkflowRunFailed,
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
		log.Printf("executor: mark succeeded %s: %v", next.ID, err)
		return
	}
	publishEvent(persist, publisher, ports.WorkflowEvent{
		Type:             ports.StepRunSucceeded,
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
		log.Printf("executor: step counts after success %s: %v", next.WorkflowRunID, err)
		return
	}
	if total > 0 && succeededAfter == total {
		if err := repo.MarkWorkflowRunSucceeded(persist, next.WorkflowRunID); err != nil {
			log.Printf("executor: mark workflow run succeeded %s: %v", next.WorkflowRunID, err)
		}
		publishEvent(persist, publisher, ports.WorkflowEvent{
			Type:             ports.WorkflowRunCompleted,
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
		if logger != nil {
			logger.Warn("executor: publish event failed (best-effort, continuing)",
				"event_type", event.Type,
				"run_id", event.RunID,
				"error", err,
			)
		} else {
			log.Printf("executor: publish event %s failed: %v", event.Type, err)
		}
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
	backoffSeconds = cfg.Retry.BackoffSeconds
	if backoffSeconds < 0 {
		backoffSeconds = 0
	}
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
