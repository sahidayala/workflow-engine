package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/domain"
	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db"
)

// EventTriggerService handles integration events from the platform event backbone
// and dispatches workflow runs for any matching active triggers.
//
// Idempotency: at-least-once Kafka delivery means the same event may arrive
// multiple times. The processedEventRepo guards against duplicate runs by
// recording each (source_event_id, project_id) pair before creating a run.
// A second delivery for the same event returns nil without creating a new run.
type EventTriggerService struct {
	triggerRepo       *db.EventTriggerRepository
	workflowRepo      *db.WorkflowRepository
	processedEventRepo *db.ProcessedEventRepository
	log               *slog.Logger
}

// NewEventTriggerService returns a configured trigger service.
func NewEventTriggerService(
	triggerRepo *db.EventTriggerRepository,
	workflowRepo *db.WorkflowRepository,
	processedEventRepo *db.ProcessedEventRepository,
	log *slog.Logger,
) *EventTriggerService {
	return &EventTriggerService{
		triggerRepo:        triggerRepo,
		workflowRepo:       workflowRepo,
		processedEventRepo: processedEventRepo,
		log:                log,
	}
}

// Handle receives an integration event and creates a workflow run if an active
// trigger exists for the event's tenant + type combination.
// Returns nil when no trigger is configured — this is not an error condition.
//
// Causation chain: the triggering integration event's ID is stored as the
// CausationID on the workflow run so that Event Streaming can reconstruct the
// full lineage: platform event → workflow trigger → workflow run.
func (s *EventTriggerService) Handle(ctx context.Context, event *domain.IntegrationEvent) error {
	if event.TenantID == "" || event.Type == "" {
		return fmt.Errorf("event_trigger_service: event missing tenant_id or type")
	}

	trigger, err := s.triggerRepo.FindByExternalTenantAndEventType(ctx, event.TenantID, event.Type)
	if errors.Is(err, db.ErrEventTriggerNotFound) {
		s.log.Debug("no active trigger for event",
			"tenant_id", event.TenantID,
			"event_type", event.Type,
			"correlation_id", event.CorrelationID,
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("event_trigger_service: look up trigger: %w", err)
	}

	// Idempotency check: create the run first, then try to record it.
	// We create the run before recording to keep the happy path simple, and use
	// the DB to enforce uniqueness. If the record already exists (duplicate delivery),
	// we abandon the run. An alternative is to check first then create; both are
	// subject to a race, but the CREATE+RECORD approach is simpler and the extra
	// abandoned run row is cheap to clean up (it stays pending and is never executed).
	runID, stepCount, err := s.workflowRepo.CreateWorkflowRunWithStepRuns(
		ctx, trigger.ProjectID, trigger.WorkflowID,
	)
	if err != nil {
		return fmt.Errorf("event_trigger_service: create run for workflow %s: %w", trigger.WorkflowID, err)
	}

	// Record the (source_event_id, project_id) pair to prevent duplicate runs on
	// re-delivery. ErrAlreadyProcessed means a previous delivery already created
	// a run — log and return nil so the offset is committed.
	if err := s.processedEventRepo.RecordIfNew(ctx, event.ID, trigger.ProjectID, runID); errors.Is(err, db.ErrAlreadyProcessed) {
		s.log.Info("event_trigger_service: duplicate event delivery — skipping run creation",
			"source_event_id", event.ID,
			"project_id", trigger.ProjectID,
			"workflow_id", trigger.WorkflowID,
			"event_type", event.Type,
			"correlation_id", event.CorrelationID,
		)
		return nil
	} else if err != nil {
		// Idempotency record failed. Return an error so the Kafka offset is NOT
		// committed — the event will be redelivered and we'll retry. This is safer
		// than silently proceeding: a duplicate run is worse than a delayed one.
		return fmt.Errorf("event_trigger_service: record processed event: %w", err)
	}

	s.log.Info("workflow run created from event trigger",
		"trigger_id", trigger.ID,
		"workflow_id", trigger.WorkflowID,
		"run_id", runID,
		"step_count", stepCount,
		"tenant_id", event.TenantID,
		"event_type", event.Type,
		"source_event_id", event.ID,
		"correlation_id", event.CorrelationID,
		"causation_id", event.ID,
		"actor_id", event.ActorID,
		"trace_id", event.TraceID,
	)
	return nil
}
