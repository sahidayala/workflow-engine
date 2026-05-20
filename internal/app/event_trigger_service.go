package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SheykoWk/workflow-engine/internal/domain"
	"github.com/SheykoWk/workflow-engine/internal/infrastructure/db"
)

// EventTriggerService handles integration events from the platform event backbone
// and dispatches workflow runs for any matching active triggers.
type EventTriggerService struct {
	triggerRepo  *db.EventTriggerRepository
	workflowRepo *db.WorkflowRepository
	log          *slog.Logger
}

// NewEventTriggerService returns a configured trigger service.
func NewEventTriggerService(
	triggerRepo *db.EventTriggerRepository,
	workflowRepo *db.WorkflowRepository,
	log *slog.Logger,
) *EventTriggerService {
	return &EventTriggerService{
		triggerRepo:  triggerRepo,
		workflowRepo: workflowRepo,
		log:          log,
	}
}

// Handle receives an integration event and creates a workflow run if an active
// trigger exists for the event's tenant + type combination.
// Returns nil when no trigger is configured — this is not an error condition.
func (s *EventTriggerService) Handle(ctx context.Context, event *domain.IntegrationEvent) error {
	if event.TenantID == "" || event.Type == "" {
		return fmt.Errorf("event_trigger_service: event missing tenant_id or type")
	}

	trigger, err := s.triggerRepo.FindByExternalTenantAndEventType(ctx, event.TenantID, event.Type)
	if errors.Is(err, db.ErrEventTriggerNotFound) {
		s.log.Debug("no active trigger for event",
			"tenant_id", event.TenantID,
			"event_type", event.Type,
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("event_trigger_service: look up trigger: %w", err)
	}

	runID, stepCount, err := s.workflowRepo.CreateWorkflowRunWithStepRuns(
		ctx, trigger.ProjectID, trigger.WorkflowID,
	)
	if err != nil {
		return fmt.Errorf("event_trigger_service: create run for workflow %s: %w", trigger.WorkflowID, err)
	}

	s.log.Info("workflow run created from event trigger",
		"trigger_id", trigger.ID,
		"workflow_id", trigger.WorkflowID,
		"run_id", runID,
		"step_count", stepCount,
		"tenant_id", event.TenantID,
		"event_type", event.Type,
		"correlation_id", event.CorrelationID,
	)
	return nil
}
