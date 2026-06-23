package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db/models"
)

// ErrEventTriggerNotFound is returned when no trigger matches the query.
var ErrEventTriggerNotFound = errors.New("event_trigger_repository: not found")

// EventTriggerRepository loads and persists event trigger mappings.
type EventTriggerRepository struct {
	db *sql.DB
}

// NewEventTriggerRepository returns a repository backed by db.
func NewEventTriggerRepository(db *sql.DB) *EventTriggerRepository {
	return &EventTriggerRepository{db: db}
}

// FindByExternalTenantAndEventType returns the active trigger for a given
// external tenant ID + event type combination, or ErrEventTriggerNotFound.
func (r *EventTriggerRepository) FindByExternalTenantAndEventType(
	ctx context.Context,
	externalTenantID, eventType string,
) (*models.EventTrigger, error) {
	const q = `
		SELECT et.id, et.project_id, et.event_type, et.workflow_id, et.is_active,
		       et.created_at, et.updated_at
		FROM   event_triggers et
		JOIN   projects p ON p.id = et.project_id
		WHERE  p.external_tenant_id = $1
		AND    et.event_type = $2
		AND    et.is_active = TRUE
		LIMIT  1
	`
	var t models.EventTrigger
	err := r.db.QueryRowContext(ctx, q, externalTenantID, eventType).Scan(
		&t.ID, &t.ProjectID, &t.EventType, &t.WorkflowID,
		&t.IsActive, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEventTriggerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("event_trigger_repository: query: %w", err)
	}
	return &t, nil
}

// Create inserts a new event trigger.
func (r *EventTriggerRepository) Create(ctx context.Context, projectID, eventType, workflowID string) (string, error) {
	const q = `
		INSERT INTO event_triggers (project_id, event_type, workflow_id)
		VALUES ($1, $2, $3)
		RETURNING id
	`
	var id string
	if err := r.db.QueryRowContext(ctx, q, projectID, eventType, workflowID).Scan(&id); err != nil {
		return "", fmt.Errorf("event_trigger_repository: insert: %w", err)
	}
	return id, nil
}

// Deactivate sets is_active=false for the given trigger.
func (r *EventTriggerRepository) Deactivate(ctx context.Context, triggerID string) error {
	const q = `UPDATE event_triggers SET is_active = FALSE, updated_at = now() WHERE id = $1`
	if _, err := r.db.ExecContext(ctx, q, triggerID); err != nil {
		return fmt.Errorf("event_trigger_repository: deactivate: %w", err)
	}
	return nil
}
