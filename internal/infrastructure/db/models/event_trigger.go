package models

import "time"

// EventTrigger is a row in event_triggers.
// It maps a platform integration event type to a workflow that should be
// automatically triggered when that event arrives for the project's tenant.
type EventTrigger struct {
	ID         string    `db:"id"`
	ProjectID  string    `db:"project_id"`
	EventType  string    `db:"event_type"`
	WorkflowID string    `db:"workflow_id"`
	IsActive   bool      `db:"is_active"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}
