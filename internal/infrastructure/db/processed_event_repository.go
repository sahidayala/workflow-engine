package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrAlreadyProcessed is returned when a (source_event_id, project_id) pair
// already exists in processed_integration_events.
var ErrAlreadyProcessed = errors.New("processed_event_repository: event already processed for this project")

// ProcessedEventRepository guards against duplicate workflow trigger execution.
// It implements the idempotency layer described in ADR-011.
type ProcessedEventRepository struct {
	db *sql.DB
}

// NewProcessedEventRepository returns a new repository.
func NewProcessedEventRepository(db *sql.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: db}
}

// RecordIfNew attempts to insert a (source_event_id, project_id, workflow_run_id)
// row. Returns ErrAlreadyProcessed if the combination already exists (PK conflict),
// meaning the event was already processed and the workflow run already created.
//
// This is the idempotency guarantee for at-least-once Kafka delivery: calling
// RecordIfNew twice for the same (source_event_id, project_id) pair is safe —
// the second call returns ErrAlreadyProcessed and the caller skips creating a run.
//
// Limitation: this only prevents duplicate runs within a single project. If an
// event matches triggers in multiple projects, each project gets its own run.
func (r *ProcessedEventRepository) RecordIfNew(
	ctx context.Context,
	sourceEventID, projectID, workflowRunID string,
) error {
	const q = `
		INSERT INTO processed_integration_events (source_event_id, project_id, workflow_run_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (source_event_id, project_id) DO NOTHING`

	res, err := r.db.ExecContext(ctx, q, sourceEventID, projectID, workflowRunID)
	if err != nil {
		return fmt.Errorf("processed_event_repository: insert: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("processed_event_repository: rows affected: %w", err)
	}
	if rows == 0 {
		// ON CONFLICT DO NOTHING fired — this event was already processed.
		return ErrAlreadyProcessed
	}
	return nil
}

// WorkflowRunIDForEvent returns the workflow_run_id that was created for the
// given (source_event_id, project_id) pair, or ("", sql.ErrNoRows) if none.
// Useful for diagnostic tooling ("which run came from this Kafka message?").
func (r *ProcessedEventRepository) WorkflowRunIDForEvent(
	ctx context.Context,
	sourceEventID, projectID string,
) (string, error) {
	var runID string
	err := r.db.QueryRowContext(ctx,
		`SELECT workflow_run_id FROM processed_integration_events
		 WHERE source_event_id = $1 AND project_id = $2`,
		sourceEventID, projectID,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("processed_event_repository: lookup: %w", err)
	}
	return runID, nil
}
