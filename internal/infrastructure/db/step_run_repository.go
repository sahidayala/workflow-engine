package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/SheykoWk/workflow-engine/internal/app/ports"
)

// Compile-time contract: StepRunRepository must satisfy ports.StepRunRepository.
var _ ports.StepRunRepository = (*StepRunRepository)(nil)

// StepRunRepository updates step_runs for the in-process executor.
type StepRunRepository struct {
	db *sql.DB
}

// NewStepRunRepository returns a repository backed by db.
func NewStepRunRepository(db *sql.DB) *StepRunRepository {
	return &StepRunRepository{db: db}
}

// GetNextPendingStepRun returns one pending step_run that may run next:
// - no other step in the same workflow_run is running
// - every step with a lower step_index in that run has status succeeded
// - next_run_at is null or not in the future (backoff elapsed)
// - global order: oldest step_run first among eligible rows
//
// FOR UPDATE OF sr SKIP LOCKED prevents concurrent executor instances from
// racing on the same row. Any row already locked by another transaction is
// skipped, so each executor claims a distinct step. The lock is held for the
// duration of the QueryRowContext call; MarkStepRunRunning provides the
// definitive atomic status check (double-check locking pattern).
// Returns (nil, nil) when there is no work.
func (r *StepRunRepository) GetNextPendingStepRun(ctx context.Context) (*ports.PendingStepRun, error) {
	const q = `
		SELECT
			sr.id,
			sr.workflow_run_id,
			ws.workflow_id,
			sr.workflow_step_id,
			ws.step_index,
			sr.attempt,
			ws.step_type,
			ws.config
		FROM step_runs sr
		INNER JOIN workflow_steps ws ON ws.id = sr.workflow_step_id
		WHERE sr.status = 'pending'
		AND (sr.next_run_at IS NULL OR sr.next_run_at <= now())
		AND NOT EXISTS (
			SELECT 1
			FROM step_runs sr_other
			WHERE sr_other.workflow_run_id = sr.workflow_run_id
			AND sr_other.status = 'running'
		)
		AND NOT EXISTS (
			SELECT 1
			FROM step_runs sr_prev
			INNER JOIN workflow_steps ws_prev ON ws_prev.id = sr_prev.workflow_step_id
			WHERE sr_prev.workflow_run_id = sr.workflow_run_id
			AND ws_prev.step_index < ws.step_index
			AND sr_prev.status <> 'succeeded'
		)
		ORDER BY sr.created_at ASC
		LIMIT 1
		FOR UPDATE OF sr SKIP LOCKED
	`
	var p ports.PendingStepRun
	err := r.db.QueryRowContext(ctx, q).Scan(
		&p.ID,
		&p.WorkflowRunID,
		&p.WorkflowID,
		&p.WorkflowStepID,
		&p.StepIndex,
		&p.Attempt,
		&p.StepType,
		&p.Config,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("step_run_repository: get next pending: %w", err)
	}
	return &p, nil
}

// GetPreviousStepOutputs returns succeeded step outputs for a workflow run (for config templating).
// Each row is keyed by workflow_steps.step_index; output is JSON (at least {}).
func (r *StepRunRepository) GetPreviousStepOutputs(ctx context.Context, workflowRunID string) ([]ports.StepIndexOutput, error) {
	const q = `
		SELECT ws.step_index, COALESCE(sr.output, '{}'::jsonb)
		FROM step_runs sr
		INNER JOIN workflow_steps ws ON ws.id = sr.workflow_step_id
		WHERE sr.workflow_run_id = $1
		AND sr.status = 'succeeded'
		ORDER BY ws.step_index ASC
	`
	rows, err := r.db.QueryContext(ctx, q, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("step_run_repository: get previous step outputs: %w", err)
	}
	defer rows.Close()

	var list []ports.StepIndexOutput
	for rows.Next() {
		var o ports.StepIndexOutput
		if err := rows.Scan(&o.StepIndex, &o.Output); err != nil {
			return nil, fmt.Errorf("step_run_repository: scan step output: %w", err)
		}
		list = append(list, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("step_run_repository: iterate step outputs: %w", err)
	}
	return list, nil
}

// MarkStepRunRunning sets status to running if the row is still pending.
func (r *StepRunRepository) MarkStepRunRunning(ctx context.Context, id string) error {
	const q = `
		UPDATE step_runs
		SET status = 'running', started_at = now(), next_run_at = NULL
		WHERE id = $1 AND status = 'pending'
	`
	res, err := r.db.ExecContext(ctx, q, id)
	if err != nil {
		return fmt.Errorf("step_run_repository: mark running: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: mark running: not pending")
	}
	return nil
}

// MarkStepRunSucceeded sets status to succeeded when the row is running.
// outputJSON is optional JSON written to step_runs.output (nil leaves the column unchanged).
func (r *StepRunRepository) MarkStepRunSucceeded(ctx context.Context, id string, outputJSON []byte) error {
	const q = `
		UPDATE step_runs
		SET
			status = 'succeeded',
			completed_at = now(),
			next_run_at = NULL,
			output = COALESCE($2::jsonb, output)
		WHERE id = $1 AND status = 'running'
	`
	res, err := r.db.ExecContext(ctx, q, id, nullJSONBytes(outputJSON))
	if err != nil {
		return fmt.Errorf("step_run_repository: mark succeeded: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: mark succeeded: not running")
	}
	return nil
}

// MarkStepRunFailed sets status to failed when the row is running.
// outputJSON is optional JSON written to step_runs.output (nil leaves the column unchanged).
func (r *StepRunRepository) MarkStepRunFailed(ctx context.Context, id string, outputJSON []byte) error {
	const q = `
		UPDATE step_runs
		SET
			status = 'failed',
			completed_at = now(),
			output = COALESCE($2::jsonb, output)
		WHERE id = $1 AND status = 'running'
	`
	res, err := r.db.ExecContext(ctx, q, id, nullJSONBytes(outputJSON))
	if err != nil {
		return fmt.Errorf("step_run_repository: mark failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: mark failed: not running")
	}
	return nil
}

// RetryStepRun schedules another attempt: pending, bumped attempt, and next_run_at for fixed backoff.
func (r *StepRunRepository) RetryStepRun(ctx context.Context, id string, nextRunAt time.Time, attempt int) error {
	const q = `
		UPDATE step_runs
		SET
			status = 'pending',
			attempt = $2,
			next_run_at = $3,
			started_at = NULL,
			completed_at = NULL,
			error_message = NULL,
			output = NULL
		WHERE id = $1 AND status = 'running'
	`
	res, err := r.db.ExecContext(ctx, q, id, attempt, nextRunAt)
	if err != nil {
		return fmt.Errorf("step_run_repository: retry step run: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: retry step run: not running")
	}
	return nil
}

// nullJSONBytes returns nil for driver NULL when output should be omitted; otherwise raw JSON bytes.
func nullJSONBytes(outputJSON []byte) any {
	if len(outputJSON) == 0 {
		return nil
	}
	return outputJSON
}

// IsWorkflowRunPending reports whether workflow_runs.status is still pending (first execution not committed yet).
func (r *StepRunRepository) IsWorkflowRunPending(ctx context.Context, workflowRunID string) (bool, error) {
	const q = `SELECT status FROM workflow_runs WHERE id = $1`
	var status string
	err := r.db.QueryRowContext(ctx, q, workflowRunID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("step_run_repository: workflow run not found")
	}
	if err != nil {
		return false, fmt.Errorf("step_run_repository: workflow run status: %w", err)
	}
	return status == "pending", nil
}

// WorkflowRunStepCounts returns total step_runs for the workflow run and how many are succeeded.
func (r *StepRunRepository) WorkflowRunStepCounts(ctx context.Context, workflowRunID string) (total int, succeeded int, err error) {
	const q = `
		SELECT
			COUNT(*)::int,
			COUNT(*) FILTER (WHERE status = 'succeeded')::int
		FROM step_runs
		WHERE workflow_run_id = $1
	`
	err = r.db.QueryRowContext(ctx, q, workflowRunID).Scan(&total, &succeeded)
	if err != nil {
		return 0, 0, fmt.Errorf("step_run_repository: workflow run step counts: %w", err)
	}
	return total, succeeded, nil
}

// MarkWorkflowRunRunning sets workflow_run to running when it is still pending (first step start).
func (r *StepRunRepository) MarkWorkflowRunRunning(ctx context.Context, workflowRunID string) error {
	const q = `
		UPDATE workflow_runs
		SET status = 'running', started_at = COALESCE(started_at, now())
		WHERE id = $1 AND status = 'pending'
	`
	res, err := r.db.ExecContext(ctx, q, workflowRunID)
	if err != nil {
		return fmt.Errorf("step_run_repository: mark workflow run running: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: mark workflow run running: not pending")
	}
	return nil
}

// MarkWorkflowRunSucceeded sets workflow_run to succeeded when it is running.
func (r *StepRunRepository) MarkWorkflowRunSucceeded(ctx context.Context, workflowRunID string) error {
	const q = `
		UPDATE workflow_runs
		SET status = 'succeeded', completed_at = now()
		WHERE id = $1 AND status = 'running'
	`
	res, err := r.db.ExecContext(ctx, q, workflowRunID)
	if err != nil {
		return fmt.Errorf("step_run_repository: mark workflow run succeeded: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: mark workflow run succeeded: not running")
	}
	return nil
}

// MarkWorkflowRunFailed sets workflow_run to failed when it is pending or running.
func (r *StepRunRepository) MarkWorkflowRunFailed(ctx context.Context, workflowRunID string) error {
	const q = `
		UPDATE workflow_runs
		SET status = 'failed', completed_at = now()
		WHERE id = $1 AND status IN ('pending', 'running')
	`
	res, err := r.db.ExecContext(ctx, q, workflowRunID)
	if err != nil {
		return fmt.Errorf("step_run_repository: mark workflow run failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("step_run_repository: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("step_run_repository: mark workflow run failed: not pending or running")
	}
	return nil
}

// GetProjectContext returns the project_id and external_tenant_id (may be empty) for the
// project that owns the given workflow run. Used by the executor to enrich lifecycle events.
func (r *StepRunRepository) GetProjectContext(ctx context.Context, workflowRunID string) (projectID, externalTenantID string, err error) {
	const q = `
		SELECT p.id, COALESCE(p.external_tenant_id, '')
		FROM workflow_runs wr
		JOIN workflows w ON w.id = wr.workflow_id
		JOIN projects p  ON p.id = w.project_id
		WHERE wr.id = $1
	`
	var extTenant sql.NullString
	if scanErr := r.db.QueryRowContext(ctx, q, workflowRunID).Scan(&projectID, &extTenant); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("step_run_repository: get project context: %w", scanErr)
	}
	if extTenant.Valid {
		externalTenantID = extTenant.String
	}
	return projectID, externalTenantID, nil
}
