// Package models holds row shapes that mirror PostgreSQL tables. They belong in
// infrastructure so the domain layer stays free of persistence details; map to
// or from domain types in repositories.
package models

import (
	"encoding/json"
	"time"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/domain"
)

// UUID columns are represented as strings for broad driver compatibility (pgx, lib/pq).

// Project is a row in projects.
type Project struct {
	ID        string    `db:"id"`
	Name      string    `db:"name"`
	Slug      string    `db:"slug"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// APIKey is a row in api_keys.
type APIKey struct {
	ID         string     `db:"id"`
	ProjectID  string     `db:"project_id"`
	Name       string     `db:"name"`
	KeyHash    string     `db:"key_hash"`
	KeyPrefix  *string    `db:"key_prefix"`
	CreatedAt  time.Time  `db:"created_at"`
	LastUsedAt *time.Time `db:"last_used_at"`
	RevokedAt  *time.Time `db:"revoked_at"`
}

// Workflow is a row in workflows.
type Workflow struct {
	ID          string    `db:"id"`
	ProjectID   string    `db:"project_id"`
	Name        string    `db:"name"`
	Slug        string    `db:"slug"`
	Description *string   `db:"description"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

// WorkflowStep is a row in workflow_steps.
type WorkflowStep struct {
	ID         string          `db:"id"`
	WorkflowID string          `db:"workflow_id"`
	StepIndex  int             `db:"step_index"`
	Name       string          `db:"name"`
	StepType   string          `db:"step_type"`
	Config     json.RawMessage `db:"config"`
	CreatedAt  time.Time       `db:"created_at"`
	UpdatedAt  time.Time       `db:"updated_at"`
}

// WorkflowRun is a row in workflow_runs.
type WorkflowRun struct {
	ID            string                `db:"id"`
	WorkflowID    string                `db:"workflow_id"`
	Status        domain.WorkflowStatus `db:"status"`
	Input         json.RawMessage       `db:"input"`
	Output        *json.RawMessage      `db:"output"`
	ErrorMessage  *string               `db:"error_message"`
	CorrelationID *string               `db:"correlation_id"`
	CreatedAt     time.Time             `db:"created_at"`
	StartedAt     *time.Time            `db:"started_at"`
	CompletedAt   *time.Time            `db:"completed_at"`
}

// StepRun is a row in step_runs.
type StepRun struct {
	ID             string               `db:"id"`
	WorkflowRunID  string               `db:"workflow_run_id"`
	WorkflowStepID string               `db:"workflow_step_id"`
	Attempt        int                  `db:"attempt"`
	Status         domain.StepRunStatus `db:"status"`
	Input          json.RawMessage      `db:"input"`
	Output         *json.RawMessage     `db:"output"`
	ErrorMessage   *string              `db:"error_message"`
	CreatedAt      time.Time            `db:"created_at"`
	StartedAt      *time.Time           `db:"started_at"`
	CompletedAt    *time.Time           `db:"completed_at"`
}
