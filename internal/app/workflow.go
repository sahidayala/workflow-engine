package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db"
	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db/models"
)

// ValidationError is returned for bad input (maps to HTTP 400).
type ValidationError struct {
	Msg string
}

func (e *ValidationError) Error() string { return e.Msg }

// WorkflowService orchestrates workflow definition use cases.
type WorkflowService struct {
	repo *db.WorkflowRepository
}

// NewWorkflowService returns a service backed by repo.
func NewWorkflowService(repo *db.WorkflowRepository) *WorkflowService {
	return &WorkflowService{repo: repo}
}

// ListByProject returns workflow definitions for a tenant.
func (s *WorkflowService) ListByProject(ctx context.Context, projectID string) ([]models.Workflow, error) {
	return s.repo.GetAllByProjectID(ctx, projectID)
}

// CreateWorkflowInput is validated application input (not the HTTP DTO).
type CreateWorkflowInput struct {
	Name  string
	Steps []CreateWorkflowStepInput
}

// CreateWorkflowStepInput is one step after HTTP mapping.
type CreateWorkflowStepInput struct {
	Name     string
	StepType string
	Config   []byte
}

// CreateWorkflowResult is returned after a successful create.
type CreateWorkflowResult struct {
	ID         string
	Name       string
	StepsCount int
}

var slugNonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// CreateWorkflow validates input and persists workflow + steps in one transaction.
func (s *WorkflowService) CreateWorkflow(ctx context.Context, projectID string, in CreateWorkflowInput) (CreateWorkflowResult, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return CreateWorkflowResult{}, &ValidationError{Msg: "name is required"}
	}
	if len(name) > 200 {
		return CreateWorkflowResult{}, &ValidationError{Msg: "name must be at most 200 characters"}
	}
	if len(in.Steps) == 0 {
		return CreateWorkflowResult{}, &ValidationError{Msg: "steps must not be empty"}
	}

	seen := make(map[string]struct{}, len(in.Steps))
	for _, st := range in.Steps {
		sn := strings.TrimSpace(st.Name)
		if sn == "" {
			return CreateWorkflowResult{}, &ValidationError{Msg: "each step must have a non-empty name"}
		}
		if _, dup := seen[sn]; dup {
			return CreateWorkflowResult{}, &ValidationError{Msg: "step names must be unique within the workflow"}
		}
		seen[sn] = struct{}{}
		stt := strings.TrimSpace(st.StepType)
		if stt == "" {
			return CreateWorkflowResult{}, &ValidationError{Msg: "each step must have a non-empty type"}
		}
	}

	slug, err := newWorkflowSlug(name)
	if err != nil {
		return CreateWorkflowResult{}, fmt.Errorf("workflow slug: %w", err)
	}

	steps := make([]db.WorkflowStepInsert, 0, len(in.Steps))
	for _, st := range in.Steps {
		cfg := st.Config
		if len(cfg) == 0 {
			cfg = []byte("{}")
		}
		if !json.Valid(cfg) {
			return CreateWorkflowResult{}, &ValidationError{Msg: "each step config must be valid JSON"}
		}
		steps = append(steps, db.WorkflowStepInsert{
			Name:     strings.TrimSpace(st.Name),
			StepType: strings.TrimSpace(st.StepType),
			Config:   cfg,
		})
	}

	id, err := s.repo.CreateWithSteps(ctx, projectID, name, slug, steps)
	if err != nil {
		if db.IsUniqueViolation(err) {
			return CreateWorkflowResult{}, &ValidationError{Msg: "workflow slug conflict; try a different name"}
		}
		return CreateWorkflowResult{}, err
	}

	return CreateWorkflowResult{
		ID:         id,
		Name:       name,
		StepsCount: len(steps),
	}, nil
}

// StartWorkflowRunResult is returned after starting a workflow run.
type StartWorkflowRunResult struct {
	RunID  string
	Status string
	Steps  int
}

// StartWorkflowRun creates a pending workflow_run and pending step_runs for each workflow step.
func (s *WorkflowService) StartWorkflowRun(ctx context.Context, projectID, workflowID string) (StartWorkflowRunResult, error) {
	wfID := strings.TrimSpace(workflowID)
	if wfID == "" {
		return StartWorkflowRunResult{}, &ValidationError{Msg: "workflow id is required"}
	}

	runID, n, err := s.repo.CreateWorkflowRunWithStepRuns(ctx, projectID, wfID)
	if errors.Is(err, db.ErrWorkflowNotFound) {
		return StartWorkflowRunResult{}, err
	}
	if err != nil {
		return StartWorkflowRunResult{}, err
	}

	return StartWorkflowRunResult{
		RunID:  runID,
		Status: "pending",
		Steps:  n,
	}, nil
}

func newWorkflowSlug(name string) (string, error) {
	base := slugFromName(name)
	if base == "" {
		base = "workflow"
	}
	var rb [4]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(rb[:])), nil
}

func slugFromName(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.ReplaceAll(s, "_", "-")
	s = slugNonAlnum.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if len(s) > 48 {
		s = strings.TrimSuffix(s[:48], "-")
	}
	return s
}

// ListRunsByProject returns all runs for a project.
func (s *WorkflowService) ListRunsByProject(ctx context.Context, projectID string) ([]db.RunListItem, error) {
	return s.repo.ListRunsByProjectID(ctx, projectID)
}

// GetRunDetail returns a single run with its step runs.
func (s *WorkflowService) GetRunDetail(ctx context.Context, projectID, runID string) (*db.RunDetail, error) {
	return s.repo.GetRunWithSteps(ctx, projectID, runID)
}

// AsValidation returns (msg, true) if err is or wraps ValidationError.
func AsValidation(err error) (string, bool) {
	var v *ValidationError
	if errors.As(err, &v) {
		return v.Msg, true
	}
	return "", false
}
