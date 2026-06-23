package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db/models"
)

// ProjectRepository loads and persists projects using database/sql.
type ProjectRepository struct {
	db *sql.DB
}

// ErrProjectNotFound is returned when no row matches the id.
var ErrProjectNotFound = errors.New("project_repository: not found")

// NewProjectRepository returns a repository backed by db.
func NewProjectRepository(db *sql.DB) *ProjectRepository {
	return &ProjectRepository{db: db}
}

// CreateProjectWithAPIKey inserts a project and its initial API key in one transaction.
func (r *ProjectRepository) CreateProjectWithAPIKey(ctx context.Context, name, slug, keyPrefix, keyHash string) (projectID string, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("project_repository: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insertProject = `
		INSERT INTO projects (name, slug)
		VALUES ($1, $2)
		RETURNING id
	`
	if err := tx.QueryRowContext(ctx, insertProject, name, slug).Scan(&projectID); err != nil {
		return "", fmt.Errorf("project_repository: insert project: %w", err)
	}

	const insertKey = `
		INSERT INTO api_keys (project_id, name, key_prefix, key_hash)
		VALUES ($1, $2, $3, $4)
	`
	if _, err := tx.ExecContext(ctx, insertKey, projectID, "default", keyPrefix, keyHash); err != nil {
		return "", fmt.Errorf("project_repository: insert api key: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("project_repository: commit: %w", err)
	}
	return projectID, nil
}

// GetByID returns one project by primary key.
func (r *ProjectRepository) GetByID(ctx context.Context, id string) (models.Project, error) {
	const q = `
		SELECT id, name, slug, created_at, updated_at
		FROM projects
		WHERE id = $1
	`
	var p models.Project
	err := r.db.QueryRowContext(ctx, q, id).Scan(
		&p.ID, &p.Name, &p.Slug, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Project{}, ErrProjectNotFound
		}
		return models.Project{}, fmt.Errorf("project_repository: get by id: %w", err)
	}
	return p, nil
}
