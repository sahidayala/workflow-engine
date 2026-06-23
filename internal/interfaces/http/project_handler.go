package httpapi

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/auth"
	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db"
)

const maxCreateProjectBody = 1 << 20 // 1 MiB

var slugNonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// ProjectHandler serves HTTP for projects (multi-tenant root entity).
type ProjectHandler struct {
	repo *db.ProjectRepository
}

// NewProjectHandler wires the handler with its dependencies.
func NewProjectHandler(repo *db.ProjectRepository) *ProjectHandler {
	return &ProjectHandler{repo: repo}
}

// CreateProjectRequest is the JSON body for POST /projects.
type CreateProjectRequest struct {
	Name string `json:"name"`
}

// CreateProjectResponse is returned once; api_key is never shown again.
type CreateProjectResponse struct {
	ProjectID string `json:"project_id"`
	APIKey    string `json:"api_key"`
}

// CreateProject handles POST /projects (public): creates project + default API key.
//
//	@Summary		Create project
//	@Description	Creates a project and returns a new API key once.
//	@Tags			projects
//	@Accept			json
//	@Produce		json
//	@Param			body	body		CreateProjectRequest	true	"Project name"
//	@Success		201		{object}	CreateProjectResponse
//	@Failure		400		{object}	JSONError
//	@Failure		409		{object}	JSONError
//	@Failure		500		{object}	JSONError
//	@Router			/projects [post]
func (h *ProjectHandler) CreateProject(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCreateProjectBody)

	var req CreateProjectRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" || len(name) > 200 {
		writeJSONError(w, http.StatusBadRequest, "name is required and must be at most 200 characters")
		return
	}

	slug, err := newProjectSlug(name)
	if err != nil {
		log.Printf("projects: slug: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	rawKey, dbPrefix, err := auth.GenerateAPIKey()
	if err != nil {
		log.Printf("projects: generate key: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	keyHash, err := auth.HashAPIKey(rawKey)
	if err != nil {
		log.Printf("projects: hash key: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	projectID, err := h.repo.CreateProjectWithAPIKey(r.Context(), name, slug, dbPrefix, keyHash)
	if err != nil {
		if db.IsUniqueViolation(err) {
			writeJSONError(w, http.StatusConflict, "resource conflict; try again")
			return
		}
		log.Printf("projects: create: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusCreated, CreateProjectResponse{
		ProjectID: projectID,
		APIKey:    rawKey,
	})
}

// GetCurrentProject handles GET /projects for the authenticated tenant (single project).
//
//	@Summary		Get current project
//	@Description	Returns the project tied to the API key.
//	@Tags			projects
//	@Produce		json
//	@Security		ApiKeyAuth
//	@Success		200	{object}	models.Project
//	@Failure		401	{object}	JSONError
//	@Failure		404	{object}	JSONError
//	@Failure		500	{object}	JSONError
//	@Router			/projects [get]
func (h *ProjectHandler) GetCurrentProject(w http.ResponseWriter, r *http.Request) {
	projectID, ok := auth.ProjectIDFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}
	p, err := h.repo.GetByID(r.Context(), projectID)
	if err != nil {
		if errors.Is(err, db.ErrProjectNotFound) {
			writeJSONError(w, http.StatusNotFound, "project not found")
			return
		}
		log.Printf("projects: get current: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, p)
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

func newProjectSlug(name string) (string, error) {
	base := slugFromName(name)
	if base == "" {
		base = "project"
	}
	var rb [4]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return "", fmt.Errorf("random suffix: %w", err)
	}
	return fmt.Sprintf("%s-%x", base, rb[:]), nil
}
