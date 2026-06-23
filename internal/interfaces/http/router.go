package httpapi

import (
	"net/http"
	"os"
	"strings"

	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/auth"
	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/infrastructure/db"
)

// NewRouter returns the HTTP handler tree for the API. It uses net/http only
// (no external router). Register routes on a ServeMux and return it as http.Handler.
func NewRouter(apiKeys *db.APIKeyRepository, workflows *WorkflowHandler, projects *ProjectHandler) http.Handler {
	protected := http.NewServeMux()
	protected.HandleFunc("GET /workflows/runs", workflows.ListAllRuns)
	protected.HandleFunc("GET /workflows/runs/{id}", workflows.GetRun)
	protected.HandleFunc("GET /workflows", workflows.GetAllWorkflows)
	protected.HandleFunc("POST /workflows", workflows.CreateWorkflow)
	protected.HandleFunc("POST /workflows/{id}/runs", workflows.CreateWorkflowRun)
	protected.HandleFunc("GET /projects", projects.GetCurrentProject)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("POST /projects", projects.CreateProject)

	// Swagger UI is disabled by default. Set ENABLE_SWAGGER=true to enable.
	if os.Getenv("ENABLE_SWAGGER") == "true" {
		mux.Handle("/swagger/", httpSwagger.WrapHandler)
	}

	mux.Handle("/", auth.APIKeyMiddleware(apiKeys)(protected))

	allowedOrigins := buildAllowedOrigins()
	return corsMiddleware(mux, allowedOrigins)
}

// buildAllowedOrigins reads ALLOWED_ORIGINS (comma-separated). Defaults to
// http://localhost:5173 for local development. Set ALLOWED_ORIGINS=* only
// when explicitly needed — it re-enables wildcard CORS.
func buildAllowedOrigins() map[string]bool {
	raw := os.Getenv("ALLOWED_ORIGINS")
	if raw == "" {
		raw = "http://localhost:5173"
	}
	origins := make(map[string]bool)
	for _, o := range strings.Split(raw, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins[o] = true
		}
	}
	return origins
}

// corsMiddleware sets CORS headers only for allowed origins.
func corsMiddleware(next http.Handler, allowed map[string]bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (allowed["*"] || allowed[origin]) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// health liveness probe.
//
//	@Summary		Health check
//	@Description	Returns plain text ok when the process is up.
//	@Tags			system
//	@Produce		plain
//	@Success		200	{string}	string	"ok"
//	@Router			/health [get]
func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
