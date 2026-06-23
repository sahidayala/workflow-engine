package httpapi

import (
	"net/http"

	"github.com/SahidAyala/Nocturn-Atlas-Workflow-Engine/internal/interfaces/http/respond"
)

// JSONError is the JSON body for error responses (e.g. 500).
// Used by Swagger / OpenAPI annotations.
type JSONError struct {
	Error string `json:"error" example:"internal server error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	respond.JSON(w, status, v)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	respond.Error(w, code, msg)
}
