// Package trace provides lightweight W3C TraceContext propagation for the
// Workflow Engine. It is intentionally minimal — only the pieces needed for
// Kafka header extraction and structured log enrichment (ADR-014).
//
// When the OTel SDK is adopted, replace this shim with:
//
//	propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(headers))
package trace

import (
	"context"
	"encoding/hex"
	"strings"
)

type contextKey struct{}

// WithTraceID returns a child context carrying the given trace ID string.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, contextKey{}, traceID)
}

// FromContext returns the trace ID stored by WithTraceID, or empty string.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}

// ParseTraceID extracts the 32-hex-char trace ID from a W3C traceparent header.
// Returns empty string if the header is absent or malformed.
//
//	Format: 00-{traceId32hex}-{parentSpanId16hex}-{flags2hex}
func ParseTraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return ""
	}
	traceID := parts[1]
	if len(traceID) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(traceID); err != nil {
		return ""
	}
	return traceID
}
