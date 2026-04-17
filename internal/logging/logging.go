// Package logging builds the structured slog.Logger used across the CLI.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

// runIDKey is the context key under which the current run's UUID is stored.
type runIDKey struct{}

// Options controls New.
type Options struct {
	Level   string // debug, info, warn, error (default info)
	Format  string // json or text (default json)
	Service string // slog attribute attached to every record
	Writer  io.Writer
}

// New returns a configured slog.Logger. Default destination is stderr so
// stdout stays clean for data.
func New(opts Options) *slog.Logger {
	w := opts.Writer
	if w == nil {
		w = os.Stderr
	}
	lvl := parseLevel(opts.Level)

	var handler slog.Handler
	handlerOpts := &slog.HandlerOptions{Level: lvl}
	switch strings.ToLower(opts.Format) {
	case "text":
		handler = slog.NewTextHandler(w, handlerOpts)
	case "", "json":
		handler = slog.NewJSONHandler(w, handlerOpts)
	default:
		handler = slog.NewJSONHandler(w, handlerOpts)
	}

	service := opts.Service
	if service == "" {
		service = "reitbrazil-sync"
	}
	return slog.New(handler).With("service", service)
}

// parseLevel turns a text level into slog.Level. Unknown values map to Info.
func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithRunID returns a context carrying a new UUID v4 as the run id.
func WithRunID(ctx context.Context) (context.Context, string) {
	id := uuid.NewString()
	return context.WithValue(ctx, runIDKey{}, id), id
}

// RunIDFromContext reads the run id previously set via WithRunID. Returns
// "" if absent.
func RunIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(runIDKey{}).(string); ok {
		return v
	}
	return ""
}

// AttachRunID returns a slog.Logger that includes run_id=<id> on every record.
func AttachRunID(l *slog.Logger, runID string) *slog.Logger {
	if runID == "" {
		return l
	}
	return l.With("run_id", runID)
}

// Must panics if err != nil. Useful at startup when logging cannot be deferred.
func Must(l *slog.Logger, err error) *slog.Logger {
	if err != nil {
		fmt.Fprintf(os.Stderr, "logging setup failed: %v\n", err)
		os.Exit(1)
	}
	return l
}
