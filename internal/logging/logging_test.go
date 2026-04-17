package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/logging"
)

func TestNew_JSONWithService(t *testing.T) {
	var buf bytes.Buffer
	l := logging.New(logging.Options{Level: "debug", Format: "json", Writer: &buf})
	l.Info("hello", "n", 1)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["service"] != "reitbrazil-sync" {
		t.Fatalf("service attr missing: %v", rec)
	}
	if rec["msg"] != "hello" {
		t.Fatalf("msg: %v", rec)
	}
}

func TestWithRunID(t *testing.T) {
	ctx, id := logging.WithRunID(context.Background())
	if id == "" {
		t.Fatal("empty id")
	}
	if got := logging.RunIDFromContext(ctx); got != id {
		t.Fatalf("round-trip: %q vs %q", got, id)
	}
}

func TestAttachRunID_NoOpOnEmpty(t *testing.T) {
	var buf bytes.Buffer
	l := logging.New(logging.Options{Format: "json", Writer: &buf})
	attached := logging.AttachRunID(l, "")
	attached.Info("x")
	if strings.Contains(buf.String(), "run_id") {
		t.Fatal("should not attach empty run_id")
	}
}
