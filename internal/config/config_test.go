package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	clearIngestEnv(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.GCPProject != "reitbrazil" {
		t.Fatalf("default project: %q", cfg.GCPProject)
	}
	if cfg.BQLocation != "southamerica-east1" {
		t.Fatalf("default location: %q", cfg.BQLocation)
	}
	if cfg.RateLimitRPS != 3.0 {
		t.Fatalf("default rps: %v", cfg.RateLimitRPS)
	}
}

func TestLoad_EnvFile(t *testing.T) {
	clearIngestEnv(t)
	tmp := filepath.Join(t.TempDir(), ".env")
	body := "" +
		"# a comment\n" +
		"INGEST_BRAPI_TOKEN=abc123\n" +
		"INGEST_GCP_PROJECT=\"reitbrazil-staging\"\n" +
		"INGEST_RATE_LIMIT_RPS=1.5\n"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BrapiToken != "abc123" {
		t.Fatalf("brapi token: %q", cfg.BrapiToken)
	}
	if cfg.GCPProject != "reitbrazil-staging" {
		t.Fatalf("project: %q", cfg.GCPProject)
	}
	if cfg.RateLimitRPS != 1.5 {
		t.Fatalf("rps: %v", cfg.RateLimitRPS)
	}
}

func TestLoad_EnvWinsOverFile(t *testing.T) {
	clearIngestEnv(t)
	t.Setenv("INGEST_BRAPI_TOKEN", "from-env")
	tmp := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(tmp, []byte("INGEST_BRAPI_TOKEN=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BrapiToken != "from-env" {
		t.Fatalf("env should win, got %q", cfg.BrapiToken)
	}
}

func TestValidateForFetch(t *testing.T) {
	clearIngestEnv(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateForFetch(); err == nil {
		t.Fatal("expected error for missing brapi token")
	}
	cfg.BrapiToken = "x"
	if err := cfg.ValidateForFetch(); err != nil {
		t.Fatal(err)
	}
}

func clearIngestEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"INGEST_BRAPI_TOKEN", "INGEST_GCP_PROJECT", "INGEST_BQ_DATASET_RAW",
		"INGEST_BQ_DATASET_CANON", "INGEST_BQ_LOCATION", "INGEST_GCS_BUCKET",
		"INGEST_GCS_KEY_LATEST", "INGEST_GCS_PREFIX_HISTORY", "INGEST_GCS_KEY_METADATA",
		"INGEST_GITHUB_TOKEN", "INGEST_GITHUB_REPO", "INGEST_LOG_LEVEL",
		"INGEST_LOG_FORMAT", "INGEST_RATE_LIMIT_RPS", "INGEST_HTTP_TIMEOUT",
		"INGEST_MIN_FUND_COUNT", "INGEST_MAX_PRICE_LAG_DAYS",
	} {
		t.Setenv(k, "")
		_ = os.Unsetenv(k)
	}
}
