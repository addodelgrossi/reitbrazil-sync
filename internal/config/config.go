// Package config loads runtime configuration from environment variables,
// an optional .env file, and an optional YAML file. Environment takes
// precedence over files. Every key is prefixed with INGEST_ (except
// GOOGLE_APPLICATION_CREDENTIALS, which follows the GCP convention).
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config is the fully-resolved runtime configuration.
type Config struct {
	BrapiToken string

	GCPProject      string
	BQDatasetRaw    string
	BQDatasetCanon  string
	BQLocation      string
	GoogleCredsPath string

	GCSBucket          string
	GCSKeyLatest       string
	GCSPrefixHistory   string
	GCSKeyMetadata     string

	GitHubToken string
	GitHubRepo  string

	LogLevel    string
	LogFormat   string
	RateLimitRPS float64
	HTTPTimeout time.Duration

	MinFundCount     int
	MaxPriceLagDays  int
}

// Load resolves configuration. Files named as envFiles are parsed in
// order (later wins on conflict), but environment variables always win
// over file-provided values.
func Load(envFiles ...string) (*Config, error) {
	for _, path := range envFiles {
		if path == "" {
			continue
		}
		if err := loadEnvFile(path); err != nil {
			return nil, fmt.Errorf("load env file %s: %w", path, err)
		}
	}

	cfg := &Config{
		BrapiToken:       os.Getenv("INGEST_BRAPI_TOKEN"),
		GCPProject:       firstNonEmpty(os.Getenv("INGEST_GCP_PROJECT"), "reitbrazil"),
		BQDatasetRaw:     firstNonEmpty(os.Getenv("INGEST_BQ_DATASET_RAW"), "reitbrazil_raw"),
		BQDatasetCanon:   firstNonEmpty(os.Getenv("INGEST_BQ_DATASET_CANON"), "reitbrazil_canon"),
		BQLocation:       firstNonEmpty(os.Getenv("INGEST_BQ_LOCATION"), "southamerica-east1"),
		GoogleCredsPath:  os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		GCSBucket:        firstNonEmpty(os.Getenv("INGEST_GCS_BUCKET"), "reitbrazil-db"),
		GCSKeyLatest:     firstNonEmpty(os.Getenv("INGEST_GCS_KEY_LATEST"), "latest/reitbrazil.db"),
		GCSPrefixHistory: firstNonEmpty(os.Getenv("INGEST_GCS_PREFIX_HISTORY"), "history"),
		GCSKeyMetadata:   firstNonEmpty(os.Getenv("INGEST_GCS_KEY_METADATA"), "latest/metadata.json"),
		GitHubToken:      os.Getenv("INGEST_GITHUB_TOKEN"),
		GitHubRepo:       firstNonEmpty(os.Getenv("INGEST_GITHUB_REPO"), "addodelgrossi/reitbrazil"),
		LogLevel:         firstNonEmpty(os.Getenv("INGEST_LOG_LEVEL"), "info"),
		LogFormat:        firstNonEmpty(os.Getenv("INGEST_LOG_FORMAT"), "json"),
	}

	rps, err := parseFloatOrDefault(os.Getenv("INGEST_RATE_LIMIT_RPS"), 3.0)
	if err != nil {
		return nil, fmt.Errorf("INGEST_RATE_LIMIT_RPS: %w", err)
	}
	cfg.RateLimitRPS = rps

	to, err := parseDurationOrDefault(os.Getenv("INGEST_HTTP_TIMEOUT"), 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("INGEST_HTTP_TIMEOUT: %w", err)
	}
	cfg.HTTPTimeout = to

	cfg.MinFundCount, err = parseIntOrDefault(os.Getenv("INGEST_MIN_FUND_COUNT"), 100)
	if err != nil {
		return nil, fmt.Errorf("INGEST_MIN_FUND_COUNT: %w", err)
	}
	cfg.MaxPriceLagDays, err = parseIntOrDefault(os.Getenv("INGEST_MAX_PRICE_LAG_DAYS"), 3)
	if err != nil {
		return nil, fmt.Errorf("INGEST_MAX_PRICE_LAG_DAYS: %w", err)
	}

	return cfg, nil
}

// ValidateForFetch ensures the minimal set of values needed for HTTP
// fetches is present.
func (c *Config) ValidateForFetch() error {
	if c.BrapiToken == "" {
		return errors.New("INGEST_BRAPI_TOKEN is required")
	}
	return nil
}

// ValidateForBigQuery ensures BQ-related values are present.
func (c *Config) ValidateForBigQuery() error {
	var missing []string
	if c.GCPProject == "" {
		missing = append(missing, "INGEST_GCP_PROJECT")
	}
	if c.BQDatasetRaw == "" {
		missing = append(missing, "INGEST_BQ_DATASET_RAW")
	}
	if c.BQDatasetCanon == "" {
		missing = append(missing, "INGEST_BQ_DATASET_CANON")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required vars: %s", strings.Join(missing, ", "))
	}
	return nil
}

// ValidateForPublish checks GCS publish requirements.
func (c *Config) ValidateForPublish() error {
	var missing []string
	if c.GCSBucket == "" {
		missing = append(missing, "INGEST_GCS_BUCKET")
	}
	if c.GCSKeyLatest == "" {
		missing = append(missing, "INGEST_GCS_KEY_LATEST")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required vars: %s", strings.Join(missing, ", "))
	}
	return nil
}

// ValidateForRelease checks GitHub release requirements.
func (c *Config) ValidateForRelease() error {
	if c.GitHubToken == "" {
		return errors.New("INGEST_GITHUB_TOKEN is required for monthly release")
	}
	if c.GitHubRepo == "" {
		return errors.New("INGEST_GITHUB_REPO is required for monthly release")
	}
	return nil
}

// loadEnvFile parses a KEY=VALUE file. Values are not set if the env
// variable is already present, so shell exports always win.
func loadEnvFile(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return err
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			return fmt.Errorf("%s:%d: missing '='", path, i+1)
		}
		key := strings.TrimSpace(line[:eq])
		value := strings.TrimSpace(line[eq+1:])
		value = strings.Trim(value, `"'`)
		if key == "" {
			return fmt.Errorf("%s:%d: empty key", path, i+1)
		}
		if _, set := os.LookupEnv(key); set {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s: %w", key, err)
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func parseFloatOrDefault(raw string, def float64) (float64, error) {
	if raw == "" {
		return def, nil
	}
	return strconv.ParseFloat(raw, 64)
}

func parseIntOrDefault(raw string, def int) (int, error) {
	if raw == "" {
		return def, nil
	}
	return strconv.Atoi(raw)
}

func parseDurationOrDefault(raw string, def time.Duration) (time.Duration, error) {
	if raw == "" {
		return def, nil
	}
	return time.ParseDuration(raw)
}
