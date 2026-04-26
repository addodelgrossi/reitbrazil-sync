//go:build integration

package bq_test

import (
	"context"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
)

func TestTransforms_DryRunAgainstBigQuery(t *testing.T) {
	project := os.Getenv("INGEST_GCP_PROJECT")
	if project == "" {
		t.Skip("INGEST_GCP_PROJECT not set")
	}
	ctx := context.Background()
	c, err := bq.New(ctx, bq.ClientOptions{
		ProjectID:    project,
		DatasetRaw:   firstEnv("INGEST_BQ_DATASET_RAW", "reitbrazil_raw"),
		DatasetCanon: firstEnv("INGEST_BQ_DATASET_CANON", "reitbrazil_canon"),
		Location:     firstEnv("INGEST_BQ_LOCATION", "southamerica-east1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	err = fs.WalkDir(bq.EmbeddedSQLForTest(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".sql") {
			return nil
		}
		if !(strings.HasPrefix(path, "10_") || strings.HasPrefix(path, "11_") ||
			strings.HasPrefix(path, "12_") || strings.HasPrefix(path, "13_") ||
			strings.HasPrefix(path, "20_")) {
			return nil
		}
		body, err := fs.ReadFile(bq.EmbeddedSQLForTest(), path)
		if err != nil {
			return err
		}
		q := c.Query(string(body))
		q.DryRun = true
		q.UseLegacySQL = false
		job, err := q.Run(ctx)
		if err != nil {
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		if err := status.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func firstEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
