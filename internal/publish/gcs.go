package publish

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"time"

	"cloud.google.com/go/storage"
)

// GCSOptions configures GCSPublisher.
type GCSOptions struct {
	Bucket        string
	KeyLatest     string // e.g. latest/reitbrazil.db
	KeyMetadata   string // e.g. latest/metadata.json
	HistoryPrefix string // e.g. history
	RunsPrefix    string // e.g. runs
	Logger        *slog.Logger
}

// GCSPublisher uploads artifacts to GCS.
type GCSPublisher struct {
	client        *storage.Client
	bucket        string
	keyLatest     string
	keyMetadata   string
	historyPrefix string
	runsPrefix    string
	log           *slog.Logger
}

// NewGCSPublisher builds a GCS publisher. The caller is responsible for
// closing the underlying client via Close().
func NewGCSPublisher(ctx context.Context, opts GCSOptions) (*GCSPublisher, error) {
	if opts.Bucket == "" {
		return nil, errors.New("publish: Bucket is required")
	}
	if opts.KeyLatest == "" {
		opts.KeyLatest = "latest/reitbrazil.db"
	}
	if opts.KeyMetadata == "" {
		opts.KeyMetadata = "latest/metadata.json"
	}
	if opts.HistoryPrefix == "" {
		opts.HistoryPrefix = "history"
	}
	if opts.RunsPrefix == "" {
		opts.RunsPrefix = "runs"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs client: %w", err)
	}
	return &GCSPublisher{
		client:        client,
		bucket:        opts.Bucket,
		keyLatest:     opts.KeyLatest,
		keyMetadata:   opts.KeyMetadata,
		historyPrefix: opts.HistoryPrefix,
		runsPrefix:    opts.RunsPrefix,
		log:           opts.Logger.With("component", "publish/gcs"),
	}, nil
}

// Close releases the storage client.
func (p *GCSPublisher) Close() error { return p.client.Close() }

// PublishSQLite uploads the DB to gs://bucket/<keyLatest> and a dated
// copy under <historyPrefix>/reitbrazil-YYYY-MM-DD.db. Also uploads the
// metadata sidecar.
func (p *GCSPublisher) PublishSQLite(ctx context.Context, dbPath string, meta Metadata) error {
	sum, err := fileSHA256(dbPath)
	if err != nil {
		return fmt.Errorf("sha256: %w", err)
	}

	// latest/reitbrazil.db
	if err := p.uploadFile(ctx, p.keyLatest, dbPath, "application/x-sqlite3"); err != nil {
		return fmt.Errorf("upload latest: %w", err)
	}

	// history/reitbrazil-YYYY-MM-DD.db
	datestamp := time.Now().UTC().Format("2006-01-02")
	historyKey := path.Join(p.historyPrefix, fmt.Sprintf("reitbrazil-%s.db", datestamp))
	if err := p.uploadFile(ctx, historyKey, dbPath, "application/x-sqlite3"); err != nil {
		return fmt.Errorf("upload history: %w", err)
	}

	// metadata sidecar
	body, err := meta.JSON()
	if err != nil {
		return err
	}
	if err := p.uploadBytes(ctx, p.keyMetadata, body, "application/json"); err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}

	p.log.InfoContext(ctx, "published to gcs",
		"bucket", p.bucket,
		"latest", p.keyLatest,
		"history", historyKey,
		"metadata", p.keyMetadata,
		"sha256", sum,
		"fund_count", meta.FundCount,
	)
	return nil
}

// PublishRunReport uploads a per-run JSON under runs/YYYY-MM-DD.json.
func (p *GCSPublisher) PublishRunReport(ctx context.Context, report []byte, label string) error {
	if label == "" {
		label = time.Now().UTC().Format("2006-01-02")
	}
	key := path.Join(p.runsPrefix, label+".json")
	return p.uploadBytes(ctx, key, report, "application/json")
}

func (p *GCSPublisher) uploadFile(ctx context.Context, key, localPath, contentType string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := p.client.Bucket(p.bucket).Object(key).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (p *GCSPublisher) uploadBytes(ctx context.Context, key string, body []byte, contentType string) error {
	w := p.client.Bucket(p.bucket).Object(key).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
