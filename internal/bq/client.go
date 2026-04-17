package bq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

// ClientOptions configures the Client.
type ClientOptions struct {
	ProjectID    string
	DatasetRaw   string
	DatasetCanon string
	Location     string
	Logger       *slog.Logger
}

// Client is a thin wrapper around *bigquery.Client with project + dataset
// bindings captured once. It is safe for concurrent use; bq.Client is.
type Client struct {
	bq           *bigquery.Client
	projectID    string
	datasetRaw   string
	datasetCanon string
	location     string
	log          *slog.Logger
}

// New builds a Client and validates the options.
func New(ctx context.Context, opts ClientOptions) (*Client, error) {
	if opts.ProjectID == "" {
		return nil, errors.New("bq: ProjectID is required")
	}
	if opts.DatasetRaw == "" {
		opts.DatasetRaw = "reitbrazil_raw"
	}
	if opts.DatasetCanon == "" {
		opts.DatasetCanon = "reitbrazil_canon"
	}
	if opts.Location == "" {
		opts.Location = "southamerica-east1"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	bq, err := bigquery.NewClient(ctx, opts.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("bq: create client: %w", err)
	}
	bq.Location = opts.Location

	return &Client{
		bq:           bq,
		projectID:    opts.ProjectID,
		datasetRaw:   opts.DatasetRaw,
		datasetCanon: opts.DatasetCanon,
		location:     opts.Location,
		log:          opts.Logger.With("component", "bq"),
	}, nil
}

// Close releases the underlying BigQuery client.
func (c *Client) Close() error { return c.bq.Close() }

// Project returns the GCP project id bound to this client.
func (c *Client) Project() string { return c.projectID }

// DatasetRaw returns the raw dataset name.
func (c *Client) DatasetRaw() string { return c.datasetRaw }

// DatasetCanon returns the canon dataset name.
func (c *Client) DatasetCanon() string { return c.datasetCanon }

// Location returns the BigQuery location.
func (c *Client) Location() string { return c.location }

// Bootstrap creates the raw and canon datasets if they do not exist.
// Idempotent — safe to call on every startup.
func (c *Client) Bootstrap(ctx context.Context) error {
	for _, ds := range []string{c.datasetRaw, c.datasetCanon} {
		if err := c.ensureDataset(ctx, ds); err != nil {
			return fmt.Errorf("ensure dataset %s: %w", ds, err)
		}
	}
	return nil
}

func (c *Client) ensureDataset(ctx context.Context, name string) error {
	ref := c.bq.Dataset(name)
	_, err := ref.Metadata(ctx)
	if err == nil {
		return nil
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == 404 {
		c.log.InfoContext(ctx, "creating dataset", "dataset", name, "location", c.location)
		return ref.Create(ctx, &bigquery.DatasetMetadata{Location: c.location})
	}
	return err
}

// Dataset returns the raw or canon dataset by name. Unknown names return nil.
func (c *Client) Dataset(kind DatasetKind) *bigquery.Dataset {
	switch kind {
	case DatasetRaw:
		return c.bq.Dataset(c.datasetRaw)
	case DatasetCanon:
		return c.bq.Dataset(c.datasetCanon)
	}
	return nil
}

// TableRef builds a fully-qualified table reference (dataset.table).
func (c *Client) TableRef(kind DatasetKind, table string) *bigquery.Table {
	ds := c.Dataset(kind)
	if ds == nil {
		return nil
	}
	return ds.Table(table)
}

// QualifiedName returns the `project.dataset.table` string for use in DDL.
func (c *Client) QualifiedName(kind DatasetKind, table string) string {
	ds := c.datasetRaw
	if kind == DatasetCanon {
		ds = c.datasetCanon
	}
	return fmt.Sprintf("`%s.%s.%s`", c.projectID, ds, table)
}

// DatasetKind identifies which dataset a table lives in.
type DatasetKind int

const (
	// DatasetRaw is the bronze layer (append-only raw payloads).
	DatasetRaw DatasetKind = iota
	// DatasetCanon is the silver layer (deduplicated canonical tables).
	DatasetCanon
)

// BQ exposes the underlying *bigquery.Client. Reserved for advanced use.
func (c *Client) BQ() *bigquery.Client { return c.bq }
