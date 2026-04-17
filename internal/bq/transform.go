package bq

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
)

// sqlFS is populated in bq_sql.go via //go:embed. Kept here as a var
// so tests can swap it for a fake.
var sqlFS fs.FS

// RegisterEmbedFS injects the filesystem containing transform SQL files
// (top-level entries like "01_create_raw_tables.sql"). Called at init
// time from bq_sql.go; tests may override via this function.
func RegisterEmbedFS(f fs.FS) {
	sqlFS = f
}

// TransformResult reports the outcome of a single SQL transform.
type TransformResult struct {
	Name            string
	BytesProcessed  int64
	DMLStats        *bigquery.QueryStatistics
	Err             error
}

// RunTransforms executes every SQL file whose name starts with one of the
// requested prefixes (e.g. "10_", "11_"). If prefixes is empty, every
// non-DDL transform (prefix >= "10_") is executed in lexicographic order.
// DDL files (01_, 02_) are handled separately via RunDDL.
func (c *Client) RunTransforms(ctx context.Context, prefixes ...string) ([]TransformResult, error) {
	files, err := listSQL(sqlFS)
	if err != nil {
		return nil, err
	}
	var selected []string
	for _, name := range files {
		switch {
		case len(prefixes) == 0:
			if strings.HasPrefix(name, "10_") || strings.HasPrefix(name, "11_") ||
				strings.HasPrefix(name, "12_") || strings.HasPrefix(name, "13_") ||
				strings.HasPrefix(name, "20_") {
				selected = append(selected, name)
			}
		default:
			for _, p := range prefixes {
				if strings.HasPrefix(name, p) {
					selected = append(selected, name)
					break
				}
			}
		}
	}

	out := make([]TransformResult, 0, len(selected))
	for _, name := range selected {
		res := c.runOne(ctx, name)
		out = append(out, res)
		if res.Err != nil {
			return out, res.Err
		}
	}
	return out, nil
}

// RunDDL executes the schema-creation SQL files (01_, 02_) so the
// datasets contain the right tables. Safe to call repeatedly.
func (c *Client) RunDDL(ctx context.Context) ([]TransformResult, error) {
	return c.RunTransforms(ctx, "01_", "02_")
}

func (c *Client) runOne(ctx context.Context, name string) TransformResult {
	raw, err := fs.ReadFile(sqlFS, name)
	if err != nil {
		return TransformResult{Name: name, Err: fmt.Errorf("read %s: %w", name, err)}
	}
	sql := c.substitute(string(raw))
	q := c.bq.Query(sql)
	q.Location = c.location

	c.log.InfoContext(ctx, "running transform", "name", name)
	job, err := q.Run(ctx)
	if err != nil {
		return TransformResult{Name: name, Err: fmt.Errorf("%s run: %w", name, err)}
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return TransformResult{Name: name, Err: fmt.Errorf("%s wait: %w", name, err)}
	}
	if err := status.Err(); err != nil {
		return TransformResult{Name: name, Err: fmt.Errorf("%s failed: %w", name, err)}
	}

	res := TransformResult{Name: name}
	if status.Statistics != nil {
		if qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
			res.DMLStats = qs
			res.BytesProcessed = qs.TotalBytesProcessed
		}
	}
	c.log.InfoContext(ctx, "transform ok", "name", name, "bytes", res.BytesProcessed)
	return res
}

// substitute resolves placeholders used in the SQL files:
//
//	${project}       → the client's GCP project id
//	${dataset_raw}   → raw dataset name
//	${dataset_canon} → canon dataset name
func (c *Client) substitute(sql string) string {
	r := strings.NewReplacer(
		"${project}", c.projectID,
		"${dataset_raw}", c.datasetRaw,
		"${dataset_canon}", c.datasetCanon,
	)
	return r.Replace(sql)
}

func listSQL(fs fs.FS) ([]string, error) {
	if fs == nil {
		return nil, nil
	}
	entries, err := readEmbedDir(fs)
	if err != nil {
		return nil, err
	}
	sort.Strings(entries)
	return entries, nil
}

func readEmbedDir(fsys fs.FS) ([]string, error) {
	var out []string
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".sql") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}
