package bq

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// Query builds a BigQuery query with placeholder substitution applied.
// The caller receives the *bigquery.Query so additional parameters can
// be set (e.g. named parameters) before execution.
func (c *Client) Query(sql string) *bigquery.Query {
	q := c.bq.Query(c.substitute(sql))
	q.Location = c.location
	return q
}

// Read executes sql and yields rows as *T using BigQuery's row-struct
// mapping. Callers supply the target struct type; BigQuery matches
// field tags `bigquery:"column_name"`.
func Read[T any](ctx context.Context, c *Client, sql string) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		q := c.Query(sql)
		it, err := q.Read(ctx)
		if err != nil {
			yield(zero, fmt.Errorf("query: %w", err))
			return
		}
		for {
			var row T
			err := it.Next(&row)
			if errors.Is(err, iterator.Done) {
				return
			}
			if err != nil {
				yield(zero, fmt.Errorf("iterate: %w", err))
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}
