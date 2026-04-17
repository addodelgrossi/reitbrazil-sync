// Package bq owns every interaction with BigQuery: dataset bootstrap,
// raw-layer writers, transform execution, and generic iterator-based
// reads. It deliberately knows nothing about HTTP sources — callers
// feed iter.Seq2 streams to the Land functions, and transforms are
// expressed in SQL files under bq_sql/ which are substituted with the
// current GCP project at execution time.
package bq
