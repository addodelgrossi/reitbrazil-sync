// Package pipeline composes the ingestion stages — fetch, land,
// transform, export, publish — into executable flows for the daily and
// monthly CLI commands. It is the only place where per-stage isolation
// is crossed; everything else treats stages as black boxes.
package pipeline
