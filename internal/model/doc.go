// Package model contains the internal pipeline entities.
//
// These types represent ingestion events flowing through the pipeline
// (fetch → land → transform → export). They are deliberately distinct
// from the MCP server's domain types: the only contract between the
// two repos is the SQLite schema, not Go types.
package model
