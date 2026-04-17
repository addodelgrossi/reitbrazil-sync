package publish

import (
	"encoding/json"
	"time"
)

// Metadata is the sidecar JSON uploaded next to the SQLite file.
type Metadata struct {
	Version           string    `json:"version"`
	GeneratedAt       time.Time `json:"generated_at"`
	GitSHA            string    `json:"git_sha,omitempty"`
	FundCount         int       `json:"fund_count"`
	PriceRows         int       `json:"price_rows"`
	DividendRows      int       `json:"dividend_rows"`
	FundamentalsRows  int       `json:"fundamentals_rows"`
	SnapshotRows      int       `json:"snapshot_rows"`
	PriceCoverageTo   string    `json:"price_coverage_to,omitempty"`
	DividendTo        string    `json:"dividend_to,omitempty"`
	RunID             string    `json:"run_id,omitempty"`
}

// JSON returns the canonical indented JSON for Metadata.
func (m Metadata) JSON() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}
