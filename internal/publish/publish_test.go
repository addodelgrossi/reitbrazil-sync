package publish_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/publish"
)

func TestMetadata_JSON(t *testing.T) {
	t.Parallel()
	m := publish.Metadata{
		Version:      "v1.0.0",
		GeneratedAt:  time.Date(2026, 4, 17, 22, 15, 0, 0, time.UTC),
		FundCount:    127,
		PriceRows:    320_000,
		DividendRows: 6200,
	}
	body, err := m.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatal(err)
	}
	if back["fund_count"].(float64) != 127 {
		t.Fatalf("fund_count: %v", back["fund_count"])
	}
	if back["version"] != "v1.0.0" {
		t.Fatalf("version: %v", back["version"])
	}
}
