package pipeline

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

// UniverseStats summarises BuildFIIUniverse's output.
type UniverseStats struct {
	BrapiCount      int `json:"brapi_count"`
	CVMB3WithTicker int `json:"cvm_b3_with_ticker"`
	Intersection    int `json:"intersection"`
	BrapiDropped    int `json:"brapi_dropped"`
}

// BuildFIIUniverse returns the canonical FII ticker list: brapi's
// /quote/list?type=fund response filtered to tickers that also appear
// in CVM's informe geral as B3-listed with an ISIN-derivable ticker.
// This rejects the ~150 ETFs brapi misclassifies under type=fund
// (BOVA11, ACWI11, GOLD11, …) while keeping brapi's "actively trading
// today" accuracy as the primary signal.
//
// year=0 picks the previous calendar year, which is the most complete
// reference CVM publishes (current year is filled in month by month).
func BuildFIIUniverse(ctx context.Context, d Deps, year int) ([]model.Ticker, UniverseStats, error) {
	if d.Brapi == nil {
		return nil, UniverseStats{}, fmt.Errorf("universe: brapi client not configured")
	}
	if d.CVM == nil {
		return nil, UniverseStats{}, fmt.Errorf("universe: cvm downloader not configured")
	}
	if year == 0 {
		year = time.Now().UTC().Year() - 1
	}

	brapiSet := map[model.Ticker]struct{}{}
	for f, err := range d.Brapi.FetchList(ctx) {
		if err != nil {
			return nil, UniverseStats{}, fmt.Errorf("brapi list: %w", err)
		}
		brapiSet[f.Ticker] = struct{}{}
	}

	zipBytes, err := d.CVM.FetchYear(ctx, year)
	if err != nil {
		return nil, UniverseStats{}, fmt.Errorf("cvm year %d: %w", year, err)
	}
	cvmSet := map[model.Ticker]struct{}{}
	for r, err := range cvm.ParseInformeGeral(ctx, zipBytes) {
		if err != nil {
			continue
		}
		if r.ListedOnBolsa && r.Ticker != "" {
			cvmSet[r.Ticker] = struct{}{}
		}
	}

	out := make([]model.Ticker, 0, len(brapiSet))
	for t := range brapiSet {
		if _, ok := cvmSet[t]; ok {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })

	stats := UniverseStats{
		BrapiCount:      len(brapiSet),
		CVMB3WithTicker: len(cvmSet),
		Intersection:    len(out),
		BrapiDropped:    len(brapiSet) - len(out),
	}
	return out, stats, nil
}
