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
	BrapiCount      int  `json:"brapi_count"`
	CVMB3WithTicker int  `json:"cvm_b3_with_ticker"`
	Intersection    int  `json:"intersection"`
	BrapiDropped    int  `json:"brapi_dropped"`
	FallbackToCVM   bool `json:"fallback_to_cvm"`
}

// BuildFIIUniverse returns the canonical FII fund list: brapi's
// /quote/list?type=fund response filtered to tickers that also appear
// in CVM's informe geral as B3-listed with an ISIN-derivable ticker.
// This rejects the ~150 ETFs brapi misclassifies under type=fund
// (BOVA11, ACWI11, GOLD11, …) while keeping brapi's "actively trading
// today" accuracy as the primary signal.
//
// year=0 picks the previous calendar year, which is the most complete
// reference CVM publishes (current year is filled in month by month).
func BuildFIIUniverse(ctx context.Context, d Deps, year int) ([]model.Fund, UniverseStats, error) {
	if d.Brapi == nil {
		return nil, UniverseStats{}, fmt.Errorf("universe: brapi client not configured")
	}
	if d.CVM == nil {
		return nil, UniverseStats{}, fmt.Errorf("universe: cvm downloader not configured")
	}
	if year == 0 {
		year = time.Now().UTC().Year() - 1
	}

	brapiFunds := map[model.Ticker]model.Fund{}
	for f, err := range d.Brapi.FetchList(ctx) {
		if err != nil {
			return nil, UniverseStats{}, fmt.Errorf("brapi list: %w", err)
		}
		brapiFunds[f.Ticker] = f
	}

	zipBytes, err := d.CVM.FetchYear(ctx, year)
	if err != nil {
		return nil, UniverseStats{}, fmt.Errorf("cvm year %d: %w", year, err)
	}
	type cvmFund struct {
		fund model.Fund
		ref  time.Time
	}
	cvmFunds := map[model.Ticker]cvmFund{}
	for r, err := range cvm.Parse(ctx, zipBytes) {
		if err != nil {
			continue
		}
		if r.Listed == nil || !*r.Listed || r.Ticker == "" {
			continue
		}
		existing, ok := cvmFunds[r.Ticker]
		if ok && !r.ReferenceMonth.After(existing.ref) {
			continue
		}
		cvmFunds[r.Ticker] = cvmFund{
			fund: model.Fund{
				Ticker:        r.Ticker,
				CNPJ:          r.CNPJ,
				ISIN:          r.ISIN,
				Name:          r.Name,
				Segment:       r.Segment,
				Mandate:       r.Mandate,
				Administrator: r.Administrator,
				Listed:        true,
				Payload:       r.Payload,
				IngestedAt:    r.IngestedAt,
			},
			ref: r.ReferenceMonth,
		}
	}

	stats := UniverseStats{
		BrapiCount:      len(brapiFunds),
		CVMB3WithTicker: len(cvmFunds),
	}

	if len(brapiFunds) == 0 {
		out := make([]model.Fund, 0, len(cvmFunds))
		for _, c := range cvmFunds {
			out = append(out, c.fund)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Ticker < out[j].Ticker })
		stats.Intersection = len(out)
		stats.FallbackToCVM = true
		return out, stats, nil
	}

	out := make([]model.Fund, 0, len(brapiFunds))
	for t, b := range brapiFunds {
		c, ok := cvmFunds[t]
		if !ok {
			continue
		}
		out = append(out, mergeFundMetadata(b, c.fund))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ticker < out[j].Ticker })

	stats.Intersection = len(out)
	stats.BrapiDropped = len(brapiFunds) - len(out)
	return out, stats, nil
}

func mergeFundMetadata(brapiFund, cvmFund model.Fund) model.Fund {
	out := brapiFund
	if out.CNPJ == "" {
		out.CNPJ = cvmFund.CNPJ
	}
	if out.ISIN == "" {
		out.ISIN = cvmFund.ISIN
	}
	if out.Name == "" || out.Name == string(out.Ticker) {
		out.Name = cvmFund.Name
	}
	if out.Segment == "" || out.Segment == "other" {
		out.Segment = cvmFund.Segment
	}
	if out.Mandate == "" {
		out.Mandate = cvmFund.Mandate
	}
	if out.Administrator == "" {
		out.Administrator = cvmFund.Administrator
	}
	if len(out.Payload) == 0 {
		out.Payload = cvmFund.Payload
	}
	out.Listed = true
	return out
}
