package brapi

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// FetchDividends yields every cash event reported for ticker. Events
// are ordered by ex-date DESC (brapi's natural order).
func (c *Client) FetchDividends(ctx context.Context, ticker model.Ticker) iter.Seq2[model.Dividend, error] {
	return func(yield func(model.Dividend, error) bool) {
		q := url.Values{}
		q.Set("dividends", "true")
		q.Set("range", "max")

		var resp quoteDetailResponse
		if err := c.getJSON(ctx, "/quote/"+string(ticker), q, &resp); err != nil {
			yield(model.Dividend{}, fmt.Errorf("dividends %s: %w", ticker, err))
			return
		}
		if len(resp.Results) == 0 || resp.Results[0].DividendsData == nil {
			return
		}

		ingested := time.Now().UTC()
		for _, ev := range resp.Results[0].DividendsData.CashDividends {
			ex, err := parseAnyDate(ev.LastDatePrior)
			if err != nil || ex.IsZero() {
				continue
			}
			payment, _ := parseAnyDate(ev.PaymentDate)
			record, _ := parseAnyDate(ev.RecordDate)
			announce, _ := parseAnyDate(ev.ApprovedOn)
			payload, _ := json.Marshal(ev)

			kind := mapDividendKind(ev.Label)
			d := model.Dividend{
				Ticker:         ticker,
				ExDate:         ex,
				AmountPerShare: ev.Rate,
				Kind:           kind,
				Source:         "brapi",
				Payload:        payload,
				IngestedAt:     ingested,
			}
			if !payment.IsZero() {
				d.PaymentDate = &payment
			}
			if !record.IsZero() {
				d.RecordDate = &record
			}
			if !announce.IsZero() {
				d.AnnounceDate = &announce
			}
			if !yield(d, nil) {
				return
			}
		}
	}
}

func mapDividendKind(label string) model.DividendKind {
	switch strings.ToUpper(strings.TrimSpace(label)) {
	case "JCP", "DIVIDEND", "":
		return model.DividendKindDividend
	case "AMORTIZATION", "AMORTIZACAO", "AMORTIZAÇÃO":
		return model.DividendKindAmortization
	case "RIGHTS", "SUBSCRICAO", "SUBSCRIÇÃO":
		return model.DividendKindRights
	default:
		return model.DividendKindDividend
	}
}

// parseAnyDate accepts YYYY-MM-DD, DD/MM/YYYY, or RFC3339. Returns zero
// for empty strings.
func parseAnyDate(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, nil
	}
	layouts := []string{
		"2006-01-02",
		"02/01/2006",
		time.RFC3339,
		"2006-01-02T15:04:05Z",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC().Truncate(24 * time.Hour), nil
		}
	}
	return time.Time{}, fmt.Errorf("unknown date format: %q", raw)
}
