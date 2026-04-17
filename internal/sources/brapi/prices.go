package brapi

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// FetchHistory yields daily OHLCV bars for ticker between [from, to].
// If to is zero, uses today. If from is zero, brapi's range=5y default
// is used.
func (c *Client) FetchHistory(ctx context.Context, ticker model.Ticker, from, to time.Time) iter.Seq2[model.PriceBar, error] {
	return func(yield func(model.PriceBar, error) bool) {
		q := url.Values{}
		q.Set("interval", "1d")
		if from.IsZero() {
			q.Set("range", "5y")
		} else {
			q.Set("range", "max")
		}

		var resp quoteDetailResponse
		if err := c.getJSON(ctx, "/quote/"+string(ticker), q, &resp); err != nil {
			yield(model.PriceBar{}, fmt.Errorf("history %s: %w", ticker, err))
			return
		}
		if len(resp.Results) == 0 {
			return
		}

		ingested := time.Now().UTC()
		for _, bar := range resp.Results[0].HistoricalDataPrice {
			date := time.Unix(bar.Date, 0).UTC().Truncate(24 * time.Hour)
			if !from.IsZero() && date.Before(from.Truncate(24*time.Hour)) {
				continue
			}
			if !to.IsZero() && date.After(to.Truncate(24*time.Hour)) {
				continue
			}
			payload, _ := json.Marshal(bar)
			pb := model.PriceBar{
				Ticker:     ticker,
				TradeDate:  date,
				Open:       bar.Open,
				High:       bar.High,
				Low:        bar.Low,
				Close:      bar.Close,
				Volume:     bar.Volume,
				Payload:    payload,
				IngestedAt: ingested,
			}
			if !yield(pb, nil) {
				return
			}
		}
	}
}
