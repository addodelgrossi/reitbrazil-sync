package brapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// FetchFundamentals asks brapi for defaultKeyStatistics+financialData.
// The return is a single snapshot with AsOf set to the call's wall clock
// (today), since brapi does not provide a stable "as of" on these fields.
func (c *Client) FetchFundamentals(ctx context.Context, ticker model.Ticker) (model.Fundamentals, error) {
	q := url.Values{}
	q.Set("modules", "defaultKeyStatistics,financialData")

	var resp quoteDetailResponse
	if err := c.getJSON(ctx, "/quote/"+string(ticker), q, &resp); err != nil {
		if !isModuleUnavailable(err, "financialData") {
			return model.Fundamentals{}, fmt.Errorf("fundamentals %s: %w", ticker, err)
		}
		q.Set("modules", "defaultKeyStatistics")
		if err := c.getJSON(ctx, "/quote/"+string(ticker), q, &resp); err != nil {
			return model.Fundamentals{}, fmt.Errorf("fundamentals %s: %w", ticker, err)
		}
	}
	if len(resp.Results) == 0 {
		return model.Fundamentals{}, fmt.Errorf("fundamentals %s: empty results", ticker)
	}

	qd := resp.Results[0]
	ingested := time.Now().UTC()
	asOf := ingested.Truncate(24 * time.Hour)

	f := model.Fundamentals{
		Ticker:     ticker,
		AsOf:       asOf,
		IngestedAt: ingested,
	}
	if qd.DefaultKeyStatistics != nil {
		f.NAVPerShare = floatPtr(qd.DefaultKeyStatistics.BookValue)
		f.PVP = floatPtr(qd.DefaultKeyStatistics.PriceToBook)
	}
	if qd.FinancialData != nil {
		f.AssetsTotal = floatPtr(qd.FinancialData.TotalAssets)
	}

	payload, _ := json.Marshal(qd)
	f.Payload = payload
	return f, nil
}

func isModuleUnavailable(err error, module string) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) &&
		httpErr.Status == http.StatusBadRequest &&
		strings.Contains(httpErr.Body, "MODULES_NOT_AVAILABLE") &&
		strings.Contains(httpErr.Body, module)
}

func floatPtr(v float64) *float64 {
	if v == 0 {
		return nil
	}
	return &v
}
