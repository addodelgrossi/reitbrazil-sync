package brapi

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"strconv"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// FetchList walks /quote/list?type=fund paginated by hasNextPage. Yields
// each fund as a model.Fund with Payload left empty (the list endpoint
// does not return per-fund JSON payload worth preserving; per-fund
// details come from /quote/{ticker}).
func (c *Client) FetchList(ctx context.Context) iter.Seq2[model.Fund, error] {
	return func(yield func(model.Fund, error) bool) {
		page := 1
		for {
			if ctx.Err() != nil {
				yield(model.Fund{}, ctx.Err())
				return
			}
			q := url.Values{}
			q.Set("type", "fund")
			q.Set("limit", "100")
			q.Set("page", strconv.Itoa(page))

			var resp quoteListResponse
			if err := c.getJSON(ctx, "/quote/list", q, &resp); err != nil {
				yield(model.Fund{}, fmt.Errorf("list page %d: %w", page, err))
				return
			}

			ingested := time.Now().UTC()
			for _, it := range resp.Stocks {
				ticker, err := model.ParseTicker(it.Stock)
				if err != nil {
					// Skip entries that are not well-formed FIIs. Log via client log.
					c.log.WarnContext(ctx, "skip non-FII ticker", "raw", it.Stock)
					continue
				}
				f := model.Fund{
					Ticker:     ticker,
					Name:       firstNonEmptyStr(it.LongName, it.Name),
					Segment:    normalizeSegment(it.Sector),
					Mandate:    mandateFromSegment(it.Sector),
					Listed:     true,
					IngestedAt: ingested,
				}
				if !yield(f, nil) {
					return
				}
			}

			if !resp.HasNextPage {
				return
			}
			page++
		}
	}
}

func firstNonEmptyStr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
