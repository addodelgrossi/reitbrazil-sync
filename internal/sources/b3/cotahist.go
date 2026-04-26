package b3

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// ParseCOTAHIST reads B3 COTAHIST fixed-width records and yields daily
// price bars. Data records are 245 bytes long; header/footer records are
// skipped. Non-FII-looking tickers are ignored so callers can feed the
// full official file without prefiltering.
func ParseCOTAHIST(ctx context.Context, r io.Reader) iter.Seq2[model.PriceBar, error] {
	return func(yield func(model.PriceBar, error) bool) {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 1024), 1024*1024)
		ingested := time.Now().UTC()
		lineNo := 0
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				yield(model.PriceBar{}, err)
				return
			}
			lineNo++
			line := scanner.Text()
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}
			if len(line) < 2 || line[0:2] != "01" {
				continue
			}
			if len(line) < 188 {
				if !yield(model.PriceBar{}, fmt.Errorf("cotahist:%d short data record: %d bytes", lineNo, len(line))) {
					return
				}
				continue
			}
			bar, ok, err := parseCOTAHISTDataRecord(line, ingested)
			if err != nil {
				if !yield(model.PriceBar{}, fmt.Errorf("cotahist:%d: %w", lineNo, err)) {
					return
				}
				continue
			}
			if !ok {
				continue
			}
			if !yield(bar, nil) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			yield(model.PriceBar{}, fmt.Errorf("cotahist scan: %w", err))
		}
	}
}

func parseCOTAHISTDataRecord(line string, ingested time.Time) (model.PriceBar, bool, error) {
	ticker, err := model.ParseTicker(slice(line, 12, 24))
	if err != nil {
		return model.PriceBar{}, false, nil
	}
	tradeDate, err := time.Parse("20060102", slice(line, 2, 10))
	if err != nil {
		return model.PriceBar{}, false, fmt.Errorf("trade date: %w", err)
	}
	open, err := parseB3Price(slice(line, 56, 69))
	if err != nil {
		return model.PriceBar{}, false, fmt.Errorf("open: %w", err)
	}
	high, err := parseB3Price(slice(line, 69, 82))
	if err != nil {
		return model.PriceBar{}, false, fmt.Errorf("high: %w", err)
	}
	low, err := parseB3Price(slice(line, 82, 95))
	if err != nil {
		return model.PriceBar{}, false, fmt.Errorf("low: %w", err)
	}
	closePrice, err := parseB3Price(slice(line, 108, 121))
	if err != nil {
		return model.PriceBar{}, false, fmt.Errorf("close: %w", err)
	}
	volume, err := parseB3Int(slice(line, 152, 170))
	if err != nil {
		return model.PriceBar{}, false, fmt.Errorf("volume: %w", err)
	}
	return model.PriceBar{
		Ticker:     ticker,
		TradeDate:  tradeDate.UTC(),
		Open:       open,
		High:       high,
		Low:        low,
		Close:      closePrice,
		Volume:     volume,
		Payload:    []byte(fmt.Sprintf(`{"source":"b3_cotahist","raw_length":%d}`, len(line))),
		IngestedAt: ingested,
	}, true, nil
}

func slice(s string, start, end int) string {
	if start >= len(s) {
		return ""
	}
	if end > len(s) {
		end = len(s)
	}
	return strings.TrimSpace(s[start:end])
}

func parseB3Price(raw string) (float64, error) {
	n, err := parseB3Int(raw)
	if err != nil {
		return 0, err
	}
	return float64(n) / 100, nil
}

func parseB3Int(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
