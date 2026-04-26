package b3_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/b3"
)

func TestParseCOTAHIST_YieldsFIIPriceBar(t *testing.T) {
	body := "00HEADER\n" + cotahistRecord("20260105", "XPLG11", 10080, 10100, 9950, 10090, 1200000) + "\n99FOOTER\n"

	var rows int
	for bar, err := range b3.ParseCOTAHIST(t.Context(), strings.NewReader(body)) {
		if err != nil {
			t.Fatal(err)
		}
		rows++
		if bar.Ticker != "XPLG11" {
			t.Fatalf("ticker: %s", bar.Ticker)
		}
		if bar.TradeDate.Format("2006-01-02") != "2026-01-05" {
			t.Fatalf("date: %s", bar.TradeDate)
		}
		if bar.Open != 100.80 || bar.High != 101 || bar.Low != 99.50 || bar.Close != 100.90 {
			t.Fatalf("prices: %+v", bar)
		}
		if bar.Volume != 1200000 {
			t.Fatalf("volume: %d", bar.Volume)
		}
	}
	if rows != 1 {
		t.Fatalf("rows: %d", rows)
	}
}

func cotahistRecord(date, ticker string, open, high, low, closePrice, volume int64) string {
	b := []byte(strings.Repeat(" ", 245))
	put := func(start, end int, value string) {
		copy(b[start:end], value)
	}
	put(0, 2, "01")
	put(2, 10, date)
	put(12, 24, padRight(ticker, 12))
	put(56, 69, padLeft(open, 13))
	put(69, 82, padLeft(high, 13))
	put(82, 95, padLeft(low, 13))
	put(108, 121, padLeft(closePrice, 13))
	put(152, 170, padLeft(volume, 18))
	return string(b)
}

func padLeft(v int64, width int) string {
	s := strconvFormat(v)
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func strconvFormat(v int64) string {
	return strconv.FormatInt(v, 10)
}
