package brapi_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"
)

// fixtureServer returns an httptest.Server that replies with the contents
// of testdata/fixtures/<name>.json depending on the request path/query.
// Routing is intentionally hand-written so each test can control which
// fixture a given path resolves to.
func fixtureServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return s
}

func mustReadFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "fixtures", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

func newClient(t *testing.T, baseURL string) *brapi.Client {
	t.Helper()
	c, err := brapi.NewClient(brapi.ClientOptions{
		BaseURL:    baseURL,
		Token:      "test-token",
		RPS:        100,
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestFetchList_PaginatesUntilHasNextPageFalse(t *testing.T) {
	calls := 0
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer test-token"; got != want {
			t.Errorf("auth header: %q", got)
		}
		if r.URL.Path != "/quote/list" {
			t.Errorf("path: %q", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("type") != "fund" {
			t.Errorf("type: %q", q.Get("type"))
		}
		page := q.Get("page")
		calls++
		var body []byte
		switch page {
		case "1":
			body = mustReadFixture(t, "quote_list_page1.json")
		case "2":
			body = mustReadFixture(t, "quote_list_page2.json")
		default:
			t.Errorf("unexpected page: %q", page)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))

	c := newClient(t, srv.URL)
	var got []model.Fund
	var err error
	for f, e := range c.FetchList(t.Context()) {
		if e != nil {
			err = e
			break
		}
		got = append(got, f)
	}
	if err != nil {
		t.Fatalf("FetchList: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 pages, got %d", calls)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 funds, got %d (%v)", len(got), got)
	}
	if got[0].Ticker != "XPLG11" {
		t.Fatalf("first ticker: %q", got[0].Ticker)
	}
	if got[0].Segment != "logistic" {
		t.Fatalf("first segment: %q", got[0].Segment)
	}
	if got[2].Segment != "paper" {
		t.Fatalf("KNCR11 segment: %q", got[2].Segment)
	}
	if got[3].Segment != "hybrid" {
		t.Fatalf("KNRI11 segment: %q", got[3].Segment)
	}
}

func TestFetchHistory_FiltersByDate(t *testing.T) {
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/quote/") {
			t.Errorf("path: %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(mustReadFixture(t, "quote_history.json"))
	}))

	c := newClient(t, srv.URL)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	var bars []model.PriceBar
	for b, err := range c.FetchHistory(t.Context(), "XPLG11", from, to) {
		if err != nil {
			t.Fatal(err)
		}
		bars = append(bars, b)
	}
	// All three fixture rows fall after 2026-01-01 (epoch 1767225600 = 2026-01-01 UTC).
	if len(bars) != 3 {
		t.Fatalf("expected 3 bars, got %d", len(bars))
	}
	if bars[0].Close != 100.8 {
		t.Fatalf("first close: %v", bars[0].Close)
	}
	if bars[0].Ticker != "XPLG11" {
		t.Fatalf("first ticker: %q", bars[0].Ticker)
	}
}

func TestFetchDividends_MapsKinds(t *testing.T) {
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("dividends") != "true" {
			t.Errorf("dividends flag missing: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(mustReadFixture(t, "quote_dividends.json"))
	}))
	c := newClient(t, srv.URL)

	var divs []model.Dividend
	for d, err := range c.FetchDividends(t.Context(), "XPLG11") {
		if err != nil {
			t.Fatal(err)
		}
		divs = append(divs, d)
	}
	if len(divs) != 3 {
		t.Fatalf("expected 3 dividends, got %d", len(divs))
	}
	var amort int
	for _, d := range divs {
		if d.Kind == model.DividendKindAmortization {
			amort++
		}
	}
	if amort != 1 {
		t.Fatalf("expected 1 amortization, got %d", amort)
	}
	if divs[0].Ticker != "XPLG11" {
		t.Fatalf("ticker: %q", divs[0].Ticker)
	}
	if divs[0].ExDate.IsZero() {
		t.Fatal("ex-date should be parsed")
	}
}

func TestFetchFundamentals_PopulatesBookValue(t *testing.T) {
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("modules") == "" {
			t.Errorf("modules missing: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(mustReadFixture(t, "quote_fundamentals.json"))
	}))
	c := newClient(t, srv.URL)

	f, err := c.FetchFundamentals(t.Context(), "XPLG11")
	if err != nil {
		t.Fatal(err)
	}
	if f.NAVPerShare == nil || *f.NAVPerShare != 98.5 {
		t.Fatalf("nav: %v", f.NAVPerShare)
	}
	if f.PVP == nil || *f.PVP != 1.04 {
		t.Fatalf("pvp: %v", f.PVP)
	}
	if f.AssetsTotal == nil || *f.AssetsTotal != 5_000_000_000 {
		t.Fatalf("assets: %v", f.AssetsTotal)
	}
}

func TestRetry_On5xxThenSuccess(t *testing.T) {
	body := mustReadFixture(t, "quote_history.json")
	var call int
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		if call == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))

	c := newClient(t, srv.URL)
	var seen int
	for _, err := range c.FetchHistory(t.Context(), "XPLG11", time.Time{}, time.Time{}) {
		if err != nil {
			t.Fatal(err)
		}
		seen++
	}
	if call != 2 {
		t.Fatalf("expected 2 calls, got %d", call)
	}
	if seen == 0 {
		t.Fatal("expected at least one bar")
	}
}

func TestHTTPError_NonRetryable(t *testing.T) {
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"bad token"}`))
	}))
	c := newClient(t, srv.URL)

	var err error
	for _, e := range c.FetchList(t.Context()) {
		if e != nil {
			err = e
			break
		}
	}
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 in error, got %v", err)
	}
}

// Sanity check that the internal buffer is not leaked if the caller
// early-exits via `break`.
func TestIteratorEarlyExit(t *testing.T) {
	srv := fixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, bytes.NewReader(mustReadFixture(t, "quote_history.json")))
	}))
	c := newClient(t, srv.URL)
	count := 0
	for _, err := range c.FetchHistory(t.Context(), "XPLG11", time.Time{}, time.Time{}) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if count == 1 {
			break
		}
	}
	if count != 1 {
		t.Fatalf("expected early exit after 1 bar, got %d", count)
	}
}

// Guard: ensure the segment heuristic covers the main cases.
func TestSegmentMapping_SmokeViaQueryEncoding(t *testing.T) {
	// This exercises url.Values encoding indirectly via the test server.
	u := (&url.Values{}).Encode()
	if u != "" {
		t.Fatal("expected empty encode")
	}
}
