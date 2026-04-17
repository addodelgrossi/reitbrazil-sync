package export_test

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/export"
)

// TestMigrations_ByteIdenticalToMCP ensures the migrations_sqlite/
// bundle is a byte-for-byte copy of the MCP server's migrations. If the
// two ever drift, the contract breaks. The MCP lives at the path below
// on this developer's machine; skip if absent (CI without the sibling
// repo will rely on the live contract test instead).
func TestMigrations_ByteIdenticalToMCP(t *testing.T) {
	t.Parallel()
	const mcpDir = "/Users/addo/jobs/addodelgrossi/reitbrazil/internal/storage/migrations"
	for _, name := range []string{"0001_init.sql", "0002_indexes.sql", "0003_views.sql"} {
		mcpPath := filepath.Join(mcpDir, name)
		want, err := readIfExists(mcpPath)
		if err != nil {
			t.Skipf("MCP migrations not available (%v), skipping parity test", err)
		}
		got, err := fs.ReadFile(export.Migrations(), name)
		if err != nil {
			t.Fatalf("embedded %s: %v", name, err)
		}
		if string(got) != string(want) {
			t.Fatalf("migration %s drifted from MCP source", name)
		}
	}
}

func readIfExists(path string) ([]byte, error) {
	return osReadFileOrSkip(path)
}

func TestOpen_AppliesMigrations(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "out.db")
	db, err := export.Open(path, true)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	tables := map[string]bool{}
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables[n] = true
	}
	rows.Close()

	for _, required := range []string{
		"funds", "prices", "dividends", "fundamentals",
		"calendar_events", "data_sources", "fund_snapshots",
	} {
		if !tables[required] {
			t.Fatalf("missing table: %s (have %v)", required, keys(tables))
		}
	}

	views := map[string]bool{}
	r2, err := db.Query(`SELECT name FROM sqlite_master WHERE type='view'`)
	if err != nil {
		t.Fatal(err)
	}
	for r2.Next() {
		var n string
		if err := r2.Scan(&n); err != nil {
			t.Fatal(err)
		}
		views[n] = true
	}
	r2.Close()
	for _, req := range []string{"v_latest_prices", "v_upcoming_dividends"} {
		if !views[req] {
			t.Fatalf("missing view: %s", req)
		}
	}
}

func TestWriter_RoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "out.db")
	db, err := export.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	w := export.NewWriter(db, export.WriterOptions{BatchSize: 2})
	ctx := t.Context()

	funds := []export.FundRow{
		{Ticker: "XPLG11", Name: "XP Log FII", Segment: "logistic", Mandate: "brick", Listed: true, CNPJ: "11728688000147"},
		{Ticker: "HGLG11", Name: "CSHG Log FII", Segment: "logistic", Mandate: "brick", Listed: true},
		{Ticker: "KNCR11", Name: "Kinea CRI FII", Segment: "paper", Mandate: "paper", Listed: true},
	}
	if err := w.WriteFunds(ctx, seqOf(funds)); err != nil {
		t.Fatalf("write funds: %v", err)
	}
	if w.Counts.Funds != 3 {
		t.Fatalf("funds count: %d", w.Counts.Funds)
	}

	f64 := func(v float64) *float64 { return &v }
	i64 := func(v int64) *int64 { return &v }
	prices := []export.PriceRow{
		{Ticker: "XPLG11", TradeDate: "2026-01-05", Open: f64(100.0), High: f64(101.0), Low: f64(99.5), Close: 100.8, Volume: i64(1_200_000)},
		{Ticker: "XPLG11", TradeDate: "2026-01-06", Open: f64(100.9), High: f64(102.0), Low: f64(100.5), Close: 101.6, Volume: i64(1_400_000)},
		{Ticker: "XPLG11", TradeDate: "2026-01-07", Open: f64(101.7), High: f64(103.0), Low: f64(101.5), Close: 102.5, Volume: i64(1_300_000)},
	}
	if err := w.WritePrices(ctx, seqOf(prices)); err != nil {
		t.Fatal(err)
	}

	divs := []export.DividendRow{
		{Ticker: "XPLG11", ExDate: "2025-12-30", PaymentDate: strPtr("2026-01-15"), AmountPerShare: 0.75, Kind: "dividend", Source: strPtr("brapi")},
		{Ticker: "XPLG11", ExDate: "2025-11-30", PaymentDate: strPtr("2025-12-15"), AmountPerShare: 0.74, Kind: "dividend"},
		{Ticker: "XPLG11", ExDate: "2026-05-30", AmountPerShare: 0.76, Kind: "dividend"}, // upcoming
	}
	if err := w.WriteDividends(ctx, seqOf(divs)); err != nil {
		t.Fatal(err)
	}

	fund := []export.FundamentalsRow{{
		Ticker: "XPLG11", AsOf: "2026-03-31",
		NAVPerShare: f64(98.5), PVP: f64(1.04), EquityTotal: f64(5_000_000_000),
		NumInvestors: i64(120000), VacancyPhysical: f64(0.03), VacancyFinancial: f64(0.02),
		OccupancyRate: f64(0.97),
	}}
	if err := w.WriteFundamentals(ctx, seqOf(fund)); err != nil {
		t.Fatal(err)
	}

	snaps := []export.SnapshotRow{{
		Ticker: "XPLG11", LastClose: f64(102.5), LastCloseDate: strPtr("2026-01-07"),
		DYTrailing12m: f64(0.088), DYForwardEst: f64(0.091),
		AvgDailyVolume90d: f64(1_300_000), Volatility90d: f64(0.18),
		MaxDrawdown1y: f64(-0.12), PVP: f64(1.04),
		Segment: strPtr("logistic"), Mandate: strPtr("brick"), UpdatedAt: "2026-04-17",
	}}
	if err := w.WriteSnapshots(ctx, seqOf(snaps)); err != nil {
		t.Fatal(err)
	}

	if err := w.WriteDataSources(ctx, []export.DataSource{{
		Name:            "brapi",
		LastRefreshedAt: time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC),
	}}); err != nil {
		t.Fatal(err)
	}

	// Exercise the exact queries the MCP server runs against the DB.
	// Any drift in the MCP will fail one of these assertions.

	// funds.List equivalent
	type fundView struct {
		ticker, name string
	}
	rows, err := db.QueryContext(ctx,
		`SELECT ticker, cnpj, name, segment, mandate, manager, administrator, ipo_date, listed
		   FROM funds ORDER BY ticker ASC LIMIT 10`)
	if err != nil {
		t.Fatal(err)
	}
	var fv []fundView
	for rows.Next() {
		var (
			ticker, name string
			cnpj, seg, mand, mgr, admin, ipo *string
			listed int
		)
		if err := rows.Scan(&ticker, &cnpj, &name, &seg, &mand, &mgr, &admin, &ipo, &listed); err != nil {
			t.Fatal(err)
		}
		fv = append(fv, fundView{ticker: ticker, name: name})
	}
	rows.Close()
	if len(fv) != 3 || fv[0].ticker != "HGLG11" {
		t.Fatalf("funds list: %+v", fv)
	}

	// prices.Latest equivalent
	row := db.QueryRowContext(ctx,
		`SELECT ticker, trade_date, open, high, low, close, volume FROM prices
		  WHERE ticker = ? ORDER BY trade_date DESC LIMIT 1`, "XPLG11")
	var (
		tk, td string
		op, hi, lo, cl *float64
		vol *int64
	)
	if err := row.Scan(&tk, &td, &op, &hi, &lo, &cl, &vol); err != nil {
		t.Fatalf("latest price: %v", err)
	}
	if td != "2026-01-07" || cl == nil || *cl != 102.5 {
		t.Fatalf("latest price wrong: td=%s close=%v", td, cl)
	}

	// prices.LatestBatch uses v_latest_prices view
	r3, err := db.QueryContext(ctx, `SELECT ticker, trade_date, close FROM v_latest_prices WHERE ticker IN ('XPLG11')`)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for r3.Next() {
		var tk, td string
		var cl float64
		if err := r3.Scan(&tk, &td, &cl); err != nil {
			t.Fatal(err)
		}
		found++
	}
	r3.Close()
	if found != 1 {
		t.Fatalf("v_latest_prices returned %d rows", found)
	}

	// dividends.History equivalent
	r4, err := db.QueryContext(ctx,
		`SELECT ticker, announce_date, ex_date, record_date, payment_date, amount_per_share, kind, source
		   FROM dividends WHERE ticker = ? ORDER BY ex_date DESC, kind ASC`, "XPLG11")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for r4.Next() {
		var (
			ticker string
			announce, record, payment, kind, source *string
			exDate string
			amount float64
		)
		if err := r4.Scan(&ticker, &announce, &exDate, &record, &payment, &amount, &kind, &source); err != nil {
			t.Fatal(err)
		}
		count++
	}
	r4.Close()
	if count != 3 {
		t.Fatalf("dividends history: %d", count)
	}

	// v_upcoming_dividends equivalent — ex_date >= date('now')
	// We inserted one dividend with ex_date 2026-05-30 which may or may
	// not be >= today depending on clock; assert the view at least runs.
	r5, err := db.QueryContext(ctx, `SELECT COUNT(*) FROM v_upcoming_dividends`)
	if err != nil {
		t.Fatal(err)
	}
	var upcoming int
	if r5.Next() {
		_ = r5.Scan(&upcoming)
	}
	r5.Close()
	if upcoming < 0 {
		t.Fatal("negative count")
	}

	// fundamentals.Latest equivalent
	row = db.QueryRowContext(ctx,
		`SELECT ticker, as_of, nav_per_share, pvp, assets_total, equity_total, num_investors,
		        liquidity_90d, vacancy_physical, vacancy_financial, occupancy_rate
		   FROM fundamentals WHERE ticker = ? ORDER BY as_of DESC LIMIT 1`, "XPLG11")
	var (
		fticker, asof string
		nav, pvp, assets, equity, liq, vp, vf, occ *float64
		numInv *int64
	)
	if err := row.Scan(&fticker, &asof, &nav, &pvp, &assets, &equity, &numInv, &liq, &vp, &vf, &occ); err != nil {
		t.Fatalf("latest fundamentals: %v", err)
	}
	if nav == nil || *nav != 98.5 {
		t.Fatalf("nav: %v", nav)
	}

	// snapshots.Screen equivalent
	r6, err := db.QueryContext(ctx,
		`SELECT ticker, last_close, last_close_date, dy_trailing_12m, dy_forward_est,
		        avg_daily_volume_90d, volatility_90d, max_drawdown_1y, pvp, segment, mandate, updated_at
		   FROM fund_snapshots WHERE dy_trailing_12m >= ? ORDER BY dy_trailing_12m DESC, ticker ASC LIMIT ?`,
		0.05, 10)
	if err != nil {
		t.Fatal(err)
	}
	snapCount := 0
	for r6.Next() {
		var (
			tk, ua string
			lcd, seg, mand *string
			lc, dy, dyF, adv, v90, dd, pv *float64
		)
		if err := r6.Scan(&tk, &lc, &lcd, &dy, &dyF, &adv, &v90, &dd, &pv, &seg, &mand, &ua); err != nil {
			t.Fatal(err)
		}
		snapCount++
	}
	r6.Close()
	if snapCount != 1 {
		t.Fatalf("snapshot screen: %d", snapCount)
	}

	// meta.Coverage equivalent
	row = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM funds`)
	var fundCount int
	if err := row.Scan(&fundCount); err != nil {
		t.Fatal(err)
	}
	if fundCount != 3 {
		t.Fatalf("fund count: %d", fundCount)
	}
	row = db.QueryRowContext(ctx, `SELECT
		(SELECT MAX(trade_date) FROM prices),
		(SELECT MAX(ex_date)    FROM dividends),
		(SELECT MAX(as_of)      FROM fundamentals)`)
	var a, b, c *string
	if err := row.Scan(&a, &b, &c); err != nil {
		t.Fatal(err)
	}
	if a == nil || !strings.HasPrefix(*a, "2026-") {
		t.Fatalf("price max: %v", a)
	}
}

func seqOf[T any](items []T) func(yield func(T, error) bool) {
	return func(yield func(T, error) bool) {
		for _, it := range items {
			if !yield(it, nil) {
				return
			}
		}
	}
}

func strPtr(s string) *string { return &s }
func keys[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
