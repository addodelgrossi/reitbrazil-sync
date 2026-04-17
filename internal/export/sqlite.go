package export

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Open opens (or creates) a SQLite DB at path with pragmas tuned for
// batch writes and applies the MCP migrations. If truncate is true an
// existing file at path is removed first.
func Open(path string, truncate bool) (*sql.DB, error) {
	if truncate {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove existing db: %w", err)
		}
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	pragmas := []string{
		"PRAGMA journal_mode = MEMORY;",
		"PRAGMA synchronous = OFF;",
		"PRAGMA cache_size = -65536;",
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA foreign_keys = ON;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("pragma %s: %w", p, err)
		}
	}
	if err := ApplyMigrations(db); err != nil {
		return nil, err
	}
	return db, nil
}

// RowCounts holds per-table write counters.
type RowCounts struct {
	Funds        int
	Prices       int
	Dividends    int
	Fundamentals int
	Snapshots    int
}

// WriterOptions controls the SQLite writer.
type WriterOptions struct {
	BatchSize int
	Logger    *slog.Logger
}

// Writer streams canon rows into the target SQLite DB. Callers use
// WriteFunds/WritePrices/etc in any order; WriteDataSources closes out
// the data_sources meta table.
type Writer struct {
	db        *sql.DB
	batchSize int
	log       *slog.Logger
	Counts    RowCounts
}

// NewWriter wraps an already-migrated *sql.DB.
func NewWriter(db *sql.DB, opts WriterOptions) *Writer {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Writer{db: db, batchSize: opts.BatchSize, log: opts.Logger}
}

// FundRow, PriceRow, DividendRow, FundamentalsRow, SnapshotRow mirror
// the SQLite schema used by the MCP. They carry only the columns the
// MCP reads via internal/sqlite.
type FundRow struct {
	Ticker        string
	CNPJ          string
	Name          string
	Segment       string
	Mandate       string
	Manager       string
	Administrator string
	IPODate       *string
	Listed        bool
}

type PriceRow struct {
	Ticker    string
	TradeDate string
	Open      *float64
	High      *float64
	Low       *float64
	Close     float64
	Volume    *int64
}

type DividendRow struct {
	Ticker         string
	AnnounceDate   *string
	ExDate         string
	RecordDate     *string
	PaymentDate    *string
	AmountPerShare float64
	Kind           string
	Source         *string
}

type FundamentalsRow struct {
	Ticker           string
	AsOf             string
	NAVPerShare      *float64
	PVP              *float64
	AssetsTotal      *float64
	EquityTotal      *float64
	NumInvestors     *int64
	Liquidity90d     *float64
	VacancyPhysical  *float64
	VacancyFinancial *float64
	OccupancyRate    *float64
}

type SnapshotRow struct {
	Ticker            string
	LastClose         *float64
	LastCloseDate     *string
	DYTrailing12m     *float64
	DYForwardEst      *float64
	AvgDailyVolume90d *float64
	Volatility90d     *float64
	MaxDrawdown1y     *float64
	PVP               *float64
	Segment           *string
	Mandate           *string
	UpdatedAt         string
}

// DataSource marks a logical dataset's freshness in the SQLite meta table.
type DataSource struct {
	Name            string
	LastRefreshedAt time.Time
	CoverageFrom    *time.Time
	CoverageTo      *time.Time
	Notes           string
}

// WriteFunds inserts funds.
func (w *Writer) WriteFunds(ctx context.Context, rows iter.Seq2[FundRow, error]) error {
	const stmt = `INSERT INTO funds(ticker, cnpj, name, segment, mandate, manager, administrator, ipo_date, listed)
	              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return writeSeq(ctx, w, rows, stmt, func(r FundRow) []any {
		listed := int64(0)
		if r.Listed {
			listed = 1
		}
		return []any{
			r.Ticker, nullIfEmpty(r.CNPJ), r.Name,
			nullIfEmpty(r.Segment), nullIfEmpty(r.Mandate),
			nullIfEmpty(r.Manager), nullIfEmpty(r.Administrator),
			stringPtrToAny(r.IPODate), listed,
		}
	}, func(n int) { w.Counts.Funds += n })
}

// WritePrices inserts OHLCV bars.
func (w *Writer) WritePrices(ctx context.Context, rows iter.Seq2[PriceRow, error]) error {
	const stmt = `INSERT INTO prices(ticker, trade_date, open, high, low, close, volume)
	              VALUES (?, ?, ?, ?, ?, ?, ?)`
	return writeSeq(ctx, w, rows, stmt, func(r PriceRow) []any {
		return []any{
			r.Ticker, r.TradeDate,
			floatPtrToAny(r.Open), floatPtrToAny(r.High), floatPtrToAny(r.Low),
			r.Close, intPtrToAny(r.Volume),
		}
	}, func(n int) { w.Counts.Prices += n })
}

// WriteDividends inserts dividend events.
func (w *Writer) WriteDividends(ctx context.Context, rows iter.Seq2[DividendRow, error]) error {
	const stmt = `INSERT INTO dividends(ticker, announce_date, ex_date, record_date, payment_date,
	                                    amount_per_share, kind, source)
	              VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	return writeSeq(ctx, w, rows, stmt, func(r DividendRow) []any {
		return []any{
			r.Ticker, stringPtrToAny(r.AnnounceDate), r.ExDate,
			stringPtrToAny(r.RecordDate), stringPtrToAny(r.PaymentDate),
			r.AmountPerShare, r.Kind, stringPtrToAny(r.Source),
		}
	}, func(n int) { w.Counts.Dividends += n })
}

// WriteFundamentals inserts fundamentals snapshots.
func (w *Writer) WriteFundamentals(ctx context.Context, rows iter.Seq2[FundamentalsRow, error]) error {
	const stmt = `INSERT INTO fundamentals(ticker, as_of, nav_per_share, pvp, assets_total, equity_total,
	                                       num_investors, liquidity_90d, vacancy_physical,
	                                       vacancy_financial, occupancy_rate)
	              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return writeSeq(ctx, w, rows, stmt, func(r FundamentalsRow) []any {
		return []any{
			r.Ticker, r.AsOf,
			floatPtrToAny(r.NAVPerShare), floatPtrToAny(r.PVP),
			floatPtrToAny(r.AssetsTotal), floatPtrToAny(r.EquityTotal),
			intPtrToAny(r.NumInvestors), floatPtrToAny(r.Liquidity90d),
			floatPtrToAny(r.VacancyPhysical), floatPtrToAny(r.VacancyFinancial),
			floatPtrToAny(r.OccupancyRate),
		}
	}, func(n int) { w.Counts.Fundamentals += n })
}

// WriteSnapshots inserts fund_snapshots rows.
func (w *Writer) WriteSnapshots(ctx context.Context, rows iter.Seq2[SnapshotRow, error]) error {
	const stmt = `INSERT INTO fund_snapshots(ticker, last_close, last_close_date, dy_trailing_12m,
	                                          dy_forward_est, avg_daily_volume_90d, volatility_90d,
	                                          max_drawdown_1y, pvp, segment, mandate, updated_at)
	              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return writeSeq(ctx, w, rows, stmt, func(r SnapshotRow) []any {
		return []any{
			r.Ticker, floatPtrToAny(r.LastClose), stringPtrToAny(r.LastCloseDate),
			floatPtrToAny(r.DYTrailing12m), floatPtrToAny(r.DYForwardEst),
			floatPtrToAny(r.AvgDailyVolume90d), floatPtrToAny(r.Volatility90d),
			floatPtrToAny(r.MaxDrawdown1y), floatPtrToAny(r.PVP),
			stringPtrToAny(r.Segment), stringPtrToAny(r.Mandate), r.UpdatedAt,
		}
	}, func(n int) { w.Counts.Snapshots += n })
}

// WriteDataSources populates data_sources.
func (w *Writer) WriteDataSources(ctx context.Context, sources []DataSource) error {
	stmt := `INSERT INTO data_sources(name, last_refreshed_at, coverage_from, coverage_to, notes)
	         VALUES (?, ?, ?, ?, ?)`
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, ds := range sources {
		if _, err := tx.ExecContext(ctx, stmt,
			ds.Name,
			ds.LastRefreshedAt.UTC().Format("2006-01-02"),
			datePtrToAny(ds.CoverageFrom),
			datePtrToAny(ds.CoverageTo),
			nullIfEmpty(ds.Notes),
		); err != nil {
			return fmt.Errorf("insert data_source %s: %w", ds.Name, err)
		}
	}
	return tx.Commit()
}

// Vacuum reclaims unused pages so the output file stays compact.
func (w *Writer) Vacuum(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, "VACUUM;")
	return err
}

// writeSeq pulls rows from src in batches of w.batchSize, wraps each
// batch in a transaction, and counts inserts via bump.
func writeSeq[T any](
	ctx context.Context,
	w *Writer,
	src iter.Seq2[T, error],
	stmt string,
	args func(T) []any,
	bump func(int),
) error {
	var (
		tx        *sql.Tx
		pstmt     *sql.Stmt
		inTx      int
		flushErr  error
	)

	openTx := func() error {
		if tx != nil {
			return nil
		}
		t, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		p, err := t.PrepareContext(ctx, stmt)
		if err != nil {
			t.Rollback()
			return fmt.Errorf("prepare: %w", err)
		}
		tx, pstmt = t, p
		inTx = 0
		return nil
	}
	commit := func() error {
		if tx == nil {
			return nil
		}
		pstmt.Close()
		err := tx.Commit()
		if err == nil {
			bump(inTx)
		}
		tx, pstmt = nil, nil
		inTx = 0
		return err
	}

	for row, err := range src {
		if err != nil {
			flushErr = err
			break
		}
		if err := openTx(); err != nil {
			flushErr = err
			break
		}
		if _, err := pstmt.ExecContext(ctx, args(row)...); err != nil {
			flushErr = fmt.Errorf("exec insert: %w", err)
			break
		}
		inTx++
		if inTx >= w.batchSize {
			if err := commit(); err != nil {
				flushErr = err
				break
			}
		}
	}

	if flushErr != nil {
		if tx != nil {
			pstmt.Close()
			tx.Rollback()
		}
		return flushErr
	}
	return commit()
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func stringPtrToAny(p *string) any {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

func floatPtrToAny(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}

func intPtrToAny(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func datePtrToAny(p *time.Time) any {
	if p == nil || p.IsZero() {
		return nil
	}
	return p.UTC().Format("2006-01-02")
}
