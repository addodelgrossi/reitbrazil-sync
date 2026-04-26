package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/civil"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/config"
	"github.com/addodelgrossi/reitbrazil-sync/internal/export"
	"github.com/addodelgrossi/reitbrazil-sync/internal/logging"
	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/publish"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

// StageResult is the outcome of a single stage.
type StageResult struct {
	Stage         string        `json:"stage"`
	RowsProcessed int           `json:"rows_processed"`
	Duration      time.Duration `json:"duration"`
	DurationMs    int64         `json:"duration_ms"`
	Errors        []string      `json:"errors,omitempty"`
}

// RunReport aggregates a full orchestrator run.
type RunReport struct {
	RunID         string        `json:"run_id"`
	Mode          string        `json:"mode"` // daily or monthly
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	Duration      time.Duration `json:"duration"`
	Stages        []StageResult `json:"stages"`
	QualityPassed bool          `json:"quality_passed"`
	FundCount     int           `json:"fund_count"`
	PriceMaxDate  string        `json:"price_max_date,omitempty"`
	PublishedTo   string        `json:"published_to,omitempty"`
}

// Deps bundles the infrastructure handles the orchestrator uses.
type Deps struct {
	Cfg   *config.Config
	Log   *slog.Logger
	Brapi BrapiSource
	BQ    *bq.Client
	CVM   CVMDownloader
	GCS   Publisher
}

// BrapiSource is the source-side contract used by the pipeline.
type BrapiSource interface {
	FetchList(context.Context) iter.Seq2[model.Fund, error]
	FetchHistory(context.Context, model.Ticker, time.Time, time.Time) iter.Seq2[model.PriceBar, error]
	FetchDividends(context.Context, model.Ticker) iter.Seq2[model.Dividend, error]
	FetchFundamentals(context.Context, model.Ticker) (model.Fundamentals, error)
}

// CVMDownloader is the minimal CVM download contract used by the pipeline.
type CVMDownloader interface {
	FetchYear(context.Context, int) ([]byte, error)
}

// Publisher is the publish-side contract used by the pipeline.
type Publisher interface {
	PublishSQLite(context.Context, string, publish.Metadata) error
	PublishRunReport(context.Context, []byte, string) error
}

// DailyOptions tweaks the daily run.
type DailyOptions struct {
	Tickers []model.Ticker // if empty, fetch the whole universe via brapi list
	From    time.Time      // price window start; zero = last 2 years
	To      time.Time
	OutDir  string // where to write the generated SQLite (default ./out)
	DryRun  bool

	skipBootstrap bool
}

// RunDaily executes fetch → land → transform → export → publish.
// Fetch covers prices, dividends, fundamentals. CVM monthly is handled
// by RunMonthly. The function is idempotent with respect to BigQuery
// canon (MERGE statements) and recreates the SQLite file from scratch.
func RunDaily(ctx context.Context, d Deps, opts DailyOptions) (*RunReport, error) {
	runID := logging.RunIDFromContext(ctx)
	log := logging.AttachRunID(loggerOrDefault(d.Log), runID).With("mode", "daily")
	start := time.Now().UTC()
	report := &RunReport{RunID: runID, Mode: "daily", StartedAt: start}

	outDir := opts.OutDir
	if outDir == "" {
		outDir = "./out"
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return report, fmt.Errorf("mkdir outDir: %w", err)
	}

	tickers := opts.Tickers

	if !opts.DryRun && !opts.skipBootstrap {
		bres := stageBootstrap(ctx, d)
		report.Stages = append(report.Stages, bres)
		if err := hasError(bres); err != nil {
			finalize(report, start)
			return report, err
		}
	}

	// Stage 1a: discover (if tickers are unset)
	if len(tickers) == 0 {
		res, list, err := stageDiscover(ctx, d, opts.DryRun)
		report.Stages = append(report.Stages, res)
		if err != nil {
			return report, err
		}
		tickers = tickersFromFunds(list)
	}
	log.InfoContext(ctx, "universe resolved", "tickers", len(tickers))

	if opts.DryRun {
		log.InfoContext(ctx, "dry-run: skipping BQ write and downstream stages")
		finalize(report, start)
		return report, nil
	}

	from := opts.From
	if from.IsZero() {
		from = time.Now().UTC().AddDate(-2, 0, 0)
	}
	to := opts.To

	// Stage 2: fetch+land prices
	report.Stages = append(report.Stages, stageLandPrices(ctx, d, tickers, from, to))

	// Stage 3: fetch+land dividends
	report.Stages = append(report.Stages, stageLandDividends(ctx, d, tickers))

	// Stage 4: fetch+land fundamentals
	report.Stages = append(report.Stages, stageLandFundamentals(ctx, d, tickers))

	// Stage 5: transform
	tres := stageTransform(ctx, d)
	report.Stages = append(report.Stages, tres)
	if err := hasError(tres); err != nil {
		finalize(report, start)
		return report, err
	}

	// Stage 6: export
	dbPath := filepath.Join(outDir, "reitbrazil.db")
	counts, eres := stageExport(ctx, d, dbPath)
	report.Stages = append(report.Stages, eres)
	if err := hasError(eres); err != nil {
		finalize(report, start)
		return report, err
	}

	// Quality gate
	if counts.Funds < d.Cfg.MinFundCount {
		report.QualityPassed = false
		report.FundCount = counts.Funds
		finalize(report, start)
		return report, fmt.Errorf("quality gate: fund_count=%d < min=%d", counts.Funds, d.Cfg.MinFundCount)
	}
	report.FundCount = counts.Funds

	priceMax, err := latestPriceDate(ctx, d.BQ, d.BQ.Project(), d.BQ.DatasetCanon())
	if err != nil {
		report.QualityPassed = false
		finalize(report, start)
		return report, fmt.Errorf("quality gate: latest price date: %w", err)
	}
	report.PriceMaxDate = priceMax.Format("2006-01-02")
	if d.Cfg.MaxPriceLagDays > 0 {
		lagDays := int(time.Since(priceMax).Hours() / 24)
		if lagDays > d.Cfg.MaxPriceLagDays {
			report.QualityPassed = false
			finalize(report, start)
			return report, fmt.Errorf("quality gate: price_max_date=%s lag=%dd > max=%dd",
				report.PriceMaxDate, lagDays, d.Cfg.MaxPriceLagDays)
		}
	}
	report.QualityPassed = true

	// Stage 7: publish
	if d.GCS != nil {
		pres := stagePublish(ctx, d, dbPath, counts, runID)
		report.Stages = append(report.Stages, pres)
		if err := hasError(pres); err != nil {
			finalize(report, start)
			return report, err
		}
		report.PublishedTo = fmt.Sprintf("gs://%s/%s", d.Cfg.GCSBucket, d.Cfg.GCSKeyLatest)
	}

	finalize(report, start)
	return report, nil
}

func loggerOrDefault(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

// RunMonthly extends the daily run with a CVM ingest for the previous
// month. It expects the caller to have configured a *cvm.Downloader on
// Deps.
type MonthlyOptions struct {
	DailyOptions
	Month time.Time // any day in the target month
}

// RunMonthly runs the daily pipeline plus a CVM ingest for opts.Month.
func RunMonthly(ctx context.Context, d Deps, opts MonthlyOptions) (*RunReport, error) {
	if opts.Month.IsZero() {
		opts.Month = time.Now().UTC().AddDate(0, -1, 0)
	}
	var bootstrapRes *StageResult
	if !opts.DryRun {
		res := stageBootstrap(ctx, d)
		bootstrapRes = &res
		if err := hasError(res); err != nil {
			start := time.Now().UTC()
			rep := &RunReport{RunID: logging.RunIDFromContext(ctx), Mode: "monthly", StartedAt: start, Stages: []StageResult{res}}
			finalize(rep, start)
			return rep, err
		}
		opts.skipBootstrap = true
	}
	// Land CVM first so the funds transform can join CNPJ.
	cvmRes := stageLandCVM(ctx, d, opts.Month, opts.DryRun)
	if err := hasError(cvmRes); err != nil {
		start := time.Now().UTC()
		rep := &RunReport{RunID: logging.RunIDFromContext(ctx), Mode: "monthly", StartedAt: start, Stages: []StageResult{cvmRes}}
		finalize(rep, start)
		return rep, err
	}
	rep, err := RunDaily(ctx, d, opts.DailyOptions)
	if rep == nil {
		rep = &RunReport{}
	}
	prefix := []StageResult{}
	if bootstrapRes != nil {
		prefix = append(prefix, *bootstrapRes)
	}
	prefix = append(prefix, cvmRes)
	rep.Stages = append(prefix, rep.Stages...)
	rep.Mode = "monthly"
	return rep, err
}

// -- stage implementations --

func stageBootstrap(ctx context.Context, d Deps) StageResult {
	start := time.Now()
	stage := "bootstrap"
	var errs []string
	if d.BQ == nil {
		errs = append(errs, "bq client not configured")
		return buildStage(stage, 0, start, errs)
	}
	if err := d.BQ.Bootstrap(ctx); err != nil {
		errs = append(errs, err.Error())
		return buildStage(stage, 0, start, errs)
	}
	results, err := d.BQ.RunDDL(ctx)
	if err != nil {
		errs = append(errs, err.Error())
	}
	return buildStage(stage, len(results), start, errs)
}

func stageDiscover(ctx context.Context, d Deps, dryRun bool) (StageResult, []model.Fund, error) {
	start := time.Now()
	stage := "discover"
	var errs []string

	funds, stats, err := BuildFIIUniverse(ctx, d, 0)
	if err != nil {
		errs = append(errs, err.Error())
		return buildStage(stage, 0, start, errs), nil, err
	}
	if d.Log != nil {
		d.Log.InfoContext(ctx, "universe resolved",
			"brapi", stats.BrapiCount,
			"cvm_b3_with_ticker", stats.CVMB3WithTicker,
			"intersection", stats.Intersection,
			"brapi_dropped_as_non_fii", stats.BrapiDropped)
	}

	if !dryRun && d.BQ != nil && len(funds) > 0 {
		landStats, err := d.BQ.LandFunds(ctx, fundsIterFor(funds))
		_ = landStats
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	return buildStage(stage, len(funds), start, errs), funds, firstErr(errs)
}

func stageLandPrices(ctx context.Context, d Deps, tickers []model.Ticker, from, to time.Time) StageResult {
	start := time.Now()
	stage := "land_prices"
	var errs []string
	rows := 0
	for _, t := range tickers {
		stats, err := d.BQ.LandPrices(ctx, d.Brapi.FetchHistory(ctx, t, from, to))
		rows += stats.RowsInserted
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t, err))
			continue
		}
		if stats.Err() != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t, stats.Err()))
		}
	}
	return buildStage(stage, rows, start, errs)
}

func stageLandDividends(ctx context.Context, d Deps, tickers []model.Ticker) StageResult {
	start := time.Now()
	stage := "land_dividends"
	var errs []string
	rows := 0
	for _, t := range tickers {
		stats, err := d.BQ.LandDividends(ctx, d.Brapi.FetchDividends(ctx, t))
		rows += stats.RowsInserted
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t, err))
			continue
		}
		if stats.Err() != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t, stats.Err()))
		}
	}
	return buildStage(stage, rows, start, errs)
}

func stageLandFundamentals(ctx context.Context, d Deps, tickers []model.Ticker) StageResult {
	start := time.Now()
	stage := "land_fundamentals"
	var errs []string
	rows := 0
	for _, t := range tickers {
		f, err := d.Brapi.FetchFundamentals(ctx, t)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t, err))
			continue
		}
		stats, err := d.BQ.LandFundamentals(ctx, f)
		rows += stats.RowsInserted
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", t, err))
		}
	}
	return buildStage(stage, rows, start, errs)
}

func stageLandCVM(ctx context.Context, d Deps, month time.Time, dryRun bool) StageResult {
	start := time.Now()
	stage := "land_cvm"
	year := month.Year()
	var errs []string
	rows := 0
	if d.CVM == nil {
		return buildStage(stage, 0, start, []string{"cvm downloader not configured"})
	}
	if dryRun {
		return buildStage(stage, 0, start, nil)
	}
	zipBytes, err := d.CVM.FetchYear(ctx, year)
	if err != nil {
		errs = append(errs, err.Error())
		return buildStage(stage, rows, start, errs)
	}
	stats, err := d.BQ.LandCVMInforme(ctx, filterCVMMonth(cvm.Parse(ctx, zipBytes), month))
	rows += stats.RowsInserted
	if err != nil {
		errs = append(errs, err.Error())
	}
	return buildStage(stage, rows, start, errs)
}

func stageTransform(ctx context.Context, d Deps) StageResult {
	start := time.Now()
	stage := "transform"
	var errs []string
	// DDL first (idempotent).
	if _, err := d.BQ.RunDDL(ctx); err != nil {
		errs = append(errs, err.Error())
		return buildStage(stage, 0, start, errs)
	}
	results, err := d.BQ.RunTransforms(ctx)
	if err != nil {
		errs = append(errs, err.Error())
	}
	return buildStage(stage, len(results), start, errs)
}

func stageExport(ctx context.Context, d Deps, dbPath string) (export.RowCounts, StageResult) {
	start := time.Now()
	stage := "export"
	var errs []string

	db, err := export.Open(dbPath, true)
	if err != nil {
		return export.RowCounts{}, buildStage(stage, 0, start, []string{err.Error()})
	}
	defer func() { _ = db.Close() }()

	w := export.NewWriter(db, export.WriterOptions{BatchSize: 1000, Logger: d.Log})
	project := d.BQ.Project()
	dsCanon := d.BQ.DatasetCanon()

	// funds
	if err := w.WriteFunds(ctx, ReadFunds(ctx, d.BQ, project, dsCanon)); err != nil {
		errs = append(errs, fmt.Sprintf("funds: %v", err))
	}
	if err := w.WritePrices(ctx, ReadPrices(ctx, d.BQ, project, dsCanon)); err != nil {
		errs = append(errs, fmt.Sprintf("prices: %v", err))
	}
	if err := w.WriteDividends(ctx, ReadDividends(ctx, d.BQ, project, dsCanon)); err != nil {
		errs = append(errs, fmt.Sprintf("dividends: %v", err))
	}
	if err := w.WriteFundamentals(ctx, ReadFundamentals(ctx, d.BQ, project, dsCanon)); err != nil {
		errs = append(errs, fmt.Sprintf("fundamentals: %v", err))
	}
	if err := w.WriteSnapshots(ctx, ReadSnapshots(ctx, d.BQ, project, dsCanon)); err != nil {
		errs = append(errs, fmt.Sprintf("snapshots: %v", err))
	}
	if err := w.WriteDataSources(ctx, []export.DataSource{
		{Name: "brapi", LastRefreshedAt: time.Now().UTC()},
		{Name: "cvm", LastRefreshedAt: time.Now().UTC()},
	}); err != nil {
		errs = append(errs, fmt.Sprintf("data_sources: %v", err))
	}
	if err := w.Vacuum(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("vacuum: %v", err))
	}

	counts := w.Counts
	total := counts.Funds + counts.Prices + counts.Dividends + counts.Fundamentals + counts.Snapshots
	return counts, buildStage(stage, total, start, errs)
}

func stagePublish(ctx context.Context, d Deps, dbPath string, counts export.RowCounts, runID string) StageResult {
	start := time.Now()
	stage := "publish"
	meta := publish.Metadata{
		Version:          "v1.0.0",
		GeneratedAt:      time.Now().UTC(),
		FundCount:        counts.Funds,
		PriceRows:        counts.Prices,
		DividendRows:     counts.Dividends,
		FundamentalsRows: counts.Fundamentals,
		SnapshotRows:     counts.Snapshots,
		RunID:            runID,
	}
	if err := d.GCS.PublishSQLite(ctx, dbPath, meta); err != nil {
		return buildStage(stage, 0, start, []string{err.Error()})
	}
	return buildStage(stage, 1, start, nil)
}

// -- helpers --

func buildStage(name string, rows int, start time.Time, errs []string) StageResult {
	d := time.Since(start)
	return StageResult{
		Stage:         name,
		RowsProcessed: rows,
		Duration:      d,
		DurationMs:    d.Milliseconds(),
		Errors:        errs,
	}
}

func finalize(r *RunReport, start time.Time) {
	r.FinishedAt = time.Now().UTC()
	r.Duration = r.FinishedAt.Sub(start)
}

func hasError(s StageResult) error {
	if len(s.Errors) == 0 {
		return nil
	}
	joined := make([]error, 0, len(s.Errors))
	for _, e := range s.Errors {
		joined = append(joined, errors.New(e))
	}
	return errors.Join(joined...)
}

func firstErr(errs []string) error {
	if len(errs) == 0 {
		return nil
	}
	return errors.New(errs[0])
}

func tickersFromFunds(funds []model.Fund) []model.Ticker {
	out := make([]model.Ticker, 0, len(funds))
	for _, f := range funds {
		out = append(out, f.Ticker)
	}
	return out
}

func fundsIterFor(funds []model.Fund) iter.Seq2[model.Fund, error] {
	return func(yield func(model.Fund, error) bool) {
		for _, f := range funds {
			if !yield(f, nil) {
				return
			}
		}
	}
}

func filterCVMMonth(src iter.Seq2[model.CVMInformeMensal, error], month time.Time) iter.Seq2[model.CVMInformeMensal, error] {
	targetYear, targetMonth, _ := month.Date()
	return func(yield func(model.CVMInformeMensal, error) bool) {
		for rec, err := range src {
			if err != nil {
				if !yield(model.CVMInformeMensal{}, err) {
					return
				}
				continue
			}
			y, m, _ := rec.ReferenceMonth.Date()
			if y == targetYear && m == targetMonth {
				if !yield(rec, nil) {
					return
				}
			}
		}
	}
}

// -- BigQuery → export readers --

// bqFundRow mirrors a row from canon.funds with bigquery tags.
type bqFundRow struct {
	Ticker        string      `bigquery:"ticker"`
	CNPJ          string      `bigquery:"cnpj"`
	Name          string      `bigquery:"name"`
	Segment       string      `bigquery:"segment"`
	Mandate       string      `bigquery:"mandate"`
	Manager       string      `bigquery:"manager"`
	Administrator string      `bigquery:"administrator"`
	IPODate       *civil.Date `bigquery:"ipo_date"`
	Listed        bool        `bigquery:"listed"`
}

type bqPriceRow struct {
	Ticker    string     `bigquery:"ticker"`
	TradeDate civil.Date `bigquery:"trade_date"`
	Open      *float64   `bigquery:"open"`
	High      *float64   `bigquery:"high"`
	Low       *float64   `bigquery:"low"`
	Close     float64    `bigquery:"close"`
	Volume    *int64     `bigquery:"volume"`
}

type bqDividendRow struct {
	Ticker         string      `bigquery:"ticker"`
	AnnounceDate   *civil.Date `bigquery:"announce_date"`
	ExDate         civil.Date  `bigquery:"ex_date"`
	RecordDate     *civil.Date `bigquery:"record_date"`
	PaymentDate    *civil.Date `bigquery:"payment_date"`
	AmountPerShare float64     `bigquery:"amount_per_share"`
	Kind           string      `bigquery:"kind"`
	Source         *string     `bigquery:"source"`
}

type bqFundamentalsRow struct {
	Ticker           string     `bigquery:"ticker"`
	AsOf             civil.Date `bigquery:"as_of"`
	NAVPerShare      *float64   `bigquery:"nav_per_share"`
	PVP              *float64   `bigquery:"pvp"`
	AssetsTotal      *float64   `bigquery:"assets_total"`
	EquityTotal      *float64   `bigquery:"equity_total"`
	NumInvestors     *int64     `bigquery:"num_investors"`
	Liquidity90d     *float64   `bigquery:"liquidity_90d"`
	VacancyPhysical  *float64   `bigquery:"vacancy_physical"`
	VacancyFinancial *float64   `bigquery:"vacancy_financial"`
	OccupancyRate    *float64   `bigquery:"occupancy_rate"`
}

type bqSnapshotRow struct {
	Ticker            string      `bigquery:"ticker"`
	LastClose         *float64    `bigquery:"last_close"`
	LastCloseDate     *civil.Date `bigquery:"last_close_date"`
	DYTrailing12m     *float64    `bigquery:"dy_trailing_12m"`
	DYForwardEst      *float64    `bigquery:"dy_forward_est"`
	AvgDailyVolume90d *float64    `bigquery:"avg_daily_volume_90d"`
	Volatility90d     *float64    `bigquery:"volatility_90d"`
	MaxDrawdown1y     *float64    `bigquery:"max_drawdown_1y"`
	PVP               *float64    `bigquery:"pvp"`
	Segment           *string     `bigquery:"segment"`
	Mandate           *string     `bigquery:"mandate"`
	UpdatedAt         time.Time   `bigquery:"updated_at"`
}

type bqMaxDateRow struct {
	MaxDate *civil.Date `bigquery:"max_date"`
}

func latestPriceDate(ctx context.Context, c *bq.Client, project, dataset string) (time.Time, error) {
	sql := fmt.Sprintf(`SELECT MAX(trade_date) AS max_date FROM `+"`%s.%s.prices`", project, dataset)
	for row, err := range bq.Read[bqMaxDateRow](ctx, c, sql) {
		if err != nil {
			return time.Time{}, err
		}
		if row.MaxDate == nil {
			return time.Time{}, errors.New("no price rows")
		}
		return row.MaxDate.In(time.UTC), nil
	}
	return time.Time{}, errors.New("no price rows")
}

func ReadFunds(ctx context.Context, c *bq.Client, project, dataset string) iter.Seq2[export.FundRow, error] {
	sql := fmt.Sprintf(`SELECT ticker, cnpj, name, segment, mandate, manager, administrator, ipo_date, listed
	                     FROM `+"`%s.%s.funds`"+` ORDER BY ticker ASC`, project, dataset)
	src := bq.Read[bqFundRow](ctx, c, sql)
	return func(yield func(export.FundRow, error) bool) {
		for row, err := range src {
			if err != nil {
				yield(export.FundRow{}, err)
				return
			}
			out := export.FundRow{
				Ticker: row.Ticker, CNPJ: row.CNPJ, Name: row.Name,
				Segment: row.Segment, Mandate: row.Mandate,
				Manager: row.Manager, Administrator: row.Administrator,
				Listed: row.Listed,
			}
			if row.IPODate != nil {
				s := row.IPODate.String()
				out.IPODate = &s
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

func ReadPrices(ctx context.Context, c *bq.Client, project, dataset string) iter.Seq2[export.PriceRow, error] {
	sql := fmt.Sprintf(`SELECT ticker, trade_date, open, high, low, close, volume
	                     FROM `+"`%s.%s.prices`"+` ORDER BY ticker ASC, trade_date ASC`, project, dataset)
	src := bq.Read[bqPriceRow](ctx, c, sql)
	return func(yield func(export.PriceRow, error) bool) {
		for row, err := range src {
			if err != nil {
				yield(export.PriceRow{}, err)
				return
			}
			out := export.PriceRow{
				Ticker: row.Ticker, TradeDate: row.TradeDate.String(),
				Open: row.Open, High: row.High, Low: row.Low,
				Close: row.Close, Volume: row.Volume,
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

func ReadDividends(ctx context.Context, c *bq.Client, project, dataset string) iter.Seq2[export.DividendRow, error] {
	sql := fmt.Sprintf(`SELECT
	                            ticker,
	                            MIN(announce_date) AS announce_date,
	                            ex_date,
	                            MIN(record_date) AS record_date,
	                            MIN(payment_date) AS payment_date,
	                            SUM(amount_per_share) AS amount_per_share,
	                            kind,
	                            ANY_VALUE(source) AS source
	                     FROM `+"`%s.%s.dividends`"+`
	                     GROUP BY ticker, ex_date, kind
	                     ORDER BY ticker ASC, ex_date ASC, kind ASC`, project, dataset)
	src := bq.Read[bqDividendRow](ctx, c, sql)
	return func(yield func(export.DividendRow, error) bool) {
		for row, err := range src {
			if err != nil {
				yield(export.DividendRow{}, err)
				return
			}
			out := export.DividendRow{
				Ticker:         row.Ticker,
				ExDate:         row.ExDate.String(),
				AmountPerShare: row.AmountPerShare,
				Kind:           row.Kind,
				Source:         row.Source,
			}
			if row.AnnounceDate != nil {
				s := row.AnnounceDate.String()
				out.AnnounceDate = &s
			}
			if row.RecordDate != nil {
				s := row.RecordDate.String()
				out.RecordDate = &s
			}
			if row.PaymentDate != nil {
				s := row.PaymentDate.String()
				out.PaymentDate = &s
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

func ReadFundamentals(ctx context.Context, c *bq.Client, project, dataset string) iter.Seq2[export.FundamentalsRow, error] {
	sql := fmt.Sprintf(`SELECT ticker, as_of, nav_per_share, pvp, assets_total, equity_total,
	                            num_investors, liquidity_90d, vacancy_physical, vacancy_financial, occupancy_rate
	                     FROM `+"`%s.%s.fundamentals`"+` ORDER BY ticker ASC, as_of ASC`, project, dataset)
	src := bq.Read[bqFundamentalsRow](ctx, c, sql)
	return func(yield func(export.FundamentalsRow, error) bool) {
		for row, err := range src {
			if err != nil {
				yield(export.FundamentalsRow{}, err)
				return
			}
			out := export.FundamentalsRow{
				Ticker:           row.Ticker,
				AsOf:             row.AsOf.String(),
				NAVPerShare:      row.NAVPerShare,
				PVP:              row.PVP,
				AssetsTotal:      row.AssetsTotal,
				EquityTotal:      row.EquityTotal,
				NumInvestors:     row.NumInvestors,
				Liquidity90d:     row.Liquidity90d,
				VacancyPhysical:  row.VacancyPhysical,
				VacancyFinancial: row.VacancyFinancial,
				OccupancyRate:    row.OccupancyRate,
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

func ReadSnapshots(ctx context.Context, c *bq.Client, project, dataset string) iter.Seq2[export.SnapshotRow, error] {
	sql := fmt.Sprintf(`SELECT ticker, last_close, last_close_date, dy_trailing_12m, dy_forward_est,
	                            avg_daily_volume_90d, volatility_90d, max_drawdown_1y, pvp,
	                            segment, mandate, updated_at
	                     FROM `+"`%s.%s.fund_snapshots`"+` ORDER BY ticker ASC`, project, dataset)
	src := bq.Read[bqSnapshotRow](ctx, c, sql)
	return func(yield func(export.SnapshotRow, error) bool) {
		for row, err := range src {
			if err != nil {
				yield(export.SnapshotRow{}, err)
				return
			}
			out := export.SnapshotRow{
				Ticker:            row.Ticker,
				LastClose:         row.LastClose,
				DYTrailing12m:     row.DYTrailing12m,
				DYForwardEst:      row.DYForwardEst,
				AvgDailyVolume90d: row.AvgDailyVolume90d,
				Volatility90d:     row.Volatility90d,
				MaxDrawdown1y:     row.MaxDrawdown1y,
				PVP:               row.PVP,
				Segment:           row.Segment,
				Mandate:           row.Mandate,
				UpdatedAt:         row.UpdatedAt.UTC().Format("2006-01-02"),
			}
			if row.LastCloseDate != nil {
				s := row.LastCloseDate.String()
				out.LastCloseDate = &s
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

// ReportJSON serialises a RunReport for sidecar upload.
func ReportJSON(r *RunReport) ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}
