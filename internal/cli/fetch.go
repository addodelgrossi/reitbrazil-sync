package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

func newFetchCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch from a single source (prices|dividends|fundamentals|cvm)",
	}
	cmd.AddCommand(newFetchPricesCmd(app))
	cmd.AddCommand(newFetchDividendsCmd(app))
	cmd.AddCommand(newFetchFundamentalsCmd(app))
	cmd.AddCommand(newFetchCVMCmd(app))
	return cmd
}

func resolveTickers(cmd *cobra.Command, raw string, d pipeline.Deps) ([]model.Ticker, error) {
	parts := parseTickerList(raw)
	if len(parts) == 0 {
		tickers, stats, err := pipeline.BuildFIIUniverse(cmd.Context(), d, 0)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(cmd.ErrOrStderr(),
			"universe: %d FIIs (brapi %d ∩ CVM %d B3-listed; dropped %d brapi tickers as non-FII)\n",
			stats.Intersection, stats.BrapiCount, stats.CVMB3WithTicker, stats.BrapiDropped)
		return tickers, nil
	}
	tickers := make([]model.Ticker, 0, len(parts))
	for _, p := range parts {
		t, err := model.ParseTicker(p)
		if err != nil {
			return nil, err
		}
		tickers = append(tickers, t)
	}
	return tickers, nil
}

func newFetchPricesCmd(app *App) *cobra.Command {
	var (
		raw  string
		from string
		to   string
	)
	cmd := &cobra.Command{
		Use:   "prices",
		Short: "Fetch OHLCV bars and land them in raw.brapi_quote",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{Brapi: &brapi.Client{}, BQ: &bq.Client{}, CVM: &cvm.Downloader{}})
			if err != nil {
				return err
			}
			defer cleanup()

			tickers, err := resolveTickers(cmd, raw, d)
			if err != nil {
				return err
			}
			fromT, err := parseDateOrEmpty(from)
			if err != nil {
				return usageErr(cmd, fmt.Errorf("invalid --from: %w", err))
			}
			toT, err := parseDateOrEmpty(to)
			if err != nil {
				return usageErr(cmd, fmt.Errorf("invalid --to: %w", err))
			}

			totalRows := 0
			var landErrs []error
			for _, t := range tickers {
				stats, err := d.BQ.LandPrices(ctx, d.Brapi.FetchHistory(ctx, t, fromT, toT))
				totalRows += stats.RowsInserted
				if err != nil {
					landErrs = append(landErrs, fmt.Errorf("%s: %w", t, err))
				}
				if stats.Err() != nil {
					landErrs = append(landErrs, fmt.Errorf("%s: %w", t, stats.Err()))
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "inserted %d price rows across %d tickers\n", totalRows, len(tickers))
			return errors.Join(landErrs...)
		},
	}
	cmd.Flags().StringVar(&raw, "tickers", "", "comma-separated tickers; empty = full universe")
	cmd.Flags().StringVar(&from, "from", "", "start date YYYY-MM-DD")
	cmd.Flags().StringVar(&to, "to", "", "end date YYYY-MM-DD")
	return cmd
}

func newFetchDividendsCmd(app *App) *cobra.Command {
	var raw string
	cmd := &cobra.Command{
		Use:   "dividends",
		Short: "Fetch dividend events and land them in raw.brapi_dividends",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{Brapi: &brapi.Client{}, BQ: &bq.Client{}, CVM: &cvm.Downloader{}})
			if err != nil {
				return err
			}
			defer cleanup()

			tickers, err := resolveTickers(cmd, raw, d)
			if err != nil {
				return err
			}
			total := 0
			var errs []error
			for _, t := range tickers {
				stats, err := d.BQ.LandDividends(ctx, d.Brapi.FetchDividends(ctx, t))
				total += stats.RowsInserted
				if err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", t, err))
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "inserted %d dividend rows\n", total)
			return errors.Join(errs...)
		},
	}
	cmd.Flags().StringVar(&raw, "tickers", "", "comma-separated tickers; empty = full universe")
	return cmd
}

func newFetchFundamentalsCmd(app *App) *cobra.Command {
	var raw string
	cmd := &cobra.Command{
		Use:   "fundamentals",
		Short: "Fetch fundamentals snapshots and land them in raw.brapi_fundamentals",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{Brapi: &brapi.Client{}, BQ: &bq.Client{}, CVM: &cvm.Downloader{}})
			if err != nil {
				return err
			}
			defer cleanup()

			tickers, err := resolveTickers(cmd, raw, d)
			if err != nil {
				return err
			}
			var errs []error
			total := 0
			for _, t := range tickers {
				f, err := d.Brapi.FetchFundamentals(ctx, t)
				if err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", t, err))
					continue
				}
				stats, err := d.BQ.LandFundamentals(ctx, f)
				total += stats.RowsInserted
				if err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", t, err))
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "inserted %d fundamentals rows\n", total)
			return errors.Join(errs...)
		},
	}
	cmd.Flags().StringVar(&raw, "tickers", "", "comma-separated tickers; empty = full universe")
	return cmd
}

func newFetchCVMCmd(app *App) *cobra.Command {
	var monthStr string
	cmd := &cobra.Command{
		Use:   "cvm",
		Short: "Download and land CVM monthly informe",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{BQ: &bq.Client{}, CVM: cvm.NewDownloader(cvm.DownloaderOptions{})})
			if err != nil {
				return err
			}
			defer cleanup()

			month := time.Now().UTC().AddDate(0, -1, 0)
			if monthStr != "" {
				t, err := time.Parse("2006-01", monthStr)
				if err != nil {
					return usageErr(cmd, fmt.Errorf("invalid --month %q, expected YYYY-MM", monthStr))
				}
				month = t
			}
			zipBytes, err := d.CVM.FetchYear(ctx, month.Year())
			if err != nil {
				return err
			}
			stats, err := d.BQ.LandCVMInforme(ctx, cvm.Parse(ctx, zipBytes))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "inserted %d CVM rows for %d\n", stats.RowsInserted, month.Year())
			return stats.Err()
		},
	}
	cmd.Flags().StringVar(&monthStr, "month", "", "target month YYYY-MM (default: previous month)")
	return cmd
}

func parseDateOrEmpty(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse("2006-01-02", s)
}
