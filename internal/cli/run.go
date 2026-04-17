package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/publish"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

func newRunCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Compose stages end-to-end (daily | monthly)",
	}
	cmd.AddCommand(newRunDailyCmd(app))
	cmd.AddCommand(newRunMonthlyCmd(app))
	return cmd
}

func newRunDailyCmd(app *App) *cobra.Command {
	var (
		tickersRaw string
		from       string
		to         string
		outDir     string
	)
	cmd := &cobra.Command{
		Use:   "daily",
		Short: "Fetch → land → transform → export → publish",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{
				Brapi: &brapi.Client{}, BQ: &bq.Client{}, GCS: &publish.GCSPublisher{},
			})
			if err != nil {
				return err
			}
			defer cleanup()

			opts := pipeline.DailyOptions{OutDir: outDir}
			if tickersRaw != "" {
				for _, p := range parseTickerList(tickersRaw) {
					t, err := parseTickerModel(p)
					if err != nil {
						return err
					}
					opts.Tickers = append(opts.Tickers, t)
				}
			}
			if from != "" {
				ft, err := parseDateOrEmpty(from)
				if err != nil {
					return err
				}
				opts.From = ft
			}
			if to != "" {
				tt, err := parseDateOrEmpty(to)
				if err != nil {
					return err
				}
				opts.To = tt
			}

			report, runErr := pipeline.RunDaily(ctx, d, opts)
			if err := uploadRunReport(ctx, d, report, time.Now().UTC().Format("2006-01-02")); err != nil {
				app.log.WarnContext(ctx, "run report upload failed", "err", err)
			}
			if runErr != nil {
				return runErr
			}
			fmt.Fprintf(cmd.OutOrStdout(), "daily run ok: fund_count=%d published_to=%s\n",
				report.FundCount, report.PublishedTo)
			return nil
		},
	}
	cmd.Flags().StringVar(&tickersRaw, "tickers", "", "comma-separated override")
	cmd.Flags().StringVar(&from, "from", "", "price window start YYYY-MM-DD")
	cmd.Flags().StringVar(&to, "to", "", "price window end YYYY-MM-DD")
	cmd.Flags().StringVar(&outDir, "out-dir", "./out", "output directory for reitbrazil.db")
	return cmd
}

func newRunMonthlyCmd(app *App) *cobra.Command {
	var (
		monthStr  string
		outDir    string
		releaseTag string
		body      string
	)
	cmd := &cobra.Command{
		Use:   "monthly",
		Short: "Daily pipeline + CVM ingest + GitHub release",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{
				Brapi: &brapi.Client{}, BQ: &bq.Client{}, GCS: &publish.GCSPublisher{},
				CVM: cvm.NewDownloader(cvm.DownloaderOptions{}),
			})
			if err != nil {
				return err
			}
			defer cleanup()

			month := time.Now().UTC().AddDate(0, -1, 0)
			if monthStr != "" {
				t, err := time.Parse("2006-01", monthStr)
				if err != nil {
					return err
				}
				month = t
			}

			opts := pipeline.MonthlyOptions{
				DailyOptions: pipeline.DailyOptions{OutDir: outDir},
				Month:        month,
			}
			report, runErr := pipeline.RunMonthly(ctx, d, opts)
			label := time.Now().UTC().Format("2006-01-02") + "-monthly"
			if err := uploadRunReport(ctx, d, report, label); err != nil {
				app.log.WarnContext(ctx, "run report upload failed", "err", err)
			}
			if runErr != nil {
				return runErr
			}

			// GitHub release (best-effort; logs on failure).
			if err := app.cfg.ValidateForRelease(); err == nil {
				pub, err := publish.NewGitHubPublisher(publish.GitHubOptions{
					Token: app.cfg.GitHubToken, Repo: app.cfg.GitHubRepo, Logger: app.log,
				})
				if err != nil {
					return err
				}
				tag := releaseTag
				if tag == "" {
					tag = "data-v" + month.Format("2006.01")
				}
				req := publish.ReleaseRequest{
					Tag:    tag,
					DBPath: outDir + "/reitbrazil.db",
					Body:   body,
				}
				if err := pub.PublishRelease(ctx, req); err != nil {
					return err
				}
			} else {
				app.log.WarnContext(ctx, "skipping github release", "err", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&monthStr, "month", "", "month YYYY-MM (default previous month)")
	cmd.Flags().StringVar(&outDir, "out-dir", "./out", "output directory for reitbrazil.db")
	cmd.Flags().StringVar(&releaseTag, "release-tag", "", "GitHub release tag (default data-vYYYY.MM)")
	cmd.Flags().StringVar(&body, "release-body", "", "release notes markdown")
	return cmd
}

func uploadRunReport(ctx context.Context, d pipeline.Deps, r *pipeline.RunReport, label string) error {
	if d.GCS == nil || r == nil {
		return nil
	}
	body, err := pipeline.ReportJSON(r)
	if err != nil {
		return err
	}
	return d.GCS.PublishRunReport(ctx, body, label)
}
