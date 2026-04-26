package cli

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/export"
	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"

	_ "modernc.org/sqlite"
)

func newDoctorCmd(app *App) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Validate environment, credentials, and in-memory migrations",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			var issues []string

			// 1. config presence
			for _, check := range []struct {
				name string
				fn   func() error
			}{
				{"config.fetch", app.cfg.ValidateForFetch},
				{"config.bigquery", app.cfg.ValidateForBigQuery},
				{"config.publish", app.cfg.ValidateForPublish},
			} {
				if err := check.fn(); err != nil {
					issues = append(issues, fmt.Sprintf("%s: %v", check.name, err))
				} else {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ok  %s\n", check.name)
				}
			}

			// 2. brapi ping
			if app.cfg.BrapiToken != "" {
				if err := pingBrapi(ctx, app.cfg.BrapiToken); err != nil {
					issues = append(issues, fmt.Sprintf("brapi.ping: %v", err))
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ok  brapi.ping")
				}
			}

			// 3. BigQuery auth (dataset Metadata)
			if err := app.cfg.ValidateForBigQuery(); err == nil {
				if err := pingBQ(ctx, app); err != nil {
					issues = append(issues, fmt.Sprintf("bq.ping: %v", err))
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ok  bq.ping")
				}
			}

			// 4. SQLite migrations in :memory:
			if err := smokeSQLite(); err != nil {
				issues = append(issues, fmt.Sprintf("sqlite.migrations: %v", err))
			} else {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "ok  sqlite.migrations")
			}

			if len(issues) > 0 {
				for _, i := range issues {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "FAIL %s\n", i)
				}
				return fmt.Errorf("doctor found %d issue(s)", len(issues))
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "all checks passed")
			return nil
		},
	}
}

func pingBrapi(ctx context.Context, token string) error {
	cli, err := brapi.NewClient(brapi.ClientOptions{
		Token: token, RPS: 2, MaxRetries: 0,
	})
	if err != nil {
		return err
	}
	count := 0
	for _, err := range cli.FetchList(ctx) {
		if err != nil {
			return err
		}
		count++
		if count >= 1 {
			break
		}
	}
	if count == 0 {
		ticker, err := model.ParseTicker("HGLG11")
		if err != nil {
			return err
		}
		if _, err := cli.FetchFundamentals(ctx, ticker); err != nil {
			return fmt.Errorf("empty fund list and quote ping failed: %w", err)
		}
	}
	return nil
}

func pingBQ(ctx context.Context, app *App) error {
	d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{BQ: &bq.Client{}})
	if err != nil {
		return err
	}
	defer cleanup()
	return d.BQ.Bootstrap(ctx)
}

func smokeSQLite() error {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return export.ApplyMigrations(db)
}
