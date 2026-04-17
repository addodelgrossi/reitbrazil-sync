package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/export"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
)

func newExportCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export the SQLite artifact consumed by the MCP",
	}
	cmd.AddCommand(newExportSQLiteCmd(app))
	return cmd
}

func newExportSQLiteCmd(app *App) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "sqlite",
		Short: "Read canon tables and write reitbrazil.db",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{BQ: &bq.Client{}})
			if err != nil {
				return err
			}
			defer cleanup()

			db, err := export.Open(output, true)
			if err != nil {
				return err
			}
			defer db.Close()

			w := export.NewWriter(db, export.WriterOptions{Logger: app.log})
			if err := pipelineExport(ctx, d, w); err != nil {
				return err
			}
			if err := w.WriteDataSources(ctx, []export.DataSource{
				{Name: "brapi", LastRefreshedAt: time.Now().UTC()},
				{Name: "cvm", LastRefreshedAt: time.Now().UTC()},
			}); err != nil {
				return err
			}
			if err := w.Vacuum(ctx); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"wrote %s  funds=%d prices=%d dividends=%d fundamentals=%d snapshots=%d\n",
				output, w.Counts.Funds, w.Counts.Prices, w.Counts.Dividends,
				w.Counts.Fundamentals, w.Counts.Snapshots)
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "./out/reitbrazil.db", "path to write SQLite to")
	return cmd
}
