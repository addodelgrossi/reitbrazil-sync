package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"
)

func newDiscoverCmd(app *App) *cobra.Command {
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Walk the brapi universe and land fresh fund entries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{Brapi: &brapi.Client{}, BQ: &bq.Client{}})
			if err != nil {
				return err
			}
			defer cleanup()

			count := 0
			for f, err := range d.Brapi.FetchList(ctx) {
				if err != nil {
					return err
				}
				count++
				if dryRun {
					fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", f.Ticker, f.Name, f.Segment)
				}
			}
			if !dryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "discovered %d funds\n", count)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print funds instead of landing")
	return cmd
}
