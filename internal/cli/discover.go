package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

func newDiscoverCmd(app *App) *cobra.Command {
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Resolve the FII universe (brapi ∩ CVM B3-listed)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{
				Brapi: &brapi.Client{},
				CVM:   &cvm.Downloader{},
			})
			if err != nil {
				return err
			}
			defer cleanup()

			funds, stats, err := pipeline.BuildFIIUniverse(ctx, d, 0)
			if err != nil {
				return err
			}

			writeUniverseSummary(cmd.ErrOrStderr(), stats)

			if dryRun {
				for _, f := range funds {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), f.Ticker)
				}
				return nil
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "discovered %d FIIs\n", len(funds))
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print tickers instead of a summary")
	return cmd
}

func writeUniverseSummary(w io.Writer, stats pipeline.UniverseStats) {
	_, _ = fmt.Fprint(w, universeSummary(stats))
}

func universeSummary(stats pipeline.UniverseStats) string {
	if stats.FallbackToCVM {
		return fmt.Sprintf("universe: %d FIIs (CVM B3-listed fallback; brapi list returned %d; CVM %d with ticker)\n",
			stats.Intersection, stats.BrapiCount, stats.CVMB3WithTicker)
	}
	return fmt.Sprintf("universe: %d FIIs (brapi %d ∩ CVM %d B3-listed; dropped %d brapi tickers as non-FII)\n",
		stats.Intersection, stats.BrapiCount, stats.CVMB3WithTicker, stats.BrapiDropped)
}
