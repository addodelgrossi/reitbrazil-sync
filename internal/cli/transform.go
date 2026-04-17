package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
)

func newTransformCmd(app *App) *cobra.Command {
	var stage string
	cmd := &cobra.Command{
		Use:   "transform",
		Short: "Run BigQuery transforms (raw → canon)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{BQ: &bq.Client{}})
			if err != nil {
				return err
			}
			defer cleanup()

			if _, err := d.BQ.RunDDL(ctx); err != nil {
				return fmt.Errorf("ddl: %w", err)
			}

			prefixes := stagePrefix(stage)
			results, err := d.BQ.RunTransforms(ctx, prefixes...)
			for _, r := range results {
				fmt.Fprintf(cmd.OutOrStdout(), "ok  %s (%d bytes)\n", r.Name, r.BytesProcessed)
			}
			return err
		},
	}
	cmd.Flags().StringVar(&stage, "stage", "all", "funds|prices|dividends|fundamentals|snapshots|all")
	return cmd
}

func stagePrefix(stage string) []string {
	switch stage {
	case "funds":
		return []string{"10_"}
	case "prices":
		return []string{"11_"}
	case "dividends":
		return []string{"12_"}
	case "fundamentals":
		return []string{"13_"}
	case "snapshots":
		return []string{"20_"}
	case "", "all":
		return nil
	}
	return nil
}
