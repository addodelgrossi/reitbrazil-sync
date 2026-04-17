package cli

import (
	"context"

	"github.com/addodelgrossi/reitbrazil-sync/internal/export"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
)

// pipelineExport bridges the CLI's export subcommand to the readers
// defined in the pipeline package. Keeps the read logic in one place
// (it also runs inside pipeline.RunDaily).
func pipelineExport(ctx context.Context, d pipeline.Deps, w *export.Writer) error {
	p := d.BQ.Project()
	ds := d.BQ.DatasetCanon()
	if err := w.WriteFunds(ctx, pipeline.ReadFunds(ctx, d.BQ, p, ds)); err != nil {
		return err
	}
	if err := w.WritePrices(ctx, pipeline.ReadPrices(ctx, d.BQ, p, ds)); err != nil {
		return err
	}
	if err := w.WriteDividends(ctx, pipeline.ReadDividends(ctx, d.BQ, p, ds)); err != nil {
		return err
	}
	if err := w.WriteFundamentals(ctx, pipeline.ReadFundamentals(ctx, d.BQ, p, ds)); err != nil {
		return err
	}
	if err := w.WriteSnapshots(ctx, pipeline.ReadSnapshots(ctx, d.BQ, p, ds)); err != nil {
		return err
	}
	return nil
}
