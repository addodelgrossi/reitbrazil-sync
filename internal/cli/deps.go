package cli

import (
	"context"
	"fmt"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/publish"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/brapi"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

// buildDeps constructs the Deps bundle from app state. Callers must
// call the returned cleanup func on exit.
func (a *App) buildDeps(ctx context.Context, want pipeline.Deps) (pipeline.Deps, func(), error) {
	cleanup := func() {}
	d := pipeline.Deps{Cfg: a.cfg, Log: a.log}

	if want.Brapi != nil || needBrapi(want) {
		if err := a.cfg.ValidateForFetch(); err != nil {
			return d, cleanup, err
		}
		cli, err := brapi.NewClient(brapi.ClientOptions{
			Token:   a.cfg.BrapiToken,
			RPS:     a.cfg.RateLimitRPS,
			Timeout: a.cfg.HTTPTimeout,
			Logger:  a.log,
		})
		if err != nil {
			return d, cleanup, fmt.Errorf("brapi: %w", err)
		}
		d.Brapi = cli
	}
	if want.BQ != nil || needBQ(want) {
		if err := a.cfg.ValidateForBigQuery(); err != nil {
			return d, cleanup, err
		}
		c, err := bq.New(ctx, bq.ClientOptions{
			ProjectID:    a.cfg.GCPProject,
			DatasetRaw:   a.cfg.BQDatasetRaw,
			DatasetCanon: a.cfg.BQDatasetCanon,
			Location:     a.cfg.BQLocation,
			Logger:       a.log,
		})
		if err != nil {
			return d, cleanup, fmt.Errorf("bq: %w", err)
		}
		d.BQ = c
		prev := cleanup
		cleanup = func() {
			prev()
			_ = c.Close()
		}
	}
	if want.GCS != nil || needGCS(want) {
		if err := a.cfg.ValidateForPublish(); err != nil {
			return d, cleanup, err
		}
		p, err := publish.NewGCSPublisher(ctx, publish.GCSOptions{
			Bucket:        a.cfg.GCSBucket,
			KeyLatest:     a.cfg.GCSKeyLatest,
			KeyMetadata:   a.cfg.GCSKeyMetadata,
			HistoryPrefix: a.cfg.GCSPrefixHistory,
			Logger:        a.log,
		})
		if err != nil {
			return d, cleanup, fmt.Errorf("gcs: %w", err)
		}
		d.GCS = p
		prev := cleanup
		cleanup = func() {
			prev()
			_ = p.Close()
		}
	}
	if want.CVM != nil {
		d.CVM = cvm.NewDownloader(cvm.DownloaderOptions{})
	}
	return d, cleanup, nil
}

// these helpers use sentinels on the want struct to indicate which deps
// the caller wants us to build. They are deliberately simple — a
// non-nil "want.X" means "build an X even if you'd otherwise skip".
func needBrapi(_ pipeline.Deps) bool { return false }
func needBQ(_ pipeline.Deps) bool    { return false }
func needGCS(_ pipeline.Deps) bool   { return false }
