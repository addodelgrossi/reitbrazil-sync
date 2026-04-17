package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/pipeline"
	"github.com/addodelgrossi/reitbrazil-sync/internal/publish"
)

func newPublishCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish",
		Short: "Upload the SQLite artifact (gcs, release)",
	}
	cmd.AddCommand(newPublishGCSCmd(app))
	cmd.AddCommand(newPublishReleaseCmd(app))
	return cmd
}

func newPublishGCSCmd(app *App) *cobra.Command {
	var input string
	cmd := &cobra.Command{
		Use:   "gcs",
		Short: "Upload reitbrazil.db to gs://reitbrazil-db/latest",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			if _, err := os.Stat(input); err != nil {
				return fmt.Errorf("input: %w", err)
			}
			d, cleanup, err := app.buildDeps(ctx, pipeline.Deps{GCS: &publish.GCSPublisher{}})
			if err != nil {
				return err
			}
			defer cleanup()

			meta := publish.Metadata{
				Version:     app.root.Version,
				GeneratedAt: time.Now().UTC(),
			}
			if err := d.GCS.PublishSQLite(ctx, input, meta); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"published gs://%s/%s\n", app.cfg.GCSBucket, app.cfg.GCSKeyLatest)
			return nil
		},
	}
	cmd.Flags().StringVar(&input, "input", "./out/reitbrazil.db", "local SQLite path")
	return cmd
}

func newPublishReleaseCmd(app *App) *cobra.Command {
	var (
		input  string
		tagArg string
		body   string
	)
	cmd := &cobra.Command{
		Use:   "release",
		Short: "Create a monthly GitHub release with reitbrazil.db as asset",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			if err := app.cfg.ValidateForRelease(); err != nil {
				return err
			}
			if _, err := os.Stat(input); err != nil {
				return fmt.Errorf("input: %w", err)
			}
			pub, err := publish.NewGitHubPublisher(publish.GitHubOptions{
				Token: app.cfg.GitHubToken, Repo: app.cfg.GitHubRepo, Logger: app.log,
			})
			if err != nil {
				return err
			}
			tag := tagArg
			if tag == "" {
				tag = "data-v" + time.Now().UTC().Format("2006.01")
			}
			req := publish.ReleaseRequest{
				Tag:          tag,
				DBPath:       input,
				MetadataPath: filepath.Join(filepath.Dir(input), "metadata.json"),
				Body:         body,
			}
			return pub.PublishRelease(ctx, req)
		},
	}
	cmd.Flags().StringVar(&input, "input", "./out/reitbrazil.db", "local SQLite path")
	cmd.Flags().StringVar(&tagArg, "tag", "", "release tag (default: data-vYYYY.MM)")
	cmd.Flags().StringVar(&body, "body", "", "release notes markdown")
	return cmd
}
