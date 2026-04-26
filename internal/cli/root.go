package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/addodelgrossi/reitbrazil-sync/internal/config"
	"github.com/addodelgrossi/reitbrazil-sync/internal/logging"
)

// globalFlags holds flags shared by every subcommand.
type globalFlags struct {
	logLevel   string
	logFormat  string
	configPath string
	envFile    string
	projectID  string
	dryRun     bool
}

// App is the root CLI wrapper. Not exported as a cobra.Command directly
// so we can attach context/cleanup hooks around Execute.
type App struct {
	root *cobra.Command
	gf   *globalFlags
	cfg  *config.Config
	log  *slog.Logger
}

// NewApp returns a configured App.
func NewApp(version string) *App {
	gf := &globalFlags{}
	app := &App{gf: gf}

	root := &cobra.Command{
		Use:           "reitbrazilctl",
		Short:         "Data pipeline CLI for reitbrazil-sync",
		Long:          "reitbrazilctl ingests real FII data, materialises canonical tables in BigQuery, and publishes a SQLite snapshot for the reitbrazil MCP server.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(gf.envFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			if gf.projectID != "" {
				cfg.GCPProject = gf.projectID
			}
			if gf.logLevel != "" {
				cfg.LogLevel = gf.logLevel
			}
			if gf.logFormat != "" {
				cfg.LogFormat = gf.logFormat
			}
			log := logging.New(logging.Options{Level: cfg.LogLevel, Format: cfg.LogFormat})
			app.cfg = cfg
			app.log = log
			slog.SetDefault(log)
			return nil
		},
	}

	root.PersistentFlags().StringVar(&gf.logLevel, "log-level", "", "log level (debug|info|warn|error)")
	root.PersistentFlags().StringVar(&gf.logFormat, "log-format", "", "log format (json|text)")
	root.PersistentFlags().StringVar(&gf.envFile, "env-file", ".env", "path to .env (defaults to ./ .env)")
	root.PersistentFlags().StringVar(&gf.configPath, "config", "", "path to optional config.yaml")
	root.PersistentFlags().StringVar(&gf.projectID, "project-id", "", "override GCP project id")
	root.PersistentFlags().BoolVar(&gf.dryRun, "dry-run", false, "skip writes to BigQuery/GCS and log intent instead")

	app.root = root

	// Attach subcommands. Each constructor closes over app so it can
	// reach the shared config/logger after PersistentPreRunE ran.
	root.AddCommand(newDoctorCmd(app))
	root.AddCommand(newDiscoverCmd(app))
	root.AddCommand(newCoverageCmd(app))
	root.AddCommand(newFetchCmd(app))
	root.AddCommand(newTransformCmd(app))
	root.AddCommand(newExportCmd(app))
	root.AddCommand(newPublishCmd(app))
	root.AddCommand(newRunCmd(app))

	return app
}

// Root returns the underlying cobra command (useful for fang.Execute).
func (a *App) Root() *cobra.Command { return a.root }

// Execute runs the CLI with signal-based cancellation.
func (a *App) Execute(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, _ = logging.WithRunID(ctx)
	return a.root.ExecuteContext(ctx)
}

// parseTickerList splits a comma-separated ticker list. Whitespace is
// trimmed, empties are dropped.
func parseTickerList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// usageErr prints the command's usage alongside err. Used by subcommands
// to keep error-handling consistent without duplicating help text.
func usageErr(cmd *cobra.Command, err error) error {
	_ = cmd.Usage()
	fmt.Fprintln(os.Stderr)
	return err
}
