// Command reitbrazilctl is the CLI entrypoint for reitbrazil-sync.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/charmbracelet/fang"

	"github.com/addodelgrossi/reitbrazil-sync/internal/cli"
	"github.com/addodelgrossi/reitbrazil-sync/internal/logging"
)

// version is injected at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, _ = logging.WithRunID(ctx)

	app := cli.NewApp(version)
	if err := fang.Execute(ctx, app.Root()); err != nil {
		fmt.Fprintf(os.Stderr, "reitbrazilctl: %v\n", err)
		os.Exit(1)
	}
}
