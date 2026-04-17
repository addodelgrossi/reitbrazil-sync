// Package cli wires every subcommand (cobra + fang) and bridges them
// to the pipeline, sources, export and publish layers. Shared global
// state (config, logger, context with run_id) is built once in NewRoot
// and injected into each subcommand via closures.
package cli
