// Package app holds the gateway's startup logic, shared by cmd/butterfly-sn and the end-to-end tests.
package app

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"

	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/gateway"
)

// Run starts the gateway with the arguments in args, logs to stdout and returns the exit status:
// 0 after ctx is cancelled and the gateway stopped, 1 on configuration or runtime failure, 2 on bad usage.
func Run(ctx context.Context, args []string, stdout io.Writer) int {
	logger := slog.New(slog.NewTextHandler(stdout, nil))

	fs := flag.NewFlagSet("butterfly-sn", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "path to the YAML configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		logger.Error("missing required -config flag")
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return 1
	}
	logger = slog.New(slog.NewTextHandler(stdout, &slog.HandlerOptions{Level: cfg.SlogLevel()}))

	gw, err := gateway.New(cfg, logger)
	if err != nil {
		logger.Error("cannot start gateway", "error", err)
		return 1
	}
	logger.Info("gateway started", "addr", gw.Addr(), "broker", cfg.Broker)

	if err := gw.Run(ctx); err != nil {
		logger.Error("gateway stopped with error", "error", err)
		return 1
	}
	logger.Info("gateway stopped")
	return 0
}
