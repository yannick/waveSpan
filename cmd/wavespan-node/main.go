// Command wavespan-node is the WaveSpan data pod process. At M0 it loads config, sets up
// logging and metrics, and serves /healthz, /readyz, and /metrics on the admin address.
// Distributed behaviour (storage, membership, replication) is added in later milestones.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cwire/wavespan/internal/config"
	"github.com/cwire/wavespan/internal/observability"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "wavespan-node:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to config YAML file")
	flag.Parse()

	cfg, err := config.Load(*configPath, nil)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Membership.Runtime, cfg.ClusterID, cfg.MemberID)
	metrics := observability.NewMetrics()
	ready := observability.NewReadiness()
	ready.Set(true) // M0: ready once config validates; later gated on membership join

	srv := &http.Server{
		Addr:              cfg.Admin.Listen,
		Handler:           observability.AdminMux(metrics, ready),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("admin server listening", "addr", cfg.Admin.Listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("admin server: %w", err)
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		logger.Info("clean shutdown complete")
		return nil
	}
}
