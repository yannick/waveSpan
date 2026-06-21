// Command wavespan-node is the WaveSpan data pod process. At M2 it opens local storage, joins
// the cluster via SWIM gossip, maintains a latency graph, and serves health/metrics plus the
// /admin/membership and /admin/latency endpoints. Replication and the KV API arrive in M3.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cwire/wavespan/internal/config"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/observability"
	"github.com/cwire/wavespan/internal/storage"
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

	// Local storage + durable storage identity (M1).
	if err := os.MkdirAll(cfg.Storage.Path, 0o755); err != nil {
		return fmt.Errorf("create storage dir: %w", err)
	}
	store, err := storage.OpenWavesdb(cfg.Storage.Path)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = store.Close() }()
	storageUUID, err := storage.EnsureStorageUUID(store)
	if err != nil {
		return fmt.Errorf("storage uuid: %w", err)
	}

	// Membership service (M2).
	self := membership.MemberFromConfig(cfg, storageUUID)
	transport := membership.NewConnectTransport(http.DefaultClient)
	disc := membership.NewDiscovery(cfg, self.GossipAddr)
	svc := membership.NewService(self, disc, transport, membership.DefaultServiceConfig())
	logger.Info("member identity", "storage_uuid", storageUUID, "gossip_addr", self.GossipAddr,
		"zone", self.Zone, "region", self.Region, "geo", self.Geo)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Gossip server on the gossip port.
	gossipMux := http.NewServeMux()
	gossipMux.Handle(svc.GossipHandler())
	gossipSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Ports.Gossip), Handler: gossipMux, ReadHeaderTimeout: 5 * time.Second}

	// Admin server: health/metrics + membership/latency introspection.
	adminMux := observability.AdminMux(metrics, ready)
	adminMux.Handle("/admin/membership", membershipHandler(svc))
	adminMux.Handle("/admin/latency", latencyHandler(svc))
	adminSrv := &http.Server{Addr: cfg.Admin.Listen, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("gossip server listening", "addr", gossipSrv.Addr)
		if err := gossipSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("gossip server: %w", err)
		}
	}()
	go func() {
		logger.Info("admin server listening", "addr", adminSrv.Addr)
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("admin server: %w", err)
		}
	}()

	// Join + gossip loop. Ready once the loop is running (later milestones gate on quorum).
	go func() {
		ready.Set(true)
		svc.Run(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = gossipSrv.Shutdown(shutdownCtx)
		_ = adminSrv.Shutdown(shutdownCtx)
		logger.Info("clean shutdown complete")
		return nil
	}
}

func membershipHandler(svc *membership.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		type entry struct {
			MemberID    string `json:"memberId"`
			State       string `json:"state"`
			Incarnation uint64 `json:"incarnation"`
			GossipAddr  string `json:"gossipAddr"`
			Zone        string `json:"zone"`
			Region      string `json:"region"`
			Geo         string `json:"geo"`
			StorageUUID string `json:"storageUuid"`
		}
		var out []entry
		for _, m := range svc.Members() {
			out = append(out, entry{
				MemberID: m.Member.MemberID, State: m.State.String(), Incarnation: m.Incarnation,
				GossipAddr: m.Member.GossipAddr, Zone: m.Member.Zone, Region: m.Member.Region,
				Geo: m.Member.Geo, StorageUUID: m.Member.StorageUUID,
			})
		}
		writeJSON(w, out)
	}
}

func latencyHandler(svc *membership.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		self := svc.Self().MemberID
		type edge struct {
			From        string  `json:"from"`
			To          string  `json:"to"`
			EWMARttMs   float64 `json:"ewmaRttMs"`
			P95RttMs    float64 `json:"p95RttMs"`
			PacketLoss  float64 `json:"packetLoss"`
			SampleCount uint32  `json:"sampleCount"`
		}
		var out []edge
		for _, e := range svc.LatencyEdges() {
			out = append(out, edge{
				From: self, To: e.To, EWMARttMs: e.EWMARttMs, P95RttMs: e.P95RttMs,
				PacketLoss: e.PacketLoss, SampleCount: e.SampleCount,
			})
		}
		writeJSON(w, out)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
