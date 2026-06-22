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

	"github.com/cwire/wavespan/internal/cache"
	"github.com/cwire/wavespan/internal/config"
	"github.com/cwire/wavespan/internal/conflict"
	"github.com/cwire/wavespan/internal/kv"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/observability"
	"github.com/cwire/wavespan/internal/placement"
	"github.com/cwire/wavespan/internal/recordstore"
	global "github.com/cwire/wavespan/internal/replication/global"
	local "github.com/cwire/wavespan/internal/replication/local"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/ttl"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/prometheus/client_golang/prometheus"
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

	// KV data path (M3): local record store, StoreReplica receiver, origin+1 coordinator.
	rstore := recordstore.NewStore(store, cfg.ClusterID, cfg.MemberID, version.NewClock(nil, 500), version.NewSequencer(0))
	idem := local.NewIdempotency(0)
	receiver := local.NewReceiver(rstore, cfg.MemberID, idem)
	subSource := cache.NewSubscriptionSource(rstore)
	replicaSrv := local.NewReplicaServer(receiver, rstore, cfg.MemberID, self.DataAddr, subSource)

	// Dynamic cache (M5): gossiped holder directory + closest-holder fetch + cache store.
	nowMs := func() int64 { return time.Now().UnixMilli() }
	cacheDir := cache.NewDirectory(cfg.MemberID, nowMs)
	svc.SetHolderHooks(
		func() membership.HolderSummaryWire {
			s := cacheDir.OwnSummary()
			return membership.HolderSummaryWire{MemberID: s.MemberID, Bloom: s.Bloom, GeneratedAtUnixMs: s.GeneratedAtUnixMs}
		},
		func(s membership.HolderSummaryWire) {
			cacheDir.ApplyPeerSummary(cache.HolderSummaryWire{MemberID: s.MemberID, Bloom: s.Bloom, GeneratedAtUnixMs: s.GeneratedAtUnixMs})
		},
	)
	onStored := func(ns string, key []byte) {
		cacheDir.AddHeldKey(ns, key) // advertise via the gossiped holder bloom
		subSource.Notify(ns, key)    // push the update to live cache subscribers
	}
	receiver.SetOnStored(onStored)
	policy := placement.Policy{
		TargetNearbyReplicas: cfg.Replication.Target(),
		MinAckNearbyReplicas: cfg.Replication.MinAck(),
		RequireDistinctNodes: true,
		Geo:                  placement.PreferLocalGeo, AllowSpilloverForDurability: true, ComplianceGeo: self.Geo,
	}
	replicator := local.NewConnectReplicator(http.DefaultClient)

	// Target-N fanout + repair engine (M4): converge to the target durable-holder count and
	// restore replicas under spot churn.
	holders := local.NewHolderDirectory(cfg.MemberID)
	fanout := local.NewFanout(self, svc, svc.Graph(), replicator, holders, policy, 2*time.Second)
	isAlive := func(id string) bool {
		for _, m := range svc.Members() {
			if m.Member.MemberID == id {
				return m.State == membership.StateAlive
			}
		}
		return false
	}
	churnHigh := func() bool {
		ms := svc.Members()
		bad := 0
		for _, m := range ms {
			if m.State != membership.StateAlive {
				bad++
			}
		}
		return len(ms) > 0 && float64(bad)/float64(len(ms)) > 0.5
	}
	repair := local.NewRepairEngine(self, svc, svc.Graph(), replicator, holders, rstore, policy,
		local.RepairConfig{IsAlive: isAlive, ChurnHigh: churnHigh, WriteTimeout: 2 * time.Second})
	fanout.SetRepair(repair)

	// M4 metrics: under-replication estimate (spot-churn alert signal) + repair queue depth.
	underReplicated := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kv_under_replicated_keys_estimate", Help: "keys below target durable-holder count"})
	repairQueueDepth := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kv_repair_queue_depth", Help: "pending repair items"})
	metrics.Registry.MustRegister(underReplicated, repairQueueDepth)
	targetHolders := policy.TargetNearbyReplicas + 1

	coord := kv.NewCoordinator(rstore, self, svc, svc.Graph(), replicator, policy, idem, holders, fanout, 2*time.Second)
	coord.SetOnStored(onStored) // advertise + notify on keys we originate
	cacheStore := cache.NewStore(rstore, nowMs)
	evictor := cache.NewEvictor(cacheStore, 10*time.Minute, nowMs)
	fetcher := cache.NewFetcher(self, cacheDir, svc, svc.Graph(), http.DefaultClient)
	subscriber := cache.NewSubscriber(self, cacheStore, fetcher, http.DefaultClient)
	reader := kv.NewReader(rstore, self).WithCache(fetcher, cacheStore).WithSubscriber(subscriber)
	// Range scans (M6): routed-eventual via ScanLocal on holders; cache-complete gated by certs.
	scanner := kv.NewScanner(rstore, self, svc, replicator, cache.NewCertStore(nil))
	kvSvc := kv.NewService(coord, reader, self).WithScanner(scanner)
	// Lazy TTL sweeper (M6): tombstone expired keys via a coordinated delete (replicates).
	ttlTombstones := prometheus.NewCounter(prometheus.CounterOpts{Name: "kv_ttl_tombstones_written_total", Help: "tombstones emitted by the TTL sweeper"})
	metrics.Registry.MustRegister(ttlTombstones)
	sweeper := ttl.NewSweeper(rstore, func(c context.Context, ns string, key []byte) error {
		if _, err := coord.Delete(c, ns, key, ""); err != nil {
			return err
		}
		ttlTombstones.Inc()
		return nil
	}, nowMs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	subscriber.SetBaseContext(ctx) // subscriptions live for the node lifetime, not per-request

	// Global active-active replication (M7): tap origin writes into a per-peer out-log, serve the
	// GlobalReplication API, ship via the sender, and reconcile via anti-entropy.
	var globalSrv *global.Server
	var startGlobal func()
	if cfg.GlobalReplication.Enabled() {
		applier := global.NewApplier(rstore, conflict.NewRegistry(), cfg.ConflictPolicy)
		ae := global.NewAntiEntropy(rstore)
		gmetrics := global.NewMetrics(metrics.Registry)
		globalSrv = global.NewServer(applier, ae)
		outlog := global.NewOutLog(store, cfg.GlobalReplication.OutLogDiskBudgetBytes)
		peers := cfg.GlobalReplication.Peers
		coord.SetGlobalTap(func(ns string, key []byte, rec *wavespanv1.StoredRecord) {
			m := &wavespanv1.GlobalMutation{
				Id:        &wavespanv1.GlobalMutationId{ClusterId: self.ClusterID, MemberId: self.MemberID, WriterSequence: rec.GetVersion().GetWriterSequence()},
				Namespace: ns, Key: key, Record: rec, Partition: global.Partition(ns, key),
			}
			for _, p := range peers {
				_ = outlog.Append(p.ClusterID, m, cfg.GlobalDurabilityRequired(ns))
			}
		})
		sender := global.NewSender(outlog, peers, nil)
		nsList := []string{"default"}
		for _, n := range cfg.Namespaces {
			nsList = append(nsList, n.Name)
		}
		reconciler := global.NewReconciler(ae, applier, outlog, peers, nsList, nil, gmetrics)
		aeInterval := time.Duration(cfg.GlobalReplication.AntiEntropyIntervalSeconds) * time.Second
		startGlobal = func() {
			go sender.Run(ctx, time.Second)
			go reconciler.Run(ctx, aeInterval)
		}
		logger.Info("global replication enabled", "peers", len(peers))
	}

	// Data server on the data port: public KvService + internal ReplicationService.
	dataMux := http.NewServeMux()
	dataMux.Handle(kvSvc.Handler())
	dataMux.Handle(replicaSrv.Handler())
	if globalSrv != nil {
		dataMux.Handle(globalSrv.Handler())
	}
	dataSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Ports.Data), Handler: dataMux, ReadHeaderTimeout: 5 * time.Second}

	// Gossip server on the gossip port.
	gossipMux := http.NewServeMux()
	gossipMux.Handle(svc.GossipHandler())
	gossipSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Ports.Gossip), Handler: gossipMux, ReadHeaderTimeout: 5 * time.Second}

	// Admin server: health/metrics + membership/latency introspection.
	adminMux := observability.AdminMux(metrics, ready)
	adminMux.Handle("/admin/membership", membershipHandler(svc))
	adminMux.Handle("/admin/latency", latencyHandler(svc))
	adminSrv := &http.Server{Addr: cfg.Admin.Listen, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 3)
	go func() {
		logger.Info("gossip server listening", "addr", gossipSrv.Addr)
		if err := gossipSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("gossip server: %w", err)
		}
	}()
	go func() {
		logger.Info("data server listening", "addr", dataSrv.Addr)
		if err := dataSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("data server: %w", err)
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
	// Background target-N fanout and repair workers (M4) + dynamic-cache evictor (M5).
	go fanout.Run(ctx)
	go repair.Run(ctx, 200*time.Millisecond)
	go evictor.Run(ctx, time.Minute)
	go sweeper.Run(ctx, 30*time.Second)
	if startGlobal != nil {
		startGlobal()
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				underReplicated.Set(float64(holders.UnderReplicatedEstimate(targetHolders, isAlive)))
				repairQueueDepth.Set(float64(repair.QueueDepth()))
			}
		}
	}()
	// Reconcile loop: feed newly dead/unreachable members to the repair engine so their keys
	// are re-replicated; reset tracking when a member returns.
	go func() {
		processed := map[string]bool{}
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, m := range svc.Members() {
					id := m.Member.MemberID
					switch m.State {
					case membership.StateUnreachable, membership.StateDead:
						if !processed[id] {
							processed[id] = true
							repair.OnMemberDead(id)
						}
					case membership.StateAlive:
						delete(processed, id)
					}
				}
			}
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = gossipSrv.Shutdown(shutdownCtx)
		_ = dataSrv.Shutdown(shutdownCtx)
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
