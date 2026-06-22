// Command wavespan-node is the WaveSpan data pod process. At M2 it opens local storage, joins
// the cluster via SWIM gossip, maintains a latency graph, and serves health/metrics plus the
// /admin/membership and /admin/latency endpoints. Replication and the KV API arrive in M3.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/cache"
	"github.com/cwire/wavespan/internal/config"
	"github.com/cwire/wavespan/internal/conflict"
	"github.com/cwire/wavespan/internal/cypher"
	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/kv"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/observability"
	"github.com/cwire/wavespan/internal/placement"
	"github.com/cwire/wavespan/internal/recordstore"
	global "github.com/cwire/wavespan/internal/replication/global"
	local "github.com/cwire/wavespan/internal/replication/local"
	"github.com/cwire/wavespan/internal/rpcopts"
	"github.com/cwire/wavespan/internal/security"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/ttl"
	"github.com/cwire/wavespan/internal/tunables"
	"github.com/cwire/wavespan/internal/ui"
	"github.com/cwire/wavespan/internal/vector"
	"github.com/cwire/wavespan/internal/vector/ann"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
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

	// Performance/behaviour tunables (koanf: defaults < the same YAML file's `tunables:` block <
	// WAVESPAN_TUNABLE_* env < runtime override). See internal/tunables + config/reference.yaml.
	tun, err := tunables.Load(*configPath, nil)
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

	// Runtime overrides (gossip-delta + persisted snapshot). The snapshot lives beside the data dir
	// (not in a column family) so it is read BEFORE the engine opens — letting Static engine
	// overrides take effect on restart. Applied at highest precedence over default/file/env.
	ovPath := tunables.OverridesPath(cfg.Storage.Path)
	overrides := tunables.NewOverrides(tun, cfg.MemberID, func(set []tunables.Override) {
		if err := tunables.SaveOverridesFile(ovPath, set); err != nil {
			logger.Warn("persist config overrides", "err", err)
		}
	})
	if snap, err := tunables.LoadOverridesFile(ovPath); err != nil {
		logger.Warn("load config overrides", "err", err)
	} else {
		overrides.LoadSnapshot(snap)
	}
	applyRuntimeTunables(tun) // gogc/memLimit after overrides so a persisted override applies

	store, err := storage.OpenWavesdbWith(cfg.Storage.Path, engineOptions(tun))
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = store.Close() }()
	storageUUID, err := storage.EnsureStorageUUID(store)
	if err != nil {
		return fmt.Errorf("storage uuid: %w", err)
	}

	// Transport security (design/15) tuned for cheap mTLS (design/27): one shared, pooled,
	// keepalive HTTP client is reused for every inter-node RPC so TLS handshakes are amortised to
	// ~zero; servers run mTLS (TLS 1.3 + session resumption + HTTP/2 ALPN). In insecureDevMode with
	// no certs this all degrades to the previous plaintext behaviour.
	tlsCfg := security.TLSConfig{
		CertFile: cfg.Security.CertFile, KeyFile: cfg.Security.KeyFile, CAFile: cfg.Security.CAFile,
		InsecureDevMode: cfg.Security.InsecureDevMode,
	}
	// Transport metrics (design/27): handshakes split by resumption (full vs resumed = reuse signal)
	// and a live open-connection gauge per server (few long-lived conns = good pooling).
	tlsHandshakes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tls_handshakes_total", Help: "completed server-side TLS handshakes, by whether the session resumed",
	}, []string{"resumed"})
	openConns := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_open_connections", Help: "currently open server connections, by server",
	}, []string{"server"})
	metrics.Registry.MustRegister(tlsHandshakes, openConns)
	tlsCfg.HandshakeObserver = func(resumed bool) {
		tlsHandshakes.WithLabelValues(strconv.FormatBool(resumed)).Inc()
	}
	httpClient, err := tlsCfg.NewHTTPClient(transportTuning(cfg))
	if err != nil {
		return fmt.Errorf("transport client: %w", err)
	}
	// On the plaintext dev/cluster path, multiplex inter-node RPCs over HTTP/2 cleartext (h2c) so
	// the origin+1 replication + anti-entropy calls don't serialize on HTTP/1.1 connections. With
	// mTLS, NewHTTPClient already negotiates HTTP/2 via ALPN.
	if cfg.Security.InsecureDevMode {
		httpClient = rpcopts.H2CClient()
	}
	serverMTLS, err := tlsCfg.ServerTLS() // machine link classes: data + gossip (nil in dev mode)
	if err != nil {
		return fmt.Errorf("server mTLS: %w", err)
	}
	adminTLS, err := tlsCfg.ServerTLSOptionalClient() // admin port also serves the browser UI
	if err != nil {
		return fmt.Errorf("admin tls: %w", err)
	}

	// Membership service (M2).
	self := membership.MemberFromConfig(cfg, storageUUID)
	transport := membership.NewConnectTransport(httpClient)
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
	// Runtime tunable overrides ride gossip: each node advertises the override set it knows and
	// LWW-merges peers' deltas into the registry, so a change made anywhere converges cluster-wide.
	svc.SetConfigHooks(
		func() []membership.ConfigDeltaWire {
			set := overrides.GossipSet() // cluster-scoped overrides only; node-local pins never gossip
			out := make([]membership.ConfigDeltaWire, 0, len(set))
			for _, o := range set {
				out = append(out, membership.ConfigDeltaWire{Key: o.Key, Value: o.Value, Version: o.Version, Origin: o.Origin})
			}
			return out
		},
		func(ds []membership.ConfigDeltaWire) {
			in := make([]tunables.Override, 0, len(ds))
			for _, d := range ds {
				in = append(in, tunables.Override{Key: d.Key, Value: d.Value, Version: d.Version, Origin: d.Origin})
			}
			overrides.ApplyRemote(in)
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
	replicator := local.NewConnectReplicator(httpClient)

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

	// "Replicate everywhere" namespaces (design/05 node sync): writes go to EVERY alive node, repair
	// targets the full alive set, and a joining node streams the existing records on bootstrap.
	everywhere := cfg.EverywhereNamespaces()
	everywhereFn := func(ns string) bool { return everywhere[ns] }
	fanout.SetEverywhere(everywhereFn)
	repair.SetEverywhere(everywhereFn)
	var everywhereList []string
	for ns := range everywhere {
		everywhereList = append(everywhereList, ns)
	}
	bootstrapper := local.NewBootstrapper(rstore, self, svc, local.NewConnectBackfill(httpClient), everywhereList)

	// Intra-cluster anti-entropy: best-effort origin+1 fanout can miss a holder of a concurrently
	// written key, and target-N repair only restores MISSING holders, not STALE ones. This pull-based
	// pass adopts the highest version of each local key from alive peers so concurrent same-key
	// writers converge across all replicas (design/13).
	intraAE := local.NewIntraAntiEntropy(rstore, self, svc, local.NewConnectPeerFetch(httpClient), intraAENamespaces(cfg))

	// M4 metrics: under-replication estimate (spot-churn alert signal) + repair queue depth.
	underReplicated := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kv_under_replicated_keys_estimate", Help: "keys below target durable-holder count"})
	repairQueueDepth := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kv_repair_queue_depth", Help: "pending repair items"})
	metrics.Registry.MustRegister(underReplicated, repairQueueDepth)
	targetHolders := policy.TargetNearbyReplicas + 1

	coord := kv.NewCoordinator(rstore, self, svc, svc.Graph(), replicator, policy, idem, holders, fanout, 2*time.Second)
	coord.SetOnStored(onStored) // advertise + notify on keys we originate
	cacheStore := cache.NewStore(rstore, nowMs)
	evictor := cache.NewEvictor(cacheStore, 10*time.Minute, nowMs)
	fetcher := cache.NewFetcher(self, cacheDir, svc, svc.Graph(), httpClient)
	subscriber := cache.NewSubscriber(self, cacheStore, fetcher, httpClient)
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

	// Vector store + indexes (M9/M10): exact + approximate search via the Cypher vector.* procedures;
	// raw vectors are ingested via VectorService and replicate through the global stream.
	vstore := vector.NewStore(store)
	var metas []*vector.IndexMeta
	for _, vi := range cfg.VectorIndexes {
		meta, verr := vector.ParseVectorIndexSpec(vector.IndexSpec{
			Name: vi.Name, Collection: vi.Collection, Metric: vi.Metric, Dimensions: vi.Dimensions,
			Label: vi.Label, Property: vi.Property, ExactEnabled: vi.ExactEnabled,
		})
		if verr != nil {
			return fmt.Errorf("vector index %q: %w", vi.Name, verr)
		}
		metas = append(metas, meta)
	}
	indexSet := vector.NewIndexSet(metas, ann.DefaultParams())
	// Rebuild the ANN indexes from the authoritative raw vectors so vectors written before a restart
	// are searchable again (the live indexes start empty otherwise — design/08 "Index rebuild").
	if err := indexSet.RebuildFromStore(vstore); err != nil {
		return fmt.Errorf("rebuild vector indexes: %w", err)
	}
	// Drain each live index's delta into its main HNSW segment on the vector.mergeInterval tunable, so
	// search isn't a growing linear scan over the delta. Re-reads the interval live on a Hot change.
	if mp := tun.Get("vector.mergeInterval"); mp != nil {
		for _, li := range indexSet.LiveIndexes() {
			go vector.NewMerger(li, 0).Run(ctx, mp.Duration())
		}
	}
	// Feed the vector store + ANN index from every durable apply (origin, replica, anti-entropy,
	// bootstrap, cross-cluster) so a vector written anywhere becomes searchable on each holder. Only
	// the LWW winner is applied; an older/losing write is ignored.
	rstore.SetApplyObserver(func(rec *wavespanv1.StoredRecord, won bool) {
		if !won || !vector.IsMutationNamespace(rec.GetNamespace()) {
			return
		}
		coll := vector.CollectionFromNamespace(rec.GetNamespace())
		id := string(rec.GetLogicalKey())
		if rec.GetTombstone() {
			_ = vstore.Delete(coll, id, rec.GetVersion())
			indexSet.OnWrite(&wavespanv1.VectorRecord{Collection: coll, VectorId: id, Tombstone: true})
			return
		}
		v, uerr := vector.Unwrap(rec)
		if uerr != nil {
			logger.Warn("vector apply: unwrap failed", "collection", coll, "err", uerr)
			return
		}
		v.Version = rec.GetVersion() // stamp the authoritative replication version onto the stored copy
		_ = vstore.Put(v)
		indexSet.OnWrite(v)
	})
	newGraphVersion := func() *wavespanv1.Version { return rstore.NextVersion().ToProto() }

	// Global active-active replication (M7/M10): tap origin KV writes AND raw-vector writes into a
	// per-peer out-log, serve the GlobalReplication API, ship via the sender, reconcile via
	// anti-entropy. Applied raw vectors route into the local vector store + ANN index (TS-084).
	var globalSrv *global.Server
	var startGlobal func()
	var vectorGlobalTap func(ns string, key []byte, rec *wavespanv1.StoredRecord)
	if cfg.GlobalReplication.Enabled() {
		applier := global.NewApplier(rstore, conflict.NewRegistry(), cfg.ConflictPolicy)
		// Note: applied vectors are fed to the local vector store + ANN index by the recordstore
		// apply-observer (wired above), which covers every apply path (origin, replica, anti-entropy,
		// bootstrap, cross-cluster) uniformly — so no separate vector sink is needed here.
		// A cross-cluster write lands on one local node; spread it within this cluster exactly like a
		// locally-originated write — so a replicate-everywhere namespace reaches every local node, and
		// the holder is advertised — instead of sitting on the single receiving node.
		applier.SetOnApply(func(ns string, key []byte, rec *wavespanv1.StoredRecord) {
			onStored(ns, key)
			if everywhereFn(ns) {
				fanout.Enqueue(local.FanoutJob{Namespace: ns, Key: key, Record: rec})
			}
		})
		ae := global.NewAntiEntropy(rstore)
		gmetrics := global.NewMetrics(metrics.Registry)
		globalSrv = global.NewServer(applier, ae)
		outlog := global.NewOutLog(store, cfg.GlobalReplication.OutLogDiskBudgetBytes)
		peers := cfg.GlobalReplication.Peers
		localOnly := cfg.LocalOnlyNamespaces() // "all" namespaces never cross to peer clusters
		appendToPeers := func(ns string, key []byte, rec *wavespanv1.StoredRecord) {
			if localOnly[ns] {
				return // replicate-everywhere-in-THIS-cluster only; do not ship globally
			}
			m := &wavespanv1.GlobalMutation{
				Id:        &wavespanv1.GlobalMutationId{ClusterId: self.ClusterID, MemberId: self.MemberID, WriterSequence: rec.GetVersion().GetWriterSequence()},
				Namespace: ns, Key: key, Record: rec, Partition: global.Partition(ns, key),
			}
			for _, p := range peers {
				_ = outlog.Append(p.ClusterID, m, cfg.GlobalDurabilityRequired(ns))
			}
		}
		coord.SetGlobalTap(appendToPeers)
		vectorGlobalTap = appendToPeers
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

	// SearchLocal serves the per-node fragment a query coordinator scatters to holders (design/08).
	vectorSvc := vector.NewService(vstore, newGraphVersion).
		WithHooks(indexSet.OnWrite, vectorGlobalTap).
		WithReplication(func(ctx context.Context, ns string, key, value []byte) error {
			// Route the vector write through the origin+1 coordinator so it replicates to holders and
			// taps cross-cluster; each holder's apply-observer feeds its HNSW.
			_, err := coord.Put(ctx, ns, key, value, nil, "")
			return err
		}, indexSet.CollectionDims).
		WithSearch(indexSet.Meta, indexSet.Live)

	// Vector search scatters SearchLocal to alive peers and merges, so a query spans the whole
	// cluster even when vectors are sharded (not fully replicated).
	vectorPeers := func() []cypher.Peer {
		var peers []cypher.Peer
		for _, m := range svc.Members() {
			if m.State == membership.StateAlive {
				peers = append(peers, cypher.Peer{Member: m.Member.MemberID, DataAddr: m.Member.DataAddr})
			}
		}
		return peers
	}

	// Graph + Cypher (M8/M9/M10): plans and executes against the local graph store, with vector search.
	graphStore := graph.NewStore(store)
	cypherSvc := cypher.NewService(graphStore, cfg.ClusterID, cfg.MemberID, newGraphVersion).
		WithVector(vstore, indexSet.Meta, indexSet.Live).
		WithVectorScatter(cypher.NewVectorScatter(cfg.MemberID, vectorPeers, httpClient)).
		WithKV(kv.NewCypherKV(reader, coord))

	// Data server on the data port: public KvService + Cypher + Vector + internal ReplicationService.
	dataMux := http.NewServeMux()
	dataMux.Handle(kvSvc.Handler())
	dataMux.Handle(replicaSrv.Handler())
	dataMux.Handle(observability.NewConfigServer(tun, overrides, cfg.ClusterID, cfg.MemberID).Handler()) // peer-reachable config reads + node-local set
	dataMux.Handle(cypherSvc.Handler())
	dataMux.Handle(vectorSvc.Handler())
	if globalSrv != nil {
		dataMux.Handle(globalSrv.Handler())
	}
	// Authorization (M12): enforce the role/surface matrix at the HTTP layer. In insecureDevMode the
	// identity middleware grants admin (dev/compose); in production the role comes from the verified
	// mTLS client certificate.
	dataIdentity := security.Identity{DevMode: cfg.Security.InsecureDevMode}
	dataSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Ports.Data), Handler: maybeH2C(dataIdentity.EnforceHTTP(dataMux), serverMTLS), ReadHeaderTimeout: 5 * time.Second, TLSConfig: serverMTLS, ConnState: connStateGauge(openConns.WithLabelValues("data"))}

	// Gossip server on the gossip port.
	gossipMux := http.NewServeMux()
	gossipMux.Handle(svc.GossipHandler())
	gossipSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Ports.Gossip), Handler: maybeH2C(gossipMux, serverMTLS), ReadHeaderTimeout: 5 * time.Second, TLSConfig: serverMTLS, ConnState: connStateGauge(openConns.WithLabelValues("gossip"))}

	// Admin server: health/metrics + membership/latency introspection.
	adminMux := observability.AdminMux(metrics, ready)
	adminMux.Handle("/admin/membership", membershipHandler(svc))
	adminMux.Handle("/admin/latency", latencyHandler(svc))
	enableProfiling(adminMux, logger) // net/http/pprof on the admin port when WAVESPAN_PROFILING_ENABLED

	// Embedded UI + ObservabilityService (M13): a gossip ring fed by a liveness tap, the streaming
	// introspection service, and the embedded SPA — all on the admin port behind admin auth.
	gossipRing := observability.NewGossipRing(4096)
	gossipTap := observability.NewGossipTap(gossipRing)
	svc.SetStateObserver(func(memberID string, st membership.State) {
		gossipTap.StateChange(memberID, livenessKind(st), st.String())
	})
	// Tap the live gossip traffic (probes, latency edges, holder summaries) so the inspector shows
	// continuous activity, not just the rare liveness transition.
	svc.SetGossipObserver(gossipTap)
	obsSvc := observability.NewObsService(gossipRing, svc, self, rstore).
		WithUnderReplicated(func() uint64 { return uint64(holders.UnderReplicatedEstimate(targetHolders, isAlive)) }).
		WithGraph(graphStore).       // enables the visual node explorer (GraphExplore)
		WithClusterScan(replicator). // cluster-wide Data Browser: fan InspectLocal out to all members
		WithKvWriter(func(ctx context.Context, target membership.Member, req *wavespanv1.PutRequest) (*wavespanv1.PutResult, error) {
			// Forward the UI's test write to the chosen coordinator's data port over the shared client.
			resp, err := wavespanv1connect.NewKvServiceClient(httpClient, "http://"+target.DataAddr).Put(ctx, connect.NewRequest(req))
			if err != nil {
				return nil, err
			}
			return resp.Msg, nil
		}).
		WithKvDeleter(func(ctx context.Context, target membership.Member, req *wavespanv1.DeleteRequest) (*wavespanv1.DeleteResult, error) {
			// Forward the Data Browser delete (tombstone) to the chosen coordinator's data port.
			resp, err := wavespanv1connect.NewKvServiceClient(httpClient, "http://"+target.DataAddr).Delete(ctx, connect.NewRequest(req))
			if err != nil {
				return nil, err
			}
			return resp.Msg, nil
		}).
		WithTunables(tun, overrides,
			func(ctx context.Context, target membership.Member) (*wavespanv1.NodeConfig, error) {
				// Read a peer's effective config over its data-port ConfigService (UI Config tab).
				resp, err := wavespanv1connect.NewConfigServiceClient(httpClient, "http://"+target.DataAddr).GetConfig(ctx, connect.NewRequest(&wavespanv1.GetConfigRequest{}))
				if err != nil {
					return nil, err
				}
				return resp.Msg, nil
			},
			func(ctx context.Context, target membership.Member, key, value string) (*wavespanv1.SetTunableResponse, error) {
				// Pin a node-local override on a chosen peer over its data-port ConfigService.
				resp, err := wavespanv1connect.NewConfigServiceClient(httpClient, "http://"+target.DataAddr).SetTunable(ctx, connect.NewRequest(&wavespanv1.SetTunableRequest{Key: key, Value: value}))
				if err != nil {
					return nil, err
				}
				return resp.Msg, nil
			})
	adminIdentity := security.Identity{DevMode: cfg.Security.InsecureDevMode}
	obsPath, obsHandler := obsSvc.Handler()
	adminMux.Handle(obsPath, adminIdentity.EnforceHTTP(obsHandler)) // ObservabilityService (admin auth)
	cypherPath, cypherHandler := cypherSvc.Handler()
	adminMux.Handle(cypherPath, adminIdentity.EnforceHTTP(cypherHandler))     // Cypher console (same origin as the UI)
	adminMux.Handle("/", adminIdentity.EnforceHTTP(ui.NewServer().Handler())) // SPA at root (health/metrics take precedence)
	adminSrv := &http.Server{Addr: cfg.Admin.Listen, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second, TLSConfig: adminTLS, ConnState: connStateGauge(openConns.WithLabelValues("admin"))}

	errCh := make(chan error, 3)
	go func() {
		logger.Info("gossip server listening", "addr", gossipSrv.Addr, "tls", gossipSrv.TLSConfig != nil)
		if err := serve(gossipSrv); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("gossip server: %w", err)
		}
	}()
	go func() {
		logger.Info("data server listening", "addr", dataSrv.Addr, "tls", dataSrv.TLSConfig != nil)
		if err := serve(dataSrv); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("data server: %w", err)
		}
	}()
	go func() {
		logger.Info("admin server listening", "addr", adminSrv.Addr, "tls", adminSrv.TLSConfig != nil)
		if err := serve(adminSrv); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	go intraAE.Run(ctx, 2*time.Second)
	go bootstrapper.Run(ctx, 2*time.Second) // stream "everywhere" namespaces from a peer on join
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

// connStateGauge tracks the number of currently open connections to a server: a new connection
// increments the gauge, and a closed/hijacked one decrements it. A small, stable value under load
// is the visible signature of connection reuse (design/27).
func connStateGauge(g prometheus.Gauge) func(net.Conn, http.ConnState) {
	return func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			g.Inc()
		case http.StateClosed, http.StateHijacked:
			g.Dec()
		}
	}
}

// serve runs an HTTP server over mTLS when a TLSConfig is present (certs are embedded in it, so the
// empty ListenAndServeTLS args are correct), and plaintext otherwise (insecureDevMode).
func serve(srv *http.Server) error {
	if srv.TLSConfig != nil {
		return srv.ListenAndServeTLS("", "")
	}
	return srv.ListenAndServe()
}

// transportTuning resolves the connection-pool tuning: start from the cheap-mTLS defaults and apply
// any explicit overrides from security.transport (design/27).
func transportTuning(cfg *config.Config) security.TransportTuning {
	t := security.DefaultTransportTuning()
	o := cfg.Security.Transport
	if o.MaxIdleConns != nil {
		t.MaxIdleConns = *o.MaxIdleConns
	}
	if o.MaxIdleConnsPerHost != nil {
		t.MaxIdleConnsPerHost = *o.MaxIdleConnsPerHost
	}
	if o.IdleConnTimeoutSeconds != nil {
		t.IdleConnTimeout = time.Duration(*o.IdleConnTimeoutSeconds) * time.Second
	}
	if o.TCPKeepAliveSeconds != nil {
		t.TCPKeepAlive = time.Duration(*o.TCPKeepAliveSeconds) * time.Second
	}
	if o.DialTimeoutSeconds != nil {
		t.DialTimeout = time.Duration(*o.DialTimeoutSeconds) * time.Second
	}
	if o.H2ReadIdleTimeoutSecond != nil {
		t.H2ReadIdleTimeout = time.Duration(*o.H2ReadIdleTimeoutSecond) * time.Second
	}
	if o.H2PingTimeoutSeconds != nil {
		t.H2PingTimeout = time.Duration(*o.H2PingTimeoutSeconds) * time.Second
	}
	return t
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

// maybeH2C enables HTTP/2 cleartext (multiplexed) on a plaintext server; a TLS server already gets
// HTTP/2 via ALPN, so it is returned unwrapped.
func maybeH2C(h http.Handler, tlsConfig *tls.Config) http.Handler {
	if tlsConfig != nil {
		return h
	}
	return rpcopts.H2CHandler(h)
}

// intraAENamespaces is the set of namespaces reconciled by intra-cluster anti-entropy (default +
// any configured namespaces, including keep-siblings ones).
func intraAENamespaces(cfg *config.Config) []string {
	ns := []string{"default"}
	seen := map[string]bool{"default": true}
	for _, n := range cfg.Namespaces {
		if n.Name != "" && !seen[n.Name] {
			seen[n.Name] = true
			ns = append(ns, n.Name)
		}
	}
	return ns
}

// livenessKind maps a membership liveness state to the gossip event kind surfaced in the UI.
func livenessKind(st membership.State) wavespanv1.GossipKind {
	switch st {
	case membership.StateSuspect:
		return wavespanv1.GossipKind_GOSSIP_SUSPECT
	case membership.StateAlive:
		return wavespanv1.GossipKind_GOSSIP_ALIVE
	case membership.StateUnreachable:
		return wavespanv1.GossipKind_GOSSIP_UNREACHABLE
	default:
		return wavespanv1.GossipKind_GOSSIP_MEMBERSHIP_DELTA
	}
}

// engineOptions resolves the storage.engine.* tunables into wavesdb open options.
func engineOptions(t *tunables.Registry) storage.EngineOptions {
	g := func(k string) *tunables.Param { return t.Get("storage.engine." + k) }
	return storage.EngineOptions{
		BlockCacheSize:       g("blockCacheSize").Int64(),
		MaxOpenSSTables:      g("maxOpenSSTables").Int(),
		MaxMemoryUsage:       g("maxMemoryUsage").Int64(),
		NumFlushThreads:      g("numFlushThreads").Int(),
		NumCompactionThreads: g("numCompactionThreads").Int(),
		WriteBufferSize:      g("writeBufferSize").Int(),
		LevelSizeRatio:       g("levelSizeRatio").Int(),
		MinLevels:            g("minLevels").Int(),
		KlogValueThreshold:   g("klogValueThreshold").Int(),
		Compression:          g("compression").String(),
		EnableBloomFilter:    g("enableBloomFilter").Bool(),
		BloomFPR:             g("bloomFpr").Float(),
		EnableBlockIndex:     g("enableBlockIndex").Bool(),
		IndexSampleRatio:     g("indexSampleRatio").Int(),
		BlockIndexPrefixLen:  g("blockIndexPrefixLen").Int(),
		SyncMode:             g("syncMode").String(),
		SyncInterval:         g("syncInterval").Duration(),
		SkipListMaxLevel:     g("skipListMaxLevel").Int(),
		SkipListProbability:  g("skipListProbability").Float(),
		DefaultIsolation:     g("defaultIsolation").String(),
		L1FileCountTrigger:   g("l1FileCountTrigger").Int(),
		L0StallThreshold:     g("l0StallThreshold").Int(),
		UseBTree:             g("useBTree").Bool(),
	}
}

// applyRuntimeTunables applies the runtime.* tunables to the Go runtime and registers OnApply hooks
// so a Hot change (later via gossip) takes effect live.
func applyRuntimeTunables(t *tunables.Registry) {
	gogc := t.Get("runtime.gogc")
	apply := func(p *tunables.Param) { debug.SetGCPercent(p.Int()) }
	apply(gogc)
	gogc.OnApply(apply)

	mem := t.Get("runtime.memLimit")
	applyMem := func(p *tunables.Param) {
		if v := p.Int64(); v > 0 {
			debug.SetMemoryLimit(v)
		}
	}
	applyMem(mem)
	mem.OnApply(applyMem)
}
