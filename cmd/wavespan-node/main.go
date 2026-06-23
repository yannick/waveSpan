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
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/cache"
	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/conflict"
	"github.com/yannick/wavespan/internal/cypher"
	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/kv"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/observability"
	"github.com/yannick/wavespan/internal/placement"
	"github.com/yannick/wavespan/internal/recordstore"
	global "github.com/yannick/wavespan/internal/replication/global"
	local "github.com/yannick/wavespan/internal/replication/local"
	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/security"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/ttl"
	"github.com/yannick/wavespan/internal/tunables"
	"github.com/yannick/wavespan/internal/ui"
	"github.com/yannick/wavespan/internal/vector"
	"github.com/yannick/wavespan/internal/vector/ann"
	"github.com/yannick/wavespan/internal/vector/quantizer"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
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

	// Throughput observability (QPS/reads/writes via a Connect interceptor on every service; wire
	// bandwidth + accepted connections via a listener wrapper). Installed before any handler/listener
	// is built so they all carry the instrumentation.
	rpcopts.InstallMetrics(metrics.Registry)
	netMetrics := observability.NewNetMetrics(metrics.Registry)
	committedTxns := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavespan_transactions_total", Help: "Durable record commits applied (origin + replica + anti-entropy + bootstrap + cross-cluster).",
	})
	metrics.Registry.MustRegister(committedTxns)

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
	// Coarse quantizers + held-bucket directory for kNN routing (design/29 Phase 2): each collection
	// gets a deterministic LSH quantizer; the directory tracks which buckets this node (and peers) hold.
	quantSet := vector.NewQuantSet(metas, vectorNumBuckets)
	bucketDir := vector.NewBucketDir(self.MemberID)
	vmetrics := newVectorMetrics(metrics.Registry)

	// Feed the vector store + ANN index from every durable apply (origin, replica, anti-entropy,
	// bootstrap, cross-cluster) so a vector written anywhere becomes searchable on each holder. Only
	// the LWW winner is applied; an older/losing write is ignored.
	rstore.SetApplyObserver(func(rec *wavespanv1.StoredRecord, won bool) {
		committedTxns.Inc() // every durable commit (all write paths) — the TPS counter
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
		if qz, ok := quantSet.For(coll); ok {
			bucketDir.AddOwn(coll, qz.Version(), qz.Bucket(v.GetValues())) // advertise the bucket we now hold
		}
	})
	// Periodically recompute this node's held-bucket set from the store so buckets that emptied
	// (deletes/migration) are de-advertised — the explicit-set property the holder bloom lacks.
	go recomputeHeldBuckets(ctx, vstore, quantSet, bucketDir, metas, vmetrics, 30*time.Second)
	// Gossip the held-bucket directory so a kNN query can route to the holders of its probed buckets.
	svc.SetBucketHooks(
		func() []membership.HeldBucketWire {
			adverts := bucketDir.OwnAdvert(time.Now().UnixMilli())
			out := make([]membership.HeldBucketWire, 0, len(adverts))
			for _, a := range adverts {
				out = append(out, membership.HeldBucketWire{MemberID: self.MemberID, Collection: a.Collection, QVer: a.QVer, Buckets: a.Buckets, GeneratedAtUnixMs: a.GeneratedAtUnixMs})
			}
			return out
		},
		func(bs []membership.HeldBucketWire) {
			for _, b := range bs {
				if b.MemberID == self.MemberID {
					continue
				}
				bucketDir.ApplyPeer(b.MemberID, vector.HeldBucket{Collection: b.Collection, QVer: b.QVer, Buckets: b.Buckets, GeneratedAtUnixMs: b.GeneratedAtUnixMs})
			}
		},
	)
	// IVF training (design/29 Phase 3.5): the lowest-member-id node periodically gathers a cross-node
	// sample, trains k-means centroids, and writes a versioned artifact; every node reads + installs
	// it so the whole cluster agrees on buckets. Until then collections use the zero-training LSH.
	go runCentroidSync(ctx, reader, quantSet, bucketDir, vstore, metas, vmetrics, logger, 15*time.Second)
	go runIVFTrainer(ctx, self, svc, coord, httpClient, quantSet, vstore, metas, logger, 20*time.Second)
	newGraphVersion := func() *wavespanv1.Version { return rstore.NextVersion().ToProto() }
	// Concentrate each bucket onto its ring (reclaim off-ring origin copies, migrate after a retrain).
	go runRebucketer(ctx, self, svc, coord, quantSet, indexSet, vstore, metas, cfg.Replication.Target()+1, newGraphVersion, logger, 20*time.Second)

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

	// Vector search scatters SearchLocal to alive peers and merges, so a query spans the whole cluster
	// even when vectors are sharded. Shared by the Cypher vector.* procedures and the VectorSearch RPC.
	vectorPeers := func() []cypher.Peer {
		var peers []cypher.Peer
		for _, m := range svc.Members() {
			if m.State == membership.StateAlive {
				peers = append(peers, cypher.Peer{Member: m.Member.MemberID, DataAddr: m.Member.DataAddr})
			}
		}
		return peers
	}
	vectorScatter := cypher.NewVectorScatter(cfg.MemberID, vectorPeers, httpClient)

	// VectorService: SearchLocal fragment + the vector-as-key API (Put/Get/Delete/Search, design/29).
	vectorSvc := vector.NewService(vstore, newGraphVersion).
		WithHooks(indexSet.OnWrite, vectorGlobalTap).
		WithReplication(func(ctx context.Context, ns string, key, value []byte, collection string, vec []float32) error {
			// Affinity placement (design/29 Phase 3): target the bucket's HRW ring so a bucket
			// concentrates on a deterministic node-set (maximally selective routing). Origin+1 is
			// unchanged. Fall back to latency placement when the collection has no quantizer.
			if qz, ok := quantSet.For(collection); ok && len(vec) > 0 {
				ring := vector.Ring(vector.BucketKey(collection, qz.Version(), qz.Bucket(vec)), aliveMemberIDs(svc), cfg.Replication.Target()+1)
				_, err := coord.PutTo(ctx, ns, key, value, ringCandidates(svc, ring, self.MemberID), "")
				return err
			}
			_, err := coord.Put(ctx, ns, key, value, nil, "")
			return err
		}, indexSet.CollectionDims).
		WithSearch(indexSet.Meta, indexSet.Live).
		WithCoordinator(
			indexSet.IndexForCollection,
			func(ctx context.Context, collection, idx string, q []float32, k, ef, nprobe int, rerank bool) ([][]vector.Hit, int) {
				return routedVectorScatter(ctx, self, svc, httpClient, quantSet, bucketDir, vmetrics, cfg.Replication.Target()+1, collection, idx, q, k, ef, nprobe, rerank)
			},
			func(ctx context.Context, ns string, key []byte) ([]byte, bool, error) {
				res, err := reader.Get(ctx, ns, key, true)
				if err != nil {
					return nil, false, err
				}
				return res.GetValue(), res.GetFound(), nil
			},
			func(ctx context.Context, ns string, key []byte) error {
				_, err := coord.Delete(ctx, ns, key, "")
				return err
			},
		)

	// Graph + Cypher (M8/M9/M10): plans and executes against the local graph store, with vector search.
	graphStore := graph.NewStore(store)
	cypherSvc := cypher.NewService(graphStore, cfg.ClusterID, cfg.MemberID, newGraphVersion).
		WithVector(vstore, indexSet.Meta, indexSet.Live).
		WithVectorScatter(vectorScatter).
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
		WithGraph(graphStore).                                                  // enables the visual node explorer (GraphExplore / GraphSubgraph)
		WithSampleDataset(!cfg.Features.DisableSampleDataset, newGraphVersion). // UI "load demo graph" action
		WithClusterScan(replicator).                                            // cluster-wide Data Browser: fan InspectLocal out to all members
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

	// Byte/connection-counting listeners (bandwidth + new connections per listener).
	gossipLn, err := netMetrics.Listen(gossipSrv.Addr, "gossip")
	if err != nil {
		return fmt.Errorf("gossip listen: %w", err)
	}
	dataLn, err := netMetrics.Listen(dataSrv.Addr, "data")
	if err != nil {
		return fmt.Errorf("data listen: %w", err)
	}
	adminLn, err := netMetrics.Listen(adminSrv.Addr, "admin")
	if err != nil {
		return fmt.Errorf("admin listen: %w", err)
	}

	errCh := make(chan error, 3)
	go func() {
		logger.Info("gossip server listening", "addr", gossipSrv.Addr, "tls", gossipSrv.TLSConfig != nil)
		if err := serve(gossipSrv, gossipLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("gossip server: %w", err)
		}
	}()
	go func() {
		logger.Info("data server listening", "addr", dataSrv.Addr, "tls", dataSrv.TLSConfig != nil)
		if err := serve(dataSrv, dataLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("data server: %w", err)
		}
	}()
	go func() {
		logger.Info("admin server listening", "addr", adminSrv.Addr, "tls", adminSrv.TLSConfig != nil)
		if err := serve(adminSrv, adminLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

// serve runs an HTTP server over the given (byte/connection-counting) listener — mTLS when a
// TLSConfig is present (certs are embedded in it, so the empty ServeTLS args are correct), plaintext
// otherwise (insecureDevMode).
func serve(srv *http.Server, ln net.Listener) error {
	if srv.TLSConfig != nil {
		return srv.ServeTLS(ln, "", "")
	}
	return srv.Serve(ln)
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
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			ns = append(ns, n)
		}
	}
	for _, n := range cfg.Namespaces {
		add(n.Name)
	}
	// Reconcile each vector collection's replication namespace too, so a vector write or delete that
	// reached only some holders (origin+1 may pick a different nearby peer per write) converges
	// LWW across all holders — otherwise a deleted vector lingers in a stale holder's HNSW.
	for _, vi := range cfg.VectorIndexes {
		add(vector.MutationNamespace(vi.Collection))
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

// vectorNumBuckets is the coarse bucket-space size per collection for kNN routing (LSH rounds up to
// the next power of two). Small enough that a node's held-bucket set is tiny; large enough that a
// bucket is a thin slice of the space.
const vectorNumBuckets = 256

// recomputeHeldBuckets periodically rebuilds this node's advertised bucket set from the store, so a
// bucket that emptied (deletes/migration) is de-advertised. Runs once at boot, then on interval.
func recomputeHeldBuckets(ctx context.Context, vstore *vector.Store, qs *vector.QuantSet, dir *vector.BucketDir, metas []*vector.IndexMeta, vm *vectorMetrics, interval time.Duration) {
	colls := distinctCollections(metas)
	recompute := func() {
		for _, coll := range colls {
			recomputeCollectionBuckets(vstore, qs, dir, coll, vm)
		}
	}
	recompute()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			recompute()
		}
	}
}

// recomputeCollectionBuckets rebuilds this node's advertised bucket set for one collection from the
// store under the current quantizer (also called right after a new IVF quantizer is installed), and
// refreshes the per-collection load metrics (dataset size, held buckets, bucket-size skew).
func recomputeCollectionBuckets(vstore *vector.Store, qs *vector.QuantSet, dir *vector.BucketDir, collection string, vm *vectorMetrics) {
	qz, ok := qs.For(collection)
	if !ok {
		return
	}
	recs, err := vstore.ScanCollection(collection)
	if err != nil {
		return
	}
	counts := map[uint32]int{}
	maxCount := 0
	for _, r := range recs {
		b := qz.Bucket(r.GetValues())
		counts[b]++
		if counts[b] > maxCount {
			maxCount = counts[b]
		}
	}
	buckets := make([]uint32, 0, len(counts))
	for b := range counts {
		buckets = append(buckets, b)
	}
	dir.SetOwn(collection, qz.Version(), buckets, time.Now().UnixMilli())
	if vm != nil {
		vm.localVectors.WithLabelValues(collection).Set(float64(len(recs)))
		vm.heldBuckets.WithLabelValues(collection).Set(float64(len(counts)))
		vm.qver.WithLabelValues(collection).Set(float64(qz.Version()))
		skew := 1.0
		if mean := float64(len(recs)) / float64(max(1, len(counts))); mean > 0 {
			skew = float64(maxCount) / mean // 1.0 = balanced; higher = a hot bucket
		}
		vm.bucketSkew.WithLabelValues(collection).Set(skew)
	}
}

func distinctCollections(metas []*vector.IndexMeta) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range metas {
		if !seen[m.Collection] {
			seen[m.Collection] = true
			out = append(out, m.Collection)
		}
	}
	return out
}

// runCentroidSync periodically reads each collection's centroid artifact and installs a newer IVF
// quantizer, so every node converges on the same buckets (design/29 Phase 3.5).
func runCentroidSync(ctx context.Context, reader *kv.Reader, qs *vector.QuantSet, dir *vector.BucketDir, vstore *vector.Store, metas []*vector.IndexMeta, vm *vectorMetrics, logger *slog.Logger, interval time.Duration) {
	colls := distinctCollections(metas)
	sync := func() {
		for _, coll := range colls {
			res, err := reader.Get(ctx, vector.CentroidNamespace(coll), vector.CentroidKey(), true)
			if err != nil || !res.GetFound() {
				continue
			}
			var art wavespanv1.IvfCentroids
			if proto.Unmarshal(res.GetValue(), &art) != nil || len(art.GetCentroids()) == 0 {
				continue
			}
			if cur, ok := qs.For(coll); ok && art.GetQver() <= cur.Version() {
				continue // not newer than what we run
			}
			qs.Set(coll, vector.IVFFromProto(&art))
			recomputeCollectionBuckets(vstore, qs, dir, coll, vm) // re-advertise our vectors under the new qver
			logger.Info("installed IVF centroids", "collection", coll, "qver", art.GetQver(), "centroids", len(art.GetCentroids()))
		}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sync()
		}
	}
}

// runRebucketer continuously migrates this node's off-ring vectors onto their affinity ring, then
// drops the local search copy — so a bucket's vectors concentrate on its ring members (the full
// replicas), routing converges to a single hop, and the off-ring origin copies left by affinity
// placement (and any drift after an IVF retrain changes bucket assignments) are reclaimed. Only the
// search index + bucket advertisement are dropped; the durable record is kept so anti-entropy does
// not fight the migration. Throttled to batchSize vectors per pass; a node never drops a copy until
// the ring has durably acknowledged it (no data loss).
func runRebucketer(ctx context.Context, self membership.Member, svc *membership.Service, coord *kv.Coordinator, qs *vector.QuantSet, indexSet *vector.IndexSet, vstore *vector.Store, metas []*vector.IndexMeta, ringSize int, newVersion func() *wavespanv1.Version, logger *slog.Logger, interval time.Duration) {
	const batchSize = 64
	colls := distinctCollections(metas)
	pass := func() {
		ids := aliveMemberIDs(svc)
		if len(ids) <= ringSize {
			return // every node is a ring member; nothing to concentrate
		}
		for _, coll := range colls {
			qz, ok := qs.For(coll)
			if !ok {
				continue
			}
			recs, err := vstore.ScanCollection(coll)
			if err != nil {
				continue
			}
			migrated := 0
			for _, r := range recs {
				if migrated >= batchSize {
					break
				}
				ring := vector.Ring(vector.BucketKey(coll, qz.Version(), qz.Bucket(r.GetValues())), ids, ringSize)
				onRing := false
				for _, m := range ring {
					if m == self.MemberID {
						onRing = true
						break
					}
				}
				if onRing {
					continue // this vector belongs on this node
				}
				// Off-ring: re-place onto the ring, then drop our local search copy.
				rec := &wavespanv1.VectorRecord{Collection: coll, VectorId: r.GetVectorId(), Values: r.GetValues(), Payload: r.GetPayload(), Dimensions: r.GetDimensions(), Version: newVersion()}
				sr, werr := vector.Wrap(rec)
				if werr != nil {
					continue
				}
				out, perr := coord.PutTo(ctx, sr.GetNamespace(), sr.GetLogicalKey(), sr.GetValue().GetInline(), ringCandidates(svc, ring, self.MemberID), "")
				if perr != nil || out.AckedNearbyReplicas < 1 {
					continue // not durably on the ring yet — keep our copy
				}
				_ = vstore.Delete(coll, r.GetVectorId(), newVersion()) // local search-copy drop (kept in rstore)
				indexSet.OnWrite(&wavespanv1.VectorRecord{Collection: coll, VectorId: r.GetVectorId(), Tombstone: true})
				migrated++
			}
			if migrated > 0 {
				logger.Info("re-bucketed vectors onto their ring", "collection", coll, "moved", migrated)
			}
		}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pass()
		}
	}
}

// runIVFTrainer, on the lowest-member-id alive node, periodically gathers a cross-node vector sample,
// trains k-means centroids, and writes a versioned artifact for runCentroidSync to distribute.
func runIVFTrainer(ctx context.Context, self membership.Member, svc *membership.Service, coord *kv.Coordinator, hc *http.Client, qs *vector.QuantSet, vstore *vector.Store, metas []*vector.IndexMeta, logger *slog.Logger, interval time.Duration) {
	const (
		samplePerNode     = 4000
		minSampleForTrain = 64
		retrainInterval   = 30 * time.Minute
	)
	colls := distinctCollections(metas)
	dim := map[string]int{}
	l2 := map[string]bool{}
	for _, m := range metas {
		dim[m.Collection] = m.Dimensions
		l2[m.Collection] = vector.MetricIsL2(m.Metric)
	}
	lastTrained := map[string]time.Time{}
	train := func() {
		if !isLowestAlive(svc, self.MemberID) {
			return // a single deterministic trainer; LWW on the artifact resolves brief overlaps
		}
		for _, coll := range colls {
			if t, ok := lastTrained[coll]; ok && time.Since(t) < retrainInterval {
				continue
			}
			samples := gatherSamples(ctx, self, svc, hc, vstore, coll, samplePerNode)
			if len(samples) < minSampleForTrain {
				continue
			}
			curVer := uint32(1)
			if q, ok := qs.For(coll); ok {
				curVer = q.Version()
			}
			k := vectorNumBuckets
			if max := len(samples) / 16; k > max {
				k = max // adapt cluster count to the data we actually have
			}
			if k < 1 {
				k = 1
			}
			ivf := quantizer.TrainIVF(samples, k, 10, l2[coll], int64(curVer), curVer+1)
			art := vector.IVFToProto(coll, ivf, dim[coll], l2[coll], time.Now().UnixMilli())
			b, merr := proto.Marshal(art)
			if merr != nil {
				continue
			}
			if _, err := coord.Put(ctx, vector.CentroidNamespace(coll), vector.CentroidKey(), b, nil, ""); err != nil {
				logger.Warn("ivf artifact write", "collection", coll, "err", err)
				continue
			}
			lastTrained[coll] = time.Now()
			logger.Info("trained IVF centroids", "collection", coll, "qver", curVer+1, "k", k, "samples", len(samples))
		}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			train()
		}
	}
}

// isLowestAlive reports whether self has the smallest member id among alive members (deterministic
// single-trainer election without coordination).
func isLowestAlive(svc *membership.Service, self string) bool {
	for _, mv := range svc.Members() {
		if mv.State == membership.StateAlive && mv.Member.MemberID < self {
			return false
		}
	}
	return true
}

// gatherSamples collects a reservoir sample from this node plus every alive peer (SampleVectors RPC).
func gatherSamples(ctx context.Context, self membership.Member, svc *membership.Service, hc *http.Client, vstore *vector.Store, collection string, limit int) [][]float32 {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	all := vector.ReservoirSample(vstore, collection, limit, rng)
	for _, mv := range svc.Members() {
		m := mv.Member
		if mv.State != membership.StateAlive || m.MemberID == self.MemberID || m.DataAddr == "" {
			continue
		}
		c := wavespanv1connect.NewVectorServiceClient(hc, "http://"+m.DataAddr)
		resp, err := c.SampleVectors(ctx, connect.NewRequest(&wavespanv1.SampleVectorsReq{Collection: collection, Limit: uint32(limit)}))
		if err != nil {
			continue
		}
		for _, fv := range resp.Msg.GetVectors() {
			all = append(all, fv.GetValues())
		}
	}
	return all
}

// routedVectorScatter queries peer holders for a kNN fragment. With nprobe>0 and routing info
// available it scatters only to the advertised holders of the query's probed buckets; otherwise it
// falls back to every alive peer. Self is excluded — the coordinator adds its own local fragment.
func routedVectorScatter(ctx context.Context, self membership.Member, svc *membership.Service, hc *http.Client, qs *vector.QuantSet, dir *vector.BucketDir, vm *vectorMetrics, ringSize int, collection, idx string, query []float32, k, ef, nprobe int, rerank bool) ([][]vector.Hit, int) {
	var allow map[string]bool
	if nprobe > 0 {
		if qz, ok := qs.For(collection); ok {
			allow = map[string]bool{}
			ids := aliveMemberIDs(svc)
			qver := qz.Version()
			graph := svc.Graph()
			for _, b := range qz.Probe(query, nprobe) {
				// The bucket's affinity ring is its full-replica set: every ring member holds ALL of the
				// bucket's vectors, so among them closest-replica (query just the nearest) is safe. Any
				// holder NOT on the ring is only a PARTIAL copy (an off-ring origin, or a vector not yet
				// migrated after a qver change) and must be queried in full or results are missed — the
				// re-bucketing worker dissolves those over time, converging routing to the single ring hop.
				ring := vector.Ring(vector.BucketKey(collection, qver, b), ids, ringSize)
				ringSet := map[string]bool{}
				for _, m := range ring {
					ringSet[m] = true
				}
				if members, ok := dir.Holders(collection, qver, []uint32{b}); ok {
					for _, m := range members {
						if m != self.MemberID && !ringSet[m] {
							allow[m] = true // partial / in-migration holder: must query
						}
					}
				}
				if ringSet[self.MemberID] {
					continue // self is a full replica; the local fragment covers this bucket
				}
				// Query the lowest-latency ring member (full replica).
				best, bestRtt := "", math.Inf(1)
				for _, m := range ring {
					if m == self.MemberID {
						continue
					}
					rtt := math.Inf(1)
					if e, ok := graph.Edge(m); ok && e.EWMARttMs > 0 {
						rtt = e.EWMARttMs
					}
					if best == "" || rtt < bestRtt {
						best, bestRtt = m, rtt
					}
				}
				if best != "" {
					allow[best] = true
				}
			}
		}
	}
	var fragments [][]vector.Hit
	unreachable, scattered := 0, 0
	for _, mv := range svc.Members() {
		m := mv.Member
		if mv.State != membership.StateAlive || m.MemberID == self.MemberID || m.DataAddr == "" {
			continue
		}
		if allow != nil && !allow[m.MemberID] {
			continue // routed: skip nodes that don't hold a probed bucket
		}
		scattered++
		c := wavespanv1connect.NewVectorServiceClient(hc, "http://"+m.DataAddr)
		resp, err := c.SearchLocal(ctx, connect.NewRequest(&wavespanv1.SearchLocalRequest{
			IndexName: idx, Query: query, K: int32(k), EfSearch: int32(ef), Rerank: rerank,
		}))
		if err != nil {
			unreachable++
			continue
		}
		fragments = append(fragments, vectorHitsFromProto(resp.Msg.GetHits()))
	}
	if vm != nil {
		vm.scatterNodes.Observe(float64(scattered))
	}
	return fragments, unreachable
}

func vectorHitsFromProto(in []*wavespanv1.VectorHit) []vector.Hit {
	out := make([]vector.Hit, 0, len(in))
	for _, h := range in {
		out = append(out, vector.Hit{
			Collection: h.GetCollection(), VectorID: h.GetVectorId(), GraphNodeID: h.GetGraphNodeId(),
			Distance: h.GetDistance(), Score: h.GetScore(),
		})
	}
	return out
}

// aliveMemberIDs returns the member ids of all alive members (the HRW ring input).
func aliveMemberIDs(svc *membership.Service) []string {
	var ids []string
	for _, mv := range svc.Members() {
		if mv.State == membership.StateAlive {
			ids = append(ids, mv.Member.MemberID)
		}
	}
	return ids
}

// ringCandidates maps ring member ids to replication candidates, excluding self (the origin is
// already locally durable). This is the affinity placement set passed to Coordinator.PutTo.
func ringCandidates(svc *membership.Service, ring []string, self string) []placement.Candidate {
	byID := map[string]membership.Member{}
	for _, mv := range svc.Members() {
		if mv.State == membership.StateAlive {
			byID[mv.Member.MemberID] = mv.Member
		}
	}
	var cands []placement.Candidate
	for _, id := range ring {
		if id == self {
			continue
		}
		if m, ok := byID[id]; ok {
			cands = append(cands, placement.Candidate{Member: m})
		}
	}
	return cands
}

// vectorMetrics exposes per-node, per-collection vector + routing observability (design/29 Phase 4):
// dataset size, how many buckets the node holds, bucket-size skew (max/mean — 1.0 is perfectly
// balanced), the live quantizer version, and the kNN query fan-out.
type vectorMetrics struct {
	localVectors *prometheus.GaugeVec
	heldBuckets  *prometheus.GaugeVec
	bucketSkew   *prometheus.GaugeVec
	qver         *prometheus.GaugeVec
	scatterNodes prometheus.Histogram
}

func newVectorMetrics(reg *prometheus.Registry) *vectorMetrics {
	m := &vectorMetrics{
		localVectors: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wavespan_vector_local_vectors", Help: "live vectors held locally, by collection"}, []string{"collection"}),
		heldBuckets:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wavespan_vector_held_buckets", Help: "distinct buckets held locally, by collection"}, []string{"collection"}),
		bucketSkew:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wavespan_vector_bucket_skew", Help: "local bucket-size skew (max/mean; 1.0 = balanced), by collection"}, []string{"collection"}),
		qver:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wavespan_vector_quantizer_version", Help: "live quantizer version, by collection"}, []string{"collection"}),
		scatterNodes: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "wavespan_vector_search_scattered_nodes", Help: "peer nodes a kNN query scattered to", Buckets: []float64{0, 1, 2, 3, 5, 8, 13, 21}}),
	}
	reg.MustRegister(m.localVectors, m.heldBuckets, m.bucketSkew, m.qver, m.scatterNodes)
	return m
}
