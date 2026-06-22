package tunables

// Default builds the registry with every WaveSpan + wavesdb tunable declared, documented, and seeded
// with its built-in default. This list is the single source of truth: the reference YAML, the
// GetNodeConfig API, koanf env/file binding, and the gossip-delta path all derive from it.
//
// Categories: Static = applied at startup / engine-open (a runtime override needs a restart);
// Hot = re-read live by its owning worker.
func Default() *Registry {
	r := NewRegistry()
	registerStorageEngine(r)
	registerMembership(r)
	registerLatency(r)
	registerReplication(r)
	registerGlobal(r)
	registerTTL(r)
	registerCache(r)
	registerKV(r)
	registerServer(r)
	registerTransport(r)
	registerObservability(r)
	registerVector(r)
	registerRuntime(r)
	return r
}

// reg is the compact registration helper used by the group functions below.
func reg(r *Registry, key, group string, kind Kind, cat Category, def, doc, why string) *Param {
	return r.register(&Param{Key: key, Group: group, Kind: kind, Category: cat, def: def, Doc: doc, Why: why})
}

// --- wavesdb storage engine (LSM) -------------------------------------------------------------
// These map to wavesdb Options / ColumnFamilyOptions and are applied when the DB / column families
// are opened, so all are Static.
func registerStorageEngine(r *Registry) {
	const g = "storage.engine"
	reg(r, g+".writeBufferSize", g, KindBytes, Static, "64MiB",
		"Memtable size that triggers a flush to an L0 SSTable; also the compaction output file-size target.",
		"64MiB balances write amplification against memory per column family (TidesDB heritage); larger = fewer, bigger flushes but more RAM and longer recovery.")
	reg(r, g+".blockCacheSize", g, KindBytes, Static, "64MiB",
		"Size of the shared block cache for SSTable reads (negative disables).",
		"64MiB is a modest default that helps hot reads without dominating pod memory; raise on read-heavy nodes.")
	reg(r, g+".maxOpenSSTables", g, KindInt, Static, "256",
		"Cap on cached open SSTable file handles.",
		"256 keeps file-descriptor use bounded while covering typical level fan-out; raise for very large datasets.")
	reg(r, g+".maxMemoryUsage", g, KindBytes, Static, "0",
		"Soft global memory limit for the engine in bytes (0 = unlimited).",
		"0 by default so the engine is bounded by GOMEMLIMIT/pod limits rather than an internal cap.")
	reg(r, g+".numFlushThreads", g, KindInt, Static, "0",
		"Background memtable-flush workers (0 = auto: min(NumCPU,4)).",
		"Auto matches flush parallelism to cores without oversubscribing a small pod.")
	reg(r, g+".numCompactionThreads", g, KindInt, Static, "0",
		"Background compaction workers (0 = auto: 2).",
		"Two compactors keep up with steady writes without starving foreground work on small nodes.")
	reg(r, g+".levelSizeRatio", g, KindInt, Static, "10",
		"Capacity multiplier between adjacent LSM levels (Li+1 holds ~ratio× Li).",
		"10 is the classic leveled-LSM ratio: good read/space amplification trade-off.")
	reg(r, g+".minLevels", g, KindInt, Static, "1",
		"Minimum number of on-disk levels.",
		"1 lets small datasets stay shallow; the tree grows levels on demand.")
	reg(r, g+".klogValueThreshold", g, KindBytes, Static, "512",
		"Value size at/above which values are stored in the value log (WiscKey key/value separation).",
		"512B keeps small values inline (fast point reads) while large values avoid bloating the key log.")
	reg(r, g+".compression", g, KindString, Static, "none",
		"SSTable block codec: none | snappy | lz4 | zstd | flate.",
		"none by default for lowest CPU and predictable latency on fast local disks; enable zstd/snappy to trade CPU for space.")
	reg(r, g+".enableBloomFilter", g, KindBool, Static, "true",
		"Build a per-SSTable bloom filter to skip non-matching files on point reads.",
		"true: a small filter dramatically cuts disk reads for absent keys, which dominate cache-miss reads.")
	reg(r, g+".bloomFpr", g, KindFloat, Static, "0.01",
		"Target bloom-filter false-positive rate.",
		"1% balances filter memory against wasted SSTable probes; lower = larger filters.")
	reg(r, g+".enableBlockIndex", g, KindBool, Static, "true",
		"Build a sparse in-SSTable block index for fast seeks.",
		"true: seeks land near the key without scanning the whole block; cost is a small index per file.")
	reg(r, g+".indexSampleRatio", g, KindInt, Static, "1",
		"Sample every Nth entry into the block index (1 = index every entry).",
		"1 gives the tightest seeks; raise to shrink the index at the cost of slightly longer in-block scans.")
	reg(r, g+".blockIndexPrefixLen", g, KindInt, Static, "16",
		"Key-prefix length (bytes) stored per block-index entry.",
		"16 bytes disambiguates typical keys while keeping the index compact.")
	reg(r, g+".syncMode", g, KindString, Static, "none",
		"WAL durability: none (OS flush) | full (fsync per commit) | interval (periodic fsync).",
		"none favours throughput on the eventually-consistent, origin+1-replicated write path; a second durable replica is the real safety net, not local fsync.")
	reg(r, g+".syncInterval", g, KindDuration, Static, "128ms",
		"Flush period when syncMode = interval.",
		"128ms bounds data-loss-on-crash to a fraction of a second while amortizing fsync cost.")
	reg(r, g+".skipListMaxLevel", g, KindInt, Static, "12",
		"Maximum level of the memtable skip list.",
		"12 supports millions of memtable entries with O(log n) ops before a flush.")
	reg(r, g+".skipListProbability", g, KindFloat, Static, "0.25",
		"Promotion probability for the memtable skip list.",
		"0.25 is the standard skip-list p: a good height/space trade-off.")
	reg(r, g+".defaultIsolation", g, KindString, Static, "snapshot",
		"Default transaction isolation: read-uncommitted | read-committed | repeatable-read | snapshot | serializable.",
		"snapshot gives consistent reads with write-write conflict detection without full SSI overhead.")
	reg(r, g+".l1FileCountTrigger", g, KindInt, Static, "4",
		"Number of L0 files that triggers an L0→L1 compaction.",
		"4 keeps L0 shallow (L0 files overlap, so reads scan them all) without compacting on every flush.")
	reg(r, g+".l0StallThreshold", g, KindInt, Static, "10",
		"Immutable-memtable queue depth at which writes are back-pressured.",
		"10 lets flushes burst-buffer under load before slowing writers to protect memory.")
	reg(r, g+".useBTree", g, KindBool, Static, "false",
		"Store the key log as a B-tree instead of sorted blocks (faster reads, slower writes).",
		"false: the sorted-block layout suits the write-heavy KV path; enable for read-dominated CFs.")
}

// --- membership / gossip ----------------------------------------------------------------------
func registerMembership(r *Registry) {
	const g = "membership"
	reg(r, g+".gossipInterval", g, KindDuration, Hot, "1s",
		"Interval between SWIM gossip rounds (each round probes one peer and exchanges deltas).",
		"1s converges membership in a few seconds for typical clusters without flooding the network.")
	reg(r, g+".suspicionTimeout", g, KindDuration, Hot, "3s",
		"Time a SUSPECT member waits before advancing to UNREACHABLE.",
		"3s (≈3 gossip rounds) tolerates a transient blip before escalating, balancing false positives vs. detection speed for spot churn.")
	reg(r, g+".unreachableTimeout", g, KindDuration, Hot, "10s",
		"Additional time an UNREACHABLE member persists before being marked DEAD.",
		"10s gives repair time to start before the member is treated as gone.")
	reg(r, g+".deadRetention", g, KindDuration, Hot, "5m",
		"Time a DEAD member is retained before being FORGOTTEN.",
		"5m lets repair fully rebuild the dead node's replicas before its identity is dropped.")
	reg(r, g+".indirectFanout", g, KindInt, Hot, "3",
		"Number of relay peers used for a SWIM indirect probe when a direct ping fails.",
		"k=3 is the canonical SWIM value: high probability of reaching a live-but-slow peer without a broadcast.")
}

// --- latency graph ----------------------------------------------------------------------------
func registerLatency(r *Registry) {
	const g = "latency"
	reg(r, g+".ewmaAlpha", g, KindFloat, Hot, "0.2",
		"EWMA smoothing factor for per-edge RTT (0,1).",
		"0.2 weights recent samples enough to track real latency shifts while damping single-sample noise.")
	reg(r, g+".refMaxRttMs", g, KindFloat, Hot, "50",
		"RTT reference baseline (ms) used to normalize the latency score to [0,1].",
		"50ms is a reasonable intra-region ceiling; RTTs above it saturate the score.")
	reg(r, g+".edgeExpiry", g, KindDuration, Hot, "30s",
		"Edges with no fresh sample within this window are dropped.",
		"30s prevents stale latency from steering placement after a path changes.")
	reg(r, g+".recentWindow", g, KindInt, Hot, "64",
		"Recent RTT samples kept per edge for p95 estimation.",
		"64 samples gives a stable p95 without unbounded per-edge memory.")
	reg(r, g+".weight.ewma", g, KindFloat, Hot, "0.55",
		"Placement-score weight: EWMA RTT (the dominant signal).",
		"0.55 makes measured average latency the primary driver of replica placement.")
	reg(r, g+".weight.p95", g, KindFloat, Hot, "0.15",
		"Placement-score weight: p95 RTT (tail latency).",
		"0.15 penalizes jittery links beyond their average.")
	reg(r, g+".weight.packetLoss", g, KindFloat, Hot, "0.10",
		"Placement-score weight: packet loss.",
		"0.10 demotes lossy peers that would need retransmits.")
	reg(r, g+".weight.loadPressure", g, KindFloat, Hot, "0.10",
		"Placement-score weight: peer load pressure.",
		"0.10 steers replicas away from busy nodes.")
	reg(r, g+".weight.diskPressure", g, KindFloat, Hot, "0.05",
		"Placement-score weight: peer disk pressure.",
		"0.05 avoids placing durable replicas on near-full nodes.")
	reg(r, g+".weight.topology", g, KindFloat, Hot, "0.05",
		"Placement-score weight: zone/region topology hint.",
		"0.05 is a tiebreaker only — measured RTT is authoritative, not labels.")
}

// --- replication / repair ---------------------------------------------------------------------
func registerReplication(r *Registry) {
	const g = "replication"
	reg(r, g+".targetNearbyReplicas", g, KindInt, Hot, "3",
		"Background target replica count per key (origin + N nearby durable replicas).",
		"3 gives durability headroom beyond the origin+1 ACK so a node loss rarely under-replicates.")
	reg(r, g+".minAckNearbyReplicas", g, KindInt, Hot, "1",
		"Nearby durable replicas required before a write ACKs (origin+1).",
		"1 = origin+1: the value survives any single pod loss before the client sees success, without quorum latency.")
	reg(r, g+".writeTimeout", g, KindDuration, Hot, "2s",
		"Timeout for replicating a write to a nearby durable replica.",
		"2s bounds origin+1 latency under a slow peer before falling back to repair.")
	reg(r, g+".repairInterval", g, KindDuration, Hot, "200ms",
		"Repair worker tick interval (rate-limit; processes a bounded batch per tick).",
		"200ms steadily heals under-replication without saturating the network during churn.")
	reg(r, g+".repairWriteTimeout", g, KindDuration, Hot, "2s",
		"Timeout for pushing a repair replica to a candidate holder.",
		"2s matches the foreground write timeout so a stuck peer is skipped quickly.")
	reg(r, g+".fanoutWriteTimeout", g, KindDuration, Hot, "2s",
		"Timeout for an async target-N fanout write after the ACK.",
		"2s keeps fanout from piling up on a slow peer.")
	reg(r, g+".fanoutQueueCapacity", g, KindInt, Static, "4096",
		"Buffered capacity of the post-ACK fanout job queue (overflow spills to repair).",
		"4096 absorbs write bursts; overflow is safe because repair re-derives the work.")
	reg(r, g+".antiEntropyInterval", g, KindDuration, Hot, "2s",
		"Intra-cluster anti-entropy reconciliation interval (LWW convergence).",
		"2s reconciles divergence promptly while keeping background scan cost low.")
	reg(r, g+".antiEntropyBatch", g, KindInt, Hot, "256",
		"Keys scanned per intra-cluster anti-entropy tick, per namespace.",
		"256 bounds per-tick work so reconciliation is incremental, not a stop-the-world scan.")
	reg(r, g+".bootstrapPage", g, KindInt, Hot, "512",
		"Page size when backfilling 'all'/'global' namespaces onto a joining node.",
		"512 records per page streams efficiently without large RPC payloads.")
	reg(r, g+".bootstrapRetryInterval", g, KindDuration, Hot, "2s",
		"Retry interval for join-time bootstrap until a source namespace is reachable.",
		"2s retries promptly during a rolling start without busy-looping.")
	reg(r, g+".backfillMaxPage", g, KindInt, Hot, "1024",
		"Maximum page size accepted for a Backfill RPC response.",
		"1024 caps a single response so a backfill can't be forced into huge allocations.")
}

// --- global (cross-cluster) replication -------------------------------------------------------
func registerGlobal(r *Registry) {
	const g = "global"
	reg(r, g+".senderInterval", g, KindDuration, Hot, "1s",
		"Interval at which the global sender drains the out-log to peer clusters.",
		"1s keeps cross-cluster lag low while batching writes for efficiency.")
	reg(r, g+".senderBatch", g, KindInt, Hot, "256",
		"Out-log entries drained per peer per partition per tick.",
		"256 amortizes RPC overhead without holding the out-log lock too long.")
	reg(r, g+".reconcileInterval", g, KindDuration, Hot, "30s",
		"Cross-cluster anti-entropy reconciliation interval.",
		"30s catches entries missed by streaming without heavy continuous cross-region traffic.")
	reg(r, g+".numPartitions", g, KindInt, Static, "16",
		"Partitions per peer's out-log (so one stalled key-range doesn't block others).",
		"16 gives good parallelism across peers without excessive per-peer state.")
	reg(r, g+".outLogDiskBudgetBytes", g, KindBytes, Static, "0",
		"Disk budget for a peer out-log before back-pressuring globally-durable writes (0 = unbounded).",
		"0 by default; set a budget when globalDurabilityRequired namespaces must not outrun a slow peer.")
}

// --- TTL --------------------------------------------------------------------------------------
func registerTTL(r *Registry) {
	const g = "ttl"
	reg(r, g+".sweepInterval", g, KindDuration, Hot, "30s",
		"Lazy TTL sweeper interval; tombstones expired keys so deletion propagates to replicas.",
		"30s reclaims/propagates expiry promptly without a tight scan loop; logical expiry is already enforced on read, so this is best-effort cleanup.")
	reg(r, g+".batch", g, KindInt, Hot, "256",
		"Expired entries processed per sweep tick.",
		"256 keeps each tick short so the sweeper never blocks foreground work.")
}

// --- dynamic cache replicas -------------------------------------------------------------------
func registerCache(r *Registry) {
	const g = "cache"
	reg(r, g+".idleTTL", g, KindDuration, Hot, "10m",
		"Idle time after which a read-created dynamic cache replica is evicted.",
		"10m keeps recently-read keys local while reclaiming cold cache entries; cache replicas are derived and safe to drop.")
	reg(r, g+".evictionInterval", g, KindDuration, Hot, "1m",
		"Cache eviction sweep interval.",
		"1m is frequent enough to bound cache memory without per-second scanning.")
}

// --- KV / recordstore -------------------------------------------------------------------------
func registerKV(r *Registry) {
	const g = "kv"
	reg(r, g+".writeTimeout", g, KindDuration, Hot, "2s",
		"Timeout for the origin+1 coordinator write.",
		"2s bounds client-visible write latency before returning an error to retry.")
	reg(r, g+".numStripes", g, KindInt, Static, "512",
		"Per-key lock stripes serializing the latest-pointer read-modify-write.",
		"512 stripes make same-key contention rare while keeping lock memory tiny; independent keys commit in parallel.")
	reg(r, g+".maxVerCachePer", g, KindInt, Static, "8192",
		"Per-stripe cap on the in-memory latest-version cache (cleared on overflow).",
		"8192 lets monotonic writes skip the storage read on the hot path while bounding memory per stripe.")
}

// --- HTTP / h2 servers ------------------------------------------------------------------------
func registerServer(r *Registry) {
	const g = "server"
	reg(r, g+".readHeaderTimeout", g, KindDuration, Static, "5s",
		"Read-header timeout for the data, gossip, and admin HTTP servers.",
		"5s defends against slowloris-style stalls without cutting off legitimately slow links.")
	reg(r, g+".shutdownTimeout", g, KindDuration, Static, "10s",
		"Graceful-shutdown deadline for in-flight requests on SIGTERM.",
		"10s lets active RPCs drain during a rolling restart before forced close.")
	reg(r, g+".h2MaxConcurrentStreams", g, KindInt, Static, "1024",
		"Max concurrent HTTP/2 streams per connection (h2c data path).",
		"1024 supports high request concurrency over the shared pooled connection.")
	reg(r, g+".h2IdleTimeout", g, KindDuration, Static, "2m",
		"HTTP/2 server idle timeout before closing a connection.",
		"2m keeps warm pooled connections alive across bursts.")
	reg(r, g+".h2ReadIdleTimeout", g, KindDuration, Static, "30s",
		"Send an HTTP/2 PING after this much read-idle to detect dead peers.",
		"30s detects a silently-dropped connection well before TCP would.")
	reg(r, g+".h2PingTimeout", g, KindDuration, Static, "10s",
		"Close the connection if a server-side HTTP/2 PING is unanswered this long.",
		"10s reclaims a half-open connection quickly.")
	reg(r, g+".h2WriteByteTimeout", g, KindDuration, Static, "30s",
		"Timeout for writing a byte to an HTTP/2 peer before giving up.",
		"30s bounds a stuck write without tripping on brief back-pressure.")
	reg(r, g+".h2ClientTimeout", g, KindDuration, Static, "30s",
		"Overall request timeout for the h2c client path.",
		"30s caps a hung request; streaming RPCs use their own context deadlines.")
}

// --- inter-node transport pool ----------------------------------------------------------------
func registerTransport(r *Registry) {
	const g = "transport"
	reg(r, g+".maxIdleConns", g, KindInt, Static, "512",
		"Total idle connections kept warm across all peers.",
		"512 keeps connections to many peers hot so mTLS handshakes stay rare (design/27).")
	reg(r, g+".maxIdleConnsPerHost", g, KindInt, Static, "64",
		"Idle connections kept warm per peer.",
		"64 sustains high concurrency to a single busy peer without re-handshaking.")
	reg(r, g+".idleConnTimeout", g, KindDuration, Static, "10m",
		"How long an idle pooled connection survives before close.",
		"10m keeps connections warm across bursty traffic, amortizing handshakes.")
	reg(r, g+".tcpKeepAlive", g, KindDuration, Static, "30s",
		"OS-level TCP keepalive probe interval.",
		"30s keeps NAT/conntrack entries warm so pooled connections aren't silently dropped.")
	reg(r, g+".dialTimeout", g, KindDuration, Static, "5s",
		"Bound on establishing a new connection (including TLS handshake).",
		"5s fails fast to an unreachable peer so callers fall back to another holder.")
	reg(r, g+".h2ReadIdleTimeout", g, KindDuration, Static, "30s",
		"Client-side HTTP/2 PING-on-idle interval (0 disables).",
		"30s detects dead pooled connections proactively.")
	reg(r, g+".h2PingTimeout", g, KindDuration, Static, "15s",
		"Drop a pooled connection if a client HTTP/2 PING goes unanswered this long.",
		"15s reclaims a dead connection before it stalls a request.")
}

// --- observability buffers / caps -------------------------------------------------------------
func registerObservability(r *Registry) {
	const g = "observability"
	reg(r, g+".gossipRingCapacity", g, KindInt, Static, "4096",
		"Bounded ring-buffer of recent gossip events for the UI tap (overflow drops with a gap marker).",
		"4096 gives the Gossip Inspector useful backfill without unbounded memory.")
	reg(r, g+".subscriberBuffer", g, KindInt, Static, "512",
		"Per-subscriber buffered channel depth for the gossip stream.",
		"512 absorbs a slow UI client before dropping events with a gap marker.")
	reg(r, g+".graphExploreCap", g, KindInt, Hot, "500",
		"Maximum nodes returned by a GraphExplore (Node Explorer) request.",
		"500 keeps the force-directed view responsive and the response bounded.")
	reg(r, g+".inspectRowCap", g, KindInt, Hot, "1000",
		"Maximum rows returned by an InspectLocal (Data Browser) scan.",
		"1000 bounds a debug scan so it can't stream an entire namespace into the UI.")
}

// --- vector engine ----------------------------------------------------------------------------
func registerVector(r *Registry) {
	const g = "vector"
	reg(r, g+".mergeInterval", g, KindDuration, Hot, "5s",
		"Interval at which the vector index merger folds the delta index into main segments.",
		"5s keeps ANN index freshness lag low while batching segment rebuilds.")
}

// --- Go runtime -------------------------------------------------------------------------------
func registerRuntime(r *Registry) {
	const g = "runtime"
	reg(r, g+".gogc", g, KindInt, Hot, "100",
		"Go garbage-collector target percentage (GOGC).",
		"100 is the Go default; lower trades CPU for less heap, higher trades heap for less GC CPU.")
	reg(r, g+".memLimit", g, KindBytes, Hot, "0",
		"Soft memory limit for the Go runtime (GOMEMLIMIT; 0 = unset).",
		"0 by default so the pod's cgroup limit governs; set to ~pod limit to make the GC back off before OOM.")
}
