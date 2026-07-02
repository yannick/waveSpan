# waveSpan: A Critical Review — Database Quality & Performance

Date: 2026-07-02.
Scope: waveSpan repo @ `6917b85` + the sibling `wavesdb` engine (`../wavesdb`, wired via go.mod replace). Findings marked **[verified]** were checked directly in source; others come from targeted code exploration and the repo's own design docs/perf reports. Context: ~33k LOC non-test in `internal/`, ~7.8k LOC in wavesdb, both with substantial test suites.

---

## Verdict

waveSpan is an unusually *honest* database design — the docs explicitly enumerate what it does not guarantee (`design/20_risks_and_non_goals.md` even has a "things not to fake" list), and the CP collections tier has been genuinely load-tested and crash-hardened (a Jepsen-style split bug and two crash-loop classes found and fixed, `design/33`). The engine has good bones where it matters most: real WAL group commit and a sharded lock-free-read memtable.

But as a *database*, three claims don't survive contact with the code:

1. **Acknowledged writes are not durable by default.** `SyncMode: "none"` ships as the engine default — the WAL is written but never fsynced except on a 128ms timer. Combined with origin+1 ack, a correlated 2-node failure (or a single-node deployment) loses acknowledged writes. This isn't in the risk docs; the docs defer the entire fsync story to an "IO rewrite" that hasn't happened.
2. **The transport regressed during the gRPC migration.** design/27's mTLS + tuning work applies to the now-deleted Connect client; the live gRPC path is plaintext with stock settings, and doc 27 is marked complete.
3. **The published performance numbers are contingent on config not in the code.** The 8–10k writes/s in design/33 require env-var Raft-clock overrides and GOGC settings; out-of-the-box defaults ship the slow WAN clock (`RTTMillisecond: 50`).

Additionally, every profile in the repo (`perf-report-*`) predates the gRPC migration — there is no performance evidence for the shipping transport.

---

## Part 1: waveSpan as a database — critical findings

### 1.1 Durability: the ack contract is weaker than documented **[verified]**

- `internal/storage/wavesdb_store.go` `defaultEngineOptions()` sets `SyncMode: "none"` (and `Compression: "none"`). Under `SyncNone`, wavesdb's WAL group-commit leader does `f.Write(buf)` and never fsyncs (`wal.go`); a timer syncs every `SyncInterval` (128ms default).
- ADR-0002's contract is "origin pod local **durable** write AND one nearby **durable** replica." With SyncNone, "durable" means "in two OS page caches." Kernel-panic/power-loss on origin+first-replica within the sync window = silent loss of acked writes. The failure model (`design/13`) admits loss when *both holders die before repair*, but not that a single fsync window is in play on every write.
- The well-engineered group commit (single fsync per commit group, fsync outside the WAL mutex — `wavesdb/internal/wal/wal.go:198-227`) only pays off under `SyncFull`, which nothing uses.
- Related incident class: Pebble (dragonboat's LogDB) **panics on ENOSPC** below waveSpan, and crash-loops permanently since replay re-hits ENOSPC (`design/36`). Mitigation is admission-level shedding; a follower can still be killed by the leader's replication stream (§5). Two embedded engines (wavesdb + Pebble) each with their own WAL/fsync story is transitional debt (`adr/0008` Phase 2 unbuilt).

### 1.2 Consistency: honest contract, thin v1 machinery

- The AP tier promises eventual + HLC LWW and explicitly disclaims linearizability, complete scans, exact TTL (`design/00`, `adr/0001`). Good honesty. But v1 ships **only two conflict resolvers** (`hlc-last-write-wins`, `keep-siblings`); every CRDT resolver is interface-only and selecting one is a validation error (`design/06`). "Active-active" in practice means "silent LWW data loss or manual sibling resolution."
- CAS is best-effort, not linearizable, with a documented race window (`design/03`) — reasonable, but it means the AP tier has no safe read-modify-write primitive; anything needing one must move to the CP tier.
- The CP tier claims linearizable writes, but the testing strategy **deliberately opts out of linearizability checkers** (Knossos/Elle) because they'd flag expected AP anomalies (`design/16`). That logic is sound for the AP tier and wrong for the CP tier — the linearizable claims are exactly what a checker should verify. The Jepsen-style harness did catch a real acked-write-loss bug in range split (`design/30` §6.1), which proves the value.
- `LeasedBudget` (design/35) self-admits its STRICT "never overspend" guarantee is broken beyond the single-cluster stage-1 core (11 documented holes). It should not be marketed as strict until fixed.

### 1.3 Documentation drift — three instances that would mislead a new engineer

- `design/21_current_implementation_state.md` says the distributed layer is greenfield; docs 30–36 describe it built and load-tested. Doc 21 is dangerously stale.
- `design/27_transport_performance.md` is marked complete but describes mTLS/keepalive machinery on the deleted `http.Client` path (`internal/security/transport.go`); the live gRPC client is plaintext **[verified]**.
- `perf-report-{before,after,compress,reads}/` attribute CPU to `connect.(*Handler).ServeHTTP` etc. — dead code since the gRPC migration. No post-migration profile exists.

### 1.4 Robustness signals

- Two crash classes on the consensus fast path (pooled-buffer reuse corrupting a committed Raft entry; SI conflict aborts under concurrent shard apply) reached staging under load before being caught (`design/33` §2). Both are fixed and the harness now floods at 2,800 concurrent with 0 restarts — but the pattern is fast-path optimization landing ahead of its failure-mode analysis.
- Unbounded scan materialization (below) is an OOM-class availability risk, not just a perf issue.

---

## Part 2: Performance — where the time actually goes

### Tier A — structural bottlenecks (biggest wins)

**A1. KV replication is serial, unary, per-key** **[verified]** — `internal/kv/coordinator.go:202` (`replicateMinAck`) loops candidates sequentially, one blocking unary `StoreReplica` per candidate, `writeTimeout` 2s. One slow candidate stalls the write before the next is tried; nothing is batched (deferred per `bench/THROUGHPUT.md:27`). Historically 27% of blocked request time. Background `Fanout`/`RepairEngine` share the one-RPC-per-(key,candidate) shape.
→ Fix: concurrent fan-out to candidates (first-minAck-wins, hedged), plus a batched/streaming `StoreReplica` that coalesces writes per destination node (the same coalescing trick that gave the consensus tier 8.5× in `proposer.go`).

**A2. Scans do N+1 random point lookups** **[verified]** — `internal/recordstore/store.go:320`: for every meta-CF pointer row, a fresh full LSM point `Get` on the data CF (bloom + level search + block read). A 10k-row scan = 10k random reads instead of one merged iterator. Same in `ScanRecords`/`ScanRecordsFrom` — which also underpin repair backfill and intra-AE, multiplying the cost.
→ Fix: co-iterate meta and data CFs (keys share ordering), or inline small values into the latest-pointer record so the common scan never touches the data CF.

**A3. Unbounded scan materialization** — `internal/kv/scan.go:107`: routed scans call local + every-alive-member `ScanLocal` with **limit 0** and merge entire namespaces in memory before applying the caller's limit. OOM-class.
→ Fix: push the limit (with over-fetch factor) into local and remote scans; stream-merge instead of materialize.

**A4. Global `writeMu` held across SSTable I/O** **[verified]** — `wavesdb/txn.go:371-381`: Snapshot/Serializable commits take the DB-global `writeMu` and, while holding it, run `latestSeq` per written key — which probes memtable, immutables, and overlapping SSTables (real disk/block-cache reads). The KV hot path dodges it via `BatchRC`, but `ApplySiblings`, TTL clears, `Forget`, and any `LocalStore.Batch` user serializes on it.
→ Fix: sharded conflict-check locks (reuse the 512-stripe pattern from `recordstore/store.go:27`), or check conflicts optimistically outside the lock and validate seq under a short critical section.

**A5. The gRPC transport is untuned (and plaintext)** **[verified]** — `internal/rpcopts/grpc.go:25-44`: dial options are exactly `insecure.NewCredentials()`. No keepalive, no HTTP/2 window sizing (64KB default caps throughput on any non-loopback link), no max-message sizing, no write-buffer tuning. Server side (`grpcsrv/server.go`) similarly bare. All replication, AE, repair, and forwarding traffic rides this.
→ Fix: set `InitialWindowSize`/`InitialConnWindowSize` (≥1MB), keepalive client+server params, message-size limits; then re-add mTLS (design/27's session-resumption approach ports to gRPC via `tls.Config`).

### Tier B — pay-as-you-scale costs

**B1. Intra-cluster anti-entropy is O(keys × peers) full-record fetches** — `internal/replication/local/antientropy_intra.go:50-86`: per 2s tick, for each of 256 scanned keys, a unary `FetchReplica` to *every* alive peer, comparing full records — no digests, no Merkle trees. Cheap when idle; an RPC storm under churn, riding the untuned transport. (Ironically the *cross-cluster* AE has range hashes — `replication/global/antientropy.go` — though it rebuilds them by full scan each round.)
→ Fix: exchange (range → hash-of-(key,version)) digests first, fetch only diverging ranges; cache/incrementalize range hashes.

**B2. Consensus throughput defaults ship slow** **[verified]** — `internal/collections/manager.go:94`: `DefaultTunables` still hard-codes `RTTMillisecond: 50` (WAN clock); the design/32 headline fix (RTT 50→1-5ms) applies only if `WAVESPAN_COLLECTIONS_RTT_MS` is set. `MaxInMemLogSize` (the design/32 backpressure lever) never landed in `Tunables`. The GOGC=600/GOMEMLIMIT fix that cut core CPU 3.2→1.0 cores is deploy-manifest config, not a binary default. A fresh cluster gets 2019-era numbers.
→ Fix: make the measured-good values the code defaults; keep env overrides for WAN topologies.

**B3. Compaction merge is O(n·k) and materializes every value** — `wavesdb/compaction.go:408` (`pickSmallest`) linearly scans all input iterators per emitted entry, though a heap `mergingIter` exists in `iterator.go:65`; `compaction.go:239` reads full values (including vlog fetches) into memory per entry, defeating klog/vlog separation.
→ Fix: use the existing heap merger; pass vlog references through compaction without materializing.

**B4. Full manifest rewrite per flush/compaction under `db.mu`** — `wavesdb/db.go:261`: the entire catalog (all CFs, all SSTable metas, JSON-encoded CF configs) is rewritten after every flush and compaction. O(total-sstables) work at every flush, serialized on a global mutex.
→ Fix: incremental versioned manifest edits (RocksDB-style VERSION log).

### Tier C — hot-path hygiene

- **Per-write value copied ~3×** (txn arena, WAL frame, proto marshal) and per-read `append([]byte(nil), ...)` copies in `columnfamily.go:379,396` and `sst/reader.go:370`.
- **Two full LSM lookups + two proto decodes per Get** (meta pointer, then data record — `recordstore/store.go:578-596`).
- **`time.Now()` inside the skiplist read loop** (`memtable.go:95` TTL check) and per `latestSeq` probe.
- **Goroutine-per-key `MultiGet`** with a 32-slot channel semaphore (`kv/read.go:63`) for cheap in-process gets.
- **`CompactRange` ignores its bounds** and compacts the whole CF (`wavesdb_store.go:303`) — TTL/GC callers trigger full-CF rewrites.
- **Every scan opens/rolls back a full Txn with snapshot registration** (`wavesdb_store.go:272`); point Gets correctly bypass this, scans don't.
- **Per-commit global `publishMu` + map mutation** to advance the visible sequence (`db.go:197`).
- Write latency floor on the CP tier is the 3-replica Raft commit (~22ms idle, 60–80ms loaded, `design/33` §4) — throughput work can't fix this; only closed-timestamp follower reads (planned D3) and forwarded-write batching (planned D2) move the experienced numbers.

---

## Part 3: Prioritized improvement plan

**P0 — correctness-adjacent (do before more throughput work)**
1. Decide the durability contract explicitly: either default `SyncMode` to group-commit fsync (`SyncFull` — the WAL is already built for it) and measure the real cost, or document the 128ms window in design/00/13 and the ack response. (A1.1)
   **RESOLVED 2026-07-02:** production default is now `full` (tunables registry; offline paths — restore, snapshot CLI, tests — keep `none`). Measured on the ReadCommitted txn path (`wavesdb/sync_bench_test.go`, M2 Max, macOS F_FULLFSYNC = worst case): none 3.7µs/op, interval ~free, full 1.62ms/op serial but 48µs/op (~21k ops/s) at 32 committers — group commit amortizes; Linux fdatasync will be far cheaper. ADR-0002, design/02, design/13 updated; defaults pinned by `TestSyncModeDefaultIsFull` + `TestSyncModePlumbing`.
2. Bound scans: push limits into `scan.go` local+remote paths. (A3)
3. Re-profile on the live gRPC transport; retire/regenerate `perf-report-*`. Every subsequent decision depends on this.

**P1 — biggest verified throughput/latency wins**
4. Concurrent + batched replication fan-out in `replicateMinAck` and `Fanout` (port the `proposer.go` coalescing pattern). (A1)
5. Merged-iterator scans in `recordstore` (kills N+1; also speeds repair backfill + AE). (A2)
6. gRPC tuning: windows, keepalive, buffer sizes; then mTLS parity with design/27. (A5)
7. Ship measured-good consensus defaults in code (`RTTMillisecond`, `MaxInMemLogSize`, GC env in manifests). (B2)

**P2 — engine-level**
8. Sharded conflict check to remove global `writeMu`-across-I/O. (A4)
9. Heap-based, non-materializing compaction merge. (B3)
10. Incremental manifest. (B4)
11. Digest-based intra-cluster AE. (B1)

**P3 — hygiene sweep** — items in Tier C, guided by the fresh profiles from P0.3.

## Verification
- Each P1/P2 item: before/after `wavespan-bench` runs + pprof capture on the gRPC build; the bench harness and hdrhistogram plumbing already exist.
- P0.1: crash-durability test — kill -9 origin+replica inside the sync window, assert acked writes survive (extend the correctness harness; it already asserts acked-op semantics).
- AE/repair: measure RPC count per reconcile tick before/after digests on a seeded divergence.
