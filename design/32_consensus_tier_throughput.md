# 32. Consensus-tier throughput (collections fast path)

## Scope

This document is an engineering plan to make the **replicated-collections consensus tier** (design/30,
ADR 0007/0008 — dragonboat multi-Raft, CP) **10–100× faster** on the write path. The user observed the
tier doing only **~62–4000 sets/s** on the consensus path; this plan targets order-of-magnitude gains.

**In scope:** the consensus path, proposal pipeline, batching, the read path, sharding / keyspan
layout, the apply pipeline, the forwarding path, and dragonboat config tuning.

**Out of scope (deliberately):** the local IO / storage-engine rewrite. A separate effort is rewriting
`wavesdb`'s IO/fsync layer. This plan does **not** propose disk-format, WAL, or fsync changes. Where
"apply" is discussed it is about *decoupling commit from apply and batching the engine write call* —
not about how the engine persists bytes. The two efforts are complementary: the IO rewrite raises the
per-batch apply ceiling; this plan raises how many ops reach each batch.

This is a planning doc, not a contract change. It does not alter design/30's safety model
(linearizable per-collection writes, opt-in linearizable reads, quorums on the stable core). Every item
below preserves those guarantees unless explicitly flagged.

## 1. Why it is slow today (ground truth, file:line)

The current path is **one synchronous `SyncPropose` per op, on a single data shard, with no
client-side batching, with an in-datacenter Raft clock tuned for the WAN**. The state machine and
session choices are already good; the proposal *driver* and the *topology* are the bottlenecks.

### 1.1 One blocking `SyncPropose` per op

`Manager.Propose` issues exactly one synchronous, blocking `SyncPropose` per call
(`internal/collections/manager.go:188`):

```go
func (m *Manager) Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) {
	res, err := m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)   // :189
	...
}
```

Every datatype op (`SAdd`, `HSet`, `ZAdd`, `HIncrBy`, …) funnels through `proposeCmd` → `proposeCore`
→ `m.shard.Propose` → this one `SyncPropose` (`collections.go:52`, `:79`, `:81`). A caller doing N
sets serially pays **N full Raft round-trips** (leader append + quorum ack + apply + reply), each
gated by `RTTMillisecond`. That is the ~62–4000/s regime: throughput ≈ 1 / (commit latency), and
commit latency is dominated by the Raft clock, not the engine.

dragonboat *can* coalesce concurrent proposals into a few large log entries — but only if many are
**in flight at once**. With one blocking call per goroutine and a typical single-writer caller, the
batch depth is 1. The `bulk.go` path is the only place that fans out (`bulkRemoveConcurrency = 64`,
`bulk.go:11`, `:58`) and it exists precisely because "*firing them concurrently lets the shard leader
batch them instead of paying one round-trip per collection serially*" (`bulk.go:8`). That insight is
correct and must be **generalized to the whole write path**.

### 1.2 Single data shard → no multi-Raft parallelism

Topology is meta shard (id 1) + **one** initial data shard (id 2):

```go
const (
	MetaShardID    uint64 = 1   // control.go:16
	firstDataShard uint64 = 2   // control.go:17
)
```

`Bootstrap` starts exactly one data shard over the full range `[-inf,+inf)` (`control.go:39-45`).
`RangeDirectory.ShardFor` routes *every* collection to it until a `Split` happens (`meta.go:182`,
`directory.go`). Splits exist (`control.go:142`) but assume a **quiescent subrange** and are operator-
driven; there is no automatic pre-split and no load-based split. So all write load serializes onto one
Raft group, one leader, one apply loop — and a single dragonboat group has a hard ceiling
(~1.25M/s in dragonboat's own benchmarks; far lower at our current clock). We are nowhere near that
ceiling, but the single-group design caps the *eventual* headroom and concentrates all writes on one
leader.

### 1.3 In-datacenter clock tuned like a WAN

```go
func DefaultTunables() Tunables {
	return Tunables{ RTTMillisecond: 50, ElectionRTT: 10, HeartbeatRTT: 1, ... }   // manager.go:62
}
```

`RTTMillisecond: 50` means the Raft logical tick is **50 ms**. Heartbeats and the commit pipeline are
multiples of it. dragonboat's own guidance is to set `RTTMillisecond` to **1** when round-trips are
sub-millisecond (their benchmarks use ~500 µs). A 50 ms tick in an intra-region cluster (design/30 runs
*per-cluster*) inflates commit latency and starves the pipeline by an order of magnitude on its own.
There is also no `MaxInMemLogSize` set — the pipeline-depth / backpressure budget is left at default.

### 1.4 What is already good (do not "fix" these)

The mapping confirms several fast-path features are **already in use** — they are not the problem:

- **`IOnDiskStateMachine`**, not `IStateMachine` — `StartOnDiskReplica` with `newShardSM`
  (`manager.go:166`, `:177`). Deferred-sync, cheap snapshots, batched apply are available.
- **Batched `Update([]sm.Entry)`** — the SM already receives a *batch* of committed entries and flushes
  them in **one** `store.Batch(u.ops)` engine write, with cross-entry overlays so entries in the same
  batch see each other's effects (`statemachine.go:172`, `:287`; same pattern in `meta.go:84`). The
  apply side is already batch-shaped.
- **`GetNoOPSession`** — the idempotency-free fast write path, mandatory for on-disk SMs
  (`manager.go:189`). Application-level dedup via `command.Idem` is separate and optional.
- **`Lookup` off a consistent snapshot** — reads run concurrent with `Update` (`statemachine.go:441`).
- **`StaleRead` vs `SyncRead`** split already wired (`manager.go:196`).
- **Leader-hint caching** in the forwarder (`rpc_forwarder.go:37`,`:57`) and HTTP connection pooling
  (`transport.go`: `MaxIdleConns: 64`, `IdleConnTimeout: 90s`).

**Conclusion:** the apply pipeline and SM design are close to best-practice. The 10–100× lives almost
entirely in **(1) how proposals are driven (batch depth)** and **(2) how many active shards exist
(parallelism)** — exactly the two *multiplicative* levers the research identifies — plus a near-free
clock-tuning win. We do not need to rewrite the state machine to get the first 10×.

## 2. How the best systems do it (research, cited)

Survey of CockroachDB, TiKV, etcd/etcd-raft, ClickHouse Keeper (NuRaft), dragonboat, and YugabyteDB.
Grouped by lever; each technique is 1–2 lines + how it maps to WaveSpan. Full citation list in §9.

The anchor number: dragonboat reports **~9M writes/s aggregate** on a 3-node cluster with 16-byte
payloads, but a **single** Raft group caps at **~1.25M/s @ ~1.3 ms latency** — and warns that "*the
number of concurrent active Raft groups affects the overall throughput as requests become harder to be
batched*." That one sentence dictates the strategy: **the gain is (batch depth per group) × (number of
busy groups), traded against each other.**

### (A) Proposal batching / pipelining — the dominant lever

- **dragonboat async `Propose` + result channel** — `Propose` returns immediately with a
  `RequestState`; concurrent proposals coalesce into few large entries. *Map:* stop calling the
  blocking `SyncPropose` one-at-a-time; drive many async proposals per shard and join their channels.
- **CockroachDB transaction (write) pipelining** — a write returns right after being *proposed* to
  Raft, without waiting for consensus; the txn proves all outstanding writes in parallel before commit
  (O(n)→O(1) rounds). *Map:* collect per-op ack futures, join before returning a batch.
- **CockroachDB Parallel Commits** — commit record and intent writes run their consensus rounds
  concurrently. *Map:* for a multi-shard logical op, fire all per-group proposals in parallel.
- **etcd-raft Ready-loop batching + optimistic pipelining** — one `Ready` carries a batch of entries;
  successive `MsgApp` are sent without waiting for prior acks up to an in-flight window. *Map:*
  dragonboat does this internally — our job is to keep it fed.
- **ClickHouse Keeper request batching** — an explicit queue ahead of Raft groups N client ops into
  one proposal (`max_requests_batch_size`, default 100). *Map:* a **front-of-Raft request queue** per
  shard that coalesces client ops arriving faster than they can be proposed.
- **YugabyteDB group commit** — batches many outstanding updates into one Raft message batch. *Map:*
  same as the queue above; one log entry can carry many ops.

### (B) Sharding / pre-split / multi-raft parallelism — the second multiplicative lever

- **dragonboat NodeHost multiplexes many shards; idle groups are cheap** — "*thousands of idle groups
  have minimal throughput impact.*" *Map:* pre-split widely; cold shards are nearly free.
- **TiKV region = one Raft group (range sharding) + auto-split/merge** — data sharded into ranges,
  each its own group; hot regions split, cold ones merge. *Map:* our `RangeDirectory` is already the
  range-sharding seam; add N initial shards + load-based split.
- **CockroachDB load-based splitting** — split a hot range when QPS crosses a threshold (default 2500;
  deliberately *raised* from 250 because over-splitting hurts latency). *Map:* monitor per-shard op
  rate; split hot shards. **Caution: do not over-split** — sparse shards defeat batching (the dragonboat
  warning).
- **CockroachDB / YugabyteDB leader (leaseholder) balancing** — spread Raft leaders across nodes so
  write handling isn't concentrated. *Map:* placement driver should balance leaders across the stable
  core.

### (C) Read path — get reads off the write path

- **dragonboat `StaleRead`** (local, non-linearizable, no quorum round-trip) and **`SyncRead` /
  ReadIndex** (linearizable, batched, no log write) and **`NAReadLocalNode`** (zero-alloc hot-path
  variant). *Map:* tier reads; use the zero-alloc variant at high QPS.
- **TiKV ReadIndex + Lease Read / LocalReader** — within a leader lease, serve reads locally with no
  heartbeat round, on a separate reader thread. *Map:* lease-local reads on the leader as the middle
  ground between stale and full ReadIndex.
- **CockroachDB closed timestamps + follower reads** — leaseholder "closes" a slightly-past timestamp;
  any follower at ≥ that timestamp serves locally. *Map:* a closed-timestamp-style bound enables
  **consistent-as-of follower/learner reads on spot nodes** — huge for the demand-fill read path.
- **etcd-raft learner reads** — non-voting learners serve reads. *Map:* our demand-fill learners
  (`demandfill.go`) already do stale learner reads; formalize a staleness bound.

### (D) Async / batched apply — already mostly done here

- **dragonboat `IOnDiskStateMachine`** (deferred `Sync`, batched apply, cheap snapshots) and
  **`IConcurrentStateMachine`** (Lookup/SaveSnapshot concurrent with Update; entries delivered as a
  batch). *Map:* **we already use `IOnDiskStateMachine` with batched `Update`** (§1.4). Remaining win:
  ensure ack happens at **commit**, with apply overlapping the next commit, and never holding a lock
  across the engine batch.
- **etcd-raft async storage writes** (`AsyncStorageWrites`) and **TiKV separate apply thread pool** —
  move apply off the consensus loop so committing the next batch overlaps applying the previous.
  *Map:* confirm dragonboat's apply workers aren't serialized behind our `store.Batch`; keep `Update`
  non-blocking and deterministic.
- **CockroachDB proposer-evaluated KV** — do expensive logic *above* Raft so below-Raft apply is a
  trivial batched write. *Map:* our SM already computes deltas then flushes one batch; keep it that way.

### (E) Transport / forwarding coalescing

- **dragonboat `pb.MessageBatch` transport** — all per-shard traffic (appends, heartbeats, votes)
  multiplexed/coalesced over shared connections per host-pair, in order. *Map:* our cheap-mTLS HTTP
  transport (`transport.go`) must actually *fill* batches under load and preserve per-target order.
- **CockroachDB coalesced heartbeats / TiKV batched messages** — one heartbeat per node-pair per tick
  regardless of shared group count. *Map:* essential once we run many shards; verify dragonboat
  coalesces over our transport.
- **CockroachDB / TiKV: batch the forwarded writes** — *Map:* the spot→leader forward path
  (`rpc_forwarder.go`) sends one RPC per write; batch many forwarded ops into one `ProposeForward`.

### (F) Config / flow control

- **dragonboat knobs:** `RTTMillisecond=1` in-DC; `HeartbeatRTT` low (1–2); `ElectionRTT` ≈ 10×
  heartbeat; **`MaxInMemLogSize` large** (this is the pipeline-depth / backpressure budget — too small
  rejects proposals under burst); `SnapshotEntries` high/0 with on-disk SM; `CompactionOverhead` so
  laggards catch up by log not snapshot; `GetNoOPSession` (already used).
- **CockroachDB ProposalQuota / Admission Control** — bound in-flight proposals so a slow follower
  can't blow the log; priority queues under overload. *Map:* a per-shard in-flight quota in our batching
  proposer.
- **TiKV Hibernate Region** — stop ticking idle groups. *Map:* dragonboat quiesces idle shards
  already (design/30 §B.9); verify thresholds so pre-split cold shards stay cheap.

## 3. The plan — prioritized, phased

Each item: technique · expected magnitude · WaveSpan files/functions · risk · interaction with the
single-meta + data-shard design. Magnitudes are rough and multiply across independent items.

### Quick wins vs deep changes

| | Item | Effort | Expected | Type |
|---|---|---|---|---|
| **QW1** | Lower `RTTMillisecond` 50→1, set `MaxInMemLogSize` | XS | 5–20× | Quick win (config only) |
| **QW2** | Batching proposer: async pipeline + per-shard coalescing queue | M | 5–30× | Quick win (generalizes `bulk.go`) |
| **QW3** | Tier the read path (stale/lease-local; zero-alloc variant) | S | reads off write path; big effective write gain | Quick win |
| **D1** | Pre-split into N data shards (static hash/range) | M | 2–N× (leader spread + parallel apply) | Deep |
| **D2** | Batch forwarded writes (spot→leader) | S–M | 5–20× on the spot path | Deep-ish |
| **D3** | Closed-timestamp-style follower/learner reads on spots | L | demand-fill reads scale out | Deep |
| **D4** | Load-based / automatic split + leader balancing | L | sustains gains under skew | Deep |
| **D5** | Apply-pipeline hardening (commit≠apply, concurrent SM) | M | removes apply ceiling | Deep |

Sequencing: **QW1 → QW2 → QW3** reach **10×** with low risk and no topology change. **D1 + D2 + D5**
push toward **100×**. **D3 + D4** make the gains durable under real (skewed, spot-heavy) load.

---

### 3.1 Proposal pipelining / batching (QW2) — the dominant lever

**Technique.** Replace one-blocking-`SyncPropose`-per-op with (a) **async proposals** that return a
completion future, and (b) a **per-shard batching proposer** that groups ops arriving within a small
time/size window into the proposal stream, so dragonboat coalesces them into few large log entries.
This is `bulk.go`'s fan-out (`bulkRemoveConcurrency`, `bulk.go:11`) generalized into a first-class
component used by *every* write op. ClickHouse-Keeper's request queue + CockroachDB write-pipelining +
YugabyteDB group-commit, mapped onto dragonboat's async `Propose`.

**Design.**
- Add a `proposer` type per (NodeHost) that, per shard id, owns a small queue. Callers enqueue
  `(encodedCmd, resultCh)` and block on `resultCh`, not on the network.
- A per-shard flusher drains the queue and issues proposals. Two complementary modes:
  1. **Pipeline mode (minimal change):** for each queued item issue dragonboat's **async** `Propose`
     (returns a `RequestState`); collect the result channels; reply to each caller from its own
     channel. Bound in-flight count with a per-shard quota (Cockroach ProposalQuota; reuse the
     `bulkRemoveConcurrency` idea but make it the default path). This alone lets the leader batch
     because many proposals are in flight simultaneously.
  2. **Coalesce mode (bigger win, needs SM support):** group multiple *single-element* ops for the
     **same (shard, op, ns, coll)** into **one** multi-item `command` (the encoding already supports
     `Items []item`, `command.go:99`/`:111`) within a ≤~1 ms / ≤~256-item window, then one proposal
     applies them all in one `Update` entry. The SM's `applyCommand` already loops over `c.Items`
     (`statemachine.go:299`) and overlays make multi-item correct, so coalescing N `SAdd`s of one
     member each into one N-member `SAdd` is semantically identical and collapses N entries → 1.
- Preserve op result semantics: coalesced ops must return per-op results. For count ops the SM already
  returns a single aggregate; to coalesce *distinct* logical ops keep them as separate `Items` only when
  the per-item "newly added?" answer is recoverable. Simplest correct first cut: coalesce only when the
  caller doesn't need per-element attribution (e.g. fire-and-pipeline writers), and use pipeline mode
  (1) for everything else. Most of the 10× comes from mode (1).

**Files/functions.**
- New `internal/collections/proposer.go`: `proposer.Propose(ctx, shardID, cmd) (ProposeResult, error)`
  with internal queue + flusher + in-flight quota.
- `manager.go:188` `Propose` → delegate to the proposer (switch `SyncPropose` for async `Propose` +
  result-channel join). Keep `SyncPropose` as a fallback for control-plane ops (split/merge/meta).
- `collections.go:79` `proposeCore` unchanged (it still calls `m.shard.Propose`); the batching is
  hidden behind `Propose`.
- `bench_test.go`: add a concurrent-writer benchmark (the current `BenchmarkSAdd` is single-writer and
  *cannot* show batching — it measures serial commit latency). This is critical: the current benchmark
  structurally hides the win.

**Expected.** 5–30×. Per-group throughput moves from "1/commit-latency" toward dragonboat's batched
regime. Multiplies with QW1's latency cut.

**Risk.** Medium. Async proposals reorder relative to call order *within a shard* — but Raft per-shard
linearizability is preserved (dragonboat assigns log order); per-key correctness holds because the SM
overlays already handle same-batch ordering deterministically (`statemachine.go:176`). Per-op error
mapping (`wrongType`/`notNumber`/`frozenMark`, `collections.go:85`) must be threaded through each
result channel. Coalesce mode (2) needs care to keep per-element counts correct — gate it behind a
capability flag and land pipeline mode (1) first.

**Interaction with single-shard design.** Works *today* with one data shard (it deepens that one
group's batch). It also composes with §3.4: the proposer keys its queues by shard id, so when there are
N shards it fans batches out per shard automatically.

---

### 3.2 dragonboat tuning (QW1) — near-free latency cut

**Technique.** Set the Raft clock for an intra-region cluster and size the pipeline budget.

**Changes (in `DefaultTunables`, `manager.go:62`, and `NodeHostConfig`/`shardConfig`):**
- `RTTMillisecond: 50 → 1` (dragonboat guidance for sub-ms RTT; design/30 is per-cluster/intra-region).
  Election/heartbeat are multiples of RTT, so also keep `ElectionRTT` ≈ 10× `HeartbeatRTT` to avoid
  election storms (e.g. `HeartbeatRTT: 1`, `ElectionRTT: 10` stay as ratios but now over a 1 ms tick).
- Add **`MaxInMemLogSize`** to `Tunables` and set it generously (e.g. tens of MB) — this is the
  in-flight/un-applied log budget and the real backpressure knob; too small rejects proposals under the
  burst that QW2 creates.
- With `IOnDiskStateMachine`, `SnapshotEntries` can be raised (or 0) and `CompactionOverhead` tuned so
  laggards catch up by log replication, not full snapshot streaming.
- Keep `CheckQuorum: true` (`manager.go:157`) and `GetNoOPSession` (already correct).

**Files/functions.** `manager.go:50` `Tunables` (add `MaxInMemLogSize`), `:62` `DefaultTunables`, `:101`
`NodeHostConfig`, `:151` `shardConfig`.

**Expected.** 5–20× on its own (commit latency falls ~50×; realized gain bounded by scheduler/engine).
This is the single highest speedup-per-effort change — a few constants.

**Risk.** Low. A 1 ms tick increases heartbeat/election traffic per shard; fine for one shard, must be
re-validated when running many shards (§3.4) — coalesced heartbeats (E) and quiescing (F) keep it cheap.
Validate failover still works (election timeout becomes 10 ms — too aggressive only if the network is
genuinely lossy; in-cluster it is fine, and `CheckQuorum` guards split-brain).

**Interaction.** Independent of topology; multiplies with everything.

---

### 3.3 Read path (QW3 + D3) — get reads off the write path

**Technique.** Tier reads so they never traverse propose→apply, and push consistent reads onto
followers/learners (spot nodes).

**QW3 (quick):**
- Default non-critical reads to `StaleRead` (already wired, `manager.go:200`); make linearizable
  opt-in per call (the API already carries a `linearizable bool`, e.g. `collections.go:153`).
- Use dragonboat's **zero-alloc** local-read variant (`NAReadLocalNode`-style) on the hot read path to
  cut GC pressure at high QPS.
- Batch `SyncRead` (ReadIndex) requests when linearizable reads are needed, so one quorum confirm
  serves many reads.

**D3 (deep — the spot demand-fill win):**
- Add a **closed-timestamp-style** bound: the shard leader periodically publishes a "consistent-as-of"
  index/HLC; a follower or demand-fill learner whose applied index ≥ that bound serves reads locally
  with a *known* staleness, no quorum round-trip. This is CockroachDB follower reads / YugabyteDB leader
  leases mapped onto our learners. The SM already exposes applied index (`base_sm.go` `Open`/applied
  key, `statemachine.go:286`); surface it to the read path.
- This makes the **spot demand-fill read path** (`demandfill.go`, `collections.go:116` `read`) scale
  horizontally: spots serve consistent-bounded reads from their learner replicas without loading the
  leader.

**Files/functions.** `manager.go:196` `Read` (add zero-alloc + lease/closed-ts modes), `collections.go:116`
`read`, `demandfill.go`, a new "read lease / closed timestamp" publisher on the leader.

**Expected.** Reads stop competing with writes for the leader → large *effective* write-throughput gain
under mixed load (dragonboat's 9:1 read:write number is mixed-IO precisely because reads are cheap).
D3 lets demand-fill reads scale out across spots.

**Risk.** QW3 low (semantics already opt-in). D3 medium-high: closed-timestamp correctness (must never
serve below the bound; interacts with TTL expiry sweeps and freezes during split, `statemachine.go:183`).

**Interaction.** Orthogonal to sharding; complements demand-fill (design/30 §9) directly.

---

### 3.4 Sharding / keyspan preallocation (D1, D4) — second multiplicative lever

**Technique.** Replace the single data shard with **N pre-split data shards** so writes spread across N
Raft groups, N leaders, and N apply loops. Static pre-split first (D1); automatic/load-based split
later (D4).

**D1 — static pre-split.**
- At `Bootstrap` (`control.go:33`), instead of one full-range shard (`control.go:39-45`), create N data
  shards (ids `firstDataShard … firstDataShard+N-1`) and seed the directory with N ranges. Two layouts:
  - **Hash-based** (recommended first): route by `hash(routeKey) mod N`. Even spread, no hotspots, no
    range metadata churn. Requires a hash-routing `Directory` implementation alongside `RangeDirectory`
    (`directory.go`, `meta.go:182` `ShardFor`). Trades range-scan locality (acceptable — collections are
    addressed by exact (ns,coll), and cross-shard enumeration already exists in `bulk.go:22`
    `ListCollections` / `Shards()`).
  - **Range-based**: pre-seed N contiguous ranges in the meta shard (reuse `opMetaPut`, `meta.go:95`),
    splitting the keyspace at chosen boundaries. Keeps locality and the existing split/merge machinery;
    needs good initial boundaries.
- Balance leaders: start the N shards so their leaders spread across the stable-core voters (placement
  driver, design/30 §4) rather than all electing on one node — otherwise N groups but one hot leader.

**D4 — load-based / automatic split.**
- Track per-shard op rate (the proposer in §3.1 is the natural metering point). When a shard exceeds a
  threshold (CockroachDB default 2500 QPS — start *high*, over-splitting hurts), trigger the existing
  `Control.Split` (`control.go:142`). Requires lifting the "quiescent subrange" assumption
  (`control.go:141`) with the freeze/cutover already present (`opFreeze`/`opUnfreeze`,
  `statemachine.go:201`).
- Use dragonboat quiescing (design/30 §B.9) so pre-allocated cold shards cost ~nothing.

**Files/functions.** `control.go:33` `Bootstrap` / `:66` `BootstrapWithPlacement`, `directory.go` (new
hash directory), `meta.go:182` `ShardFor`, `control.go:142` `Split`, new placement/leader-balance logic.

**Expected.** 2–N× additional, bounded by leader spread and how busy each shard stays. **Caution:** the
gain is real only if each shard stays busy enough to batch (the dragonboat warning). With QW2 feeding
each shard, N≈ (cores / 2) busy shards is a reasonable first target, not hundreds.

**Risk.** Medium–high. Cross-shard ops become multi-group: `ListCollections`/`BulkRemove` already fan
out across `Shards()` (`bulk.go:22`,`:45`) so they generalize cleanly. Hash routing breaks range scans
(not currently a feature for collections). Split correctness under live writes (D4) is the hardest part;
D1 (static, at bootstrap) sidesteps it.

**Interaction.** This is *the* change that turns "single-meta + one data-shard" into real multi-Raft.
The meta shard stays single (it is low-write: directory updates only). The proposer (§3.1) and read
tiering (§3.3) both already key by shard id, so they light up automatically when N grows.

---

### 3.5 Apply pipeline (D5) — remove the apply ceiling

**Technique.** Ensure **commit is acked without waiting for apply**, apply runs batched and overlaps the
next commit, and reads never block behind apply. Most of this is already true here (§1.4); D5 is
*hardening*, not a rewrite.

**Changes.**
- Verify dragonboat acks the proposal at **commit** and applies asynchronously; ensure our `Update`
  (`statemachine.go:172`) holds **no lock across `store.Batch`** and does no network/external IO
  (design/30 §B.11 already mandates this — add a test/assert).
- Consider `IConcurrentStateMachine` for the data SM so `Lookup`/`SaveSnapshot` provably run concurrent
  with `Update` (today `Lookup` uses a snapshot so it already can; making the contract explicit lets
  dragonboat overlap apply and read scheduling).
- Keep apply cheap (CockroachDB PropEval-KV): the SM already computes deltas in memory then flushes one
  batch — preserve that; don't add per-entry engine round-trips.
- This is where the **separate IO/storage rewrite** plugs in: a faster `store.Batch`
  (`statemachine.go:287`) directly raises the per-apply-batch ceiling. This plan keeps the *shape*
  (one batched engine write per committed batch) that the IO rewrite optimizes.

**Files/functions.** `statemachine.go:172` `Update` / `:287` flush, `base_sm.go` (`Sync`/`Open`),
`manager.go:177` factory (potential `IConcurrentStateMachine`).

**Expected.** Removes a ceiling rather than adding a flat multiplier; matters most once QW2+D1 push
real load through. Synergistic with the IO rewrite.

**Risk.** Medium. Concurrent SM semantics and deferred `Sync` recovery (re-apply of un-synced entries
after reboot) must be tested against the existing crash-recovery invariant (applied index in the same
batch, `statemachine.go:286`; design/30 §B.12).

---

### 3.6 Forwarding path (D2) — batch spot→leader writes

**Technique.** Batch many forwarded writes into one `ProposeForward` RPC, and keep multiplexed
keep-alive connections. Today each forwarded write is a separate RPC (`rpc_forwarder.go:52`) even though
leader-hint caching (`:57`) and HTTP pooling (`transport.go`) already exist.

**Changes.**
- Add a batched forward: accumulate forwarded `(ns, coll, cmd)` triples for a short window and send one
  `ProposeForwardBatch` RPC; the receiver applies them via the §3.1 proposer (so they coalesce into the
  leader's batch too). This is double-batching: batch on the wire *and* into the Raft log.
- Extend the `Forwarder` interface (`collections.go:25`) and `ProposeForward` service handler
  (`service.go:320`) with a batch variant; keep the single-op path for compatibility.
- Keep the leader-hint cache (`rpc_forwarder.go:37`) — steady state stays one hop.

**Files/functions.** `rpc_forwarder.go:35` `Forward`, `collections.go:25` `Forwarder` /
`:75` `ProposeRaw`, `service.go:320` `ProposeForward`, proto (`ProposeForwardRequest` → add a batch
message).

**Expected.** 5–20× on the spot write path specifically (the demand-fill / edge-write path), which is
where many real writes originate.

**Risk.** Medium. Partial-failure semantics in a batch (some ops commit, some don't) must return
per-op results — mirror the per-op result channels from §3.1. Ordering across a batch must respect
per-key linearizability (group by shard, preserve order within the RPC).

**Interaction.** Sits on top of §3.1 (the receiver's proposer batches the forwarded ops) and §3.4 (the
batch may span shards → split per shard before enqueueing).

## 4. Sequencing to 10× then 100×

**Reach 10× (low risk, no topology change):**
1. **QW1** — clock/`MaxInMemLogSize` tuning (`manager.go`). Hours. Biggest speedup-per-effort.
2. **QW2 pipeline mode (1)** — async proposer + per-shard in-flight quota. Days. Generalize `bulk.go`.
3. Add a **concurrent-writer benchmark** so the gain is visible (the current single-writer
   `BenchmarkSAdd` cannot show batching). Half a day — do this *before* QW2 to baseline correctly.
4. **QW3** — default stale reads + zero-alloc local read.

**Reach 100× (the multiplicative levers + ceilings):**
5. **D1** — static pre-split into N hash-routed data shards + leader balancing.
6. **QW2 coalesce mode (2)** — collapse same-(shard,op,coll) ops into one multi-item entry.
7. **D2** — batched forwarded writes for the spot path.
8. **D5** — apply-pipeline hardening (commit≠apply, concurrent SM), and dovetail with the IO rewrite.

**Make it durable under real load:**
9. **D4** — load-based split + automatic leader balancing (handles skew/hotspots).
10. **D3** — closed-timestamp-style follower/learner reads so demand-fill reads scale across spots.

The first multiplicative product (QW1 latency × QW2 batch depth) is expected to clear 10× on a single
shard. Stacking D1 (N busy shards) × D5 (no apply ceiling) × D2 (spot path) is what reaches 100×, with
D3/D4 preventing regressions under skew.

## 5. Top 5 highest-leverage changes (ranked by speedup-per-effort)

1. **QW1 — RTTMillisecond 50→1 + MaxInMemLogSize** (`manager.go`): a handful of constants, 5–20×.
2. **QW2 — batching/pipelining proposer** (new `proposer.go`, `manager.go:188`): generalizes `bulk.go`,
   5–30×, the dominant lever.
3. **QW3 — tier the read path** (`manager.go:196`): reads off the write path; large effective gain
   under mixed load, low risk.
4. **D1 — static pre-split into N shards** (`control.go:33`, `directory.go`): the second multiplicative
   lever; 2–N×.
5. **D2 — batch forwarded writes** (`rpc_forwarder.go`, `service.go:320`): 5–20× on the spot path
   specifically.

## 6. Risks & invariants to preserve

- **Per-collection linearizable writes** (design/30 contract) — async/coalesced proposals stay within
  one shard's Raft order; the SM overlays (`statemachine.go:176`) already make same-batch ordering
  deterministic. Do not coalesce across shards into one entry.
- **Crash recovery / no double-apply** — applied index is flushed in the same batch as state
  (`statemachine.go:286`); deferred `Sync` (D5) re-applies un-synced entries on reboot — must stay
  idempotent (design/30 §B.12).
- **Split/freeze correctness** — coalescing and pre-split must honor `frozenMark` rejection during
  migration (`statemachine.go:212`, `collections.go:90`).
- **Over-splitting hurts** — keep shards busy enough to batch (the dragonboat warning); prefer few busy
  shards over many sparse ones.
- **Failover at a 1 ms tick** — re-validate election behavior under the aggressive clock; `CheckQuorum`
  guards split-brain.

## 7. Validation

- Add a **concurrent-writer** benchmark (N goroutines, M ops each) alongside `BenchmarkSAdd`
  (`bench_test.go`) — the single-writer benchmark structurally cannot show batching and will *under*-
  report every change here.
- Per-shard op-rate metrics from the proposer (feeds D4 and the UI, design/30 §13.14).
- Correctness harness (design/25) must pass unchanged after each item — these are throughput changes,
  not semantic ones.

## 8. Relationship to other docs

- **design/30** (replicated collections) — this is its throughput follow-up; §5.7 (many groups), §6
  (split/merge), §9 (demand-fill), §10 (TTL), §12/Appendix B (dragonboat usage) are the substrate.
- **ADR 0007/0008** — consensus tier + dragonboat choice; unchanged.
- **design/27** (transport performance) — the cheap-mTLS transport that must coalesce `MessageBatch`.
- **The separate IO/storage rewrite** — orthogonal; D5 keeps the one-batched-write apply shape it
  optimizes. This plan does not touch disk/fsync.

## 9. Citations

- **dragonboat:** github.com/lni/dragonboat · pkg.go.dev/github.com/lni/dragonboat/v4 (+ `/config`,
  `/statemachine`, `v3/raftio`) — async `Propose`, `IOnDiskStateMachine`/`IConcurrentStateMachine`,
  `StaleRead`/`SyncRead`/`NAReadLocalNode`, `MessageBatch`, `RTTMillisecond`/`MaxInMemLogSize`/
  `GetNoOPSession`, the 9M/s aggregate & 1.25M/s single-group & "concurrent active groups affect
  batching" notes.
- **CockroachDB:** cockroachlabs.com/blog/{transaction-pipelining, parallel-commits, scaling-raft,
  follower-reads-stale-data, admission-control-unexpected-overload} · docs/stable/{load-based-splitting,
  follower-reads, admission-control} · github.com/cockroachdb/cockroach/{issues/17500,
  docs/design.md (proposer-evaluated KV), pull/39687 (split threshold 250→2500)}.
- **TiKV:** pingcap.com/blog/design-and-implementation-of-multi-raft · tikv.org/blog/{lease-read,
  double-system-read-throughput, tune-with-massive-regions-in-tikv, how-tikv-reads-writes} ·
  docs.pingcap.com/tidb/stable/{tune-tikv-thread-performance, tikv-configuration-file} ·
  github.com/tikv/{raft-rs, tikv/issues/10540}.
- **etcd / etcd-raft:** pkg.go.dev/go.etcd.io/raft/v3 · github.com/etcd-io/raft ·
  github.com/etcd-io/etcd/pull/14627 (AsyncStorageWrites).
- **ClickHouse Keeper / NuRaft:** clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper ·
  clickhouse.com/blog/clickhouse-keeper-a-zookeeper-alternative-written-in-cpp.
- **YugabyteDB:** yugabyte.com/blog/low-latency-reads-in-geo-distributed-sql-with-raft-leader-leases ·
  github.com/yugabyte/yugabyte-db/blob/master/architecture/design/docdb-raft-enhancements.md.

Two caveats from the research: CockroachDB's exact `MaxQuotaProposalSize` constant was not source-
verified (the `quotaPool`/ProposalQuota mechanism is confirmed — grep `pkg/kv/kvserver` to pin the
name); etcd-the-cluster and ClickHouse Keeper are single-Raft and contribute the per-group engine, not
the sharding layer.
