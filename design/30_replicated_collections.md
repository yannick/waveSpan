# 30. Replicated collections (consensus tier)

## Scope

A second distribution tier for **complex, fleet-shared datatypes** — sets, hash tables, sorted
sets — that are **written from a central point and read everywhere**. It provides:

- linearizable writes and atomic multi-element updates per collection (within a shard);
- bounded-stale **local** reads on every replica, with opt-in linearizable reads;
- read scaling by **on-demand replicas** that fill where queries land;
- coexistence with the existing AP cache KV (design/03) with **no change to its hot path**;
- safety under spot-edge churn by keeping quorums on an annotated **stable core**.

It deliberately does **not** provide: synchronous cross-geo writes (the global layer, design/06,
stays async/eventual); linearizable reads on every `get` (opt-in only); or any change to the cache
tier's semantics.

This tier runs **per-cluster** (intra-region). It does not contradict ADR 0001 — that ADR sets eventual
consistency as the *default* and rejects global linearizability; this tier is an opt-in, per-namespace,
in-region exception. It does press on design/00's spot-churn assumption, which §4 resolves with node
roles. The consensus-tier decision is recorded in ADR 0007; the Raft-library choice (dragonboat) is
ADR 0008 — see §12 and Appendix B.

## 1. Motivation: two workloads, two tiers

WaveSpan carries two workloads with opposite needs:

| | **Cache tier** (design/03, unchanged) | **Replicated tier** (this doc) |
|---|---|---|
| Purpose | fast local node caching | application data distributed from a central point |
| Consistency | eventual, HLC-LWW, best-effort | linearizable writes, bounded-stale reads |
| Write origin | any node (coordinator) | a central writer → shard leader |
| Read pattern | point, local-first | read-everywhere, enumeration |
| Durability model | origin+1, repair | Raft quorum |
| Runs on | every node incl. spot edge | voters on stable core; learners anywhere |

The tiers are selected **per namespace** (`distribution: cache | replicated`). A request's class is
resolved by an O(1) namespace lookup at the edge; a cache write never enters the consensus path, so
the existing put/get latency is unaffected. The only shared resource is the local storage engine
(§11) and gossip metadata, both isolated and bounded.

## 2. Sharding model: range-based multi-Raft

Collection cardinality and size **vary widely** (millions of tiny collections; a handful of huge
ones). Therefore **a collection is not a Raft group** — it is a key *prefix* that may span one or many
shards.

The replicated keyspace is a single ordered space:

```text
r / <namespace> / lenPrefix(collectionID) || <element>
```

It is split into contiguous **ranges** by size and load; **each range is one Raft group** (a shard).
Terminology: **range = Raft group = shard** — one object, used interchangeably here; "shard" is the
dragonboat API spelling (Appendix B).
This handles the variance naturally:

- thousands of tiny collections co-inhabit a single range/group;
- a huge collection spreads across many ranges/groups, each independently led and replicated.

Ranges **split** at size/throughput thresholds and **merge** when adjacent ranges shrink (§6) — the
TiKV/CockroachDB model. Per-collection groups are just the degenerate case where one collection fills
one range. Crucially, **group count ≈ number of ranges, not number of collections** — co-inhabiting
many small collections per range, plus quiescing idle groups (§5.7), is what keeps multi-Raft
affordable at high collection counts.

Each group: **3 or 5 voters** (per-namespace, 5 for hotter/churnier ranges) on core nodes, plus
**0..N learners** (non-voting) added on demand.

## 3. Element layout per datatype

All keys for a collection share the `lenPrefix(collectionID)` prefix, so a collection's data is
contiguous and a single collection's small mutations stay within one range.

- **Set** — `…\x00<member>` → empty. `SADD`=Put, `SREM`=Delete, `SISMEMBER`=point read,
  `SMEMBERS`=prefix scan over `\x00`.
- **Hash** — `…\x00<field>` → value. `HSET`/`HGET`/`HDEL`/`HGETALL` analogously.
- **Sorted set** — `…\x00<sortableScore><member>` → empty (range-scannable in score order, `ZRANGE`),
  plus a pointer `…\x01<member>` → score so `ZADD` can find and replace the old score key. A score
  change (delete old score-key + put new + update pointer) is a **single-range atomic batch** because
  all three keys share the collection prefix and a member's keys are adjacent.
- **Reserved sub-keys** (state-machine maintained, not user-visible):
  - `…\x02header` → `{type, created_index}` — fixes the collection's datatype (§13.3).
  - `…\x03card` → element count **for the portion of the collection in this range** (a *per-range
    partial counter*). Exact and lossless because the shard is single-writer linearizable — the
    counter is updated in the same log entry as the element. `SCARD` sums the partial counters across
    the ranges the collection spans (bounded by span count).
  - `…\x04<expiryUnixMs><element>` → TTL index (see §10).

## 4. Node roles: voter-eligible core, learner edge

Stable nodes are annotated in Kubernetes (e.g. `wavespan.io/voter-eligible=true`); the annotation is
surfaced into node config and advertised as a membership flag (`voterEligible`) over gossip.

- **Voters** (Raft voting members) are placed **only** on voter-eligible nodes. Quorum therefore
  lives entirely on the stable core.
- **Learners** (non-voting replicas) run **anywhere**, including spot edge. They receive the committed
  log but are not counted in quorum, so they never slow commits and their loss never affects
  availability.

Consequence for churn: losing spot-edge nodes removes only learners (re-added elsewhere on demand);
the write/commit quorum is untouched. Core scale-down must be **graceful** — a k8s `preStop` hook
cordons the node, the placement driver transfers any leaderships away and hands off voter membership
(learner-add-then-voter-remove, §5.6) before termination.

## 5. Multi-Raft protocol

Each range runs an independent Raft group over the chosen library (§12). This section defines the
**protocol semantics** the library must support; §12 maps them onto candidate libraries.

### 5.1 Group identity and membership

A group has a stable `group_id`, a key span `[start, end)`, a term-versioned **voter set** (on
voter-eligible nodes) and **learner set** (anywhere). The authoritative span and the *desired* replica
set live in the meta directory (§7); the *effective* membership is whatever the group's own committed
conf-changes say. The directory **follows** the group's committed membership, never the reverse — so a
node never serves as voter for a group that has not committed its addition.

### 5.2 Log command set

The replicated state machine consumes a single log-entry type:

```proto
message LogCommand {
  oneof cmd {
    Mutate     mutate = 1;   // data write: element ops + optional preconditions (the Apply batch)
    ExpireBatch expire = 2;  // leader-proposed TTL deletes (§10)
    ConfChange conf   = 3;   // add/remove voter, add/promote/remove learner
    RangeSplit split  = 4;   // split this range at split_key (§6)
    RangeMerge merge  = 5;   // absorb the right neighbour into this group (§6)
    bytes      noop   = 6;   // leader no-op at term start (commits prior term entries)
  }
  uint64 proposal_id = 7;    // routes the apply result back to the waiting RPC
}
```

### 5.3 State machine and conditional apply

The state machine is **deterministic and on-disk**: it applies committed `Mutate` ops to `CFReplData`
(element keys + reserved sub-keys) through **one atomic engine batch that also writes the applied
Raft index**, so apply is crash-consistent (on restart, the engine and the log agree on the applied
index; replay resumes from there).

Preconditions (CAS) are part of the `Mutate` command and are **evaluated at apply time against the
committed state**, not at propose time — every replica evaluates identically, so all agree on
commit-vs-abort. The apply produces the op's result (e.g. `added_count`, or `FAILED_PRECONDITION`),
which the leader routes back to the waiting RPC via a `proposal_id`-keyed result channel. This makes
CAS linearizable without a read-then-write race.

**Determinism rules:** no wall-clock, RNG, or host-specific input in apply. TTL expiry is leader-
proposed (§10); generated ids/timestamps ride in the command.

### 5.4 Reads

- **Linearizable read:** routed to the leader, which performs a **read-index** (confirm it still leads
  via a heartbeat quorum, or a leader lease if enabled) and serves at that index.
- **Bounded-stale read:** any in-sync replica (voter or learner) serves from its applied state and
  reports `applied_log_index` + `staleness_ms`. Read-your-writes uses the `read_after_index` watermark
  (§13.10): the replica waits until `applied_index ≥ read_after_index` before answering.

### 5.5 Snapshots and catch-up

Because state is on-disk in `CFReplData`, a snapshot is a **consistent view of the range's key span at
an applied index** (an engine checkpoint/iterator over `[start,end)`), not a serialized blob of all
state — so snapshots are cheap and depend on `wavesdb`'s checkpoint/snapshot capability. Log is
truncated up to the snapshot index. A new voter or learner catches up by: receiving the range snapshot
(stream the key span) → installing it → tailing the log to the leader's commit index → then serving.

### 5.6 Membership changes

All membership changes are Raft conf-changes (single-server change or joint consensus, per library):

- **Add a voter:** first add as a **learner** (catch up via §5.5), then promote to voter once caught
  up. This avoids stalling quorum on a slow new member.
- **Remove a voter / graceful drain:** transfer leadership away if it leads, add a replacement learner,
  promote it, then remove the departing voter.
- **Learner add/remove:** demand-fill (§9) and eviction, also conf-changes.

The meta directory is updated **after** the conf-change commits.

### 5.7 Many groups on one node

A node hosts many groups, so the protocol must be cheap per-group:

- **Coalesced heartbeats:** one periodic message per *peer node* carries ticks/heartbeats for all
  co-located groups (the "multi-Raft" optimization), over the shared transport (§11).
- **Batched appends/applies:** entries for many groups to the same peer ride one stream; applies share
  a worker pool.
- **Quiescing:** a group with no in-flight proposals and stable membership stops ticking until traffic
  or a membership event wakes it — essential when there are very many small, idle ranges.

These three are the difference between "multi-Raft scales to thousands of groups" and "a heartbeat
storm." They are also the main criteria for the library choice (§12).

## 6. Range split and merge

The range is the unit of Raft replication and must stay bounded (log size, snapshot size, single-
leader throughput). The PD (§7) splits large/hot ranges and merges cold adjacent ones to cap group
count.

### 6.1 Split (migrate-on-split)

Trigger: a range exceeds a size or write-rate threshold (the group leader reports load to the meta
group; the PD decides). The split key is chosen at a **collection boundary** when possible; only a
single collection that alone exceeds a range is split at an interior element boundary.

**Implementation reality (ADR 0008): dragonboat shards are independent Raft groups with no split
primitive, so a split is *migrate-on-split*, not in-place.** An in-place split (truncate the parent,
spin up a new group over the *same* local data, no movement — the CockroachDB model) needs a Raft
engine where one group can be divided; dragonboat does not offer that. So splitting `[start,end)` at
`split_key` is:

1. allocate a new shard id (`max(existing)+1`) and **start the new (empty) shard**;
2. **migrate** the subrange `[split_key, end)` — read the source shard's keys whose routing key falls
   in that range (covering both the datatype state and the TTL index) and **ingest** them into the new
   shard (batched, log-committed);
3. **cut the directory over** — shrink the source range to `[start, split_key)` and add
   `[split_key, end) -> newShard`;
4. **purge** the migrated subrange from the source shard.

Clients with a stale directory get `WRONG_RANGE` and refresh. The cost is real data movement (size of
the migrated subrange) and a brief window between cutover and purge where the source still holds stale
copies (unread, since the directory already points at the new shard). v1 assumes the splitting
subrange is **quiescent** during migration; a freeze/cutover for concurrent writes (§6.2) is the
follow-up. This is acceptable for a centrally-written tier where splits are rare.

### 6.2 Merge

Merging adjacent ranges is more delicate (must not lose writes):

1. PD first **aligns replica sets** — moves replicas so the left and right ranges share the same nodes.
2. The right range is **frozen** (stops accepting proposals; in-flight ones drain or abort).
3. A `RangeMerge` entry is committed in the **left** group; on apply, the left group absorbs the right
   range's key span and data, and the right group is retired.
4. The directory updates; the right `group_id` is tombstoned.

The freeze window is the only unavailability and is bounded; merges are rate-limited and only chosen
for sustained low load.

### 6.3 Interaction with cursors and in-flight ops

Enumeration cursors encode per-range `{range_id, last_key, read_index}` (§13.8). After a split, a
cursor resumes by directory lookup of its current key position → it lands in whichever child now owns
that key, transparently. Proposals that arrive after a `RangeSplit`/`RangeMerge` for a key that moved
are rejected `WRONG_RANGE` and retried to the correct group.

## 7. Control plane: meta group + placement driver

### 7.1 Meta group

A dedicated Raft group (the "system range"), **3–5 voters on core**, bootstrapped from a seed set
(k8s StatefulSet ordinals / config). It holds, linearizably:

- the **range directory**: `[start,end) → {group_id, replicas:[{member, role, …}], leader_hint, epoch}`;
- the **node registry**: `{member_id, addr, zone/region/geo, voterEligible, health}`;
- **namespace config** for the replicated tier;
- the **PD work queue** and per-range load/health reports.

Its content changes slowly (splits, placement, membership), so it is low-throughput despite being
linearizable.

### 7.2 Bootstrap

At cluster start the seed nodes form the meta group via standard Raft bootstrap with an initial member
set from config/k8s. The first replicated namespace creates a single initial range `[min,max)` placed
on N voter-eligible nodes; everything else grows from there by splits (§6).

### 7.3 Directory distribution and routing

The range directory is the routing table; every node caches it. It is bootstrapped by reading the meta
group and kept fresh by **watching the meta group's applied changes** (it is a Raft group, so it can
stream a change feed) with a gossiped directory-version for staleness detection. Routing is a local
interval lookup `key → range → group → leader/replicas`. A stale cache self-heals: the contacted node
answers `WRONG_RANGE`/`NOT_LEADER` carrying the authoritative mapping, and the caller retries.

### 7.4 Placement-driver algorithm

A control loop on the meta leader:

- **Inputs:** per-range size/load/health (group leaders report periodically to the meta group); the
  node registry.
- **Replica placement scoring:** for each range needing a voter (new, or after a loss), score voter-
  eligible nodes by (1) **failure-domain diversity** — spread voters across zones/regions; (2) **load**
  — range count, disk, CPU; (3) **locality** — proximity to the writer/other leaders. Choose the top
  scorers that preserve diversity. If fewer than the namespace's `voters` count of eligible nodes
  exist, the range runs **degraded** at the available voter count (never two voters on one node) and
  the PD up-replicates to the target as more voter-eligible nodes appear.
- **Up/down-replication:** on voter loss, add a replacement (learner → catch up → promote); on node
  join, rebalance some ranges onto it.
- **Leader balancing:** spread leadership across nodes via leadership transfer to avoid hot leader
  nodes.
- **Split/merge:** trigger splits on threshold breach; merge sustained-low-load adjacent same-replica
  ranges.
- **Learner lifecycle:** approve demand-fill learner adds (from read-heat reports, §9), evict cold
  learners (LRU/heat), and enforce `pinned` (a learner on every node for the namespace/collection).

Every PD decision is enacted as a conf-change / split / merge **in the target group**; the directory
updates only after the group commits it.

### 7.5 Meta-group failure

Meta quorum loss **freezes the PD** (no placement, splits, merges, or learner changes) but data groups
keep serving from the cached directory and their own committed membership — **management degrades, data
serving does not.** The meta group uses the same `unsafe-recover` tooling (§16) if it loses majority.

## 8. Write path

1. A write (typically from the central application writer) lands on any node and is routed to the
   target range's **Raft leader** via the cached directory (redirect on staleness).
2. The leader proposes a `Mutate` `LogCommand` (the `Apply` batch: ops + optional preconditions),
   commits on a **voter majority**, the state machine applies it atomically (§5.3), and the result is
   returned to the client.
3. Committed entries replicate to **learners** as well, keeping read replicas fresh.

**Atomicity.** A batch that stays **within one range** is fully atomic and linearizable (one log
entry). This covers "update several fields of a hash" and "modify members of a set" whenever the
touched keys share a range — which, for any single bounded collection, they do. Atomic operations that
span **multiple ranges** of one huge collection are **out of scope for v1** and return `CROSS_RANGE`;
a cross-range transaction (Percolator/2PC) is the v2 extension (§19).

## 9. Read path and demand-fill

**Default = bounded-stale local read.** If the node holds a replica (voter or learner) of the range,
it serves from applied state. Staleness equals apply lag (typically single-digit ms). The reply's
honesty metadata carries the served-by role, the applied log index, and a staleness estimate
(§15) — consistent with the existing `ResponseMeta` contract.

**Miss path (demand-fill).** If the node does not hold the range:

1. it forwards the read one hop to the nearest replica (immediate answer); and
2. if read heat for that range crosses a threshold, it asks the PD to add it as a **learner**. The PD
   adds it; the node catches up (§5.5), then serves locally. Until caught up, reads forward to a voter
   or report `NOT_READY`.

**Eviction.** Learners are reclaimed by the PD on an LRU/heat policy to bound footprint. A namespace or
collection may be **pinned** (`pinned: true`) so a learner is kept on every node — the eager
"replicate everywhere" mode, equivalent to today's `replicationFactor: all` but consistent.

**Opt-in linearizable read.** A request may set `consistency: LINEARIZABLE`; it is routed to the leader
for a read-index (§5.4). This costs a leader round-trip and is reserved for read-your-writes needs; it
is **not** the default, because the default's whole value is local reads.

**Enumeration** (`SMEMBERS`/`HGETALL`/`ZRANGE`) is a prefix scan. It is `COMPLETE` iff the node holds
**and is caught up on all ranges** the collection spans; otherwise it gathers the missing ranges from
their replicas (bounded fan-out to *known* ranges, far stronger than the cache tier's open scatter) or
reports `PARTIAL`. For pinned/replicate-everywhere collections, every node holds all ranges, so
enumeration is a local `COMPLETE` scan — the headline win over the cache tier's `routed-eventual` scan.

## 10. TTL and GC must be log-driven

Replicas must apply identical state, so expiry **cannot** run independently per replica. The **leader**
owns the TTL index (`…\x04<expiry><element>`); when an entry is due it **proposes an `ExpireBatch` log
entry**, which every replica applies deterministically. The same rule governs tombstone GC and range
compaction triggers. (Independent per-replica expiry would diverge the state machines.)

A write's **relative** `ttl_ms` (Appendix A `ElementOp.ttl_ms`) is resolved by the **leader to an
absolute `expiryUnixMs` before the entry is proposed**, and stored absolute — so every replica writes
the identical expiry into the TTL index and nothing is ever recomputed from apply-time wall-clock.

## 11. Storage isolation and transport

Replicated-tier data lives in its own column families, separate from the cache tier:

- `CFReplLog` — per-group Raft log (unless the library owns log storage — §12);
- `CFReplData` — applied state-machine KV (element + reserved sub-keys);
- `CFReplMeta` — Raft hard state, snapshot metadata, the cached range directory.

The cache tier keeps `CFKVData`/`CFKVMeta` untouched. The one real coupling is shared engine resources
(WAL, compaction, block cache). Mitigation: the replicated tier is read-heavy/write-light by design, so
its write pressure is small; if needed, the Raft log runs in a separate engine instance for WAL
isolation.

**Transport.** Heartbeats are coalesced per peer (§5.7) over the existing pooled, cheap-mTLS HTTP/2
client (design/27). Whether Raft messages reuse this transport or the library's own is a §12 decision.

## 12. Raft library selection

**Decision: dragonboat** (§12.5, ADR 0008) — chosen to ship the consensus tier faster by reusing its
multi-Raft manager rather than building one on a single-group kernel; we accept the adapter work to
honor WaveSpan's constraints. WaveSpan needs a Go Raft implementation that supports **many groups per
process** efficiently (§5.7). Two constraints are **decided**:

- **Raft log storage is unified in `wavesdb`** (one engine for log + state; simpler ops/backup).
- **CGO-free** is a hard requirement (README rule #1: single-language Go, no FFI on the data path).

### 12.1 Candidates

| Dimension | **dragonboat** (`lni/dragonboat`) | **etcd/raft** (`go.etcd.io/raft`) | **hashicorp/raft** |
|---|---|---|---|
| Multi-group native | ✅ `NodeHost` runs thousands of groups; coalesced heartbeats, pipelining, batching built in | ⚠️ kernel only — you build the multi-group manager | ❌ single-group oriented; no coalescing |
| Log storage | bundled **Pebble** (Tan optional); pluggable | **BYO** — store the log in `wavesdb` | BYO (`LogStore`) |
| Transport | pluggable `raftio.ITransport` — can wrap cheap-mTLS | **BYO** — wire it yourself (reuse cheap-mTLS) | pluggable `Transport` |
| On-disk state machine | ✅ `IOnDiskStateMachine` → persist into `wavesdb`, snapshot deltas | you orchestrate apply + snapshots yourself | FSM + snapshots, you orchestrate |
| Learners (non-voting) | ✅ observers + witnesses | ✅ learners | ⚠️ non-voting added later, less ergonomic |
| Maturity / governance | production use; **largely single-maintainer** (longevity risk for a foundational dep) | battle-tested (etcd, CockroachDB, …); **CNCF-governed** | very stable; Consul/Nomad/Vault — but **few groups** |
| Build effort for us | **low** — multi-Raft plumbing is solved | **high** — we build Cockroach's lower half | high *and* poor fit at scale |
| Fit to "build above the engine" (README rule #1) | medium (cedes log + concurrency model) | **high** (unified `wavesdb`, our transport, full control) | low |

`hashicorp/raft` is effectively **ruled out**: it is not built for thousands of groups (per-group
goroutines/transport, no heartbeat coalescing). The real choice is **dragonboat vs etcd/raft**.

### 12.2 The trade-off

- **dragonboat** solves the hardest, most error-prone part (the multi-Raft manager: tick coalescing,
  snapshot streaming, flow control, quiescing) and ships an on-disk state-machine model that fits
  `wavesdb`. Cost: it brings its own **LogDB** (a second embedded engine unless we implement a
  `wavesdb`-backed LogDB), an opinionated `NodeHost` concurrency model, and a **single-maintainer**
  governance/longevity risk for a foundational dependency.
- **etcd/raft** is a minimal, battle-tested kernel that lets us put the log in `wavesdb`, reuse the
  cheap-mTLS transport, and keep total control — the best fit with how WaveSpan builds everything else
  above the engine. Cost: we build the **entire** multi-Raft manager ourselves (the bulk of what
  CockroachDB built on top of etcd/raft), which is substantial and correctness-critical.

With storage unification and CGO-free now **decided**, and governance **deferred**, the remaining live
axis is build-vs-buy weighed against the integration footprint below.

### 12.3 Integration footprint: control plane, transport, CGO

| Concern | dragonboat | etcd/raft |
|---|---|---|
| Raft transport | **its own** (pluggable `raftio`); to use cheap-mTLS HTTP/2 we write a custom transport | **BYO** — we wire it over the existing transport directly |
| Node discovery | **its own** (optional memberlist gossip for NodeHost-ID→addr); to avoid a 2nd gossip plane we feed addresses from SWIM and disable its gossip | **BYO** — resolve addresses from SWIM directly |
| Raft log (LogDB) | bundled **Pebble** (separate engine, CGO-free) by default; **unifying onto `wavesdb` needs a custom `ILogDB`** (constrained, perf-sensitive contract) | log lives wherever we implement `raft.Storage` → **`wavesdb` natively** |
| Applied state machine | ours (on-disk state-machine iface → `wavesdb`) | ours (we drive apply → `wavesdb`) |
| CGO | **CGO-free by default** (Pebble; RocksDB removed in v4) | pure Go |
| Multi-Raft orchestration | **provided** (NodeHost, quiescing, coalesced heartbeats, snapshot streaming) | **we build it** |

The consequence of the decided constraints: dragonboat's value is its orchestration, but its batteries
(LogDB + transport + discovery) are exactly what unified-storage/one-control-plane asks us to replace.
Picking dragonboat therefore means **dragonboat-as-orchestrator + custom `ILogDB`(wavesdb) + custom
transport + SWIM address bridge** — keeping the hard multi-Raft manager while reimplementing its
pluggable layers. etcd/raft satisfies unified-storage + CGO-free + single-control-plane **natively**,
at the cost of building the orchestration (§5.7) ourselves. We accept the dragonboat adapters in
exchange for its manager — see the decision in §12.5.

### 12.4 Either way

Regardless of library, the **state machine** (the set/hash/zset semantics, reserved sub-keys, CAS,
TTL-by-log, split/merge handlers) and the **protocol contract** (§5) are ours and library-independent.
The library supplies log replication, leader election, membership change, and snapshot transport. We
isolate it behind a `raftshard` interface so the choice is swappable.

### 12.5 Decision: dragonboat, and the adapters

We choose **dragonboat** for its multi-Raft manager (NodeHost, quiescing, coalesced heartbeats,
snapshot streaming) — the faster path to a working tier than building that manager on a single-group
kernel. We accept the adapters needed to honor WaveSpan's constraints:

- **State machine** — wrap the collection state machine (§5.3) in dragonboat's on-disk state-machine
  interface, so applied state lands in `CFReplData`/`wavesdb`.
- **Transport** — implement dragonboat's `raftio` transport over the existing cheap-mTLS HTTP/2 client
  (design/27); no second transport.
- **Discovery** — resolve NodeHost addresses from SWIM gossip; dragonboat's own memberlist gossip is
  **disabled**, so there is one membership plane.
- **LogDB (phased — open sub-decision):** to ship fastest, the initial milestones run on dragonboat's
  bundled **Pebble** LogDB (pure-Go, CGO-free, *zero* storage adapter); a **`wavesdb`-backed `ILogDB`**
  that fulfils the unified-storage decision is a **tracked follow-up**. Until it lands, the Raft log is
  in Pebble and applied state is in `wavesdb` (two engines transiently). dragonboat **v4 is CGO-free by
  default** (Pebble; RocksDB was removed in v4), so there is no CGO trap. Alternative: commit to the
  `wavesdb` `ILogDB` from M-0 (unified immediately, slower start).

dragonboat sits behind the `raftshard` interface (§12.4), so the single-maintainer governance risk is
mitigated — the engine stays swappable to etcd/raft later if that concern materializes (the comparison
above is retained for exactly that purpose). API/interface names should be re-verified against the
current dragonboat release during M-0.

## 13. Client API and interfaces

The replicated tier is reached through three surfaces, all over the same request envelope:

1. **`CollectionService`** — a typed Connect/gRPC service on the data port (the primary surface).
2. **Cypher procedures** — `CALL set.*`, `CALL hash.*`, `CALL zset.*`, mirroring the `kv.*`/`vector.*`
   built-ins (design/07) so graph queries can read/write fleet-shared collections inline.
3. **Optional RESP (Redis-wire) adapter** — a gateway that maps a subset of Redis commands onto
   `CollectionService`, for drop-in clients. **Decision flagged** (§13.1): it adds a compatibility
   surface and a semantic-mismatch risk (Redis is single-node linearizable; our default reads are
   bounded-stale), so it is opt-in and not the canonical API.

### 13.1 Surfaces and the native-vs-RESP choice

`CollectionService` is the source of truth: strongly typed, versioned with the proto, and the only
surface that exposes consistency flags, cursors, preconditions, and watch. The Cypher procedures are
thin wrappers over the same handlers (read procedures are usable inline in `MATCH`/`WHERE`; mutations
are `CALL … YIELD`). The RESP adapter, if built, is a separate process/role that holds a
`CollectionService` client and translates commands; it inherits the bounded-stale read semantics and
must document the deviations from Redis (no `MULTI/EXEC` across collections, bounded-stale `GET`,
cluster-redirect instead of `MOVED`). We default to **native + Cypher**; RESP is a later adapter if a
drop-in story is required.

### 13.2 Request/response envelope

```proto
message CollectionRequest {                 // embedded in each typed op
  string namespace = 1;
  bytes  collection_id = 2;
  Consistency consistency = 3;              // BOUNDED_STALE (default) | LINEARIZABLE
  optional string idempotency_key = 4;      // dedupes retried writes
  optional uint64 read_after_index = 5;     // read-your-writes watermark (§13.10)
  optional int64 deadline_ms = 6;
}
enum Consistency { BOUNDED_STALE = 0; LINEARIZABLE = 1; }

message CollectionMeta {                     // on every reply; extends the shared ResponseMeta
  ResponseMeta base = 1;                     // served_by_member_id, observed_at_unix_ms, warnings, completeness
  ReplicaRole replica_role = 2;              // LEADER | VOTER | LEARNER
  uint64 applied_log_index = 3;              // for RYW chaining
  uint64 write_index = 4;                    // committed Raft index of a write (0 for reads)
  int64  staleness_ms = 5;                   // estimate for bounded-stale reads
  optional RangeRedirect redirect = 6;       // set on NOT_LEADER / WRONG_RANGE
}
```

`ResponseMeta` is the shared honesty block; **design/11 is authoritative** for its fields. This tier
relies on `warnings` (enumeration, §13.8) — note that design/03's abbreviated copy of `ResponseMeta`
omits `warnings` and should be synced to design/11.

### 13.3 Collections, types, and addressing

A collection is `(namespace, collection_id)` with a **fixed type** chosen at creation —
`SET | HASH | ZSET` — recorded in the `\x02header` sub-key (§3). Type-mismatched ops fail `WRONG_TYPE`.
`collection_id`, members, fields, and values are byte strings. Creation is implicit on first write (the
header is written in the same log entry) or explicit via `Create(type, opts)`.

### 13.4 Set operations

| Op | Request → Returns | Semantics |
|---|---|---|
| `SAdd` | members[] → added_count | one log entry; idempotent per member |
| `SRem` | members[] → removed_count | tombstones; log-driven |
| `SIsMember` / `SMIsMember` | member(s) → bool(s) | **point read** (cheap) |
| `SCard` | → count | sum of per-range partial counters (§3) |
| `SMembers` | → stream | enumeration (§13.8) |
| `SScan` | cursor, count → page + cursor | resumable paging (§13.8) |
| `SPop` / `SRandMember` | [n] → members | `SPop` is a write |
| `SMove` | src, dst, member | atomic **iff src+dst share a range/group**, else `CROSS_RANGE` |
| `SUnion`/`SInter`/`SDiff` | ids[] → stream | multi-collection read; `COMPLETE` only if the serving node holds all operands caught-up, else `PARTIAL` |

Note the two distinct failure modes: `CROSS_RANGE` is a **write** error (an atomic op spanned ranges,
§13.9), while `PARTIAL` is a **read-completeness** signal (not all spanned ranges were locally caught
up, §13.8) — they are unrelated.

### 13.5 Hash operations

| Op | Request → Returns | Semantics |
|---|---|---|
| `HSet` | field-value[] → set_count | one log entry; concurrent distinct fields never conflict |
| `HSetNX` | field, value → bool | conditional (CAS not-exists) |
| `HGet` / `HMGet` | field(s) → value(s) | **point read** |
| `HDel` | fields[] → deleted_count | |
| `HExists` / `HLen` | → bool / count | |
| `HIncrBy` | field, delta → new_value | exact (single-writer; no LWW loss) |
| `HGetAll` / `HKeys` / `HVals` | → stream | enumeration |
| `HScan` | cursor, count → page + cursor | resumable paging |

### 13.6 Sorted-set operations

| Op | Request → Returns | Semantics |
|---|---|---|
| `ZAdd` | (score,member)[] + NX/XX/GT/LT → added/updated | score change = delete old score-key + put new + pointer, atomic within range |
| `ZRem` / `ZScore` / `ZIncrBy` | … | point ops + atomic incr |
| `ZRank` / `ZRevRank` / `ZCard` / `ZCount` | → rank/count | rank within a range is cheap; global rank for a multi-range collection sums range offsets |
| `ZRangeByScore` / `ZRangeByLex` | range → stream | **prefix scan** over the score-ordered keyspace |
| `ZRange` (by index) | start,stop → stream | offset/limit over the ordered scan |
| `ZPopMin` / `ZPopMax` | [n] → members | write |

### 13.7 Collection-level operations

`Exists`, `Type`, `Create(type, opts)`, `Del` (drop the whole collection — deletes every element key;
for a multi-range collection this is a coordinated, idempotent delete fanned across the spanned groups,
eventually consistent for the duration), `Expire`/`Ttl` (collection-level TTL sweeps all elements;
distinct from per-element TTL of §10), `Touch`. `Rename` is **deferred** (cross-range).

### 13.8 Enumeration and pagination

Enumeration is offered in two shapes because collection size varies wildly (§2):

- **Streaming** (`SMembers`, `HGetAll`, `ZRangeByScore`): the server streams rows then a trailer with
  `completeness` and a resumable `cursor`. Good for bounded collections and one-shot reads.
- **Cursor / SCAN** (`SScan`, `HScan`, `ZScan`): a bounded page plus an opaque `cursor` token to
  resume. SCAN-style — never a consistent snapshot, but every element present throughout the scan is
  returned at least once, and it is safe under concurrent mutation and **range splits**.

The cursor encodes, per spanned range, `{range_id, last_key, read_index}` so resumption stays correct
when ranges split/merge or the client reconnects to a different replica (§6.3). `completeness` is
`COMPLETE` only when the serving node held and was caught up on **all** spanned ranges; otherwise
`PARTIAL`, with the not-locally-served ranges listed in `warnings`.

### 13.9 Atomic batches and conditional writes

```proto
message Apply {                             // atomic multi-element mutation
  CollectionRequest req = 1;
  repeated ElementOp ops = 2;               // PUT/DELETE element/field, ZADD, HINCRBY, …
  repeated Precondition preconditions = 3;  // EXISTS | NOT_EXISTS | VALUE_EQ | SCORE_CMP (CAS)
}
```

`Apply` is atomic and linearizable **iff every touched key falls in one range** (one Raft log entry):
all preconditions are checked, then all ops applied, or nothing is (`FAILED_PRECONDITION`). This is the
lossless replacement for the cache tier's read-modify-write-under-LWW pattern — e.g. "add member iff
absent **and** bump the cardinality counter" is a single atomic entry. A batch spanning multiple ranges
returns `CROSS_RANGE` in v1 (the client splits it, or pins the collection under a single range via a
size hint); cross-range transactions (Percolator/2PC) are the v2 extension (§19).

### 13.10 Consistency, sessions, and read-your-writes

- **Per request:** `BOUNDED_STALE` (default — served from the nearest in-sync replica, `staleness_ms`
  reported) or `LINEARIZABLE` (routed to the leader, served after a read-index confirmation).
- **Read-your-writes without paying linearizable on every read:** a write reply returns `write_index`
  (the committed Raft index for that collection's range). A subsequent `BOUNDED_STALE` read may set
  `read_after_index = write_index`; the serving replica blocks until `applied_index ≥ read_after_index`
  (bounded by the deadline) before answering. A **session token** bundles the per-range high-water
  marks so a sequence of ops across ranges is monotonic and read-your-writes end to end.

### 13.11 Watch / change subscription

```proto
rpc Watch(WatchRequest) returns (stream ChangeEvent);   // PUT/DELETE element, with version + index
```

`Watch(namespace, collection_id [, key_prefix] [, from_index] [, with_snapshot])` streams changes,
backed by the same subscription mechanism that keeps learners fresh. `with_snapshot` delivers the
current state then tails; `from_index` resumes from a known point. Buffers are bounded; a slow consumer
receives a `GAP` marker (as the gossip stream does, design/26) and must re-snapshot. This is the
reactive primitive for app-side cache invalidation and fan-out from the central writer.

### 13.12 Error and routing model

| Status (Connect/gRPC) | Meaning | Client action |
|---|---|---|
| `NOT_LEADER` (+ leader hint) | write hit a follower | retry to the hinted leader |
| `WRONG_RANGE` (+ range map) | stale directory | refresh directory, retry |
| `NOT_READY` | learner still catching up | fall back to another replica |
| `UNAVAILABLE` | range lost quorum | backoff + retry (idempotency key makes it safe) |
| `FAILED_PRECONDITION` | CAS/precondition failed | application decides |
| `CROSS_RANGE` | atomic op spans ranges | split, or pin collection |
| `WRONG_TYPE` | op vs collection type mismatch | fix call |

Writes carry `idempotency_key`; the leader dedupes by `(writer, key)` within a window so a retried
`NOT_LEADER`/`UNAVAILABLE` write applies **exactly once**.

### 13.13 Client library responsibilities

The shipped client caches the range directory (fed by the meta watch/gossip), routes **writes to the
range leader** and **reads to the nearest in-sync replica** (local when co-located), transparently
follows `NOT_LEADER`/`WRONG_RANGE` redirects, retries `UNAVAILABLE`/`NOT_READY` with backoff +
idempotency keys, and exposes typed `Set`/`Hash`/`SortedSet` handles. It pools connections over the
cheap-mTLS HTTP/2 transport (design/27). A thin client may instead call any node, which proxies to the
right leader/replica at the cost of one extra hop.

### 13.14 Admin and node-console UI surface

A **Collections** tab in the node console (design/26) exposes the operator/developer view:

- **List** — collections in a namespace: id, type, cardinality estimate, `#ranges`, leader + voter/
  learner placement, `pinned?`, and per-collection apply lag.
- **Inspect** — open a collection to browse members/fields/scores, paged via the cursor API with a
  `completeness` badge; view the **range map** (which ranges the collection spans, each group's leader
  and replica set), and replication health.
- **Admin actions** (admin-gated) — add/remove an element, run a one-off `Apply`, or drop a collection,
  mirroring the existing `AdminPut`/`AdminDelete` pattern (design/26) but routed to the range leader;
  results (including `NOT_LEADER`/`UNAVAILABLE`) surface in the response panel.

Served by `ObservabilityService` (admin port) for the read/inspect views and `CollectionService` for
actions, reusing the Linea UI components — the consensus-tier analogue of the Data Browser + KV Writer.

## 14. Configuration

```yaml
namespaces:
  - name: featureflags
    distribution: replicated      # vs the default "cache"
    voters: 5                     # 3 (default) | 5
    pinned: true                  # keep a learner on every node (eager everywhere)
    read: bounded-stale           # default; per-request override allowed
    maxRangeBytes: 67108864       # split threshold (64 MiB default)
```

`distribution: cache` namespaces are exactly today's behaviour. Node config gains
`voterEligible: <bool>` (sourced from the k8s annotation).

## 15. Consistency / honesty contract

Every replicated reply extends the existing `ResponseMeta` (§13.2):

- `served_by_member_id`, `replica_role` (leader | voter | learner);
- `applied_log_index` and a `staleness_ms` estimate;
- for enumeration, a `completeness` of `COMPLETE` (all spanned ranges held + caught up) or `PARTIAL`.

This keeps the design/03 principle: eventual/bounded reads are *honest*, never silently wrong.

## 16. Failure model

- **Edge/learner loss** — no quorum impact; PD re-adds learners on demand. Reads that targeted it fail
  over to another replica.
- **Single voter loss** — group runs on the remaining majority; PD up-replicates a fresh voter from a
  survivor snapshot.
- **Majority loss of a data group** — that range is **write-unavailable** (and linearizable-read
  unavailable) until recovered; **bounded-stale reads still serve** from any surviving replica.
  Mitigations: 5-voter groups for hot ranges, fast re-replication, graceful drain on scale-down. An
  operator **unsafe-recover** tool can reconstitute a range from a surviving replica, accepting loss of
  any uncommitted tail.
- **Meta-group majority loss** — control plane frozen; data groups serve from cached config (§7.5).

## 17. Worked example: a fleet-wide feature-flag set

Namespace `flags` (`distribution: replicated`, `pinned: true`, `voters: 5`). A central controller
writes; every node reads locally; an app reacts to changes.

```text
# Central write (controller → range leader → 5-voter quorum → applied → pinned learners everywhere)
SAdd(ns=flags, id="enabled", members=["new-ui","beta-search"])   → added_count=2, write_index=42

# Hot read on any node (local learner point read, ~ms stale)
SIsMember(ns=flags, id="enabled", member="new-ui", consistency=BOUNDED_STALE)   → true

# Complete local enumeration (pinned everywhere ⇒ COMPLETE)
SMembers(ns=flags, id="enabled")   → ["beta-search","new-ui"], completeness=COMPLETE

# Read-your-writes for the controller without full linearizable
SAdd(...) → write_index=43;  SMembers(..., read_after_index=43)   # waits applied≥43, then serves

# Reactive invalidation in the app
Watch(ns=flags, id="enabled", with_snapshot=true)   → snapshot, then PUT/DELETE events → app refreshes
```

Failure behaviour: a spot-edge learner dies → no effect. A core voter dies → group continues 4/5, PD
replaces it. Lose 3/5 voters → writes pause (`UNAVAILABLE`) but reads keep serving bounded-stale from
survivors until the PD recovers the range.

## 18. Rollout

Additive and staged; the cache tier ships untouched throughout.

0. **M-0 (spike)** Stand up **dragonboat** behind the `raftshard` interface (§12.5, Appendix B): one
   shard end to end (propose → commit → apply to `wavesdb` via the on-disk state machine), with the
   cheap-mTLS transport adapter and the SWIM address registry, on the interim Pebble LogDB; validate
   snapshot + restart and a **CGO-free build**.
1. **M-A** Meta group + range directory; single-range data groups (no split yet); `CollectionService`
   set ops; bounded-stale + linearizable reads.
2. **M-B** Range split/merge (§6) + placement driver (§7); voter/learner placement honoring
   `voterEligible`; graceful drain.
3. **M-C** Learner demand-fill + eviction + `pinned` mode (§9).
4. **M-D** Hash and sorted-set datatypes; TTL (log-driven, §10); Cypher procedures; Collections UI tab.

## 19. Open questions / future

- **Raft library** — dragonboat, behind `raftshard` (ADR 0008, §12.5, Appendix B). Open sub-decision:
  **LogDB phasing** — when to land the `wavesdb`-backed `ILogDB` that replaces the interim Pebble LogDB.
- **Cross-range transactions** for atomic ops spanning a huge multi-range collection (Percolator/2PC
  over the leaders of the touched ranges). v1 supports single-range atomic batches only.
- **Hot-collection write ceiling** — a single collection's writes serialize through one (or, when
  multi-range, a few) leaders; **reads scale with replicas, writes do not.** Acceptable for centrally-
  written data; documented as a ceiling.
- **Cross-cluster** — replicated collections propagate to peer clusters via the existing async global
  layer (design/06), remaining eventual across geos (no synchronous cross-geo writes, per the product
  requirement). A leader-per-region + async mirror is the likely shape.
- **Repair/anti-entropy** (design/23) is **not** used within a Raft group (the log is the source of
  truth); it remains for the cache tier and the cross-cluster layer.

## Appendix A: `CollectionService` proto (sketch)

Representative; per-op request/response messages that only carry members/fields/values follow the
envelope pattern and are elided for brevity.

```proto
service CollectionService {
  // Sets
  rpc SAdd(SAddRequest) returns (CountReply);
  rpc SRem(SRemRequest) returns (CountReply);
  rpc SIsMember(MemberRequest) returns (BoolReply);
  rpc SCard(CollectionRequest) returns (CountReply);
  rpc SMembers(CollectionRequest) returns (stream ElementRow);   // streaming enumeration
  rpc SScan(ScanRequest) returns (ScanPage);                     // cursor paging
  rpc SMove(SMoveRequest) returns (BoolReply);
  // Hashes
  rpc HSet(HSetRequest) returns (CountReply);
  rpc HGet(FieldRequest) returns (ValueReply);
  rpc HDel(HDelRequest) returns (CountReply);
  rpc HIncrBy(HIncrByRequest) returns (Int64Reply);
  rpc HGetAll(CollectionRequest) returns (stream FieldRow);
  rpc HScan(ScanRequest) returns (ScanPage);
  // Sorted sets
  rpc ZAdd(ZAddRequest) returns (CountReply);
  rpc ZRangeByScore(ZRangeRequest) returns (stream ScoredRow);
  rpc ZScore(MemberRequest) returns (ScoreReply);
  // Collection-level + generic
  rpc Create(CreateRequest) returns (Ack);
  rpc Exists(CollectionRequest) returns (BoolReply);
  rpc Del(CollectionRequest) returns (Ack);
  rpc Apply(Apply) returns (ApplyReply);                          // atomic batch + CAS (§13.9)
  rpc Watch(WatchRequest) returns (stream ChangeEvent);          // §13.11
}

message ElementOp {
  OpKind kind = 1;          // PUT | DELETE | ZADD | HINCRBY | …
  bytes  element = 2;       // member / field
  bytes  value = 3;         // field value (hash) — empty for set member
  double score = 4;         // zset
  optional int64 ttl_ms = 5;
}
message Precondition {
  PrecondKind kind = 1;     // EXISTS | NOT_EXISTS | VALUE_EQ | SCORE_CMP
  bytes element = 2;
  bytes expect_value = 3;
  double expect_score = 4;
}
message Cursor { repeated RangeCursor ranges = 1; }              // {range_id,last_key,read_index}[]
message ScanPage { repeated ElementRow rows = 1; Cursor cursor = 2; CollectionMeta meta = 3; }
message ChangeEvent { ChangeKind kind = 1; bytes element = 2; bytes value = 3; uint64 index = 4; }
message RangeRedirect { uint64 group_id = 1; string leader_member_id = 2; bytes range_start = 3; bytes range_end = 4; }

enum ReplicaRole  { LEADER = 0; VOTER = 1; LEARNER = 2; }
enum CollectionType { SET = 0; HASH = 1; ZSET = 2; }
enum OpKind { PUT = 0; DELETE = 1; ZADD = 2; HINCRBY = 3; }
enum PrecondKind { EXISTS = 0; NOT_EXISTS = 1; VALUE_EQ = 2; SCORE_CMP = 3; }
enum ChangeKind { CHANGE_PUT = 0; CHANGE_DELETE = 1; GAP = 2; }
```

## Appendix B: dragonboat integration spec

Implementation-level detail for §12.5. dragonboat **v4** terminology is used (`ShardID`/`ReplicaID`,
`IOnDiskStateMachine`, `NodeHost`); **all interface/method names must be re-verified against the
pinned release during M-0**, as they have changed across major versions (v3 used `ClusterID`/`NodeID`).

### B.1 The `raftshard` boundary

dragonboat is reached only through one internal interface, so the engine stays swappable (§12.4):

```go
type RaftShard interface {
    // lifecycle
    StartShard(shardID uint64, members map[uint64]string, join bool, sm StateMachineFactory) error
    StopShard(shardID uint64) error
    // proposals + reads
    Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) // SyncPropose
    LinearizableRead(ctx context.Context, shardID uint64, query []byte) ([]byte, error) // SyncRead
    StaleRead(shardID uint64, query []byte) ([]byte, error)                            // local read
    // membership (driven by the PD, §7)
    AddVoter(ctx, shardID, replicaID uint64, addr string, configIdx uint64) error
    AddLearner(ctx, shardID, replicaID uint64, addr string, configIdx uint64) error
    DeleteReplica(ctx, shardID, replicaID uint64, configIdx uint64) error
    TransferLeadership(shardID, targetReplicaID uint64) error
    // observation
    ShardInfo(shardID uint64) ShardInfo // leader, applied index, members, roles
}
```

The collection service, PD, and meta layer call only `RaftShard`. The dragonboat implementation wraps
a single `NodeHost`.

### B.2 NodeHost topology, IDs, and config

**One `NodeHost` per node**, hosting many shards. `ShardID` = the range's `group_id` (allocated by the
meta group, §7); `ReplicaID` = a stable per-node id within a shard (mapped to `member_id` via the
registry, B.6). Shards start with `NodeHost.StartOnDiskReplica`. Representative config (verify field
names against the pinned release):

```go
config.NodeHostConfig{
    DeploymentID:   deploymentID,            // hash of cluster_id — isolates environments on the wire
    RaftAddress:    selfRaftAddr,            // stable pod DNS:port (core voters live on StatefulSet pods)
    NodeHostID:     stableNodeID,            // stable per node; the registry target (B.6)
    RTTMillisecond: 50,                      // in-region logical tick; election/heartbeat are multiples
    Expert: config.ExpertConfig{
        TransportFactory:    cheapMTLSTransport, // B.5 — reuse the design/27 transport
        NodeRegistryFactory: swimRegistry,       // B.6 — resolve via SWIM; dragonboat gossip stays off
        LogDBFactory:        nil,                // nil ⇒ built-in Pebble (Phase 1); wavesdb ILogDB later (B.4)
    },
    // DefaultNodeRegistryEnabled stays false ⇒ no built-in memberlist gossip
}

config.Config{ // per shard
    ShardID: groupID, ReplicaID: replicaID,
    ElectionRTT: 10, HeartbeatRTT: 1,         // ~500 ms election, ~50 ms heartbeat at RTT=50
    CheckQuorum: true, PreVote: true,         // leader steps down without quorum; avoid disruptive elections
    OrderedConfigChange: true,                // serialize membership changes (fencing)
    SnapshotEntries: 10_000, CompactionOverhead: 5_000, // snapshot cadence vs log retention (§5.5)
    // quiesce enabled so idle shards stop ticking (§5.7)
}
```

dragonboat coalesces heartbeats per peer automatically and quiesces idle shards — the two properties
that make thousands of small shards affordable (§5.7).

### B.3 State machine: `IOnDiskStateMachine` over wavesdb

Each shard's state machine is our collection state machine (§5.3) behind
`statemachine.IOnDiskStateMachine`:

| method | mapping |
|---|---|
| `Open(stopc) (uint64, error)` | open the shard's slice of `CFReplData`; return the **persisted applied index** (a reserved key) so dragonboat replays only the tail |
| `Update([]Entry) ([]Entry, error)` | per entry: decode the `LogCommand`, evaluate preconditions at apply (§5.3), write element keys + reserved sub-keys (counter, TTL index) **and the new applied index** in **one atomic wavesdb batch**; set each `Entry.Result` (`Value` = added/removed count, or `Data` = encoded result / precondition-failed) |
| `Lookup(any) (any, error)` | serve point reads / scans; **runs concurrently with `Update`** (dragonboat serialises only Update/Recover/Sync, *not* Lookup) → must read a **consistent wavesdb snapshot**, not live state |
| `Sync() error` | fsync the engine (durability barrier) |
| `PrepareSnapshot() (any, error)` | capture a **point-in-time wavesdb snapshot handle** for the shard's key span, under the SM mutex |
| `SaveSnapshot(ctx, w, stopc)` | stream that captured view's key span to `w`; **also runs concurrently with `Update`**, so it must read the point-in-time view, not live state (§5.5) |
| `RecoverFromSnapshot(r, stopc)` | ingest a snapshot stream into `CFReplData` for the shard's span |
| `Close() error` | release shard resources |

**wavesdb capabilities this requires** (track as engine work):

- an **atomic multi-key batch** that also writes the applied-index key (already used by the cache tier);
- a **consistent point-in-time snapshot / iterator** so `Lookup` (concurrent with `Update`) and
  `SaveSnapshot` (streamed while writes continue) read a stable view — **the key dependency**; if
  wavesdb lacks MVCC snapshots, either the SM serialises `Lookup` against `Update` (slower) or the
  engine grows snapshot support;
- ordered **prefix/range scan** over the shard's key span (already present);
- **`Sync`/fsync** for the durability barrier.

Because `Update` persists the applied index in the same atomic batch as the data, crash recovery is
exact — `Open` returns the persisted index and dragonboat replays only the tail, **no double-apply**.

### B.4 LogDB (phased)

dragonboat **v4 is CGO-free by default**: the built-in **Pebble** LogDB is pure Go and **RocksDB was
removed in v4**, so there is no CGO path to avoid. (Tan, dragonboat's newer pure-Go log engine, is
experimental and slated to become the default later — a drop-in option, not required.)

- **Phase 1 (ship fast):** the default Pebble LogDB (`Expert.LogDBFactory = nil`) — zero adapter. Raft
  log + hard state + snapshot metadata live in Pebble; applied state lives in wavesdb (two engines
  transiently — the only departure from the unified-storage target).
- **Phase 2 (unify):** a custom `raftio.ILogDB` over wavesdb (`CFReplLog`), wired via
  `Expert.LogDBFactory` (`LogDBFactory{ Create(NodeHostConfig, LogDBCallback, []string, []string)
  (raftio.ILogDB, error); Name() string }`). It must satisfy dragonboat's `ILogDB` contract
  (save/iterate/read raft state, entry compaction, snapshot metadata, bootstrap/node info) with
  batched, fsync-correct writes, and it sits on the **hot write path** (every proposal appends before
  commit) so it must be fast. Heaviest adapter; the LogDB-phasing sub-decision (§19).

### B.5 Transport adapter

Provide `Expert.TransportFactory` (`TransportFactory{ Create(NodeHostConfig, raftio.MessageHandler,
raftio.ChunkHandler) raftio.ITransport; Validate(string) bool }`), replacing dragonboat's default TCP
transport with one over the existing cheap-mTLS HTTP/2 client + server mux (design/27). The
`MessageHandler` receives Raft messages and the **`ChunkHandler` receives snapshot chunks**, so both
Raft traffic *and* snapshot streaming ride the one transport — **no second port**. dragonboat batches
and coalesces messages per destination.

### B.6 Discovery / node registry

Provide `Expert.NodeRegistryFactory` (`Create(nhid string, streamConnections uint64, v TargetValidator)
(raftio.INodeRegistry, error)`) returning an `INodeRegistry` backed by **SWIM membership** (design/04):
it resolves a replica's `NodeHostID` → `RaftAddress`. dragonboat's built-in gossip stays **off**
(`DefaultNodeRegistryEnabled = false`), so SWIM is the single membership/liveness plane; stale
resolutions self-heal exactly as routing does (§7.3). Targets in `StartOnDiskReplica` and membership
calls are `NodeHostID`s resolved through this registry.

### B.7 Membership changes

PD decisions (§7.4) call `RaftShard` membership methods, which map to dragonboat:
`SyncRequestAddReplica` (voter), `SyncRequestAddNonVoting` (learner), `RequestDeleteReplica`,
`RequestLeaderTransfer`. Promotion = add as NonVoting → catch up → add as voter → delete the NonVoting
entry (dragonboat handles the conf-change ordering). Every change carries dragonboat's
`configChangeIndex` for fencing; the directory updates after it commits (§5.6).

### B.8 Proposals, reads, sessions, and error mapping

- **Writes:** `NodeHost.SyncPropose(ctx, session, cmd)` with the encoded `Apply` batch; the SM `Update`
  sets `Entry.Result` (`Value`/`Data`), which dragonboat returns to the caller.
- **Idempotency:** dragonboat **client sessions** give exactly-once for a *registered* session; we map
  the client `idempotency_key` to a session — or, on the no-op session path (`GetNoOPSession`), dedup
  in the SM via a `(writer, key)` table with a TTL window (§13.12). dragonboat already prevents
  *crash-replay* double-apply via the on-disk applied index (B.3), so SM dedup covers only *client*
  retries.
- **Linearizable read:** `SyncRead(ctx, shardID, query)` (dragonboat does the read-index) → `Lookup`.
- **Bounded-stale read:** `StaleRead(shardID, query)` → local `Lookup`; the SM returns its applied index
  for `staleness_ms`/RYW (§13.10).
- **Error mapping** to §13.12: not-leader → `NOT_LEADER` (+ hint from `ShardInfo`); unknown/wrong shard
  → `WRONG_RANGE`; propose timeout / no quorum → `UNAVAILABLE`; SM precondition failure →
  `FAILED_PRECONDITION`.

### B.9 Snapshots, compaction, quiescing

`OnDiskStateMachine` snapshots are cheap (engine checkpoint, B.3). Tune `SnapshotEntries` /
`CompactionOverhead` per shard so the log is trimmed without excessive snapshotting. dragonboat
**quiesces** idle shards (no proposals + stable membership) — essential at high shard counts (§5.7);
verify the quiesce thresholds in M-0.

### B.10 Split / merge mapping

A `RangeSplit` apply (§6.1) creates the child shard via `RaftShard.StartShard` on the **same nodes**
over the already-local subrange data (no movement); the child gets a fresh `ShardID` from the meta
group. `RangeMerge` (§6.2) stops the right shard (`StopShard`) after its span is absorbed by the left
shard's SM. Both are coordinated by the parent/left shard's committed log entry so all replicas act
identically.

### B.11 Concurrency, resources, and observability

dragonboat owns its execution goroutines (per-NodeHost step/apply workers); our SM `Update` must be
**non-blocking and deterministic** (no network or external locks held across the engine batch). The
`CollectionService` handlers are the only callers of `RaftShard` proposals/reads and enforce deadlines
+ idempotency (B.8). Engine resources are shared with the cache tier per §11 (separate CFs; optional
separate engine for the LogDB in Phase 2). A `raftio.ISystemEventListener` feeds leader changes,
snapshot, and membership events into the meta group / gossip and the Collections UI (§13.14).

### B.12 M-0 verification checklist

- one shard: `SyncPropose` → commit on a 3-voter quorum → `Update` applies to `wavesdb` → `StaleRead`
  and `SyncRead` return correctly;
- restart a node → `Open` resumes from the persisted applied index, **no double-apply**;
- `Lookup` reads a **consistent snapshot** while `Update` runs concurrently (confirm wavesdb snapshot
  semantics, B.3) — the key engine dependency;
- snapshot install onto a fresh learner via `SaveSnapshot`/`RecoverFromSnapshot` over the cheap-mTLS
  transport (`ChunkHandler`), no extra port; the SWIM-backed `INodeRegistry` resolves targets;
- `go build` is **CGO-free** (default Pebble LogDB); confirm dragonboat v4 API names used above.
