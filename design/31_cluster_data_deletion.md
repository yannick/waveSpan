# 31. Fast cluster-wide data deletion (drop namespace / drop all)

## Goal

Delete data fast and at any scale, cluster-wide, without issuing one delete RPC per key.
Two admin operations:

1. **Drop a namespace** — remove a namespace and **all** its keys, on **all** nodes.
2. **Drop all data** — wipe **every** namespace's data across the whole cluster.

The mechanism is a **gossip-propagated drop command** carrying a monotonic per-namespace
**drop epoch**. Every node applies the drop locally with an efficient range delete, and the
epoch durably suppresses resurrection (repair / anti-entropy / bootstrap). Cost is O(nodes ×
ranges), **independent of key count** — not O(keys) as the existing per-key path is.

This replaces the current best-effort, per-key `ObservabilityService.DeleteNamespace`
(`internal/observability/inspect_write.go:74`), which enumerates every live key cluster-wide
and issues a coordinated `Delete` per key — O(keys), slow, and racy with concurrent writes.

The KV (AP) tier and the collections (CP/Raft) tier are dropped by **different mechanisms**
because they have different consistency contracts. A single admin call coordinates both.

## Non-goals

- No cross-cluster guarantee for `global` namespaces beyond best-effort: the drop epoch is
  gossiped intra-cluster and shipped over the existing global tap; a partitioned peer cluster
  converges when reconnected (same eventual-consistency stance as doc 06).
- No point-in-time snapshot / undo. Drop is destructive and final (see safety guard, §6).
- No change to per-key `Delete` semantics or the HLC (doc 22).

---

## 1. Semantics per tier

### KV tier (AP, eventually consistent)

`drop-namespace` and `drop-all` are **eventually consistent**, matching the cluster default
(doc 00, `adr/0001_eventual_consistency.md`). The guarantee is **convergent and monotonic**:

- Every node that observes drop epoch `E` for namespace `ns` removes all `ns` data whose
  write **precedes** `E` and refuses to (re)materialize it thereafter.
- A node offline at drop time applies `E` on rejoin (the epoch rides gossip and is durable),
  so it converges without a separate catch-up RPC.
- Writes to `ns` with an HLC strictly **after** `E` survive — they belong to a *recreated*
  namespace at epoch `E` (see §5, "recreated after drop").

There is **no linearization point** for KV: at any instant during convergence some nodes may
still hold pre-`E` data. Reads already declare completeness/consistency mode (doc 00 rule 5);
a drop in flight is just normal eventual convergence.

### Collections tier (CP, linearizable)

A collections namespace is `distribution: replicated` (doc 30) and lives behind dragonboat
Raft. Dropping it is a **Raft proposal**, not a gossip apply, so it is **linearizable**: the
drop commits at a single point in the meta shard's log and every replica (voters **and**
learners) applies it deterministically in log order (doc 30 §4; `internal/collections/
statemachine.go:172`). No concurrent collections write can be lost or re-ordered around it.

### Why the split

| | KV tier | Collections tier |
|---|---|---|
| Storage | `recordstore.Store` over wavesdb CFs | dragonboat shards over wavesdb CFs |
| Consistency | eventual (gossip merge, LWW by HLC) | linearizable (Raft log order) |
| Drop carrier | gossiped **drop epoch** | Raft **proposal** on meta + data shards |
| Resurrection risk | repair / intra-AE / bootstrap | none (log is the only writer) |
| Apply site | every node, locally, async | every replica, in commit order |

A namespace is **either** KV-cache **or** replicated-collections, never both — the tier is
selected per namespace (`distribution`, doc 30 §1). The admin layer routes the drop to the
owning tier (§4).

---

## 2. KV mechanism: the gossiped drop epoch

### 2.1 Drop epoch and the drop table

A **drop epoch** is an HLC `version.Version` (doc 22): the coordinator stamps the drop with
`Store.NextVersion()` (`internal/recordstore/store.go:93`). Using the HLC — not a fresh
counter — means the epoch is **directly comparable to every record version** by
`version.Version.Compare` (`internal/version/version.go:26`). That comparability is the whole
trick: "did this record's write happen before the drop?" is one `Compare`.

Each node keeps a durable **drop table**: `namespace -> DropMark`.

```go
// internal/drop/table.go  (new package)
type DropMark struct {
    Namespace string
    Epoch     version.Version // HLC of the drop; records with Version < Epoch are dropped
    Origin    string          // member id that issued it (provenance / LWW tie-break)
    UnixMs    int64           // wall time, for ops/UI
}
```

Merge rule (idempotent, monotonic, convergent — mirrors `tunables.Overrides.ApplyRemote`,
`internal/tunables/overrides.go:108`):

```text
on receive DropMark d for ns:
    cur = table[ns]
    if !exists(cur) or d.Epoch.Compare(cur.Epoch) > 0:
        table[ns] = d
        schedule local range-delete of ns for everything < d.Epoch   // §2.4
    // equal or older epoch: ignore (already applied or stale)
```

A namespace **recreated** after a drop and dropped **again** gets a strictly higher HLC epoch
(HLC is monotonic, doc 22), so the second drop dominates the first — drops are totally ordered
per namespace.

### 2.2 Riding gossip

The gossip payload already carries `Members`, `Summaries` (`HolderSummaryWire`),
`ConfigDeltas` (`ConfigDeltaWire`), and `HeldBuckets` (`internal/membership/gossip.go:62`).
Add a fifth slice, modeled exactly on `ConfigDeltaWire` (the closest existing analog — a
versioned cluster fact that rides gossip with LWW merge):

```go
// internal/membership/gossip.go
type DropMarkWire struct {
    Namespace       string
    EpochPhysicalMs uint64
    EpochLogical    uint32
    EpochCluster    string
    EpochMember     string
    EpochSequence   uint64
    Origin          string
}

type GossipMessage struct {
    From         Member
    Members      []MemberView
    Summaries    []HolderSummaryWire
    ConfigDeltas []ConfigDeltaWire
    HeldBuckets  []HeldBucketWire
    DropMarks    []DropMarkWire   // NEW
}
```

Wire it with the established provider/consumer hook pattern:

- add `SetDropHooks(provide func() []DropMarkWire, consume func([]DropMarkWire))` next to
  `SetConfigHooks` (`internal/membership/gossip.go:117`);
- emit in `outgoing()` (`gossip.go:267`, beside `msg.ConfigDeltas = g.provideConfig()`);
- consume in **both** `HandleGossip` (`gossip.go:191`) and the reply `merge` (`gossip.go:257`),
  via a `consumeDropMarks` helper that calls `g.consumeDrop(in.DropMarks)`.

Serialization is protobuf3 over Connect (`proto/wavespan/v1/admin.proto`,
`GossipExchangeRequest`/`Response`): add `repeated DropMark drop_marks = 6;` and the
`*ToProto`/`*FromProto` converters in `internal/membership/connect.go` (next to
`configDeltasToProto`, line 104).

The provider returns the node's full drop table (like `Overrides.GossipSet`); the consumer
calls `dropTable.ApplyRemote(...)`. Because drop marks are tiny (one per dropped namespace)
and merge is idempotent, they piggyback on every gossip round at negligible cost and reach
every node — including ones that were down at drop time — within gossip-convergence time.

Wire-up in `cmd/wavespan-node/main.go` mirrors the tunables block (`main.go:203`): construct
the drop table, set the hooks, and on each applied mark invoke the KV range-delete (§2.4).

### 2.3 Persistence

The drop table must survive restart, or a rebooted node would forget the epoch and let
repair/AE/bootstrap resurrect dropped data. Persist it the same way overrides are persisted
(`internal/tunables/overrides.go:169`, snapshot file via a `main.go` hook), and additionally
mirror each `DropMark` into the `sys` column family so it is loaded **before** the replication
subsystems start (the same CF that backs `writer_sequence`, doc 22). On startup: load the drop
table, then start fanout/repair/AE/bootstrap — never the reverse.

### 2.4 Local efficient delete

wavesdb exposes no native range-delete; `CompactRange` bounds are advisory
(`internal/storage/wavesdb_store.go:302`). So "efficient" means **one batched iterator sweep
per namespace prefix**, not a coordinated per-key delete with replication. Keys are namespace
-prefixed: `latestKey = lenPrefix(ns)||userKey` in `CFKVMeta`; `dataKey =
lenPrefix(ns)||lenPrefix(userKey)||versionEnc` in `CFKVData` (`internal/recordstore/encode.go:
22`). The whole namespace is the half-open range `[namespacePrefix(ns),
prefixEnd(namespacePrefix(ns)))`.

Add `Store.DropNamespaceLocal(ns string, epoch version.Version)`:

```text
scan CFKVMeta over [namespacePrefix(ns), prefixEnd(...)):
    decode each latest pointer's winner version
    if winner.Version < epoch:                      // skip survivors written after the drop
        batch-delete the CFKVMeta latest key
        scan+batch-delete its CFKVData versions over dataKeyPrefix(ns,key)
        adjust liveKeys (-1 if the deleted winner was live)   // internal/recordstore/store.go:40
    flush the batch every N ops (storage.Batch / BatchRC)
```

This is exactly the existing local-only `Forget` pattern (`internal/recordstore/store.go:540`)
generalized to a prefix sweep and gated by the epoch. It writes **no tombstones and no
mutation-log entries** — the drop is propagated by the gossiped epoch, not by per-key
tombstones, so it never enters fanout/global replication as N delete mutations. `drop-all`
is the same over every namespace prefix (enumerate via the holder/namespace list the cache
directory already tracks, `internal/cache/directory.go`).

The sweep is bounded/resumable: keep a per-namespace cursor (like
`antientropy_intra` and repair backfill do) so a large namespace drains in rate-limited
batches and resumes after restart. The drop is "logically done" the instant the epoch is in
the durable drop table; the physical sweep is background reclaim, and the epoch gate (§3)
makes pre-`E` data invisible and unresurrectable even before the sweep finishes.

### 2.5 The epoch gate (the read/serve side)

Until the background sweep completes, pre-`E` records may physically remain. A central
predicate makes them invisible and inert:

```go
// dropped reports whether a record must be treated as deleted by the drop at epoch E.
func (t *Table) Dropped(ns string, v version.Version) bool {
    if m, ok := t.get(ns); ok {
        return v.Compare(m.Epoch) < 0   // strictly-before the drop epoch
    }
    return false
}
```

`recordstore.Store` consults `Dropped` on the read path (`Get`/`ScanRange`) so a dropped key
reads as absent immediately, and on `Apply` so an inbound pre-`E` record for `ns` is a no-op
(this is the single choke point that also defeats resurrection, §3).

---

## 3. Suppressing resurrection (critical)

A drop that only deletes local bytes is undone within seconds by three paths that re-replicate
from peers. Each must consult the drop gate. The single most important hook is
**`recordstore.Store.Apply`** (`internal/recordstore/store.go:131`) — the one writer all three
paths funnel through — but we also short-circuit earlier to avoid wasted work.

### 3.1 Repair (`internal/replication/local/repair.go`)

Repair re-pushes under-replicated keys to holders (`ProcessOne` → `StoreReplica`, line 202;
`BackfillOnce` scans namespaces and enqueues, line 314; `OnMemberDead` re-enqueues a dead
member's keys, line 144). For `everywhere` namespaces it targets every alive member
(`repairCandidates`, line 228). Gate:

- `BackfillOnce`: skip enqueue when `Dropped(ns, rec.Version)` — don't even queue pre-`E` keys.
- `ProcessOne`: before `StoreReplica`, re-check `Dropped(ns, rec.Version)`; if dropped, treat
  the item as done (drop from queue) instead of pushing.
- `OnMemberDead`: same skip when re-enqueuing.

### 3.2 Intra-region anti-entropy (`antientropy_intra.go`)

`ReconcileOnce` (line 53) scans local keys, fetches each from every alive peer, adopts the
**highest version** (LWW), and calls `store.Apply(best, kind)` (line 82). A lagging peer that
missed the drop still serves pre-`E` records, so without a gate AE pulls them back. Gate:
**`Apply` rejects any record with `Dropped(ns, rec.Version)`** (§2.5) — the receive-side gate
defeats AE regardless of what a peer offers. Optionally also skip the per-key fetch when the
namespace is dropped, to save round-trips.

### 3.3 Bootstrap (`bootstrap.go`)

`BootstrapOnce` (line 40) back-fills a (re)joining node: for each `everywhere` namespace it
streams **all** records from a peer via the `Backfill` RPC and `Apply`s every one (line 54).
A new node back-filling a dropped namespace is the worst resurrection. Two gates:

- **Serve side** (`ReplicaServer.Backfill`, `internal/replication/local/connect.go:117`):
  the *serving* peer skips records with `Dropped(ns, v)` so it never ships pre-`E` data.
- **Apply side** (`BootstrapOnce`): the joining node's `Apply` gate rejects pre-`E` records
  anyway. Because the joining node loaded its drop table from `sys`/snapshot **before**
  bootstrap (§2.3), it already knows the epoch even on first boot after the drop gossiped to
  it.

Both sides gated means resurrection is impossible whether the stale data comes from the sender
or is accepted by the receiver.

### 3.4 Fanout and the recreate race

Fanout (`fanout.go`) is the normal write path, not a resurrection path, but it is where a
**recreated** namespace's post-`E` writes flow. Those have `Version > Epoch`, so `Dropped`
returns false and they replicate normally — exactly right (§5).

---

## 4. Collections path (Raft)

A collections namespace drop is a **Raft proposal**, applied deterministically on voters and
spot learners alike. There is no gossip epoch and no resurrection problem, because the Raft
log is the only writer (doc 30 §4).

### 4.1 New command

Add an opcode to the state-machine command set (`internal/collections/command.go:36`, after
`opRemove = 14`):

```go
opDropNamespace opKind = 15  // remove every collection (and its elements) in a namespace
```

with `encodeDropNamespace(ns)` / `decodeDropNamespace` beside the existing codecs.

### 4.2 Proposal and apply

A collections namespace spans many data shards (collections route by
`routeKey(ns, coll)` through the meta shard's range directory,
`internal/collections/meta.go:144`). The drop is a **two-level** operation coordinated by a new
`Collections.DropNamespace(ctx, ns)`:

1. **Meta shard proposal** (`MetaShardID = 1`): propose `opDropNamespace(ns)` via
   `Manager.Propose(ctx, MetaShardID, cmd)` (`internal/collections/manager.go:188` →
   dragonboat `SyncPropose`). `metaSM.Update` (`internal/collections/meta.go:73`) deletes every
   range-directory entry whose `routeKey` falls in `[ns-prefix, ns-prefix-end)` and records a
   durable **namespace-drop marker** in the meta state (so a data shard that later asks the
   directory gets a definitive "gone").
2. **Per data-shard purge**: for each data shard that held a range of `ns`, propose
   `opDropNamespace(ns)` (or reuse the existing `opPurge` sub-range mechanism,
   `command.go:44`) so `shardSM.Update` (`internal/collections/statemachine.go:172`)
   range-deletes all keys under the shard's `ns` prefix in one atomic batch (type headers,
   elements, cardinality counters, TTL index entries). Each shard commit is atomic on that
   shard; the set of shard commits is **sequenced by the coordinator** after the meta commit.

Cross-shard atomicity is **not** available in v1 (doc 30 §19 defers cross-range transactions),
so the drop is *linearizable per shard* and *coordinated* across shards: meta commits first
(new routing refuses the namespace), then data shards purge. A coordinator crash mid-drop is
recovered by making `DropNamespace` **idempotent and resumable** — re-running re-proposes the
same `opDropNamespace`, which is a no-op on shards already purged (apply checks the meta
marker / empty prefix). The meta marker is the source of truth that the namespace is dropped.

### 4.3 Voters vs learners; ordering

Voters and learners apply the **same committed entries in the same order** (doc 30 §4), so a
spot learner on an edge node drops the namespace exactly when it applies the committed
`opDropNamespace` — no special path. A learner that was offline replays the committed log on
rejoin and applies the drop then; it can never resurrect because it has no independent writer
(unlike KV's AE/bootstrap).

**Ordering vs the gossip epoch:** the two tiers are independent — a namespace is one tier or
the other — so there is no cross-tier ordering to reconcile for a single namespace. For
`drop-all`, the admin layer fans out to both tiers (§4 + §2) and reports per-tier results; the
KV epoch and the Raft commit are each authoritative for their own namespaces.

---

## 5. Edge cases

- **Node offline during drop.** The epoch is durable (drop table in `sys`/snapshot) and rides
  gossip; the node applies it on rejoin **before** starting replication (§2.3), so it sweeps
  locally and the Apply gate blocks any stale inbound. Collections learners replay the
  committed `opDropNamespace` on rejoin.
- **Namespace recreated after drop.** New writes carry `Version > Epoch` (HLC monotonic), so
  `Dropped` returns false and they live. A *second* drop stamps a strictly higher epoch that
  dominates the first; the namespace's drop history is a totally ordered chain of epochs.
- **Drop racing concurrent writes.** A write with HLC `< Epoch` loses (dropped); a write with
  HLC `> Epoch` survives. The HLC `Compare` (doc 22) is the single deterministic arbiter, so
  every node makes the same survive/drop decision regardless of arrival order — convergent.
  The narrow ambiguity window of normal eventual consistency applies (a write in flight near
  the drop instant may or may not precede `E`); this matches the existing write contract.
- **Partial failure.** KV: the gossiped epoch is the commit; the physical sweep is idempotent
  and resumable, and the Apply gate covers the gap, so a node that crashes mid-sweep finishes
  later with no resurrection. Collections: meta commits first; data-shard purges are
  idempotent and re-proposed until all succeed; the meta marker prevents a half-dropped
  namespace from being routed to.
- **`everywhere` / `ref` namespaces.** These replicate to every node (`ReplicateEverywhere`,
  `internal/config/global.go:55`) and are back-filled on bootstrap, so they are the highest
  resurrection risk. The drop epoch reaches every node via gossip, and the bootstrap serve-side
  **and** apply-side gates (§3.3) both refuse pre-`E` records — so even an `everywhere`
  namespace drop is safe. `global` namespaces additionally ship the epoch over the global tap;
  peer clusters converge on reconnect (best-effort, per non-goals).
- **Drop then immediate read.** Local reads consult the gate and return absent immediately
  (§2.5), even before the sweep runs. Cluster reads are eventually consistent as always.

---

## 6. API surface

### KV tier — `ObservabilityService` (admin port 7900)

Destructive admin ops already live in `ObservabilityService`
(`proto/wavespan/v1/observability.proto`, impl `internal/observability/inspect_write.go`),
behind admin auth (`adminIdentity.EnforceHTTP`, `cmd/wavespan-node/main.go:703`). Replace the
O(keys) `DeleteNamespace` body with the epoch path, and add `DropAllData`:

```protobuf
service ObservabilityService {
  // ... existing ...
  rpc DropNamespace(DropNamespaceRequest) returns (DropNamespaceResponse);
  rpc DropAllData(DropAllDataRequest)     returns (DropAllDataResponse);
}

message DropNamespaceRequest {
  string namespace          = 1;
  string confirm_namespace  = 2;  // must equal `namespace` (typed-confirmation guard)
}
message DropNamespaceResponse { bool ok = 1; string epoch = 2; string error = 3; }

message DropAllDataRequest  { string confirm_token = 1; }  // must equal a server-configured phrase
message DropAllDataResponse { bool ok = 1; uint32 namespaces_dropped = 2; string error = 3; }
```

Handler: stamp `epoch = Store.NextVersion()`, install the `DropMark` in the local drop table
(which gossips it and triggers the local sweep), and return. **No cluster fan-out, no per-key
loop** — gossip does the spreading. Idempotent: re-issuing produces a higher epoch that
dominates; safe but advances the chain (UI should warn).

### Collections tier — `CollectionService` (data port 7901)

Collections admin ops belong on `CollectionService` (`proto/wavespan/v1/collections.proto`,
impl `internal/collections/service.go`), beside `BulkRemove`, because the operation must go
through the Raft leader (linearizable), not the admin observability surface:

```protobuf
rpc DropNamespace(DropCollNamespaceRequest) returns (DropCollNamespaceResult);
```

Handler calls `Collections.DropNamespace(ctx, ns)` (§4.2). Idempotent via the meta marker.

### Single coordinating admin call

A top-level `DropNamespace(namespace)` admin entrypoint resolves the namespace's tier
(`distribution`, doc 30 §1 / `config`) and routes to **exactly one** path. `DropAllData`
fans out to **both** — drop every KV namespace by epoch and propose `opDropNamespace` for
every collections namespace — and returns a per-tier summary. This lives in the admin
surface / `wavespanctl`.

### Safety guard

Both ops are irreversible, so:

1. **Typed confirmation** — `DropNamespace` requires `confirm_namespace == namespace`;
   `DropAllData` requires `confirm_token` to equal a server-configured phrase (default
   disabled: `DropAllData` returns `error` unless an operator has set the phrase, like the
   existing `kvDeleter == nil` feature gate, `inspect_write.go:84`).
2. **Admin role** — both behind `admin` (not `reader`) auth on the admin port (doc 15, doc 26).
3. **Audit** — log `{op, namespace, epoch, origin, member, ts}` and emit a metric (§7); the
   epoch is a permanent record of the drop in the drop table.

---

## 7. Observability

```text
drop_marks_active                  // namespaces currently carrying a drop epoch
drop_sweep_keys_deleted_total      // physical keys reclaimed by the background sweep
drop_sweep_in_progress             // 1 while a namespace sweep is draining
drop_apply_suppressed_total        // inbound pre-epoch records rejected (resurrection blocked),
                                   //   labeled by path: repair|antientropy|bootstrap
drop_epoch_gossip_lag_seconds      // time from issue to a node observing the mark
collections_drop_namespace_total   // committed opDropNamespace proposals (meta + data shards)
```

`drop_apply_suppressed_total` is the key correctness signal: a non-zero, decaying value after
a drop is exactly the resurrection attempts being blocked. A value that **stays** high means a
peer never learned the epoch — alert on it.

---

## 8. Implementation plan

### Phase 1 — KV drop epoch (core)

1. New package `internal/drop`: `Table` (`ApplyRemote`, `GossipSet`, `Dropped`, persistence).
2. `recordstore.Store`: `DropNamespaceLocal(ns, epoch)` (prefix sweep, generalizes `Forget`,
   `store.go:540`); the `Dropped` gate in `Get`/`ScanRange`/`Apply` (`store.go:131`).
3. Persist the drop table to `sys` CF + snapshot; load it **before** replication starts in
   `cmd/wavespan-node/main.go`.

### Phase 2 — Gossip carrier

4. `DropMarkWire` + `GossipMessage.DropMarks` (`internal/membership/gossip.go:62`); `SetDropHooks`,
   emit in `outgoing()`, consume in `HandleGossip`+`merge`.
5. Proto `repeated DropMark drop_marks = 6;` in `admin.proto` + converters in
   `internal/membership/connect.go:104`.
6. Wire hooks in `main.go` (mirror the tunables block, `main.go:203`).

### Phase 3 — Resurrection gates

7. Gate repair (`repair.go` ProcessOne/BackfillOnce/OnMemberDead), intra-AE (`Apply` gate
   suffices; optional fetch skip), bootstrap (serve-side `Backfill` skip + apply-side gate).

### Phase 4 — Collections drop

8. `opDropNamespace` opcode + codecs (`command.go`); apply in `metaSM.Update` (range-directory
   purge + marker) and `shardSM.Update` (per-shard prefix delete).
9. `Collections.DropNamespace` coordinator (meta-first, then data shards, idempotent/resumable);
   `CollectionService.DropNamespace` RPC.

### Phase 5 — API, safety, ops

10. `ObservabilityService.DropNamespace`/`DropAllData` (replace the O(keys) impl); typed-confirm
    + admin-role + audit; top-level tier-routing admin call + `wavespanctl`; metrics (§7);
    node-UI affordance (doc 26) with the confirmation guard.

### Critical files

```text
internal/drop/table.go                                   (new)
internal/recordstore/store.go                            DropNamespaceLocal, Dropped gate in Apply/Get/Scan
internal/membership/gossip.go, connect.go                DropMarkWire, hooks, proto converters
proto/wavespan/v1/admin.proto, observability.proto, collections.proto
internal/replication/local/{repair,antientropy_intra,bootstrap,connect}.go   gates
internal/collections/{command,meta,statemachine,manager,service}.go          opDropNamespace path
internal/observability/inspect_write.go                  DropNamespace/DropAllData handlers
cmd/wavespan-node/main.go                                wiring + startup ordering
```

## 9. Test plan

- **Unit — drop table.** `ApplyRemote` is idempotent/monotonic; higher epoch dominates; equal/
  lower ignored. `Dropped` strictly-before semantics at the epoch boundary.
- **Unit — local sweep.** `DropNamespaceLocal` removes all `< epoch` keys in both CFs, keeps
  `> epoch` survivors, adjusts `liveKeys` exactly, is resumable from a cursor.
- **Resurrection harness (the crux).** A 3-node cluster (extend `everywhere_test.go` /
  `harness_test.go`): write N keys to `ref`, drop it, then force **repair**, **intra-AE**, and
  **a fresh node bootstrap** from a lagging peer that still holds the keys — assert the keys
  stay gone on all nodes and `drop_apply_suppressed_total` increments per path.
- **Offline-node convergence.** Partition a node, drop a namespace, heal — assert the node
  applies the epoch on rejoin before serving and resurrects nothing.
- **Recreate.** Drop, then write post-epoch keys — assert they survive; drop again — assert the
  second (higher) epoch wins.
- **Collections.** Propose `opDropNamespace`; assert meta range-directory entries gone, every
  data shard's `ns` prefix empty on voters **and** learners, a rejoining learner replays the
  drop, and the coordinator is idempotent across a simulated crash mid-drop.
- **API/safety.** Wrong `confirm_namespace`/`confirm_token` rejected; reader role rejected;
  `DropAllData` disabled until phrase configured; audit log + metric emitted.
- **Scale.** Assert drop latency to "logically done" (epoch durable + gossiped) is independent
  of key count — O(1) vs the old per-key path's O(keys).
