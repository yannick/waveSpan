# Snapshot Backups — Design Spec

Status: design approved, ready for implementation planning.
Date: 2026-06-27 (re-reviewed 2026-06-29 against `main@f903964`).
Worktree/branch: `waveSpan-backup` / `backup`.

Re-review note (2026-06-29): re-validated against the 45 commits that landed since, dominated
by the new **LeasedBudget** datatype (collections tier, Stage 1+2) and a wavesdb B+tree klog
(`UseBTree`). LeasedBudget confirmed the design's datatype-immunity — covered by the existing
Collections contributor with zero backup-core changes (§3.B) — and surfaced one new documented
concept: time-relative state across restore (§7.1). `UseBTree` is transparent to the physical
plane (§6).

## Context

WaveSpan has no production backup/restore. The pieces exist but are disconnected: the
embedded engine `wavesdb` already ships `Checkpoint`/`Backup`/`CloneColumnFamily`,
read-only multi-open, `PromoteToPrimary`, a native `ObjectStore` mode (`objstore_mode.go`),
and `GlobalSeq` as a consistent-point marker; waveSpan has `internal/backup` (a record-level
`Backup`/`Restore` codec over the 7 authoritative column families) and a never-implemented
`WaveSpanBackup` operator CRD stub ("wire to wavesdb object-store in M12"); collections
already do consistent per-shard dragonboat `SaveSnapshot`/`RecoverFromSnapshot`. This spec
ties these into one system that **streams consistent snapshots from waveSpan nodes directly
to S3 and reconstitutes/clones clusters from there**.

### Goals (all four, confirmed)
1. **Disaster recovery** — restore the same cluster after data loss.
2. **Clone / fork** — spin up a copy elsewhere, possibly a **different node/shard count**.
3. **Partial / tenant extract** — back up or move individual namespaces or collections.
4. **Archival / PITR** — long-term retention and point-in-time recovery.

### Confirmed requirements
- **Consistency:** a backup is a **global point-in-time cut** (cluster-wide HLC frontier).
- **Restore shape:** must support **re-sharding on restore** (restore into an arbitrary-sized
  cluster), which forces a logical/portable record format.
- **Incremental:** full **and** incremental from v1.
- **Dedup model:** **owner-assigned with all-export fallback** (one live holder exports each
  range; fall back to any holder if a range has no live owner).
- **v1 scope:** **both planes** — logical (portable) and physical (same-shape DR fast path).
- **Standing invariant (carried project-wide):** no input or operation may crash a node;
  overload/failure degrades gracefully. Backups must never report silent success on missing data.

### Non-goals (v1)
- Cross-**major**-format-version compatibility (additive-only within a major; a major bump
  ships a migrator). Within a major version, old backups are always importable.
- Re-shard of a datatype that violates the key-routing invariant (§3.C) — such a datatype
  still gets same-shape physical restore and blind logical restore, just not generic re-shard.
- Continuous/streaming CDC backup (this is snapshot + incremental, not per-write shipping).

## Architecture overview

Two planes, one shared manifest:

- **Logical plane (portable backbone).** Record-level, HLC-versioned export of opaque
  `(cf, key, value, hlc_version)` triplets. Re-shardable, partial-capable, incremental by
  seq/arrival (like the physical plane). The only plane that can clone into a different shape,
  extract a single tenant, or clone to a new cluster identity.
- **Physical plane (same-shape DR fast path).** Per-node wavesdb SSTables streamed to S3 via
  the engine's `ObjectStore` mode. Incremental is near-free because SSTables are immutable
  (upload only new file-ids). Topology-bound: restore maps source node → target node by
  ordinal and calls `PromoteToPrimary()`.
- Both planes write into one `cluster.manifest`. A backup may carry logical + physical
  artifacts; restore picks the cheaper valid path based on **target shape *and* intent** — the
  physical fast path is used only for **DR of the same cluster identity** at matching shape;
  any **clone** (new cluster identity), shape change, or partial restore uses the logical path
  (§7). Physical never imports a source node's identity into a different cluster.

## 1. Consistency — global cut via HLC frontier

A backup is defined by a cluster-wide **HLC frontier `T`** that seals two kinds of tier
coherently against a single pinned engine snapshot:

1. The coordinator picks `T = now() + small lease`.
2. Each participating node, on receiving `T`:
   a. **advances its local HLC past `T`** (`clock.Update`) so no *future* local write can be
      `≤ T`;
   b. **drains in-flight writes** with `Version ≤ T`;
   c. **pins a wavesdb snapshot** at its current `GlobalSeq` (new `AcquireSnapshot`, §4).
3. From that one snapshot:
   - **AP tiers (KV, graph, vector)** — emit records with `Version ≤ T` (the HLC *ceiling*).
   - **CP tiers (collections, and any future raft tier)** — emit the shard state at the
     `GlobalSeq`/raft-index reached at seal time (all raft entries applied by seal are
     included). These tiers do **not** HLC-filter; their consistency is the pinned seq.

Because both kinds are read from the same pinned `GlobalSeq`, they form one coherent cut.

**Precise cut semantics (AP).** The data model has no cross-key/cross-shard transactions (KV
is eventual + HLC-LWW; collections are per-shard raft), so an HLC cut is the correct and
maximal notion. With **owner-assigned dedup** (§2), each AP range's `≤ T` view is the
**assigned owner's converged state at seal**, not a true union across all replicas. A write
acknowledged before `T` (origin+1) but not yet propagated to the assigned owner at seal time is
not in this logical backup; since logical backups are **full each time** (§6, Phase 3
refinement), it is simply captured by the **next full logical backup** once it has converged to
an owner. An optional best-effort anti-entropy pull on owners just before seal tightens the
window. (The physical plane is per-node and self-heals via its seq-based incrementals.)

**Version extraction without parsing values (invariant A, §3).** The cut and incrementals need
each record's HLC version, but the backup *core* never parses `value` bytes. The version is
supplied by the record's **contributor** via `VersionOf` (§3.B): for KV it is read directly
from the byte-comparable version **key suffix** (`internal/recordstore/encode.go`); for graph/vector it
is unmarshalled from the value proto's version field by *that datatype's* registered extractor
(`NodeRecord`/`EdgeRecord` in `cypher.proto`, `VectorRecord.version` in `vector.proto`).
Datatype-specific knowledge thus lives in the registered plug, not the core.

## 2. Coordination protocol (phased, durable, resumable)

The backup catalog/intent lives in the **collections meta shard** (already raft-replicated) —
a single durable serialization point that survives coordinator crash and makes backups
resumable.

1. **Begin.** Admin RPC to any node → allocate `backupID`, frontier `T`, `parent` (for
   incrementals), selection (full | namespaces | collections), planes. Persist `BackupIntent`
   to the meta shard.
2. **Assign.** Coordinator computes ownership from `holder directory + placement + live roster`:
   each KV/graph/vector key-range → one live owner; each collection shard → its raft leader;
   ranges with **no live owner → all-export fallback** (any live holder). Assignments pushed
   to nodes via the fanout RPC.
3. **Prepare.** Each node seals `T` (advance HLC, drain, pin snapshot) and ACKs its `GlobalSeq`
   plus its held-range summary.
4. **Export.** Each node streams its assigned data to S3 (logical chunks and/or physical
   SSTables), writes a per-node sub-manifest, and reports progress via gossip piggyback +
   final ACK.
5. **Commit.** Coordinator **cross-checks assignment coverage against the held-range
   summaries** (gap detection), writes `cluster.manifest`, marks the intent `COMPLETE`. An
   uncovered range with no live holder → status **`PARTIAL`** with the gap enumerated in the
   manifest. Never a silent success.

Stragglers/down nodes: their ranges reassign to another live holder (replication guarantees a
copy exists); only a fully-unavailable range degrades a backup to `PARTIAL`. A crashed
coordinator's backup is re-driven from the meta-shard intent; export is idempotent because
objects are content-addressed by `(backupID, seq, key-range)`.

## 3. Extensibility & format stability (binding invariants)

The backup core and on-disk format must be **immune to new datatypes and replication types**,
and **old backups must always be importable** within a major version. Four invariants:

**A. Opaque payloads in the core.** The backup *core* (codec, coordinator, object I/O) never
parses `value` bytes. The logical stream is generic `(cf, key, value, hlc_version)`. The one
piece of per-record knowledge the core needs — the HLC version, for the cut and incrementals —
is provided by the record's **contributor** (`VersionOf`, below), not by parsing the value in
the core. Datatype payloads (protobuf) evolve under their own forward-compat rules; adding a
field never touches backup-core code.

**B. Registry, not hardcoding.** Each subsystem registers a `BackupContributor`; the
coordinator iterates the registry and never names a datatype:

```go
type Consistency int // HLCCeiling (AP, filter Version<=T) | SeqSnapshot (CP, pin at GlobalSeq)

type BackupContributor interface {
    CFs() []CFSpec                          // owned CFs/key-prefixes; authoritative vs derived
    Consistency() Consistency               // how this tier is sealed against the cut
    OwnerOf(ns string, kr KeyRange) NodeID  // dedup/ownership resolver
    // VersionOf extracts a record's HLC version for the ceiling filter + seq incrementals.
    // ok=false for non-versioned CFs (e.g. system/config); no HLC filter applied then.
    VersionOf(cf CFID, key, value []byte) (ts hlc.Timestamp, ok bool)
    // RebuildAfterRestore runs post-restore for derived-index rebuild AND time-relative
    // state reconciliation (§7.1). RestoreInfo carries capture wall-clock, restore wall-clock,
    // frontier T, intent (DR|clone), and shape-changed, so a datatype can reason about the
    // elapsed gap without the backup core understanding the datatype.
    RebuildAfterRestore(ctx, ri RestoreInfo) error  // optional
}
```
A new datatype or replication type = one new registration, zero changes to the backup core or
format. A novel CP sub-tier supplies its own `OwnerOf` + `Consistency()=SeqSnapshot`; it plugs
in, it does not fork the engine. **Validated by LeasedBudget** (landed after this spec): a new
CP datatype stored in `CFReplData` under the standard shard prefix, routed by the canonical
`ShardForKey(ns,coll)` FNV hash, Epoch-versioned (no HLC), with an authoritative shard-level
expiry index that travels inside the dragonboat shard snapshot — it is covered by the existing
Collections contributor with **zero backup-core changes**. Its only backup-relevant wrinkle is
time-relative state (§7.1), handled by its `RebuildAfterRestore` hook.

**C. Generic key-encoded routing (the one thing re-shard needs).** Re-shard routes a record
with a generic `route(namespace, key, targetTopology)` over the **standard length-prefixed
key + hash convention** (the FNV routing collections already use; placement-hash for KV).
It never inspects `value`. **Invariant for every datatype:** encode the routing key into the
key prefix and use the standard hash. In return, the datatype gets backup/restore/re-shard
for free.

**D. Versioned, additive, blind-restorable format.** The manifest and chunk framing carry a
`format_version`, extending the existing codec's magic + version byte in `internal/backup`
rather than introducing a parallel scheme. Within a major version, changes are additive-only
and readers ignore unknown fields. Restore:
- **tolerates absent subsystems** — an old backup lacking a new datatype's objects restores
  fine into a new binary;
- **restores unknown CFs blind** — a newer backup containing a CF an older binary does not
  recognize is still restored as raw `(cf,key,value)` into an auto-created CF (the engine
  creates CFs lazily). Data is never lost; semantics light up after the binary is upgraded;
- **derived state is declared derived, never stored, rebuilt via the registered hook** — a new
  index type cannot break old backups.

## 4. New wavesdb APIs (additive, off the hot path)

Implemented in `/Volumes/HOME/code/storage-engines/wavesdb`:

```go
// Pin a consistent read view at a GlobalSeq for the whole export without holding a Txn open.
// Build on the existing private acquireSnapshot/releaseSnapshot machinery.
func (db *DB) AcquireSnapshot() *Snapshot
func (s *Snapshot) Seq() uint64
func (s *Snapshot) NewIterator(cf *ColumnFamily) *Iterator
func (s *Snapshot) Release()

// Seq-based change detection for BOTH planes' incrementals: SSTable file-ids whose MaxSeq > seq
// (immutable ids → stable diff). Build on the existing SSTMeta.MaxSeq.
func (db *DB) SSTablesSince(seq uint64) []SSTableMeta

// Stream a consistent checkpoint's SSTables (+ manifest) to an object sink, uploading only
// ids absent from `parent`. Reuses the engine's existing ObjectStore plumbing.
func (db *DB) CheckpointToObjectStore(ctx, sink ObjectSink, parent *CheckpointManifest) (CheckpointManifest, error)
```
`ListColumnFamilies()` already exists (`wavesdb/db.go:320`) and is reused for generic CF
discovery — not a new API. Restore-side physical reuses the existing `Open{ReadOnly:true}` +
`PromoteToPrimary()`. The snapshot view must hold consistent reads under concurrent writes
(verified `-race`).

## 5. Contributors (v1 registry instances)

| Contributor | Owned CFs | Consistency | Version source (`VersionOf`) | `OwnerOf` (dedup) | Rebuild hook |
|---|---|---|---|---|---|
| System/config | `CFSys` (config/namespace snapshots only) | SeqSnapshot | n/a (`ok=false`) | meta-shard leader (single copy) | — |
| KV | `CFKVData`, `CFKVMeta` | HLCCeiling | version **key suffix** (`internal/recordstore/encode.go`) | placement/holder-dir → one live holder; all-export fallback | — |
| Collections | `CFReplData` | SeqSnapshot (raft index) | n/a | shard's raft leader (1 logical copy per shard) | — |
| Graph | `CFGraphData` (auth), `CFGraphIndex` (derived) | HLCCeiling | `Version` field in `NodeRecord`/`EdgeRecord` value proto | holder-dir | `graph.(*Store).RebuildIndexes(graph)` |
| Vector | `CFVectorRaw` (auth), `CFVectorIndex` (derived) | HLCCeiling | `VectorRecord.version` (field 8) in value proto | holder-dir | `vector.RebuildLiveIndex()` |

Excluded everywhere (transient/replayable): `CFReplLog`, `CFCacheMeta`. Adding a datatype adds
a row here and nothing else.

**The existing codec's authoritative set vs. this registry.** `internal/backup` today streams
7 CFs `{CFSys, CFKVData, CFKVMeta, CFGraphData, CFGraphIndex, CFVectorRaw, CFVectorIndex}`. The
registry above owns the same authoritative data plus `CFReplData` (collections — not in the old
codec) and **splits `CFSys`**: cluster/namespace **config** is backed up and restored, but
**node-local identity** (storage UUID, member metadata in `internal/storage/identity.go`) is
**excluded from logical export and never written by logical restore** — identity is per-node
and regenerated on first start, so importing a source node's UUID would corrupt the target.
The derived index CFs (`CFGraphIndex`, `CFVectorIndex`) are exported by neither plane's logical
path; they are rebuilt via the hook. (Physical same-shape DR of the *same* cluster does carry
identity in its SSTables, which is correct for that case; cloning to a *new* cluster identity
must use the logical path — see §7.)

## 6. Export mechanics

- **Logical.** On its pinned snapshot, a node iterates each owned CF for its assigned
  key-ranges and streams opaque `(cf,key,value,version)` chunks via the existing
  `internal/backup` codec, pointed at an S3 multipart sink instead of an `io.Writer`.
  - **Full:** emit every record; AP tiers apply the `Version ≤ T` ceiling (via the
    contributor's `VersionOf`), CP tiers emit at the pinned seq.
  - **Incremental: the logical plane is FULL-ONLY (Phase 3 refinement, 2026-06-30).** Logical
    backups are always full. Incrementals are carried solely by the **physical plane** (§8),
    which is inherently per-node (each node diffs its own immutable SSTables by `MaxSeq`), so
    there is no owner-assignment/ownership-change watermark problem. Re-shard / partial / clone
    (the things only the logical plane can do) are always taken from a full logical backup. This
    removes the per-(owner,range) logical-watermark bookkeeping that was previously an open
    question, at the cost of larger logical backups (full each time) — an acceptable trade since
    same-shape DR/PITR, where incremental cost matters most, runs on the physical plane.
  - Collections reuse the dragonboat `SaveSnapshot` iteration (already raft-index-consistent),
    redirected to S3.
- **Physical.** A node calls `CheckpointToObjectStore` → hard-link checkpoint at the pinned
  `GlobalSeq`, upload only SSTable file-ids absent from `parent`'s manifest. The node records
  its source `storageUUID` + ordinal for same-shape mapping on restore. The physical plane is
  **klog-format-agnostic** — it byte-copies whole SSTable files, so wavesdb's flat vs B+tree
  hybrid klog (`UseBTree`) is transparent to backup.

## 7. Restore / clone / PITR

**Driven at bootstrap (Phase 3 refinement):** restore is a node-startup operation — a node
configured with a backup reference restores from S3 **before it begins serving** (physical:
pull my checkpoint by ordinal + recover; logical: import + re-route into this cluster's shape).
Online restore-into-a-live-cluster (for partial/tenant import) is deferred. The mechanics below
are unchanged; only the trigger is bootstrap rather than an online RPC.

Restore reads `cluster.manifest`, reconstructs namespace configs + collection inventory +
source topology, then picks a path per the **target shape and the restore intent**. Intent
(`restore` = DR of the same cluster vs `clone` = new cluster identity) comes from the CLI/API
invocation — it is *not* a field in the backup:

- **DR of the same cluster, same shape, physical present → physical fast path.** Map source
  `storageUUID` → target node by ordinal, drop SSTables into each data dir, `Open{ReadOnly}` +
  `PromoteToPrimary()`. No re-replication (each node already holds its shard). **This path is
  gated on intent = DR, not just shape**: it intentionally carries node identity (storage UUID,
  member metadata) from `CFSys`, which is correct only when restoring *the same logical
  cluster*. The CLI's `restore` (DR) may use it; `clone` (new cluster identity) must **not** —
  even at matching shape — because importing the source identity would corrupt the new cluster.
  `clone` always takes the logical path below, which excludes node-local identity (§5).
- **Different shape, any clone, or logical-only → logical path.** (Re-shard when the shape
  differs; verbatim re-route when it matches.) Stream chunks back; KV →
  normal coordinator write path (re-routes via generic `route()`, re-replicates to target-N,
  repair fills). Collections re-shard at **whole-collection granularity**: routing hashes
  `routeKey(ns,coll)` (`ShardForKey`), so a collection relocates atomically to exactly one
  target shard. Restore groups a collection's records by its target shard under the new `N`,
  then builds that shard's state and loads it via `RecoverFromSnapshot` (or proposes the rows)
  — there is no per-row raft proposal and no blob replay into a mismatched layout. Unknown CFs
  → blind raw-KV restore (invariant D). Derived indexes rebuilt via hooks.
- **PITR.** Pick base full + incremental chain up to target `T'`; apply in HLC order (LWW makes
  it idempotent); stop-at-`T'` by filtering `Version ≤ T'`.
- **Partial.** Select namespaces/collections from the manifest; stream only those objects.

### 7.1 Time-relative state across restore

Some datatypes hold **absolute wall-clock state** — KV lazy TTL, and LeasedBudget's lease
reclaim deadlines (`ReclaimNotBeforeMs`), pacing token-bucket (`LastRefillMs`, `Tokens`), and
tombstone-GC timers. A backup captures these **byte-faithfully** at the cut; the backup core
never interprets them (invariant A). What changes across restore is *time itself*: a backup
taken at `T0` and restored at `T1 > T0` (DR minutes later, a clone, or PITR) has deadlines that
may now be in the past.

Reconciliation is **each datatype's own responsibility**, run from its `RebuildAfterRestore`
hook (which receives `RestoreInfo`: capture wall-clock, restore wall-clock, frontier `T`,
intent, shape-changed). The backup core stays datatype-agnostic. Principles:

- **Default is conservative and safe, not transparent.** For LeasedBudget, the first
  leader-driven expiry sweep after restore will reclaim any lease whose deadline has passed,
  debiting its outstanding remainder to `spent` (it is *not* credited back to `available`).
  Unspent-but-expired quantity is therefore stranded as underspend rather than
  double-granted — the correct conservative outcome (a holder cannot resume spending across a
  restore without re-drawing).
- **Recovery is via the datatype's existing mechanism, not the backup.** Stranded budget is
  recovered by calling `BudgetReconcile` (already implemented, Stage 2) with the authoritative
  acked total from the external ledger. The backup spec only *documents* this; it adds no
  budget-specific code.
- **`RestoreInfo` enables informed reconciliation.** Because the manifest records the capture
  wall-clock (§8), a hook can compute the elapsed gap and choose its policy (e.g. block new
  budget draws for a `maxPauseMs + 2·maxClockSkewMs` grace window so auto-reclaim settles
  first, or shift deadlines for a clone). This is datatype policy, kept in the datatype's plug.
- **PITR is exact at the cut.** Restoring to `T'` reproduces the byte-exact time-state as of
  `T'`; subsequent sweeps then progress normally from the restore wall-clock.

This is a documented operational contract, not a backup-core feature: time-relative datatypes
opt into reconciliation through the hook; datatypes without wall-clock state ignore it.

## 8. S3 object layout

```
s3://<bucket>/<clusterID>/backups/<backupID>/
  cluster.manifest.json   # format_version, frontier T, capture_wall_clock_ms, parent, planes,
                          #   source topology, namespace/collection inventory,
                          #   per-node submanifest pointers,
                          #   per-object sha256, status (COMPLETE|PARTIAL + gap list)
  nodes/<storageUUID>/
    node.manifest.json     # assignments, per-CF GlobalSeq watermarks (for seq incrementals),
                          #   object list, counts
    logical/<cf>/<namespace>/part-NNNNN.chunk   # zstd, length-prefixed (cf,key,value,version)
    physical/<cf>/<sstID>.{klog,vlog}           # immutable SSTable files (checkpoint)
  shards/<shardID>/part-NNNNN.chunk             # collections per-shard logical (raft-index consistent)
```
- **Incremental (physical plane only — Phase 3 refinement):** `parent` in the manifest; physical
  uploads only SSTable file-ids absent from the parent manifest (per-node `SSTablesSince` diff,
  seq-based). Restore walks the parent chain. The logical plane is full-only (§6), so a logical
  object set never has a `parent`.
- **Integrity:** every object has a sha256 in the manifest, verified on restore; the manifest
  is itself checksummed.

## 9. Object store abstraction & config

`internal/backup/objstore` exposes `Sink`/`Source` interfaces (multipart upload, ranged
download, list, retry/backoff). Implementations: **S3** (MinIO-compatible via endpoint
override) and **filesystem** (tests + local/dev). Config (env + file): bucket, prefix, region,
endpoint, credentials (prefer IRSA/IAM role; secret fallback), SSE-S3/KMS, multipart part
size, max concurrency, and a **bandwidth rate-limit** so export cannot starve the hot path.

## 10. Failure handling & operability

- **Durable, resumable:** `BackupIntent` in the meta shard; crashed-coordinator backups are
  re-driven; export is idempotent (content-addressed objects).
- **No silent success:** coverage cross-check → explicit `PARTIAL` status with enumerated gaps.
- **Integrity:** per-object + manifest sha256, verified on restore.
- **Durable-artifact lifecycle — nothing becomes trash (Phase 3 requirement).** Every durable
  artifact the backup system creates is both explicitly deletable AND bounded by a TTL/retention,
  swept by a leader-driven sweep (the same pattern as the existing TTL / budget-lease sweeps):
  - `BackupIntent` (meta shard) carries a **lease deadline** while in-progress — if the
    coordinator dies and nobody renews/resumes by the deadline, the sweep marks it `FAILED`
    (and reclaims, mirroring budget lease-reclaim). Terminal intents (`COMPLETE`/`PARTIAL`/
    `FAILED`) carry a **`retainUntilMs`**; the sweep deletes them after retention. No intent
    lingers forever.
  - **`DeleteBackup(backupID)`** admin RPC removes the intent **and** its S3 objects,
    **chain-aware**: refuses to delete a backup a live incremental child depends on (or cascades
    the whole chain on explicit `force`).
  - **S3 retention + orphan GC:** a chain-aware retention policy (max-age / max-count) sweeps old
    chains; an orphan-reconciliation pass removes objects under the cluster prefix not referenced
    by any live intent's manifest (failed/partial-export debris).
- **Retention/GC:** chain-aware — an incremental pins its parents; GC refuses to delete a
  parent with live children. The catalog (meta shard + mirrored S3 index) tracks chains. Any
  bounded coverage is logged, never silent.
- **Throughput safety:** export reads bypass the disk-pressure write gate, but export I/O and
  S3 concurrency are rate-limited; physical checkpoint uses hard-links (no double disk).
- **Overload (standing invariant):** killing a node mid-export triggers reassignment +
  `PARTIAL` detection; disk-pressure during restore degrades gracefully; a write flood during
  a backup is simply excluded by the `>T` cut; no node crashes.

## 11. Components / file breakdown

- `internal/backup/` (extend): keep the stream codec; add `registry.go` (contributor interface
  + registrations), `coordinator.go` (phases, assignment, commit), `agent.go` (node-side
  prepare/export), `manifest.go` (versioned cluster+node schema), extend `restore.go`
  (re-shard / physical / PITR / partial), `objstore/` (S3 + fs).
- `proto/wavespan/v1/backup.proto`: `BackupService` — `BeginBackup`, `BackupStatus`,
  `ListBackups`, `RestoreBackup` (admin); internal `PrepareBackup` / `ExportBackup` node RPCs;
  streaming progress. Mounted on the admin port (7900).
- Meta-shard SM: `BackupIntent` create/update/complete commands (small addition to the
  existing meta SM).
- `cmd/wavespan-node/main.go`: wire `BackupService`, objstore config, the node agent, and
  register the four contributors.
- **wavesdb** (`/Volumes/HOME/code/storage-engines/wavesdb`): the four APIs in §4.
- **Operator:** implement the stub `WaveSpanBackup` controller → `BeginBackup`; add
  `WaveSpanRestore` + a cron schedule for periodic full+incremental.
- **CLI:** `wavespanctl backup {create,list,show,restore,clone}`.

## 12. Testing strategy

- **Unit:** stream codec round-trip incl. unknown-CF blind restore; manifest forward-compat
  (old manifest read by new code; new manifest with extra fields read by old code); chain-aware
  GC; `route()` re-shard determinism.
- **Engine:** the four wavesdb APIs — snapshot isolation under concurrent writes (`-race`);
  incremental SSTable diff correctness; checkpoint-to-objstore against fs + MinIO.
- **Integration (real cluster):** full backup → restore into (a) same-shape via physical and
  (b) **different N** via logical re-shard; verify KV + collections + graph + vector (ANN
  rebuilt) data equality. Incremental chain: full → writes → incremental → PITR restore to
  `T'`. Partial: extract one namespace into a fresh cluster.
- **Chaos/overload:** kill a node mid-export → reassignment + `PARTIAL` detection (no silent
  gap); disk-pressure during restore → graceful; write flood during backup → cut excludes
  `>T`, no node crash.

## Implementation status (2026-06-29)

- **Phase 1 (wavesdb engine primitives) — DONE**, branch `backup-primitives`: `AcquireSnapshot`/
  `*SnapshotHandle`, `SSTablesSince`, `CheckpointToObjectStore`, `RestoreFromObjectStore`. TDD,
  two-stage reviewed, `-race` clean.
- **Phase 2a (logical core, same-shape) — DONE**, branch `backup`: `Contributor` registry + 5
  contributors, versioned `NodeManifest`, `ExportLogical`/`RestoreLogical` with manifest
  entry-count integrity check. 2a copies derived index CFs verbatim (rebuild deferred). TDD,
  two-stage reviewed.

### Phase 2b grounding findings (de-risk the next phase)

- **Collections re-shard is feasible and partly pre-built.** `internal/collections/migrate.go`
  already extracts the routing key from a `CFReplData` key suffix — `routeKeyOf(suffix)
  ([]byte, bool)` handles `subData`/`subTTL` rows and returns `ok=false` for shard-meta
  (`subMeta`) and budget secondary-index keys (`subBudExp`/`subBudTombGC`). Re-shard restore =
  for each data row, `routeKeyOf` → recompute `FirstDataShard + fnv64a(routeKey) % newN` →
  rewrite the 8-byte shard prefix. The `migrate.go` ingest/purge codec is reusable. Caveat:
  budget shard-level secondary indexes (`subBudExp`/`subBudTombGC`) are NOT carried by
  `routeKeyOf` → after re-shard they must be **rebuilt** from the re-routed lease rows (a budget
  rebuild hook), or auto-reclaim/tomb-GC won't fire in the new shard. The shard `applied` raft
  index (`subMeta`) must be dropped (new shard starts fresh), not re-routed.
- **Vector ANN index specs are CONFIG-only, not in storage.** `vector.IndexSpec` "mirrors the
  VectorIndex CRD spec"; specs are built in `cmd/wavespan-node/main.go:338` from config
  (`ParseVectorIndexSpec` → `IndexMeta`; `NewIndexSet(metas, ann.DefaultParams())`), never
  persisted to a CF. So `RebuildLiveIndex(store, coll, metric, params)` cannot recover
  metric/params from a logical backup of `CFVectorRaw` alone. Restore-side rebuild must source
  specs from the **destination cluster's configured `IndexSet`** (the common clone/DR case), OR
  the backup must **capture the index specs** (from running config at backup time → manifest) so
  a spec-less destination can rebuild. Raw vectors restore regardless; ANN comes up once specs
  are available. Graph rebuild has no such dependency — `RebuildIndexes(graph)` derives entirely
  from stored node/edge data (only needs graph-name enumeration).

### Phase 2c (partial selection) — DONE
Per-package prefix-aware key decoders + `Selector` + per-contributor matchers + `ExportLogical`
export-time filter; panic-safe (uvarint overflow guard). Branch `backup`. TDD, two-stage reviewed.

### Phase 3 design decisions (2026-06-30, brainstormed + approved)
The companion spec is `docs/superpowers/specs/2026-06-30-backup-phase3-cluster-design.md`. Decisions
that refine THIS spec (folded in above):
- **Scope:** all of Phase 3 (3a coordination + 3b incrementals + 3c physical) designed together.
- **Incrementals = physical-plane only; logical full-only** (§1/§6/§8 updated) → retires the
  per-(owner,range) logical-watermark open question below.
- **Restore = bootstrap-from-backup** (node restores from S3 at startup before serving); online
  restore-into-a-live-cluster RPC deferred.
- **Durable-artifact lifecycle** (§10): lease-deadline'd in-progress intents → `FAILED` on expiry;
  `retainUntilMs` on terminal intents; `DeleteBackup` (chain-aware) + S3 orphan reconciliation.
- **Resolved:** `BackupIntent` uses the meta-shard `metaCommand` opcode pattern (new
  `opBackup*` ops); `BackupService` is served gRPC on the data port + Connect on the admin port
  (matching `BudgetService`).

## Open implementation questions (for the plan, not blockers)

- Credential sourcing precedence on OVH stag (IRSA-equivalent vs sealed-secret) — deployment.
- Default schedule/retention values (operator scheduling = Phase 4; manual trigger + retention
  knobs are Phase 3).
- ~~Per-(owner,range) logical incremental watermark bookkeeping~~ — **RESOLVED:** logical is
  full-only; incrementals are physical-plane only (per-node `SSTablesSince`, no owner-change).
- PITR granularity for an **unknown** CF: with physical incrementals, PITR is per-node-seq
  chain-granular (not HLC-`T'` sub-filtered) — acceptable for same-shape DR/PITR.
