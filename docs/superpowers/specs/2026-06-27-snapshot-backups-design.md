# Snapshot Backups — Design Spec

Status: design approved, ready for implementation planning.
Date: 2026-06-27.
Worktree/branch: `waveSpan-backup` / `backup`.

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
  `(cf, key, value, hlc_version)` triplets. Re-shardable, partial-capable, incremental by HLC
  range. The only plane that can clone into a different shape or extract a single tenant.
- **Physical plane (same-shape DR fast path).** Per-node wavesdb SSTables streamed to S3 via
  the engine's `ObjectStore` mode. Incremental is near-free because SSTables are immutable
  (upload only new file-ids). Topology-bound: restore maps source node → target node by
  ordinal and calls `PromoteToPrimary()`.
- Both planes write into one `cluster.manifest`. A backup may carry logical + physical
  artifacts; restore picks the cheaper valid path for the target shape.

## 1. Consistency — global cut via HLC frontier

A backup is defined by a cluster-wide **HLC frontier `T`**. The cut is clean because every
record carries an HLC version (`internal/version/hlc.go`) and HLC is monotonic per node:

1. The coordinator picks `T = now() + small lease`.
2. Each participating node, on receiving `T`:
   a. **advances its local HLC past `T`** (`clock.Update`) so no *future* local write can be
      `≤ T`;
   b. **drains in-flight writes** with `Version ≤ T`;
   c. **pins a wavesdb snapshot** at its current `GlobalSeq` (new `AcquireSnapshot`, §4).
3. The union of every node's `Version ≤ T` records is a causally-coherent instant, bounded by
   the existing 500ms HLC skew cap. Writes after `T` proceed normally and are excluded.

The data model has no cross-key/cross-shard transactions (KV is eventual + HLC-LWW;
collections are per-shard raft), so an HLC cut is the correct and maximal consistency notion.
The seal is datatype-agnostic: every contributor reduces to "pin a snapshot at `GlobalSeq`."

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

**A. Opaque payloads.** The backup engine never parses `value` bytes. The logical stream is
generic `(cf, key, value, hlc_version)`. Datatype payloads (protobuf) evolve under their own
forward-compat rules; adding a field never touches backup code.

**B. Registry, not hardcoding.** Each subsystem registers a `BackupContributor`; the
coordinator iterates the registry and never names a datatype:

```go
type BackupContributor interface {
    CFs() []CFSpec                          // owned CFs/key-prefixes; authoritative vs derived
    OwnerOf(ns string, kr KeyRange) NodeID  // dedup/ownership resolver
    RebuildAfterRestore(ctx) error          // optional: rebuild derived indexes
}
```
A new datatype or replication type = one new registration, zero changes to the backup core or
format. A novel CP sub-tier supplies its own `OwnerOf`/seal; it plugs in, it does not fork the
engine.

**C. Generic key-encoded routing (the one thing re-shard needs).** Re-shard routes a record
with a generic `route(namespace, key, targetTopology)` over the **standard length-prefixed
key + hash convention** (the FNV routing collections already use; placement-hash for KV).
It never inspects `value`. **Invariant for every datatype:** encode the routing key into the
key prefix and use the standard hash. In return, the datatype gets backup/restore/re-shard
for free.

**D. Versioned, additive, blind-restorable format.** The manifest and chunk framing carry a
`format_version`. Within a major version, changes are additive-only and readers ignore unknown
fields. Restore:
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
func (db *DB) AcquireSnapshot() *Snapshot
func (s *Snapshot) Seq() uint64
func (s *Snapshot) NewIterator(cf *ColumnFamily) *Iterator
func (s *Snapshot) Release()

// Generic CF discovery — no hardcoded CF list (used by both planes).
func (db *DB) ListColumnFamilies() []string

// Physical incremental: SSTable file-ids whose MaxSeq > seq (immutable ids → stable diff).
func (db *DB) SSTablesSince(seq uint64) []SSTableMeta

// Stream a consistent checkpoint's SSTables (+ manifest) to an object sink, uploading only
// ids absent from `parent`. Reuses the engine's existing ObjectStore plumbing.
func (db *DB) CheckpointToObjectStore(ctx, sink ObjectSink, parent *CheckpointManifest) (CheckpointManifest, error)
```
Restore-side physical reuses the existing `Open{ReadOnly:true}` + `PromoteToPrimary()`.
The snapshot view must hold consistent reads under concurrent writes (verified `-race`).

## 5. Contributors (v1 registry instances)

| Contributor | Owned CFs | Authoritative | `OwnerOf` (dedup) | Rebuild hook |
|---|---|---|---|---|
| KV | `CFKVData`, `CFKVMeta` | yes | placement/holder-dir → one live holder; all-export fallback | — |
| Collections | `CFReplData` | yes | shard's raft leader (1 logical copy per shard) | — |
| Graph | `CFGraphData` (auth), `CFGraphIndex` (derived) | yes | holder-dir | rebuild graph index |
| Vector | `CFVectorRaw` (auth), `CFVectorIndex` (derived) | yes | holder-dir | `vector.RebuildLiveIndex()` |

Excluded everywhere (transient/replayable): `CFReplLog`, `CFCacheMeta`. Adding a datatype adds
a row here and nothing else.

## 6. Export mechanics

- **Logical.** On its pinned snapshot, a node runs `snapshot.NewIterator(cf)` over each owned
  CF for its assigned key-ranges, filters `Version ≤ T` (and `> T_prev` for incrementals), and
  streams opaque `(cf,key,value,version)` chunks. This reuses the existing `internal/backup`
  codec, pointed at an S3 multipart sink instead of an `io.Writer`. Collections reuse the
  dragonboat `SaveSnapshot` iteration (already raft-index-consistent), redirected to S3.
- **Physical.** A node calls `CheckpointToObjectStore` → hard-link checkpoint at the pinned
  `GlobalSeq`, upload only SSTable file-ids absent from `parent`'s manifest. The node records
  its source `storageUUID` + ordinal for same-shape mapping on restore.

## 7. Restore / clone / PITR

Restore reads `cluster.manifest`, reconstructs namespace configs + collection inventory +
source topology, then picks a path per the **target** shape:

- **Same shape + physical present → physical fast path.** Map source `storageUUID` → target
  node by ordinal, drop SSTables into each data dir, `Open{ReadOnly}` + `PromoteToPrimary()`.
  No re-replication (each node already holds its shard).
- **Different shape (or logical-only) → re-shard logical path.** Stream chunks back; KV →
  normal coordinator write path (re-routes via generic `route()`, re-replicates to target-N,
  repair fills); collections → re-route `(ns,coll)` through the new `HashDirectory` and
  propose/`RecoverFromSnapshot` per new shard. Unknown CFs → blind raw-KV restore (invariant
  D). Derived indexes rebuilt via hooks.
- **PITR.** Pick base full + incremental chain up to target `T'`; apply in HLC order (LWW makes
  it idempotent); stop-at-`T'` by filtering `Version ≤ T'`.
- **Partial.** Select namespaces/collections from the manifest; stream only those objects.

## 8. S3 object layout

```
s3://<bucket>/<clusterID>/backups/<backupID>/
  cluster.manifest.json   # format_version, frontier T, parent, planes, source topology,
                          #   namespace/collection inventory, per-node submanifest pointers,
                          #   per-object sha256, status (COMPLETE|PARTIAL + gap list)
  nodes/<storageUUID>/
    node.manifest.json     # assignments, GlobalSeq, object list, counts
    logical/<cf>/<namespace>/part-NNNNN.chunk   # zstd, length-prefixed (cf,key,value,version)
    physical/<cf>/<sstID>.{klog,vlog}           # immutable SSTable files (checkpoint)
  shards/<shardID>/part-NNNNN.chunk             # collections per-shard logical (raft-index consistent)
```
- **Incremental:** `parent` in the manifest; logical uploads only `T_prev < Version ≤ T`;
  physical uploads only SSTable file-ids absent from the parent manifest. Restore walks the
  parent chain.
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

## Open implementation questions (for the plan, not blockers)

- Exact meta-shard SM command encoding for `BackupIntent` (reuse `opBatch` framing vs a new
  command type).
- Whether `BackupService` is a Connect service bridged like the others or a native gRPC handler.
- Credential sourcing precedence on OVH stag (IRSA-equivalent vs sealed-secret).
- Default schedule/retention values for the operator CRD.
