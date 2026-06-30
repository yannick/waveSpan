# Snapshot Backups — Phase 3 (cluster layer) — Design Spec

Status: design approved (brainstormed 2026-06-30), ready for implementation planning.
Companion to the master spec `2026-06-27-snapshot-backups-design.md` (source of truth) — this doc
details the **cluster layer** that lifts the single-node engine (Phases 1, 2a, 2b, 2c — all DONE)
to a live, distributed, lifecycle-managed backup/restore system. Worktree/branch: `waveSpan-backup`
/ `backup`.

## Context

Phases 1–2c built and verified single-node logical backup (full + re-shard + partial) and the
wavesdb physical primitives. Phase 3 makes it a **cluster** capability: coordinate a consistent
cluster-wide backup to S3, manage its lifecycle durably, and reconstitute/clone a cluster from S3
at startup. Scope is all of Phase 3: **3a** coordination + **3b** (physical) incrementals + **3c**
physical fast-path, designed together.

### Confirmed decisions (from brainstorming; also folded into the master spec)
- **Incrementals = physical-plane only; logical = full-only.** The physical plane is per-node
  (each node diffs its own immutable SSTables by seq), so incrementals have no owner-change
  watermark problem. Re-shard/partial/clone always come from a full logical backup.
- **Restore = bootstrap-from-backup.** A node restores from S3 at startup, before serving. Online
  restore-into-a-live-cluster RPC is deferred.
- **Two-plane consistency:** logical full backup uses a cluster-wide **HLC cut `T`**; the physical
  plane is **per-node-consistent** (each node pins its own snapshot; raft groups recover
  independently).
- **Durable-artifact lifecycle:** every durable artifact is deletable AND TTL/retention-bounded
  (no trash).
- **Transport/encoding (resolved):** `BackupService` served gRPC on the data port + Connect on the
  admin port (matching `BudgetService`); `BackupIntent` uses the meta-shard `metaCommand` opcode
  pattern.

### Non-goals (Phase 3)
- Operator CRD + `wavespanctl` CLI (Phase 4). Phase 3 exposes the RPCs + bootstrap-restore config
  the operator/CLI will drive.
- Online restore into a running cluster (partial/tenant import) — deferred.
- Logical incrementals (dropped by design).

## 1. Components

- **`proto/wavespan/v1/backup.proto` → `BackupService`** — gRPC on data port (inter-node + clients),
  Connect on admin port (UI/CLI/operator). Admin RPCs: `BeginBackup(BackupSpec)→{backupID}`,
  `BackupStatus(backupID)→BackupState`, `ListBackups()→[]BackupSummary`, `DeleteBackup(backupID,
  force)`. Internal node RPCs: `PrepareBackup(backupID, frontierT, assignment)→{globalSeq,
  heldRanges}`, `ExportBackup(backupID)→{subManifestRef}` (or a single `RunBackup` that prepares
  then exports), and progress reporting (gossip piggyback preferred; an explicit ack RPC as
  fallback).
- **`internal/backup/coordinator.go`** — drives the phased protocol; any node that receives
  `BeginBackup`; resumable from the meta-shard intent.
- **`internal/backup/agent.go`** — node-side: executes `PrepareBackup`/`ExportBackup` (Phase 2
  `ExportLogical` + Phase 1 `CheckpointToObjectStore`) against the S3 object store; reports progress.
- **`internal/backup/intent.go`** + **`internal/collections/meta.go`** — `BackupIntent` persisted
  via new meta-shard `metaCommand` opcodes (`opBackupBegin/Update/Complete/Delete`); a leader-driven
  **intent sweep** (lease-expiry + retention). NOTE (review): this is a genuine **mirror, not reuse**
  of the budget/TTL sweep — that machinery (`Manager.sweepOnce`, `scanBudDue`, `sweepBudget`) is
  bound to **data shards** (`sweepOnce` filters `r.isData`; meta is `isData=false`) and its helpers
  are methods on `*shardSM`. Phase 3 must add net-new **meta-side** code: a due-index, a `Lookup`
  query, an `Update` apply case in `metaSM`, AND extend `sweepOnce`'s data-only filter (or run a
  separate meta sweep). The meta propose path (`proposeRaw` for `MetaShardID`) and leader-gating
  already work. ALSO: the existing `metaCommand` codec is fixed-field (`{Op,Start,End,ShardID}`,
  hand-rolled positional) — a rich `BackupIntent` (frontierT, perNodeState, leaseDeadlineMs,
  retainUntilMs, …) does NOT fit; carry the intent as a **serialized blob value** under a new meta
  key sub-space (or extend the codec). The plan must budget for both.
- **`internal/backup/restore_bootstrap.go`** + `cmd/wavespan-node/main.go` — startup restore.
- **`internal/backup/gc.go`** — chain-aware retention + S3 orphan reconciliation.
- **`internal/grpcsrv/backup.go`** — `BackupService` gRPC handler bridging to the coordinator/agent.
- **S3 config** — reuse `wavesdb/objstore` S3 backend; bucket/prefix/region/endpoint/creds via env.

## 2. Backup coordination protocol (phased, durable, resumable)

The catalog/intent lives in the **meta shard** (raft) — a single serialization point that survives
coordinator failure. Phases (master spec §2):
1. **Begin** — coordinator allocates `backupID` + frontier `T = HLC.now()+lease`; persists
   `BackupIntent{backupID, frontierT, captureWallClockMs, selection, planes, parent, status=RUNNING,
   leaseDeadlineMs, perNodeState}` via `opBackupBegin`.
2. **Assign** — ownership from holder-directory + placement + live roster: each KV/graph/vector
   range → one live owner; each collection shard → its raft leader; **no live owner → all-export
   fallback**. Physical plane: every live node owns its own SSTables.
3. **Prepare** — send `PrepareBackup` to each assigned node. NOTE (review): `Fanout` is a target-N
   replica-fill worker (sends `StoreReplica` RPCs), NOT a generic coordination fan-out — the
   coordinator iterates `Roster.Live()` and calls each node via the `BackupService` client (the
   same live-member iteration pattern as `Fanout.fillEverywhere`, not the worker itself); progress
   comes back via gossip piggyback. Each node seals `T` (advance HLC past `T` via `Clock.Update`,
   drain in-flight `≤T`, pin `LocalStore.Snapshot()` for logical / a wavesdb snapshot for physical),
   ACKs its `GlobalSeq` + held-range summary. **`T` must be within the HLC skew cap** — `Clock.Update`
   returns a `*SkewError` and refuses to advance past `wall + maxSkewMs`, so the frontier lease must
   be chosen inside the cap; the coordinator handles the seal-rejection path (retry with a nearer `T`).
4. **Export** — each node streams assigned data to `s3://…/backups/<backupID>/…` (Phase 2
   `ExportLogical(selector, ownedRanges)` + Phase 1 `CheckpointToObjectStore(parent)`), writes its
   per-node sub-manifest, reports progress; coordinator renews the intent lease as progress arrives.
5. **Commit** — coordinator cross-checks coverage (assignment vs held-range summaries), writes
   `cluster.manifest`, sets `status=COMPLETE` (+ `retainUntilMs`). An uncovered range with no live
   holder → `status=PARTIAL` with enumerated gaps. Never a silent success.

Coordinator crash → another node resumes from the intent; if no one resumes before
`leaseDeadlineMs`, the intent sweep sets `status=FAILED`. Export is idempotent (content-addressed
objects), so resume/retry is safe.

## 3. Consistency (two planes)

- **Logical full backup → cluster-wide HLC cut `T`** (master spec §1). Each owner exports its
  `Version ≤ T` converged view (AP, bounded by the skew cap). Logical is full-only, so writes not
  yet converged to an owner at seal are simply captured by the next full backup.
- **Physical → per-node pinned snapshot** at each node's `GlobalSeq`. No cluster barrier; each
  shard's raft state is internally consistent and recovers independently. Physical incrementals
  (3b) = SSTable file-ids absent from the parent (`SSTablesSince`), per node.

## 4. Physical incrementals (3b)

Each node records its last-backup `GlobalSeq` in its sub-manifest. An incremental physical backup
passes the parent `CheckpointManifest` to `CheckpointToObjectStore`, uploading only new SSTable
file-ids. `cluster.manifest` records `parent`; a chain is `full → inc → inc → …`. Restore
(bootstrap) walks base + chain via `RestoreFromObjectStore`. Logical objects never have a `parent`.

## 5. Bootstrap-restore (3a restore side + 3c)

At node startup, if configured (`WAVESPAN_RESTORE_FROM=s3://…/<backupID>`, target intent/shape), the
node restores **before serving**:
- **Physical same-shape DR** — read `cluster.manifest` topology and map each source node's checkpoint
  to a target node by **stable identity**, then `RestoreFromObjectStore` that checkpoint chain
  (base+incrementals) into the data dir, open, raft groups recover. NOTE (review): there is **no
  numeric StatefulSet ordinal field** in code — stable identity is the `MemberID` / advertised DNS
  host (per-ordinal DNS like `wavespan-core-0…`) plus durable `StorageUUID` (`membership/identity.go`).
  The manifest records each source node's `MemberID`/DNS + `StorageUUID`; a target node matches its
  own `MemberID`/DNS to pull the right checkpoint. (Intent = restore-same-cluster; carries node
  identity, correct for DR. Exact matching rule is open question #5.)
- **Logical clone / re-shard** — bootstrap empty, import the logical record stream, re-routing
  collections via Phase 2b `RerouteSuffix` under *this* cluster's N; node-local identity excluded
  (Phase 2a); partial selection honored. (Intent = clone; new cluster identity.)
The bootstrap config names the backupID + intent (DR vs clone) + target shape; selection of plane
(physical vs logical) follows the master spec §7 rule (DR-same-shape → physical fast path; clone /
shape-change → logical).

### 5.1 Forking multiple independent clones from one backup (first-class)
A single backup can seed **any number** of independent clone clusters (master spec goal #2). This
works because:
- **The S3 backup is immutable / read-only** — restore only reads it; no step assumes a single
  target or mutates the backup. N clusters can each bootstrap-restore the same `backupID`,
  concurrently or over time, with no contention beyond S3 read load.
- **Node identity is not imported** — the logical clone path skips `/sys/storage_uuid`, so every
  node in every clone generates its own fresh `StorageUUID`; no collision across clones or with the
  source.
- **Cluster identity is deployment config, not backup data** — `ClusterID` comes only from
  `WAVESPAN_CLUSTER_ID` (never persisted to `CFSys`), so each clone is deployed with its own
  `ClusterID` and gets a distinct cluster identity automatically.
- **Re-shard on restore** — each clone may use a different shard count `N` than the source.

To fork: deploy each clone cluster with its own `ClusterID` + `WAVESPAN_RESTORE_FROM=<same backupID>`
+ intent=`clone`. Clones always use the **logical** path (the physical fast-path is DR-only — it
carries source identity). Caveats: backed-up records carry the source's `writer_cluster_id` in their
historical HLC versions (harmless for a standalone clone; *new* writes use the clone's `ClusterID`)
— it only needs attention if a clone later joins active-active global replication with the source or
a sibling (each must keep a distinct live `ClusterID`, which config already ensures). Each clone
needs its own vector-index config to rebuild ANN (raw vectors restore regardless; specs are
config-only).

## 6. Durable-artifact lifecycle & GC (the "no trash" requirement)

Every durable artifact is explicitly deletable AND TTL/retention-bounded, enforced by a
leader-driven sweep (same pattern as the existing TTL / budget-lease sweeps):
- **`BackupIntent` (meta shard):** in-progress intents carry a **lease deadline**; if not
  renewed/resumed by it (dead coordinator), the sweep sets `status=FAILED` (reclaim, mirroring
  budget lease-reclaim). Terminal intents carry **`retainUntilMs`**; the sweep deletes them after
  retention. No intent lingers.
- **`DeleteBackup(backupID, force)`:** removes the intent AND its S3 objects, **chain-aware** —
  refuses if a live incremental child depends on it, unless `force` cascades the whole chain.
- **S3 retention + orphan GC** (`gc.go`): chain-aware retention policy (max-age / max-count) sweeps
  old chains; an orphan-reconciliation pass lists objects under the cluster prefix and removes any
  not referenced by a live intent's manifest (failed/partial-export debris).
- Per-node watermarks live inside sub-manifests → deleted with the backup.

## 7. Failure handling & overload (standing invariant)

Down node → ranges reassign to a live holder; fully-unavailable range → `PARTIAL`+gap. Coordinator
crash → resume from intent, else lease-expire → `FAILED`. Export reads bypass the disk-pressure
write gate but are rate-limited; `Prepare` drain is bounded; corrupt keys can't panic (Phase 2c
guards). A write flood during backup is excluded by the `>T` cut. No node crashes. No silent
success (explicit `PARTIAL` + gaps).

## 8. S3 / object-store config

Reuse `wavesdb/objstore` S3 backend. Config (env + file): bucket, prefix, region, endpoint
(MinIO-compatible), credentials (prefer IAM-role/IRSA-equivalent; secret fallback), SSE/KMS,
multipart part size, max concurrency, bandwidth rate-limit. Restore config:
`WAVESPAN_RESTORE_FROM` + intent + target shape.

## 9. Components / file breakdown

- `proto/wavespan/v1/backup.proto` (+ generated gRPC/Connect stubs).
- `internal/backup/{coordinator,agent,intent,restore_bootstrap,gc,progress}.go`.
- `internal/collections/meta.go` — `opBackup*` metaCommands + intent sweep.
- `internal/grpcsrv/backup.go` — `BackupService` gRPC handler.
- `cmd/wavespan-node/main.go` — register `BackupService` (gRPC + Connect), objstore config, node
  agent wiring, bootstrap-restore before serving, intent-sweep start.
- gitops `apps/ovh-stag/.../wavespan/` — S3 creds env + (per-node) restore-from config.

## 10. Testing (real OVH stag cluster)

- **Unit:** coordinator phases + resume; owner assignment incl. all-export fallback; `PARTIAL`
  coverage detection; `BackupIntent` metaCommand encode/decode + sweep (lease-expiry→`FAILED`,
  `retainUntilMs` deletion); chain-aware `DeleteBackup`; orphan GC reconciliation.
- **Integration (cluster):** full logical backup → bootstrap-clone into a **different-N** cluster
  (all datatypes verified); physical full + incremental → same-shape DR bootstrap-restore; PITR via
  physical chain; partial extract → bootstrap-clone; lifecycle (`DeleteBackup` chain-aware refuse +
  cascade; abandoned-coordinator intent lease-expires; retention/orphan sweep).
- **Chaos/overload:** kill a node mid-backup → reassignment + `PARTIAL`; coordinator crash → resume
  (and lease-expire path); write flood during backup → no crash, cut excludes `>T`; disk-pressure
  during restore → graceful.

## 11. Open implementation questions (for the plan)

- Single `RunBackup` node RPC (prepare+export) vs separate `PrepareBackup`/`ExportBackup` (two-phase
  lets the coordinator establish the cut across all nodes before exporting — likely two-phase).
- Progress dissemination: gossip piggyback (a `BackupProgressWire`) vs coordinator-poll. Lean
  piggyback (matches existing gossip hooks), poll as fallback.
- Frontier-`T` lease duration + `Prepare` drain timeout defaults.
- Intent lease-deadline + default `retainUntilMs` / retention policy values (operator overrides them
  in Phase 4).
- Bootstrap-restore physical mapping: exact rule for matching a target node to a source node's
  checkpoint via **stable identity** (`MemberID` / advertised DNS host like `wavespan-core-0…` +
  durable `StorageUUID`, recorded per source node in `cluster.manifest`) — there is no numeric
  ordinal field; the match is by `MemberID`/DNS.
- S3 credential sourcing on OVH stag (IAM-role-equivalent vs sealed-secret).
