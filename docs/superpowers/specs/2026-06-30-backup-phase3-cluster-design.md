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
  **intent sweep** (lease-expiry + retention), mirroring the budget/TTL sweep.
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
3. **Prepare** — fan out `PrepareBackup` to assigned nodes (reuse `Fanout`/gossip). Each node seals
   `T` (advance HLC past `T`, drain in-flight `≤T`, pin `LocalStore.Snapshot()` for logical / a
   wavesdb snapshot for physical), ACKs its `GlobalSeq` + held-range summary.
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
- **Physical same-shape DR** — read `cluster.manifest` topology, map source `storageUUID` → this
  node by ordinal, `RestoreFromObjectStore` my checkpoint chain (base+incrementals) into the data
  dir, open, raft groups recover. (Intent = restore-same-cluster; carries node identity, correct
  for DR.)
- **Logical clone / re-shard** — bootstrap empty, import the logical record stream, re-routing
  collections via Phase 2b `RerouteSuffix` under *this* cluster's N; node-local identity excluded
  (Phase 2a); partial selection honored. (Intent = clone; new cluster identity.)
The bootstrap config names the backupID + intent (DR vs clone) + target shape; selection of plane
(physical vs logical) follows the master spec §7 rule (DR-same-shape → physical fast path; clone /
shape-change → logical).

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
- Bootstrap-restore: exactly how a node learns the source ordinal→`storageUUID` topology mapping for
  the physical path (from `cluster.manifest`) and how it's matched to its own ordinal.
- S3 credential sourcing on OVH stag (IAM-role-equivalent vs sealed-secret).
