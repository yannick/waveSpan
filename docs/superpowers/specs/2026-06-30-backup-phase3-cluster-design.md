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
  force)`. `BackupSpec` carries `{selection, planes, parent, destination}` — see §1.1 for the
  destination override and §11 for the admin Backup UI that consumes these RPCs. `BackupState`
  carries `{status, phase, perNode[]{memberID, phase, objects, bytes, done}, overallPct, gaps,
  destination, startedMs, finishedMs, parent}` so the UI can render live progress. Internal node
  RPCs: `PrepareBackup(backupID, frontierT, assignment, destination)→{globalSeq, heldRanges}`,
  `ExportBackup(backupID)→{subManifestRef}` (or a single `RunBackup` that prepares then exports),
  and progress reporting (gossip piggyback preferred; an explicit ack RPC as fallback).
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
- **`ui/` Backup view** (admin SPA) — a new page backed by the `BackupService` Connect endpoint
  (§11), the same way the Budget/KV/Vector views are mounted.
- **S3 config + destinations** — reuse `wavesdb/objstore` S3 backend. Config supplies a **default
  destination** plus zero or more **named alternate destinations** (each a bucket/prefix/region/
  endpoint + a credential *reference*, e.g. a sealed-secret/IAM-role name); see §1.1.

### 1.1 Backup destination (default, named, or explicit override)
`BackupSpec.destination` selects where a backup is written, so a backup is **not limited to the
config/env bucket**:
- **Omitted** → the **default** destination from config (the common case).
- **Named** → one of the operator-pre-registered named destinations from config (a dropdown in the
  UI). No secrets in the request — credentials are resolved server-side from the named reference.
- **Explicit override** → `{bucket, prefix, region, endpoint, credential}` supplied in the request,
  for an ad-hoc bucket the operator hasn't pre-registered. `credential` is preferably a **secret
  reference** (resolved server-side); raw inline credentials are accepted only over the
  authenticated admin endpoint and are **transient** — used for that backup run and **never
  persisted** (the `BackupIntent` and `cluster.manifest` store only the non-secret destination
  descriptor — bucket/prefix/region/endpoint — and the credential *reference*, never raw secrets),
  and never logged. Every node's agent receives the destination (incl. resolved/transient creds)
  via `PrepareBackup` over the authenticated inter-node channel.
- **Security:** destination override is an authenticated admin operation (the admin port enforces
  identity via `EnforceHTTP`). The endpoint/bucket are used as the operator specifies (the operator
  is trusted for where their data goes); creds are excluded from the intent, manifest, and logs.
  `DeleteBackup`/GC/retention all key off the recorded destination descriptor, so alternate-bucket
  backups are lifecycle-managed exactly like default-bucket ones.

## 2. Backup coordination protocol (phased, durable, resumable)

The catalog/intent lives in the **meta shard** (raft) — a single serialization point that survives
coordinator failure. Phases (master spec §2):
1. **Begin** — coordinator allocates `backupID` + frontier `T = HLC.now()+lease`; resolves the
   destination (§1.1; named refs resolved here, inline creds held transiently — not persisted);
   persists `BackupIntent{backupID, frontierT, captureWallClockMs, selection, planes, parent,
   destination(descriptor + cred *reference* only), status=RUNNING, leaseDeadlineMs, perNodeState}`
   via `opBackupBegin`.
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

> **Implementation status (2026-06-30): the cluster-wide HLC cut is DEFERRED to Phase 3a.1.**
> Phase 3a as built delivers **per-node snapshot isolation** — each live node exports a consistent
> point-in-time snapshot of its *own* store taken at export time (correct full-coverage union via
> all-export + LWW dedup on restore), but there is **no coordinated cluster-wide frontier `T` and no
> `Version ≤ T` AP-tier filtering** yet. So a 3a logical backup is internally consistent per node but
> not sealed to a single cluster-wide instant. **Phase 3a.1** implements the spec's cut: coordinator
> picks `T = now + lease` (skew-cap bounded), each node advances its HLC past `T` (`Clock.Update`,
> handling `*SkewError`), drains in-flight `≤T`, and AP-tier export filters `Version ≤ T` (needs a
> per-contributor `VersionOf` extractor — KV version key-suffix, graph/vector value field — which
> Phase 2 left out). 3a.1 also adds the commit-time coverage cross-check (held-ranges vs assignment)
> so `PARTIAL` reflects real cluster gaps, not only an assigner-supplied list. Until 3a.1, treat the
> logical backup's cross-node consistency as eventual (bounded by replication lag), not a hard cut.

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

### 5.0 Collections & the dragonboat Raft LogDB (consistency rule — grounded 2026-06-30)
Collections state is split across **two** stores: the shard data + per-shard applied-raft-index live
in the wavesdb `store`'s `CFReplData`, but the dragonboat **Raft LogDB is a separate pebble dir**
(`<storage>/collections-raft`) that the backup does **not** capture. On `Open()`, a shard's
`baseSM` reads its persisted applied index from `CFReplData` and reports it to dragonboat; a fresh
LogDB has no matching log → divergence if a stale applied index is restored. **Rule:** on restore,
collections **reset their raft bookkeeping** — drop the `subMeta` applied-index rows + dedup
(`subDedup`/`subDedupRing`) keys (exactly what Phase 2b's `RerouteSuffix` already drops) — and the
shards **bootstrap fresh** (`BootstrapWithPlacement`/`BootstrapN`); the restored `CFReplData` data
rows then become the shards' initial on-disk SM state at applied-index 0, with a fresh log. This
holds for **every** restore path (logical clone, logical same-shape, and physical DR): KV / graph /
vector are raft-free and take the fast physical-SSTable path, but **collections are always
re-established via reset-bookkeeping + fresh bootstrap**, never by carrying the LogDB. (Restore must
run before the collections tier bootstraps — see §5; the data is in `CFReplData` when `Open()`
reads it.)

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

### 5.2 Restore operational consequences & known limitations (verified 2026-06-30)
Implemented and reviewed, with these honest consequences to operate around:
- **Restore resets the meta shard → the BackupIntent catalog is dropped.** `StripRaftBookkeeping`
  clears the whole meta shard (so collections re-bootstrap fresh), which includes the `subBackup`
  BackupIntent catalog. A restored/cloned cluster therefore starts with **no backup history or
  schedule** — the S3 backups themselves remain, but the operator must **re-register backup intents
  post-restore**. Correct for a clone (it shouldn't inherit the source's schedule); for same-cluster
  DR the catalog is rebuildable by listing S3 / re-registering.
- **Multi-node logical restore over-replicates (wasteful, not wrong).** The startup hook has each
  node restore the full per-node export set into its own store; collections rows for shards a node
  doesn't host are inert (each shard SM scans only its own prefix), and KV over-replication is
  tolerated (versioned + holder-directory/repair). **Caveat:** KV repair only converges *up* (adds
  replicas to target) — there is **no shed/prune path**, so multi-node-clone KV over-replication is
  *permanent* until an external placement-GC/compaction removes the extra copies. Acceptable at
  current scale; a **"restore only my shards/placement"** optimization is a tracked follow-up.
- **Physical node match is by `MemberID`** (the manifest also carries `StorageUUID`, currently
  unused for matching) — correct while member ids are stable (ordinal DNS); id reassignment would
  need the `StorageUUID` fallback.

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
- `cmd/wavespan-node/main.go` — register `BackupService` (gRPC + Connect at `bckPath`, admin-auth),
  objstore config + named destinations, node agent wiring, bootstrap-restore before serving,
  intent-sweep start.
- `ui/` — a "Backups" SPA view (list / live progress / trigger-with-destination / delete) using the
  `BackupService` Connect client (§11), mirroring the existing Budget view.
- gitops `apps/ovh-stag/.../wavespan/` — S3 creds env, named alternate destinations, (per-node)
  restore-from config.

## 10. Testing (real OVH stag cluster)

- **Unit:** coordinator phases + resume; owner assignment incl. all-export fallback; `PARTIAL`
  coverage detection; `BackupIntent` metaCommand encode/decode + sweep (lease-expiry→`FAILED`,
  `retainUntilMs` deletion); chain-aware `DeleteBackup`; orphan GC reconciliation.
- **Integration (cluster):** full logical backup → bootstrap-clone into a **different-N** cluster
  (all datatypes verified); physical full + incremental → same-shape DR bootstrap-restore; PITR via
  physical chain; partial extract → bootstrap-clone; lifecycle (`DeleteBackup` chain-aware refuse +
  cascade; abandoned-coordinator intent lease-expires; retention/orphan sweep);
  **destination override** — trigger a backup to an alternate (named and explicit) bucket and
  confirm objects land there + the intent/manifest record the destination but **no raw creds**.
- **UI:** `ListBackups`/`BackupStatus`/`BeginBackup`/`DeleteBackup` exercised via the Connect
  endpoint (admin-auth enforced); live-progress rendering during a `RUNNING` backup; trigger form
  produces a backup to the chosen destination; creds never appear in `BackupStatus`/`ListBackups`
  responses or logs.
- **Chaos/overload:** kill a node mid-backup → reassignment + `PARTIAL`; coordinator crash → resume
  (and lease-expire path); write flood during backup → no crash, cut excludes `>T`; disk-pressure
  during restore → graceful.

## 11. Admin Backup UI (the admin SPA "Backups" view)

A new page in the embedded admin SPA (served on the admin port, gated by `EnforceHTTP` admin
identity — same mounting as the Budget/KV/Vector views), backed entirely by the `BackupService`
**Connect** endpoint. No new server surface beyond `BackupService`; the UI is a client of it.

- **See which backups exist** — `ListBackups()` renders a table: `backupID`, status
  (`RUNNING`/`COMPLETE`/`PARTIAL`/`FAILED`), planes (logical/physical/both), full vs incremental +
  parent chain, started/finished time, total size, **destination** (bucket/prefix), and
  `retainUntilMs`. `PARTIAL` rows expand to show the enumerated gaps.
- **See in-progress backups + live progress** — for a `RUNNING` backup the view polls
  `BackupStatus(backupID)` (or consumes a server-stream) and renders the **phase**
  (assign/prepare/export/commit), an **overall %**, and a **per-node breakdown** (`memberID`, phase,
  objects/bytes uploaded, done) from `BackupState.perNode[]`. Progress data originates from the
  coordinator's `perNodeState` (intent) fed by gossip-piggybacked node progress.
- **Trigger a backup** — a form invokes `BeginBackup(BackupSpec)`: choose **selection** (full, or
  pick namespaces / graphs / vector-collections — Phase 2c `Selector`), **planes** (logical /
  physical / both), full vs incremental (pick a `parent`), and **destination** (§1.1): the default,
  a **named** alternate from a dropdown, or an **explicit** bucket/prefix/region/endpoint + a
  credential (secret-reference preferred; inline only over the authenticated admin endpoint,
  transient, never persisted/logged). The UI shows the resulting `backupID` and switches to its
  live-progress view.
- **Manage** — `DeleteBackup(backupID, force)` from the row (chain-aware; the UI warns + offers
  `force` when a backup has live incremental children).

All four capabilities are pure `BackupService` calls, so the CLI/operator (Phase 4) reuse the same
RPCs. The UI adds a `ui/` view + a `BackupService` Connect handler mounted at a `bckPath` in
`main.go` (mirroring `budPath`); no other backend change.

## 12. Open implementation questions (for the plan)

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
- `BackupStatus` for the UI: server-streamed live updates vs UI polling (poll is simplest; stream is
  nicer for progress — decide in the plan).
- Config schema for **named alternate destinations** (a list of `{name, bucket, prefix, region,
  endpoint, credentialRef}`) and whether explicit inline-credential overrides are enabled by policy
  (some deployments may want to restrict destinations to the named set only).
