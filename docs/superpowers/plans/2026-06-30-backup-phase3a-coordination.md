# Backup Phase 3a — cluster coordination backbone — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A cluster can take a consistent **full** backup of itself to S3 via a coordinated, durable, resumable protocol — `BackupService` (RPC), a meta-shard `BackupIntent` catalog, a phased coordinator (begin/assign/prepare/export/commit), owner-assigned export with all-export fallback, and explicit `PARTIAL` detection.

**Architecture:** `BackupService` is defined once in proto and exposed both as gRPC (data port) and Connect (admin port), mirroring `BudgetService`. The coordinator (any node that gets `BeginBackup`) records a durable `BackupIntent` blob in the meta shard, picks a cluster-wide HLC frontier `T`, assigns owners, fans out `PrepareBackup`/`ExportBackup` to nodes via the `BackupService` client, and commits a `cluster.manifest`. Each node's agent runs the Phase 2 `ExportLogical` (+ Phase 1 `CheckpointToObjectStore` for physical) against an S3 object store. This plan covers **full** backup only; incrementals (3b), restore (3c), lifecycle/GC (3d), destination override (3e), and the UI (3f) are separate plans.

**Tech Stack:** Go (`github.com/yannick/wavespan`), connectrpc/connect-go + stdlib grpc-go (dual transport via `buf`), dragonboat meta shard, `wavesdb/objstore` (S3 + FS), gossip membership. Spec: `docs/superpowers/specs/2026-06-30-backup-phase3-cluster-design.md` (§1, §2, §3) + master spec.

## Where to work
Worktree **/Volumes/HOME/code/storage-engines/waveSpan-backup** (branch `backup`). Commit per task. Build/regen: `make proto` (buf — regenerates Go gRPC + Connect + TS), `go build ./...`, `go test ./...`.

## Confirmed API reference (grounded against the code)
- **Add a proto service:** create `proto/wavespan/v1/backup.proto` (`service BackupService` + messages; `import "wavespan/v1/common.proto"` for `ResponseMeta`); run `make proto` (buf auto-discovers). Produces `backup.pb.go`, `backup_grpc.pb.go` (`RegisterBackupServiceServer`, `UnimplementedBackupServiceServer`), `wavespanv1connect/backup.connect.go` (`NewBackupServiceHandler`, `BackupServiceName`), and `ui/src/gen/.../backup_pb.ts`.
- **Impl once, two transports** (mirror `BudgetService`): impl the Connect-signature methods on `*collections.Service` (internal/collections/, e.g. new `backup_service.go`), e.g. `func (s *Service) BeginBackup(ctx, req *connect.Request[pb.BeginBackupRequest]) (*connect.Response[pb.BeginBackupResult], error)`. Add `func (s *Service) BackupHandler() (string, http.Handler) { return wavespanv1connect.NewBackupServiceHandler(s, rpcopts.Handler()...) }`. Create `internal/grpcsrv/backup.go` `Backup`/`NewBackup(svc)` delegating each method via `connect.NewRequest(m)` → method → `connectToGRPC(err)` → `res.Msg` (mirror `internal/grpcsrv/budget.go`). Register in `cmd/wavespan-node/main.go`: `wavespanv1.RegisterBackupServiceServer(grpcDataSrv, grpcsrv.NewBackup(collectionsSvc))` (~line 714) + `bckPath, bckHandler := collectionsSvc.BackupHandler(); adminMux.Handle(bckPath, adminIdentity.EnforceHTTP(bckHandler))` (~line 798).
- **Access to the engine:** `*collections.Service` holds `cols *Collections`, `self membership.Member`, `tier *tierStatus` (carries `*Manager`). Add a `WithBackup(coord *backup.Coordinator)` option mirroring `WithTierStatus`. The `*Manager` is the access point for all shards + the meta shard (`MetaShardID`), `proposeRaw` (manager.go:238), and leadership (`GetLeaderID`). Roster via the gossip `svc`/`peersFn` already used by `NewRPCForwarder(peersFn)`.
- **Meta-shard commands:** `metaCommand{Op,Start,End,ShardID}` + `encode/decodeMetaCommand` + dispatch in `metaSM.Update` (internal/collections/meta.go:19-99). The fixed-field codec won't hold a rich `BackupIntent` → store the intent as a **serialized blob value** under a new meta key sub-space (a new `metaOp` `opBackupPut`/`opBackupDelete` whose payload is `(key, blobBytes)`), applied into `CFReplData` under the meta shard's prefix in a reserved sub-space. Reads via the meta shard's `Lookup`.
- **Object store:** `objstore.NewS3(objstore.S3Config{Endpoint,Bucket,Prefix,AccessKey,SecretKey,Region,UseSSL,UsePathStyle}) (*S3, error)` and `objstore.NewFS(dir) (*FS, error)` — both satisfy the local `backup.ObjectStore` (Phase 2a). Use FS in tests.
- **Phase 2 building blocks (in-package `internal/backup`):** `ExportLogical(src storage.LocalStore, store ObjectStore, keyPrefix string, reg *Registry, captureMs int64, sel Selector)`, `DefaultRegistry()`, `Selector`, `NodeManifest`. Phase 1 (engine): `CheckpointToObjectStore`.
- **gRPC server obj:** `grpcDataSrv` built by `grpcsrv.New(...)` (~main.go:556). `rpcopts.Handler()` supplies shared Connect handler opts. `connectToGRPC` (grpcsrv/errcode.go), `collErr` (collections/service.go:85).

## File structure
- `proto/wavespan/v1/backup.proto` — `BackupService` + messages (`BackupSpec`, `BeginBackupResult`, `BackupState`, `BackupSummary`, `Destination`, `BackupStatusRequest`, `ListBackupsRequest`).
- `internal/backup/intent.go` — `BackupIntent` struct + (de)serialize (protobuf or gob blob) + meta-shard read/write helpers (via `*Manager`).
- `internal/backup/coordinator.go` — `Coordinator` (phases, assignment, commit).
- `internal/backup/agent.go` — node-side prepare/export executor.
- `internal/backup/clustermanifest.go` — `cluster.manifest` schema + write/read.
- `internal/collections/meta.go` — new `opBackupPut/opBackupDelete` metaOps + apply + Lookup.
- `internal/collections/backup_service.go` — the 4 Connect methods on `*Service` + `BackupHandler()` + `WithBackup`.
- `internal/grpcsrv/backup.go` — gRPC adapter.
- `cmd/wavespan-node/main.go` — construct `Coordinator`/agent, wire `WithBackup`, register service.

---

## Task 1: `backup.proto` + codegen + dual-transport skeleton
**Files:** Create `proto/wavespan/v1/backup.proto`; create `internal/collections/backup_service.go`, `internal/grpcsrv/backup.go`; modify `cmd/wavespan-node/main.go`.

- [ ] **Step 1:** Write `backup.proto`: `service BackupService { rpc BeginBackup(BeginBackupRequest) returns (BeginBackupResult); rpc BackupStatus(BackupStatusRequest) returns (BackupState); rpc ListBackups(ListBackupsRequest) returns (ListBackupsResult); rpc DeleteBackup(DeleteBackupRequest) returns (DeleteBackupResult); }` with messages: `BackupSpec{ Selection selection; repeated Plane planes; string parent; Destination destination; }`, `Destination{ string name; string bucket; string prefix; string region; string endpoint; bool use_ssl; bool use_path_style; CredentialRef credential; }` (no raw secrets here beyond an explicit-override sub-message used transiently — see 3e), `BackupState{ string backup_id; Status status; Phase phase; repeated NodeProgress per_node; double overall_pct; repeated string gaps; int64 started_ms; int64 finished_ms; string parent; Destination destination; }`, `NodeProgress{ string member_id; Phase phase; int64 objects; int64 bytes; bool done; }`, enums `Status{RUNNING,COMPLETE,PARTIAL,FAILED}`, `Phase{ASSIGN,PREPARE,EXPORT,COMMIT}`, `Plane{LOGICAL,PHYSICAL}`. Add internal node RPCs `PrepareBackup`/`ExportBackup` (can be a second service `BackupNodeService` on the data port, or methods on the same service guarded as internal). Each result carries `ResponseMeta meta`.
- [ ] **Step 2:** Run `make proto`; `go build ./...` → expect the new generated stubs to compile (no impl yet → `UnimplementedBackupServiceServer` satisfies gRPC).
- [ ] **Step 3:** Stub the 4 Connect methods on `*Service` in `backup_service.go` returning `connect.NewError(connect.CodeUnimplemented, …)` for now; add `BackupHandler()`. Add `grpcsrv/backup.go` delegating. Register both in main.go.
- [ ] **Step 4:** `go build ./...` + a smoke test hitting `ListBackups` over the Connect handler returns Unimplemented (proves wiring). 
- [ ] **Step 5:** Commit `feat(backup): BackupService proto + dual-transport skeleton (gRPC + Connect)`.

## Task 2: `BackupIntent` in the meta shard (durable catalog)
**Files:** `internal/backup/intent.go`; `internal/collections/meta.go`; tests `intent_test.go`, `meta_backup_test.go`.

- [ ] **Step 1 (failing test):** `intent_test.go` — `BackupIntent{BackupID, FrontierT, CaptureWallClockMs, Selection, Planes, Parent, Destination(descriptor), Status, Phase, LeaseDeadlineMs, RetainUntilMs, PerNode}` round-trips through `MarshalIntent`/`UnmarshalIntent` (protobuf or gob); unknown future fields tolerated.
- [ ] **Step 2:** implement the codec.
- [ ] **Step 3 (failing test):** `meta_backup_test.go` — propose `opBackupPut{key, blob}` to a meta SM and read it back via `Lookup`; `opBackupDelete` removes it; list-by-prefix returns all intents. Use the existing meta SM test harness.
- [ ] **Step 4:** add `opBackupPut`/`opBackupDelete` metaOps + encode/decode + apply (write/delete a blob under a reserved meta sub-space in `CFReplData`) + a `Lookup` query (`getIntent(backupID)`, `listIntents()`). Add `internal/backup/intent.go` helpers `PutIntent(mgr, intent)` / `GetIntent(mgr, id)` / `ListIntents(mgr)` that propose/lookup via the `*Manager` meta-shard path (`proposeRaw`/`SyncRead` on `MetaShardID`).
- [ ] **Step 5:** tests pass; commit `feat(backup): durable BackupIntent in the meta shard (opBackupPut/Delete + Lookup)`.

## Task 3: Node agent (prepare + export one node's assignment)
**Files:** `internal/backup/agent.go`; test `agent_test.go`.

- [ ] **Step 1 (failing test):** build a `storage.LocalStore` (memstore) with KV+collections data; `agent.Export(ctx, store, objStore, backupID, assignment, frontierT)` writes per-CF objects + a per-node sub-manifest to an FS object store; assert objects present and the sub-manifest lists counts. (Assignment for the single node = all its ranges.)
- [ ] **Step 2:** implement `agent.go`: `Prepare` (advance HLC past `T` via the node's clock, pin `LocalStore.Snapshot()`, return `{globalSeq, heldRangeSummary}`) and `Export` (run `ExportLogical(snap-backed store, objStore, prefix, DefaultRegistry(), captureMs, sel)` filtered to the assignment + `CheckpointToObjectStore` if physical plane; write the per-node sub-manifest; report progress via a callback). Reuse Phase 2 `ExportLogical`.
- [ ] **Step 3:** test passes; commit `feat(backup): node agent — prepare (seal T) + export assignment to object store`.

## Task 4: Coordinator phases (begin/assign/prepare/export/commit) + PARTIAL
**Files:** `internal/backup/coordinator.go`, `internal/backup/clustermanifest.go`; tests with a fake multi-node harness (in-process fakes for the node RPC + roster).

- [ ] **Step 1 (failing test):** with a fake roster of 3 nodes + fake per-node agents (in-process), `Coordinator.BeginBackup(spec)` returns a backupID; drives assign→prepare→export→commit; writes a `cluster.manifest` to the FS object store referencing each node's sub-manifest; `BackupStatus(id)` reports `COMPLETE`. A node missing a range with no live holder → `PARTIAL` + gap listed.
- [ ] **Step 2:** implement `coordinator.go`: `BeginBackup` (allocate id + `T = clock.now()+lease` bounded by the HLC skew cap; persist `BackupIntent` via Task 2; handle `Clock.Update` SkewError by retrying with a nearer `T`); `assign` (ownership from holder-directory + placement + `Roster.Live()`; collections shard → raft leader; no-live-owner → all-export fallback; physical → every live node); `prepare`/`export` (iterate `Roster.Live()`, call each node's `PrepareBackup`/`ExportBackup` via the `BackupService` client — NOT `Fanout`, which is a replica-fill worker; collect acks; renew intent lease on progress); `commit` (coverage cross-check vs held-range summaries → `cluster.manifest` + `Status=COMPLETE`, else `PARTIAL` with enumerated gaps; never silent). `clustermanifest.go`: schema (`{formatVersion, backupID, frontierT, captureWallClockMs, planes, parent, sourceTopology[]{memberID,storageUUID}, namespaceInventory, perNode[]{ref,counts}, status, gaps, perObjectSha256}`) + write/read.
- [ ] **Step 3:** wire `BeginBackup`/`BackupStatus`/`ListBackups` Connect methods (Task 1 stubs) to the coordinator + intent store. Construct the `Coordinator` in main.go (give it the `*Manager`, objstore config, roster/`peersFn`, the `BackupService` client factory) and attach via `WithBackup`.
- [ ] **Step 4:** tests pass; `go build ./... && go test ./internal/backup/... ./internal/collections/... && go vet ./...`. Commit `feat(backup): coordinator phases + cluster.manifest + PARTIAL detection`.

## Task 5: Resumability
**Files:** `internal/backup/coordinator.go`; test.

- [ ] **Step 1 (failing test):** start a backup, simulate coordinator loss after `prepare` (drop the in-memory coordinator but keep the meta-shard intent), have another node `resume(backupID)` from the intent and complete it; objects are idempotent (content-addressed), so re-export is safe; final `COMPLETE`.
- [ ] **Step 2:** implement `resume(backupID)` reading the intent + already-written sub-manifests, continuing from the recorded phase. (Lease-expiry→`FAILED` sweep is Phase 3d.)
- [ ] **Step 3:** test passes; commit `feat(backup): resumable coordinator from the meta-shard intent`.

## Done criteria (3a)
- [ ] `BackupService` served gRPC (data) + Connect (admin); `make proto` clean; `go build ./...` green.
- [ ] Durable `BackupIntent` in the meta shard (put/get/list/delete blob); coordinator drives a full cluster backup to an object store with a `cluster.manifest`; `PARTIAL` on uncovered ranges; resumable.
- [ ] `go test ./internal/backup/... ./internal/collections/...` green; `go vet ./...` clean. No hot-path change.

Builds on: Phase 1 (`CheckpointToObjectStore`), Phase 2a/b/c (`ExportLogical`, `Selector`, `DefaultRegistry`, `RerouteSuffix`). Unblocks: 3b/3c/3d/3e/3f.
