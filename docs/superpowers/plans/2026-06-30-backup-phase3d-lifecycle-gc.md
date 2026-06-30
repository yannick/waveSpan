# Backup Phase 3d — lifecycle & GC (no trash) — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** No durable backup artifact becomes trash: in-progress `BackupIntent`s lease-expire to `FAILED`, terminal intents are deleted after `retainUntilMs`, `DeleteBackup` removes an intent + its S3 objects (chain-aware), and an orphan-reconciliation pass cleans failed-export debris.

**Architecture:** A leader-driven sweep over the meta-shard backup intents (a genuine **mirror** of the budget/TTL sweep — NOT reuse: that machinery is data-shard-bound; this is net-new meta-side code). `DeleteBackup` is a `BackupService` RPC. S3 GC reconciles objects against live intents.

**Tech Stack:** Go; meta-shard SM (sweep + due-index); `BackupService.DeleteBackup`; `wavesdb/objstore` (List/Delete); 3a/3b (intents, chains). Spec §6.

## Where to work
`waveSpan-backup` / `backup`. Depends on **3a** (intent, BackupService, coordinator) and **3b** (`ResolveChain`). Tests: `go test ./internal/backup/... ./internal/collections/...`.

## Confirmed API / facts (grounded)
- Budget/TTL sweep is `Manager.sweepOnce` (manager.go:358) on a ticker, **leader-gated** via `GetLeaderID` (manager.go:370), filtered to **data shards** (`r.isData`; meta is `isData=false`) with helpers on `*shardSM`. So a backup-intent sweep needs **net-new meta-side** code: a due-index under the meta shard, a `Lookup` to scan due intents, an `Update` apply case, AND either extend `sweepOnce`'s filter to include the meta shard or run a separate meta sweep ticker. Leader-gating + the meta propose path (`proposeRaw` for `MetaShardID`) already work.
- `BackupIntent` (3a) carries `Status`, `LeaseDeadlineMs`, `RetainUntilMs`. Intent put/get/list/delete via 3a's meta-shard helpers.
- objstore: `List(prefix)`, `Delete(key)`, `Exists(key)` on the `ObjectStore` interface.

## File structure
- `internal/collections/meta.go` (extend) — a backup-intent **due-index** (keyed by `min(leaseDeadlineMs, retainUntilMs)`), an `opBackupSweep` apply (or reuse `opBackupPut`/`Delete` with computed transitions), and a meta-side `sweepBackupIntents(nowMs)` proposed by the leader.
- `internal/collections/manager.go` (extend) — start a meta sweep ticker (leader-gated), or extend `sweepOnce` to also sweep the meta shard's intents.
- `internal/backup/gc.go` — `DeleteBackup` impl (chain-aware) + S3 orphan reconciliation.
- `internal/backup/backup_service.go` (extend) — `DeleteBackup` Connect method → `gc.DeleteBackup`.

---

## Task 1: Intent lease-expiry + retention sweep (meta-side)
- [ ] **Failing test** (`meta_backup_sweep_test.go`): a `RUNNING` intent with `LeaseDeadlineMs` in the past → after `sweepBackupIntents(now)` it is `FAILED`. A terminal (`COMPLETE`) intent with `RetainUntilMs` in the past → swept (deleted). Intents not yet due are untouched. Idempotent (second sweep no-ops). Use the meta SM test harness.
- [ ] **Implement:** a due-index `{dueMs → backupID}` under the meta shard maintained on intent put/update; `sweepBackupIntents(nowMs)` scans due entries: `RUNNING` past lease → set `FAILED` (+ set `RetainUntilMs`); terminal past `RetainUntilMs` → delete intent (and enqueue its objects for GC — Task 3). All transitions are raft-proposed by the **leader** (mirror `sweepOnce`'s leader gate). Wire a leader-gated meta sweep ticker in `manager.go`.
- [ ] Run → PASS. Commit `feat(backup): meta-shard intent sweep — lease-expiry→FAILED + retention deletion`.

## Task 2: `DeleteBackup` (chain-aware)
- [ ] **Failing test** (`gc_test.go`): `B0`(full) ← `B1`(inc). `DeleteBackup(B0, force=false)` → error (live child `B1` depends on it). `DeleteBackup(B1, false)` → removes `B1`'s intent + its S3 objects (only `B1`'s delta objects, not `B0`'s). `DeleteBackup(B0, force=true)` after `B1` gone → removes `B0`. Objects gone from the FS object store; intents gone from the meta shard.
- [ ] **Implement** `gc.DeleteBackup(mgr, objStore, backupID, force)`: load chain (3b `ResolveChain` + reverse lookup for children); refuse if a live child exists unless `force` (which cascades children→base); list the backup's objects under its prefix and `Delete` them (respecting shared base objects in a chain — only delete objects this backup uniquely added, tracked via its per-node sub-manifests); delete the intent. Wire the `DeleteBackup` Connect method to it.
- [ ] Run → PASS. Commit `feat(backup): DeleteBackup — chain-aware intent + object removal`.

## Task 3: S3 orphan reconciliation
- [ ] **Failing test:** seed the object store with a completed backup's objects + some **orphan** objects under the cluster prefix not referenced by any live intent (simulate a failed export). `ReconcileOrphans(mgr, objStore, clusterPrefix)` deletes only the orphans, leaving live-backup objects intact.
- [ ] **Implement** `gc.ReconcileOrphans`: `List(clusterPrefix)`, build the live-object set from all live intents' `cluster.manifest`s + per-node sub-manifests, `Delete` objects not in the live set. Run it from the meta sweep ticker periodically (leader-gated) and after a `FAILED` transition.
- [ ] Run → PASS. Commit `feat(backup): S3 orphan reconciliation (delete unreferenced objects)`.

## Done criteria (3d)
- [ ] In-progress intents lease-expire to `FAILED`; terminal intents deleted after retention; `DeleteBackup` removes intent + objects chain-aware (refuse/ cascade); orphan objects reconciled. All leader-gated, raft-durable.
- [ ] `go test ./internal/backup/... ./internal/collections/...` green; vet+build clean. No durable artifact lingers unbounded.
