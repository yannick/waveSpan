# Backup Phase 3b — physical incrementals — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** Incremental physical backups: each node uploads only the SSTables changed since a parent backup, chained off a full physical backup, so repeated same-shape DR/PITR backups are cheap.

**Architecture:** Physical incrementals are per-node and seq-based (no owner-change problem — logical stays full-only). The node agent records its last-backup per-node `GlobalSeq` watermark in its sub-manifest; an incremental run passes the parent `CheckpointManifest` to Phase 1's `CheckpointToObjectStore`, which uploads only SSTable file-ids absent from the parent. The `cluster.manifest` records `parent`; restore (3c) walks base + chain.

**Tech Stack:** Go; Phase 1 `CheckpointToObjectStore(ctx, store, prefix, parent *CheckpointManifest)` + `SSTablesSince`; the 3a coordinator/agent/`BackupIntent`/`cluster.manifest`. Spec §4, §8.

## Where to work
`waveSpan-backup` / `backup`. Tests: `go test ./internal/backup/...`. Depends on **3a** (coordinator, agent, cluster.manifest, intent).

## Confirmed API
- `wavesdb` (engine, via `*SnapshotHandle`/checkpoint): `CheckpointToObjectStore(ctx, store, keyPrefix, parent)` returns a `*CheckpointManifest{GlobalSeq, NextFileID, Tables[]{CF,ID,MaxSeq,KlogSize,VlogSize}}`; incremental uploads only ids absent from `parent` (Phase 1, proven).
- 3a `cluster.manifest` already has a `parent` field + per-node sub-manifests; 3a node sub-manifest records per-node `GlobalSeq`.

## File structure
- `internal/backup/agent.go` (extend) — pass/return the per-node physical `CheckpointManifest`; record the per-node watermark.
- `internal/backup/coordinator.go` (extend) — accept `BackupSpec.parent`; resolve each node's parent `CheckpointManifest` from the parent backup's per-node sub-manifest; thread it into `ExportBackup`.
- `internal/backup/clustermanifest.go` (extend) — store per-node physical `CheckpointManifest` in the sub-manifest; `parent` chain pointer.

---

## Task 1: Agent carries the physical CheckpointManifest
- [ ] **Failing test** (`agent_incremental_test.go`): node has a store; full physical export → sub-manifest records a `CheckpointManifest` (N tables) + the node `GlobalSeq`. Write more + flush; incremental export with the prior `CheckpointManifest` as parent uploads exactly the new SSTable(s); the returned sub-manifest lists ALL tables (full set) and the new `GlobalSeq`.
- [ ] **Implement:** `agent.Export` gains an optional `parentCkpt *wavesdb.CheckpointManifest` (per CF / per node) and calls `CheckpointToObjectStore(ctx, objStore, prefix, parentCkpt)`; record the returned manifest + `GlobalSeq` in the per-node sub-manifest. Logical export is unchanged (full-only — no parent).
- [ ] Run → PASS. Commit `feat(backup): node agent physical incrementals (CheckpointToObjectStore parent diff)`.

## Task 2: Coordinator incremental orchestration
- [ ] **Failing test** (fake multi-node): a full physical backup `B0`; then `BeginBackup({planes:[PHYSICAL], parent:B0})` → each node resolves its parent `CheckpointManifest` from `B0`'s per-node sub-manifest and uploads only its delta; the new `cluster.manifest` records `parent=B0`; total uploaded objects < full.
- [ ] **Implement:** when `BackupSpec.parent != ""`, the coordinator loads the parent `cluster.manifest`, maps each live node to its parent per-node sub-manifest (by stable identity — `MemberID`/`StorageUUID`), and passes the parent `CheckpointManifest` to that node's `ExportBackup`. If a node has no parent entry (new node since `B0`) → it does a full physical export of its assignment (safe). Reject a logical-plane `parent` (logical is full-only) with a clear error.
- [ ] Run → PASS. Commit `feat(backup): coordinator physical-incremental orchestration (parent chain per node)`.

## Task 3: Chain integrity in the cluster.manifest
- [ ] **Failing test:** `B0` (full) → `B1` (inc, parent B0) → `B2` (inc, parent B1); each `cluster.manifest` records its parent; a `ResolveChain(B2)` helper returns `[B0,B1,B2]` in order; a broken/missing parent is a loud error.
- [ ] **Implement:** `clustermanifest.go` `ResolveChain(store, backupID) ([]string, error)` walking `parent` pointers; validate each link exists.
- [ ] Run → PASS. Commit `feat(backup): physical backup chain resolution (base → incrementals)`.

## Done criteria (3b)
- [ ] Per-node physical incrementals upload only changed SSTables; chains recorded in cluster.manifests and resolvable; logical remains full-only (incremental parent rejected for logical).
- [ ] `go test ./internal/backup/...` green; vet+build clean. Restore-side chain replay is Phase 3c.
