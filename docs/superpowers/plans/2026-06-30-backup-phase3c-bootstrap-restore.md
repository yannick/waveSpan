# Backup Phase 3c — bootstrap-restore (DR / clone / re-shard) — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** A node, at startup, reconstitutes from an S3 backup **before serving** — physical same-shape DR, or logical clone/re-shard — so a cluster can be restored or forked from a backup. Supports forking any number of independent clones from one backup.

**Architecture:** A startup hook (env-gated `WAVESPAN_RESTORE_FROM`) runs immediately after the store opens and **before** `EnsureStorageUUID`, the collections tier bootstrap, and serving. It reads the `cluster.manifest`, then either: **physical** — match this node to a source node by stable identity, `RestoreFromObjectStore` its checkpoint chain into the data dir (KV/graph/vector raft-free); or **logical** — stream records back via `RestoreLogical`, re-routing collections under this cluster's N. **Collections always reset raft bookkeeping + re-bootstrap fresh** (spec §5.0) — the dragonboat LogDB is never carried.

**Tech Stack:** Go; `main.go` startup; Phase 1 `RestoreFromObjectStore`; Phase 2 `RestoreLogical` + `RerouteSuffix`; `objstore.NewS3`/`NewFS`; 3a `cluster.manifest`. Spec §5, §5.0, §5.1.

## Where to work
`waveSpan-backup` / `backup`. Depends on **3a** (cluster.manifest) and **3b** (chains, for physical). Tests: `go test ./internal/backup/... ./cmd/...`.

## Confirmed API / facts (grounded)
- **Startup window** (`cmd/wavespan-node/main.go`): store opens ~L117; `EnsureStorageUUID` ~L122; recordstore ~L175; collections bootstrap (`BootstrapWithPlacement`) ~L580-708; `Serve` ~L818-836. **Insert restore right after L117, before L122** (so a restored `sys`/identity is read, not minted) and well before collections bootstrap + serve. Mirror the `os.Getenv("WAVESPAN_COLLECTIONS_ENABLED") != "0"` env-gate style (L580). `cfg.Storage.Path`, `cfg.ClusterID`, `cfg.MemberID` available.
- **recordstore does not assume empty state** — restoring KV/CFReplData before serving is read live on demand. **Collections `baseSM.Open()` reads the applied index from `CFReplData`** → restored data is picked up by the bootstrap that follows, BUT a stale applied index vs a fresh LogDB diverges → reset bookkeeping (spec §5.0).
- Phase 1 `RestoreFromObjectStore(ctx, store, keyPrefix, destDir)` downloads a checkpoint (base+chain) into an openable dir. Phase 2 `RestoreLogical(dst, objStore, keyPrefix, reg, ri RestoreInfo)` with `ri.CollectionsDataShards` for re-shard and identity-exclusion.
- Logical restore already **skips `/sys/storage_uuid`** (node identity) and **drops `subMeta`/dedup on re-shard** (Phase 2a/2b). For bootstrap-restore, the reset must apply to collections on **every** path (same-shape too), per §5.0.

## File structure
- `internal/backup/restore_bootstrap.go` — the bootstrap entry: parse `WAVESPAN_RESTORE_FROM` + intent + shape; read `cluster.manifest`; dispatch physical vs logical; collections raft-bookkeeping reset.
- `cmd/wavespan-node/main.go` — call the bootstrap hook in the startup window.
- `internal/backup/restore_config.go` — restore config (backupID, intent DR|clone, target shape, source object store).

---

## Task 1: Restore config + manifest read
- [ ] **Failing test:** parse `WAVESPAN_RESTORE_FROM=s3://bucket/prefix/<backupID>` (+ `WAVESPAN_RESTORE_INTENT=dr|clone`, `WAVESPAN_RESTORE_SHARDS=<N>`) into a `RestoreConfig`; `LoadClusterManifest(objStore, backupID)` returns the topology + planes + namespace inventory + parent chain. Use an FS object store seeded by a prior export (reuse a 3a full-backup fixture).
- [ ] **Implement** `restore_config.go` + `LoadClusterManifest`. Commit `feat(backup): restore config + cluster.manifest read`.

## Task 2: Logical clone / re-shard restore (collections reset + re-bootstrap)
- [ ] **Failing test:** seed a source store (KV+collections+graph+vector at N=4), full **logical** export to FS objstore. Into a fresh dst store, `RestoreBootstrapLogical(dst, objStore, backupID, RestoreInfo{CollectionsDataShards: 8, Clone: true})`: assert KV/graph/vector restored; collections `CFReplData` data rows present under the **new** N=8 shard prefixes; **no `subMeta`/dedup rows** (raft bookkeeping reset); node identity (`/sys/storage_uuid`) NOT overwritten. (Re-uses Phase 2 `RestoreLogical` + `RerouteSuffix`.)
- [ ] **Implement** `RestoreBootstrapLogical` = `RestoreLogical(dst, …, ri)` with `ri.CollectionsDataShards` set (re-shard) and a post-pass (or rely on RerouteSuffix's drop) ensuring `subMeta`/`subDedup`/`subDedupRing` are absent for collections so the subsequent fresh `BootstrapN` sees applied-index 0. For same-shape clone (`CollectionsDataShards == sourceN`), still drop the collections raft-bookkeeping (do not restore `subMeta`) — add that to the CFReplData restore path when intent is bootstrap (§5.0). Commit `feat(backup): logical bootstrap-restore (clone/re-shard, collections raft reset)`.

## Task 3: Physical same-shape DR restore
- [ ] **Failing test:** seed source store; full **physical** checkpoint to FS objstore (+ a chain from 3b). Match a target node to the source node by stable identity (`MemberID`/`StorageUUID` from the manifest); `RestoreBootstrapPhysical(dataDir, objStore, backupID, memberID)` downloads the checkpoint chain into `dataDir`; `Open(dataDir)` yields all KV/graph/vector data. Collections: assert the CFReplData data is present but raft bookkeeping is reset (so the subsequent fresh collections bootstrap starts clean — §5.0).
- [ ] **Implement** `RestoreBootstrapPhysical` = resolve the chain (3b `ResolveChain`), `RestoreFromObjectStore` base+increments into the data dir for this node's matched source checkpoint, then strip collections `subMeta`/dedup from the restored `CFReplData` (so the fresh `collections-raft` LogDB + applied-index-0 are consistent). KV/graph/vector are raft-free → recovered as-is. Commit `feat(backup): physical same-shape DR bootstrap-restore (+ collections raft reset)`.

## Task 4: main.go startup wiring + intent guard
- [ ] **Implement:** in `main.go`, after store open (L117) and before `EnsureStorageUUID` (L122): `if src := os.Getenv("WAVESPAN_RESTORE_FROM"); src != "" { backup.RunBootstrapRestore(store, cfg, restoreCfgFromEnv()) }`. The restore selects physical vs logical per intent+shape (master spec §7: DR-same-shape→physical; clone/shape-change→logical). It must complete before collections bootstrap (L580). Add a one-shot guard so a node doesn't re-restore on every boot (e.g. a `sys` marker `/sys/restored_from` written after a successful restore; skip if present unless forced).
- [ ] **Test:** an integration-style test (or a `cmd` test) that sets the env, runs the startup restore against an FS objstore fixture, and asserts the store is populated + the marker written. Build + vet.
- [ ] Commit `feat(backup): wire bootstrap-restore into node startup (before serving)`.

## Task 5: Multi-clone verification
- [ ] **Failing test:** one logical backup → restore into **two** separate fresh dst stores, each with a distinct `ClusterID`/identity; assert both have the data, each kept its own `/sys/storage_uuid`, and neither mutated the backup objects (immutable). Proves §5.1 (fork N clones from one backup).
- [ ] **Implement:** nothing new expected (the immutability + identity-exclusion already give this) — the test locks it in. Commit `test(backup): fork multiple independent clones from one backup`.

## Done criteria (3c)
- [ ] Node startup restores from S3 before serving (env-gated, one-shot); logical clone/re-shard + physical same-shape DR both work; collections reset raft bookkeeping + re-bootstrap (§5.0); LogDB never carried; node identity preserved; N independent clones from one backup verified.
- [ ] `go test ./internal/backup/... ` green; vet+build clean.
