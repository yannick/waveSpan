# Backup Phase 3a.1 — HLC consistent cut (Version ≤ T) — Implementation Plan

> **For agentic workers:** REQUIRED: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** A full logical backup seals the **KV** tier to a cluster-wide HLC frontier `T` (each KV record with `Version ≤ T` included, newer excluded), so all nodes capture a consistent instant for KV. Graph/vector are captured at the export snapshot (single-slot, no history — documented limitation, NOT strict ≤T, to avoid data loss). Collections stays raft-consistent (per-shard applied-index, orthogonal to the HLC cut).

**Architecture:** `T = FrontierT` (already `now + frontierLeaseMs`, recorded in the cluster manifest) becomes a **comparison ceiling**: a record passes iff `Version.HLCPhysicalMs ≤ T`. The cut is realized by (a) a per-CF `VersionOf` extractor on the Contributor, (b) a `≤T` skip in `ExportLogical` applied to **CFKVData only**, and (c) making **CFKVMeta a derived CF** rebuilt on restore from the surviving `≤T` CFKVData (else the latest-pointer dangles → silent key loss). No HLC-clock advance and no write-barrier/drain are used (none exists; not needed — the `≤T` filter over each node's consistent snapshot + the all-export union is sufficient).

**Tech Stack:** Go. `internal/backup` (contributor, logical_export, logical_restore, contributors, agent/coordinator), `internal/recordstore`/`internal/storage` (version decode + `RebuildLatestPointer`), `internal/version`.

## Grounded facts (from exploration, with citations)
- KV version is in the CFKVData **value** (`StoredRecord.Version`) — decode via `storage.DecodeStoredRecord(value)` then `version.FromProto(rec.GetVersion())` (`internal/storage/envelope.go:40`, `internal/version/version.go:89`). (Also a key suffix `recordstore/encode.go:103`, but no decoder — use the value.)
- CFKVData is **multi-version** (version suffix per key, `recordstore/encode.go:33`) — older `≤T` versions are retained, so a correct latest-as-of-T is achievable.
- CFKVMeta holds a `LatestPointer{Winner Version, Tombstone, ExpiresAtUnixMs, SiblingVersions}` per key (`recordstore/store.go:183`, `storage/envelope.go:49`); the read path resolves the value via `Get(CFKVData, dataKey(ns,key,Winner))` (`recordstore/store.go:596`). **Restore is verbatim, no LWW recompute** (`logical_restore.go:182`); `RebuildAfterRestore` is a no-op today (`contributor.go:26`).
- `storage.RebuildLatestPointer` (`storage/envelope.go:123`) already does the LWW winner+tombstone+expiry selection — currently unused; reuse it.
- Graph/vector are **single-slot** (overwrite, no version suffix): `graph.NodeKey` (`graph/keys.go:60`), `vector.rawKey` (`vector/store.go:46`). Version is a value field: `NodeRecord.Version` (`cypher.pb.go:348`), `EdgeRecord.Version` (`cypher.pb.go:435`), `VectorRecord.Version` (`vector.pb.go:35`).
- Collections (CFReplData) is raft/CP — selection by namespace/collection + shard routing, NOT version (`contributors.go:53`, `logical_restore.go:36`). Exclude from the cut.
- Contributor interface (`contributor.go:33`) has NO version hook today. `ExportLogical` scan loop at `logical_export.go:59-70`; `captureMs`/`frontierT` is passed but **unused as a filter** today.
- `FrontierT` is wall-clock ms (`coordinator.go:236`); compare against `Version.HLCPhysicalMs` (both physical ms). `version.Version.Compare` (`version/version.go:30`) for full ordering if a ceiling Version is built.

---

## Task 1: `VersionOf` on the Contributor + T-ceiling helper
**Files:** Modify `internal/backup/contributor.go`, `internal/backup/contributors.go`; test `internal/backup/contributor_version_test.go`.
- [ ] Failing test: `VersionOf` for the KV contributor decodes a `StoredRecord` value → its `version.Version`; returns `ok=false` for CFKVMeta. Graph/vector contributors decode their record value → version; collections + CFSys return `ok=false`.
- [ ] Add `VersionOf(cf storage.ColumnFamily, key, value []byte) (version.Version, bool)` to the `Contributor` interface. Implement per contributor (KV: `storage.DecodeStoredRecord`; graph: `graph.DecodeNode`/`DecodeEdge` by CF; vector: unmarshal `VectorRecord`; system/collections: return `_,false`). For derived/index CFs return `_,false`.
- [ ] Add a helper `versionLEQ(v version.Version, frontierMs int64) bool` → `v.HLCPhysicalMs <= frontierMs` (the ceiling test; same-ms included).
- [ ] `go test ./internal/backup/...`, `make lint`. Commit.

## Task 2: `≤T` filter in `ExportLogical` — KV only
**Files:** Modify `internal/backup/logical_export.go`; test `internal/backup/logical_cut_test.go`.
- [ ] Failing test: a store with key `k` having v1(`HLCPhysicalMs=100`) and v2(`HLCPhysicalMs=200`); `ExportLogical(..., frontierT=150, ...)` → the exported CFKVData stream contains v1 but NOT v2 (assert by decoding the emitted object). With `frontierT=0` or a far-future T, all versions exported (back-compat: `frontierT<=0` means "no cut", current behavior).
- [ ] In the scan loop, build the `owner` map unconditionally; after the `Selects` check, for **CFKVData only** (or: any CF where `VersionOf` returns ok AND the CF is the KV data CF), skip records with `!versionLEQ(ver, frontierT)`. Do NOT apply to graph/vector data CFs (snapshot-current per design) or to CFKVMeta (handled by Task 3). Guard: `frontierT <= 0` disables the cut.
- [ ] `go test`, `make lint`. Commit.

## Task 3: CFKVMeta → derived; rebuild on restore (the correctness-critical part)
**Files:** Modify `internal/backup/contributors.go` (mark CFKVMeta non-authoritative), `internal/backup/contributor.go`/`contributors.go` (KV `RebuildAfterRestore`); test `internal/backup/cut_roundtrip_test.go`.
- [ ] Failing test (THE crux): write key `k` v1(ms=100,"a") then v2(ms=200,"b") into a real recordstore; `ExportLogical(frontierT=150)` → restore into a fresh store → the restored store's `Get(k)` returns **"a"** (the ≤T winner), `found=true`, and there is **no dangling pointer** (no Get-miss). A second key written only after T (ms=300) is absent. (Use the recordstore Get path to prove the rebuilt CFKVMeta resolves correctly.)
- [ ] Mark CFKVMeta `Authoritative: false` in the KV contributor's `CFs()` so it is NOT exported (`AuthoritativeCFs()` already drives the export set, `logical_export.go:51`).
- [ ] Implement the KV contributor `RebuildAfterRestore(dst, ri)`: scan restored CFKVData grouped by `(ns,userKey)` (strip the version suffix to group), and for each group write a fresh CFKVMeta `LatestPointer` via `storage.RebuildLatestPointer` over the group's records; also rebuild the TTL index entries (the `0xff` CFKVMeta keys) from surviving records' `ExpiresAtUnixMs`. (All restored CFKVData is already `≤T`.)
- [ ] Verify the existing logical/reshard/partial round-trip tests still pass (they now exercise the rebuild path). `go test ./internal/backup/... ./internal/recordstore/...`, `make lint`. Commit.

## Task 4: Wire T cluster-wide + document tiers
**Files:** Modify `internal/backup/agent.go`/`coordinator.go` (pass `FrontierT` as the export ceiling), spec `docs/superpowers/specs/2026-06-30-backup-phase3-cluster-design.md` (update §5.2/§3).
- [ ] Confirm/ensure every node's `Export` → `ExportLogical` receives the cluster-wide `FrontierT` from the coordinator (the same `T` recorded in `cluster.manifest`), not a per-node value. Test: a multi-node (in-process) backup applies the same `T` on all nodes; a `>T` KV write on one node is excluded from the union.
- [ ] Update the spec: 3a.1 delivers a true `≤T` cut for **KV**; **graph/vector** are snapshot-current (single-slot, documented — not sealed to T); **collections** is raft/applied-index consistent. Remove the now-inaccurate "advance HLC + drain" language; note the cut is filter-over-snapshot + all-export union (no write-barrier).
- [ ] `go test`, `make lint` + `make test-race`. Commit.

## Task 5 (optional, smaller): commit-time held-range coverage → real PARTIAL
**Files:** `internal/backup/coordinator.go`, `agent.go` (PrepareLocal real held ranges).
- [ ] PrepareLocal returns the node's actual held ranges (range-directory derived); `commit()` cross-checks held-ranges-union vs assignment coverage → `PARTIAL` reflects real cluster gaps, not only the assigner's list. (Defer if Task 1–4 is the agreed 3a.1 scope; flag.)

## Done criteria (3a.1)
- [ ] KV: a backup with frontier `T` excludes `>T` versions and, after restore, presents the correct latest-as-of-T per key with no dangling pointers / lost keys (Task 3 crux test green).
- [ ] Graph/vector: captured at snapshot (no strict drop, no loss); collections: raft-consistent. Documented in the spec.
- [ ] `make lint` + `make test-race` green. The cut `T` is cluster-wide (same value on all nodes), recorded in `cluster.manifest`.
