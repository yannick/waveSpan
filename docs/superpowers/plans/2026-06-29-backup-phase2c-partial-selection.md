# Backup Phase 2c — partial selection — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a logical backup export only a chosen subset of the data — by **namespace** (KV + collections), **graph name** (graph), and **vector collection** (vector) — so a single tenant or dataset can be backed up or moved on its own. An empty selector means "everything" (full backup, today's behavior).

**Architecture:** Partial selection is purely an **export-time filter**; restore is unchanged (it restores whatever objects exist in the backup). A `Selector` carries three independent sets (`Namespaces`, `Graphs`, `VectorCollections`). When the selector is non-empty, `ExportLogical` consults the owning `Contributor` for each key to decide inclusion; each contributor decodes its own CF keys (KV → namespace, collections → namespace via the re-shard parser, graph → graph name, vector → collection) using small decoders added in the owning packages. Semantics: an empty selector → full backup; a non-empty selector includes **only** matching keys per type (a type whose selector set is empty exports nothing), except `CFSys` (cluster config/identity) which is always included.

**Tech Stack:** Go (`github.com/yannick/wavespan`), `internal/backup`, `internal/recordstore`, `internal/collections`, `internal/graph`, `internal/vector`. All use uvarint length-prefixed keys (`binary.PutUvarint`/`Uvarint`). Spec: `docs/superpowers/specs/2026-06-27-snapshot-backups-design.md` (§7 "Partial").

**Phase roadmap:** Phase 1 ✅, 2a ✅ (logical core), 2b ✅ (re-shard). **Phase 2c (this doc)** = partial selection. Phase 3 = cluster coordinator + incrementals + physical plane. Phase 4 = operator + CLI.

## Where to work
Worktree **/Volumes/HOME/code/storage-engines/waveSpan-backup** (branch `backup`). Commit per task. Tests: `go test ./internal/backup/... ./internal/recordstore/... ./internal/collections/... ./internal/graph/... ./internal/vector/...`.

## Confirmed API reference
- KV keys (CFKVData `dataKey`, CFKVMeta `latestKey`) both START with `lenPrefix(ns)` = `uvarint(len(ns))||ns` (recordstore/encode.go:22,34,47). recordstore already decodes a namespace via `binary.Uvarint` (encode.go:92).
- Collections CFReplData key = `be8(shardID)||subPrefix||…`; `(ns,coll)` is recoverable from the suffix for data rows via the Phase 2b parsing (`RerouteSuffix`/`routeKeyOf` in collections, which `takeChunk`→ns,coll for subData=suffix[1:] and subTTL/subBudExp/subBudTombGC=suffix[9:]; meta/bookkeeping rows have no ns). Shard id `< FirstDataShard` = meta/system.
- Graph CFGraphData keys: `NodePrefix(graph)=lp([]byte("n"),graph)` / `EdgePrefix=lp([]byte("e"),graph)` (graph/keys.go:52,55); `lp(dst,s)=dst||uvarint(len(s))||s` (keys.go:24); graph has a uvarint decode at keys.go:44. So a node/edge key = `pfx(1 byte: 'n'/'e')||uvarint(len(graph))||graph||…`.
- Vector CFVectorRaw keys: `rawPrefix(collection)=lp([]byte(pfxRaw),collection)` (store.go:56); `lp` + uvarint decode at store.go:18,26. Key = `pfxRaw||uvarint(len(coll))||coll||…`.
- Backup: `ExportLogical(src, store, keyPrefix, reg, captureMs)`; `Registry`/`funcContributor{name, cfs, rebuild}`; `cf.Name()`; CFs iterated via `reg.AuthoritativeCFs()` over `src.Snapshot()`.

---

## Task 1: Per-package key decoders (selector value ← CF key)

Add a small exported decoder in each owning package (they own the key layout + already have a uvarint decoder). Each returns `ok=false` for keys it can't attribute (so the matcher can decide).

@superpowers:test-driven-development

**Files / signatures:**
- `internal/recordstore/encode.go` (or a new `keys.go`): `func NamespaceOfKey(key []byte) (string, bool)` — read the leading `lenPrefix` chunk (uvarint len + bytes) of a CFKVData/CFKVMeta key. Test: `dataKey("ns","k",v)` and `latestKey("ns","k")` both decode to `"ns"`.
- `internal/collections/reshard.go` (refactor): factor the suffix→(ns,coll) parsing out of `RerouteSuffix` into `nsCollOfSuffix(suffix) (ns, coll []byte, ok bool)`, and add `func NamespaceCollectionOfKey(key []byte) (ns, coll string, ok bool)` that drops the 8-byte prefix (ok=false if `len<8` or shardID `< FirstDataShard` or a bookkeeping sub-prefix). `RerouteSuffix` then reuses `nsCollOfSuffix`. Test: a `be8(ShardForKey("ns","c",4))||subData||chunk(ns)||chunk(coll)` key decodes to `("ns","c",true)`; a meta-shard / subMeta key → `ok=false`.
- `internal/graph/keys.go`: `func GraphOfKey(key []byte) (string, bool)` — for a node/edge key, skip the 1-byte `pfxNode`/`pfxEdge`, read the uvarint graph chunk. Test: `NodeKey("g","n1")` and `EdgeKey("g","e1")` decode to `"g"`; a key with an unknown leading byte → `ok=false`.
- `internal/vector/store.go`: `func CollectionOfKey(key []byte) (string, bool)` — skip the 1-byte `pfxRaw`, read the uvarint collection chunk. Test: `rawKey("coll","v1")` decodes to `"coll"`.

- [ ] **Step 1–4 (per package): failing test → run fail → implement → run pass.** Do each package's decoder with its own test, reusing that package's existing uvarint decode helper. Keep each decoder defensive (bounds-checked; never panic on a short/garbage key — return `("", false)`).
- [ ] **Step 5: commit** — `git commit -am "feat(recordstore,collections,graph,vector): key decoders for partial-backup selection"`

---

## Task 2: `Selector` + contributor matchers

@superpowers:test-driven-development

**Files:**
- Create: `internal/backup/selector.go`
- Modify: `internal/backup/contributors.go` (add a `selects` matcher per contributor; wire in `DefaultRegistry`)
- Modify: `internal/backup/contributor.go` (extend `funcContributor` / `Contributor` with a selection method)
- Test: `internal/backup/selector_test.go`

- [ ] **Step 1: failing test** —
```go
sel := backup.Selector{
	Namespaces:        backup.Set("ns1"),
	Graphs:            backup.Set("g1"),
	VectorCollections: backup.Set("c1"),
}
// KV key in ns1 included; ns2 excluded.
// collections key (ns1,*) included; (ns2,*) excluded.
// graph key for g1 included; g2 excluded.
// vector key for c1 included; c2 excluded.
// CFSys key always included.
// Empty selector ⇒ Selector.IsEmpty() true.
```
Drive these through the contributor matchers obtained from `DefaultRegistry()`.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** —
  - `Selector{Namespaces, Graphs, VectorCollections map[string]struct{}}`; `Set(...string) map[string]struct{}` helper; `IsEmpty()` = all three empty.
  - Add to the `Contributor` interface (and `funcContributor`) a method `Selects(cf storage.ColumnFamily, key []byte, sel Selector) bool`. Provide it per contributor via a new `selects` closure field on `funcContributor` (default nil ⇒ always true):
    - **system**: always `true` (CFSys is cluster config/identity; always backed up).
    - **kv**: `ns, ok := recordstore.NamespaceOfKey(key); return ok && contains(sel.Namespaces, ns)`.
    - **collections**: `ns, _, ok := collections.NamespaceCollectionOfKey(key); return ok && contains(sel.Namespaces, ns)` (bookkeeping/meta rows ⇒ ok=false ⇒ excluded from a partial namespace export; the target rebuilds them).
    - **graph**: `g, ok := graph.GraphOfKey(key); return ok && contains(sel.Graphs, g)`.
    - **vector**: `c, ok := vector.CollectionOfKey(key); return ok && contains(sel.VectorCollections, c)`.
  - Note: each type checks ITS OWN selector set, so a non-empty selector that names only namespaces naturally excludes all graph/vector data (and vice-versa). The "empty selector ⇒ everything" shortcut is handled in ExportLogical (Task 3), so matchers are only consulted when a filter is active.
  - Import-cycle check: `internal/backup` importing recordstore/collections/graph/vector — confirm none import backup (they don't). If any cycle, define a decode-func type and inject it instead.

- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** — `git commit -am "feat(backup): Selector + per-contributor key matchers for partial backup"`

---

## Task 3: `ExportLogical` applies the selector

@superpowers:test-driven-development

**Files:**
- Modify: `internal/backup/logical_export.go` (+ a selector parameter; keep a no-selector wrapper or variadic for existing callers)
- Test: extend `internal/backup/logical_export_test.go`

- [ ] **Step 1: failing test** — seed a store with KV in ns1+ns2, a collections row in ns1+ns2, graph g1+g2, vector c1+c2. `ExportLogical(..., Selector{Namespaces:Set("ns1"), Graphs:Set("g1"), VectorCollections:Set("c1")})`. Assert the manifest counts include only ns1/g1/c1 keys (ns2/g2/c2 absent), and CFSys present. Then assert an empty `Selector{}` exports everything (regression — same counts as a full export).

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — give `ExportLogical` a `sel Selector` argument (update existing call sites; the Phase 2a tests pass `Selector{}`). In the per-CF scan loop: if `sel.IsEmpty()` keep today's include-all behavior (fast path); else, for each key, find the contributor owning that CF and call `c.Selects(cf, key, sel)` — skip the key if false. (Owning contributor: build a `cf → contributor` map from the registry once. A CF is owned by exactly one contributor in `DefaultRegistry`.) Empty CFs (all keys filtered out) are omitted from the manifest, same as Phase 2a's empty-CF rule.

- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** — `git commit -am "feat(backup): ExportLogical honors a Selector (partial backup by namespace/graph/collection)"`

---

## Task 4: End-to-end partial round-trip

@superpowers:test-driven-development

**Files:**
- Test: `internal/backup/partial_roundtrip_test.go`

- [ ] **Step 1: failing test** — seed two tenants' worth of data across all four datatypes (ns1/ns2, g1/g2, c1/c2). Partial-export selecting only ns1 + g1 + c1 to an FS object store. Restore into a fresh `dst`. Assert: ns1 KV + ns1 collections + g1 graph + c1 vector are present and correct in `dst`; ns2/g2/c2 data is ABSENT. (Restore is unchanged — this validates the export filter end-to-end through a real restore.)
- [ ] **Step 2: run → FAIL (until Tasks 1–3).**
- [ ] **Step 3: make pass.**
- [ ] **Step 4:** `go test ./internal/backup/... ./internal/recordstore/... ./internal/collections/... ./internal/graph/... ./internal/vector/...` green; `go vet ./...` clean; `go build ./...` green.
- [ ] **Step 5: commit** — `git commit -am "test(backup): partial backup round-trip — one tenant's namespace/graph/collection extracted"`

---

## Done criteria for Phase 2c
- [ ] Key decoders (`NamespaceOfKey`, `NamespaceCollectionOfKey`, `GraphOfKey`, `CollectionOfKey`) added in their owning packages, defensive + tested.
- [ ] `Selector` + per-contributor matchers; `ExportLogical` filters when the selector is non-empty, full backup when empty (regression-guarded).
- [ ] Partial round-trip: one namespace + one graph + one vector collection extracted and restored; other tenants' data absent.
- [ ] All affected package tests green; vet + build clean. Restore code unchanged. No hot-path change.

Phase 2c completes single-node logical backup (full, re-shard, partial). Phase 3 lifts it to the cluster: the gossip/meta-shard coordinator, cluster-wide HLC-cut, owner assignment, incrementals, and the physical plane.
