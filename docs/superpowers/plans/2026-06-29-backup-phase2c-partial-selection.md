# Backup Phase 2c — partial selection — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a logical backup export only a chosen subset of the data — by **namespace** (KV + collections), **graph name** (graph), and **vector collection** (vector) — so a single tenant or dataset can be backed up or moved on its own. An empty selector means "everything" (full backup, today's behavior).

**Architecture:** Partial selection is purely an **export-time filter**; restore is unchanged (it restores whatever objects exist in the backup). A `Selector` carries three independent sets (`Namespaces`, `Graphs`, `VectorCollections`). When the selector is non-empty, `ExportLogical` consults the owning `Contributor` for each key to decide inclusion; each contributor decodes its own CF keys (KV → namespace, collections → namespace via the re-shard parser, graph → graph name, vector → collection) using small decoders added in the owning packages. Semantics: an empty selector → full backup; a non-empty selector includes **only** matching keys per type (a type whose selector set is empty exports nothing), except `CFSys` (cluster config/identity) which is always included.

**Tech Stack:** Go (`github.com/yannick/wavespan`), `internal/backup`, `internal/recordstore`, `internal/collections`, `internal/graph`, `internal/vector`. All use uvarint length-prefixed keys (`binary.PutUvarint`/`Uvarint`). Spec: `docs/superpowers/specs/2026-06-27-snapshot-backups-design.md` (§7 "Partial").

**Phase roadmap:** Phase 1 ✅, 2a ✅ (logical core), 2b ✅ (re-shard). **Phase 2c (this doc)** = partial selection. Phase 3 = cluster coordinator + incrementals + physical plane. Phase 4 = operator + CLI.

## Where to work
Worktree **/Volumes/HOME/code/storage-engines/waveSpan-backup** (branch `backup`). Commit per task. Tests: `go test ./internal/backup/... ./internal/recordstore/... ./internal/collections/... ./internal/graph/... ./internal/vector/...`.

## Confirmed API reference
- KV CFKVData (`dataKey`) + CFKVMeta (`latestKey`) START with `lenPrefix(ns)` = `uvarint(len(ns))||ns` (recordstore/encode.go:22,34,47). BUT CFKVMeta ALSO holds TTL-sentinel keys: `0xff||bucketStart(8B)||lenPrefix(ns)||userKey` (`ttlSentinel`=0xff, encode.go:54-66); decode ns via `parseTTLKey` (encode.go:87). recordstore decodes uvarint at encode.go:92.
- Collections CFReplData key = `be8(shardID)||subPrefix||…`; `(ns,coll)` is recoverable from data sub-prefixes via the Phase 2b parsing (`takeChunk`→ns,coll for subData=suffix[1:] and subTTL/subBudExp/subBudTombGC=suffix[9:]); meta/bookkeeping rows (subMeta/subDedup/subDedupRing) and shard id `< FirstDataShard` have no ns. (Do not reuse `routeKeyOf` — incomplete; factor `nsCollOfSuffix` from `RerouteSuffix`.)
- Graph CFGraphData + CFGraphIndex: graph name = the FIRST `lp` chunk after the prefix. Prefixes `pfxNode="n"`,`pfxEdge="e"`,`pfxLabel="l"`,`pfxProp="p"` are 1 byte; `pfxOutAdj="ao"`,`pfxInAdj="ai"` are 2 bytes (keys.go:16-21). `lp(dst,s)=dst||uvarint(len(s))||s` (keys.go:24), uvarint decode keys.go:44. e.g. `LabelKey=lp(lp(lp("l",graph),label),nodeID)` → after "l", first chunk=graph.
- Vector CFVectorRaw + CFVectorIndex: ALL prefixes are 2 bytes — `pfxRaw="vr"`,`pfxMeta="vm"`,`pfxAttach="va"` (store.go:13-15). `rawKey=lp(lp("vr",coll),vec)`, `metaKey=lp(lp("vm",coll),vec)` → collection = 1st chunk after the 2-byte prefix. `attachKey=lp(lp(lp("va",nodeID),coll),vec)` → collection = **2nd** chunk after "va". `lp`+uvarint decode store.go:18,26.
- Backup: `ExportLogical(src, store, keyPrefix, reg, captureMs)`; `Registry`/`funcContributor{name, cfs, rebuild}`; `cf.Name()`; CFs iterated via `reg.AuthoritativeCFs()` over `src.Snapshot()`.

---

## Task 1: Per-package key decoders (selector value ← CF key)

Add an exported decoder in each owning package. **CRITICAL — each CF holds MULTIPLE key prefixes; the decoder must handle EVERY prefix in the CF(s) the contributor owns**, because the index CFs are exported verbatim (authoritative) and must be filterable consistently. The selector entity is always recoverable (indexes are per-graph/per-collection scoped). All packages use `lp(dst,s)=dst||uvarint(len(s))||s` and have a uvarint decoder. Every decoder is bounds-checked and returns `("",false)` on a short/garbage/unattributable key — never panics.

@superpowers:test-driven-development

**Files / signatures:**
- `internal/recordstore` (encode.go or new keys.go): `func NamespaceOfKey(key []byte) (string, bool)` covering BOTH KV CFs' key types:
  - `0xff` TTL-sentinel keys (`ttlSentinel`, encode.go:60) → reuse `parseTTLKey` (encode.go:87) to get ns.
  - otherwise (latest-pointer `latestKey` and versioned `dataKey`) → read the leading `lenPrefix` chunk → ns.
  - Test: `dataKey("ns","k",v)`, `latestKey("ns","k")`, AND `ttlKey(t,"ns","k")` all decode to `"ns"`.
- `internal/collections/reshard.go` (refactor): factor the suffix→(ns,coll) parse out of `RerouteSuffix` into `nsCollOfSuffix(suffix) (ns, coll []byte, ok bool)` (do NOT reuse migrate.go `routeKeyOf` — it only covers subData/subTTL and returns combined bytes); add `func NamespaceCollectionOfKey(key []byte) (ns, coll string, ok bool)` → `ok=false` if `len(key)<8`, shardID `< FirstDataShard`, or a bookkeeping sub-prefix (subMeta/subDedup/subDedupRing); else parse ns,coll from subData/subTTL/subBudExp/subBudTombGC bodies. `RerouteSuffix` then reuses `nsCollOfSuffix`. Test: `be8(ShardForKey("ns","c",4))||subData||chunk(ns)||chunk(coll)` → `("ns","c",true)`; meta-shard/subMeta → `ok=false`.
- `internal/graph/keys.go`: `func GraphOfKey(key []byte) (string, bool)` covering ALL CFGraphData + CFGraphIndex prefixes. The graph name is the FIRST `lp` chunk after the prefix bytes; prefixes are `pfxNode/pfxEdge/pfxLabel/pfxProp` (1 byte: "n","e","l","p") and `pfxOutAdj/pfxInAdj` (2 bytes: "ao","ai"). Detect the 2-byte adjacency prefixes first, else the 1-byte ones; skip the prefix; read the uvarint chunk → graph. Test: `NodeKey/EdgeKey/LabelKey/PropKey/OutAdjKey/InAdjKey` for graph `"g"` all decode to `"g"`; unknown leading byte → `ok=false`.
- `internal/vector/store.go`: `func CollectionOfKey(key []byte) (string, bool)` covering CFVectorRaw + CFVectorIndex. **All vector prefixes are 2 bytes** (`pfxRaw="vr"`, `pfxMeta="vm"`, `pfxAttach="va"`). For `vr`/`vm`: collection = 1st chunk after the 2-byte prefix. For `va` (`attachKey = lp(lp(lp("va",nodeID),collection),vectorID)`): collection = **2nd** chunk after the 2-byte prefix (skip the nodeID chunk first). Test: `rawKey("coll","v")`, `metaKey("coll","v")`, AND `attachKey("node","coll","v")` all decode to `"coll"`.

- [ ] **Step 1–4 (per package): failing test → run fail → implement → run pass.** One decoder + test per package; reuse the package's uvarint decode helper.
- [ ] **Step 5: commit** — `git commit -am "feat(recordstore,collections,graph,vector): prefix-aware key decoders for partial-backup selection"`

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
  - Adding `Selects` to the `Contributor` interface also requires the test-only `staticContributor` (contributor_test.go) to implement it — add a trivial `Selects(...) bool { return true }` there so the package still compiles.

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

- [ ] **Step 1: failing test** — seed two tenants' worth of data across all four datatypes (ns1/ns2, g1/g2, c1/c2), building real graph + vector INDEXES (so CFGraphIndex/CFVectorIndex have entries). Partial-export selecting only ns1 + g1 + c1 to an FS object store. Restore into a fresh `dst`. Assert: (a) ns1 KV + ns1 collections + g1 graph + c1 vector data are present and correct in `dst`; ns2/g2/c2 data ABSENT; (b) **the copied indexes are consistent for the selected subset** — a g1 graph query that reads CFGraphIndex (e.g. label lookup / adjacency traversal) returns the seeded g1 node, and a c1 vector search returns the seeded c1 vector, while g2/c2 queries return nothing. This proves prefix-aware filtering kept index entries attributed to the selected entities (the index CFs are exported verbatim-but-filtered, no rebuild). (Restore is unchanged.)
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
