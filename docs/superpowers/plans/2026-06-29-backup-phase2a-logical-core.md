# Backup Phase 2a — logical backup/restore core — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single-node, datatype-agnostic **logical** backup/restore engine in waveSpan: stream a node's authoritative state to an object store (full backup) and restore it into a fresh same-shape store, with derived indexes rebuilt and node identity preserved — driven by a `Contributor` registry so new datatypes need zero backup-core changes.

**Architecture:** Reorganize the existing flat `internal/backup` codec into a **registry-driven** engine. Each subsystem registers a `Contributor` declaring its column families (authoritative vs derived) and a post-restore rebuild hook. Export iterates each authoritative CF over a consistent `LocalStore.Snapshot()` and streams `(cf,key,value)` chunks to an object store (reusing `wavesdb/objstore` — no new S3 client), writing a versioned manifest. Restore reads the manifest, raw-restores authoritative CFs (blind `(cf,key,value)` — invariant D), skips node-local identity, and invokes each contributor's rebuild hook. Same-shape only here; re-shard/partial = Phase 2b, cluster-coordination/incrementals = Phase 3.

**Tech Stack:** Go (module `github.com/yannick/wavespan`), `internal/storage` (`LocalStore`, `ColumnFamily` enum, per-CF `Scan`/`Snapshot`), `internal/recordstore`, `internal/graph`, `internal/vector`, `wavesdb/objstore` (FS backend for tests). Spec: `docs/superpowers/specs/2026-06-27-snapshot-backups-design.md` (§3 invariants, §5 contributors, §6 export, §7 restore, §8 manifest).

**Phase roadmap:** Phase 1 ✅ wavesdb primitives (`AcquireSnapshot`/`*SnapshotHandle`, `SSTablesSince`, `CheckpointToObjectStore`, `RestoreFromObjectStore`, branch `backup-primitives`). **Phase 2a (this doc)** = logical core, same-shape. Phase 2b = re-shard (collections re-route by `(ns,coll)` under new N) + partial (namespace/collection selection). Phase 3 = distributed coordination (`backup.proto`/`BackupService`, meta-shard `BackupIntent`, gossip coordinator, owner assignment, incrementals via `SSTablesSince` watermarks, physical-plane cluster integration). Phase 4 = operator CRD + `wavespanctl` CLI.

---

## Where to work

All work is in the existing worktree **/Volumes/HOME/code/storage-engines/waveSpan-backup** (branch `backup`, module `github.com/yannick/wavespan`), package `internal/backup` (extend it). The `backup` branch's go.mod already points wavesdb at `../wavesdb-backup` (Phase 1), so `wavesdb/objstore` resolves to the isolated engine. Commit per task. Run tests with `go test ./internal/backup/...`.

## Confirmed API reference (use these exact signatures)

- `storage.LocalStore`: `Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error)`, `Snapshot() (Snapshot, error)`, `Put/Get/Delete/Batch/BatchRC`. `Snapshot` has `Scan(cf, start, end, limit)` + `Close()`. `Iterator`: `Valid()/Next()/Key()/Value()/Err()/Close()`. (`internal/storage/store.go:73-96`)
- `storage.ColumnFamily` int enum: `CFSys, CFKVData, CFKVMeta, CFGraphData, CFGraphIndex, CFVectorRaw, CFVectorIndex, CFReplLog, CFCacheMeta, CFReplData` (`internal/storage/store.go:8-31`).
- `storage.OpenWavesdb(path) (*WavesdbStore, error)` (`wavesdb_store.go:141`) — satisfies `LocalStore`; creates all CFs. Test pattern: `s, _ := storage.OpenWavesdb(t.TempDir()); t.Cleanup(func(){ s.Close() })`.
- Existing codec to learn from / supersede: `backup.Backup(src LocalStore, w io.Writer, _ bool) (*Manifest, error)`, `backup.Restore(dst LocalStore, r io.Reader) (*Manifest, error)`; wire = repeating `uvarint(cf) || lenPrefix(key) || lenPrefix(value)`; restore batches 1000 via `Batch` (`internal/backup/backup.go:39`, `restore.go:26`). Helpers `putUvarint`/`writeBytes`/`readBytes` already exist there — reuse them.
- `storage.EnsureStorageUUID(LocalStore) (string, error)`; identity key const `storageUUIDKey = "/sys/storage_uuid"` (`internal/storage/identity.go:8`). This is node-local; exclude from restore.
- Rebuild hooks: `graph.(*Store).RebuildIndexes(graph string) error` (`internal/graph/index.go:97`); `vector.RebuildLiveIndex(store *vector.Store, collection string, metric vector.Metric, params ann.Params) (*vector.LiveIndex, error)` (`internal/vector/rebuild.go:8`).
- `wavesdb/objstore.NewFS(dir string) (*objstore.FS, error)` — FS object store for tests; satisfies the `ObjectStore` interface below.
- `prefixEnd(prefix []byte) []byte` helper exists in the codebase (used by collections); if not importable from `internal/backup`, add a local copy (it returns the smallest key greater than all keys with `prefix`).

---

## Task 1: ObjectStore interface + Contributor registry

@superpowers:test-driven-development

**Files:**
- Create: `internal/backup/objstore.go` (local `ObjectStore` interface — decouples backup from wavesdb)
- Create: `internal/backup/contributor.go` (`Contributor`, `CFSpec`, `RestoreInfo`, registry)
- Test: `internal/backup/contributor_test.go`

- [ ] **Step 1: Write the failing test**

```go
package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
)

func TestRegistryRegisterAndList(t *testing.T) {
	reg := NewRegistry()
	reg.Register(staticContributor{
		name: "demo",
		cfs:  []CFSpec{{CF: storage.CFKVData, Authoritative: true}, {CF: storage.CFGraphIndex, Authoritative: false}},
	})
	got := reg.Contributors()
	if len(got) != 1 || got[0].Name() != "demo" {
		t.Fatalf("want 1 contributor 'demo', got %+v", got)
	}
	// Authoritative CFs across the registry exclude the derived one.
	auth := reg.AuthoritativeCFs()
	if len(auth) != 1 || auth[0] != storage.CFKVData {
		t.Fatalf("want [CFKVData] authoritative, got %v", auth)
	}
}

// staticContributor is a test-only Contributor.
type staticContributor struct {
	name string
	cfs  []CFSpec
}

func (s staticContributor) Name() string    { return s.name }
func (s staticContributor) CFs() []CFSpec    { return s.cfs }
func (s staticContributor) RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error { return nil }
```

- [ ] **Step 2: Run to verify it fails** — `go test ./internal/backup/ -run TestRegistry -v` → FAIL (undefined types).

- [ ] **Step 3: Implement**

`internal/backup/objstore.go`:
```go
package backup

import "io"

// ObjectStore is the minimal object-storage surface the backup engine needs.
// wavesdb/objstore.FS and the S3 backend satisfy it structurally.
type ObjectStore interface {
	Put(key string, r io.Reader, size int64) error
	Get(key string) (io.ReadCloser, error)
	List(prefix string) ([]string, error)
	Exists(key string) (bool, error)
}
```

`internal/backup/contributor.go`:
```go
package backup

import "github.com/yannick/wavespan/internal/storage"

// CFSpec declares one column family a contributor owns and whether it is
// authoritative (backed up) or derived (skipped, rebuilt on restore).
type CFSpec struct {
	CF            storage.ColumnFamily
	Authoritative bool
}

// RestoreInfo is passed to rebuild hooks; it carries restore context so a
// datatype can reconcile (e.g. time-relative state). Grows in later phases.
type RestoreInfo struct {
	CaptureWallClockMs int64
	RestoreWallClockMs int64
	Clone              bool // new cluster identity (vs same-cluster DR)
}

// Contributor is how a subsystem participates in backup without the core
// knowing the datatype. New datatypes implement this and Register; the engine
// never names a datatype.
type Contributor interface {
	Name() string
	CFs() []CFSpec
	// RebuildAfterRestore rebuilds this contributor's derived indexes (and, in
	// later phases, reconciles time-relative state) after raw data is restored.
	RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error
}

// Registry holds the registered contributors.
type Registry struct{ contributors []Contributor }

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) Register(c Contributor) { r.contributors = append(r.contributors, c) }

func (r *Registry) Contributors() []Contributor { return r.contributors }

// AuthoritativeCFs returns the deduplicated set of authoritative CFs across all
// contributors, in CF order.
func (r *Registry) AuthoritativeCFs() []storage.ColumnFamily {
	seen := map[storage.ColumnFamily]bool{}
	var out []storage.ColumnFamily
	for _, c := range r.contributors {
		for _, s := range c.CFs() {
			if s.Authoritative && !seen[s.CF] {
				seen[s.CF] = true
				out = append(out, s.CF)
			}
		}
	}
	return out
}
```

- [ ] **Step 4: Run to verify it passes** — `go test ./internal/backup/ -run TestRegistry -v` → PASS.

- [ ] **Step 5: Commit** — `git add internal/backup/objstore.go internal/backup/contributor.go internal/backup/contributor_test.go && git commit -m "feat(backup): Contributor registry + ObjectStore interface (registry-driven, datatype-agnostic)"`

---

## Task 2: The five contributor registrations

Registers System, KV, Collections, Graph, Vector. Graph/Vector declare their index CFs derived; their rebuild hooks are wired in Task 6 (here they return nil — a TODO comment notes Task 6). System owns CFSys and is responsible for excluding node identity on restore (handled in restore, Task 5; here it just declares CFSys authoritative).

@superpowers:test-driven-development

**Files:**
- Create: `internal/backup/contributors.go`
- Test: `internal/backup/contributors_test.go`

- [ ] **Step 1: Write the failing test**

```go
package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
)

func TestDefaultRegistryCoverage(t *testing.T) {
	reg := DefaultRegistry()
	auth := map[storage.ColumnFamily]bool{}
	for _, cf := range reg.AuthoritativeCFs() {
		auth[cf] = true
	}
	// Authoritative data CFs must be covered.
	for _, cf := range []storage.ColumnFamily{
		storage.CFSys, storage.CFKVData, storage.CFKVMeta,
		storage.CFGraphData, storage.CFVectorRaw, storage.CFReplData,
	} {
		if !auth[cf] {
			t.Errorf("authoritative CF %v not covered by DefaultRegistry", cf)
		}
	}
	// Derived index CFs must NOT be authoritative (they are rebuilt).
	for _, cf := range []storage.ColumnFamily{storage.CFGraphIndex, storage.CFVectorIndex} {
		if auth[cf] {
			t.Errorf("derived CF %v must not be authoritative", cf)
		}
	}
	// Transient CFs are owned by nobody (never backed up).
	for _, cf := range []storage.ColumnFamily{storage.CFReplLog, storage.CFCacheMeta} {
		if auth[cf] {
			t.Errorf("transient CF %v must not be backed up", cf)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./internal/backup/ -run TestDefaultRegistry -v` → FAIL (`DefaultRegistry` undefined).

- [ ] **Step 3: Implement** `internal/backup/contributors.go`

Define one small struct per contributor (or reuse a generic `funcContributor`). Each `RebuildAfterRestore` returns nil for now EXCEPT graph/vector get a `// TODO(phase2a Task 6): rebuild indexes` placeholder. `DefaultRegistry()` registers all five:
- **System**: `CFSpec{CFSys, true}`.
- **KV**: `{CFKVData, true}`, `{CFKVMeta, true}`.
- **Collections**: `{CFReplData, true}`.
- **Graph**: `{CFGraphData, true}`, `{CFGraphIndex, false}` (derived).
- **Vector**: `{CFVectorRaw, true}`, `{CFVectorIndex, false}` (derived).

`CFReplLog`/`CFCacheMeta` are owned by no contributor → never exported. Use a small generic:
```go
type funcContributor struct {
	name    string
	cfs     []CFSpec
	rebuild func(dst storage.LocalStore, ri RestoreInfo) error
}
func (f funcContributor) Name() string { return f.name }
func (f funcContributor) CFs() []CFSpec { return f.cfs }
func (f funcContributor) RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error {
	if f.rebuild == nil { return nil }
	return f.rebuild(dst, ri)
}

func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(funcContributor{name: "system", cfs: []CFSpec{{storage.CFSys, true}}})
	r.Register(funcContributor{name: "kv", cfs: []CFSpec{{storage.CFKVData, true}, {storage.CFKVMeta, true}}})
	r.Register(funcContributor{name: "collections", cfs: []CFSpec{{storage.CFReplData, true}}})
	r.Register(funcContributor{name: "graph", cfs: []CFSpec{{storage.CFGraphData, true}, {storage.CFGraphIndex, false}}}) // rebuild in Task 6
	r.Register(funcContributor{name: "vector", cfs: []CFSpec{{storage.CFVectorRaw, true}, {storage.CFVectorIndex, false}}}) // rebuild in Task 6
	return r
}
```

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(backup): DefaultRegistry — System/KV/Collections/Graph/Vector contributors"`

---

## Task 3: Versioned node manifest

@superpowers:test-driven-development

**Files:**
- Create: `internal/backup/manifest.go`
- Test: `internal/backup/manifest_test.go`

- [ ] **Step 1: Write the failing test** (JSON round-trip + forward-compat: extra unknown field ignored)

```go
package backup

import (
	"bytes"
	"testing"
)

func TestNodeManifestRoundTrip(t *testing.T) {
	m := NodeManifest{
		FormatVersion:      1,
		CaptureWallClockMs: 1719000000000,
		StorageUUID:        "uuid-123",
		CFs: []CFEntry{
			{CF: "kv_data", Entries: 42, Bytes: 1000},
			{CF: "repl_data", Entries: 7, Bytes: 256},
		},
	}
	var buf bytes.Buffer
	if err := m.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := ReadNodeManifest(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got.FormatVersion != 1 || got.StorageUUID != "uuid-123" || len(got.CFs) != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestNodeManifestForwardCompat(t *testing.T) {
	// A manifest written by a newer version with an extra field must still parse.
	raw := []byte(`{"format_version":1,"capture_wall_clock_ms":1,"cfs":[],"future_field":{"x":1}}`)
	m, err := ReadNodeManifest(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("forward-compat parse failed: %v", err)
	}
	if m.FormatVersion != 1 {
		t.Fatalf("want format_version 1, got %d", m.FormatVersion)
	}
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (undefined `NodeManifest`).

- [ ] **Step 3: Implement** `internal/backup/manifest.go` using `encoding/json` (additive, unknown fields ignored by default — that gives forward-compat for free). Use snake_case json tags. `CFEntry{CF string; Entries int64; Bytes int64}` (CF as the wavesdb cf name string, e.g. `cf.Name()`). `WriteTo(w io.Writer) error` marshals indented; `ReadNodeManifest(r io.Reader) (*NodeManifest, error)` decodes. Include `FormatVersion int`, `CaptureWallClockMs int64`, `StorageUUID string`, `CFs []CFEntry`. (Const `manifestFormatVersion = 1`.)

- [ ] **Step 4: Run to verify it passes** — PASS (both tests).

- [ ] **Step 5: Commit** — `git commit -am "feat(backup): versioned NodeManifest (json, forward-compatible)"`

---

## Task 4: Logical export

Iterate each authoritative CF over a consistent snapshot; stream `(cf,key,value)` to one object per CF under `<prefix>/cf/<cfname>`; write `<prefix>/node.manifest.json`. Derived/transient CFs are never iterated (they're not in `AuthoritativeCFs()`). The storage UUID is captured into the manifest (for restore-time identity decisions) but its CFSys key is still exported as data (restore decides exclusion — Task 5).

@superpowers:test-driven-development

**Files:**
- Create: `internal/backup/logical_export.go`
- Test: `internal/backup/logical_export_test.go`

- [ ] **Step 1: Write the failing test**

```go
package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

func TestExportLogicalWritesObjectsAndManifest(t *testing.T) {
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { _ = src.Close() })

	// Seed authoritative + derived + transient data.
	mustPut(t, src, storage.CFKVData, []byte("k1"), []byte("v1"))
	mustPut(t, src, storage.CFKVData, []byte("k2"), []byte("v2"))
	mustPut(t, src, storage.CFReplData, []byte("\x00\x00\x00\x00\x00\x00\x00\x02coll"), []byte("set"))
	mustPut(t, src, storage.CFGraphIndex, []byte("idx"), []byte("derived")) // must NOT be exported
	mustPut(t, src, storage.CFCacheMeta, []byte("c"), []byte("transient"))  // must NOT be exported

	store, err := objstore.NewFS(t.TempDir())
	if err != nil { t.Fatal(err) }

	man, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000)
	if err != nil { t.Fatal(err) }

	if man.CFEntryCount("kv_data") != 2 { t.Fatalf("want 2 kv_data entries, got %d", man.CFEntryCount("kv_data")) }
	if man.CFEntryCount("repl_data") != 1 { t.Fatalf("want 1 repl_data entry, got %d", man.CFEntryCount("repl_data")) }
	if man.CFEntryCount("graph_index") != 0 { t.Fatal("derived graph_index must not be exported") }
	if man.CFEntryCount("cache_meta") != 0 { t.Fatal("transient cache_meta must not be exported") }
	if ok, _ := store.Exists("bk/node.manifest.json"); !ok { t.Fatal("manifest object missing") }
}
```
(Add `mustPut(t, s, cf, k, v)` helper in the test: `if err := s.Put(cf, k, v); err != nil { t.Fatal(err) }`. Add `NodeManifest.CFEntryCount(name string) int64` to manifest.go in this task — small helper returning the entry count for a CF name, 0 if absent.)

- [ ] **Step 2: Run to verify it fails** — FAIL (`ExportLogical` undefined).

- [ ] **Step 3: Implement** `internal/backup/logical_export.go`

```go
func ExportLogical(src storage.LocalStore, store ObjectStore, keyPrefix string, reg *Registry, captureMs int64) (*NodeManifest, error)
```
Steps:
1. `uuid, _ := storage.EnsureStorageUUID(src)` — record in manifest (informational; do not fail if unavailable).
2. `snap, err := src.Snapshot()` ; `defer snap.Close()` — the consistent cut.
3. For each `cf := range reg.AuthoritativeCFs()`: open `it, _ := snap.Scan(cf, nil, nil, 0)` (full CF: nil start/end = whole range — confirm nil bounds scan all; if the impl requires explicit bounds, use `nil, nil`), stream entries to an objstore object `keyPrefix + "/cf/" + cf.Name()` using the existing codec helpers (`putUvarint`(unused here since one object per CF — instead write `lenPrefix(key) || lenPrefix(value)` repeating via `writeBytes`), counting entries + bytes. Use an `io.Pipe` or buffer → `store.Put(key, reader, size)`. Simplest: write to a `bytes.Buffer`, then `store.Put(objKey, bytes.NewReader(buf.Bytes()), int64(buf.Len()))`.
4. Record a `CFEntry{CF: cf.Name(), Entries: n, Bytes: b}` per non-empty CF.
5. Build `NodeManifest{FormatVersion: manifestFormatVersion, CaptureWallClockMs: captureMs, StorageUUID: uuid, CFs: entries}`; write it to `keyPrefix + "/node.manifest.json"` via `man.WriteTo` into a buffer → `store.Put`.
6. Return the manifest.

Reuse `writeBytes`/`readBytes` from backup.go (same package). Per-CF object format: repeating `lenPrefix(key) || lenPrefix(value)` (no cf tag needed — the object IS the CF). Confirm `cf.Name()` exists (cf.go maps CF→string; if the method is named differently, e.g. a `cfNames[cf]` map, expose a small `func cfName(cf storage.ColumnFamily) string`).

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(backup): ExportLogical — authoritative CFs to object store + node manifest"`

---

## Task 5: Logical restore + KV/collections/system round-trip

Restore reads the manifest, raw-restores each authoritative CF object via batched `Batch`/`BatchRC`, but **skips the node-identity key** (`/sys/storage_uuid`) so the target keeps its own identity. Rebuild hooks invoked at the end (no-ops until Task 6). Round-trip test proves KV + collections + system survive and identity is preserved.

@superpowers:test-driven-development

**Files:**
- Create: `internal/backup/logical_restore.go`
- Test: `internal/backup/logical_restore_test.go`

- [ ] **Step 1: Write the failing round-trip test**

```go
package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

func TestLogicalRoundTripSameShape(t *testing.T) {
	src, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = src.Close() })
	srcUUID, _ := storage.EnsureStorageUUID(src)

	mustPut(t, src, storage.CFKVData, []byte("k1"), []byte("v1"))
	mustPut(t, src, storage.CFKVMeta, []byte("k1"), []byte("ptr"))
	mustPut(t, src, storage.CFReplData, []byte("\x00\x00\x00\x00\x00\x00\x00\x02coll"), []byte("set"))

	store, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000); err != nil {
		t.Fatal(err)
	}

	// Fresh destination with its OWN identity.
	dst, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst.Close() })
	dstUUID, _ := storage.EnsureStorageUUID(dst)
	if dstUUID == srcUUID {
		t.Fatal("test precondition: dst should have a different UUID")
	}

	if err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{RestoreWallClockMs: 1719000100000}); err != nil {
		t.Fatal(err)
	}

	// Data restored.
	if v, ok, _ := dst.Get(storage.CFKVData, []byte("k1")); !ok || string(v) != "v1" {
		t.Fatalf("kv_data k1 not restored: ok=%v v=%q", ok, v)
	}
	if v, ok, _ := dst.Get(storage.CFReplData, []byte("\x00\x00\x00\x00\x00\x00\x00\x02coll")); !ok || string(v) != "set" {
		t.Fatalf("repl_data coll not restored: ok=%v v=%q", ok, v)
	}
	// Identity preserved (NOT overwritten by source).
	nowUUID, _, _ := dst.Get(storage.CFSys, []byte("/sys/storage_uuid"))
	if string(nowUUID) != dstUUID {
		t.Fatalf("dst identity was overwritten: want %q got %q", dstUUID, nowUUID)
	}
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (`RestoreLogical` undefined).

- [ ] **Step 3: Implement** `internal/backup/logical_restore.go`

```go
func RestoreLogical(dst storage.LocalStore, store ObjectStore, keyPrefix string, reg *Registry, ri RestoreInfo) error
```
Steps:
1. Read `keyPrefix + "/node.manifest.json"` via `store.Get` → `ReadNodeManifest`. (Reject `FormatVersion > manifestFormatVersion` with a clear error — major-version guard.)
2. For each `CFEntry` in the manifest: `cf := cfByName(entry.CF)` (map name→ColumnFamily; add `func cfByName(string) (storage.ColumnFamily, bool)` in cf-helper). `store.Get(keyPrefix + "/cf/" + entry.CF)`, decode repeating `readBytes(key), readBytes(value)`, accumulate `storage.StoreOp{CF: cf, Key: key, Value: value}`, flushing via `dst.Batch(ops)` every 1000. **Skip the identity key**: if `cf == CFSys && string(key) == "/sys/storage_uuid"`, do not restore it.
3. After all CFs: for each `c := range reg.Contributors()` call `c.RebuildAfterRestore(dst, ri)` (no-ops until Task 6); return first error.

Use `storage.StoreOp` (confirm its field names: `{CF, Key, Value}` and that `Batch([]StoreOp)` is the restore path — backup.go's Restore uses it). For unknown CF names in the manifest (future datatype), `cfByName` returns false → restore the object **blind** into a CF created by name if the store supports it; for Phase 2a, log-and-skip unknown CFs with a clear message (full blind-restore of unknown CFs is a Phase 3 concern — note it). 

- [ ] **Step 4: Run to verify it passes** — `go test ./internal/backup/ -run TestLogicalRoundTrip -v` → PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(backup): RestoreLogical — raw same-shape restore, node identity preserved"`

---

## Task 6: Graph + vector rebuild hooks + round-trip

Wire the Graph and Vector contributors' `RebuildAfterRestore` to actually rebuild their derived indexes, and prove it with a round-trip that exports graph+vector raw data (no index), restores, and confirms the index is rebuilt and queryable.

@superpowers:test-driven-development

**Files:**
- Modify: `internal/backup/contributors.go` (wire graph/vector rebuild)
- Test: `internal/backup/rebuild_test.go`

- [ ] **Step 1: Investigate enumeration APIs**

The rebuild hooks must enumerate which graphs / vector collections exist in the restored data. Run:
`grep -rn "func.*Store.*Graph\|ListGraphs\|forEachNode\|ScanCollection\|ListCollections" internal/graph internal/vector | head -40`
Determine how to (a) construct a `*graph.Store` / `*vector.Store` over a `storage.LocalStore`, and (b) enumerate graph names and vector collections from the restored authoritative data. If a clean enumeration API exists, use it. If not, report `NEEDS_CONTEXT` describing exactly what's missing — do NOT invent an enumeration by guessing key layouts.

- [ ] **Step 2: Write the failing test** (shape — adjust constructor calls to the real graph/vector Store APIs found in Step 1)

```go
// Seed a graph (nodes/edges) and a vector collection in src via the real
// graph.Store / vector.Store write APIs; build their indexes; export; restore
// into a fresh dst (whose CFGraphIndex/CFVectorIndex are EMPTY because export
// excluded them); then assert a label/adjacency graph query and a vector search
// return correct results on dst — proving the rebuild hook reconstructed the index.
```
Concrete assertions: after `RestoreLogical(dst, ...)`, a graph query that depends on `CFGraphIndex` (e.g. lookup by label) returns the seeded node, and a `vector` search returns the seeded vector — both impossible unless the hook rebuilt the index, since export wrote no index objects.

- [ ] **Step 3: Run to verify it fails** — FAIL (rebuild hooks are no-ops → index empty → queries return nothing).

- [ ] **Step 4: Implement the rebuild wiring** in `contributors.go`: graph contributor's `rebuild` constructs a `*graph.Store` over `dst`, enumerates graphs, calls `RebuildIndexes(name)` for each; vector contributor's `rebuild` constructs a `*vector.Store` over `dst`, enumerates collections, calls `RebuildLiveIndex(store, coll, metric, params)` for each (use the collection's stored metric/params if available, else the documented defaults). Keep datatype specifics inside these hooks (invariant B).

- [ ] **Step 5: Run to verify it passes** — PASS (queries succeed on the restored, rebuilt indexes).

- [ ] **Step 6: Commit** — `git commit -am "feat(backup): graph + vector rebuild-after-restore hooks (derived indexes reconstructed)"`

---

## Done criteria for Phase 2a

- [ ] Registry-driven logical backup: `DefaultRegistry` covers all authoritative CFs incl. `CFReplData`; derived (`CFGraphIndex`, `CFVectorIndex`) excluded + rebuilt; transient (`CFReplLog`, `CFCacheMeta`) never exported.
- [ ] `ExportLogical` → object store + versioned manifest; `RestoreLogical` raw-restores, preserves node identity, rebuilds derived indexes.
- [ ] Round-trips pass: KV+collections+system (Task 5), graph+vector with index rebuild (Task 6).
- [ ] `go test ./internal/backup/...` green; `go vet ./internal/backup/...` clean; `go build ./...` green.
- [ ] No change to existing hot-path code; existing `backup.Backup`/`Restore` left intact (superseded by the registry engine but not removed in 2a — removal/migration is a later cleanup).

Phase 2a delivers a demonstrable single-node logical backup→restore for every datatype. Phase 2b adds re-shard (collections re-route by `(ns,coll)` under a new N) + partial selection; Phase 3 adds the cluster coordinator, incrementals, and physical-plane integration.
