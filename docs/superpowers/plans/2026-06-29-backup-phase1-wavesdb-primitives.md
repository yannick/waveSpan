# Backup Phase 1 — wavesdb engine primitives — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the four additive wavesdb engine primitives the snapshot-backup design depends on — an external consistent `Snapshot` handle, a seq-based changed-SSTable diff, and consistent checkpoint upload/restore against the existing `ObjectStore` interface — so the waveSpan backup layer (later phases) can stream consistent state to S3 and reconstitute a database from it.

**Architecture:** All work is additive to the wavesdb engine, building on existing machinery: the private `acquireSnapshot`/`releaseSnapshot` refcount, the `manifest.SSTMeta`/`Manifest` catalog, the `Checkpoint`/`Backup` flush-then-enumerate pattern, and the existing `ObjectStore` interface (`Put/Get/List/Exists`) with its FS + S3 backends. Uploads happen **outside the DB lock** with table handles pinned (`incref`), so a slow object store never stalls flush/compaction. No hot-path code is touched.

**Tech Stack:** Go, wavesdb (module `wavesdb`), `internal/manifest`, `internal/sst`, the existing `ObjectStore` FS backend for tests. Spec: `docs/superpowers/specs/2026-06-27-snapshot-backups-design.md` §4.

**Phase roadmap (context — later phases get their own plans):** Phase 1 = wavesdb primitives (this doc). Phase 2 = waveSpan logical backup core (contributor registry, manifest, coordinator, node agent, logical export/restore). Phase 3 = physical plane + `backup.proto`/`BackupService` + meta-shard intent + main wiring. Phase 4 = operator CRD + `wavespanctl` CLI.

---

## Isolation & build wiring

waveSpan consumes wavesdb via `replace wavesdb v0.0.1 => ../wavesdb` (waveSpan-backup/go.mod:75). The shared `../wavesdb` checkout may be touched by parallel IO-layer work, so Phase 1 is done in an **isolated wavesdb worktree** and the `backup` branch's replace directive is repointed at it.

### Task 0: Isolate the engine work

**Files:**
- Create: git worktree `/Volumes/HOME/code/storage-engines/wavesdb-backup` on new branch `backup-primitives` (from `wavesdb` `main`)
- Modify: `/Volumes/HOME/code/storage-engines/waveSpan-backup/go.mod:75` (replace directive)

- [ ] **Step 1: Create the wavesdb worktree**

```bash
cd /Volumes/HOME/code/storage-engines/wavesdb
git worktree add -b backup-primitives /Volumes/HOME/code/storage-engines/wavesdb-backup main
```
Expected: `Preparing worktree (new branch 'backup-primitives')`.

- [ ] **Step 2: Repoint the backup branch's replace directive at the isolated worktree**

In `/Volumes/HOME/code/storage-engines/waveSpan-backup/go.mod`, change line 75:
```
replace wavesdb v0.0.1 => ../wavesdb-backup
```

- [ ] **Step 3: Verify the backup worktree still builds against the isolated engine**

Run: `cd /Volumes/HOME/code/storage-engines/waveSpan-backup && go build ./... 2>&1 | tail -5`
Expected: builds clean (no module errors). All engine work below happens in `/Volumes/HOME/code/storage-engines/wavesdb-backup`.

- [ ] **Step 4: Commit the wiring change**

```bash
cd /Volumes/HOME/code/storage-engines/waveSpan-backup
git add go.mod && git commit -m "build(backup): point wavesdb replace at isolated wavesdb-backup worktree"
```

---

## Task 1: External consistent `Snapshot` handle

A public handle that pins a commit sequence (so compaction won't collapse versions it needs) and serves iterators at that sequence — without holding a `Txn` open for the whole export. Wraps the existing private `acquireSnapshot`/`releaseSnapshot` and `cf.newIterator(readSeq, nil)`.

@superpowers:test-driven-development

**Files:**
- Create: `/Volumes/HOME/code/storage-engines/wavesdb-backup/snapshot.go`
- Test: `/Volumes/HOME/code/storage-engines/wavesdb-backup/snapshot_handle_test.go`

- [ ] **Step 1: Write the failing test**

```go
package wavesdb

import "testing"

func TestSnapshotPinsPointInTime(t *testing.T) {
	db, cf := openDB(t)
	defer db.Close()

	if err := db.Put(cf, []byte("k"), []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	defer snap.Release()

	// Write a newer version AFTER the snapshot was taken.
	if err := db.Put(cf, []byte("k"), []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}

	it := snap.NewIterator(cf)
	defer it.Close()
	it.Seek([]byte("k"))
	if !it.Valid() {
		t.Fatal("snapshot iterator should see key k")
	}
	if got := string(it.Value()); got != "v1" {
		t.Fatalf("snapshot must read as-of acquire time: want v1, got %q", got)
	}
	// A live read sees the newer value.
	if v, err := db.Get(cf, []byte("k")); err != nil || string(v) != "v2" {
		t.Fatalf("live Get want v2: got %q err %v", v, err)
	}
}

func TestSnapshotReleaseIsIdempotent(t *testing.T) {
	db, _ := openDB(t)
	defer db.Close()
	snap := db.AcquireSnapshot()
	snap.Release()
	snap.Release() // must not panic or double-decrement
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestSnapshot ./... -v`
Expected: FAIL — `db.AcquireSnapshot undefined`.

- [ ] **Step 3: Implement the Snapshot handle**

Create `snapshot.go`. Use the real machinery confirmed in `db.go` (`acquireSnapshot`/`releaseSnapshot` at db.go:226-258, `db.seq.Load()`) and `iterator.go` (`cf.newIterator(readSeq, extra)` at iterator.go:221). The `Iterator` already has `Seek`/`Valid`/`Value`/`Key`/`Close`.

```go
package wavesdb

import "sync/atomic"

// Snapshot is a pinned, consistent read view of the database at a fixed commit
// sequence. It holds off compaction's version collapse for that sequence until
// Release, so a long-running export reads a stable point-in-time view without
// keeping a transaction open. Always Release a Snapshot.
type Snapshot struct {
	db       *DB
	seq      uint64
	released atomic.Bool
}

// AcquireSnapshot pins the current committed sequence as a consistent read view.
func (db *DB) AcquireSnapshot() *Snapshot {
	seq := db.seq.Load()
	db.acquireSnapshot(seq)
	return &Snapshot{db: db, seq: seq}
}

// Seq is the committed sequence this snapshot reads at (the backup cut marker).
func (s *Snapshot) Seq() uint64 { return s.seq }

// NewIterator returns an iterator over cf at the snapshot's sequence. Caller Closes it.
func (s *Snapshot) NewIterator(cf *ColumnFamily) *Iterator {
	return cf.newIterator(s.seq, nil)
}

// Release unpins the snapshot. Safe to call more than once.
func (s *Snapshot) Release() {
	if s.released.CompareAndSwap(false, true) {
		s.db.releaseSnapshot(s.seq)
	}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestSnapshot ./... -v`
Expected: PASS (both tests).

- [ ] **Step 5: Run with the race detector**

Run: `go test -race -run TestSnapshot ./...`
Expected: PASS, no race.

- [ ] **Step 6: Commit**

```bash
cd /Volumes/HOME/code/storage-engines/wavesdb-backup
git add snapshot.go snapshot_handle_test.go
git commit -m "feat(backup): external consistent Snapshot handle (AcquireSnapshot/Seq/NewIterator/Release)"
```

---

## Task 2: `SSTablesSince` — seq-based changed-table diff

Returns every SSTable whose `MaxSeq > seq`, tagged with its column family. Because SSTables are immutable, passing a previous backup's `GlobalSeq` yields exactly the files written since — the basis for both planes' incrementals.

@superpowers:test-driven-development

**Files:**
- Create: `/Volumes/HOME/code/storage-engines/wavesdb-backup/sstables_since.go`
- Test: `/Volumes/HOME/code/storage-engines/wavesdb-backup/sstables_since_test.go`

- [ ] **Step 1: Write the failing test**

```go
package wavesdb

import (
	"fmt"
	"testing"
)

func TestSSTablesSince(t *testing.T) {
	db, cf := openDB(t)
	defer db.Close()

	for i := 0; i < 50; i++ {
		if err := db.Put(cf, []byte(fmt.Sprintf("a%03d", i)), []byte("1"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.FlushMemtable(cf); err != nil {
		t.Fatal(err)
	}
	seq1 := db.Stats().GlobalSeq

	for i := 0; i < 50; i++ {
		if err := db.Put(cf, []byte(fmt.Sprintf("b%03d", i)), []byte("2"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.FlushMemtable(cf); err != nil {
		t.Fatal(err)
	}

	// Only the second flush's SSTable has MaxSeq > seq1.
	since := db.SSTablesSince(seq1)
	if len(since) != 1 {
		t.Fatalf("want 1 changed sstable since seq1, got %d", len(since))
	}
	if since[0].CF != "default" {
		t.Fatalf("changed table should be tagged with its CF, got %q", since[0].CF)
	}
	// Since 0, both tables are returned.
	if all := db.SSTablesSince(0); len(all) != 2 {
		t.Fatalf("want 2 sstables since 0, got %d", len(all))
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestSSTablesSince ./... -v`
Expected: FAIL — `db.SSTablesSince undefined`.

- [ ] **Step 3: Implement**

Create `sstables_since.go`. Mirror the locking in `Checkpoint` (db.mu.RLock then per-cf cf.mu.RLock over `cf.levels`), reading `th.meta` (`manifest.SSTMeta`, fields ID/Level/MaxSeq/KlogSize/VlogSize/MinKey/MaxKey).

```go
package wavesdb

import "wavesdb/internal/manifest"

// ChangedSSTable is an SSTable's catalog entry plus the column family it belongs to.
type ChangedSSTable struct {
	CF string
	manifest.SSTMeta
}

// SSTablesSince returns all SSTables across all column families whose MaxSeq is
// greater than seq. SSTables are immutable, so this is a stable incremental diff:
// pass a previous backup's GlobalSeq to get exactly the tables written since.
func (db *DB) SSTablesSince(seq uint64) []ChangedSSTable {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var out []ChangedSSTable
	for name, cf := range db.cfs {
		cf.mu.RLock()
		for _, level := range cf.levels {
			for _, th := range level {
				if th.meta.MaxSeq > seq {
					out = append(out, ChangedSSTable{CF: name, SSTMeta: th.meta})
				}
			}
		}
		cf.mu.RUnlock()
	}
	return out
}
```
Note: confirm the manifest import path matches existing files (grep an existing top-level file for `internal/manifest` — it is imported as `"wavesdb/internal/manifest"`).

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestSSTablesSince ./... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sstables_since.go sstables_since_test.go
git commit -m "feat(backup): SSTablesSince(seq) changed-table diff for incrementals"
```

---

## Task 3: `CheckpointToObjectStore` — consistent upload (full + incremental)

Mirrors `Checkpoint` but streams SSTable files to an `ObjectStore` under a key prefix instead of a directory, skipping tables already present in `parent` (incremental). Uploads happen **outside the DB lock** with handles pinned, so a slow store can't stall the engine. Also uploads the wavesdb `MANIFEST` (so the checkpoint is restorable, incl. per-CF config). Returns a public `CheckpointManifest` summary (for the waveSpan backup manifest and as the next incremental's parent).

@superpowers:test-driven-development

**Files:**
- Create: `/Volumes/HOME/code/storage-engines/wavesdb-backup/checkpoint_objstore.go`
- Test: `/Volumes/HOME/code/storage-engines/wavesdb-backup/checkpoint_objstore_test.go`

- [ ] **Step 1: Identify the FS ObjectStore constructor for the test**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && grep -rn "ObjectStore" objstore.go objstore_mode.go | grep -iE "func New|fsObject|FSObject|NewFS" | head`
Record the exact filesystem-backed `ObjectStore` constructor name (e.g. `NewFSObjectStore(dir)`); use it in the test below. If only an unexported type exists, add a tiny test helper in the test file that constructs it.

- [ ] **Step 2: Write the failing test**

```go
package wavesdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func countKlogs(keys []string) int {
	n := 0
	for _, k := range keys {
		if strings.HasSuffix(k, ".klog") {
			n++
		}
	}
	return n
}

func TestCheckpointToObjectStoreFullAndIncremental(t *testing.T) {
	db, cf := openDB(t)
	defer db.Close()
	for i := 0; i < 200; i++ {
		if err := db.Put(cf, []byte(fmt.Sprintf("k%04d", i)), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.FlushMemtable(cf); err != nil {
		t.Fatal(err)
	}

	store := NewFSObjectStore(t.TempDir()) // from Step 1
	ctx := context.Background()

	full, err := db.CheckpointToObjectStore(ctx, store, "b1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Tables) == 0 {
		t.Fatal("full checkpoint should list tables")
	}
	if full.GlobalSeq == 0 {
		t.Fatal("checkpoint manifest should carry GlobalSeq")
	}
	if ok, _ := store.Exists("b1/MANIFEST"); !ok {
		t.Fatal("checkpoint MANIFEST not uploaded")
	}
	keysAfterFull, _ := store.List("b1/")
	klogsFull := countKlogs(keysAfterFull)

	// New flush -> one new table. Incremental should upload exactly it.
	if err := db.Put(cf, []byte("zzz"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := db.FlushMemtable(cf); err != nil {
		t.Fatal(err)
	}
	inc, err := db.CheckpointToObjectStore(ctx, store, "b1", full)
	if err != nil {
		t.Fatal(err)
	}
	if len(inc.Tables) != len(full.Tables)+1 {
		t.Fatalf("incremental manifest must list ALL tables: got %d want %d", len(inc.Tables), len(full.Tables)+1)
	}
	keysAfterInc, _ := store.List("b1/")
	if delta := countKlogs(keysAfterInc) - klogsFull; delta != 1 {
		t.Fatalf("incremental should upload exactly 1 new klog, uploaded %d", delta)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestCheckpointToObjectStore ./... -v`
Expected: FAIL — `db.CheckpointToObjectStore undefined`.

- [ ] **Step 4: Implement**

Create `checkpoint_objstore.go`. Mirror `Checkpoint` (maintenance.go:123) for flush + `cf.snapshotManifest()`, but: collect `(cfName, meta, klogPath, vlogPath)` and `incref` each handle under the lock, release the lock, then upload outside it, then `decref` (defer). Reuse `th.reader.KlogPath()`/`VlogPath()`. Upload via the `ObjectStore.Put(key, io.Reader, size)` method (objstore.go:8). Serialize the wavesdb `manifest.Manifest` to a temp file with its existing `Save` and upload it as `<prefix>/MANIFEST`. Skip tables whose `ID` is in `parent`.

```go
package wavesdb

import (
	"context"
	"fmt"
	"os"
	"path"

	"wavesdb/internal/manifest"
)

// CheckpointTable summarizes one uploaded SSTable (public; safe for callers).
type CheckpointTable struct {
	CF       string
	ID       uint64
	MaxSeq   uint64
	KlogSize int64
	VlogSize int64
}

// CheckpointManifest is the public summary of a checkpoint written to an object
// store: the cut sequence and the full table set. Pass it back as `parent` to
// take an incremental; persist its fields in the caller's backup manifest.
type CheckpointManifest struct {
	GlobalSeq  uint64
	NextFileID uint64
	Tables     []CheckpointTable
}

func (cm *CheckpointManifest) idset() map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(cm.Tables))
	for _, t := range cm.Tables {
		m[t.ID] = struct{}{}
	}
	return m
}

func objKey(prefix, cf string, id uint64, ext string) string {
	return path.Join(prefix, "cf_"+cf, fmt.Sprintf("%d.%s", id, ext))
}

// CheckpointToObjectStore writes a consistent checkpoint to store under keyPrefix.
// Memtables are flushed first. If parent != nil, only tables absent from parent are
// uploaded (incremental). Uploads run outside the DB lock with handles pinned.
func (db *DB) CheckpointToObjectStore(ctx context.Context, store ObjectStore, keyPrefix string, parent *CheckpointManifest) (*CheckpointManifest, error) {
	// 1. flush (writable db) — same as Checkpoint.
	db.mu.RLock()
	cfs := make([]*ColumnFamily, 0, len(db.cfs))
	for _, cf := range db.cfs {
		cfs = append(cfs, cf)
	}
	db.mu.RUnlock()
	if !db.opts.ReadOnly {
		for _, cf := range cfs {
			if err := db.FlushMemtable(cf); err != nil {
				return nil, err
			}
		}
	}

	// 2. snapshot catalog + pin handles under lock.
	type pinned struct {
		cf, klog, vlog string
		meta           manifest.SSTMeta
		th             *tableHandle
	}
	var plan []pinned
	db.mu.RLock()
	man := &manifest.Manifest{NextFileID: db.nextFileID.Load(), GlobalSeq: db.seq.Load()}
	for _, cf := range cfs {
		cf.mu.RLock()
		for _, level := range cf.levels {
			for _, th := range level {
				th.incref()
				p := pinned{cf: cf.name, klog: th.reader.KlogPath(), meta: th.meta, th: th}
				if th.meta.VlogSize > 0 {
					p.vlog = th.reader.VlogPath()
				}
				plan = append(plan, p)
			}
		}
		cf.mu.RUnlock()
		man.CFs = append(man.CFs, cf.snapshotManifest())
	}
	db.mu.RUnlock()

	// 3. release pins no matter what.
	defer func() {
		for _, p := range plan {
			p.th.decref() // use the existing unref method name
		}
	}()

	// 4. upload tables outside the lock, skipping parent's IDs.
	var skip map[uint64]struct{}
	if parent != nil {
		skip = parent.idset()
	}
	out := &CheckpointManifest{GlobalSeq: man.GlobalSeq, NextFileID: man.NextFileID}
	for _, p := range plan {
		out.Tables = append(out.Tables, CheckpointTable{
			CF: p.cf, ID: p.meta.ID, MaxSeq: p.meta.MaxSeq,
			KlogSize: p.meta.KlogSize, VlogSize: p.meta.VlogSize,
		})
		if _, done := skip[p.meta.ID]; done {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := uploadFile(store, objKey(keyPrefix, p.cf, p.meta.ID, "klog"), p.klog); err != nil {
			return nil, err
		}
		if p.vlog != "" {
			if err := uploadFile(store, objKey(keyPrefix, p.cf, p.meta.ID, "vlog"), p.vlog); err != nil {
				return nil, err
			}
		}
	}

	// 5. upload the wavesdb MANIFEST (restorable, includes per-CF config).
	tmp, err := os.CreateTemp("", "wavesdb-ckpt-manifest-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)
	if err := man.Save(tmpPath); err != nil {
		return nil, err
	}
	if err := uploadFile(store, path.Join(keyPrefix, "MANIFEST"), tmpPath); err != nil {
		return nil, err
	}
	return out, nil
}

func uploadFile(store ObjectStore, key, srcPath string) error {
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return store.Put(key, f, fi.Size())
}
```
Implementation notes for the worker:
- Confirm the handle unref method name: `grep -n "func (th \*tableHandle)" db.go` — use the existing decrement (likely `decref` or `unref`). `incref` is confirmed at iterator.go.
- Confirm `manifest.Manifest` has `Save(path string)` (used by Checkpoint at maintenance.go:169 via `man.Save(manifest.Path(dir))`) and `cf.snapshotManifest()` exists (used in Checkpoint).
- `tableHandle.reader` exposes `KlogPath()`/`VlogPath()` (used in Checkpoint).

- [ ] **Step 5: Run to verify it passes**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestCheckpointToObjectStore ./... -v`
Expected: PASS.

- [ ] **Step 6: Race check**

Run: `go test -race -run TestCheckpointToObjectStore ./...`
Expected: PASS (verifies upload-outside-lock + pin/unpin is race-free against background compaction).

- [ ] **Step 7: Commit**

```bash
git add checkpoint_objstore.go checkpoint_objstore_test.go
git commit -m "feat(backup): CheckpointToObjectStore — consistent full+incremental upload (lock-free upload, pinned handles)"
```

---

## Task 4: `RestoreFromObjectStore` — download a checkpoint into an openable dir

Round-trips Task 3: reads `<prefix>/MANIFEST`, recreates `cf_<name>/` dirs, downloads each referenced klog/vlog, and places the MANIFEST so `Open(Options{Path: dir})` works. This is the engine half of the physical same-shape restore path.

@superpowers:test-driven-development

**Files:**
- Create: `/Volumes/HOME/code/storage-engines/wavesdb-backup/restore_objstore.go`
- Test: `/Volumes/HOME/code/storage-engines/wavesdb-backup/restore_objstore_test.go`

- [ ] **Step 1: Write the failing round-trip test**

```go
package wavesdb

import (
	"context"
	"fmt"
	"testing"
)

func TestCheckpointObjStoreRoundTrip(t *testing.T) {
	src := t.TempDir()
	db, err := Open(Options{Path: src})
	if err != nil {
		t.Fatal(err)
	}
	cf, err := db.CreateColumnFamily("default", DefaultColumnFamilyOptions())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if err := db.Put(cf, []byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("val%04d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	store := NewFSObjectStore(t.TempDir())
	ctx := context.Background()
	if _, err := db.CheckpointToObjectStore(ctx, store, "b1", nil); err != nil {
		t.Fatal(err)
	}
	db.Close()

	dst := t.TempDir()
	if err := RestoreFromObjectStore(ctx, store, "b1", dst); err != nil {
		t.Fatal(err)
	}
	rdb, err := Open(Options{Path: dst})
	if err != nil {
		t.Fatalf("restored dir must open: %v", err)
	}
	defer rdb.Close()
	cf2 := rdb.GetColumnFamily("default")
	if cf2 == nil {
		t.Fatal("restored db missing cf")
	}
	for i := 0; i < 1000; i++ {
		v, err := rdb.Get(cf2, []byte(fmt.Sprintf("k%04d", i)))
		if err != nil {
			t.Fatalf("restored db missing k%04d: %v", i, err)
		}
		if want := fmt.Sprintf("val%04d", i); string(v) != want {
			t.Fatalf("k%04d: want %q got %q", i, want, v)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestCheckpointObjStoreRoundTrip ./... -v`
Expected: FAIL — `RestoreFromObjectStore undefined`.

- [ ] **Step 3: Implement**

Create `restore_objstore.go`. Download MANIFEST into `manifest.Path(dir)`, load it with the manifest package's loader (confirm: `grep -n "func Load\|func Path" internal/manifest/manifest.go`), then for each `CFManifest` create `cf_<Name>/` and download each `SSTMeta` table's `<ID>.klog` (and `.vlog` when `VlogSize > 0`).

```go
package wavesdb

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"

	"wavesdb/internal/manifest"
)

// RestoreFromObjectStore downloads the checkpoint at keyPrefix in store into dir,
// producing a directory openable with Open(Options{Path: dir}).
func RestoreFromObjectStore(ctx context.Context, store ObjectStore, keyPrefix, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// 1. fetch MANIFEST into place and load it.
	manPath := manifest.Path(dir)
	if err := downloadFile(store, path.Join(keyPrefix, "MANIFEST"), manPath); err != nil {
		return err
	}
	man, err := manifest.Load(manPath)
	if err != nil {
		return err
	}
	// 2. download each CF's tables.
	for _, cf := range man.CFs {
		cfDir := filepath.Join(dir, "cf_"+cf.Name)
		if err := os.MkdirAll(cfDir, 0o755); err != nil {
			return err
		}
		for _, t := range cf.SSTables {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := downloadFile(store, objKey(keyPrefix, cf.Name, t.ID, "klog"),
				filepath.Join(cfDir, fileName(t.ID, "klog"))); err != nil {
				return err
			}
			if t.VlogSize > 0 {
				if err := downloadFile(store, objKey(keyPrefix, cf.Name, t.ID, "vlog"),
					filepath.Join(cfDir, fileName(t.ID, "vlog"))); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func fileName(id uint64, ext string) string { return objKey("", "", id, ext)[len("/cf_/"):] }
```
Implementation notes:
- Confirm the on-disk SSTable file names Open expects (Checkpoint writes `fmt.Sprintf("%d.klog", id)` directly into `cf_<name>/`); match that exactly — simplest is a local helper `func sstName(id uint64, ext string) string { return fmt.Sprintf("%d.%s", id, ext) }` rather than the slicing hack above. Replace `fileName` with that.
- Add `downloadFile(store, key, destPath)`: `r, err := store.Get(key)`; create dest; `io.Copy`; close both.
- Confirm `manifest.Load` and `manifest.Path` exist (Checkpoint uses `manifest.Path`; `Load` is used on Open — grep to confirm exact name; it may be `manifest.Load` or `manifest.Open`).

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Volumes/HOME/code/storage-engines/wavesdb-backup && go test -run TestCheckpointObjStoreRoundTrip ./... -v`
Expected: PASS — the restored DB returns every key with the right value.

- [ ] **Step 5: Full package test + race**

Run: `go test ./... && go test -race -run 'TestSnapshot|TestSSTablesSince|TestCheckpoint|TestCheckpointObjStore' ./...`
Expected: PASS — no regressions across the engine, no races in the new primitives.

- [ ] **Step 6: Commit**

```bash
git add restore_objstore.go restore_objstore_test.go
git commit -m "feat(backup): RestoreFromObjectStore — download a checkpoint into an openable dir (round-trip)"
```

---

## Done criteria for Phase 1

- [ ] `AcquireSnapshot`/`Snapshot`, `SSTablesSince`, `CheckpointToObjectStore`, `RestoreFromObjectStore` all implemented with passing TDD tests, `-race` clean.
- [ ] `go test ./...` green in `wavesdb-backup` (no regressions).
- [ ] `waveSpan-backup` builds against the isolated engine (`go build ./...`).
- [ ] All work isolated on `wavesdb-backup` branch + `backup`-branch go.mod repoint; shared `../wavesdb` untouched.

These four primitives are the complete engine dependency for Phase 2 (the waveSpan logical backup core): `Snapshot` gives the consistent cut read view, `SSTablesSince` powers incrementals, and `CheckpointToObjectStore`/`RestoreFromObjectStore` are the physical-plane transport.
