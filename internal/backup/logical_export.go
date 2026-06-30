package backup

import (
	"bufio"
	"bytes"

	"github.com/yannick/wavespan/internal/storage"
)

// ExportLogical streams a node's authoritative state to an object store under
// keyPrefix and writes a versioned node manifest. Each authoritative CF is
// iterated over a single consistent snapshot and written to one object at
// <keyPrefix>/cf/<cfname> as a repeating length-prefixed (key,value) stream (no
// cf tag — the object IS the CF). Derived/transient CFs are never iterated: they
// are absent from reg.AuthoritativeCFs(). The storage UUID is recorded in the
// manifest (informational, for restore-time identity decisions); its CFSys key
// is still exported as data — restore decides whether to skip it.
//
// When sel is non-empty, it filters which keys are exported: each key is offered
// to the contributor that owns its CF (sel.Selects), so a partial backup includes
// only the chosen namespaces/graphs/collections (CFSys is always included). An
// empty sel exports everything (full backup) on the fast path.
func ExportLogical(src storage.LocalStore, store ObjectStore, keyPrefix string, reg *Registry, captureMs int64, sel Selector) (*NodeManifest, error) {
	// Storage identity: informational, read-only. A backup must not mutate its
	// source, so we look the UUID up rather than EnsureStorageUUID (which would
	// persist one when absent). uuid stays "" if the source has no identity yet.
	var uuid string
	if v, ok, _ := src.Get(storage.CFSys, []byte(storageIdentityKey)); ok {
		uuid = string(v)
	}

	// Owning contributor per CF (each CF is owned by exactly one), so the per-key
	// filter can consult the right matcher. Only built/used when a filter is active.
	filtering := !sel.IsEmpty()
	owner := map[storage.ColumnFamily]Contributor{}
	if filtering {
		for _, c := range reg.Contributors() {
			for _, s := range c.CFs() {
				owner[s.CF] = c
			}
		}
	}

	snap, err := src.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	var entries []CFEntry
	for _, cf := range reg.AuthoritativeCFs() {
		it, err := snap.Scan(cf, nil, nil, 0) // nil bounds = whole CF
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		var n, b int64
		for it.Valid() {
			k, v := it.Key(), it.Value()
			if filtering && !owner[cf].Selects(cf, k, sel) {
				it.Next()
				continue
			}
			writeBytes(bw, k)
			writeBytes(bw, v)
			n++
			b += int64(len(k) + len(v))
			it.Next()
		}
		cerr := it.Err()
		_ = it.Close()
		if cerr != nil {
			return nil, cerr
		}
		if err := bw.Flush(); err != nil {
			return nil, err
		}
		if n == 0 {
			continue // omit empty CFs from the manifest and the store
		}
		objKey := keyPrefix + "/cf/" + cf.Name()
		if err := store.Put(objKey, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
			return nil, err
		}
		entries = append(entries, CFEntry{CF: cf.Name(), Entries: n, Bytes: b})
	}

	man := &NodeManifest{
		FormatVersion:      manifestFormatVersion,
		CaptureWallClockMs: captureMs,
		StorageUUID:        uuid,
		CFs:                entries,
	}
	var mbuf bytes.Buffer
	if err := man.Encode(&mbuf); err != nil {
		return nil, err
	}
	if err := store.Put(keyPrefix+"/node.manifest.json", bytes.NewReader(mbuf.Bytes()), int64(mbuf.Len())); err != nil {
		return nil, err
	}
	return man, nil
}
