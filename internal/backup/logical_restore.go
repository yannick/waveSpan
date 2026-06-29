package backup

import (
	"bufio"
	"fmt"
	"io"
	"log"

	"github.com/yannick/wavespan/internal/storage"
)

// storageIdentityKey is the node-local storage identity key in CFSys. It mirrors
// storage.storageUUIDKey ("/sys/storage_uuid"), which is unexported. Restore
// skips this key so the target keeps its own identity (storage/identity.go).
const storageIdentityKey = "/sys/storage_uuid"

// knownColumnFamilies is the full set of logical CFs, used to resolve a manifest
// CF name back to its ColumnFamily (storage.cfNames is unexported, so we build
// the reverse lookup here).
var knownColumnFamilies = []storage.ColumnFamily{
	storage.CFSys, storage.CFKVData, storage.CFKVMeta,
	storage.CFGraphData, storage.CFGraphIndex,
	storage.CFVectorRaw, storage.CFVectorIndex,
	storage.CFReplLog, storage.CFCacheMeta, storage.CFReplData,
}

// cfByName resolves a wavesdb cf name string to its ColumnFamily.
func cfByName(name string) (storage.ColumnFamily, bool) {
	for _, cf := range knownColumnFamilies {
		if cf.Name() == name {
			return cf, true
		}
	}
	return 0, false
}

// RestoreLogical reads the node manifest at <keyPrefix>/node.manifest.json and
// raw-restores each authoritative CF object into dst via batched writes. It is a
// blind same-shape restore: (cf,key,value) is written as-is. The node-identity
// key (/sys/storage_uuid in CFSys) is skipped so the target keeps its own
// identity. After all CFs are restored, each contributor's RebuildAfterRestore
// hook is invoked (no-ops in Phase 2a — the extension seam Phase 2b fills).
func RestoreLogical(dst storage.LocalStore, store ObjectStore, keyPrefix string, reg *Registry, ri RestoreInfo) error {
	// 1. Read + version-guard the manifest.
	mr, err := store.Get(keyPrefix + "/node.manifest.json")
	if err != nil {
		return err
	}
	man, err := ReadNodeManifest(mr)
	_ = mr.Close()
	if err != nil {
		return err
	}
	if man.FormatVersion > manifestFormatVersion {
		return fmt.Errorf("backup: manifest format version %d is newer than supported %d", man.FormatVersion, manifestFormatVersion)
	}

	// 2. Restore each CF object.
	const batchSize = 1000
	for _, entry := range man.CFs {
		cf, ok := cfByName(entry.CF)
		if !ok {
			// Unknown CF (e.g. a future datatype). Full blind-restore of unknown
			// CFs is a Phase 3 concern; for 2a, log and skip.
			log.Printf("backup: RestoreLogical skipping unknown CF %q (not restorable in Phase 2a)", entry.CF)
			continue
		}
		if err := restoreCFObject(dst, store, keyPrefix+"/cf/"+entry.CF, cf, batchSize); err != nil {
			return err
		}
	}

	// 3. Invoke rebuild hooks (no-ops in 2a).
	for _, c := range reg.Contributors() {
		if err := c.RebuildAfterRestore(dst, ri); err != nil {
			return fmt.Errorf("backup: rebuild hook %q: %w", c.Name(), err)
		}
	}
	return nil
}

// restoreCFObject decodes one per-CF object (repeating length-prefixed
// key,value) and writes it into dst in batches, skipping the node-identity key.
func restoreCFObject(dst storage.LocalStore, store ObjectStore, objKey string, cf storage.ColumnFamily, batchSize int) error {
	rc, err := store.Get(objKey)
	if err != nil {
		return err
	}
	defer rc.Close()
	br := bufio.NewReader(rc)

	var ops []storage.StoreOp
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if err := dst.Batch(ops); err != nil {
			return err
		}
		ops = ops[:0]
		return nil
	}

	for {
		key, err := readBytes(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		val, err := readBytes(br)
		if err != nil {
			return err
		}
		// Preserve the target's own node identity.
		if cf == storage.CFSys && string(key) == storageIdentityKey {
			continue
		}
		ops = append(ops, storage.StoreOp{CF: cf, Key: key, Value: val})
		if len(ops) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
