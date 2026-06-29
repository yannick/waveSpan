package backup

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
)

// be8 encodes v as an 8-byte big-endian shard prefix (the CFReplData key prefix).
func be8(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// reshardKey computes the re-routed CFReplData key for a row under target shard
// count n, or reports that the row should be dropped (keep=false, nil err).
//
// It drops two kinds of rows the target cluster rebuilds for itself:
//   - meta/system-shard rows (shardID < collections.FirstDataShard) — the meta
//     shard shares CFReplData; its range directory (be8(1)|subData|<rangeStart>,
//     whose initial [-inf,+inf) range has an EMPTY routing body that RerouteSuffix
//     could not decode) and its applied index are re-established at bootstrap. This
//     guard runs BEFORE RerouteSuffix precisely because RerouteSuffix sees only the
//     suffix and cannot tell a meta-shard row from a data-shard row.
//   - shard-local bookkeeping on data shards (subMeta/subDedup/subDedupRing), via
//     RerouteSuffix returning keep=false.
//
// It errors only on a genuinely un-routable data-shard row (unknown sub-prefix or
// corrupt suffix), so re-shard fails loudly rather than misrouting data.
func reshardKey(key []byte, n uint64) (newKey []byte, keep bool, err error) {
	if len(key) < 8 {
		return nil, false, fmt.Errorf("backup: CFReplData key too short to re-shard (%d bytes)", len(key))
	}
	if binary.BigEndian.Uint64(key[:8]) < collections.FirstDataShard {
		return nil, false, nil // meta/system shard: not a data shard
	}
	suffix := key[8:]
	newShard, keep, err := collections.RerouteSuffix(suffix, n)
	if err != nil {
		return nil, false, err
	}
	if !keep {
		return nil, false, nil
	}
	return append(be8(newShard), suffix...), true, nil
}

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
	if man.FormatVersion < 1 || man.FormatVersion > manifestFormatVersion {
		return fmt.Errorf("backup: unsupported manifest format version %d (supported range 1..%d)", man.FormatVersion, manifestFormatVersion)
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
		if err := restoreCFObject(dst, store, keyPrefix+"/cf/"+entry.CF, cf, entry.Entries, batchSize, ri); err != nil {
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
// It cross-checks the number of decoded pairs against wantEntries (the manifest's
// recorded count) and errors on mismatch: readBytes returns a clean io.EOF after a
// complete pair, so without this check a truncated tail would restore silently.
// The decoded count includes the skipped identity key (which is in the manifest
// count but intentionally not applied), so it is compared, not the applied count.
//
// When cf is CFReplData and ri.CollectionsDataShards > 0, each row is re-routed to
// its shard under the target N (re-shard): the 8-byte shard prefix is rewritten via
// collections.RerouteSuffix; shard-local rows (subMeta/dedup) are dropped; an
// unknown sub-prefix aborts the restore. decoded is still counted before any
// drop/skip, so the entry-count integrity check is unaffected by re-routing.
func restoreCFObject(dst storage.LocalStore, store ObjectStore, objKey string, cf storage.ColumnFamily, wantEntries int64, batchSize int, ri RestoreInfo) error {
	rc, err := store.Get(objKey)
	if err != nil {
		return err
	}
	defer rc.Close()
	br := bufio.NewReader(rc)

	reshard := cf == storage.CFReplData && ri.CollectionsDataShards > 0

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

	var decoded int64
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
		decoded++
		// Preserve the target's own node identity.
		if cf == storage.CFSys && string(key) == storageIdentityKey {
			continue
		}
		if reshard {
			newKey, keep, err := reshardKey(key, ri.CollectionsDataShards)
			if err != nil {
				return fmt.Errorf("backup: re-shard CFReplData: %w", err)
			}
			if !keep {
				continue // meta/system shard or shard-local bookkeeping; rebuilt fresh
			}
			key = newKey
		}
		ops = append(ops, storage.StoreOp{CF: cf, Key: key, Value: val})
		if len(ops) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if decoded != wantEntries {
		return fmt.Errorf("backup: CF %q entry-count mismatch: manifest records %d, object decoded %d (truncated or corrupt)", cf.Name(), wantEntries, decoded)
	}
	return flush()
}
