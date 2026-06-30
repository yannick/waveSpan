package collections

import (
	"encoding/binary"

	"github.com/yannick/wavespan/internal/storage"
)

// StripRaftBookkeeping removes collections raft bookkeeping from a restored CFReplData so the subsequent
// FRESH collections bootstrap starts at applied-index 0 with a new dragonboat LogDB (design/backup §5.0).
// The restored CFReplData data rows become the initial state-machine state; the LogDB is never carried,
// so a stale applied index or dedup window must not survive. It deletes:
//   - every meta/system-shard row (shardID < firstDataShard) — the range directory + applied index are
//     re-established by the bootstrap that follows;
//   - per-data-shard subMeta (applied index), subDedup and subDedupRing (idempotency window/ring) rows.
//
// It KEEPS data rows (subData) and the routable secondary indexes (subTTL/subBudExp/subBudTombGC). It is
// idempotent and safe to run after either restore path (logical re-shard already drops these; same-shape
// restore relies on this pass). Deletes are batched.
//
// NOTE (backup catalog reset): dropping every meta-shard row (shardID 1) also clears the BackupIntent
// catalog stored under the meta shard's subBackup sub-space — so a restored/cloned cluster starts with NO
// backup history or schedule. The S3 backup objects themselves are untouched; the operator re-registers
// backup intents/schedule post-restore. For a clone this is correct (don't inherit the source's
// schedule); for same-cluster DR the catalog can be rebuilt by listing S3 or re-registering.
func StripRaftBookkeeping(store storage.LocalStore) error {
	it, err := store.Scan(storage.CFReplData, nil, nil, 0)
	if err != nil {
		return err
	}
	var keys [][]byte
	for it.Valid() {
		if isRaftBookkeepingKey(it.Key()) {
			keys = append(keys, append([]byte(nil), it.Key()...))
		}
		it.Next()
	}
	serr := it.Err()
	_ = it.Close()
	if serr != nil {
		return serr
	}

	const batch = 1000
	ops := make([]storage.StoreOp, 0, batch)
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		err := store.Batch(ops)
		ops = ops[:0]
		return err
	}
	for _, k := range keys {
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Delete: true})
		if len(ops) >= batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// isRaftBookkeepingKey reports whether a CFReplData key is collections raft bookkeeping that a restore
// must drop (meta/system-shard rows, or per-shard applied-index/dedup rows).
func isRaftBookkeepingKey(key []byte) bool {
	if len(key) < 8 {
		return false
	}
	if binary.BigEndian.Uint64(key[:8]) < firstDataShard {
		return true // meta/system shard: re-bootstrapped fresh
	}
	suffix := key[8:]
	if len(suffix) == 0 {
		return false
	}
	switch suffix[0] {
	case subMeta, subDedup, subDedupRing:
		return true
	}
	return false
}
