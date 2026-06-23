package collections

import (
	"encoding/binary"
	"errors"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
)

// subMeta is the reserved sub-space holding a shard's applied Raft index.
const subMeta byte = 0x00

// baseSM is the shared on-disk state-machine scaffolding for any Raft shard (data shards and the meta
// shard): it owns the per-shard CFReplData prefix, the applied-index key, and the snapshot/restart
// machinery. Embedders supply Update and Lookup. Snapshots capture the entire shard prefix (data +
// TTL index), so a recovered replica gets complete state.
type baseSM struct {
	store   storage.LocalStore
	shardID uint64
	prefix  []byte // 8-byte big-endian shardID
}

func newBaseSM(store storage.LocalStore, shardID uint64) baseSM {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, shardID)
	return baseSM{store: store, shardID: shardID, prefix: p}
}

func (b *baseSM) appliedKey() []byte {
	return append(append(append([]byte{}, b.prefix...), subMeta), []byte("applied")...)
}

// Open returns the most recently persisted applied Raft index for this shard (0 if new).
func (b *baseSM) Open(_ <-chan struct{}) (uint64, error) {
	v, found, err := b.store.Get(storage.CFReplData, b.appliedKey())
	if err != nil || !found {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("collections: corrupt applied-index value")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (b *baseSM) Sync() error { return b.store.Flush(storage.CFReplData) }

type snapState struct {
	snap    storage.Snapshot
	applied uint64
}

func (b *baseSM) PrepareSnapshot() (interface{}, error) {
	snap, err := b.store.Snapshot()
	if err != nil {
		return nil, err
	}
	applied := uint64(0)
	if v, found, gerr := snap.Get(storage.CFReplData, b.appliedKey()); gerr != nil {
		_ = snap.Close()
		return nil, gerr
	} else if found && len(v) == 8 {
		applied = binary.BigEndian.Uint64(v)
	}
	return &snapState{snap: snap, applied: applied}, nil
}

func (b *baseSM) SaveSnapshot(ctx interface{}, w io.Writer, stopc <-chan struct{}) error {
	st := ctx.(*snapState)
	defer func() { _ = st.snap.Close() }()
	if err := writeChunk(w, u64(st.applied)); err != nil {
		return err
	}
	it, err := st.snap.Scan(storage.CFReplData, b.prefix, prefixEnd(b.prefix), 0)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
		}
		if err := writeChunk(w, it.Key()[len(b.prefix):]); err != nil {
			return err
		}
		if err := writeChunk(w, it.Value()); err != nil {
			return err
		}
		it.Next()
	}
	return it.Err()
}

// RecoverFromSnapshot installs a snapshot stream (applied index + every key under the shard prefix).
// It first clears the shard prefix so recovery REPLACES rather than merges: a replica that fell behind
// and is caught up by a snapshot may already hold stale keys absent from the snapshot (e.g. set
// members removed since), and merging would leave them behind — diverging the data from the counter.
// Clear + install is idempotent, so a crash mid-recovery is re-run safely from the same snapshot.
func (b *baseSM) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) error {
	idxBytes, err := readChunk(r)
	if err != nil {
		return err
	}
	if err := b.clearPrefix(); err != nil {
		return err
	}
	ops := []storage.StoreOp{}
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		e := b.store.Batch(ops)
		ops = ops[:0]
		return e
	}
	for {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
		}
		suffix, rerr := readChunk(r)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
		val, verr := readChunk(r)
		if verr != nil {
			return verr
		}
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: append(append([]byte{}, b.prefix...), suffix...), Value: val})
		if len(ops) >= 1024 {
			if ferr := flush(); ferr != nil {
				return ferr
			}
		}
	}
	ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: b.appliedKey(), Value: idxBytes})
	return flush()
}

// clearPrefix deletes every key under the shard prefix (used before installing a snapshot). Keys are
// collected first, then deleted in batches, so the iterator is not mutated mid-scan.
func (b *baseSM) clearPrefix() error {
	it, err := b.store.Scan(storage.CFReplData, b.prefix, prefixEnd(b.prefix), 0)
	if err != nil {
		return err
	}
	var keys [][]byte
	for it.Valid() {
		keys = append(keys, append([]byte(nil), it.Key()...))
		it.Next()
	}
	serr := it.Err()
	_ = it.Close()
	if serr != nil {
		return serr
	}
	ops := make([]storage.StoreOp, 0, 1024)
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		e := b.store.Batch(ops)
		ops = ops[:0]
		return e
	}
	for _, k := range keys {
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Delete: true})
		if len(ops) >= 1024 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (b *baseSM) Close() error { return nil }

func u64(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}
