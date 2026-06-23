package collections

import (
	"encoding/binary"
	"errors"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/cwire/wavespan/internal/storage"
)

// shardSM is the dragonboat on-disk state machine for one Raft shard (design/30 §5.3, Appendix B.3).
// It persists applied state into CFReplData under a per-shard prefix and writes the applied Raft index
// in the same atomic batch as the data, so crash recovery resumes exactly (Open returns the persisted
// index, no double-apply). Reads come off a consistent wavesdb Snapshot so Lookup / SaveSnapshot run
// safely concurrent with Update.
type shardSM struct {
	store   storage.LocalStore
	shardID uint64
	prefix  []byte // 8-byte big-endian shardID
}

const (
	subMeta byte = 0x00 // <prefix>|subMeta|"applied"
	subData byte = 0x01 // <prefix>|subData|<userKey>
)

func newShardSM(store storage.LocalStore, shardID uint64) *shardSM {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, shardID)
	return &shardSM{store: store, shardID: shardID, prefix: p}
}

func (s *shardSM) appliedKey() []byte {
	out := append(append([]byte{}, s.prefix...), subMeta)
	return append(out, []byte("applied")...)
}

func (s *shardSM) dataPrefix() []byte {
	return append(append([]byte{}, s.prefix...), subData)
}

func (s *shardSM) dataKey(userKey []byte) []byte {
	return append(s.dataPrefix(), userKey...)
}

// Open returns the most recently persisted applied Raft index for this shard (0 if new).
func (s *shardSM) Open(stopc <-chan struct{}) (uint64, error) {
	v, found, err := s.store.Get(storage.CFReplData, s.appliedKey())
	if err != nil || !found {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("collections: corrupt applied-index value")
	}
	return binary.BigEndian.Uint64(v), nil
}

// Update applies a committed batch atomically with the new applied index.
func (s *shardSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	ops := make([]storage.StoreOp, 0, len(entries)+1)
	for i := range entries {
		c, err := decodeCommand(entries[i].Cmd)
		if err != nil {
			return nil, err
		}
		switch c.Op {
		case opPut:
			ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.dataKey(c.Key), Value: c.Val})
		case opDelete:
			ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.dataKey(c.Key), Delete: true})
		default:
			return nil, errors.New("collections: unknown op")
		}
		entries[i].Result = sm.Result{Value: 1}
	}
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], entries[len(entries)-1].Index)
	ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.appliedKey(), Value: idx[:]})
	if err := s.store.Batch(ops); err != nil {
		return nil, err
	}
	return entries, nil
}

// Lookup reads a key from a consistent snapshot (query is the raw user key bytes).
func (s *shardSM) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.([]byte)
	if !ok {
		return nil, errors.New("collections: lookup query must be []byte key")
	}
	snap, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snap.Close() }()
	v, found, err := snap.Get(storage.CFReplData, s.dataKey(key))
	if err != nil || !found {
		return nil, err
	}
	return append([]byte(nil), v...), nil
}

func (s *shardSM) Sync() error { return s.store.Flush(storage.CFReplData) }

// PrepareSnapshot captures a point-in-time view; SaveSnapshot streams it concurrently with Update.
func (s *shardSM) PrepareSnapshot() (interface{}, error) {
	return s.store.Snapshot()
}

func (s *shardSM) SaveSnapshot(ctx interface{}, w io.Writer, stopc <-chan struct{}) error {
	snap := ctx.(storage.Snapshot)
	defer func() { _ = snap.Close() }()
	dp := s.dataPrefix()
	it, err := snap.Scan(storage.CFReplData, dp, prefixEnd(dp), 0)
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
		userKey := it.Key()[len(dp):]
		if err := writeChunk(w, userKey); err != nil {
			return err
		}
		if err := writeChunk(w, it.Value()); err != nil {
			return err
		}
		it.Next()
	}
	return it.Err()
}

func (s *shardSM) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) error {
	var ops []storage.StoreOp
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		err := s.store.Batch(ops)
		ops = ops[:0]
		return err
	}
	for {
		select {
		case <-stopc:
			return sm.ErrSnapshotStopped
		default:
		}
		userKey, err := readChunk(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		val, err := readChunk(r)
		if err != nil {
			return err
		}
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.dataKey(userKey), Value: val})
		if len(ops) >= 1024 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// Close is a no-op: the wavesdb store is owned by the Manager, not the SM.
func (s *shardSM) Close() error { return nil }
