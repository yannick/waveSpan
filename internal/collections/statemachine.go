package collections

import (
	"encoding/binary"
	"errors"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/cwire/wavespan/internal/storage"
)

// shardSM is the dragonboat on-disk state machine for one Raft shard (design/30 §5.3, Appendix B.3).
// It applies committed Set mutations into CFReplData under a per-shard prefix and writes the applied
// Raft index in the same atomic batch (exact crash recovery, no double-apply). Reads come off a
// consistent wavesdb Snapshot so Lookup / SaveSnapshot run safely concurrent with Update.
//
// Key layout within CFReplData (all under the 8-byte big-endian shard prefix):
//
//	<prefix>|subMeta|"applied"                 -> applied Raft index (uint64 BE)
//	<prefix>|subData|chunk(ns)|chunk(coll)|0x00 -> set cardinality (uint64 BE)
//	<prefix>|subData|chunk(ns)|chunk(coll)|0x01|<member> -> set member (empty value)
type shardSM struct {
	store   storage.LocalStore
	shardID uint64
	prefix  []byte // 8-byte big-endian shardID
}

const (
	subMeta byte = 0x00
	subData byte = 0x01

	scopeCard   byte = 0x00 // <collScope>|scopeCard
	scopeMember byte = 0x01 // <collScope>|scopeMember|<member>
)

func newShardSM(store storage.LocalStore, shardID uint64) *shardSM {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, shardID)
	return &shardSM{store: store, shardID: shardID, prefix: p}
}

func (s *shardSM) appliedKey() []byte {
	return append(append(append([]byte{}, s.prefix...), subMeta), []byte("applied")...)
}

func (s *shardSM) dataSpace() []byte { return append(append([]byte{}, s.prefix...), subData) }

func (s *shardSM) collScope(ns, coll []byte) []byte {
	out := s.dataSpace()
	out = appendChunk(out, ns)
	return appendChunk(out, coll)
}

func (s *shardSM) cardKey(ns, coll []byte) []byte { return append(s.collScope(ns, coll), scopeCard) }

func (s *shardSM) memberPrefix(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeMember)
}

func (s *shardSM) memberKey(ns, coll, member []byte) []byte {
	return append(s.memberPrefix(ns, coll), member...)
}

// --- read queries (passed in-process to Lookup; no serialization) ---

type isMemberQuery struct{ NS, Coll, Member []byte }
type cardQuery struct{ NS, Coll []byte }
type membersQuery struct {
	NS, Coll []byte
	Limit    int
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

// Update applies a committed batch of Set mutations atomically with the new applied index. It keeps an
// in-batch membership overlay so multiple entries touching the same member in one Update are counted
// and applied correctly (determinism), and accumulates the per-collection cardinality delta.
func (s *shardSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	var ops []storage.StoreOp
	pending := map[string]bool{}    // memberKey -> exists after this batch so far
	cardDelta := map[string]int64{} // string(cardKey) -> delta

	exists := func(k []byte) (bool, error) {
		if v, ok := pending[string(k)]; ok {
			return v, nil
		}
		_, found, err := s.store.Get(storage.CFReplData, k)
		return found, err
	}

	for i := range entries {
		c, err := decodeCommand(entries[i].Cmd)
		if err != nil {
			return nil, err
		}
		ck := string(s.cardKey(c.NS, c.Coll))
		var changed int64
		for _, m := range c.Members {
			mk := s.memberKey(c.NS, c.Coll, m)
			present, err := exists(mk)
			if err != nil {
				return nil, err
			}
			switch c.Op {
			case opSAdd:
				if !present {
					ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: mk, Value: []byte{}})
					pending[string(mk)] = true
					changed++
				}
			case opSRem:
				if present {
					ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: mk, Delete: true})
					pending[string(mk)] = false
					changed++
				}
			}
		}
		if c.Op == opSRem {
			cardDelta[ck] -= changed
		} else {
			cardDelta[ck] += changed
		}
		entries[i].Result = sm.Result{Value: uint64(changed)}
	}

	for ckStr, delta := range cardDelta {
		if delta == 0 {
			continue
		}
		ck := []byte(ckStr)
		cur, err := s.readCard(ck)
		if err != nil {
			return nil, err
		}
		nv := int64(cur) + delta
		if nv < 0 {
			nv = 0
		}
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: ck, Value: u64(uint64(nv))})
	}

	ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.appliedKey(), Value: u64(entries[len(entries)-1].Index)})
	if err := s.store.Batch(ops); err != nil {
		return nil, err
	}
	return entries, nil
}

// Lookup answers a read query from a consistent snapshot.
func (s *shardSM) Lookup(query interface{}) (interface{}, error) {
	snap, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snap.Close() }()

	switch q := query.(type) {
	case isMemberQuery:
		_, found, err := snap.Get(storage.CFReplData, s.memberKey(q.NS, q.Coll, q.Member))
		return found, err
	case cardQuery:
		v, found, err := snap.Get(storage.CFReplData, s.cardKey(q.NS, q.Coll))
		if err != nil || !found {
			return uint64(0), err
		}
		if len(v) != 8 {
			return uint64(0), errors.New("collections: corrupt card value")
		}
		return binary.BigEndian.Uint64(v), nil
	case membersQuery:
		mp := s.memberPrefix(q.NS, q.Coll)
		it, err := snap.Scan(storage.CFReplData, mp, prefixEnd(mp), q.Limit)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var out [][]byte
		for it.Valid() {
			out = append(out, append([]byte(nil), it.Key()[len(mp):]...))
			it.Next()
		}
		return out, it.Err()
	default:
		return nil, errors.New("collections: unknown lookup query")
	}
}

func (s *shardSM) readCard(cardKey []byte) (uint64, error) {
	v, found, err := s.store.Get(storage.CFReplData, cardKey)
	if err != nil || !found {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("collections: corrupt card value")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *shardSM) Sync() error { return s.store.Flush(storage.CFReplData) }

// snapState is the point-in-time identifier returned by PrepareSnapshot: a consistent wavesdb view
// plus the applied index at that point, so a recovered replica resumes from the right index.
type snapState struct {
	snap    storage.Snapshot
	applied uint64
}

func (s *shardSM) PrepareSnapshot() (interface{}, error) {
	snap, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	applied := uint64(0)
	if v, found, gerr := snap.Get(storage.CFReplData, s.appliedKey()); gerr != nil {
		_ = snap.Close()
		return nil, gerr
	} else if found && len(v) == 8 {
		applied = binary.BigEndian.Uint64(v)
	}
	return &snapState{snap: snap, applied: applied}, nil
}

// SaveSnapshot streams the applied index, then every key/value in the shard's data space, from the
// captured point-in-time view (safe to run concurrent with Update).
func (s *shardSM) SaveSnapshot(ctx interface{}, w io.Writer, stopc <-chan struct{}) error {
	st := ctx.(*snapState)
	defer func() { _ = st.snap.Close() }()
	if err := writeChunk(w, u64(st.applied)); err != nil {
		return err
	}
	sp := s.dataSpace()
	it, err := st.snap.Scan(storage.CFReplData, sp, prefixEnd(sp), 0)
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
		if err := writeChunk(w, it.Key()[len(sp):]); err != nil {
			return err
		}
		if err := writeChunk(w, it.Value()); err != nil {
			return err
		}
		it.Next()
	}
	return it.Err()
}

// RecoverFromSnapshot installs a snapshot stream (applied index + data) for the shard. TODO(M-B): for
// an already-populated replica, clear the shard's data space before applying so recovery replaces
// rather than merges; a fresh learner is empty so M-A merges safely.
func (s *shardSM) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) error {
	idxBytes, err := readChunk(r)
	if err != nil {
		return err
	}
	sp := s.dataSpace()
	ops := []storage.StoreOp{}
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
		key := append(append([]byte{}, sp...), suffix...)
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: key, Value: val})
		if len(ops) >= 1024 {
			if ferr := flush(); ferr != nil {
				return ferr
			}
		}
	}
	ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.appliedKey(), Value: idxBytes})
	return flush()
}

// Close is a no-op: the wavesdb store is owned by the Manager, not the SM.
func (s *shardSM) Close() error { return nil }

func u64(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}
