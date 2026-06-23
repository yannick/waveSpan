package collections

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/cwire/wavespan/internal/storage"
)

// shardSM is the dragonboat on-disk state machine for one Raft shard (design/30 §5.3, Appendix B.3).
// It applies committed mutations into CFReplData under a per-shard prefix and writes the applied Raft
// index in the same atomic batch (exact crash recovery). Reads come off a consistent wavesdb Snapshot
// so Lookup / SaveSnapshot run safely concurrent with Update. Each collection has a fixed type
// (set/hash/zset) recorded in a header; type-mismatched ops fail WRONGTYPE.
//
// Key layout within CFReplData (under the 8-byte big-endian shard prefix; collScope =
// subData|chunk(ns)|chunk(coll)):
//
//	<prefix>|subMeta|"applied"           -> applied Raft index (uint64 BE)
//	<collScope>|scopeType                -> collection type (1 byte)
//	<collScope>|scopeCard                -> element count (uint64 BE)
//	<collScope>|scopeElem|<member|field> -> set member / hash field(value) (set/hash)
//	<collScope>|scopeElem|<sortScore>|<member> -> zset score-ordered index (empty)
//	<collScope>|scopeZPtr|<member>       -> zset member->score (float64 BE bits)
type shardSM struct {
	store   storage.LocalStore
	shardID uint64
	prefix  []byte
}

const (
	subMeta byte = 0x00
	subData byte = 0x01

	scopeCard byte = 0x00
	scopeElem byte = 0x01
	scopeZPtr byte = 0x02
	scopeType byte = 0x03
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
func (s *shardSM) typeKey(ns, coll []byte) []byte { return append(s.collScope(ns, coll), scopeType) }
func (s *shardSM) elemPrefix(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeElem)
}
func (s *shardSM) elemKey(ns, coll, member []byte) []byte {
	return append(s.elemPrefix(ns, coll), member...)
}
func (s *shardSM) zscoreKey(ns, coll []byte, score float64, member []byte) []byte {
	return append(append(s.elemPrefix(ns, coll), sortableScore(score)...), member...)
}
func (s *shardSM) zptrKey(ns, coll, member []byte) []byte {
	return append(append(s.collScope(ns, coll), scopeZPtr), member...)
}

// --- read queries (passed in-process to Lookup; no serialization) ---

type isMemberQuery struct{ NS, Coll, Member []byte }
type cardQuery struct{ NS, Coll []byte }
type membersQuery struct {
	NS, Coll []byte
	Limit    int
}
type hGetQuery struct{ NS, Coll, Field []byte }
type hGetAllQuery struct {
	NS, Coll []byte
	Limit    int
}
type zScoreQuery struct{ NS, Coll, Member []byte }
type zRangeQuery struct {
	NS, Coll []byte
	Limit    int
}

// FieldValue is a hash field/value pair (HGETALL).
type FieldValue struct{ Field, Value []byte }

// ScoredMember is a sorted-set member with its score (ZRANGE).
type ScoredMember struct {
	Member []byte
	Score  float64
}

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

// updateCtx carries the in-batch overlays so multiple entries in one Update see each other's effects.
type updateCtx struct {
	s         *shardSM
	ops       []storage.StoreOp
	exists    map[string]bool     // set/hash element key -> exists-after
	zscore    map[string]*float64 // zset member ptr key -> current score (nil = absent)
	cardDelta map[string]int64    // string(cardKey) -> delta
	htype     map[string]collType // string(collScope) -> resolved type (0 = unknown/new)
}

func (u *updateCtx) elemExists(k []byte) (bool, error) {
	if v, ok := u.exists[string(k)]; ok {
		return v, nil
	}
	_, found, err := u.s.store.Get(storage.CFReplData, k)
	return found, err
}

func (u *updateCtx) zScore(ns, coll, member []byte) (float64, bool, error) {
	pk := u.s.zptrKey(ns, coll, member)
	if v, ok := u.zscore[string(pk)]; ok {
		if v == nil {
			return 0, false, nil
		}
		return *v, true, nil
	}
	raw, found, err := u.s.store.Get(storage.CFReplData, pk)
	if err != nil || !found {
		return 0, false, err
	}
	return float64FromBits(raw), true, nil
}

// ensureType resolves/sets the collection type; returns false (WRONGTYPE) on a mismatch.
func (u *updateCtx) ensureType(ns, coll []byte, want collType) (bool, error) {
	cs := string(u.s.collScope(ns, coll))
	if t, ok := u.htype[cs]; ok {
		return t == want, nil
	}
	v, found, err := u.s.store.Get(storage.CFReplData, u.s.typeKey(ns, coll))
	if err != nil {
		return false, err
	}
	if found {
		t := collType(v[0])
		u.htype[cs] = t
		return t == want, nil
	}
	// new collection: set the header now
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.typeKey(ns, coll), Value: []byte{byte(want)}})
	u.htype[cs] = want
	return true, nil
}

func (s *shardSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	u := &updateCtx{
		s: s, exists: map[string]bool{}, zscore: map[string]*float64{},
		cardDelta: map[string]int64{}, htype: map[string]collType{},
	}
	for i := range entries {
		c, err := decodeCommand(entries[i].Cmd)
		if err != nil {
			return nil, err
		}
		ok, err := u.ensureType(c.NS, c.Coll, typeForOp(c.Op))
		if err != nil {
			return nil, err
		}
		if !ok {
			entries[i].Result = sm.Result{Value: 0, Data: wrongType}
			continue
		}
		changed, err := u.applyCommand(c)
		if err != nil {
			return nil, err
		}
		entries[i].Result = sm.Result{Value: uint64(changed)}
	}
	// flush card deltas
	for ckStr, delta := range u.cardDelta {
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
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ck, Value: u64(uint64(nv))})
	}
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: s.appliedKey(), Value: u64(entries[len(entries)-1].Index)})
	if err := s.store.Batch(u.ops); err != nil {
		return nil, err
	}
	return entries, nil
}

// applyCommand applies one command's items via the overlays and returns the count of changes
// (Redis-style: added members / new fields / new zset members / removed elements).
func (u *updateCtx) applyCommand(c command) (int64, error) {
	s := u.s
	ck := string(s.cardKey(c.NS, c.Coll))
	var changed int64
	for _, it := range c.Items {
		switch c.Op {
		case opSAdd:
			ek := s.elemKey(c.NS, c.Coll, it.Key)
			present, err := u.elemExists(ek)
			if err != nil {
				return 0, err
			}
			if !present {
				u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ek, Value: []byte{}})
				u.exists[string(ek)] = true
				u.cardDelta[ck]++
				changed++
			}
		case opSRem, opHDel:
			ek := s.elemKey(c.NS, c.Coll, it.Key)
			present, err := u.elemExists(ek)
			if err != nil {
				return 0, err
			}
			if present {
				u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ek, Delete: true})
				u.exists[string(ek)] = false
				u.cardDelta[ck]--
				changed++
			}
		case opHSet:
			ek := s.elemKey(c.NS, c.Coll, it.Key)
			present, err := u.elemExists(ek)
			if err != nil {
				return 0, err
			}
			u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ek, Value: it.Val})
			u.exists[string(ek)] = true
			if !present {
				u.cardDelta[ck]++
				changed++ // HSET returns the number of NEW fields
			}
		case opZAdd:
			old, had, err := u.zScore(c.NS, c.Coll, it.Key)
			if err != nil {
				return 0, err
			}
			if had {
				u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: s.zscoreKey(c.NS, c.Coll, old, it.Key), Delete: true})
			} else {
				u.cardDelta[ck]++
				changed++ // ZADD returns the number of NEW members
			}
			u.ops = append(u.ops,
				storage.StoreOp{CF: storage.CFReplData, Key: s.zscoreKey(c.NS, c.Coll, it.Score, it.Key), Value: []byte{}},
				storage.StoreOp{CF: storage.CFReplData, Key: s.zptrKey(c.NS, c.Coll, it.Key), Value: bitsOf(it.Score)})
			sc := it.Score
			u.zscore[string(s.zptrKey(c.NS, c.Coll, it.Key))] = &sc
		case opZRem:
			old, had, err := u.zScore(c.NS, c.Coll, it.Key)
			if err != nil {
				return 0, err
			}
			if had {
				u.ops = append(u.ops,
					storage.StoreOp{CF: storage.CFReplData, Key: s.zscoreKey(c.NS, c.Coll, old, it.Key), Delete: true},
					storage.StoreOp{CF: storage.CFReplData, Key: s.zptrKey(c.NS, c.Coll, it.Key), Delete: true})
				u.zscore[string(s.zptrKey(c.NS, c.Coll, it.Key))] = nil
				u.cardDelta[ck]--
				changed++
			}
		}
	}
	return changed, nil
}

func (s *shardSM) Lookup(query interface{}) (interface{}, error) {
	snap, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snap.Close() }()

	switch q := query.(type) {
	case isMemberQuery:
		_, found, err := snap.Get(storage.CFReplData, s.elemKey(q.NS, q.Coll, q.Member))
		return found, err
	case cardQuery:
		return s.snapCard(snap, q.NS, q.Coll)
	case membersQuery:
		mp := s.elemPrefix(q.NS, q.Coll)
		return scanSuffixes(snap, mp, q.Limit)
	case hGetQuery:
		v, found, err := snap.Get(storage.CFReplData, s.elemKey(q.NS, q.Coll, q.Field))
		if err != nil || !found {
			return nil, err // nil result = absent
		}
		return append([]byte(nil), v...), nil
	case hGetAllQuery:
		mp := s.elemPrefix(q.NS, q.Coll)
		it, err := snap.Scan(storage.CFReplData, mp, prefixEnd(mp), q.Limit)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var out []FieldValue
		for it.Valid() {
			out = append(out, FieldValue{
				Field: append([]byte(nil), it.Key()[len(mp):]...),
				Value: append([]byte(nil), it.Value()...),
			})
			it.Next()
		}
		return out, it.Err()
	case zScoreQuery:
		raw, found, err := snap.Get(storage.CFReplData, s.zptrKey(q.NS, q.Coll, q.Member))
		if err != nil || !found {
			return nil, err
		}
		sc := float64FromBits(raw)
		return &sc, nil
	case zRangeQuery:
		mp := s.elemPrefix(q.NS, q.Coll)
		it, err := snap.Scan(storage.CFReplData, mp, prefixEnd(mp), q.Limit)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var out []ScoredMember
		for it.Valid() {
			k := it.Key()[len(mp):] // sortableScore(8) || member
			if len(k) >= 8 {
				out = append(out, ScoredMember{
					Score:  unsortableScore(k[:8]),
					Member: append([]byte(nil), k[8:]...),
				})
			}
			it.Next()
		}
		return out, it.Err()
	default:
		return nil, errors.New("collections: unknown lookup query")
	}
}

func (s *shardSM) snapCard(snap storage.Snapshot, ns, coll []byte) (uint64, error) {
	v, found, err := snap.Get(storage.CFReplData, s.cardKey(ns, coll))
	if err != nil || !found {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("collections: corrupt card value")
	}
	return binary.BigEndian.Uint64(v), nil
}

func scanSuffixes(snap storage.Snapshot, prefix []byte, limit int) ([][]byte, error) {
	it, err := snap.Scan(storage.CFReplData, prefix, prefixEnd(prefix), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out [][]byte
	for it.Valid() {
		out = append(out, append([]byte(nil), it.Key()[len(prefix):]...))
		it.Next()
	}
	return out, it.Err()
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

// RecoverFromSnapshot installs a snapshot stream (applied index + data). TODO(M-B): clear the shard's
// data space first so recovery replaces rather than merges for an already-populated replica.
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
		ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: append(append([]byte{}, sp...), suffix...), Value: val})
		if len(ops) >= 1024 {
			if ferr := flush(); ferr != nil {
				return ferr
			}
		}
	}
	ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: s.appliedKey(), Value: idxBytes})
	return flush()
}

func (s *shardSM) Close() error { return nil }

func u64(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func bitsOf(f float64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	return b
}
func float64FromBits(b []byte) float64 { return math.Float64frombits(binary.BigEndian.Uint64(b)) }
