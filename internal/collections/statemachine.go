package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
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
	baseSM
}

const (
	subData byte = 0x01

	scopeCard byte = 0x00
	scopeElem byte = 0x01
	scopeZPtr byte = 0x02
	scopeType byte = 0x03
)

func newShardSM(store storage.LocalStore, shardID uint64) *shardSM {
	return &shardSM{baseSM: newBaseSM(store, shardID)}
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
type cardCheckQuery struct{ NS, Coll []byte }
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
type collectionsQuery struct{ NS []byte }     // list collection names in a namespace (design/30 §13.7)
type collectionInfosQuery struct{ NS []byte } // list collection names + their datatype in a namespace

// CollInfo is a collection's name paired with its datatype (set/hash/zset), as recorded in its header.
type CollInfo struct {
	Name []byte
	Type collType
}

// CardCheck reports the stored cardinality counter against the actual element count, read from one
// consistent snapshot. They must always be equal — an internal invariant probe for tests/ops.
type CardCheck struct{ Stored, Counted uint64 }

// FieldValue is a hash field/value pair (HGETALL).
type FieldValue struct{ Field, Value []byte }

// ScoredMember is a sorted-set member with its score (ZRANGE).
type ScoredMember struct {
	Member []byte
	Score  float64
}

// updateCtx carries the in-batch overlays so multiple entries in one Update see each other's effects.
type updateCtx struct {
	s         *shardSM
	ops       []storage.StoreOp
	exists    map[string]bool     // set/hash element key -> exists-after
	zscore    map[string]*float64 // zset member ptr key -> current score (nil = absent)
	cardDelta map[string]int64    // string(cardKey) -> delta
	htype     map[string]collType // string(collScope) -> resolved type (0 = unknown/new)
	vals      map[string][]byte   // hash field elemKey -> value-after (for HIncr; nil = deleted)
	dedupSeq  uint64              // idempotency ring sequence (design/30 §13.12)
	// inBatchDedup mirrors dedup records created earlier in THIS Update batch. dedupGet reads the store,
	// which does not see un-flushed in-batch records, so without this overlay two identical keyed writes
	// coalesced into one entry (QW2) would both apply — breaking idempotency for non-idempotent ops
	// (HIncrBy). Checking it first makes coalesced keyed writes dedup exactly like un-coalesced ones.
	inBatchDedup map[string]ProposeResult
}

func (u *updateCtx) elemExists(k []byte) (bool, error) {
	if v, ok := u.exists[string(k)]; ok {
		return v, nil
	}
	_, found, err := u.s.getData(k)
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
	raw, found, err := u.s.getData(pk)
	if err != nil || !found {
		return 0, false, err
	}
	return float64FromBits(raw), true, nil
}

// typeOf reads a collection's type without creating it (0 = absent), honoring the in-batch overlay.
func (u *updateCtx) typeOf(ns, coll []byte) (collType, error) {
	cs := string(u.s.collScope(ns, coll))
	if t, ok := u.htype[cs]; ok {
		return t, nil
	}
	v, found, err := u.s.getData(u.s.typeKey(ns, coll))
	if err != nil || !found || len(v) == 0 {
		return 0, err
	}
	t := collType(v[0])
	u.htype[cs] = t
	return t, nil
}

// ensureType resolves/sets the collection type; returns false (WRONGTYPE) on a mismatch.
func (u *updateCtx) ensureType(ns, coll []byte, want collType) (bool, error) {
	cs := string(u.s.collScope(ns, coll))
	if t, ok := u.htype[cs]; ok {
		return t == want, nil
	}
	v, found, err := u.s.getData(u.s.typeKey(ns, coll))
	if err != nil {
		return false, err
	}
	if found && len(v) > 0 {
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
		cardDelta: map[string]int64{}, htype: map[string]collType{}, vals: map[string][]byte{},
		inBatchDedup: map[string]ProposeResult{},
	}
	// Frozen ranges are read once per batch; a freeze committed in an earlier batch (Control.Split
	// proposes it before migrating) rejects subrange mutations here. Same-batch writes still commit but
	// are captured by the post-freeze migrateScan, so none are lost.
	frozen, err := s.loadFrozen()
	if err != nil {
		return nil, err
	}
	if u.dedupSeq, err = s.readDedupSeq(); err != nil {
		return nil, err
	}
	startDedupSeq := u.dedupSeq
	var scratch []item             // reused across entries to decode Items without a per-entry alloc (A1)
	var subResults []ProposeResult // reused result scratch for opBatch expansion (QW2)
	for i := range entries {
		// CRITICAL ROBUSTNESS: applying a committed entry must never return an error or panic for any
		// reason attributable to the entry's bytes (malformed/truncated/corrupt) — dragonboat treats an
		// Update error as fatal and the poison entry replays into a crash-loop. So each entry is applied
		// under a recover() with its accumulated effects snapshotted: a non-fatal (decode/corruption)
		// failure OR a panic rolls the entry back and SKIPS it deterministically (every replica skips the
		// same bytes, so state stays consistent), leaving a benign result. Only a genuine storage fault
		// (wrapped fatalErr) still propagates and stops this replica.
		snap := u.snapshot()
		res, err := s.applyEntrySafe(u, &entries[i], frozen, scratch, &subResults)
		if err != nil {
			if isFatal(err) {
				return nil, err // genuine storage fault — must stop this replica
			}
			u.restore(snap) // discard any partial effects from this entry
			logCorruptEntry(corruptEntry{index: entries[i].Index, err: err})
			entries[i].Result = sm.Result{} // benign: applied nothing, continue
			continue
		}
		entries[i].Result = res
	}
	if u.dedupSeq != startDedupSeq {
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: s.dedupSeqKey(), Value: u64(u.dedupSeq)})
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
	// BatchRC (ReadCommitted) not Batch (Snapshot): the SM apply is a deterministic, authoritative
	// write of shard-prefixed keys and orders same-key writes itself (the in-batch overlays), so it
	// needs no write-write conflict check. Under N concurrent data-shard applies to the shared store,
	// the SI check spuriously aborted with ErrConflict — which dragonboat treats as fatal and panicked
	// the node. BatchRC lets the independent shards commit in parallel without that false conflict.
	if err := s.store.BatchRC(u.ops); err != nil {
		return nil, fatal(err)
	}
	return entries, nil
}

// applyOne decodes and applies a single encoded command (the shared body for both a top-level entry and
// a coalesced opBatch sub-command, QW2). It honors freeze, idempotency dedup, type checks, and the
// HIncr counter path, returning the op's apply result (Value + optional Data sentinel/payload). scratch
// is reused to decode Items without a per-call allocation (A1); its backing array is overwritten on the
// next call, so callers must consume the result before applying the next command (the loops do).
func (u *updateCtx) applyOne(cmd []byte, frozen []frozenRange, scratch []item) (ProposeResult, error) {
	c, err := decodeCommandInto(cmd, scratch)
	if err != nil {
		return ProposeResult{}, err
	}
	if len(frozen) > 0 && mutates(c.Op) && frozenCovers(frozen, routeKey(c.NS, c.Coll)) {
		return ProposeResult{Data: frozenMark}, nil // splitting: client retries onto the new shard
	}
	// Idempotency: a repeated keyed write returns its cached result without re-applying (§13.12). Check
	// the in-batch overlay first (a duplicate coalesced into this same entry), then the persisted cache.
	deduped := mutates(c.Op) && len(c.Idem) > 0
	if deduped {
		if r, ok := u.inBatchDedup[string(c.Idem)]; ok {
			return r, nil
		}
		if cached, cdata, found, derr := u.s.dedupGet(c.Idem); derr != nil {
			return ProposeResult{}, derr
		} else if found {
			return ProposeResult{Value: cached, Data: cdata}, nil
		}
	}
	if c.Op != opExpire && c.Op != opRemove { // these are type-agnostic: they only delete existing elements
		ok, terr := u.ensureType(c.NS, c.Coll, typeForOp(c.Op))
		if terr != nil {
			return ProposeResult{}, terr
		}
		if !ok {
			return ProposeResult{Data: wrongType}, nil
		}
	}
	// HIncrBy/HIncrByFloat are atomic counters whose result is the new value (Data), not a count.
	if c.Op == opHIncrBy || c.Op == opHIncrByFloat {
		if len(c.Items) == 0 { // a corrupt HIncr entry with no item — skip deterministically (non-fatal)
			return ProposeResult{}, errShortCommand
		}
		var changed int64
		var data []byte
		if c.Op == opHIncrBy {
			changed, data, err = u.applyHIncrInt(c, c.Items[0])
		} else {
			changed, data, err = u.applyHIncrFloat(c, c.Items[0])
		}
		if err != nil {
			return ProposeResult{}, err
		}
		if deduped && !bytes.Equal(data, notNumber) { // don't cache a failed (non-number) increment
			if derr := u.dedupRecord(c.Idem, uint64(changed), data); derr != nil {
				return ProposeResult{}, derr
			}
			u.inBatchDedup[string(c.Idem)] = ProposeResult{Value: uint64(changed), Data: append([]byte(nil), data...)}
		}
		return ProposeResult{Value: uint64(changed), Data: data}, nil
	}
	changed, err := u.applyCommand(c)
	if err != nil {
		return ProposeResult{}, err
	}
	if deduped {
		if derr := u.dedupRecord(c.Idem, uint64(changed), nil); derr != nil {
			return ProposeResult{}, derr
		}
		u.inBatchDedup[string(c.Idem)] = ProposeResult{Value: uint64(changed)}
	}
	return ProposeResult{Value: uint64(changed)}, nil
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
			if it.ExpiryMs > 0 { // SADD with a TTL (re)sets the member's expiry
				if err := u.setTTL(c.NS, c.Coll, it.Key, uint64(it.ExpiryMs)); err != nil {
					return 0, err
				}
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
				u.vals[string(ek)] = nil // value gone (for an in-batch HIncr)
				u.cardDelta[ck]--
				changed++
			}
			if err := u.clearTTL(c.NS, c.Coll, it.Key); err != nil { // no-op when no TTL
				return 0, err
			}
		case opExpire:
			exp, found, err := s.ttlExpiryOf(c.NS, c.Coll, it.Key)
			if err != nil {
				return 0, err
			}
			if !found || exp > uint64(it.ExpiryMs) {
				continue // refreshed to a later time, or already cleared
			}
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
			if err := u.clearTTL(c.NS, c.Coll, it.Key); err != nil {
				return 0, err
			}
		case opHSet:
			ek := s.elemKey(c.NS, c.Coll, it.Key)
			present, err := u.elemExists(ek)
			if err != nil {
				return 0, err
			}
			u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ek, Value: it.Val})
			u.exists[string(ek)] = true
			u.vals[string(ek)] = it.Val // keep the value overlay current for an in-batch HIncr
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
		case opRemove:
			// Type-agnostic removal (bulk cross-collection delete): dispatch on the collection's actual
			// type, skipping a collection that doesn't exist (design/30 §13.7).
			t, err := u.typeOf(c.NS, c.Coll)
			if err != nil {
				return 0, err
			}
			switch t {
			case typeSet, typeHash:
				ek := s.elemKey(c.NS, c.Coll, it.Key)
				present, err := u.elemExists(ek)
				if err != nil {
					return 0, err
				}
				if present {
					u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ek, Delete: true})
					u.exists[string(ek)] = false
					u.vals[string(ek)] = nil
					u.cardDelta[ck]--
					changed++
				}
				if err := u.clearTTL(c.NS, c.Coll, it.Key); err != nil {
					return 0, err
				}
			case typeZSet:
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
	case cardCheckQuery:
		stored, err := s.snapCard(snap, q.NS, q.Coll)
		if err != nil {
			return nil, err
		}
		mp := s.elemPrefix(q.NS, q.Coll)
		it, err := snap.Scan(storage.CFReplData, mp, prefixEnd(mp), 0)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var counted uint64
		for it.Valid() {
			counted++
			it.Next()
		}
		return CardCheck{Stored: stored, Counted: counted}, it.Err()
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
	case collectionsQuery:
		prefix := appendChunk(s.dataSpace(), q.NS) // dataSpace || chunk(ns)
		it, err := snap.Scan(storage.CFReplData, prefix, prefixEnd(prefix), 0)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var out [][]byte
		for it.Valid() {
			rest := it.Key()[len(prefix):] // chunk(coll) || scope-suffix
			if coll, suffix, terr := takeChunk(rest); terr == nil && len(suffix) == 1 && suffix[0] == scopeType {
				out = append(out, append([]byte(nil), coll...))
			}
			it.Next()
		}
		return out, it.Err()
	case collectionInfosQuery:
		prefix := appendChunk(s.dataSpace(), q.NS) // dataSpace || chunk(ns)
		it, err := snap.Scan(storage.CFReplData, prefix, prefixEnd(prefix), 0)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var out []CollInfo
		for it.Valid() {
			rest := it.Key()[len(prefix):] // chunk(coll) || scope-suffix
			if coll, suffix, terr := takeChunk(rest); terr == nil && len(suffix) == 1 && suffix[0] == scopeType {
				var ct collType // 0 = unknown if the type byte is missing
				if v := it.Value(); len(v) >= 1 {
					ct = collType(v[0])
				}
				out = append(out, CollInfo{Name: append([]byte(nil), coll...), Type: ct})
			}
			it.Next()
		}
		return out, it.Err()
	case ttlDueQuery:
		return s.scanDue(snap, q.NowMs, q.Limit)
	case migrateScanQuery:
		return scanRange(snap, s.prefix, q.StartRoute, q.EndRoute, q.Limit)
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
	v, found, err := s.getData(cardKey)
	if err != nil || !found {
		return 0, err
	}
	if len(v) != 8 {
		// Internal card counter is corrupt (not entry-attributable). Treat as fatal: it indicates a real
		// storage/state-integrity fault, never a malformed input, so it must stop this replica.
		return 0, fatal(errors.New("collections: corrupt card value"))
	}
	return binary.BigEndian.Uint64(v), nil
}

func bitsOf(f float64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	return b
}
func float64FromBits(b []byte) float64 { return math.Float64frombits(binary.BigEndian.Uint64(b)) }
