package collections

import (
	"encoding/binary"

	"github.com/yannick/wavespan/internal/storage"
)

// TTL key layout (design/30 §10). A shard-level, expiry-ordered index lets the leader sweep all
// collections in one scan; a per-collection pointer lets element removal find and drop the index
// entry:
//
//	<prefix>|subTTL|be(expiry)|chunk(ns)|chunk(coll)|<member> -> empty   (due-ordered index)
//	<collScope>|scopeTTLPtr|<member>                          -> be(expiry) (reverse pointer)
const (
	subTTL      byte = 0x02
	scopeTTLPtr byte = 0x04
)

// dueElem is an element whose expiry has passed, returned by ttlDueQuery for the sweeper.
type dueElem struct {
	NS, Coll, Member []byte
	Expiry           uint64
}

// ttlDueQuery scans the shard's TTL index for elements due at or before NowMs (Limit 0 = all).
type ttlDueQuery struct {
	NowMs int64
	Limit int
}

func (s *shardSM) ttlSpace() []byte { return append(append([]byte{}, s.prefix...), subTTL) }

func (s *shardSM) ttlIndexKey(expiry uint64, ns, coll, member []byte) []byte {
	out := append(s.ttlSpace(), u64(expiry)...)
	out = appendChunk(out, ns)
	out = appendChunk(out, coll)
	return append(out, member...)
}

func (s *shardSM) ttlPtrKey(ns, coll, member []byte) []byte {
	return append(append(s.collScope(ns, coll), scopeTTLPtr), member...)
}

// ttlExpiryOf returns the absolute expiry recorded for an element, if any.
func (s *shardSM) ttlExpiryOf(ns, coll, member []byte) (uint64, bool, error) {
	v, found, err := s.store.Get(storage.CFReplData, s.ttlPtrKey(ns, coll, member))
	if err != nil || !found {
		return 0, false, err
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// setTTL appends ops to (re)set an element's expiry, clearing any prior index entry.
func (u *updateCtx) setTTL(ns, coll, member []byte, expiry uint64) error {
	if old, found, err := u.s.ttlExpiryOf(ns, coll, member); err != nil {
		return err
	} else if found {
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.ttlIndexKey(old, ns, coll, member), Delete: true})
	}
	u.ops = append(u.ops,
		storage.StoreOp{CF: storage.CFReplData, Key: u.s.ttlIndexKey(expiry, ns, coll, member), Value: []byte{}},
		storage.StoreOp{CF: storage.CFReplData, Key: u.s.ttlPtrKey(ns, coll, member), Value: u64(expiry)})
	return nil
}

// clearTTL appends ops to drop an element's expiry entry (if any).
func (u *updateCtx) clearTTL(ns, coll, member []byte) error {
	old, found, err := u.s.ttlExpiryOf(ns, coll, member)
	if err != nil || !found {
		return err
	}
	u.ops = append(u.ops,
		storage.StoreOp{CF: storage.CFReplData, Key: u.s.ttlIndexKey(old, ns, coll, member), Delete: true},
		storage.StoreOp{CF: storage.CFReplData, Key: u.s.ttlPtrKey(ns, coll, member), Delete: true})
	return nil
}

// scanDue reads the TTL index for elements due at or before nowMs.
func (s *shardSM) scanDue(snap storage.Snapshot, nowMs int64, limit int) ([]dueElem, error) {
	ts := s.ttlSpace()
	end := append(append([]byte{}, ts...), u64(uint64(nowMs)+1)...) // include expiry == now
	it, err := snap.Scan(storage.CFReplData, ts, end, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []dueElem
	for it.Valid() {
		suffix := it.Key()[len(ts):]
		if len(suffix) >= 8 {
			expiry := binary.BigEndian.Uint64(suffix[:8])
			rest := suffix[8:]
			ns, rest, err := takeChunk(rest)
			if err == nil {
				coll, member, err2 := takeChunk(rest)
				if err2 == nil {
					out = append(out, dueElem{
						NS:     append([]byte(nil), ns...),
						Coll:   append([]byte(nil), coll...),
						Member: append([]byte(nil), member...),
						Expiry: expiry,
					})
				}
			}
		}
		it.Next()
	}
	return out, it.Err()
}
