package collections

import (
	"encoding/binary"

	"github.com/yannick/wavespan/internal/storage"
)

// Idempotency dedup (design/30 §13.12). A write carrying an idempotency key is applied exactly once:
// the result count is cached so a retry (e.g. after a forwarded write times out) returns the original
// count without re-applying. The cache is a fixed-size FIFO ring keyed off a replicated sequence
// counter — deterministic across replicas, so every replica caches and evicts identically — which
// bounds memory while covering the retry window:
//
//	<prefix>|subDedup|<key>          -> count (uint64 BE)
//	<prefix>|subDedupRing|be(slot)   -> key            (for FIFO eviction)
//	<prefix>|subMeta|"dedupseq"      -> next slot sequence (uint64 BE)
const (
	subDedup      byte   = 0x03
	subDedupRing  byte   = 0x04
	dedupRingSize uint64 = 4096
)

func (b *baseSM) dedupSeqKey() []byte {
	return append(append(append([]byte{}, b.prefix...), subMeta), []byte("dedupseq")...)
}
func (s *shardSM) dedupKey(key []byte) []byte {
	return append(append(append([]byte{}, s.prefix...), subDedup), key...)
}
func (s *shardSM) dedupRingKey(slot uint64) []byte {
	return append(append(append([]byte{}, s.prefix...), subDedupRing), u64(slot)...)
}

func (b *baseSM) readDedupSeq() (uint64, error) {
	v, found, err := b.store.Get(storage.CFReplData, b.dedupSeqKey())
	if err != nil || !found || len(v) != 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(v), nil
}

// dedupGet returns the cached result count for an idempotency key, if present.
func (s *shardSM) dedupGet(key []byte) (uint64, bool, error) {
	v, found, err := s.store.Get(storage.CFReplData, s.dedupKey(key))
	if err != nil || !found || len(v) != 8 {
		return 0, false, err
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// dedupRecord appends ops to cache key->count with FIFO eviction and advances the sequence.
func (u *updateCtx) dedupRecord(key []byte, count uint64) error {
	s := u.s
	slot := u.dedupSeq % dedupRingSize
	if oldKey, found, err := s.store.Get(storage.CFReplData, s.dedupRingKey(slot)); err != nil {
		return err
	} else if found && len(oldKey) > 0 {
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: s.dedupKey(oldKey), Delete: true})
	}
	u.ops = append(u.ops,
		storage.StoreOp{CF: storage.CFReplData, Key: s.dedupRingKey(slot), Value: append([]byte(nil), key...)},
		storage.StoreOp{CF: storage.CFReplData, Key: s.dedupKey(key), Value: u64(count)})
	u.dedupSeq++
	return nil
}
