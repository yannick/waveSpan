package collections

import (
	"encoding/binary"

	"github.com/yannick/wavespan/internal/storage"
)

// Idempotency dedup (design/30 §13.12). A write carrying an idempotency key is applied exactly once:
// the full result (value + optional data, e.g. an HIncrBy's new value) is cached so a retry returns
// the original result without re-applying — essential for non-idempotent ops like increment. The cache
// is a fixed-size FIFO ring keyed off a replicated sequence counter — deterministic across replicas,
// so every replica caches and evicts identically — bounding memory while covering the retry window:
//
//	<prefix>|subDedup|<key>          -> value(uint64 BE) || data
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
	v, found, err := b.getData(b.dedupSeqKey())
	if err != nil || !found || len(v) != 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(v), nil
}

// dedupGet returns the cached result (value + data) for an idempotency key, if present.
func (s *shardSM) dedupGet(key []byte) (uint64, []byte, bool, error) {
	v, found, err := s.getData(s.dedupKey(key))
	if err != nil || !found || len(v) < 8 {
		return 0, nil, false, err
	}
	return binary.BigEndian.Uint64(v[:8]), append([]byte(nil), v[8:]...), true, nil
}

// dedupRecord appends ops to cache key->(value,data) with FIFO eviction and advances the sequence.
func (u *updateCtx) dedupRecord(key []byte, value uint64, data []byte) error {
	s := u.s
	slot := u.dedupSeq % dedupRingSize
	if oldKey, found, err := s.getData(s.dedupRingKey(slot)); err != nil {
		return err
	} else if found && len(oldKey) > 0 {
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: s.dedupKey(oldKey), Delete: true})
	}
	rec := append(u64(value), data...)
	u.ops = append(u.ops,
		storage.StoreOp{CF: storage.CFReplData, Key: s.dedupRingKey(slot), Value: append([]byte(nil), key...)},
		storage.StoreOp{CF: storage.CFReplData, Key: s.dedupKey(key), Value: rec})
	u.dedupSeq++
	return nil
}
