// Package cache implements WaveSpan's dynamic cache replicas: closest-holder FetchReplica,
// the dynamic cache store, key subscriptions, resync, and eviction (design/05). It also carries
// the gossiped holder-summary directory so a read miss resolves holders without broadcast
// (design/README.md hard rule 3; design/04 "Holder summaries").
package cache

import (
	"encoding/binary"
	"hash/fnv"
)

// bloomBits is the fixed bit-size of a holder bloom filter (~16 KiB, good for ~13k keys at ~1%).
const (
	bloomBits  = 1 << 17
	bloomWords = bloomBits / 64
	bloomK     = 7
)

// Bloom is a fixed-size bloom filter over the keys a node holds, gossiped as a compact summary.
type Bloom struct {
	words [bloomWords]uint64
}

// NewBloom returns an empty filter.
func NewBloom() *Bloom { return &Bloom{} }

func bloomHashes(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write(key)
	a := h1.Sum64()
	h2 := fnv.New64()
	_, _ = h2.Write(key)
	b := h2.Sum64() | 1 // odd, so the step never degenerates
	return a, b
}

// Add inserts a key.
func (f *Bloom) Add(key []byte) {
	a, b := bloomHashes(key)
	for i := 0; i < bloomK; i++ {
		bit := (a + uint64(i)*b) % bloomBits
		f.words[bit/64] |= 1 << (bit % 64)
	}
}

// MaybeContains reports whether the key may be present (false = definitely absent).
func (f *Bloom) MaybeContains(key []byte) bool {
	a, b := bloomHashes(key)
	for i := 0; i < bloomK; i++ {
		bit := (a + uint64(i)*b) % bloomBits
		if f.words[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// Bytes serialises the filter for gossip.
func (f *Bloom) Bytes() []byte {
	out := make([]byte, bloomWords*8)
	for i, w := range f.words {
		binary.LittleEndian.PutUint64(out[i*8:], w)
	}
	return out
}

// BloomFromBytes deserialises a filter; it returns an empty filter on a size mismatch.
func BloomFromBytes(b []byte) *Bloom {
	f := &Bloom{}
	if len(b) != bloomWords*8 {
		return f
	}
	for i := range f.words {
		f.words[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return f
}
