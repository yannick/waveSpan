package cache

import (
	"hash/fnv"
	"math"
	"math/bits"
)

// HyperLogLog parameters: p=14 gives m=16384 one-byte registers (16 KiB on the wire, matching the
// holder bloom's order of magnitude) and a standard error of ≈1.04/sqrt(m) ≈ 0.81%. A register holds
// the max observed rho (≤ 64-p+1 = 51), which fits a byte. The sketch is gossiped and merged so the
// union over all members estimates the cluster's distinct logical key count — a key hashes identically
// on every replica, so replicas collide in the union and are counted once (design/04).
const (
	hllP    = 14
	hllM    = 1 << hllP
	hllMaxR = 64 - hllP + 1 // max possible rho, fits in one byte
)

// HLL is a HyperLogLog distinct-cardinality sketch over the keys a node holds.
type HLL struct {
	reg [hllM]uint8
}

// NewHLL returns an empty sketch.
func NewHLL() *HLL { return &HLL{} }

// hllHash returns a well-distributed 64-bit hash. FNV-1a is fast but has weak avalanche, so we run
// the output through a SplitMix64 finalizer; the result is a pure function of the key, so every node
// hashes a given key identically (required for the union to dedup replicas).
func hllHash(key []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(key)
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Add records a key.
func (h *HLL) Add(key []byte) {
	x := hllHash(key)
	idx := x >> (64 - hllP) // top p bits select the register
	rho := uint8(bits.LeadingZeros64(x<<hllP)) + 1
	if rho > hllMaxR { // only when the remaining bits are all zero (astronomically rare)
		rho = hllMaxR
	}
	if rho > h.reg[idx] {
		h.reg[idx] = rho
	}
}

// Merge folds other into h by taking the per-register maximum (the union of the two key sets).
func (h *HLL) Merge(other *HLL) {
	if other == nil {
		return
	}
	for i := range h.reg {
		if other.reg[i] > h.reg[i] {
			h.reg[i] = other.reg[i]
		}
	}
}

// Estimate returns the approximate number of distinct keys added. It uses the raw HLL estimator with
// HyperLogLog's linear-counting correction for the small-cardinality range; the large-range
// correction is unnecessary with a 64-bit hash (no hash-space collisions in practice).
func (h *HLL) Estimate() uint64 {
	const m = float64(hllM)
	alpha := 0.7213 / (1 + 1.079/m)
	sum := 0.0
	zeros := 0
	for _, r := range h.reg {
		sum += math.Ldexp(1, -int(r)) // 2^-r
		if r == 0 {
			zeros++
		}
	}
	est := alpha * m * m / sum
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros)) // linear counting
	}
	return uint64(est + 0.5)
}

// Bytes serialises the registers for gossip (one byte each).
func (h *HLL) Bytes() []byte {
	out := make([]byte, hllM)
	copy(out, h.reg[:])
	return out
}

// HLLFromBytes deserialises a sketch; a size mismatch yields an empty sketch (best-effort gossip).
func HLLFromBytes(b []byte) *HLL {
	h := &HLL{}
	if len(b) != hllM {
		return h
	}
	copy(h.reg[:], b)
	return h
}
