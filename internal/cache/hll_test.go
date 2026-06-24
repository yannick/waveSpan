package cache

import (
	"fmt"
	"math"
	"testing"
)

// withinPct asserts got is within pct percent of want (relative error).
func withinPct(t *testing.T, label string, got, want float64, pct float64) {
	t.Helper()
	if want == 0 {
		if math.Abs(got) > pct {
			t.Fatalf("%s: got %v, want ~0 (±%v)", label, got, pct)
		}
		return
	}
	rel := math.Abs(got-want) / want * 100
	if rel > pct {
		t.Fatalf("%s: got %v, want %v (rel err %.2f%% > %.2f%%)", label, got, want, rel, pct)
	}
}

func TestHLLEmptyIsZero(t *testing.T) {
	h := NewHLL()
	if got := h.Estimate(); got != 0 {
		t.Fatalf("empty HLL estimate = %d, want 0", got)
	}
}

func TestHLLEstimateAccuracy(t *testing.T) {
	for _, n := range []int{100, 1000, 100_000} {
		h := NewHLL()
		for i := 0; i < n; i++ {
			h.Add([]byte(fmt.Sprintf("key-%d", i)))
		}
		// p=14 standard error ≈ 0.81%; allow generous headroom for any single seed.
		withinPct(t, fmt.Sprintf("n=%d", n), float64(h.Estimate()), float64(n), 3.0)
	}
}

func TestHLLIdempotent(t *testing.T) {
	h := NewHLL()
	for i := 0; i < 5000; i++ {
		h.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	first := h.Estimate()
	// Re-add every key; a distinct-counting sketch must be unchanged by duplicates.
	for i := 0; i < 5000; i++ {
		h.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	if h.Estimate() != first {
		t.Fatalf("re-adding duplicates changed estimate: %d -> %d", first, h.Estimate())
	}
}

func TestHLLMergeUnion(t *testing.T) {
	// A: 0..59999, B: 40000..99999 → union is 0..99999 (100k distinct), 20k overlap.
	a, b := NewHLL(), NewHLL()
	for i := 0; i < 60_000; i++ {
		a.Add([]byte(fmt.Sprintf("u%d", i)))
	}
	for i := 40_000; i < 100_000; i++ {
		b.Add([]byte(fmt.Sprintf("u%d", i)))
	}
	a.Merge(b)
	withinPct(t, "union", float64(a.Estimate()), 100_000, 3.0)
}

func TestHLLMergeIsCommutativeWithReencode(t *testing.T) {
	// The same key added on two "nodes" must collide in the union (dedup), so merging two
	// identical sets yields the set size, not double.
	a, b := NewHLL(), NewHLL()
	for i := 0; i < 10_000; i++ {
		k := []byte(fmt.Sprintf("dup%d", i))
		a.Add(k)
		b.Add(k)
	}
	a.Merge(HLLFromBytes(b.Bytes()))
	withinPct(t, "identical-union", float64(a.Estimate()), 10_000, 3.0)
}

func TestHLLBytesRoundTrip(t *testing.T) {
	h := NewHLL()
	for i := 0; i < 25_000; i++ {
		h.Add([]byte(fmt.Sprintf("rt%d", i)))
	}
	got := HLLFromBytes(h.Bytes())
	if got.Estimate() != h.Estimate() {
		t.Fatalf("round-trip estimate mismatch: %d != %d", got.Estimate(), h.Estimate())
	}
}

func TestHLLFromBytesSizeMismatchIsEmpty(t *testing.T) {
	if got := HLLFromBytes([]byte{1, 2, 3}).Estimate(); got != 0 {
		t.Fatalf("malformed bytes should yield empty HLL, got estimate %d", got)
	}
	if got := HLLFromBytes(nil).Estimate(); got != 0 {
		t.Fatalf("nil bytes should yield empty HLL, got estimate %d", got)
	}
}
