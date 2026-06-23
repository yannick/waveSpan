package vector

import (
	"strconv"
	"testing"
)

func TestRingDeterministicAndSized(t *testing.T) {
	members := []string{"n1", "n2", "n3", "n4", "n5"}
	a := Ring(BucketKey("docs", 1, 7), members, 3)
	b := Ring(BucketKey("docs", 1, 7), shuffle(members), 3)
	if len(a) != 3 {
		t.Fatalf("ring size = %d, want 3", len(a))
	}
	if !equal(a, b) {
		t.Fatalf("ring must be independent of input order: %v vs %v", a, b)
	}
	// different bucket → (usually) different holders
	if equal(a, Ring(BucketKey("docs", 1, 99), members, 3)) {
		t.Log("note: two buckets mapped to the same ring (possible but unlikely)")
	}
}

// TestRingMinimalReshuffle: removing a member should move only the buckets that mapped to it, not
// reshuffle everything (the HRW property that makes rebalancing cheap).
func TestRingMinimalReshuffle(t *testing.T) {
	full := []string{"n1", "n2", "n3", "n4", "n5"}
	reduced := []string{"n1", "n2", "n3", "n4"} // n5 left
	moved, total := 0, 2000
	for i := 0; i < total; i++ {
		key := BucketKey("docs", 1, uint32(i))
		before := Ring(key, full, 1)[0]
		after := Ring(key, reduced, 1)[0]
		if before != after {
			moved++
			if before != "n5" {
				t.Fatalf("bucket %d primary moved from %s to %s but %s did not leave", i, before, after, before)
			}
		}
	}
	// only buckets whose primary was n5 should move; that's ~1/5 of them, not all.
	if moved > total/3 {
		t.Errorf("too many buckets reshuffled: %d/%d (HRW should move ~1/5)", moved, total)
	}
	t.Logf("reshuffled %d/%d buckets after one node left", moved, total)
}

func TestBucketKey(t *testing.T) {
	if BucketKey("c", 2, 5) != "c/2/5" {
		t.Fatalf("unexpected bucket key %q", BucketKey("c", 2, 5))
	}
}

func shuffle(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ = strconv.Itoa
