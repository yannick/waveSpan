package version

import (
	"math/rand"
	"sort"
	"testing"
)

func v(phys uint64, logical uint32, cluster, member string, seq uint64) Version {
	return Version{
		HLCPhysicalMs:   phys,
		HLCLogical:      logical,
		WriterClusterID: cluster,
		WriterMemberID:  member,
		WriterSequence:  seq,
	}
}

// orderedAscending is a list of versions in strictly increasing Compare order,
// exercising every tie-break level (physical, logical, cluster, member, sequence).
func orderedAscending() []Version {
	return []Version{
		v(10, 0, "a", "m1", 1),
		v(10, 0, "a", "m1", 2), // higher sequence wins
		v(10, 0, "a", "m2", 1), // higher member wins over sequence
		v(10, 0, "b", "m1", 1), // higher cluster wins over member
		v(10, 1, "a", "m1", 1), // higher logical wins over writer fields
		v(11, 0, "a", "m1", 1), // higher physical wins over everything
	}
}

func TestCompareTotalOrderDeterministic(t *testing.T) {
	want := orderedAscending()

	// Shuffle many times; sorting by Compare must always reproduce `want`.
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		got := append([]Version(nil), want...)
		rng.Shuffle(len(got), func(i, j int) { got[i], got[j] = got[j], got[i] })
		sort.Slice(got, func(i, j int) bool { return got[i].Compare(got[j]) < 0 })
		for i := range want {
			if !got[i].Equal(want[i]) {
				t.Fatalf("iter %d: sorted[%d]=%+v want %+v", iter, i, got[i], want[i])
			}
		}
	}
}

func TestCompareAntisymmetricAndReflexive(t *testing.T) {
	all := orderedAscending()
	for _, a := range all {
		if a.Compare(a) != 0 {
			t.Fatalf("Compare(a,a) != 0 for %+v", a)
		}
		for _, b := range all {
			if sign(a.Compare(b)) != -sign(b.Compare(a)) {
				t.Fatalf("not antisymmetric: %+v vs %+v", a, b)
			}
		}
	}
}

func TestEqualOnlyForSameIdentity(t *testing.T) {
	a := v(10, 0, "a", "m1", 1)
	if !a.Equal(a) {
		t.Fatal("version not equal to itself")
	}
	for _, b := range orderedAscending()[1:] {
		if a.Equal(b) {
			t.Fatalf("distinct versions compared equal: %+v == %+v", a, b)
		}
		if a.Compare(b) == 0 {
			t.Fatalf("Compare returned 0 for distinct identities: %+v %+v", a, b)
		}
	}
}

func TestMutationIDStableAndIdempotent(t *testing.T) {
	a := v(10, 3, "clusterA", "member7", 42)
	b := v(99, 9, "clusterA", "member7", 42) // same identity fields, different HLC observation
	if a.MutationID() != b.MutationID() {
		t.Fatalf("mutation id should depend only on cluster/member/sequence: %q vs %q", a.MutationID(), b.MutationID())
	}
	c := v(10, 3, "clusterA", "member7", 43)
	if a.MutationID() == c.MutationID() {
		t.Fatalf("different sequence must yield different mutation id")
	}
	// no ambiguity across separator boundaries
	x := v(0, 0, "ab", "c", 1)
	y := v(0, 0, "a", "bc", 1)
	if x.MutationID() == y.MutationID() {
		t.Fatalf("mutation id is ambiguous across field boundaries: %q == %q", x.MutationID(), y.MutationID())
	}
}

func TestProtoRoundTrip(t *testing.T) {
	a := v(123456789, 7, "clusterA", "member3", 99)
	if got := FromProto(a.ToProto()); !got.Equal(a) || got != a {
		t.Fatalf("proto round trip lost data: %+v -> %+v", a, got)
	}
}

func TestSequencerMonotonicAndNoRegressionAfterRestart(t *testing.T) {
	s := NewSequencer(0)
	var last uint64
	for i := 0; i < 1000; i++ {
		n := s.Next()
		if n <= last {
			t.Fatalf("sequence not strictly increasing: %d after %d", n, last)
		}
		last = n
	}
	// Simulate restart: resume from the persisted high-water mark.
	restarted := NewSequencer(s.Last())
	n := restarted.Next()
	if n <= last {
		t.Fatalf("sequence regressed after restart: %d <= %d", n, last)
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}
