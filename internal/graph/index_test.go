package graph

import (
	"sort"
	"testing"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func seed(t *testing.T, s *Store) {
	t.Helper()
	mk := func(id string, age int64, city string) *wavespanv1.NodeRecord {
		return node("g", id, []string{"User"}, map[string]*wavespanv1.Value{"age": intVal(age), "city": strVal(city)}, 1)
	}
	for _, n := range []*wavespanv1.NodeRecord{mk("a", 30, "NYC"), mk("b", 30, "LA"), mk("c", 40, "NYC"), mk("d", 25, "SF")} {
		if err := s.CreateNode(n); err != nil {
			t.Fatal(err)
		}
	}
	// a non-User node must not show up in User scans
	if err := s.CreateNode(node("g", "x", []string{"Post"}, nil, 1)); err != nil {
		t.Fatal(err)
	}
}

func sorted(s []string) []string { sort.Strings(s); return s }

func TestLabelScan(t *testing.T) {
	s := newStore(t)
	seed(t, s)
	got, _ := s.ScanLabel("g", "User")
	if want := []string{"a", "b", "c", "d"}; !eqStr(sorted(got), want) {
		t.Fatalf("label scan = %v, want %v", sorted(got), want)
	}
}

func TestPropertySeek(t *testing.T) {
	s := newStore(t)
	seed(t, s)
	eq, _ := s.SeekProperty("g", "User", "age", intVal(30))
	if want := []string{"a", "b"}; !eqStr(sorted(eq), want) {
		t.Fatalf("age==30 = %v, want %v", sorted(eq), want)
	}
	gte, _ := s.SeekPropertyGTE("g", "User", "age", 30)
	if want := []string{"a", "b", "c"}; !eqStr(sorted(gte), want) {
		t.Fatalf("age>=30 = %v, want %v", sorted(gte), want)
	}
}

func TestIndexRebuildFromRecords(t *testing.T) {
	s := newStore(t)
	seed(t, s)
	before, _ := s.ScanLabel("g", "User")
	if err := s.RebuildIndexes("g"); err != nil {
		t.Fatal(err)
	}
	after, _ := s.ScanLabel("g", "User")
	if !eqStr(sorted(before), sorted(after)) {
		t.Fatalf("rebuild changed label scan: %v vs %v", sorted(before), sorted(after))
	}
	eq, _ := s.SeekProperty("g", "User", "age", intVal(40))
	if !eqStr(eq, []string{"c"}) {
		t.Fatalf("property seek after rebuild = %v", eq)
	}
}

func TestStaleIndexEntryFiltered(t *testing.T) {
	s := newStore(t)
	seed(t, s)
	// tombstone node 'a' (a later version) but leave its label/prop index entries in place
	tomb := node("g", "a", nil, nil, 99)
	tomb.Tombstone = true
	if err := s.CreateNode(tomb); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ScanLabel("g", "User")
	if want := []string{"b", "c", "d"}; !eqStr(sorted(got), want) {
		t.Fatalf("tombstoned node should be filtered from label scan: %v", sorted(got))
	}
	eq, _ := s.SeekProperty("g", "User", "age", intVal(30))
	if !eqStr(sorted(eq), []string{"b"}) {
		t.Fatalf("stale property entry for tombstoned node should be filtered: %v", sorted(eq))
	}
}

func eqStr(a, b []string) bool {
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
