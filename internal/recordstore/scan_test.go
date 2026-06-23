package recordstore

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func newStoreAt(t *testing.T, wallMs uint64) *Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return NewStore(mem, "dev", "node1", version.NewClock(func() uint64 { return wallMs }, 500), version.NewSequencer(0))
}

func put(t *testing.T, s *Store, key, val string, ttlMs *int64) {
	t.Helper()
	v := s.NextVersion()
	if _, err := s.Apply(s.BuildRecord("default", []byte(key), []byte(val), v, false, ttlMs), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

func scanKeys(t *testing.T, s *Store, start, end string, nowMs int64) []string {
	t.Helper()
	var sb, eb []byte
	if start != "" {
		sb = []byte(start)
	}
	if end != "" {
		eb = []byte(end)
	}
	rows, err := s.ScanRange("default", sb, eb, 0, nowMs)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r.Key)
	}
	return out
}

func eq(a, b []string) bool {
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

func TestScanRangeOrderedAndBounded(t *testing.T) {
	s := newStoreAt(t, 1000)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		put(t, s, k, "V"+k, nil)
	}
	if got := scanKeys(t, s, "b", "e", 0); !eq(got, []string{"b", "c", "d"}) {
		t.Fatalf("scan [b,e) = %v", got)
	}
	if got := scanKeys(t, s, "", "", 0); !eq(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("full scan = %v", got)
	}
	// value is read back
	rows, _ := s.ScanRange("default", []byte("c"), []byte("d"), 0, 0)
	if len(rows) != 1 || string(rows[0].Value) != "Vc" {
		t.Fatalf("scan value mismatch: %+v", rows)
	}
}

func TestScanRangeHidesTombstones(t *testing.T) {
	s := newStoreAt(t, 1000)
	put(t, s, "a", "1", nil)
	put(t, s, "b", "2", nil)
	// delete b (tombstone with a newer version)
	v := s.NextVersion()
	if _, err := s.Apply(s.BuildRecord("default", []byte("b"), nil, v, true, nil), wavespanv1.MutationKind_MUTATION_KIND_DELETE); err != nil {
		t.Fatal(err)
	}
	if got := scanKeys(t, s, "", "", 0); !eq(got, []string{"a"}) {
		t.Fatalf("scan should hide tombstoned b: %v", got)
	}
}

func TestScanRangeHidesExpired(t *testing.T) {
	s := newStoreAt(t, 1_000_000) // wall = 1_000_000ms
	put(t, s, "live", "x", nil)
	ttl := int64(1000)
	put(t, s, "soon", "y", &ttl) // expires at 1_001_000

	// before expiry: both visible
	if got := scanKeys(t, s, "", "", 1_000_500); !eq(got, []string{"live", "soon"}) {
		t.Fatalf("before expiry = %v", got)
	}
	// after expiry: 'soon' hidden (best-effort hide-expired on read)
	if got := scanKeys(t, s, "", "", 1_002_000); !eq(got, []string{"live"}) {
		t.Fatalf("after expiry = %v", got)
	}
}
