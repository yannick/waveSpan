package recordstore

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// countingStore counts point lookups against CFKVData so tests can prove the inline-value fast
// path (design/37 P1.5) really skips the second LSM lookup.
type countingStore struct {
	storage.LocalStore
	dataGets atomic.Int64
}

func (c *countingStore) Get(cf storage.ColumnFamily, key []byte) ([]byte, bool, error) {
	if cf == storage.CFKVData {
		c.dataGets.Add(1)
	}
	return c.LocalStore.Get(cf, key)
}

func newCountingStore(t *testing.T) (*Store, *countingStore) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	cs := &countingStore{LocalStore: mem}
	clock := version.NewClock(nil, 500)
	return NewStore(cs, "dev", "node1", clock, version.NewSequencer(0)), cs
}

// TestInlineValueServesGetWithoutDataCF: a small value must be read back from the latest pointer
// alone — zero CFKVData point lookups.
func TestInlineValueServesGetWithoutDataCF(t *testing.T) {
	s, cs := newCountingStore(t)
	putLocal(t, s, "default", []byte("k1"), []byte("small-value"))

	cs.dataGets.Store(0)
	out, err := s.Get("default", []byte("k1"))
	if err != nil || !out.Found || !bytes.Equal(out.Value, []byte("small-value")) {
		t.Fatalf("get = %+v err=%v", out, err)
	}
	if n := cs.dataGets.Load(); n != 0 {
		t.Fatalf("small-value Get did %d data-CF lookups, want 0 (inline fast path)", n)
	}
}

// TestInlineValueServesScanWithoutDataCF: a range scan over small values must be a single meta-CF
// iteration — zero CFKVData point lookups (the old N+1 pattern).
func TestInlineValueServesScanWithoutDataCF(t *testing.T) {
	s, cs := newCountingStore(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		putLocal(t, s, "default", []byte(k), []byte("v-"+k))
	}

	cs.dataGets.Store(0)
	rows, err := s.ScanRange("default", nil, nil, 0, 0)
	if err != nil || len(rows) != 4 {
		t.Fatalf("scan rows=%d err=%v", len(rows), err)
	}
	for _, r := range rows {
		if !bytes.Equal(r.Value, append([]byte("v-"), r.Key...)) {
			t.Fatalf("row %q value %q", r.Key, r.Value)
		}
	}
	if n := cs.dataGets.Load(); n != 0 {
		t.Fatalf("scan did %d data-CF lookups for 4 small rows, want 0", n)
	}
}

// TestLargeValueFallsBackToDataCF: values over the inline threshold keep the two-lookup path and
// stay correct.
func TestLargeValueFallsBackToDataCF(t *testing.T) {
	s, cs := newCountingStore(t)
	big := bytes.Repeat([]byte("x"), storage.InlineValueMax+1)
	putLocal(t, s, "default", []byte("big"), big)

	cs.dataGets.Store(0)
	out, err := s.Get("default", []byte("big"))
	if err != nil || !out.Found || !bytes.Equal(out.Value, big) {
		t.Fatalf("get big: found=%v err=%v", out.Found, err)
	}
	if n := cs.dataGets.Load(); n == 0 {
		t.Fatal("large value must fall back to the data CF")
	}

	rows, err := s.ScanRange("default", nil, nil, 0, 0)
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Value, big) {
		t.Fatalf("scan big: rows=%d err=%v", len(rows), err)
	}
}

// TestLosingApplyPreservesInlineValue: an older write arriving after a newer one must not clear
// the standing winner's inline value (the pointer is rewritten on losing applies).
func TestLosingApplyPreservesInlineValue(t *testing.T) {
	s, cs := newCountingStore(t)
	vOld := s.NextVersion()
	vNew := s.NextVersion()

	newRec := s.BuildRecord("default", []byte("k"), []byte("newer"), vNew, false, nil)
	if _, err := s.Apply(newRec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	oldRec := s.BuildRecord("default", []byte("k"), []byte("older"), vOld, false, nil)
	if _, err := s.Apply(oldRec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}

	cs.dataGets.Store(0)
	out, err := s.Get("default", []byte("k"))
	if err != nil || !out.Found || !bytes.Equal(out.Value, []byte("newer")) {
		t.Fatalf("get after losing apply = %q found=%v err=%v", out.Value, out.Found, err)
	}
	if n := cs.dataGets.Load(); n != 0 {
		t.Fatalf("losing apply cleared the inline value (%d data-CF lookups)", n)
	}
}

// TestTombstoneClearsInline: deleting a key must not leave a stale inline value behind.
func TestTombstoneClearsInline(t *testing.T) {
	s, _ := newCountingStore(t)
	putLocal(t, s, "default", []byte("k"), []byte("v"))
	v := s.NextVersion()
	del := s.BuildRecord("default", []byte("k"), nil, v, true, nil)
	if _, err := s.Apply(del, wavespanv1.MutationKind_MUTATION_KIND_DELETE); err != nil {
		t.Fatal(err)
	}
	out, err := s.Get("default", []byte("k"))
	if err != nil || out.Found {
		t.Fatalf("get after delete: found=%v err=%v", out.Found, err)
	}
}
