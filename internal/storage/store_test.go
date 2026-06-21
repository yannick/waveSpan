package storage

import (
	"bytes"
	"testing"
)

// storeFactory builds a fresh LocalStore and a cleanup func. Both implementations
// (mem_store and wavesdb_store) are run against the same conformance suite.
type storeFactory func(t *testing.T) LocalStore

func runConformance(t *testing.T, newStore storeFactory) {
	t.Run("PutGetDelete", func(t *testing.T) {
		s := newStore(t)
		if err := s.Put(CFKVData, []byte("k1"), []byte("v1")); err != nil {
			t.Fatal(err)
		}
		got, found, err := s.Get(CFKVData, []byte("k1"))
		if err != nil || !found || !bytes.Equal(got, []byte("v1")) {
			t.Fatalf("get k1 = (%q,%v,%v), want v1,true,nil", got, found, err)
		}
		// absent key
		if _, found, err := s.Get(CFKVData, []byte("missing")); err != nil || found {
			t.Fatalf("get missing = (_,%v,%v), want false,nil", found, err)
		}
		// different CFs are isolated
		if _, found, _ := s.Get(CFSys, []byte("k1")); found {
			t.Fatal("key leaked across column families")
		}
		// delete
		if err := s.Delete(CFKVData, []byte("k1")); err != nil {
			t.Fatal(err)
		}
		if _, found, _ := s.Get(CFKVData, []byte("k1")); found {
			t.Fatal("key present after delete")
		}
		// delete absent key is not an error
		if err := s.Delete(CFKVData, []byte("nope")); err != nil {
			t.Fatalf("delete absent: %v", err)
		}
	})

	t.Run("OrderedScan", func(t *testing.T) {
		s := newStore(t)
		keys := []string{"a", "b", "c", "d", "e", "f"}
		for _, k := range keys {
			if err := s.Put(CFKVData, []byte(k), []byte("V"+k)); err != nil {
				t.Fatal(err)
			}
		}
		// [b, e) yields b,c,d in order
		got := collect(t, s, CFKVData, []byte("b"), []byte("e"), 0)
		if want := []string{"b", "c", "d"}; !eqKeys(got, want) {
			t.Fatalf("scan [b,e) = %v want %v", got, want)
		}
		// limit
		got = collect(t, s, CFKVData, []byte("a"), []byte("z"), 2)
		if want := []string{"a", "b"}; !eqKeys(got, want) {
			t.Fatalf("scan limit 2 = %v want %v", got, want)
		}
		// full scan with nil bounds
		got = collect(t, s, CFKVData, nil, nil, 0)
		if !eqKeys(got, keys) {
			t.Fatalf("full scan = %v want %v", got, keys)
		}
	})

	t.Run("BatchAtomic", func(t *testing.T) {
		s := newStore(t)
		if err := s.Put(CFKVData, []byte("x"), []byte("0")); err != nil {
			t.Fatal(err)
		}
		// a batch whose final op is invalid (unknown CF) must apply nothing
		bad := []StoreOp{
			{CF: CFKVData, Key: []byte("x"), Value: []byte("1")},
			{CF: CFKVData, Key: []byte("y"), Value: []byte("2")},
			{CF: ColumnFamily(999), Key: []byte("z"), Value: []byte("3")},
		}
		if err := s.Batch(bad); err == nil {
			t.Fatal("expected error from invalid batch")
		}
		if got, _, _ := s.Get(CFKVData, []byte("x")); !bytes.Equal(got, []byte("0")) {
			t.Fatalf("batch was not rolled back: x = %q want 0", got)
		}
		if _, found, _ := s.Get(CFKVData, []byte("y")); found {
			t.Fatal("partial batch applied: y should be absent")
		}
		// a valid batch applies all-or-nothing
		good := []StoreOp{
			{CF: CFKVData, Key: []byte("x"), Value: []byte("1")},
			{CF: CFKVData, Key: []byte("y"), Value: []byte("2")},
			{CF: CFKVData, Key: []byte("x"), Delete: false, Value: []byte("1")},
			{CF: CFKVMeta, Key: []byte("m"), Value: []byte("9")},
		}
		if err := s.Batch(good); err != nil {
			t.Fatal(err)
		}
		if got, _, _ := s.Get(CFKVData, []byte("y")); !bytes.Equal(got, []byte("2")) {
			t.Fatalf("batch y = %q want 2", got)
		}
		if got, _, _ := s.Get(CFKVMeta, []byte("m")); !bytes.Equal(got, []byte("9")) {
			t.Fatalf("batch meta m = %q want 9", got)
		}
	})

	t.Run("SnapshotIsolation", func(t *testing.T) {
		s := newStore(t)
		if err := s.Put(CFKVData, []byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = snap.Close() }()

		// a write after the snapshot must not be visible through it, via Get or Scan
		if err := s.Put(CFKVData, []byte("b"), []byte("2")); err != nil {
			t.Fatal(err)
		}
		if _, found, _ := snap.Get(CFKVData, []byte("b")); found {
			t.Fatal("snapshot Get observed a write that happened after it")
		}
		got := collectSnap(t, snap, CFKVData, nil, nil, 0)
		if want := []string{"a"}; !eqKeys(got, want) {
			t.Fatalf("snapshot scan = %v want %v (post-snapshot write leaked)", got, want)
		}

		// the live store does see it
		if _, found, _ := s.Get(CFKVData, []byte("b")); !found {
			t.Fatal("live store missing b")
		}
	})
}

func TestMemStore(t *testing.T) {
	runConformance(t, func(t *testing.T) LocalStore {
		s := NewMemStore()
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// --- helpers ---

func collect(t *testing.T, s LocalStore, cf ColumnFamily, start, end []byte, limit int) []string {
	t.Helper()
	it, err := s.Scan(cf, start, end, limit)
	if err != nil {
		t.Fatal(err)
	}
	return drain(t, it)
}

func collectSnap(t *testing.T, snap Snapshot, cf ColumnFamily, start, end []byte, limit int) []string {
	t.Helper()
	it, err := snap.Scan(cf, start, end, limit)
	if err != nil {
		t.Fatal(err)
	}
	return drain(t, it)
}

func drain(t *testing.T, it Iterator) []string {
	t.Helper()
	defer func() { _ = it.Close() }()
	var out []string
	for it.Valid() {
		out = append(out, string(it.Key()))
		it.Next()
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return out
}

func eqKeys(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
