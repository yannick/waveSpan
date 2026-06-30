package backup

import (
	"bufio"
	"io"
	"testing"

	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// kvVersionsInObject decodes the emitted CFKVData object (repeating length-prefixed key,value) and
// returns the HLC physical-ms of each record's version.
func kvVersionsInObject(t *testing.T, objStore ObjectStore, key string) []uint64 {
	t.Helper()
	rc, err := objStore.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer func() { _ = rc.Close() }()
	br := bufio.NewReader(rc)
	var out []uint64
	for {
		_, err := readBytes(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read key: %v", err)
		}
		val, err := readBytes(br)
		if err != nil {
			t.Fatalf("read value: %v", err)
		}
		rec, err := storage.DecodeStoredRecord(val)
		if err != nil {
			t.Fatalf("decode record: %v", err)
		}
		out = append(out, rec.GetVersion().GetHlcPhysicalMs())
	}
	return out
}

func has(xs []uint64, v uint64) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// TestExportLogicalKVCut proves the HLC ≤T cut on CFKVData: a key with v1(ms=100) and v2(ms=200),
// exported at frontierT=150, emits v1 but NOT v2; frontierT=0 disables the cut and emits both.
func TestExportLogicalKVCut(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))

	v1 := version.Version{HLCPhysicalMs: 100, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: 1}
	v2 := version.Version{HLCPhysicalMs: 200, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: 2}
	if _, err := rs.Apply(rs.BuildRecord("app", []byte("k"), []byte("a"), v1, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Apply(rs.BuildRecord("app", []byte("k"), []byte("b"), v2, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}

	// Cut at T=150: v1 included, v2 excluded.
	cut, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(mem, cut, "bk/nodes/m1", DefaultRegistry(), 150, Selector{}); err != nil {
		t.Fatalf("ExportLogical(T=150): %v", err)
	}
	got := kvVersionsInObject(t, cut, "bk/nodes/m1/cf/kv_data")
	if !has(got, 100) || has(got, 200) {
		t.Fatalf("cut T=150 exported versions %v, want {100} and NOT 200", got)
	}

	// No cut (T=0): both versions exported (back-compat).
	full, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(mem, full, "bk/nodes/m1", DefaultRegistry(), 0, Selector{}); err != nil {
		t.Fatalf("ExportLogical(T=0): %v", err)
	}
	all := kvVersionsInObject(t, full, "bk/nodes/m1/cf/kv_data")
	if !has(all, 100) || !has(all, 200) {
		t.Fatalf("no-cut exported versions %v, want both 100 and 200", all)
	}
}
