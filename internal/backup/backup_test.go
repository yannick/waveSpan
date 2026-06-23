package backup

import (
	"bytes"
	"testing"

	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/vector"
	"github.com/yannick/wavespan/internal/vector/ann"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func seededStore(t *testing.T) storage.LocalStore {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "m1", version.NewClock(nil, 500), version.NewSequencer(0))
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		ver := rs.NextVersion()
		if _, err := rs.Apply(rs.BuildRecord("default", []byte(kv.k), []byte(kv.v), ver, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
			t.Fatal(err)
		}
	}
	vs := vector.NewStore(mem)
	for _, v := range []struct {
		id  string
		val []float32
	}{{"v1", []float32{1, 0}}, {"v2", []float32{0, 1}}} {
		if err := vs.Put(&wavespanv1.VectorRecord{Collection: "docs", VectorId: v.id, Values: v.val, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	return mem
}

func TestBackupAndRestoreValidatesData(t *testing.T) {
	src := seededStore(t)
	var buf bytes.Buffer
	man, err := Backup(src, &buf, false)
	if err != nil {
		t.Fatal(err)
	}
	if man.Entries == 0 {
		t.Fatal("backup produced no entries")
	}

	dstMem := storage.NewMemStore()
	t.Cleanup(func() { _ = dstMem.Close() })
	if _, err := Restore(dstMem, &buf); err != nil {
		t.Fatal(err)
	}

	// every KV record matches the source
	srcRS := recordstore.NewStore(src, "dev", "m1", version.NewClock(nil, 500), version.NewSequencer(0))
	dstRS := recordstore.NewStore(dstMem, "dev", "m1", version.NewClock(nil, 500), version.NewSequencer(0))
	for _, k := range []string{"a", "b", "c"} {
		s, _ := srcRS.Get("default", []byte(k))
		d, _ := dstRS.Get("default", []byte(k))
		if !d.Found || string(d.Value) != string(s.Value) {
			t.Fatalf("restored KV %q = %q, want %q", k, d.Value, s.Value)
		}
	}
}

func TestRestoreRebuildsVectorIndexes(t *testing.T) {
	src := seededStore(t)
	var buf bytes.Buffer
	if _, err := Backup(src, &buf, false); err != nil {
		t.Fatal(err)
	}
	dstMem := storage.NewMemStore()
	t.Cleanup(func() { _ = dstMem.Close() })
	if _, err := Restore(dstMem, &buf); err != nil {
		t.Fatal(err)
	}

	// raw vectors restored; rebuild the ANN index from them (it was never in the backup)
	dstVS := vector.NewStore(dstMem)
	li, err := vector.RebuildLiveIndex(dstVS, "docs", vector.Cosine, ann.DefaultParams())
	if err != nil {
		t.Fatal(err)
	}
	res := li.Search([]float32{1, 0}, 1, 0)
	if len(res) != 1 || res[0].ID != "v1" {
		t.Fatalf("rebuilt vector index search wrong: %+v", res)
	}
}
