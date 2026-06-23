package vector

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

func vec(collection, id string, vals []float32, node string) *wavespanv1.VectorRecord {
	return &wavespanv1.VectorRecord{
		Collection: collection, VectorId: id, Values: vals, Dtype: "float32", Dimensions: uint32(len(vals)),
		GraphNodeId: node, Version: &wavespanv1.Version{HlcPhysicalMs: 1, WriterMemberId: "m1"},
	}
}

func TestVectorPutGet(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := NewStore(mem)
	v := vec("docs", "v1", []float32{1, 2, 3}, "")
	if err := s.Put(v); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Get("docs", "v1")
	if err != nil || !found {
		t.Fatalf("get: %v found=%v", err, found)
	}
	if !proto.Equal(v, got) {
		t.Fatalf("round-trip mismatch:\n%v\n%v", v, got)
	}
}

func TestVectorPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ws, err := storage.OpenWavesdb(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewStore(ws).Put(vec("docs", "v1", []float32{4, 5, 6}, "")); err != nil {
		t.Fatal(err)
	}
	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}

	ws2, err := storage.OpenWavesdb(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws2.Close() })
	got, found, err := NewStore(ws2).Get("docs", "v1")
	if err != nil || !found {
		t.Fatalf("vector not found after restart: %v found=%v", err, found)
	}
	if got.GetValues()[2] != 6 {
		t.Fatalf("value not persisted: %v", got.GetValues())
	}
}

func TestGraphAttachment(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := NewStore(mem)
	if err := s.Put(vec("docs", "v1", []float32{1, 0}, "node-a")); err != nil {
		t.Fatal(err)
	}
	// retrievable by (collection, id)
	if _, found, _ := s.Get("docs", "v1"); !found {
		t.Fatal("vector not retrievable by id")
	}
	// retrievable by graph node
	attached, err := s.GetByNode("node-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(attached) != 1 || attached[0].GetVectorId() != "v1" {
		t.Fatalf("graph attachment lookup wrong: %+v", attached)
	}
}

func TestScanCollectionFiltersTombstones(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := NewStore(mem)
	_ = s.Put(vec("docs", "v1", []float32{1, 0}, ""))
	_ = s.Put(vec("docs", "v2", []float32{0, 1}, ""))
	_ = s.Delete("docs", "v2", &wavespanv1.Version{HlcPhysicalMs: 9})
	got, _ := s.ScanCollection("docs")
	if len(got) != 1 || got[0].GetVectorId() != "v1" {
		t.Fatalf("tombstoned vector should be filtered: %+v", got)
	}
}

func TestPartitionRouting(t *testing.T) {
	pa, pb := Partition("g", "n1"), PartitionBare("c", "v1")
	if pa != Partition("g", "n1") || pb != PartitionBare("c", "v1") {
		t.Fatal("partitioning must be deterministic")
	}
	if pa >= NumPartitions || pb >= NumPartitions {
		t.Fatal("partition out of range")
	}
}
