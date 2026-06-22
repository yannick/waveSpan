package graph

import (
	"testing"

	"github.com/cwire/wavespan/internal/storage"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return NewStore(mem)
}

func node(graph, id string, labels []string, props map[string]*wavespanv1.Value, phys uint64) *wavespanv1.NodeRecord {
	return &wavespanv1.NodeRecord{GraphId: graph, NodeId: id, Labels: labels, Properties: props, Version: &wavespanv1.Version{HlcPhysicalMs: phys, WriterMemberId: "m1"}}
}

func TestCreateAndGetNodeEdge(t *testing.T) {
	s := newStore(t)
	if err := s.CreateNode(node("g", "a", []string{"User"}, map[string]*wavespanv1.Value{"name": strVal("alice")}, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateNode(node("g", "b", []string{"User"}, nil, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateEdge(&wavespanv1.EdgeRecord{GraphId: "g", EdgeId: "e1", StartNode: "a", EndNode: "b", Type: "FOLLOWS", Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
		t.Fatal(err)
	}
	n, found, _ := s.GetNode("g", "a")
	if !found || string(n.GetProperties()["name"].GetStringValue()) != "alice" {
		t.Fatalf("node a not retrievable: %+v", n)
	}
	out, _ := s.ScanOutgoing("g", "a", "")
	if len(out) != 1 || out[0].GetEndNode() != "b" {
		t.Fatalf("outgoing adjacency wrong: %+v", out)
	}
	in, _ := s.ScanIncoming("g", "b", "FOLLOWS")
	if len(in) != 1 || in[0].GetStartNode() != "a" {
		t.Fatalf("incoming adjacency wrong: %+v", in)
	}
}

// failingStore wraps a MemStore and fails the Nth Batch op set, to prove atomicity.
type failingStore struct {
	storage.LocalStore
	fail bool
}

func (f *failingStore) Batch(ops []storage.StoreOp) error {
	if f.fail {
		return errInjected
	}
	return f.LocalStore.Batch(ops)
}

var errInjected = &batchError{}

type batchError struct{}

func (*batchError) Error() string { return "injected batch failure" }

func TestMutationBatchIsSingleTxn(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	fs := &failingStore{LocalStore: mem, fail: true}
	s := NewStore(fs)

	// a create that touches node + label + prop fails atomically -> nothing written
	err := s.CreateNode(node("g", "a", []string{"User"}, map[string]*wavespanv1.Value{"age": intVal(30)}, 1))
	if err == nil {
		t.Fatal("expected injected failure")
	}
	// flip to success and confirm the store had no partial write from the failed attempt
	fs.fail = false
	if _, found, _ := s.GetNode("g", "a"); found {
		t.Fatal("failed batch must not leave a partial node")
	}
	labels, _ := s.ScanLabel("g", "User")
	if len(labels) != 0 {
		t.Fatal("failed batch must not leave a label entry")
	}
}

func TestPartitionKeyStable(t *testing.T) {
	p := Partition("g", "n1")
	if p != Partition("g", "n1") {
		t.Fatal("partition must be deterministic")
	}
	if p >= NumPartitions {
		t.Fatalf("partition %d out of range", p)
	}
}
