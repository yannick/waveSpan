package planner

import (
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/vector"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func newVecExec(t *testing.T) (*Executor, *vector.Store) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	vstore := vector.NewStore(mem)
	idx := map[string]*vector.IndexMeta{
		"docs": {Name: "docs", Collection: "docs", Metric: vector.Cosine, Dimensions: 2, ExactEnabled: true},
	}
	seq := uint64(0)
	e := &Executor{
		Store: graph.NewStore(mem), GraphID: "g", Limits: DefaultLimits(),
		Router: LocalRouter{Self: "self"}, SelfCluster: "dev", SelfMember: "self",
		VectorStore: vstore,
		VectorIndex: func(name string) (*vector.IndexMeta, bool) { m, ok := idx[name]; return m, ok },
		NewVersion: func() *wavespanv1.Version {
			seq++
			return &wavespanv1.Version{HlcPhysicalMs: seq, WriterMemberId: "self", WriterSequence: seq}
		},
	}
	return e, vstore
}

func putVec(t *testing.T, s *vector.Store, id string, node string, vals ...float32) {
	t.Helper()
	if err := s.Put(&wavespanv1.VectorRecord{Collection: "docs", VectorId: id, Values: vals, Dimensions: uint32(len(vals)), GraphNodeId: node, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
		t.Fatal(err)
	}
}

func TestVectorSearchExactProcedure(t *testing.T) {
	e, vs := newVecExec(t)
	putVec(t, vs, "a", "", 1, 0)     // closest to [1,0]
	putVec(t, vs, "b", "", 0.9, 0.1) // second
	putVec(t, vs, "c", "", 0, 1)     // orthogonal
	putVec(t, vs, "d", "", -1, 0)    // opposite

	res := run(t, e, "CALL vector.searchExact('docs', [1.0, 0.0], 2) YIELD node, score RETURN node, score")
	if len(res.Rows) != 2 {
		t.Fatalf("expected top-2, got %d rows", len(res.Rows))
	}
	// rows are in score order (closest first): a then b
	if res.Rows[0]["node"].GetStringValue() != "a" || res.Rows[1]["node"].GetStringValue() != "b" {
		t.Fatalf("nearest order wrong: %v, %v", res.Rows[0]["node"], res.Rows[1]["node"])
	}
	if res.Rows[0]["score"].GetDoubleValue() < res.Rows[1]["score"].GetDoubleValue() {
		t.Fatal("scores should be descending (closest highest)")
	}
}

func TestVectorSearchExactHybrid(t *testing.T) {
	e, vs := newVecExec(t)
	// graph: chunk nodes attached to vectors, each PART_OF a Document
	run(t, e, "CREATE (:Chunk {id:'ch1'})")
	run(t, e, "CREATE (:Chunk {id:'ch2'})")
	run(t, e, "CREATE (:Document {id:'doc1', title:'Alpha'})")
	run(t, e, "CREATE (:Document {id:'doc2', title:'Beta'})")
	run(t, e, "MATCH (c:Chunk {id:'ch1'}), (d:Document {id:'doc1'}) CREATE (c)-[:PART_OF]->(d)")
	run(t, e, "MATCH (c:Chunk {id:'ch2'}), (d:Document {id:'doc2'}) CREATE (c)-[:PART_OF]->(d)")
	putVec(t, vs, "v1", "ch1", 1, 0)
	putVec(t, vs, "v2", "ch2", 0, 1)

	// a query near [1,0] should surface ch1 -> doc1 (Alpha) first
	res := run(t, e, "CALL vector.searchExact('docs', [1.0, 0.0], 1) YIELD node, score MATCH (node)-[:PART_OF]->(d:Document) RETURN d.title, score")
	if len(res.Rows) != 1 || res.Rows[0]["d.title"].GetStringValue() != "Alpha" {
		t.Fatalf("hybrid vector+graph query wrong: %+v", res.Rows)
	}
	if !res.Meta.GetPartialGraphPossible() && len(res.Meta.GetParticipatingMembers()) <= 1 {
		// single-pod local -> not partial; that's fine, just assert meta exists
		if res.Meta.GetConsistency() != wavespanv1.QueryConsistency_QUERY_CONSISTENCY_EVENTUAL {
			t.Fatal("hybrid query must declare eventual consistency")
		}
	}
}
