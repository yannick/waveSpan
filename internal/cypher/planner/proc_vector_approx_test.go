package planner

import (
	"testing"

	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/vector"
	"github.com/cwire/wavespan/internal/vector/ann"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func newApproxExec(t *testing.T) (*Executor, *vector.Store, *vector.LiveIndex) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	vstore := vector.NewStore(mem)
	live := vector.NewLiveIndex(vector.Cosine, ann.Params{M: 8, EfConstruction: 64, EfSearchDefault: 32, Seed: 1})
	idx := map[string]*vector.IndexMeta{"docs": {Name: "docs", Collection: "docs", Metric: vector.Cosine, Dimensions: 2}}
	lives := map[string]*vector.LiveIndex{"docs": live}
	seq := uint64(0)
	e := &Executor{
		Store: graph.NewStore(mem), GraphID: "g", Limits: DefaultLimits(),
		Router: LocalRouter{Self: "self"}, SelfCluster: "dev", SelfMember: "self",
		VectorStore: vstore,
		VectorIndex: func(n string) (*vector.IndexMeta, bool) { m, ok := idx[n]; return m, ok },
		VectorLive:  func(n string) (*vector.LiveIndex, bool) { l, ok := lives[n]; return l, ok },
		NewVersion:  func() *wavespanv1.Version { seq++; return &wavespanv1.Version{HlcPhysicalMs: seq} },
	}
	return e, vstore, live
}

// ingest writes a vector to both the authoritative store and the live index.
func ingest(t *testing.T, vs *vector.Store, live *vector.LiveIndex, node string, id string, vals ...float32) {
	t.Helper()
	if err := vs.Put(&wavespanv1.VectorRecord{Collection: "docs", VectorId: id, Values: vals, GraphNodeId: node, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
		t.Fatal(err)
	}
	live.Insert(id, vals)
}

func TestVectorSearchApprox(t *testing.T) {
	e, vs, live := newApproxExec(t)
	ingest(t, vs, live, "", "a", 1, 0)
	ingest(t, vs, live, "", "b", 0.9, 0.1)
	ingest(t, vs, live, "", "c", 0, 1)

	res := run(t, e, "CALL vector.searchApprox('docs', [1.0, 0.0], 2, {efSearch:64}) YIELD node, score RETURN node, score")
	got := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		got[i] = r["node"].GetStringValue()
	}
	if len(got) != 2 || got[0] != "a" {
		t.Fatalf("approx search nearest = %v, want a first", got)
	}

	// tombstone 'a' in the authoritative store -> it must be filtered from approx results
	_ = vs.Delete("docs", "a", &wavespanv1.Version{HlcPhysicalMs: 9})
	live.Delete("a")
	res = run(t, e, "CALL vector.searchApprox('docs', [1.0, 0.0], 3, {efSearch:64}) YIELD node, score RETURN node, score")
	for _, r := range res.Rows {
		if r["node"].GetStringValue() == "a" {
			t.Fatal("tombstoned vector must not appear in approximate results")
		}
	}
}

func TestVectorSearchApproxRerank(t *testing.T) {
	e, vs, live := newApproxExec(t)
	ingest(t, vs, live, "", "a", 1, 0)
	ingest(t, vs, live, "", "b", 0.8, 0.2)
	ingest(t, vs, live, "", "c", 0.95, 0.05)
	res := run(t, e, "CALL vector.searchApprox('docs', [1.0, 0.0], 3, {rerank:true}) YIELD node, score RETURN node, score")
	got := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		got[i] = r["node"].GetStringValue()
	}
	// exact order by cosine: a, c, b
	if len(got) != 3 || got[0] != "a" || got[1] != "c" || got[2] != "b" {
		t.Fatalf("reranked order = %v, want a,c,b", got)
	}
}

func TestVectorSearchApproxHybrid(t *testing.T) {
	e, vs, live := newApproxExec(t)
	run(t, e, "CREATE (:Chunk {id:'ch1'})")
	run(t, e, "CREATE (:Document {id:'doc1', title:'Alpha'})")
	run(t, e, "MATCH (c:Chunk {id:'ch1'}), (d:Document {id:'doc1'}) CREATE (c)-[:PART_OF]->(d)")
	ingest(t, vs, live, "ch1", "v1", 1, 0)

	res := run(t, e, "CALL vector.searchApprox('docs', [1.0, 0.0], 1, {efSearch:64}) YIELD node, score MATCH (node)-[:PART_OF]->(d:Document) RETURN d.title, score")
	if len(res.Rows) != 1 || res.Rows[0]["d.title"].GetStringValue() != "Alpha" {
		t.Fatalf("approx hybrid query wrong: %+v", res.Rows)
	}
}
