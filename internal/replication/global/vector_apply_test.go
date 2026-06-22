package global

import (
	"testing"

	"github.com/cwire/wavespan/internal/vector"
	"github.com/cwire/wavespan/internal/vector/ann"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func TestAppliedVectorEntersDeltaIndex(t *testing.T) {
	is := vector.NewIndexSet([]*vector.IndexMeta{
		{Name: "docs", Collection: "docs", Metric: vector.Cosine, Dimensions: 2},
	}, ann.DefaultParams())

	applier := NewApplier(nil, nil, nil) // KV store unused on the vector path
	applier.SetVectorSink(is.OnWrite)

	vrec := &wavespanv1.VectorRecord{Collection: "docs", VectorId: "v1", Values: []float32{1, 0}, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
	sr, err := vector.Wrap(vrec)
	if err != nil {
		t.Fatal(err)
	}
	m := &wavespanv1.GlobalMutation{
		Id:        &wavespanv1.GlobalMutationId{ClusterId: "test-a", MemberId: "a1", WriterSequence: 1},
		Namespace: sr.GetNamespace(), Key: sr.GetLogicalKey(), Record: sr,
	}
	applied, err := applier.Apply(m)
	if err != nil || !applied {
		t.Fatalf("apply: applied=%v err=%v", applied, err)
	}

	// the applied raw vector is now searchable via the local live index (no HNSW internals crossed)
	live, _ := is.Live("docs")
	res := live.Search([]float32{1, 0}, 1, 0)
	if len(res) != 1 || res[0].ID != "v1" {
		t.Fatalf("applied vector not in local index: %+v", res)
	}

	// a replay is a no-op
	if again, _ := applier.Apply(m); again {
		t.Fatal("replayed vector mutation should be a no-op")
	}
}
