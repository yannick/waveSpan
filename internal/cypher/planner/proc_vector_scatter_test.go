package planner

import (
	"context"
	"testing"

	"github.com/cwire/wavespan/internal/vector"
)

// TestVectorSearchScatterMergesRemoteFragment proves a vector held only on a peer (not on the
// coordinator) still appears in the result: the executor scatters, then merges the remote fragment.
func TestVectorSearchScatterMergesRemoteFragment(t *testing.T) {
	e, vs := newVecExec(t)
	putVec(t, vs, "local-mediocre", "", 0.5, 0.5) // local, farther from [1,0]

	e.VectorScatter = func(_ context.Context, _ string, _ []float32, _, _ int, _, _ bool) ([][]vector.Hit, int) {
		// a peer holds the true nearest vector; coordinator has no copy of it
		return [][]vector.Hit{{
			{Collection: "docs", VectorID: "remote-best", GraphNodeID: "node-remote", Distance: 0.0, Score: 1.0},
		}}, 0
	}

	res := run(t, e, "CALL vector.searchExact('docs', [1.0, 0.0], 2) YIELD node, score RETURN node, score")
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 merged rows, got %d", len(res.Rows))
	}
	if res.Rows[0]["node"].GetStringValue() != "node-remote" {
		t.Fatalf("remote-held nearest must rank first, got %v", res.Rows[0]["node"])
	}
	if res.Meta.GetPartialGraphPossible() {
		t.Fatal("no holder was unreachable; result should not be flagged partial")
	}
}

// TestVectorSearchScatterUnreachableFlagsPartial proves an unreachable holder makes the result
// honestly partial (PartialGraphPossible + a warning), instead of silently returning a subset.
func TestVectorSearchScatterUnreachableFlagsPartial(t *testing.T) {
	e, vs := newVecExec(t)
	putVec(t, vs, "a", "", 1, 0)

	e.VectorScatter = func(_ context.Context, _ string, _ []float32, _, _ int, _, _ bool) ([][]vector.Hit, int) {
		return nil, 1 // one holder unreachable
	}

	res := run(t, e, "CALL vector.searchExact('docs', [1.0, 0.0], 2) YIELD node, score RETURN node, score")
	if !res.Meta.GetPartialGraphPossible() {
		t.Fatal("an unreachable holder must flag the result partial")
	}
	found := false
	for _, w := range res.Meta.GetWarnings() {
		if len(w) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a warning explaining the partial result")
	}
}
