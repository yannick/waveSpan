package planner

import (
	"context"
	"fmt"

	"github.com/yannick/wavespan/internal/vector"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func init() {
	RegisterProcedure("vector.searchExact", vectorSearchExact)
	RegisterProcedure("vector.searchApprox", vectorSearchApprox)
}

// vectorSearchApprox implements CALL vector.searchApprox(indexName, queryVector, k, {efSearch, rerank})
// (design/08). It searches the local live ANN index, scatters the same query to holder peers, merges
// the per-node fragments into the global top-k, and binds node + score. Each fragment is exact-scored
// so the merged top-k is exact over the candidate union.
func vectorSearchApprox(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("vector.searchApprox(indexName, queryVector, k, opts?) requires at least 3 arguments")
	}
	if e.VectorStore == nil || e.VectorIndex == nil || e.VectorLive == nil {
		return nil, fmt.Errorf("vector.searchApprox: vector backend not configured")
	}
	indexName := args[0].GetStringValue()
	query := toFloat32(args[1])
	k := int(args[2].GetIntValue())
	efSearch, rerank := readApproxOpts(args)

	meta, ok := e.VectorIndex(indexName)
	if !ok {
		return nil, fmt.Errorf("vector.searchApprox: unknown index %q", indexName)
	}
	live, ok := e.VectorLive(indexName)
	if !ok {
		return nil, fmt.Errorf("vector.searchApprox: no live index for %q", indexName)
	}

	local := vector.LocalSearch(e.VectorStore, meta, live, query, k, efSearch, false, rerank)
	hits := e.gatherVector(indexName, query, k, efSearch, false, rerank, local)
	return e.bindVectorHits(hits, k, row), nil
}

// gatherVector merges the coordinator's local fragment with the fragments returned by holder peers
// (scatter-gather, design/08). When a holder is unreachable the result is flagged partial.
func (e *Executor) gatherVector(indexName string, query []float32, k, efSearch int, exact, rerank bool, local []vector.Hit) []vector.Hit {
	fragments := [][]vector.Hit{local}
	if e.VectorScatter != nil {
		ctx := e.Ctx
		if ctx == nil {
			ctx = context.Background()
		}
		remote, unreachable := e.VectorScatter(ctx, indexName, query, k, efSearch, exact, rerank)
		fragments = append(fragments, remote...)
		if unreachable > 0 {
			e.MarkPartial(fmt.Sprintf("vector.search: %d holder(s) unreachable; top-k may be partial", unreachable))
		}
	}
	return vector.MergeTopK(fragments, k)
}

// bindVectorHits binds each merged hit's graph node (local lookup, else its id) to `node` and its
// similarity to `score`. Per-fragment results were already authoritatively filtered by their holder.
func (e *Executor) bindVectorHits(hits []vector.Hit, k int, row bindingRow) []bindingRow {
	out := make([]bindingRow, 0, len(hits))
	for _, h := range hits {
		nr := cloneRow(row)
		nr["node"] = e.vectorNodeBinding(h.GraphNodeID, h.VectorID)
		nr["score"] = vFloat(h.Score)
		out = append(out, nr)
		if len(out) >= k {
			break
		}
	}
	return out
}

// readApproxOpts reads the optional 4th-arg map {efSearch: int, rerank: bool}.
func readApproxOpts(args []*wavespanv1.Value) (efSearch int, rerank bool) {
	if len(args) < 4 || args[3].GetMapValue() == nil {
		return 0, false
	}
	m := args[3].GetMapValue().GetEntries()
	if ef, ok := m["efSearch"]; ok {
		efSearch = int(ef.GetIntValue())
	}
	if r, ok := m["rerank"]; ok {
		rerank = r.GetBoolValue()
	}
	return efSearch, rerank
}

// vectorNodeBinding resolves a graph node binding for a hit, falling back to the vector id.
func (e *Executor) vectorNodeBinding(graphNodeID, vectorID string) any {
	if graphNodeID != "" {
		if n, found, _ := e.Store.GetNode(e.GraphID, graphNodeID); found {
			e.touch(graphNodeID)
			return n
		}
		return vStr(graphNodeID)
	}
	return vStr(vectorID)
}

// vectorSearchExact implements CALL vector.searchExact(indexName, queryVector, k) YIELD node, score
// (design/08). It exact-searches the coordinator's local vectors, scatters the same query to holder
// peers, and merges the fragments into the exact global top-k. Results are eventual (each holder
// scans its locally visible, winner-only records).
func vectorSearchExact(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("vector.searchExact(indexName, queryVector, k) requires 3 arguments")
	}
	if e.VectorStore == nil || e.VectorIndex == nil {
		return nil, fmt.Errorf("vector.searchExact: vector backend not configured")
	}
	indexName := args[0].GetStringValue()
	query := toFloat32(args[1])
	k := int(args[2].GetIntValue())

	meta, ok := e.VectorIndex(indexName)
	if !ok {
		return nil, fmt.Errorf("vector.searchExact: unknown index %q", indexName)
	}
	local := vector.LocalSearch(e.VectorStore, meta, nil, query, k, 0, true, false)
	hits := e.gatherVector(indexName, query, k, 0, true, false, local)
	return e.bindVectorHits(hits, k, row), nil
}

// toFloat32 converts a Value (list of numbers) to a query vector.
func toFloat32(v *wavespanv1.Value) []float32 {
	list := v.GetListValue()
	if list == nil {
		return nil
	}
	out := make([]float32, 0, len(list.GetValues()))
	for _, el := range list.GetValues() {
		switch x := el.GetValue().(type) {
		case *wavespanv1.Value_IntValue:
			out = append(out, float32(x.IntValue))
		case *wavespanv1.Value_DoubleValue:
			out = append(out, float32(x.DoubleValue))
		}
	}
	return out
}
