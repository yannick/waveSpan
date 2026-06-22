package planner

import (
	"fmt"

	"github.com/cwire/wavespan/internal/vector"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func init() {
	RegisterProcedure("vector.searchExact", vectorSearchExact)
	RegisterProcedure("vector.searchApprox", vectorSearchApprox)
}

// vectorSearchApprox implements CALL vector.searchApprox(indexName, queryVector, k, {efSearch, rerank})
// (design/08). It searches the live index (main ANN segments + delta), filters tombstoned/missing
// records against the authoritative store, optionally exact-reranks, and binds node + score.
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

	fetch := k
	if rerank {
		fetch = k * 4 // over-fetch ANN candidates, then exact-rerank down to k
	}
	cands := live.Search(query, fetch, efSearch)
	if rerank {
		cands = vector.Rerank(e.VectorStore, meta.Collection, query, cands, k, meta.Metric)
	}

	out := make([]bindingRow, 0, k)
	for _, c := range cands {
		// authoritative filter: a record tombstoned since the ANN scan is excluded (winner-only).
		rec, found, _ := e.VectorStore.Get(meta.Collection, c.ID)
		if !found {
			continue
		}
		nr := cloneRow(row)
		nr["node"] = e.vectorNodeBinding(rec.GetGraphNodeId(), c.ID)
		nr["score"] = vFloat(vector.Score(meta.Metric, query, rec.GetValues()))
		out = append(out, nr)
		if len(out) >= k {
			break
		}
	}
	return out, nil
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
// (design/08). It resolves the index, exact-searches the locally visible vectors, and binds each
// hit's graph node (or vector id) to `node` and its similarity to `score`. Results are eventual
// (locally visible records only).
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
	candidates, err := e.VectorStore.ScanCollection(meta.Collection)
	if err != nil {
		return nil, err
	}
	hits := vector.SearchPartition(candidates, query, k, meta.Metric, nil)

	out := make([]bindingRow, 0, len(hits))
	for _, h := range hits {
		nr := cloneRow(row)
		if h.GraphNodeID != "" {
			if n, found, _ := e.Store.GetNode(e.GraphID, h.GraphNodeID); found {
				nr["node"] = n
				e.touch(h.GraphNodeID)
			} else {
				nr["node"] = vStr(h.GraphNodeID)
			}
		} else {
			nr["node"] = vStr(h.VectorID)
		}
		nr["score"] = vFloat(h.Score)
		out = append(out, nr)
	}
	return out, nil
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
