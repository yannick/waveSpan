package planner

import (
	"fmt"

	"github.com/cwire/wavespan/internal/vector"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func init() { RegisterProcedure("vector.searchExact", vectorSearchExact) }

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
