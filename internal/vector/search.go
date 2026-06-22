package vector

// LocalSearch returns this node's fragment of the top-k for a query over the vectors it holds
// locally (design/08 "scatter to partition-holders"). exact scans the whole collection; otherwise
// the ANN live index supplies candidates (over-fetched when rerank is set). Either way every kept
// hit is EXACT-scored from its stored vector, so a coordinator merging fragments from all holders
// gets the exact global top-k over the candidate union. Records tombstoned since the ANN scan are
// dropped (winner-only authoritative filter).
func LocalSearch(store *Store, meta *IndexMeta, live *LiveIndex, query []float32, k, efSearch int, exact, rerank bool) []Hit {
	if exact || live == nil {
		candidates, err := store.ScanCollection(meta.Collection)
		if err != nil {
			return nil
		}
		return SearchPartition(candidates, query, k, meta.Metric, nil)
	}
	fetch := k
	if rerank {
		fetch = k * 4
	}
	tk := NewTopK(k)
	for _, c := range live.Search(query, fetch, efSearch) {
		rec, found, _ := store.Get(meta.Collection, c.ID)
		if !found {
			continue
		}
		tk.Add(Hit{
			Collection: meta.Collection, VectorID: c.ID, GraphNodeID: rec.GetGraphNodeId(),
			Distance: Distance(meta.Metric, query, rec.GetValues()), Score: Score(meta.Metric, query, rec.GetValues()),
		})
	}
	return tk.Result()
}
