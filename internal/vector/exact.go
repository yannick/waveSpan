package vector

import wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"

// Filter optionally restricts which candidate vectors are scored (graph/property filters applied
// before scoring, design/08 step 1). A nil filter scores all candidates.
type Filter func(*wavespanv1.VectorRecord) bool

// SearchPartition scans a partition's candidate vectors and returns its local top-k by exact
// distance. Tombstoned records are already excluded by the store's winner-only scan.
func SearchPartition(candidates []*wavespanv1.VectorRecord, query []float32, k int, metric Metric, filter Filter) []Hit {
	tk := NewTopK(k)
	for _, c := range candidates {
		if filter != nil && !filter(c) {
			continue
		}
		tk.Add(Hit{
			Collection: c.GetCollection(), VectorID: c.GetVectorId(), GraphNodeID: c.GetGraphNodeId(),
			Distance: Distance(metric, query, c.GetValues()), Score: Score(metric, query, c.GetValues()),
		})
	}
	return tk.Result()
}

// MergeTopK merges per-fragment top-k results into the global top-k (coordinator step).
func MergeTopK(fragments [][]Hit, k int) []Hit {
	tk := NewTopK(k)
	for _, frag := range fragments {
		for _, h := range frag {
			tk.Add(h)
		}
	}
	return tk.Result()
}
