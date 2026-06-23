package vector

import "github.com/yannick/wavespan/internal/vector/ann"

// Rerank reorders ANN candidates by exact distance over their raw vectors and returns the exact
// top-k of that candidate set (design/08 "exact reranking"). It reuses the M9 distance kernels and
// fetches authoritative raw records, filtering any that vanished (tombstoned) since the ANN scan.
func Rerank(store *Store, collection string, query []float32, candidates []ann.Candidate, k int, metric Metric) []ann.Candidate {
	tk := NewTopK(k)
	for _, c := range candidates {
		v, found, err := store.Get(collection, c.ID)
		if err != nil || !found {
			continue
		}
		tk.Add(Hit{
			VectorID: c.ID, Distance: Distance(metric, query, v.GetValues()), Score: Score(metric, query, v.GetValues()),
		})
	}
	hits := tk.Result()
	out := make([]ann.Candidate, len(hits))
	for i, h := range hits {
		out[i] = ann.Candidate{ID: h.VectorID, Distance: h.Distance}
	}
	return out
}
