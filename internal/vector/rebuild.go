package vector

import "github.com/yannick/wavespan/internal/vector/ann"

// RebuildLiveIndex reconstructs a live index from the authoritative raw vector records of a
// collection (design/08 "Index rebuild"). The ANN index is fully derived: ScanCollection already
// skips tombstones and losing siblings (winner-only).
func RebuildLiveIndex(store *Store, collection string, metric Metric, params ann.Params) (*LiveIndex, error) {
	recs, err := store.ScanCollection(collection)
	if err != nil {
		return nil, err
	}
	vecs := make(map[string][]float32, len(recs))
	for _, r := range recs {
		vecs[r.GetVectorId()] = r.GetValues()
	}
	return &LiveIndex{
		metric: metric, params: params,
		main:  buildSegment(metric, params, vecs),
		delta: NewDelta(metric),
	}, nil
}
