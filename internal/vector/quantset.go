package vector

import (
	"hash/fnv"
	"sync"

	"github.com/cwire/wavespan/internal/vector/quantizer"
)

// QuantSet holds one coarse quantizer per collection — the routing tokens for kNN (design/29). For a
// cold-start collection it builds a deterministic LSH (seeded from the collection name so every node
// derives identical buckets); a trained IVF can replace it later via Set, bumping the version.
type QuantSet struct {
	mu     sync.RWMutex
	byColl map[string]quantizer.Quantizer
}

// NewQuantSet builds an LSH quantizer per collection over its index dimension. numBuckets is rounded
// up to the next power of two for LSH.
func NewQuantSet(metas []*IndexMeta, numBuckets int) *QuantSet {
	q := &QuantSet{byColl: map[string]quantizer.Quantizer{}}
	bits := quantizer.NumBitsFor(numBuckets)
	for _, m := range metas {
		if _, ok := q.byColl[m.Collection]; ok || m.Dimensions <= 0 {
			continue
		}
		q.byColl[m.Collection] = quantizer.NewLSH(m.Dimensions, bits, collectionSeed(m.Collection), 1)
	}
	return q
}

// For returns the collection's quantizer.
func (q *QuantSet) For(collection string) (quantizer.Quantizer, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	z, ok := q.byColl[collection]
	return z, ok
}

// Set installs (or replaces) a collection's quantizer — used after IVF training.
func (q *QuantSet) Set(collection string, z quantizer.Quantizer) {
	q.mu.Lock()
	q.byColl[collection] = z
	q.mu.Unlock()
}

// collectionSeed derives a stable LSH seed from the collection name so all nodes build the same
// hyperplanes (and therefore agree on which bucket a vector falls in).
func collectionSeed(collection string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(collection))
	return int64(h.Sum64())
}
