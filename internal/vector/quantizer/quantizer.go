// Package quantizer maps a high-dimensional vector to a small coarse "bucket" id, the routing token
// that lets a kNN query reach only the nodes holding its nearest buckets instead of every holder
// (design/29). Two quantizers are provided: LSH (random hyperplanes, zero training, angular/cosine)
// and IVF (k-means centroids, trained, any metric). Both expose the same interface so a collection
// picks one without the routing layer caring which.
package quantizer

// Quantizer assigns a vector to a bucket and enumerates the buckets a query should probe.
type Quantizer interface {
	// Bucket returns the single bucket a vector belongs to.
	Bucket(vec []float32) uint32
	// Probe returns up to nprobe buckets to search for a query, nearest/most-likely first (always
	// including the query's own bucket). nprobe<=1 returns just the bucket.
	Probe(vec []float32, nprobe int) []uint32
	// NumBuckets is the bucket-space size.
	NumBuckets() int
	// Version changes when the quantizer's mapping changes (e.g. IVF retrain), so stale buckets can be
	// distinguished and re-bucketed lazily.
	Version() uint32
	// Kind names the quantizer ("lsh" | "ivf").
	Kind() string
}

// dot is the inner product of two equal-length vectors.
func dot(a, b []float32) float64 {
	var s float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

// l2sq is the squared Euclidean distance.
func l2sq(a, b []float32) float64 {
	var s float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		d := float64(a[i]) - float64(b[i])
		s += d * d
	}
	return s
}
