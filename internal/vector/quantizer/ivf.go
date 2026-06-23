package quantizer

import (
	"math"
	"math/rand"
	"sort"
)

// IVF is an inverted-file quantizer: the bucket is the nearest of k trained centroids, so buckets are
// balanced and follow the data distribution (better routing selectivity than LSH). It requires
// training (k-means) but works with any metric. Probe returns the nprobe nearest centroids.
type IVF struct {
	centroids [][]float32
	l2        bool // true = Euclidean; false = angular/dot
	version   uint32
}

// NewIVF wraps a trained centroid set.
func NewIVF(centroids [][]float32, l2 bool, version uint32) *IVF {
	if version == 0 {
		version = 1
	}
	return &IVF{centroids: centroids, l2: l2, version: version}
}

// Centroids exposes the trained centroids (for persistence/gossip of the artifact).
func (v *IVF) Centroids() [][]float32 { return v.centroids }

func (v *IVF) dist(a, b []float32) float64 {
	if v.l2 {
		return l2sq(a, b)
	}
	return -dot(a, b) // angular/dot: larger inner product = closer, so negate into a distance
}

// Bucket returns the nearest centroid index.
func (v *IVF) Bucket(vec []float32) uint32 {
	best, bestD := 0, math.Inf(1)
	for i, c := range v.centroids {
		if d := v.dist(vec, c); d < bestD {
			bestD, best = d, i
		}
	}
	return uint32(best)
}

// Probe returns the nprobe nearest centroids, nearest first.
func (v *IVF) Probe(vec []float32, nprobe int) []uint32 {
	type cd struct {
		i int
		d float64
	}
	cds := make([]cd, len(v.centroids))
	for i, c := range v.centroids {
		cds[i] = cd{i, v.dist(vec, c)}
	}
	sort.Slice(cds, func(a, b int) bool { return cds[a].d < cds[b].d })
	if nprobe < 1 {
		nprobe = 1
	}
	if nprobe > len(cds) {
		nprobe = len(cds)
	}
	out := make([]uint32, nprobe)
	for i := 0; i < nprobe; i++ {
		out[i] = uint32(cds[i].i)
	}
	return out
}

// NumBuckets returns the number of IVF centroids (coarse-quantizer cells).
func (v *IVF) NumBuckets() int { return len(v.centroids) }

// Version returns the quantizer model version.
func (v *IVF) Version() uint32 { return v.version }

// Kind returns the quantizer kind identifier.
func (v *IVF) Kind() string { return "ivf" }

// TrainIVF runs Lloyd's k-means over a sample to produce k centroids. It uses k-means++ seeding for a
// good init and a bounded iteration count. An empty/too-small sample yields fewer centroids. The
// caller bumps version on retrain.
func TrainIVF(sample [][]float32, k, iters int, l2 bool, seed int64, version uint32) *IVF {
	if k < 1 {
		k = 1
	}
	if len(sample) <= k {
		// Not enough data to train k clusters: every sample point is its own centroid.
		cs := make([][]float32, len(sample))
		for i, s := range sample {
			cs[i] = append([]float32(nil), s...)
		}
		if len(cs) == 0 {
			cs = [][]float32{{}}
		}
		return NewIVF(cs, l2, version)
	}
	rng := rand.New(rand.NewSource(seed))
	tmp := &IVF{l2: l2}
	centroids := kmeansPlusPlus(sample, k, tmp, rng)
	if iters < 1 {
		iters = 8
	}
	dim := len(sample[0])
	for it := 0; it < iters; it++ {
		tmp.centroids = centroids
		sums := make([][]float64, k)
		counts := make([]int, k)
		for i := range sums {
			sums[i] = make([]float64, dim)
		}
		for _, s := range sample {
			b := tmp.Bucket(s)
			counts[b]++
			for j := 0; j < dim && j < len(s); j++ {
				sums[b][j] += float64(s[j])
			}
		}
		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				continue // keep the old centroid for an empty cluster
			}
			nc := make([]float32, dim)
			for j := 0; j < dim; j++ {
				nc[j] = float32(sums[c][j] / float64(counts[c]))
			}
			centroids[c] = nc
		}
	}
	return NewIVF(centroids, l2, version)
}

// kmeansPlusPlus picks k well-spread initial centroids.
func kmeansPlusPlus(sample [][]float32, k int, m *IVF, rng *rand.Rand) [][]float32 {
	centroids := make([][]float32, 0, k)
	centroids = append(centroids, append([]float32(nil), sample[rng.Intn(len(sample))]...))
	d2 := make([]float64, len(sample))
	for len(centroids) < k {
		m.centroids = centroids
		var total float64
		for i, s := range sample {
			best := math.Inf(1)
			for _, c := range centroids {
				if dd := m.dist(s, c); dd < best {
					best = dd
				}
			}
			if best < 0 {
				best = 0 // angular distances can be negative; clamp for sampling weight
			}
			d2[i] = best
			total += best
		}
		if total == 0 {
			centroids = append(centroids, append([]float32(nil), sample[rng.Intn(len(sample))]...))
			continue
		}
		target := rng.Float64() * total
		var acc float64
		pick := len(sample) - 1
		for i, w := range d2 {
			acc += w
			if acc >= target {
				pick = i
				break
			}
		}
		centroids = append(centroids, append([]float32(nil), sample[pick]...))
	}
	return centroids
}
