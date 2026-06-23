package quantizer

import (
	"math"
	"math/bits"
	"math/rand"
	"sort"
)

// LSH is a random-hyperplane locality-sensitive quantizer for angular/cosine (and dot) similarity:
// the bucket is the sign pattern of the vector against numBits fixed random hyperplanes, so nearby
// vectors (small angle) usually share a bucket. It needs no training — the right cold-start choice.
// numBits hyperplanes give 2^numBits buckets. Probe is multi-probe: it flips the lowest-margin bits
// (the ones the query is least confident about) to also search the most likely neighbouring buckets.
type LSH struct {
	planes  [][]float32 // numBits hyperplane normals, each of length dim
	numBits int
	version uint32
}

// NumBitsFor returns the hyperplane count whose bucket space covers at least numBuckets.
func NumBitsFor(numBuckets int) int {
	if numBuckets < 2 {
		return 1
	}
	n := bits.Len(uint(numBuckets - 1))
	if n > 24 {
		n = 24
	}
	return n
}

// NewLSH builds a quantizer over dim-dimensional vectors with 2^numBits buckets, seeded for
// determinism (so every node derives the same buckets — the seed is part of the collection config).
func NewLSH(dim, numBits int, seed int64, version uint32) *LSH {
	if numBits < 1 {
		numBits = 1
	}
	if numBits > 24 {
		numBits = 24
	}
	rng := rand.New(rand.NewSource(seed))
	planes := make([][]float32, numBits)
	for i := range planes {
		p := make([]float32, dim)
		for j := range p {
			p[j] = float32(rng.NormFloat64())
		}
		planes[i] = p
	}
	if version == 0 {
		version = 1
	}
	return &LSH{planes: planes, numBits: numBits, version: version}
}

func (l *LSH) projections(vec []float32) []float64 {
	p := make([]float64, l.numBits)
	for i, plane := range l.planes {
		p[i] = dot(plane, vec)
	}
	return p
}

func bucketFrom(proj []float64) uint32 {
	var b uint32
	for i, p := range proj {
		if p >= 0 {
			b |= 1 << uint(i)
		}
	}
	return b
}

// Bucket returns the sign-pattern bucket of vec.
func (l *LSH) Bucket(vec []float32) uint32 { return bucketFrom(l.projections(vec)) }

// Probe returns the query bucket plus the most likely neighbouring buckets (multi-probe), nearest
// first by total flip margin.
func (l *LSH) Probe(vec []float32, nprobe int) []uint32 {
	proj := l.projections(vec)
	base := bucketFrom(proj)
	if nprobe <= 1 {
		return []uint32{base}
	}
	// Order bits by |projection| ascending — the smallest margin is the bit most likely on the wrong
	// side, i.e. the cheapest flip to reach a near bucket.
	type bit struct {
		idx  int
		cost float64
	}
	bset := make([]bit, l.numBits)
	for i, p := range proj {
		bset[i] = bit{i, math.Abs(p)}
	}
	sort.Slice(bset, func(i, j int) bool { return bset[i].cost < bset[j].cost })

	out := []uint32{base}
	seen := map[uint32]bool{base: true}
	add := func(mask uint32) {
		c := base ^ mask
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	// Single-bit flips, lowest margin first.
	for _, b := range bset {
		if len(out) >= nprobe {
			return out
		}
		add(1 << uint(b.idx))
	}
	// Two-bit flips, lowest combined margin first, until satisfied.
	type pair struct {
		mask uint32
		cost float64
	}
	var pairs []pair
	for i := 0; i < len(bset); i++ {
		for j := i + 1; j < len(bset); j++ {
			pairs = append(pairs, pair{(1 << uint(bset[i].idx)) | (1 << uint(bset[j].idx)), bset[i].cost + bset[j].cost})
		}
	}
	sort.Slice(pairs, func(a, b int) bool { return pairs[a].cost < pairs[b].cost })
	for _, pr := range pairs {
		if len(out) >= nprobe {
			break
		}
		add(pr.mask)
	}
	return out
}

// NumBuckets returns the number of LSH buckets (2^numBits).
func (l *LSH) NumBuckets() int { return 1 << l.numBits }

// Version returns the quantizer model version.
func (l *LSH) Version() uint32 { return l.version }

// Kind returns the quantizer kind identifier.
func (l *LSH) Kind() string { return "lsh" }
