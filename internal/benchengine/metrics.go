package benchengine

import (
	"math/bits"
	"time"
)

const subBucketBits = 3 // 8 sub-buckets per power of two → ≤~7% relative error

// Hist is a fixed-bucket log-linear latency histogram (HDR-lite): cheap Record, approximate
// percentiles, no per-sample retention. Microsecond resolution.
type Hist struct {
	buckets []uint64
	count   uint64
}

// NewHist returns an empty latency histogram.
func NewHist() *Hist { return &Hist{buckets: make([]uint64, 64<<subBucketBits)} }

func bucketIndex(us uint64) int {
	if us == 0 {
		return 0
	}
	hi := bits.Len64(us) - 1 // power of two
	sub := 0
	if hi >= subBucketBits {
		sub = int((us >> (hi - subBucketBits)) & ((1 << subBucketBits) - 1))
	}
	return hi<<subBucketBits | sub
}

func bucketValueUS(idx int) uint64 { // midpoint reconstruction
	hi := idx >> subBucketBits
	sub := idx & ((1 << subBucketBits) - 1)
	if hi < subBucketBits {
		return uint64(idx)
	}
	base := uint64(1) << hi
	step := base >> subBucketBits
	return base + uint64(sub)*step + step/2
}

// Record adds one latency observation.
func (h *Hist) Record(d time.Duration) {
	us := uint64(d.Microseconds())
	i := bucketIndex(us)
	if i >= len(h.buckets) {
		i = len(h.buckets) - 1
	}
	h.buckets[i]++
	h.count++
}

// Count returns the number of recorded observations.
func (h *Hist) Count() uint64 { return h.count }

// Merge folds another histogram's buckets into h.
func (h *Hist) Merge(o *Hist) {
	for i := range o.buckets {
		h.buckets[i] += o.buckets[i]
	}
	h.count += o.count
}

// Percentile returns the approximate q-quantile latency (q in [0,1]).
func (h *Hist) Percentile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	target := uint64(float64(h.count) * q)
	var cum uint64
	for i, c := range h.buckets {
		cum += c
		if cum >= target {
			return time.Duration(bucketValueUS(i)) * time.Microsecond
		}
	}
	return 0
}

// WindowStat is one workload's stats over a window (or cumulative). Latencies in ms (float).
type WindowStat struct {
	Tput  float64 `json:"tput"` // ops/sec
	P50Ms float64 `json:"p50Ms"`
	P95Ms float64 `json:"p95Ms"`
	P99Ms float64 `json:"p99Ms"`
	Errs  uint64  `json:"errs"`
	Total uint64  `json:"total"`
}

// Sample is one tick across all running workloads.
type Sample struct {
	TimeMs      int64                 `json:"timeMs"`
	PerWorkload map[string]WindowStat `json:"perWorkload"`
}
