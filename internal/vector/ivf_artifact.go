package vector

import (
	"math/rand"
	"strings"

	"github.com/cwire/wavespan/internal/vector/quantizer"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// The trained IVF centroid set for a collection is stored as a single KV record under a reserved
// namespace, replicated by the normal write path. Every node periodically reads it and installs the
// quantizer, so all nodes agree on buckets (design/29 Phase 3.5).
const (
	vidxNSPrefix = "\x00vidx\x00"
	centroidKey  = "centroids"
)

// CentroidNamespace is the reserved namespace holding a collection's trained centroid artifact.
func CentroidNamespace(collection string) string { return vidxNSPrefix + collection }

// IsCentroidNamespace reports whether ns is a centroid-artifact namespace.
func IsCentroidNamespace(ns string) bool { return strings.HasPrefix(ns, vidxNSPrefix) }

// CentroidKey is the fixed key for the artifact within its namespace.
func CentroidKey() []byte { return []byte(centroidKey) }

// IVFFromProto builds an IVF quantizer from a stored centroid artifact.
func IVFFromProto(c *wavespanv1.IvfCentroids) *quantizer.IVF {
	cs := make([][]float32, len(c.GetCentroids()))
	for i, fv := range c.GetCentroids() {
		cs[i] = fv.GetValues()
	}
	return quantizer.NewIVF(cs, c.GetL2(), c.GetQver())
}

// IVFToProto serializes a trained IVF into a shareable artifact.
func IVFToProto(collection string, ivf *quantizer.IVF, dim int, l2 bool, nowMs int64) *wavespanv1.IvfCentroids {
	cs := ivf.Centroids()
	out := make([]*wavespanv1.FloatVector, len(cs))
	for i, c := range cs {
		out[i] = &wavespanv1.FloatVector{Values: c}
	}
	return &wavespanv1.IvfCentroids{
		Collection: collection, Qver: ivf.Version(), Dim: uint32(dim), L2: l2, Centroids: out, TrainedAtUnixMs: nowMs,
	}
}

// ReservoirSample returns up to limit uniformly-random local vectors for a collection (Algorithm R),
// the per-node contribution to IVF training.
func ReservoirSample(store *Store, collection string, limit int, rng *rand.Rand) [][]float32 {
	recs, err := store.ScanCollection(collection)
	if err != nil || len(recs) == 0 || limit <= 0 {
		return nil
	}
	if len(recs) <= limit {
		out := make([][]float32, len(recs))
		for i, r := range recs {
			out[i] = r.GetValues()
		}
		return out
	}
	out := make([][]float32, limit)
	for i := 0; i < limit; i++ {
		out[i] = recs[i].GetValues()
	}
	for i := limit; i < len(recs); i++ {
		if j := rng.Intn(i + 1); j < limit {
			out[j] = recs[i].GetValues()
		}
	}
	return out
}

// MetricIsL2 reports whether a collection's metric uses Euclidean distance (vs angular/dot).
func MetricIsL2(m Metric) bool { return m == L2 }
