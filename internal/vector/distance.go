package vector

import "math"

// Metric is a vector distance metric (design/08 "Distance metrics").
type Metric int

// Supported metrics.
const (
	Cosine Metric = iota
	Dot
	L2
)

// ParseMetric maps a metric name to a Metric (default cosine).
func ParseMetric(s string) Metric {
	switch s {
	case "dot", "dot_product", "dotproduct":
		return Dot
	case "l2", "euclidean":
		return L2
	default:
		return Cosine
	}
}

// Distance returns a ranking distance where SMALLER means more similar: cosine -> 1-cosineSim,
// dot -> -dotProduct, L2 -> euclidean distance. Top-k keeps the smallest distances.
func Distance(metric Metric, a, b []float32) float64 {
	switch metric {
	case Dot:
		return -dotProduct(a, b)
	case L2:
		return math.Sqrt(l2sq(a, b))
	default: // Cosine
		return 1 - cosineSim(a, b)
	}
}

// metricDist adapts a Metric to an ann.DistanceFunc.
func metricDist(metric Metric) func(a, b []float32) float64 {
	return func(a, b []float32) float64 { return Distance(metric, a, b) }
}

// Score returns the user-facing similarity score where LARGER means more similar (the inverse
// ranking of Distance): cosine similarity, dot product, or -euclidean.
func Score(metric Metric, a, b []float32) float64 {
	switch metric {
	case Dot:
		return dotProduct(a, b)
	case L2:
		return -math.Sqrt(l2sq(a, b))
	default:
		return cosineSim(a, b)
	}
}

func cosineSim(a, b []float32) float64 {
	d := dotProduct(a, b)
	na := math.Sqrt(dotProduct(a, a))
	nb := math.Sqrt(dotProduct(b, b))
	if na == 0 || nb == 0 {
		return 0
	}
	return d / (na * nb)
}

func l2sq(a, b []float32) float64 {
	n := min(len(a), len(b))
	var sum float64
	for i := 0; i < n; i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return sum
}

// dotScalar is the reference dot product (straight accumulation).
func dotScalar(a, b []float32) float64 {
	n := min(len(a), len(b))
	var sum float64
	for i := 0; i < n; i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
