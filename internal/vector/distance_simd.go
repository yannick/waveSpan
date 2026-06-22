package vector

import "golang.org/x/sys/cpu"

// dotProduct dispatches to a block-unrolled accumulation on CPUs with wide SIMD (AVX2 / ARM ASIMD),
// falling back to the scalar reference elsewhere (design/08 "SIMD where the platform allows"). The
// block path uses independent accumulators so the compiler can vectorize; results match the scalar
// path within float tolerance.
var dotProduct = func() func(a, b []float32) float64 {
	if cpu.X86.HasAVX2 || cpu.ARM64.HasASIMD {
		return dotBlock
	}
	return dotScalar
}()

// dotBlock accumulates four partial sums in parallel (vectorizable), then combines them.
func dotBlock(a, b []float32) float64 {
	n := min(len(a), len(b))
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+4 <= n; i += 4 {
		s0 += float64(a[i]) * float64(b[i])
		s1 += float64(a[i+1]) * float64(b[i+1])
		s2 += float64(a[i+2]) * float64(b[i+2])
		s3 += float64(a[i+3]) * float64(b[i+3])
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
