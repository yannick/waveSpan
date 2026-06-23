package benchengine

import (
	"testing"
	"time"
)

func TestHistPercentiles(t *testing.T) {
	h := NewHist()
	for i := 1; i <= 1000; i++ { // 1ms..1000ms uniform
		h.Record(time.Duration(i) * time.Millisecond)
	}
	approx := func(got, want time.Duration) bool {
		lo, hi := time.Duration(float64(want)*0.95), time.Duration(float64(want)*1.05)
		return got >= lo && got <= hi
	}
	if p := h.Percentile(0.50); !approx(p, 500*time.Millisecond) {
		t.Fatalf("p50=%v want ~500ms", p)
	}
	if p := h.Percentile(0.99); !approx(p, 990*time.Millisecond) {
		t.Fatalf("p99=%v want ~990ms", p)
	}
}

func TestHistCountAndMerge(t *testing.T) {
	a, b := NewHist(), NewHist()
	a.Record(5 * time.Millisecond)
	b.Record(7 * time.Millisecond)
	a.Merge(b)
	if a.Count() != 2 {
		t.Fatalf("count=%d want 2", a.Count())
	}
}
