package collections

import (
	"testing"
	"time"
)

func TestNextSweepInterval(t *testing.T) {
	base := 500 * time.Millisecond
	maxInterval := 4 * time.Second

	// Work found → reset to the base cadence (responsive under load), regardless of the current interval.
	if got := nextSweepInterval(2*time.Second, base, maxInterval, true); got != base {
		t.Fatalf("didWork → base: got %v want %v", got, base)
	}
	// No work → double, capped at maxInterval.
	if got := nextSweepInterval(base, base, maxInterval, false); got != time.Second {
		t.Fatalf("no work from base → 2×base: got %v want 1s", got)
	}
	if got := nextSweepInterval(2*time.Second, base, maxInterval, false); got != maxInterval {
		t.Fatalf("no work: 2s×2=4s (cap): got %v want %v", got, maxInterval)
	}
	if got := nextSweepInterval(maxInterval, base, maxInterval, false); got != maxInterval {
		t.Fatalf("no work at cap → stays at cap: got %v want %v", got, maxInterval)
	}
	// Never below the base floor (defensive against a zero current).
	if got := nextSweepInterval(0, base, maxInterval, false); got != base {
		t.Fatalf("zero current → base floor: got %v want %v", got, base)
	}
}
