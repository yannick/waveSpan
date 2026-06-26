package storage

import (
	"runtime"
	"testing"
)

func TestStatfsOnTempDir(t *testing.T) {
	u, err := Statfs(t.TempDir())
	if err != nil {
		t.Fatalf("Statfs: %v", err)
	}
	if runtime.GOOS == "plan9" || runtime.GOOS == "js" {
		return // fallback platforms report zero capacity, no pressure
	}
	if u.TotalBytes == 0 {
		t.Fatalf("expected non-zero capacity on %s, got %+v", runtime.GOOS, u)
	}
	if u.FreeBytes > u.TotalBytes {
		t.Fatalf("free %d exceeds total %d", u.FreeBytes, u.TotalBytes)
	}
	if f := u.FreeFraction(); f < 0 || f > 1 {
		t.Fatalf("free fraction out of range: %v", f)
	}
}

func TestFreeFractionZeroCapacityIsNoPressure(t *testing.T) {
	if got := (DiskUsage{}).FreeFraction(); got != 1 {
		t.Fatalf("zero-capacity FreeFraction should be 1 (no pressure), got %v", got)
	}
}
