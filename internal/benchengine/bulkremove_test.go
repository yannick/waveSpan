package benchengine

import (
	"context"
	"testing"
	"time"
)

func TestBulkRemoveOnce(t *testing.T) {
	res, err := bulkRemoveOnce(context.Background(), func(_ context.Context) (int, uint64, error) {
		time.Sleep(5 * time.Millisecond)
		return 1000, 1000, nil
	})
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if res.Sets != 1000 {
		t.Fatalf("Sets=%d want 1000", res.Sets)
	}
	if res.Removed != 1000 {
		t.Fatalf("Removed=%d want 1000", res.Removed)
	}
	if res.WallMs <= 0 {
		t.Fatalf("WallMs=%d want >0", res.WallMs)
	}
	if res.SetsPerSec <= 0 {
		t.Fatalf("SetsPerSec=%f want >0", res.SetsPerSec)
	}
}

func TestBulkRemoveOnceZeroCountNoDivByZero(t *testing.T) {
	res, err := bulkRemoveOnce(context.Background(), func(_ context.Context) (int, uint64, error) {
		return 0, 0, nil
	})
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if res.SetsPerSec != 0 {
		t.Fatalf("SetsPerSec=%f want 0", res.SetsPerSec)
	}
}
