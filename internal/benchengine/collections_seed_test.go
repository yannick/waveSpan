package benchengine

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestSeedWithCallsAddSetExactlyN(t *testing.T) {
	const n = 1000
	var calls atomic.Int64
	var lastTotal atomic.Int64
	var progressCalled atomic.Bool

	err := seedWith(context.Background(), n, 8,
		func(_ context.Context, _ int) error {
			calls.Add(1)
			return nil
		},
		func(_, total int) {
			progressCalled.Store(true)
			lastTotal.Store(int64(total))
		},
	)
	if err != nil {
		t.Fatalf("seedWith err=%v", err)
	}
	if got := calls.Load(); got != n {
		t.Fatalf("addSet called %d times, want %d", got, n)
	}
	if !progressCalled.Load() {
		t.Fatal("progress never reported")
	}
	if got := lastTotal.Load(); got != n {
		t.Fatalf("progress total=%d, want %d", got, n)
	}
}

func TestSeedWithZeroConcDefaultsToOne(t *testing.T) {
	const n = 10
	var calls atomic.Int64
	if err := seedWith(context.Background(), n, 0,
		func(_ context.Context, _ int) error { calls.Add(1); return nil },
		nil,
	); err != nil {
		t.Fatalf("err=%v", err)
	}
	if got := calls.Load(); got != n {
		t.Fatalf("calls=%d want %d", got, n)
	}
}

func TestSeedWithHonorsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := seedWith(ctx, 100000, 4,
		func(c context.Context, _ int) error { return c.Err() },
		nil,
	)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}
