package benchengine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngineLifecycleWithFakeOp(t *testing.T) {
	var ops atomic.Int64
	r := newRunForTest(func(ctx context.Context) error { ops.Add(1); return nil }, 4 /*workers*/)

	if r.State() != StateIdle {
		t.Fatalf("state=%v", r.State())
	}
	ch, unsub := r.Subscribe()
	defer unsub()
	r.Start()
	if r.State() != StateRunning {
		t.Fatal("not running")
	}

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no sample within 3s")
	}

	r.Pause()
	if r.State() != StatePaused {
		t.Fatal("not paused")
	}
	before := ops.Load()
	time.Sleep(200 * time.Millisecond)
	if grew := ops.Load() - before; grew > int64(4) {
		t.Fatalf("ops kept growing while paused: +%d", grew)
	}
	r.Resume()
	time.Sleep(100 * time.Millisecond)
	if ops.Load() == before {
		t.Fatal("resume did not continue")
	}

	r.Stop()
	if r.State() != StateDone && r.State() != StateStopped {
		t.Fatalf("state=%v", r.State())
	}
}

// TestConcurrentControl hammers Pause/Resume/Stop/State from several goroutines at once. It exists
// to catch the pause-gate divergence bug where a Resume could Unlock a gate that was never Locked
// (fatal "sync: Unlock of unlocked RWMutex", which kills the process and cannot be recovered).
// Must pass under -race and must not crash.
func TestConcurrentControl(t *testing.T) {
	r := newRunForTest(func(ctx context.Context) error { return nil }, 4 /*workers*/)
	r.Start()

	const (
		goroutines = 4
		iterations = 80
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch i % 3 {
				case 0:
					r.Pause()
				case 1:
					r.Resume()
				default:
					_ = r.State()
				}
			}
		}()
	}
	wg.Wait()

	r.Stop()
	if s := r.State(); s != StateStopped && s != StateDone {
		t.Fatalf("state after stop=%v", s)
	}
}
