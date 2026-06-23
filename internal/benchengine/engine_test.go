package benchengine

import (
	"context"
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
