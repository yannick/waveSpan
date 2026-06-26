package health

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// fakeUsage is an injectable free-bytes source: the test sets free.Store(bytes) and Sample reads it.
type fakeUsage struct {
	total uint64
	free  atomic.Uint64
	err   atomic.Pointer[error]
}

func (f *fakeUsage) usage(string) (storage.DiskUsage, error) {
	if ep := f.err.Load(); ep != nil {
		return storage.DiskUsage{}, *ep
	}
	return storage.DiskUsage{TotalBytes: f.total, FreeBytes: f.free.Load()}, nil
}

func newMon(t *testing.T, f *fakeUsage, m Metrics) *Monitor {
	t.Helper()
	return NewMonitor(Config{
		Path:            "/fake",
		MinFreePct:      8,
		ResumeFreePct:   12,
		CriticalFreePct: 3,
		CheckInterval:   time.Hour, // Sample is driven manually
		Usage:           f.usage,
	}, m)
}

func TestMonitorHysteresis(t *testing.T) {
	f := &fakeUsage{total: 1000}
	m := newMon(t, f, nil)

	// Healthy: 50% free -> none.
	f.free.Store(500)
	if got := m.Sample(); got != LevelNone {
		t.Fatalf("50%% free: want none, got %s", got)
	}
	if m.UnderPressure() {
		t.Fatal("UnderPressure true at 50%% free")
	}

	// Drop to 7% (< 8% low watermark) -> pressure.
	f.free.Store(70)
	if got := m.Sample(); got != LevelPressure {
		t.Fatalf("7%% free: want pressure, got %s", got)
	}
	if !m.UnderPressure() {
		t.Fatal("UnderPressure false at 7%% free")
	}

	// HYSTERESIS: recover to 10% — above the 8% low watermark but BELOW the 12% resume watermark. Must
	// stay in pressure (no flapping back the moment we cross the low line).
	f.free.Store(100)
	if got := m.Sample(); got != LevelPressure {
		t.Fatalf("10%% free (in hysteresis band): want pressure, got %s", got)
	}

	// Recover above the resume watermark (13% >= 12%) -> clears to none.
	f.free.Store(130)
	if got := m.Sample(); got != LevelNone {
		t.Fatalf("13%% free: want none, got %s", got)
	}
	if m.UnderPressure() {
		t.Fatal("UnderPressure true after recovery above resume watermark")
	}
}

func TestMonitorCritical(t *testing.T) {
	f := &fakeUsage{total: 1000}
	m := newMon(t, f, nil)

	// 2% free (< 3% critical) -> critical, and UnderPressure (writes shed).
	f.free.Store(20)
	if got := m.Sample(); got != LevelCritical {
		t.Fatalf("2%% free: want critical, got %s", got)
	}
	if !m.UnderPressure() {
		t.Fatal("UnderPressure false at critical")
	}

	// Recover to 5% — past critical (3%) but still below the 12% resume watermark -> de-escalate to
	// pressure, NOT none (still shedding).
	f.free.Store(50)
	if got := m.Sample(); got != LevelPressure {
		t.Fatalf("5%% free after critical: want pressure, got %s", got)
	}

	// Full recovery clears it.
	f.free.Store(200)
	if got := m.Sample(); got != LevelNone {
		t.Fatalf("20%% free: want none, got %s", got)
	}
}

func TestMonitorByteFloor(t *testing.T) {
	// Large volume where 8% is still huge: the absolute byte floor must engage pressure independently.
	f := &fakeUsage{total: 1_000_000}
	m := NewMonitor(Config{
		Path:          "/fake",
		MinFreePct:    8,
		ResumeFreePct: 12,
		MinFreeBytes:  100_000, // 10% in bytes
		CheckInterval: time.Hour,
		Usage:         f.usage,
	}, nil)

	// 50% free by percent, but below the 100k byte floor would need <100k free; 500k > 100k so still none.
	f.free.Store(500_000)
	if got := m.Sample(); got != LevelNone {
		t.Fatalf("500k free (above byte floor): want none, got %s", got)
	}
	// 90k free: above 8% (=80k) by percent, but BELOW the 100k byte floor -> pressure.
	f.free.Store(90_000)
	if got := m.Sample(); got != LevelPressure {
		t.Fatalf("90k free (below byte floor): want pressure, got %s", got)
	}
}

func TestMonitorStatfsErrorDoesNotShed(t *testing.T) {
	f := &fakeUsage{total: 1000}
	m := newMon(t, f, nil)
	f.free.Store(500)
	m.Sample() // none

	// A Statfs error must not engage the shed (we cannot reason about free space). Keep prior level.
	statErr := context.DeadlineExceeded
	f.err.Store(&statErr)
	if got := m.Sample(); got != LevelNone {
		t.Fatalf("statfs error: want prior level none, got %s", got)
	}
	if m.UnderPressure() {
		t.Fatal("statfs error must not engage the shed")
	}
	if m.Status().LastErr == nil {
		t.Fatal("Status should surface the statfs error")
	}
}

// countingMetrics is a thread-safe Metrics sink for the test.
type countingMetrics struct {
	mu    sync.Mutex
	level Level
	setN  int
	shedN int64
}

func (c *countingMetrics) SetDiskPressure(l Level) {
	c.mu.Lock()
	c.level, c.setN = l, c.setN+1
	c.mu.Unlock()
}
func (c *countingMetrics) IncShedWrites() { atomic.AddInt64(&c.shedN, 1) }

func TestMonitorMetricsOnTransition(t *testing.T) {
	f := &fakeUsage{total: 1000}
	cm := &countingMetrics{}
	m := newMon(t, f, cm)
	// NewMonitor sets the gauge once at construction.
	cm.mu.Lock()
	base := cm.setN
	cm.mu.Unlock()

	f.free.Store(70) // -> pressure (transition, one Set)
	m.Sample()
	f.free.Store(60) // still pressure (no transition, no Set)
	m.Sample()
	f.free.Store(200) // -> none (transition, one Set)
	m.Sample()

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.setN != base+2 {
		t.Fatalf("want 2 gauge transitions after base, got %d", cm.setN-base)
	}
}

// TestMonitorConcurrent runs samples and reads concurrently; -race guards the atomic flag.
func TestMonitorConcurrent(t *testing.T) {
	f := &fakeUsage{total: 1000}
	f.free.Store(500)
	m := newMon(t, f, nil)

	var readers sync.WaitGroup
	stop := make(chan struct{})
	// Reader goroutines hammering the hot-path accessor.
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = m.UnderPressure()
					_ = m.Level()
				}
			}
		}()
	}
	// Writer flipping free space across the watermarks, concurrent with the readers.
	for i := 0; i < 2000; i++ {
		if i%2 == 0 {
			f.free.Store(20)
		} else {
			f.free.Store(500)
		}
		m.Sample()
	}
	close(stop) // stop the readers and wait for them
	readers.Wait()
}

func TestMonitorStartStop(t *testing.T) {
	f := &fakeUsage{total: 1000}
	f.free.Store(20) // critical at start
	m := newMon(t, f, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx) // primes the flag immediately
	// The immediate prime should already have flagged pressure without waiting a full interval.
	deadline := time.Now().Add(time.Second)
	for !m.UnderPressure() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !m.UnderPressure() {
		t.Fatal("Start did not prime the pressure flag")
	}
	m.Stop()
	m.Stop() // idempotent
}
