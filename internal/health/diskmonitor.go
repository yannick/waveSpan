// Package health provides node-local resource admission control. Its first concern is disk pressure:
// a per-node monitor that watches free space on the storage volume and flips an atomic flag the write
// path checks before proposing, so a heavy write burst sheds new writes instead of filling the volume
// and crash-looping the node (design/36).
//
// Why this is a hard "never bring it down" gap: the collections-raft LogDB is pebble, an LSM that
// panics on "no space left on device". That panic fires BELOW WaveSpan — before the collections state
// machine's defensive skip-on-error can act — so once the 5Gi PVC fills, all voters panic; on restart
// the volume is still full, replay hits the same write, and they crash-loop. The fix is to stop the log
// from growing at all once free space is low: shed writes at admission, let compaction free space, then
// resume. Reads and control-plane ops are never gated — only writes that grow the volume.
package health

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// Level is the disk-pressure level reported by the monitor.
type Level int32

const (
	// LevelNone means free space is healthy; writes are admitted normally.
	LevelNone Level = iota
	// LevelPressure means free space dropped below the low watermark; application writes are shed until
	// free space recovers above the (higher) resume watermark. This is the hysteresis band.
	LevelPressure
	// LevelCritical means free space dropped below the critical watermark — the volume is nearly full and
	// an ENOSPC panic is imminent. Behaviour for the write path is identical to LevelPressure (shed); the
	// distinct level exists for alerting and so operators can see how close the node came to the edge.
	LevelCritical
)

func (l Level) String() string {
	switch l {
	case LevelNone:
		return "none"
	case LevelPressure:
		return "pressure"
	case LevelCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// Config tunes the disk-pressure monitor. Zero fields fall back to DefaultConfig.
type Config struct {
	// Path is the storage volume to watch (cfg.Storage.Path — holds the collections-raft LogDB + wavesdb).
	Path string

	// MinFreePct is the low watermark: when the free fraction drops below this, the node enters pressure
	// and sheds writes. Expressed as a percentage in (0,100). Default 8.
	MinFreePct float64
	// MinFreeBytes is an absolute floor OR-ed with MinFreePct: pressure also engages when free bytes drop
	// below this, regardless of percentage. Useful on large volumes where 8% is still many GB. 0 disables
	// the byte floor. Default 0.
	MinFreeBytes uint64
	// ResumeFreePct is the high watermark: pressure clears only once the free fraction climbs back above
	// this. Must be > MinFreePct to give hysteresis (no flapping). Default 12.
	ResumeFreePct float64
	// CriticalFreePct is the critical watermark (< MinFreePct). Default 3.
	CriticalFreePct float64

	// CheckInterval is how often the volume is polled. Default 5s.
	CheckInterval time.Duration

	// Now and Usage are injection seams for tests; nil uses time.Now and storage.Statfs.
	Now   func() time.Time
	Usage func(path string) (storage.DiskUsage, error)
}

// DefaultConfig returns the built-in watermarks and interval.
func DefaultConfig() Config {
	return Config{
		MinFreePct:      8,
		MinFreeBytes:    0,
		ResumeFreePct:   12,
		CriticalFreePct: 3,
		CheckInterval:   5 * time.Second,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MinFreePct <= 0 {
		c.MinFreePct = d.MinFreePct
	}
	if c.ResumeFreePct <= 0 {
		c.ResumeFreePct = d.ResumeFreePct
	}
	// Resume must sit strictly above the low watermark or there is no hysteresis band.
	if c.ResumeFreePct <= c.MinFreePct {
		c.ResumeFreePct = c.MinFreePct + 4
	}
	if c.CriticalFreePct <= 0 {
		c.CriticalFreePct = d.CriticalFreePct
	}
	if c.CriticalFreePct >= c.MinFreePct {
		c.CriticalFreePct = c.MinFreePct / 2
	}
	if c.CheckInterval <= 0 {
		c.CheckInterval = d.CheckInterval
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Usage == nil {
		c.Usage = storage.Statfs
	}
	return c
}

// Metrics is the optional metrics sink. nil is fine (no metrics).
type Metrics interface {
	// SetDiskPressure records the current level as a 0/1/2 gauge.
	SetDiskPressure(level Level)
	// IncShedWrites counts one write shed because of disk pressure.
	IncShedWrites()
}

// Monitor watches a volume's free space and exposes an atomic pressure flag the write path checks. It
// is safe for concurrent use: UnderPressure / Level read an atomic int32, so the hot write path never
// takes a lock.
type Monitor struct {
	cfg     Config
	metrics Metrics

	level atomic.Int32 // current Level

	mu       sync.Mutex
	lastFree float64 // last observed free fraction, for the operator view
	lastErr  error

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewMonitor builds a disk-pressure monitor over cfg. metrics may be nil. It does not start polling;
// call Start (background loop) or drive Sample directly (tests). The initial level is LevelNone.
func NewMonitor(cfg Config, metrics Metrics) *Monitor {
	m := &Monitor{
		cfg:     cfg.withDefaults(),
		metrics: metrics,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	if m.metrics != nil {
		m.metrics.SetDiskPressure(LevelNone)
	}
	return m
}

// Start runs the polling loop in a goroutine until ctx is cancelled or Stop is called. It samples once
// immediately so the flag reflects reality before the first interval elapses.
func (m *Monitor) Start(ctx context.Context) {
	go func() {
		defer close(m.doneCh)
		m.Sample() // prime the flag at startup
		t := time.NewTicker(m.cfg.CheckInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case <-t.C:
				m.Sample()
			}
		}
	}()
}

// Stop halts the polling loop and waits for it to exit. Safe to call more than once.
func (m *Monitor) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	<-m.doneCh
}

// Sample reads the volume once and updates the level with hysteresis. Exposed so tests can drive the
// state machine deterministically without the ticker. Returns the level after this sample.
func (m *Monitor) Sample() Level {
	u, err := m.cfg.Usage(m.cfg.Path)
	if err != nil {
		// A Statfs error must not engage the shed (we cannot reason about free space, and gating writes on
		// a transient stat error would self-DoS). Record it and keep the previous level.
		m.mu.Lock()
		m.lastErr = err
		m.mu.Unlock()
		return m.currentLevel()
	}
	free := u.FreeFraction()
	freePct := free * 100
	lowBytes := m.cfg.MinFreeBytes > 0 && u.FreeBytes < m.cfg.MinFreeBytes

	cur := m.currentLevel()
	next := m.transition(cur, freePct, lowBytes, u.FreeBytes)

	m.mu.Lock()
	m.lastFree = free
	m.lastErr = nil
	m.mu.Unlock()

	if next != cur {
		m.level.Store(int32(next))
		if m.metrics != nil {
			m.metrics.SetDiskPressure(next)
		}
	}
	return next
}

// transition computes the next level from the current one with hysteresis: pressure engages below the
// low watermark (or below the byte floor) and clears only above the resume watermark; critical engages
// below the critical watermark and de-escalates to plain pressure once back above it.
func (m *Monitor) transition(cur Level, freePct float64, lowBytes bool, freeBytes uint64) Level {
	belowLow := freePct < m.cfg.MinFreePct || lowBytes
	belowCritical := freePct < m.cfg.CriticalFreePct ||
		(m.cfg.MinFreeBytes > 0 && freeBytes < m.cfg.MinFreeBytes/2)
	aboveResume := freePct >= m.cfg.ResumeFreePct &&
		(m.cfg.MinFreeBytes == 0 || freeBytes >= m.cfg.MinFreeBytes)

	switch cur {
	case LevelNone:
		switch {
		case belowCritical:
			return LevelCritical
		case belowLow:
			return LevelPressure
		default:
			return LevelNone
		}
	case LevelPressure:
		switch {
		case belowCritical:
			return LevelCritical
		case aboveResume:
			return LevelNone // hysteresis: only clear above the (higher) resume watermark
		default:
			return LevelPressure
		}
	case LevelCritical:
		switch {
		case belowCritical:
			return LevelCritical
		case aboveResume:
			return LevelNone
		default:
			return LevelPressure // recovered past critical but still in the shed band
		}
	default:
		return LevelNone
	}
}

func (m *Monitor) currentLevel() Level { return Level(m.level.Load()) }

// Level returns the current disk-pressure level (atomic read, no lock).
func (m *Monitor) Level() Level { return m.currentLevel() }

// UnderPressure reports whether application writes should be shed right now (pressure OR critical). This
// is the single check the write path calls; it is an atomic load, safe on the hot path.
func (m *Monitor) UnderPressure() bool { return m.currentLevel() != LevelNone }

// Status is an operator-facing snapshot of the monitor.
type Status struct {
	Level        Level
	FreeFraction float64
	LastErr      error
}

// Status returns the current monitor snapshot for the operator view.
func (m *Monitor) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Status{Level: m.currentLevel(), FreeFraction: m.lastFree, LastErr: m.lastErr}
}
