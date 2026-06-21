// Package version implements WaveSpan's hybrid logical clock (HLC), the Version
// envelope, its deterministic last-write-wins compare order, and writer-sequence /
// mutation-identity helpers. It is the single source of truth for the ordering and
// idempotency rules specified in design/22_versioning_and_hlc.md.
package version

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// maxLogical is the 16-bit ceiling of the HLC logical counter. Exceeding it advances
// the physical component by one millisecond and resets the logical counter to zero.
const maxLogical = 0xFFFF

// Timestamp is the (physical-ms, logical) HLC pair.
type Timestamp struct {
	PhysicalMs uint64
	Logical    uint32
}

// Compare orders timestamps by physical then logical component.
func (t Timestamp) Compare(o Timestamp) int {
	if t.PhysicalMs != o.PhysicalMs {
		return cmpUint64(t.PhysicalMs, o.PhysicalMs)
	}
	return cmpUint32(t.Logical, o.Logical)
}

// SkewError is returned when a remote stamp runs further ahead of local wall-clock
// time than the configured skew bound allows.
type SkewError struct {
	RemoteMs  uint64
	WallMs    uint64
	MaxSkewMs uint64
}

func (e *SkewError) Error() string {
	return fmt.Sprintf("hlc: remote physical %dms exceeds wall %dms + maxClockSkew %dms",
		e.RemoteMs, e.WallMs, e.MaxSkewMs)
}

// Clock is a hybrid logical clock (Lamport HLC). It is safe for concurrent use.
type Clock struct {
	mu        sync.Mutex
	last      Timestamp
	wall      func() uint64
	maxSkewMs uint64

	skewRejects atomic.Uint64
}

// WallClockMs reads the system wall clock in milliseconds since the Unix epoch.
func WallClockMs() uint64 { return uint64(time.Now().UnixMilli()) }

// NewClock builds a clock from a wall-clock source and a skew bound in milliseconds.
// A nil wall source defaults to the system clock; a zero skew bound defaults to 500ms.
func NewClock(wall func() uint64, maxSkewMs uint64) *Clock {
	if wall == nil {
		wall = WallClockMs
	}
	if maxSkewMs == 0 {
		maxSkewMs = 500
	}
	return &Clock{wall: wall, maxSkewMs: maxSkewMs}
}

// Now issues a local/send HLC event (design/22, "Local / send event").
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := max64(c.last.PhysicalMs, c.wall())
	var logical uint32
	if phys == c.last.PhysicalMs {
		logical = c.last.Logical + 1
	}
	c.last = normalize(phys, logical)
	return c.last
}

// Update merges an observed remote HLC stamp (design/22, "Receive event"). It returns
// a *SkewError without advancing the clock when the remote stamp is beyond the skew bound.
func (c *Clock) Update(remote Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := c.wall()
	if remote.PhysicalMs > wall+c.maxSkewMs {
		c.skewRejects.Add(1)
		return Timestamp{}, &SkewError{RemoteMs: remote.PhysicalMs, WallMs: wall, MaxSkewMs: c.maxSkewMs}
	}

	phys := max64(max64(c.last.PhysicalMs, remote.PhysicalMs), wall)
	var logical uint32
	switch {
	case phys == c.last.PhysicalMs && phys == remote.PhysicalMs:
		logical = max32(c.last.Logical, remote.Logical) + 1
	case phys == c.last.PhysicalMs:
		logical = c.last.Logical + 1
	case phys == remote.PhysicalMs:
		logical = remote.Logical + 1
	default:
		logical = 0
	}
	c.last = normalize(phys, logical)
	return c.last, nil
}

// SkewRejections reports how many remote stamps have been rejected for excessive skew.
func (c *Clock) SkewRejections() uint64 { return c.skewRejects.Load() }

// forceLast overrides the last issued stamp. Test-only.
func (c *Clock) forceLast(t Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = t
}

// normalize carries a logical-counter overflow into the physical component so the
// logical field stays within its 16-bit range.
func normalize(phys uint64, logical uint32) Timestamp {
	for logical > maxLogical {
		phys++
		logical -= maxLogical + 1
	}
	return Timestamp{PhysicalMs: phys, Logical: logical}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpUint32(a, b uint32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
