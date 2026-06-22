package membership

import (
	"sync"
	"time"
)

// HolderSummary is a compact per-range holder advertisement (design/04 "Holder summaries").
// M2 ships the type and the range directory; M4 populates them. Carrying the right envelope
// now means gossip wire format is stable from the start.
type HolderSummary struct {
	Namespace         string
	RangeID           string
	MemberID          string
	BloomFilter       []byte
	ApproximateKeys   uint64
	GeneratedAtUnixMs int64
}

// Stale reports whether the summary is older than the stale TTL (design/04 "Holder summary
// staleness": default 3x gossip interval). A stale summary is a hint only, not authoritative.
func (h HolderSummary) Stale(now time.Time, staleTTL time.Duration) bool {
	return now.UnixMilli()-h.GeneratedAtUnixMs > staleTTL.Milliseconds()
}

// HolderType classifies how a member holds a range.
type HolderType int

const (
	// HolderDurable is a durable replica counted for write durability.
	HolderDurable HolderType = iota
	// HolderDynamicCache is a read-created cache replica (not durable).
	HolderDynamicCache
	// HolderSummaryOnly is known only via a gossiped summary.
	HolderSummaryOnly
)

// RangeHolder is one member's hold on a range.
type RangeHolder struct {
	MemberID string
	Type     HolderType
}

// RangeDirectory is the eventually-consistent range->holders map (design/04 "Range directory").
// It is a stub at M2 (gossip carries empty summaries) and is populated by the holder directory
// in M4. It exists now so resolution code has a stable seam.
type RangeDirectory struct {
	mu      sync.RWMutex
	holders map[string][]RangeHolder // rangeID -> holders
}

// NewRangeDirectory builds an empty directory.
func NewRangeDirectory() *RangeDirectory {
	return &RangeDirectory{holders: map[string][]RangeHolder{}}
}

// Holders returns the known holders for a range.
func (d *RangeDirectory) Holders(rangeID string) []RangeHolder {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]RangeHolder(nil), d.holders[rangeID]...)
}

// SetHolders replaces the holder list for a range (used by M4).
func (d *RangeDirectory) SetHolders(rangeID string, holders []RangeHolder) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.holders[rangeID] = append([]RangeHolder(nil), holders...)
}
