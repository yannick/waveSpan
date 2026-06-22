package vector

import (
	"context"
	"time"
)

// Merger periodically folds a live index's delta into its main segment (design/08 background
// merge). It merges when the delta reaches a threshold or on each tick.
type Merger struct {
	li        *LiveIndex
	threshold int
}

// NewMerger builds a background merger. threshold is the delta size that triggers a merge between
// ticks (0 merges on every tick).
func NewMerger(li *LiveIndex, threshold int) *Merger {
	return &Merger{li: li, threshold: threshold}
}

// MaybeMerge merges if the delta has reached the threshold.
func (m *Merger) MaybeMerge() {
	if m.li.delta.Len() >= m.threshold {
		m.li.Merge()
	}
}

// Run merges on the given interval until ctx is done.
func (m *Merger) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.li.Merge()
		}
	}
}
