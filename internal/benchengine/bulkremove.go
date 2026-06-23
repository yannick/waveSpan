package benchengine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/yannick/wavespan/internal/bench"
)

// BulkRemoveResult reports a single full-namespace bulk-remove run.
type BulkRemoveResult struct {
	Sets       int     `json:"sets"`
	Removed    uint64  `json:"removed"`
	WallMs     int64   `json:"wallMs"`
	SetsPerSec float64 `json:"setsPerSec"`
}

// SweepPoint is one (N, timing) datapoint of a Sweep.
type SweepPoint struct {
	N          int     `json:"n"`
	WallMs     int64   `json:"wallMs"`
	SetsPerSec float64 `json:"setsPerSec"`
}

// bulkRemoveOnce times a single remove op and builds the result. Server-free test seam.
func bulkRemoveOnce(ctx context.Context, removeOp func(ctx context.Context) (count int, removed uint64, err error)) (BulkRemoveResult, error) {
	start := time.Now()
	count, removed, err := removeOp(ctx)
	wall := time.Since(start)
	if err != nil {
		return BulkRemoveResult{}, err
	}
	res := BulkRemoveResult{
		Sets:    count,
		Removed: removed,
		WallMs:  wall.Milliseconds(),
	}
	if secs := wall.Seconds(); secs > 0 {
		res.SetsPerSec = float64(count) / secs
	}
	return res, nil
}

// RunFullNamespaceRemove times one BulkRemove(ns, nil, [member]) — removing member from EVERY set
// in ns — and reports timing. WARNING: this is destructive across the ENTIRE namespace; pass a
// dedicated/isolated ns (Sweep uses per-N sub-namespaces) so it never wipes another workload's sets.
func RunFullNamespaceRemove(ctx context.Context, dataAddr, ns string, member []byte) (BulkRemoveResult, error) {
	// The whole-namespace fan-out is one Connect call that proposes per collection through Raft, so it
	// needs a (generous) deadline; without one every propose fails with "deadline not set".
	ctx, cancel := withDeadline(ctx, bulkRemoveTimeout)
	defer cancel()
	c := bench.CollectionsClientLong(dataAddr) // no 30s client cap; the context deadline bounds the fan-out
	return bulkRemoveOnce(ctx, func(ctx context.Context) (int, uint64, error) {
		return bench.OpBulkRemove(ctx, c, ns, nil, [][]byte{member})
	})
}

// Sweep, for each N in sizes: (re)seeds N sets into a per-N sub-namespace, runs a full-namespace
// remove against exactly those sets, and appends a timing point.
func Sweep(ctx context.Context, dataAddr, baseNS string, member []byte, sizes []int, filler, conc int, progress func(msg string)) ([]SweepPoint, error) {
	if progress == nil {
		progress = func(string) {}
	}
	points := make([]SweepPoint, 0, len(sizes))
	for _, n := range sizes {
		if ctx.Err() != nil {
			return points, ctx.Err()
		}
		ns := baseNS + "/" + strconv.Itoa(n)
		progress(fmt.Sprintf("seeding %d sets into %s", n, ns))
		if err := SeedSets(ctx, dataAddr, ns, n, filler, conc, member, nil); err != nil {
			return points, err
		}
		progress(fmt.Sprintf("removing from %d sets in %s", n, ns))
		res, err := RunFullNamespaceRemove(ctx, dataAddr, ns, member)
		if err != nil {
			return points, err
		}
		points = append(points, SweepPoint{N: n, WallMs: res.WallMs, SetsPerSec: res.SetsPerSec})
	}
	return points, nil
}
