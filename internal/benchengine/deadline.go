package benchengine

import (
	"context"
	"time"
)

// Collection writes commit through Raft (dragonboat SyncPropose), which REQUIRES the request context
// to carry a deadline — without one every write fails with "deadline not set". (KV writes go straight
// to wavesdb and need no deadline, which is why only the collection paths set one.) The Connect client
// turns the context deadline into the per-RPC timeout header the server propagates down to the
// propose. collOpTimeout bounds a single op (incl. a small batched bulk-remove); bulkRemoveTimeout
// bounds a whole-namespace fan-out, which issues one propose per collection and can run long.
const (
	collOpTimeout     = 30 * time.Second
	bulkRemoveTimeout = 30 * time.Minute
)

// withDeadline returns ctx bounded by d, unless it already carries a deadline (then it is returned
// unchanged with a no-op cancel). Always call the returned cancel.
func withDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
