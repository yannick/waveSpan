package local

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Coalescing knobs for the per-destination replica-write batcher (design/37 P1.4). The shape
// mirrors the consensus proposer (collections/proposer.go): concurrent StoreReplica calls to the
// same peer within batchWindow ride one StoreReplicaBatch RPC — one round-trip and one
// receiver-side WAL commit group instead of a unary call per record (the serial unary path was
// 99.55% of write-path blocking, perf-report-grpc). A lone write pays at most batchWindow extra.
const (
	batchWindow = 200 * time.Microsecond
	batchMaxOps = 128
	// batchRPCTimeout bounds the coalesced RPC. Individual callers still honor their own
	// (usually tighter) contexts while waiting: a caller that gives up just stops listening —
	// the batch itself completes or times out for everyone at this bound.
	batchRPCTimeout = 2 * time.Second
)

type storeResult struct {
	resp *wavespanv1.StoreReplicaResponse
	err  error
}

type pendingStore struct {
	req *wavespanv1.StoreReplicaRequest
	res chan storeResult // buffered(1); the flusher never blocks on a departed caller
}

// destBatcher coalesces StoreReplica calls to ONE destination address. The first enqueuer whose
// arrival finds no flusher becomes the flusher (group-commit leader pattern, wal.AppendBatch):
// it sleeps batchWindow to let concurrent callers pile on, then ships the whole queue.
type destBatcher struct {
	mu       sync.Mutex
	queue    []*pendingStore
	flushing bool
}

// storeBatchCall issues one coalesced batch to addr and returns positional responses.
type storeBatchCall func(ctx context.Context, reqs []*wavespanv1.StoreReplicaRequest) ([]*wavespanv1.StoreReplicaResponse, error)

// storeUnaryCall issues one legacy unary StoreReplica (mixed-version fallback).
type storeUnaryCall func(ctx context.Context, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error)

func (b *destBatcher) enqueue(ctx context.Context, req *wavespanv1.StoreReplicaRequest, batch storeBatchCall, unary storeUnaryCall) (*wavespanv1.StoreReplicaResponse, error) {
	p := &pendingStore{req: req, res: make(chan storeResult, 1)}
	b.mu.Lock()
	b.queue = append(b.queue, p)
	if !b.flushing {
		b.flushing = true
		go b.flushLoop(batch, unary)
	}
	b.mu.Unlock()

	select {
	case r := <-p.res:
		return r.resp, r.err
	case <-ctx.Done():
		// The batch flies on without this caller; its entry still replicates (extra durability,
		// same as a late fanout push) — only the ack is abandoned.
		return nil, ctx.Err()
	}
}

// flushLoop drains the queue in batchWindow-sized groups until it is empty, then exits (a new
// arrival starts a fresh flusher). Runs on its own goroutine; at most one per destination.
func (b *destBatcher) flushLoop(batch storeBatchCall, unary storeUnaryCall) {
	for {
		time.Sleep(batchWindow)
		b.mu.Lock()
		group := b.queue
		if len(group) > batchMaxOps {
			group, b.queue = group[:batchMaxOps], group[batchMaxOps:]
		} else {
			b.queue = nil
		}
		if len(group) == 0 {
			b.flushing = false
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()
		b.ship(group, batch, unary)
	}
}

func (b *destBatcher) ship(group []*pendingStore, batch storeBatchCall, unary storeUnaryCall) {
	ctx, cancel := context.WithTimeout(context.Background(), batchRPCTimeout)
	defer cancel()

	reqs := make([]*wavespanv1.StoreReplicaRequest, len(group))
	for i, p := range group {
		reqs[i] = p.req
	}
	resps, err := batch(ctx, reqs)
	switch {
	case err == nil && len(resps) == len(group):
		for i, p := range group {
			p.res <- storeResult{resp: resps[i]}
		}
		return
	case status.Code(err) == codes.Unimplemented:
		// Mixed-version cluster: the peer predates StoreReplicaBatch. Replay this group as unary
		// calls (one wasted batch attempt per group until the peer upgrades — cheap and self-healing).
		for _, p := range group {
			resp, uerr := unary(ctx, p.req)
			p.res <- storeResult{resp: resp, err: uerr}
		}
		return
	case err == nil:
		err = status.Errorf(codes.Internal, "wavespan: StoreReplicaBatch returned %d responses for %d requests", len(resps), len(group))
	}
	for _, p := range group {
		p.res <- storeResult{err: err}
	}
}
