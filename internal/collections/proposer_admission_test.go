package collections

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// blockingShard is a fake asyncShard whose Propose blocks until released, so the proposer's per-shard
// queue fills up and exercises the non-blocking (load-shedding) enqueue.
type blockingShard struct {
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func (b *blockingShard) Propose(ctx context.Context, _ uint64, _ []byte) (ProposeResult, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	select {
	case <-b.release:
		return ProposeResult{Value: 1}, nil
	case <-ctx.Done():
		return ProposeResult{}, ctx.Err()
	}
}

// TestProposerShedsWhenQueueFull verifies the proposer rejects with ErrBusy (non-blocking enqueue) once
// the bounded per-shard queue is saturated, instead of blocking the caller and building an unbounded
// backlog. After the backlog drains, the proposer serves normally again — the node stays UP.
func TestProposerShedsWhenQueueFull(t *testing.T) {
	bs := &blockingShard{release: make(chan struct{})}
	p := newProposer(bs, time.Hour, 1) // huge window + maxOps 1 so the flusher parks on the first job
	ctx := context.Background()

	// Fire enough concurrent proposes to overflow the queue (depth + flusher in-flight + slack).
	const flood = proposeQueueDepth + 256
	var busy, other int
	var mu sync.Mutex
	var wg sync.WaitGroup
	cmd := encodeCommand(command{Op: opSAdd, NS: []byte("n"), Coll: []byte("c"), Items: itemsFromKeys([][]byte{[]byte("x")})})
	for i := 0; i < flood; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err := p.Propose(cctx, 1, cmd)
			mu.Lock()
			switch {
			case errors.Is(err, ErrBusy):
				busy++
			default:
				other++ // a success (1) or a ctx timeout after release — both fine
			}
			mu.Unlock()
		}()
	}

	// Give the flood time to saturate the queue, then confirm shedding happened.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		b := busy
		mu.Unlock()
		if b > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	if busy == 0 {
		mu.Unlock()
		t.Fatal("no ErrBusy under a flood that overflows the queue — the proposer accepted unbounded backlog")
	}
	mu.Unlock()

	// Drain: release the blocked Propose calls so the backlog clears and goroutines finish.
	close(bs.release)
	wg.Wait()

	// The node stays healthy: a fresh proposal now succeeds.
	res, err := p.Propose(ctx, 1, cmd)
	if err != nil || res.Value != 1 {
		t.Fatalf("after the flood drained, Propose = %+v,%v want success", res, err)
	}
}
