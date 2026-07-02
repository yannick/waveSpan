package local

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestDestBatcherCoalesces pins design/37 P1.4: concurrent StoreReplica calls to one destination
// must ride a shared StoreReplicaBatch RPC (fewer RPCs than callers) and every caller must get
// the response for ITS request, positionally.
func TestDestBatcherCoalesces(t *testing.T) {
	var rpcs atomic.Int64
	batch := func(_ context.Context, reqs []*wavespanv1.StoreReplicaRequest) ([]*wavespanv1.StoreReplicaResponse, error) {
		rpcs.Add(1)
		time.Sleep(2 * time.Millisecond) // let the next group pile up while this one is in flight
		out := make([]*wavespanv1.StoreReplicaResponse, len(reqs))
		for i, r := range reqs {
			out[i] = &wavespanv1.StoreReplicaResponse{Durable: true, MemberId: string(r.GetKey())}
		}
		return out, nil
	}
	unary := func(_ context.Context, _ *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
		t.Error("unary fallback must not run when batch succeeds")
		return nil, nil
	}

	b := &destBatcher{}
	const callers = 64
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%03d", i)
			resp, err := b.enqueue(context.Background(), &wavespanv1.StoreReplicaRequest{Key: []byte(key)}, batch, unary)
			if err != nil {
				errs <- err
				return
			}
			if !resp.GetDurable() || resp.GetMemberId() != key {
				errs <- fmt.Errorf("caller %s got response for %q", key, resp.GetMemberId())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := rpcs.Load(); n >= callers {
		t.Fatalf("no coalescing: %d RPCs for %d callers", n, callers)
	}
}

// TestDestBatcherUnimplementedFallsBackToUnary covers the mixed-version cluster path: a peer
// without StoreReplicaBatch must be served by unary replay, transparently to callers.
func TestDestBatcherUnimplementedFallsBackToUnary(t *testing.T) {
	batch := func(_ context.Context, _ []*wavespanv1.StoreReplicaRequest) ([]*wavespanv1.StoreReplicaResponse, error) {
		return nil, status.Error(codes.Unimplemented, "old node")
	}
	var unaries atomic.Int64
	unary := func(_ context.Context, r *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
		unaries.Add(1)
		return &wavespanv1.StoreReplicaResponse{Durable: true, MemberId: string(r.GetKey())}, nil
	}

	b := &destBatcher{}
	resp, err := b.enqueue(context.Background(), &wavespanv1.StoreReplicaRequest{Key: []byte("k1")}, batch, unary)
	if err != nil || !resp.GetDurable() || resp.GetMemberId() != "k1" {
		t.Fatalf("fallback resp=%v err=%v", resp, err)
	}
	if unaries.Load() != 1 {
		t.Fatalf("unary calls = %d, want 1", unaries.Load())
	}
}

// TestDestBatcherCallerCancelDoesNotBlockBatch: a caller abandoning its wait must get ctx.Err()
// while the batch itself proceeds for the other callers.
func TestDestBatcherCallerCancelDoesNotBlockBatch(t *testing.T) {
	release := make(chan struct{})
	batch := func(_ context.Context, reqs []*wavespanv1.StoreReplicaRequest) ([]*wavespanv1.StoreReplicaResponse, error) {
		<-release
		out := make([]*wavespanv1.StoreReplicaResponse, len(reqs))
		for i := range reqs {
			out[i] = &wavespanv1.StoreReplicaResponse{Durable: true}
		}
		return out, nil
	}
	unary := func(_ context.Context, _ *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
		return nil, nil
	}

	b := &destBatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.enqueue(ctx, &wavespanv1.StoreReplicaRequest{Key: []byte("k1")}, batch, unary)
		done <- err
	}()
	time.Sleep(2 * batchWindow) // let the flusher pick it up and block in the RPC
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled caller stayed blocked on the batch")
	}
	close(release) // batch completes; its buffered result channel absorbs the orphan response
}
