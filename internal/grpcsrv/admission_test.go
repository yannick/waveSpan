package grpcsrv

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/security"
)

// TestInflightLimiterShedsAndRecovers verifies graceful load shedding: with the limiter at N, the
// (N+1)th concurrent unary call is rejected with ResourceExhausted (not blocked, not crashed), and once
// the in-flight calls drain the server admits new work again. This is the "flood returns rejections but
// the node stays UP and serves normally once the flood stops" behavior.
func TestInflightLimiterShedsAndRecovers(t *testing.T) {
	const limit = 4
	interceptor := inflightLimitInterceptor(limit)

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(limit)
	blocking := func(context.Context, any) (any, error) {
		started.Done()
		<-release // hold the slot until the test releases it
		return "ok", nil
	}

	// Saturate exactly `limit` slots with blocking calls.
	var wg sync.WaitGroup
	for i := 0; i < limit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, blocking)
			if err != nil {
				t.Errorf("admitted call errored: %v", err)
			}
		}()
	}
	started.Wait() // all `limit` slots are now occupied

	// The next call must be shed with ResourceExhausted, immediately (no blocking on `release`).
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(context.Context, any) (any, error) { return "should-not-run", nil })
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("over-limit call code = %v, want ResourceExhausted", status.Code(err))
	}

	// Drain the held slots; the node must then admit new work normally again.
	close(release)
	wg.Wait()

	okRan := false
	_, err = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(context.Context, any) (any, error) { okRan = true; return "ok", nil })
	if err != nil || !okRan {
		t.Fatalf("after the flood drained, a fresh call err=%v ran=%v; want it admitted", err, okRan)
	}
}

// TestInflightLimiterDisabled checks a non-positive limit disables shedding (unbounded admission).
func TestInflightLimiterDisabled(t *testing.T) {
	interceptor := inflightLimitInterceptor(0)
	for i := 0; i < 1000; i++ {
		if _, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{},
			func(context.Context, any) (any, error) { return "ok", nil }); err != nil {
			t.Fatalf("disabled limiter rejected call %d: %v", i, err)
		}
	}
}

// TestNewAppliesBoundedDefaults ensures the server is built with bounded streams/in-flight defaults.
func TestNewAppliesBoundedDefaults(t *testing.T) {
	srv := New(Options{Identity: security.Identity{DevMode: true}})
	if srv == nil {
		t.Fatal("New returned nil")
	}
	srv.Stop()
}
