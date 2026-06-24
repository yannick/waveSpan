package rpcopts

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"
)

func TestGRPCConnCachesPerAddr(t *testing.T) {
	c1, err := GRPCConn("127.0.0.1:60001")
	if err != nil {
		t.Fatalf("GRPCConn: %v", err)
	}
	c2, err := GRPCConn("127.0.0.1:60001")
	if err != nil {
		t.Fatalf("GRPCConn: %v", err)
	}
	if c1 != c2 {
		t.Fatal("same addr returned different conns")
	}
	c3, err := GRPCConn("127.0.0.1:60002")
	if err != nil {
		t.Fatalf("GRPCConn: %v", err)
	}
	if c1 == c3 {
		t.Fatal("different addrs returned the same conn")
	}
}

func TestGRPCConnConcurrentRaceClean(t *testing.T) {
	const addr = "127.0.0.1:60003"
	var wg sync.WaitGroup
	conns := make([]grpc.ClientConnInterface, 32)
	for i := range conns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := GRPCConn(addr)
			if err != nil {
				t.Errorf("GRPCConn: %v", err)
				return
			}
			conns[i] = c
		}(i)
	}
	wg.Wait()
	for i := 1; i < len(conns); i++ {
		if conns[i] != conns[0] {
			t.Fatalf("conn %d differs from conn 0 for the same addr", i)
		}
	}
}

func TestGRPCMetricsUnaryInterceptorCallsThrough(t *testing.T) {
	ran := false
	handler := func(ctx context.Context, req any) (any, error) {
		ran = true
		return "ok", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/wavespan.v1.KvService/Get"}
	resp, err := GRPCMetricsUnaryInterceptor()(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("handler did not run")
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want ok", resp)
	}
}

func TestGRPCMetricsStreamInterceptorCallsThrough(t *testing.T) {
	ran := false
	handler := func(srv any, stream grpc.ServerStream) error {
		ran = true
		return nil
	}
	info := &grpc.StreamServerInfo{FullMethod: "/wavespan.v1.KvService/Scan"}
	if err := GRPCMetricsStreamInterceptor()(nil, nil, info, handler); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("handler did not run")
	}
}
