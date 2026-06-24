package rpcopts

import (
	"context"
	"testing"

	"google.golang.org/grpc"
)

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
