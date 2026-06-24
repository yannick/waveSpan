package security

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGRPCUnaryInterceptor(t *testing.T) {
	cases := []struct {
		name       string
		id         Identity
		md         metadata.MD
		fullMethod string
		wantCode   codes.Code // codes.OK means handler must run
		wantRole   Role       // only checked when handler runs
	}{
		{
			name:       "devmode no cert no metadata defaults admin",
			id:         Identity{DevMode: true},
			fullMethod: "/wavespan.v1.KvService/Get",
			wantCode:   codes.OK,
			wantRole:   RoleAdmin,
		},
		{
			name:       "devmode reader write method denied",
			id:         Identity{DevMode: true},
			md:         metadata.Pairs("x-wavespan-role", string(RoleReader)),
			fullMethod: "/wavespan.v1.KvService/Put",
			wantCode:   codes.PermissionDenied,
		},
		{
			name:       "devmode reader read method allowed",
			id:         Identity{DevMode: true},
			md:         metadata.Pairs("x-wavespan-role", string(RoleReader)),
			fullMethod: "/wavespan.v1.KvService/Get",
			wantCode:   codes.OK,
			wantRole:   RoleReader,
		},
		{
			name:       "non-devmode no cert unauthenticated",
			id:         Identity{DevMode: false},
			fullMethod: "/wavespan.v1.KvService/Get",
			wantCode:   codes.Unauthenticated,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tc.md)
			}
			ran := false
			var gotRole Role
			handler := func(ctx context.Context, _ any) (any, error) {
				ran = true
				gotRole = RoleFrom(ctx)
				return "ok", nil
			}
			info := &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}
			_, err := tc.id.GRPCUnaryInterceptor()(ctx, nil, info, handler)

			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v, want %v (err=%v)", got, tc.wantCode, err)
			}
			if tc.wantCode == codes.OK {
				if !ran {
					t.Fatal("handler did not run")
				}
				if gotRole != tc.wantRole {
					t.Errorf("role in ctx = %q, want %q", gotRole, tc.wantRole)
				}
			} else if ran {
				t.Error("handler ran despite denial")
			}
		})
	}
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s fakeServerStream) Context() context.Context { return s.ctx }

func TestGRPCStreamInterceptor(t *testing.T) {
	id := Identity{DevMode: true}
	md := metadata.Pairs("x-wavespan-role", string(RoleReader))
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Read method on a streaming RPC: allowed, role injected into wrapped stream ctx.
	var gotRole Role
	ran := false
	handler := func(_ any, stream grpc.ServerStream) error {
		ran = true
		gotRole = RoleFrom(stream.Context())
		return nil
	}
	info := &grpc.StreamServerInfo{FullMethod: "/wavespan.v1.KvService/Get"}
	err := id.GRPCStreamInterceptor()(nil, fakeServerStream{ctx: ctx}, info, handler)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("handler did not run")
	}
	if gotRole != RoleReader {
		t.Errorf("stream role = %q, want %q", gotRole, RoleReader)
	}

	// Write method: denied.
	ran = false
	infoW := &grpc.StreamServerInfo{FullMethod: "/wavespan.v1.KvService/Put"}
	err = id.GRPCStreamInterceptor()(nil, fakeServerStream{ctx: ctx}, infoW, handler)
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", got)
	}
	if ran {
		t.Error("handler ran despite denial")
	}
}
