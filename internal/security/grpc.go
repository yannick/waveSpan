package security

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// roleFromContext derives the caller's role for a gRPC call, mirroring EnforceHTTP: a verified
// client cert wins; otherwise, in dev mode, the x-wavespan-role metadata header (defaulting to
// admin when absent); otherwise unauthenticated.
func (id Identity) roleFromContext(ctx context.Context) Role {
	if p, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			return id.roleForCert(tlsInfo.State.PeerCertificates[0])
		}
	}
	if id.DevMode {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("x-wavespan-role"); len(v) > 0 && v[0] != "" {
				return Role(v[0])
			}
		}
		return RoleAdmin // insecure dev mode without an explicit role: full access
	}
	return RoleNone
}

// authorizeGRPC derives the role for fullMethod and returns the role plus a gRPC status error if
// the call is not permitted.
func (id Identity) authorizeGRPC(ctx context.Context, fullMethod string) (Role, error) {
	role := id.roleFromContext(ctx)
	if !Allowed(role, SurfaceForProcedure(fullMethod)) {
		if role == RoleNone {
			return role, status.Error(codes.Unauthenticated, "security: unauthenticated")
		}
		return role, status.Error(codes.PermissionDenied, "security: role not permitted for this API")
	}
	return role, nil
}

// GRPCUnaryInterceptor returns a unary server interceptor that derives the caller's role (from the
// verified client cert, or a dev-mode metadata header) and authorizes the procedure named by
// info.FullMethod before the handler runs, injecting the role into the handler's context.
func (id Identity) GRPCUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		role, err := id.authorizeGRPC(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(WithRole(ctx, role), req)
	}
}

// roleStream wraps a grpc.ServerStream so its Context() carries the authorized role.
type roleStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s roleStream) Context() context.Context { return s.ctx }

// GRPCStreamInterceptor returns a streaming server interceptor performing the same identity +
// authorization as GRPCUnaryInterceptor, wrapping the stream so its Context() carries the role.
func (id Identity) GRPCStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		role, err := id.authorizeGRPC(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, roleStream{ServerStream: ss, ctx: WithRole(ss.Context(), role)})
	}
}
