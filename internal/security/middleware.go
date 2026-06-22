package security

import (
	"context"
	"crypto/x509"
	"errors"
	"net/http"

	"connectrpc.com/connect"
)

type ctxKey int

const roleKey ctxKey = 0

// WithRole returns a context carrying the caller's role.
func WithRole(ctx context.Context, role Role) context.Context {
	return context.WithValue(ctx, roleKey, role)
}

// RoleFrom extracts the caller's role from a context (RoleNone if absent).
func RoleFrom(ctx context.Context) Role {
	if r, ok := ctx.Value(roleKey).(Role); ok {
		return r
	}
	return RoleNone
}

// Authorizer is a Connect interceptor enforcing the role/surface capability matrix on every call
// (design/15). It denies unclassified/unauthenticated internal calls and forbids replicator
// credentials from driving public writes.
type Authorizer struct{}

// WrapUnary enforces authorization on unary calls.
func (Authorizer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := authorize(ctx, req.Spec().Procedure); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingClient passes through (client-side authorization is the server's job).
func (Authorizer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler enforces authorization on streaming calls.
func (Authorizer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := authorize(ctx, conn.Spec().Procedure); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}

func authorize(ctx context.Context, procedure string) error {
	role := RoleFrom(ctx)
	surface := SurfaceForProcedure(procedure)
	if !Allowed(role, surface) {
		if role == RoleNone {
			return connect.NewError(connect.CodeUnauthenticated, errors.New("security: unauthenticated"))
		}
		return connect.NewError(connect.CodePermissionDenied, errors.New("security: role not permitted for this API"))
	}
	return nil
}

// Identity maps an authenticated caller to a role (design/15 "Identity"). PeerAllowlist gates
// replicator identities (cross-cluster peers); an off-allowlist peer maps to RoleNone.
type Identity struct {
	// CertRole maps a verified client certificate to a role (e.g. by SAN/SPIFFE id).
	CertRole func(*x509.Certificate) Role
	// PeerAllowlist is the set of peer identities permitted to act as replicators.
	PeerAllowlist map[string]bool
	// DevMode permits a plaintext X-WaveSpan-Role header (insecureDevMode only).
	DevMode bool
}

// HTTPMiddleware sets the caller's role on the request context from the verified client cert (or,
// in dev mode, from a header). It is applied before the Connect handlers.
func (id Identity) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := RoleNone
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			role = id.roleForCert(r.TLS.PeerCertificates[0])
		} else if id.DevMode {
			role = Role(r.Header.Get("X-WaveSpan-Role"))
		}
		next.ServeHTTP(w, r.WithContext(WithRole(r.Context(), role)))
	})
}

// EnforceHTTP wraps a handler with identity + authorization at the HTTP layer: it derives the
// caller's role (from the verified client cert, or a dev header in dev mode) and authorizes the
// Connect procedure named by the request path, rejecting unauthorized calls before the handler.
// This is the single integration point for the data server.
func (id Identity) EnforceHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := RoleNone
		switch {
		case r.TLS != nil && len(r.TLS.PeerCertificates) > 0:
			role = id.roleForCert(r.TLS.PeerCertificates[0])
		case id.DevMode:
			if h := r.Header.Get("X-WaveSpan-Role"); h != "" {
				role = Role(h)
			} else {
				role = RoleAdmin // insecure dev mode without an explicit role: full access
			}
		}
		if !Allowed(role, SurfaceForProcedure(r.URL.Path)) {
			if role == RoleNone {
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
			} else {
				http.Error(w, "forbidden", http.StatusForbidden)
			}
			return
		}
		next.ServeHTTP(w, r.WithContext(WithRole(r.Context(), role)))
	})
}

func (id Identity) roleForCert(cert *x509.Certificate) Role {
	if id.CertRole == nil {
		return RoleNone
	}
	role := id.CertRole(cert)
	// a replicator identity must be on the peer allowlist (cross-cluster auth, design/15).
	if role == RoleReplicator && id.PeerAllowlist != nil && !id.PeerAllowlist[cert.Subject.CommonName] {
		return RoleNone
	}
	return role
}
