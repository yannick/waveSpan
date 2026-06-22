package security

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
)

func TestRoleCapabilities(t *testing.T) {
	// reader: public read yes, public write no
	if !Allowed(RoleReader, SurfacePublicRead) || Allowed(RoleReader, SurfacePublicWrite) {
		t.Fatal("reader caps wrong")
	}
	// writer: public write yes
	if !Allowed(RoleWriter, SurfacePublicWrite) {
		t.Fatal("writer should write")
	}
	// replicator: internal yes, public write NO (the credential-separation rule)
	if !Allowed(RoleReplicator, SurfaceInternal) {
		t.Fatal("replicator should call internal API")
	}
	if Allowed(RoleReplicator, SurfacePublicWrite) || Allowed(RoleReplicator, SurfacePublicRead) {
		t.Fatal("replication credentials must NOT drive public writes/reads")
	}
	// operator: admin yes
	if !Allowed(RoleOperator, SurfaceAdmin) {
		t.Fatal("operator should call admin")
	}
	// admin: everything
	for _, s := range []Surface{SurfacePublicRead, SurfacePublicWrite, SurfaceInternal, SurfaceAdmin} {
		if !Allowed(RoleAdmin, s) {
			t.Fatalf("admin should be allowed on surface %d", s)
		}
	}
	// unauthenticated: nothing
	for _, s := range []Surface{SurfacePublicRead, SurfacePublicWrite, SurfaceInternal, SurfaceAdmin} {
		if Allowed(RoleNone, s) {
			t.Fatalf("unauthenticated must be denied on surface %d", s)
		}
	}
}

func TestSurfaceMapping(t *testing.T) {
	cases := map[string]Surface{
		"/wavespan.v1.KvService/Get":                   SurfacePublicRead,
		"/wavespan.v1.KvService/Put":                   SurfacePublicWrite,
		"/wavespan.v1.VectorService/Put":               SurfacePublicWrite,
		"/wavespan.v1.ReplicationService/StoreReplica": SurfaceInternal,
		"/wavespan.v1.GlobalReplication/PushGlobal":    SurfaceInternal,
		"/wavespan.v1.Admin/Something":                 SurfaceAdmin,
	}
	for proc, want := range cases {
		if got := SurfaceForProcedure(proc); got != want {
			t.Fatalf("%s -> surface %d, want %d", proc, got, want)
		}
	}
}

// fakeReq is a minimal connect.AnyRequest carrying a procedure.
type fakeReq struct {
	connect.AnyRequest
	proc string
}

func (f fakeReq) Spec() connect.Spec { return connect.Spec{Procedure: f.proc} }

func callUnary(t *testing.T, role Role, proc string) error {
	t.Helper()
	var called bool
	next := func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		return nil, nil
	}
	wrapped := Authorizer{}.WrapUnary(next)
	_, err := wrapped(WithRole(context.Background(), role), fakeReq{proc: proc})
	if err == nil && !called {
		t.Fatal("authorized call did not reach the handler")
	}
	return err
}

func TestUnauthenticatedInternalCallRejected(t *testing.T) {
	if err := callUnary(t, RoleNone, "/wavespan.v1.ReplicationService/StoreReplica"); err == nil {
		t.Fatal("unauthenticated internal call must be rejected")
	} else if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("want Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestReplicatorCannotPublicWrite(t *testing.T) {
	if err := callUnary(t, RoleReplicator, "/wavespan.v1.KvService/Put"); err == nil {
		t.Fatal("replicator must not be able to call public KV.Put")
	} else if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("want PermissionDenied, got %v", connect.CodeOf(err))
	}
	// but the same replicator CAN call the internal API
	if err := callUnary(t, RoleReplicator, "/wavespan.v1.ReplicationService/StoreReplica"); err != nil {
		t.Fatalf("replicator should be allowed on internal API: %v", err)
	}
}

func TestKeyHashRedaction(t *testing.T) {
	h1 := KeyHash("default", []byte("k1"))
	h2 := KeyHash("default", []byte("k1"))
	h3 := KeyHash("default", []byte("k2"))
	if h1 != h2 {
		t.Fatal("key hash must be stable")
	}
	if h1 == h3 {
		t.Fatal("different keys must hash differently")
	}
	if len(h1) == 0 || h1 == "default" || h1 == "k1" {
		t.Fatalf("hash must not reveal raw key: %q", h1)
	}
	// value redaction: only admin + includeValue reveals
	if RedactValue([]byte("secret"), RoleReader, true) != "<redacted>" {
		t.Fatal("non-admin must not see values")
	}
	if RedactValue([]byte("secret"), RoleAdmin, true) != "secret" {
		t.Fatal("admin with includeValue should see the value")
	}
}

func TestPerPeerRateLimit(t *testing.T) {
	l := NewPeerRateLimiter(10, 3) // 3 burst
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }
	allowed := 0
	for i := 0; i < 10; i++ {
		if l.Allow("peer-a") {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("burst should cap at 3, allowed %d", allowed)
	}
	// after 1s, ~10 more tokens accrue
	now = now.Add(time.Second)
	if !l.Allow("peer-a") {
		t.Fatal("tokens should refill over time")
	}
	// a different peer has its own bucket
	if !l.Allow("peer-b") {
		t.Fatal("per-peer buckets are independent")
	}
}

func TestAuditLogRetains(t *testing.T) {
	a := NewAuditLog(nil, 2)
	a.Record("peer-connect", "test-b", "tls ok")
	a.Record("apply-error", "test-b", "decode failed")
	a.Record("peer-connect", "test-c", "tls ok") // evicts the oldest
	ev := a.Events()
	if len(ev) != 2 || ev[len(ev)-1].Peer != "test-c" {
		t.Fatalf("audit ring wrong: %+v", ev)
	}
}
