package local_test

// End-to-end transport regression guard for intra-cluster anti-entropy's pull path.
//
// The data port is a PURE gRPC server (grpc-go only routes application/grpc). A Connect-wire client
// pointed at it fails at the transport layer, so the PeerFetch used by IntraAntiEntropy returned
// (nil,false) for every peer and anti-entropy was a silent no-op: divergent replicas never converged
// after a partition healed. This test stands up a REAL gRPC ReplicationService over a real TCP
// listener, seeds a record, and asserts the production PeerFetch fetches it over the wire. It lives
// in the external test package so it can import grpcsrv (which imports local) without a cycle.

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/yannick/wavespan/internal/grpcsrv"
	"github.com/yannick/wavespan/internal/replication/local"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	"github.com/yannick/wavespan/internal/recordstore"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// serveReplication seeds a recordstore with (ns,"reg") at hlc and serves it over a real gRPC
// ReplicationService on a fresh localhost port. It returns the listener address.
func serveReplication(t *testing.T, ns, key, val string, hlc uint64) string {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	store := recordstore.NewStore(mem, "dev", "peer", version.NewClock(func() uint64 { return 1000 }, 500), version.NewSequencer(0))
	v := version.Version{HLCPhysicalMs: hlc, WriterClusterID: "dev", WriterMemberID: "peer", WriterSequence: hlc}
	rec := store.BuildRecord(ns, []byte(key), []byte(val), v, false, nil)
	if _, err := store.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatalf("seed: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	// recv is nil: this server only serves FetchReplica reads. source is nil.
	wavespanv1.RegisterReplicationServiceServer(srv, grpcsrv.NewReplication(nil, store, "peer", lis.Addr().String(), nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestPeerFetchOverRealGRPC proves the production PeerFetch talks to a real gRPC ReplicationService
// end-to-end. With the old Connect-wire client this fails (transport error -> found=false); with the
// gRPC-backed PeerFetch it fetches the seeded record.
func TestPeerFetchOverRealGRPC(t *testing.T) {
	const hlc = 743014
	addr := serveReplication(t, "default", "reg", "winner", hlc)

	replicator := local.NewConnectReplicator()
	fetch := replicator.PeerFetch()

	rec, found := fetch(context.Background(), addr, "default", []byte("reg"))
	if !found || rec == nil {
		t.Fatalf("PeerFetch over real gRPC: found=%v rec=%v (transport mismatch would give found=false)", found, rec)
	}
	if got := string(rec.GetValue().GetInline()); got != "winner" {
		t.Fatalf("value: got %q want %q", got, "winner")
	}
	if got := version.FromProto(rec.GetVersion()).HLCPhysicalMs; got != hlc {
		t.Fatalf("version HLC: got %d want %d", got, hlc)
	}
}
