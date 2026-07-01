package membership

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
)

// serveGossipGRPC stands up svc's gossip service on a real 127.0.0.1 TCP port over a plain
// grpc.NewServer (mirroring production's dedicated gossip gRPC server) and returns its address.
func serveGossipGRPC(t *testing.T, svc *Service, ln net.Listener) {
	t.Helper()
	gs := grpc.NewServer()
	svc.RegisterGRPC(gs)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
}

// TestGRPCTransportThreeNodeConvergence runs three real membership services over the actual gRPC
// gossip transport (real TCP listeners) and asserts they converge to a full all-ALIVE roster —
// exercising proto serialization, the gRPC gossip handler, and indirect-probe relaying.
func TestGRPCTransportThreeNodeConvergence(t *testing.T) {
	ids := []string{"node1", "node2", "node3"}

	// Listen first so each node learns its address before wiring seeds.
	addrs := map[string]string{}
	lns := map[string]net.Listener{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		lns[id] = ln
		addrs[id] = ln.Addr().String()
	}

	svcs := map[string]*Service{}
	for _, id := range ids {
		self := Member{ClusterID: "dev", MemberID: id, NodeName: id, GossipAddr: addrs[id]}
		seeds := staticSeeds{addrs["node1"]}
		if id == "node1" {
			seeds = staticSeeds{addrs["node2"]}
		}
		svc := NewService(self, seeds, NewGRPCTransport(), DefaultServiceConfig())
		svcs[id] = svc
		serveGossipGRPC(t, svc, lns[id])
	}

	ctx := context.Background()
	for _, s := range svcs {
		s.gossip.Join(ctx)
	}
	for round := 0; round < 20; round++ {
		for _, id := range ids {
			svcs[id].gossip.Tick(ctx)
		}
	}

	for _, id := range ids {
		live := svcs[id].Live()
		if len(live) != 3 {
			t.Fatalf("%s sees %d live members over the real gRPC transport, want 3: %v", id, len(live), liveIDs(live))
		}
	}
	// latency edges recorded over the real wire
	if len(svcs["node1"].LatencyEdges()) == 0 {
		t.Fatal("no latency edges recorded over the gRPC transport")
	}
}

// TestGRPCTransportPingAndIndirectRelay asserts the gRPC transport round-trips a gossip exchange
// (the caller's member survives serialization into the target's roster reply) and that an indirect
// probe is actually relayed to the target (the reply carries the TARGET's identity, not the relay's).
func TestGRPCTransportPingAndIndirectRelay(t *testing.T) {
	lnT, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	lnR, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen relay: %v", err)
	}
	addrT, addrR := lnT.Addr().String(), lnR.Addr().String()

	svcT := NewService(Member{ClusterID: "dev", MemberID: "target", NodeName: "target", GossipAddr: addrT}, staticSeeds{}, NewGRPCTransport(), DefaultServiceConfig())
	serveGossipGRPC(t, svcT, lnT)
	svcR := NewService(Member{ClusterID: "dev", MemberID: "relay", NodeName: "relay", GossipAddr: addrR}, staticSeeds{}, NewGRPCTransport(), DefaultServiceConfig())
	serveGossipGRPC(t, svcR, lnR)

	ctx := context.Background()
	client := NewGRPCTransport()
	caller := Member{ClusterID: "dev", MemberID: "caller", NodeName: "caller", GossipAddr: "caller:7700"}
	msg := &GossipMessage{From: caller, Members: []MemberView{{Member: caller, State: StateAlive}}}

	// Direct exchange with the target: the target replies with its own identity, and the caller's
	// member must have survived the wire round-trip into the target's returned roster.
	reply, err := client.Ping(ctx, addrT, msg)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if reply.From.MemberID != "target" {
		t.Fatalf("direct reply From: got %q want %q", reply.From.MemberID, "target")
	}
	if !containsMember(reply.Members, "caller") {
		t.Fatalf("caller not present in target reply roster: %v", liveIDs(reply.Members))
	}

	// Indirect probe: ask the relay to exchange with the target on our behalf. A correct relay hands
	// back the TARGET's reply (From=="target"); a relay that answered itself would return "relay".
	reply2, err := client.IndirectPing(ctx, addrR, addrT, msg)
	if err != nil {
		t.Fatalf("IndirectPing: %v", err)
	}
	if reply2.From.MemberID != "target" {
		t.Fatalf("indirect reply From: got %q want %q (relay did not reach the target)", reply2.From.MemberID, "target")
	}
}

func containsMember(vs []MemberView, id string) bool {
	for _, v := range vs {
		if v.Member.MemberID == id {
			return true
		}
	}
	return false
}
