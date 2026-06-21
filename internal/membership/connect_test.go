package membership

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestConnectTransportThreeNodeConvergence runs three real membership services over the actual
// Connect HTTP transport (httptest servers) and asserts they converge to a full all-ALIVE
// roster — exercising proto serialization, the gossip handler, and indirect-probe relaying.
func TestConnectTransportThreeNodeConvergence(t *testing.T) {
	ids := []string{"node1", "node2", "node3"}
	muxes := map[string]*http.ServeMux{}
	servers := map[string]*httptest.Server{}
	addrs := map[string]string{}

	// stand up an httptest server per node first to learn its address
	for _, id := range ids {
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		muxes[id] = mux
		servers[id] = srv
		addrs[id] = strings.TrimPrefix(srv.URL, "http://")
	}

	svcs := map[string]*Service{}
	for _, id := range ids {
		self := Member{ClusterID: "dev", MemberID: id, NodeName: id, GossipAddr: addrs[id]}
		seeds := staticSeeds{addrs["node1"]}
		if id == "node1" {
			seeds = staticSeeds{addrs["node2"]}
		}
		transport := NewConnectTransport(http.DefaultClient)
		svc := NewService(self, seeds, transport, DefaultServiceConfig())
		svcs[id] = svc
		path, h := svc.GossipHandler()
		muxes[id].Handle(path, h)
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
			t.Fatalf("%s sees %d live members over the real transport, want 3: %v", id, len(live), liveIDs(live))
		}
	}
	// latency edges recorded over the real wire
	if len(svcs["node1"].LatencyEdges()) == 0 {
		t.Fatal("no latency edges recorded over the Connect transport")
	}
}
