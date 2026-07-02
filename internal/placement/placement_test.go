package placement

import (
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
)

func mem(id, node, zone, region, geo string) membership.Member {
	return membership.Member{MemberID: id, NodeName: node, Zone: zone, Region: region, Geo: geo}
}

func alive(m membership.Member) membership.MemberView {
	return membership.MemberView{Member: m, State: membership.StateAlive}
}

func selfMember() membership.Member { return mem("self", "node-self", "z1", "r1", "g1") }

func emptyGraph() *latencygraph.Graph { return latencygraph.New(latencygraph.DefaultConfig()) }

func TestDistinctNodeFilter(t *testing.T) {
	self := selfMember()
	members := []membership.MemberView{
		alive(mem("p-sameNode", "node-self", "z1", "r1", "g1")), // same physical node -> excluded
		alive(mem("p-other", "node-2", "z1", "r1", "g1")),
	}
	cands, err := Select(self, members, emptyGraph(), Policy{MinAckNearbyReplicas: 1, RequireDistinctNodes: true, Geo: LatencyOnly})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cands {
		if c.Member.MemberID == "p-sameNode" {
			t.Fatal("distinct-node filter must exclude a same-node peer")
		}
	}
	if len(cands) != 1 {
		t.Fatalf("want 1 candidate, got %d", len(cands))
	}
}

func TestNoCandidatesFails(t *testing.T) {
	self := selfMember()
	_, err := Select(self, nil, emptyGraph(), Policy{MinAckNearbyReplicas: 1, Geo: LatencyOnly})
	if err != ErrNoCandidates {
		t.Fatalf("want ErrNoCandidates, got %v", err)
	}
}

func TestRequireLocalGeoFailsWithoutLocalPeer(t *testing.T) {
	self := selfMember() // geo g1
	members := []membership.MemberView{
		alive(mem("p-eu", "node-2", "z9", "r9", "g2")), // different geo
	}
	_, err := Select(self, members, emptyGraph(), Policy{MinAckNearbyReplicas: 1, Geo: RequireLocalGeo})
	if err != ErrInsufficientLocalReplicas {
		t.Fatalf("require-local-geo with no local peer should fail, got %v", err)
	}
	// with a local peer it succeeds and never returns the foreign-geo member
	members = append(members, alive(mem("p-local", "node-3", "z1", "r1", "g1")))
	cands, err := Select(self, members, emptyGraph(), Policy{MinAckNearbyReplicas: 1, Geo: RequireLocalGeo})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cands {
		if c.Member.Geo != "g1" {
			t.Fatalf("require-local-geo must not return foreign-geo member %s", c.Member.MemberID)
		}
	}
}

func TestPreferLocalGeoSpillsOnlyWhenNeededAndAllowed(t *testing.T) {
	self := selfMember()
	members := []membership.MemberView{
		alive(mem("p-far", "node-2", "z9", "r9", "g2")), // other geo
	}
	// no local peer, spillover disallowed -> no candidates
	if _, err := Select(self, members, emptyGraph(), Policy{MinAckNearbyReplicas: 1, Geo: PreferLocalGeo, AllowSpilloverForDurability: false}); err != ErrNoCandidates {
		t.Fatalf("prefer-local-geo without spillover and no local peer should fail, got %v", err)
	}
	// spillover allowed -> returns the far peer tagged spillover
	cands, err := Select(self, members, emptyGraph(), Policy{MinAckNearbyReplicas: 1, Geo: PreferLocalGeo, AllowSpilloverForDurability: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 1 || !cands[0].GeoSpillover {
		t.Fatalf("spillover candidate should be tagged: %+v", cands)
	}

	// with a local peer, the local one is preferred (first) and not tagged spillover
	members = append(members, alive(mem("p-local", "node-3", "z1", "r1", "g1")))
	cands, err = Select(self, members, emptyGraph(), Policy{MinAckNearbyReplicas: 1, Geo: PreferLocalGeo, AllowSpilloverForDurability: true})
	if err != nil {
		t.Fatal(err)
	}
	if cands[0].Member.MemberID != "p-local" || cands[0].GeoSpillover {
		t.Fatalf("local-geo peer should be preferred and not spillover: %+v", cands[0])
	}
}

func TestScoringOrdersByMeasuredLatency(t *testing.T) {
	self := selfMember()
	g := emptyGraph()
	now := time.Unix(1000, 0)
	g.AddSample("p-fast", 2*time.Millisecond, true, now)
	g.AddSample("p-slow", 80*time.Millisecond, true, now)
	members := []membership.MemberView{
		alive(mem("p-slow", "node-2", "z1", "r1", "g1")),
		alive(mem("p-fast", "node-3", "z1", "r1", "g1")),
	}
	cands, err := Select(self, members, g, Policy{MinAckNearbyReplicas: 1, Geo: LatencyOnly})
	if err != nil {
		t.Fatal(err)
	}
	if cands[0].Member.MemberID != "p-fast" {
		t.Fatalf("lower-latency peer should rank first, got %s", cands[0].Member.MemberID)
	}
}

func TestSelectAllocatesAtMostOnce(t *testing.T) {
	self := selfMember()
	members := []membership.MemberView{
		alive(mem("p-1", "node-1", "z1", "r1", "g1")),
		alive(mem("p-2", "node-2", "z2", "r1", "g1")),
		alive(mem("p-3", "node-3", "z1", "r1", "g1")),
		alive(mem("p-4", "node-4", "z9", "r9", "g2")),
		alive(mem("p-5", "node-5", "z9", "r9", "g2")),
	}
	g := emptyGraph()
	policy := Policy{
		TargetNearbyReplicas: 3, MinAckNearbyReplicas: 1, RequireDistinctNodes: true,
		Geo: PreferLocalGeo, AllowSpilloverForDurability: true,
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := Select(self, members, g, policy); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("Select allocates %.1f objects per call, want <= 1 (the candidate buffer); it runs on every write", allocs)
	}
}
