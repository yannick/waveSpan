package collections

import (
	"sync"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
)

// TestSwimRegistryResolves runs two nodes in NodeHostID addressing mode (custom registry): membership
// carries NodeHostIDs, and a SWIM-style resolver maps each NodeHostID to its current address. A write
// replicates only if the registry resolved both peers and the custom transport reached them
// (design/30 §12, Appendix B.5).
func TestSwimRegistryResolves(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}

	// The SWIM seam: a shared NodeHostID -> address map the resolver consults. In production this is
	// the cluster's gossip directory; here it is populated explicitly after the nodes are created.
	var mu sync.Mutex
	swim := map[string]string{}
	resolver := func(id string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		a, ok := swim[id]
		return a, ok
	}
	opts := Options{TransportFactory: &TransportFactory{}, RegistryFactory: &NodeRegistryFactory{Resolver: resolver}}

	addr1, addr2 := freeAddr(t), freeAddr(t)
	mem1, mem2 := storage.NewMemStore(), storage.NewMemStore()
	t.Cleanup(func() { _ = mem1.Close(); _ = mem2.Close() })
	mgr1 := newMgrOpts(t, t.TempDir(), addr1, mem1, opts)
	mgr2 := newMgrOpts(t, t.TempDir(), addr2, mem2, opts)
	defer mgr1.Stop()
	defer mgr2.Stop()

	// Publish each node's NodeHostID -> its raft address into the SWIM directory.
	mu.Lock()
	swim[mgr1.NodeHostID()] = addr1
	swim[mgr2.NodeHostID()] = addr2
	mu.Unlock()

	// Membership is by NodeHostID (the addressing mode the custom registry selects).
	members := map[uint64]string{1: mgr1.NodeHostID(), 2: mgr2.NodeHostID()}
	if err := mgr1.StartShard(1, 1, members, false); err != nil {
		t.Fatalf("node1 StartShard: %v", err)
	}
	if err := mgr2.StartShard(1, 2, members, false); err != nil {
		t.Fatalf("node2 StartShard: %v", err)
	}
	c1 := New(mgr1, SingleShardDirectory(1))
	c2 := New(mgr2, SingleShardDirectory(1))

	ns, coll := []byte("flags"), []byte("enabled")
	if got := clusterSAdd(t, map[uint64]*Collections{1: c1, 2: c2}, ns, coll, []byte("x")); got != 1 {
		t.Fatalf("SAdd via NodeHostID registry = %d want 1", got)
	}
	if !awaitMember(t, c2, ns, coll, []byte("x")) {
		t.Fatal("write not replicated — registry/transport resolution failed")
	}
}
