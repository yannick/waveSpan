package collections

import (
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

func newMgrOpts(t *testing.T, dir, addr string, store storage.LocalStore, opts Options) *Manager {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, err := NewManagerWithOptions(dir, addr, store, opts)
		if err == nil {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewManagerWithOptions never succeeded: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestCheapMTLSTransportReplicates brings up two nodes whose Raft traffic flows over the custom
// HTTP transport + node registry (plaintext here; mTLS configs are wired in production), forms a
// 2-voter shard, and confirms a write replicates to both replicas (design/30 §12, Appendix B.4-B.5).
func TestCheapMTLSTransportReplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	// Transport-only (default address-based registry): isolates the custom transport. The SWIM
	// registry (NodeHostID addressing) is exercised separately in registry_test.go.
	opts := Options{TransportFactory: &TransportFactory{}}
	addr1, addr2 := freeAddr(t), freeAddr(t)
	mem1, mem2 := storage.NewMemStore(), storage.NewMemStore()
	t.Cleanup(func() { _ = mem1.Close(); _ = mem2.Close() })

	mgr1 := newMgrOpts(t, t.TempDir(), addr1, mem1, opts)
	mgr2 := newMgrOpts(t, t.TempDir(), addr2, mem2, opts)
	defer mgr1.Stop()
	defer mgr2.Stop()

	members := map[uint64]string{1: addr1, 2: addr2}
	if err := mgr1.StartShard(1, 1, members, false); err != nil {
		t.Fatalf("node1 StartShard: %v", err)
	}
	if err := mgr2.StartShard(1, 2, members, false); err != nil {
		t.Fatalf("node2 StartShard: %v", err)
	}
	c1 := New(mgr1, SingleShardDirectory(1))
	c2 := New(mgr2, SingleShardDirectory(1))

	ns, coll := []byte("flags"), []byte("enabled")
	// A commit requires both replicas (quorum 2/2) — so success proves the custom transport carried
	// the Raft AppendEntries + ack between the two NodeHosts.
	if got := clusterSAdd(t, map[uint64]*Collections{1: c1, 2: c2}, ns, coll, []byte("x"), []byte("y")); got != 2 {
		t.Fatalf("SAdd over custom transport = %d want 2", got)
	}
	if !awaitMember(t, c1, ns, coll, []byte("x")) {
		t.Fatal("member not visible on node1")
	}
	if !awaitMember(t, c2, ns, coll, []byte("x")) {
		t.Fatal("member not replicated to node2 over the custom transport")
	}
}
