package collections

import (
	"context"
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

// TestCheapMTLSSnapshotCatchup forces a snapshot catch-up over the custom transport: node A commits
// far more than SnapshotEntries, so the log is compacted; a learner B added afterward can only catch
// up via a streamed snapshot (ChunkHandler path), not log replay. If the transport's snapshot path is
// broken, B never serves the data.
func TestCheapMTLSSnapshotCatchup(t *testing.T) {
	opts := Options{TransportFactory: &TransportFactory{}}
	addrA, addrB := freeAddr(t), freeAddr(t)
	memA, memB := storage.NewMemStore(), storage.NewMemStore()
	t.Cleanup(func() { _ = memA.Close(); _ = memB.Close() })
	mgrA := newMgrOpts(t, t.TempDir(), addrA, memA, opts)
	defer mgrA.Stop()
	mgrB := newMgrOpts(t, t.TempDir(), addrB, memB, opts)
	defer mgrB.Stop()

	const shard = firstDataShard
	if err := mgrA.StartShard(shard, 1, map[uint64]string{1: addrA}, false); err != nil {
		t.Fatalf("A StartShard: %v", err)
	}
	cA := New(mgrA, SingleShardDirectory(shard))
	waitReady(t, cA)
	ns, coll := []byte("snap"), []byte("set")

	// Commit > SnapshotEntries (1000) + CompactionOverhead (500) so the early log is trimmed and a late
	// learner must snapshot.
	for i := 0; i < 2000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := cA.SAdd(ctx, ns, coll, []byte(itoa(i)))
		cancel()
		if err != nil {
			t.Fatalf("SAdd %d: %v", i, err)
		}
	}

	// Add B as a learner and start it; it must catch up via a snapshot over the custom transport.
	actx, acancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := mgrA.AddLearner(actx, shard, 2, addrB); err != nil {
		acancel()
		t.Fatalf("AddLearner: %v", err)
	}
	acancel()
	if err := mgrB.StartLearner(shard, 2); err != nil {
		t.Fatalf("StartLearner: %v", err)
	}

	cB := New(mgrB, SingleShardDirectory(shard))
	deadline := time.Now().Add(30 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 1*time.Second)
		n, err := cB.SCard(rctx, ns, coll, false)
		rcancel()
		if err == nil && n == 2000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("learner never caught up via snapshot over the custom transport (last SCard=%d err=%v)", func() uint64 {
				rctx, rcancel := context.WithTimeout(context.Background(), 1*time.Second)
				n, _ := cB.SCard(rctx, ns, coll, false)
				rcancel()
				return n
			}(), nil)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
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
