//go:build chaos

package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

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

// TestCheapMTLSRestartedVoterSnapshot covers the precise gap behind the TTL chaos stall: a voter that
// is down while the cluster advances PAST log compaction must, on restart, catch up via a streamed
// snapshot installed over its EXISTING (stale) state — exercising RecoverFromSnapshot's clear+install
// over the custom transport, not the fresh-learner path.
func TestCheapMTLSRestartedVoterSnapshot(t *testing.T) {
	opts := Options{TransportFactory: &TransportFactory{}}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	members := map[uint64]string{1: addrs[0], 2: addrs[1], 3: addrs[2]}
	stores := []storage.LocalStore{storage.NewMemStore(), storage.NewMemStore(), storage.NewMemStore()}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	t.Cleanup(func() {
		for _, s := range stores {
			_ = s.Close()
		}
	})
	mgrs := make([]*Manager, 3)
	startNode := func(i int) {
		mgrs[i] = newMgrOpts(t, dirs[i], addrs[i], stores[i], opts)
		if err := mgrs[i].StartShard(firstDataShard, uint64(i+1), members, false); err != nil {
			t.Fatalf("StartShard %d: %v", i+1, err)
		}
	}
	for i := 0; i < 3; i++ {
		startNode(i)
	}
	defer func() {
		for _, m := range mgrs {
			if m != nil {
				m.Stop()
			}
		}
	}()
	cols := []*Collections{New(mgrs[0], SingleShardDirectory(firstDataShard)), New(mgrs[1], SingleShardDirectory(firstDataShard)), New(mgrs[2], SingleShardDirectory(firstDataShard))}
	ns, coll := []byte("rv"), []byte("set")

	add := func(c *Collections, m string) {
		deadline := time.Now().Add(8 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			_, err := c.SAdd(ctx, ns, coll, []byte(m))
			cancel()
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("SAdd %s: %v", m, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for i := 0; i < 1500; i++ {
		add(cols[0], itoa(i))
	}
	// Take node 3 down, advance the cluster (1+2 quorum) far past compaction, then restart 3.
	mgrs[2].Stop()
	mgrs[2] = nil
	for i := 1500; i < 4000; i++ {
		add(cols[0], itoa(i))
	}
	startNode(2)
	cols[2] = New(mgrs[2], SingleShardDirectory(firstDataShard))

	// Node 3 must catch up to all 4000 via a snapshot over the custom transport.
	deadline := time.Now().Add(40 * time.Second)
	var lastN uint64
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 1*time.Second)
		n, err := cols[2].SCard(rctx, ns, coll, false)
		rcancel()
		if err == nil {
			lastN = n
			if n == 4000 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted voter never caught up via snapshot: SCard=%d want 4000", lastN)
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
