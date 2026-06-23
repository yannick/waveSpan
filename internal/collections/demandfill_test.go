package collections

import (
	"context"
	"errors"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"

	"github.com/yannick/wavespan/internal/storage"
)

// TestLearnerDemandFill exercises the demand-fill mechanism across two nodes: a node that does not host
// a shard joins it as a non-voting learner, streams its state, serves a bounded-stale read locally,
// and is then evicted (design/30 §9, M-C). The cross-node trigger orchestration (over RPC) is a later
// milestone; here the test drives the engine primitives directly.
func TestLearnerDemandFill(t *testing.T) {
	memA, memB := storage.NewMemStore(), storage.NewMemStore()
	t.Cleanup(func() { _ = memA.Close(); _ = memB.Close() })
	addrA, addrB := freeAddr(t), freeAddr(t)
	mgrA := newMgr(t, t.TempDir(), addrA, memA)
	defer mgrA.Stop()
	mgrB := newMgr(t, t.TempDir(), addrB, memB)
	defer mgrB.Stop()

	const shard = firstDataShard
	if err := mgrA.StartShard(shard, 1, map[uint64]string{1: addrA}, false); err != nil {
		t.Fatalf("A StartShard: %v", err)
	}
	cA := New(mgrA, SingleShardDirectory(shard))
	waitReady(t, cA)

	ns, coll := []byte("app"), []byte("cfg")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cA.SAdd(ctx, ns, coll, []byte("x"), []byte("y"), []byte("z")); err != nil {
		t.Fatalf("SAdd on A: %v", err)
	}

	// B does not host the shard yet: a read returns ErrShardNotFound (the demand-fill trigger).
	if _, err := mgrB.Read(ctx, shard, cardQuery{NS: ns, Coll: coll}, false); !errors.Is(err, dragonboat.ErrShardNotFound) {
		t.Fatalf("B read before fill = %v, want ErrShardNotFound", err)
	}

	// Demand-fill: A (the member/leader) admits B as a learner; B starts the local learner.
	actx, acancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer acancel()
	if err := mgrA.AddLearner(actx, shard, 2, addrB); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	if err := mgrB.StartLearner(shard, 2); err != nil {
		t.Fatalf("StartLearner: %v", err)
	}

	// B catches up and serves the bounded-stale read locally.
	cB := New(mgrB, SingleShardDirectory(shard))
	deadline := time.Now().Add(20 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 1*time.Second)
		n, err := cB.SCard(rctx, ns, coll, false) // StaleRead on the local learner
		rcancel()
		if err == nil && n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("learner never caught up (last SCard=%d err=%v)", n, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Eviction: A drops B from membership; B stops its local replica; B no longer hosts the shard.
	ectx, ecancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ecancel()
	if err := mgrA.RemoveLearner(ectx, shard, 2); err != nil {
		t.Fatalf("RemoveLearner: %v", err)
	}
	_ = mgrB.StopLocalReplica(shard, 2)
	evicted := time.Now().Add(15 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := mgrB.Read(rctx, shard, cardQuery{NS: ns, Coll: coll}, false)
		rcancel()
		if errors.Is(err, dragonboat.ErrShardNotFound) {
			break
		}
		if time.Now().After(evicted) {
			t.Fatalf("B still hosts the shard after eviction (err=%v)", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
