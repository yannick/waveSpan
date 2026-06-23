package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// directAdmitter is the in-process LearnerAdmitter seam for the test: it admits the learner by calling
// a member node's Manager directly. In production this is an RPC to a peer.
type directAdmitter struct{ member *Manager }

func (a directAdmitter) AdmitLearner(ctx context.Context, shardID, replicaID uint64, target string) error {
	return a.member.AddLearner(ctx, shardID, replicaID, target)
}

// TestAutoDemandFill exercises the full auto demand-fill loop end to end: a spot node that has never
// hosted a shard issues an ordinary read, the read path detects not-hosted, joins the shard as a
// learner (via the admitter), and then serves the data locally — a dynamically-filling cache with no
// explicit provisioning (design/30 §9).
func TestAutoDemandFill(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
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
		t.Fatalf("seed SAdd: %v", err)
	}

	// B is a spot node with demand-fill: reads auto-join the shard as a learner (replicaID 2 @ addrB),
	// admitted by A.
	filler := NewDemandFiller(mgrB, 2, addrB, directAdmitter{member: mgrA})
	cB := New(mgrB, SingleShardDirectory(shard)).WithDemandFill(filler)

	// A read on B that would otherwise fail (not hosted) triggers the fill and then serves locally.
	deadline := time.Now().Add(30 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		n, err := cB.SCard(rctx, ns, coll, false) // first call triggers demand-fill, then retries local
		rcancel()
		if err == nil && n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("demand-fill never served the data (last SCard=%d err=%v)", n, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !awaitMember(t, cB, ns, coll, []byte("x")) {
		t.Fatal("member not served locally after demand-fill")
	}
}
