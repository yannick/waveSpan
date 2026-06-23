package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestSpotNodeServes exercises the full spot/edge-node path: a node that holds no shards joins the
// meta shard as a learner to obtain the range directory, then serves a collection by demand-filling
// its data shard on the first read — a dynamically-filling cache with zero provisioning (design/30
// §9). The admit step is in-process here (directAdmitter); in the node it is the AdmitLearner RPC.
func TestSpotNodeServes(t *testing.T) {
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

	// Voter A bootstraps the control plane (meta + initial data shard) and seeds a collection.
	bctx, bcancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer bcancel()
	ctrlA, err := Bootstrap(bctx, mgrA, 1, map[uint64]string{1: addrA}, map[uint64]string{1: addrA})
	if err != nil {
		t.Fatalf("voter Bootstrap: %v", err)
	}
	cA := ctrlA.Collections()
	waitReady(t, cA)
	ns, coll := []byte("app"), []byte("cfg")
	if _, err := cA.SAdd(bctx, ns, coll, []byte("x"), []byte("y"), []byte("z")); err != nil {
		t.Fatalf("seed SAdd: %v", err)
	}

	// Spot node B joins: demand-fill the meta shard for the directory, then serve via data demand-fill.
	admitter := directAdmitter{member: mgrA}
	jctx, jcancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer jcancel()
	ctrlB, err := JoinAsSpot(jctx, mgrB, SpotReplicaID("node-B"), addrB, admitter)
	if err != nil {
		t.Fatalf("JoinAsSpot: %v", err)
	}
	cB := ctrlB.Collections()

	// B routes the collection (directory obtained from the meta shard) and demand-fills its data shard.
	deadline := time.Now().Add(30 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		n, err := cB.SCard(rctx, ns, coll, false)
		rcancel()
		if err == nil && n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("spot node never served the collection (last SCard=%d err=%v)", n, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !awaitMember(t, cB, ns, coll, []byte("x")) {
		t.Fatal("spot node did not serve the member locally")
	}
}
