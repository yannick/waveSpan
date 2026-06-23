package local

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/version"
)

// TestRepairRestoresReplicaAfterHolderDeath builds an under-replicated key (target 2 holders),
// kills a holder, and asserts the engine creates a replacement on a surviving node and converges.
func TestRepairRestoresReplicaAfterHolderDeath(t *testing.T) {
	n1, n2, n3 := newTestNode(t, "node1"), newTestNode(t, "node2"), newTestNode(t, "node3")
	repl := &routingReplicator{nodes: map[string]*testNode{"node1": n1, "node2": n2, "node3": n3}, down: map[string]bool{}}
	holders := NewHolderDirectory("node1")

	rec := putRecord(t, n1, "default", []byte("k"), []byte("v"))
	// target nearby=1 -> 2 holders: node1 (origin) + node2 (replica)
	holders.RecordHolder("default", []byte("k"), "node1", versionOf(rec))
	holders.RecordHolder("default", []byte("k"), "node2", versionOf(rec))
	// node2 actually holds it
	putRecordOn(t, n2, rec)

	cluster := staticCluster{aliveView("node1"), aliveView("node2"), aliveView("node3")}
	dead := map[string]bool{}
	cfg := RepairConfig{IsAlive: func(id string) bool { return !dead[id] }}
	engine := NewRepairEngine(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, holders, n1.store, targetPolicy(1), cfg)

	// kill node2 (the replica). It is now dead and unreachable.
	dead["node2"] = true
	repl.down["node2"] = true
	engine.OnMemberDead("node2")

	if engine.QueueDepth() == 0 {
		t.Fatal("dead holder should enqueue the key for repair")
	}
	engine.Drain(context.Background())

	// converged: 2 alive holders again (node1 + node3), node2 gone
	alive := 0
	for _, h := range holders.Holders("default", []byte("k")) {
		if !dead[h] {
			alive++
		}
		if h == "node2" {
			t.Fatal("dead node2 should have been removed from holders")
		}
	}
	if alive < 2 {
		t.Fatalf("repair did not converge to target: holders=%v", holders.Holders("default", []byte("k")))
	}
	// the replacement (node3) durably holds the record
	if r, _ := n3.store.Get("default", []byte("k")); !r.Found {
		t.Fatal("replacement holder node3 should hold the record after repair")
	}
}

func TestRepairQueueDrainsMostUnderReplicatedFirst(t *testing.T) {
	holders := NewHolderDirectory("node1")
	// 5 alive members so the capped target (nearby 3 -> 4 holders) is meaningful
	cluster := staticCluster{aliveView("node1"), aliveView("node2"), aliveView("node3"), aliveView("node4"), aliveView("node5")}
	engine := NewRepairEngine(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), &routingReplicator{nodes: map[string]*testNode{}, down: map[string]bool{}}, holders, nil, targetPolicy(3), RepairConfig{})

	// key A has 1 holder (deficit 3), key B has 3 holders (deficit 1) against target 4
	v := version.Version{HLCPhysicalMs: 1}
	holders.RecordHolder("ns", []byte("A"), "node1", v)
	holders.RecordHolder("ns", []byte("B"), "node1", v)
	holders.RecordHolder("ns", []byte("B"), "node2", v)
	holders.RecordHolder("ns", []byte("B"), "node3", v)
	engine.Enqueue(RepairItem{Namespace: "ns", Key: []byte("B")})
	engine.Enqueue(RepairItem{Namespace: "ns", Key: []byte("A")})

	first, _ := engine.pop()
	if string(first.Key) != "A" {
		t.Fatalf("most under-replicated key (A) should drain first, got %q", first.Key)
	}
}
