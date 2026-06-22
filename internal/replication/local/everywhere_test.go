package local

import (
	"context"
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/latencygraph"
)

// TestFanoutEverywhereReplicatesToAllNodes: an "everywhere" namespace replicates to every alive
// node even though the nearby target is only 1.
func TestFanoutEverywhereReplicatesToAllNodes(t *testing.T) {
	n1, n2, n3 := newTestNode(t, "node1"), newTestNode(t, "node2"), newTestNode(t, "node3")
	repl := &routingReplicator{nodes: map[string]*testNode{"node1": n1, "node2": n2, "node3": n3}, down: map[string]bool{}}
	holders := NewHolderDirectory("node1")
	rec := putRecord(t, n1, "ref", []byte("k"), []byte("v"))
	holders.RecordHolder("ref", []byte("k"), "node1", versionOf(rec))

	cluster := staticCluster{aliveView("node1"), aliveView("node2"), aliveView("node3")}
	f := NewFanout(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, holders, targetPolicy(1), time.Second)
	f.SetEverywhere(func(ns string) bool { return ns == "ref" })

	f.Fill(context.Background(), FanoutJob{Namespace: "ref", Key: []byte("k"), Record: rec})

	if got := holders.Count("ref", []byte("k")); got != 3 {
		t.Fatalf("everywhere fanout should reach all 3 nodes, got %d", got)
	}
	for name, n := range map[string]*testNode{"node2": n2, "node3": n3} {
		if r, _ := n.store.Get("ref", []byte("k")); !r.Found {
			t.Fatalf("%s should hold the everywhere replica", name)
		}
	}
}

// TestRepairEverywhereTargetsAllAlive: repair for an everywhere namespace pushes to every alive
// member, not just nearby candidates.
func TestRepairEverywhereTargetsAllAlive(t *testing.T) {
	n1, n2, n3 := newTestNode(t, "node1"), newTestNode(t, "node2"), newTestNode(t, "node3")
	repl := &routingReplicator{nodes: map[string]*testNode{"node1": n1, "node2": n2, "node3": n3}, down: map[string]bool{}}
	holders := NewHolderDirectory("node1")
	rec := putRecord(t, n1, "ref", []byte("k"), []byte("v"))
	holders.RecordHolder("ref", []byte("k"), "node1", versionOf(rec))

	cluster := staticCluster{aliveView("node1"), aliveView("node2"), aliveView("node3")}
	repair := NewRepairEngine(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, holders, n1.store, targetPolicy(1), RepairConfig{})
	repair.SetEverywhere(func(ns string) bool { return ns == "ref" })

	repair.Enqueue(RepairItem{Namespace: "ref", Key: []byte("k"), Record: rec})
	repair.Drain(context.Background())

	if got := holders.Count("ref", []byte("k")); got != 3 {
		t.Fatalf("everywhere repair should reach all 3 nodes, got %d", got)
	}
}
