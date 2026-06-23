package local

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
)

func TestFanoutFillsToTarget(t *testing.T) {
	n1, n2, n3 := newTestNode(t, "node1"), newTestNode(t, "node2"), newTestNode(t, "node3")
	repl := &routingReplicator{nodes: map[string]*testNode{"node1": n1, "node2": n2, "node3": n3}, down: map[string]bool{}}
	holders := NewHolderDirectory("node1")

	rec := putRecord(t, n1, "default", []byte("k"), []byte("v"))
	holders.RecordHolder("default", []byte("k"), "node1", versionOf(rec)) // origin

	cluster := staticCluster{aliveView("node1"), aliveView("node2"), aliveView("node3")}
	// target nearby=2 -> 3 total holders (origin + 2)
	f := NewFanout(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, holders, targetPolicy(2), time.Second)

	f.Fill(context.Background(), FanoutJob{Namespace: "default", Key: []byte("k"), Record: rec})

	if got := holders.Count("default", []byte("k")); got != 3 {
		t.Fatalf("fanout should reach 3 holders, got %d: %v", got, holders.Holders("default", []byte("k")))
	}
	// the record is durable on the new holders
	if r, _ := n2.store.Get("default", []byte("k")); !r.Found {
		t.Fatal("node2 should hold the replica after fanout")
	}
}

func TestFanoutShortfallGoesToRepair(t *testing.T) {
	n1, n2 := newTestNode(t, "node1"), newTestNode(t, "node2")
	repl := &routingReplicator{nodes: map[string]*testNode{"node1": n1, "node2": n2}, down: map[string]bool{"node2": true}}
	holders := NewHolderDirectory("node1")
	rec := putRecord(t, n1, "default", []byte("k"), []byte("v"))
	holders.RecordHolder("default", []byte("k"), "node1", versionOf(rec))

	cluster := staticCluster{aliveView("node1"), aliveView("node2")}
	f := NewFanout(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, holders, targetPolicy(2), 100*time.Millisecond)
	repair := NewRepairEngine(testMember("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, holders, n1.store, targetPolicy(2), RepairConfig{})
	f.SetRepair(repair)

	// node2 is down so fanout cannot reach target -> the gap must be queued for repair
	f.Fill(context.Background(), FanoutJob{Namespace: "default", Key: []byte("k"), Record: rec})

	if repair.QueueDepth() == 0 {
		t.Fatal("fanout shortfall should enqueue a repair item")
	}
}
