package kv

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/placement"
	"github.com/yannick/wavespan/internal/recordstore"
	local "github.com/yannick/wavespan/internal/replication/local"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// node is an in-process node: a local record store + a StoreReplica receiver.
type node struct {
	id    string
	store *recordstore.Store
	recv  *local.Receiver
}

func newNode(t *testing.T, id string) *node {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	st := recordstore.NewStore(mem, "dev", id, version.NewClock(nil, 500), version.NewSequencer(0))
	return &node{id: id, store: st, recv: local.NewReceiver(st, id, local.NewIdempotency(0))}
}

// fakeReplicator routes StoreReplica to in-process receivers, and can drop a node (down).
type fakeReplicator struct {
	nodes map[string]*node
	down  map[string]bool
}

func (f *fakeReplicator) StoreReplica(_ context.Context, target membership.Member, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
	if f.down[target.MemberID] {
		return nil, context.DeadlineExceeded
	}
	n, ok := f.nodes[target.MemberID]
	if !ok {
		return nil, context.Canceled
	}
	return n.recv.Apply(req)
}

func member(id string) membership.Member {
	return membership.Member{ClusterID: "dev", MemberID: id, NodeName: "node-" + id, Geo: "g1", DataAddr: id + ":7800"}
}

type staticCluster []membership.MemberView

func (c staticCluster) Members() []membership.MemberView { return []membership.MemberView(c) }

func aliveView(id string) membership.MemberView {
	return membership.MemberView{Member: member(id), State: membership.StateAlive}
}

func defaultPolicy() placement.Policy {
	return placement.Policy{TargetNearbyReplicas: 3, MinAckNearbyReplicas: 1, RequireDistinctNodes: true, Geo: placement.LatencyOnly}
}

func TestOriginPlusOneSucceedsWithOneReplica(t *testing.T) {
	n1, n2 := newNode(t, "node1"), newNode(t, "node2")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1, "node2": n2}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1"), aliveView("node2")}
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, defaultPolicy(), local.NewIdempotency(0), nil, nil, time.Second)

	out, err := coord.Put(context.Background(), "default", []byte("foo"), []byte("bar"), nil, "")
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if out.AckedNearbyReplicas != 1 {
		t.Fatalf("ackedNearbyReplicas = %d, want 1", out.AckedNearbyReplicas)
	}
	// the value is durable on the replica (node2)
	got, _ := n2.store.Get("default", []byte("foo"))
	if !got.Found || !bytes.Equal(got.Value, []byte("bar")) {
		t.Fatalf("replica missing value: %+v", got)
	}
}

func TestOriginPlusOneFailsWithNoCandidates(t *testing.T) {
	n1 := newNode(t, "node1")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1")} // only self
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, defaultPolicy(), local.NewIdempotency(0), nil, nil, time.Second)

	_, err := coord.Put(context.Background(), "default", []byte("k"), []byte("v"), nil, "")
	if err != ErrInsufficientNearbyReplicas {
		t.Fatalf("want ErrInsufficientNearbyReplicas with no candidates, got %v", err)
	}
	// but the origin copy is still durable locally
	if got, _ := n1.store.Get("default", []byte("k")); !got.Found {
		t.Fatal("origin copy should be durable even when ack fails")
	}
}

func TestOriginKillValueSurvivesOnReplica(t *testing.T) {
	n1, n2 := newNode(t, "node1"), newNode(t, "node2")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1, "node2": n2}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1"), aliveView("node2")}
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, defaultPolicy(), local.NewIdempotency(0), nil, nil, time.Second)

	if _, err := coord.Put(context.Background(), "default", []byte("foo"), []byte("bar"), nil, ""); err != nil {
		t.Fatal(err)
	}
	// "kill" the origin: drop node1's store; the value must remain readable on node2
	n1.store = nil
	got, err := n2.store.Get("default", []byte("foo"))
	if err != nil || !got.Found || !bytes.Equal(got.Value, []byte("bar")) {
		t.Fatalf("value did not survive origin kill on replica: %+v %v", got, err)
	}
}

func TestIdempotentRetryCollapsesToOneMutation(t *testing.T) {
	n1, n2 := newNode(t, "node1"), newNode(t, "node2")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1, "node2": n2}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1"), aliveView("node2")}
	idem := local.NewIdempotency(0)
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, defaultPolicy(), idem, nil, nil, time.Second)

	out1, err := coord.Put(context.Background(), "default", []byte("k"), []byte("v1"), nil, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	out2, err := coord.Put(context.Background(), "default", []byte("k"), []byte("v2"), nil, "req-1") // retry, same id
	if err != nil {
		t.Fatal(err)
	}
	if !out1.Version.Equal(out2.Version) {
		t.Fatalf("retried idempotent put got a new version: %+v vs %+v", out1.Version, out2.Version)
	}
	// the retry must NOT have overwritten with v2
	got, _ := n1.store.Get("default", []byte("k"))
	if !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("idempotent retry changed the value: %q", got.Value)
	}
}
