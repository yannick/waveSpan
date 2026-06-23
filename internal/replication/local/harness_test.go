package local

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/placement"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// testNode is an in-process node: a record store + StoreReplica receiver.
type testNode struct {
	id    string
	store *recordstore.Store
	recv  *Receiver
}

func newTestNode(t *testing.T, id string) *testNode {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	st := recordstore.NewStore(mem, "dev", id, version.NewClock(nil, 500), version.NewSequencer(0))
	return &testNode{id: id, store: st, recv: NewReceiver(st, id, NewIdempotency(0))}
}

// routingReplicator routes StoreReplica to in-process nodes; down members fail.
type routingReplicator struct {
	nodes map[string]*testNode
	down  map[string]bool
}

func (r *routingReplicator) StoreReplica(_ context.Context, target membership.Member, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
	if r.down[target.MemberID] {
		return nil, context.DeadlineExceeded
	}
	n, ok := r.nodes[target.MemberID]
	if !ok {
		return nil, context.Canceled
	}
	return n.recv.Apply(req)
}

func testMember(id string) membership.Member {
	return membership.Member{ClusterID: "dev", MemberID: id, NodeName: "node-" + id, Geo: "g1", DataAddr: id + ":7800"}
}

type staticCluster []membership.MemberView

func (c staticCluster) Members() []membership.MemberView { return []membership.MemberView(c) }

func aliveView(id string) membership.MemberView {
	return membership.MemberView{Member: testMember(id), State: membership.StateAlive}
}

// targetPolicy yields a policy whose targetHolders = nearby+1.
func targetPolicy(nearby int) placement.Policy {
	return placement.Policy{TargetNearbyReplicas: nearby, MinAckNearbyReplicas: 1, RequireDistinctNodes: true, Geo: placement.LatencyOnly}
}

// versionOf extracts the version from a stored record.
func versionOf(rec *wavespanv1.StoredRecord) version.Version {
	return version.FromProto(rec.GetVersion())
}

// putRecord stores a record on a node and returns it.
func putRecord(t *testing.T, n *testNode, ns string, key, val []byte) *wavespanv1.StoredRecord {
	t.Helper()
	v := n.store.NextVersion()
	rec := n.store.BuildRecord(ns, key, val, v, false, nil)
	if _, err := n.store.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	return rec
}

// putRecordOn applies an existing record to another node's store (simulating a replica).
func putRecordOn(t *testing.T, n *testNode, rec *wavespanv1.StoredRecord) {
	t.Helper()
	if _, err := n.store.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}
