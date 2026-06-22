package kv

import (
	"context"
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/placement"
	"github.com/cwire/wavespan/internal/replication/local"
)

// newCypherKVFixture builds a single-node CypherKV over one store. MinAckNearbyReplicas:0 lets the
// origin-only write ack on a one-node cluster (otherwise Put returns ErrInsufficientNearbyReplicas).
func newCypherKVFixture(t *testing.T) *CypherKV {
	t.Helper()
	n1 := newNode(t, "node1")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1")}
	policy := placement.Policy{TargetNearbyReplicas: 1, MinAckNearbyReplicas: 0, RequireDistinctNodes: true, Geo: placement.LatencyOnly}
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, policy, local.NewIdempotency(0), nil, nil, time.Second)
	reader := NewReader(n1.store, member("node1"))
	return NewCypherKV(reader, coord)
}

func TestCypherKVRoundTrip(t *testing.T) {
	kvAdapter := newCypherKVFixture(t)

	ctx := context.Background()
	ver, err := kvAdapter.Put(ctx, "profile", []byte("u1"), []byte("hello"), nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if ver == "" {
		t.Fatal("expected non-empty version")
	}
	got, found, err := kvAdapter.Get(ctx, "profile", []byte("u1"))
	if err != nil || !found || string(got) != "hello" {
		t.Fatalf("get: got=%q found=%v err=%v", got, found, err)
	}
	if _, found, _ := kvAdapter.Get(ctx, "profile", []byte("absent")); found {
		t.Fatal("absent key should not be found")
	}
	if _, err := kvAdapter.Delete(ctx, "profile", []byte("u1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, _ := kvAdapter.Get(ctx, "profile", []byte("u1")); found {
		t.Fatal("deleted key should not be found")
	}
}

// TestCypherKVEmptyValueIsPresent pins the present-but-empty invariant: a stored empty value must
// read back as found=true with value "", never be mistaken for an absent key (which is found=false).
func TestCypherKVEmptyValueIsPresent(t *testing.T) {
	kvAdapter := newCypherKVFixture(t)
	ctx := context.Background()

	if _, err := kvAdapter.Put(ctx, "profile", []byte("empty"), []byte(""), nil); err != nil {
		t.Fatalf("put empty: %v", err)
	}
	got, found, err := kvAdapter.Get(ctx, "profile", []byte("empty"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatal("present empty value must report found=true, not absent")
	}
	if string(got) != "" {
		t.Fatalf("expected empty value, got %q", got)
	}
}
