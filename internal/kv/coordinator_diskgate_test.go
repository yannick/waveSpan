package kv

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	local "github.com/yannick/wavespan/internal/replication/local"
)

type kvFakeGate struct{ pressure atomic.Bool }

func (g *kvFakeGate) UnderPressure() bool { return g.pressure.Load() }

func TestCoordinatorShedsWritesUnderDiskPressure(t *testing.T) {
	n1, n2 := newNode(t, "node1"), newNode(t, "node2")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1, "node2": n2}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1"), aliveView("node2")}
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, defaultPolicy(), local.NewIdempotency(0), nil, nil, time.Second)

	gate := &kvFakeGate{}
	var shed int64
	coord.WithDiskGate(gate, func() { atomic.AddInt64(&shed, 1) })

	// Clear: the write serves and lands on the store.
	if _, err := coord.Put(context.Background(), "default", []byte("k1"), []byte("v1"), nil, ""); err != nil {
		t.Fatalf("clear gate: put should serve, got %v", err)
	}
	if atomic.LoadInt64(&shed) != 0 {
		t.Fatalf("clear gate: shed counter should be 0, got %d", atomic.LoadInt64(&shed))
	}

	// Under pressure: a NEW write is shed with ErrDiskPressure before touching the store.
	gate.pressure.Store(true)
	_, err := coord.Put(context.Background(), "default", []byte("k2"), []byte("v2"), nil, "")
	if !errors.Is(err, ErrDiskPressure) {
		t.Fatalf("pressure: want ErrDiskPressure, got %v", err)
	}
	if atomic.LoadInt64(&shed) != 1 {
		t.Fatalf("pressure: shed counter should be 1, got %d", atomic.LoadInt64(&shed))
	}
	// The shed write must NOT have been persisted.
	if got, _ := n1.store.Get("default", []byte("k2")); got.Found {
		t.Fatal("shed write must not be persisted to the store")
	}

	// A delete (tombstone) is also a write and is shed under pressure.
	if _, derr := coord.Delete(context.Background(), "default", []byte("k1"), ""); !errors.Is(derr, ErrDiskPressure) {
		t.Fatalf("pressure: delete should shed with ErrDiskPressure, got %v", derr)
	}

	// Recover: writes flow again.
	gate.pressure.Store(false)
	if _, err := coord.Put(context.Background(), "default", []byte("k3"), []byte("v3"), nil, ""); err != nil {
		t.Fatalf("recovered: put should serve, got %v", err)
	}
}

func TestCoordinatorIdempotentReplayServesUnderPressure(t *testing.T) {
	// An idempotent replay of an already-applied write adds no new bytes, so it must succeed even under
	// pressure (the gate is checked AFTER the idempotency short-circuit).
	n1, n2 := newNode(t, "node1"), newNode(t, "node2")
	repl := &fakeReplicator{nodes: map[string]*node{"node1": n1, "node2": n2}, down: map[string]bool{}}
	cluster := staticCluster{aliveView("node1"), aliveView("node2")}
	idem := local.NewIdempotency(0)
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraph.New(latencygraph.DefaultConfig()), repl, defaultPolicy(), idem, nil, nil, time.Second)
	gate := &kvFakeGate{}
	coord.WithDiskGate(gate, nil)

	// First write (clear) records the idempotency key.
	out1, err := coord.Put(context.Background(), "default", []byte("k"), []byte("v"), nil, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	// Now under pressure, the SAME idempotency key replays — must serve from the dedup cache, not shed.
	gate.pressure.Store(true)
	out2, err := coord.Put(context.Background(), "default", []byte("k"), []byte("v"), nil, "req-1")
	if err != nil {
		t.Fatalf("idempotent replay under pressure should serve, got %v", err)
	}
	if out1.Version != out2.Version {
		t.Fatalf("idempotent replay version mismatch: %v vs %v", out1.Version, out2.Version)
	}
}
