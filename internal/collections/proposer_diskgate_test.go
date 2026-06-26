package collections

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// fakeGate is a togglable DiskGate for the admission tests.
type fakeGate struct{ pressure atomic.Bool }

func (g *fakeGate) UnderPressure() bool { return g.pressure.Load() }

// okShard is an asyncShard that always commits, so the "serve when clear" path returns a real result.
type okShard struct{ calls atomic.Int64 }

func (s *okShard) Propose(context.Context, uint64, []byte) (ProposeResult, error) {
	s.calls.Add(1)
	return ProposeResult{Value: 7}, nil
}

// newGatedManager wires a Manager with the batching proposer over shard, plus a disk gate + shed
// counter — but NO dragonboat NodeHost, so it only exercises the admission branch + proposer path.
func newGatedManager(shard asyncShard, gate DiskGate) (*Manager, *int64) {
	var shed int64
	m := &Manager{
		shards: map[uint64]shardReg{},
		tun:    DefaultTunables(),
	}
	m.prop = newProposer(shard, 0, 0)
	m.WithDiskGate(gate, func() { atomic.AddInt64(&shed, 1) })
	return m, &shed
}

func TestProposeShedsUnderDiskPressure(t *testing.T) {
	shard := &okShard{}
	gate := &fakeGate{}
	m, shed := newGatedManager(shard, gate)

	// A data-shard write (coalescable op) on shard 2 (shard 1 is the meta shard).
	cmd := []byte{byte(opSAdd), 0, 0}

	// Clear: the write is admitted and reaches the shard.
	if _, err := m.Propose(context.Background(), 2, cmd); err != nil {
		t.Fatalf("clear gate: write should serve, got %v", err)
	}
	if shard.calls.Load() != 1 {
		t.Fatalf("clear gate: want shard called once, got %d", shard.calls.Load())
	}
	if got := atomic.LoadInt64(shed); got != 0 {
		t.Fatalf("clear gate: shed counter should be 0, got %d", got)
	}

	// Under pressure: the write is shed BEFORE the shard, with ErrDiskPressure, and the counter bumps.
	gate.pressure.Store(true)
	_, err := m.Propose(context.Background(), 2, cmd)
	if !errors.Is(err, ErrDiskPressure) {
		t.Fatalf("pressure: want ErrDiskPressure, got %v", err)
	}
	if shard.calls.Load() != 1 {
		t.Fatalf("pressure: shard must NOT be called, calls=%d", shard.calls.Load())
	}
	if got := atomic.LoadInt64(shed); got != 1 {
		t.Fatalf("pressure: shed counter should be 1, got %d", got)
	}

	// Recover: writes flow again (compaction freed space → resume).
	gate.pressure.Store(false)
	if _, err := m.Propose(context.Background(), 2, cmd); err != nil {
		t.Fatalf("recovered gate: write should serve, got %v", err)
	}
	if shard.calls.Load() != 2 {
		t.Fatalf("recovered: want shard called twice, got %d", shard.calls.Load())
	}
}

func TestProposeShedsMetaShardUnderDiskPressure(t *testing.T) {
	// The meta shard (control-plane directory writes) also grows the LogDB, so it is shed too. It takes the
	// un-batched path; with no NodeHost the only safe assertion is that the gate rejects it before raft.
	gate := &fakeGate{}
	gate.pressure.Store(true)
	m, shed := newGatedManager(&okShard{}, gate)

	_, err := m.Propose(context.Background(), MetaShardID, []byte{byte(opIngest)})
	if !errors.Is(err, ErrDiskPressure) {
		t.Fatalf("meta-shard under pressure: want ErrDiskPressure, got %v", err)
	}
	if atomic.LoadInt64(shed) != 1 {
		t.Fatalf("meta-shard shed counter should be 1, got %d", atomic.LoadInt64(shed))
	}
}

func TestProposeNoGateAlwaysAdmits(t *testing.T) {
	// A Manager with no disk gate installed must behave exactly as before (no gating).
	shard := &okShard{}
	m := &Manager{shards: map[uint64]shardReg{}, tun: DefaultTunables()}
	m.prop = newProposer(shard, 0, 0)
	if _, err := m.Propose(context.Background(), 2, []byte{byte(opSAdd), 0, 0}); err != nil {
		t.Fatalf("no gate: write should serve, got %v", err)
	}
	if shard.calls.Load() != 1 {
		t.Fatalf("no gate: want shard called once, got %d", shard.calls.Load())
	}
}
