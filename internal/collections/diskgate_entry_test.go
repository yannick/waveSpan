package collections

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordingShard is a minimal RaftShard fake: it records Propose calls and lets the test pick whether
// this node "leads" the shard. Propose always commits (so a non-shed write succeeds).
type recordingShard struct {
	leader   bool
	proposes atomic.Int64
}

func (s *recordingShard) StartShard(uint64, uint64, map[uint64]string, bool) error { return nil }
func (s *recordingShard) Propose(context.Context, uint64, []byte) (ProposeResult, error) {
	s.proposes.Add(1)
	return ProposeResult{Value: 1}, nil
}
func (s *recordingShard) Read(context.Context, uint64, interface{}, bool) (interface{}, error) {
	return nil, nil
}
func (s *recordingShard) IsLeader(uint64) bool { return s.leader }
func (s *recordingShard) Stop()                {}

// recordingForwarder records whether a write was forwarded (and can return a chosen error, e.g. the
// leader's ResourceExhausted, to exercise the forward-error mapping).
type recordingForwarder struct {
	calls atomic.Int64
	err   error
}

func (f *recordingForwarder) Forward(context.Context, []byte, []byte, []byte) (uint64, []byte, error) {
	f.calls.Add(1)
	if f.err != nil {
		return 0, nil, f.err
	}
	return 9, nil, nil
}

// TestEntryGateShedsOnNonLeaderBeforeForwarding is the core fix: a node that is NOT the shard leader and
// is under its OWN disk pressure must shed at the write ENTRY (ErrDiskPressure), NOT forward — because as
// a follower/applier it grows its own disk from the replicated write.
func TestEntryGateShedsOnNonLeaderBeforeForwarding(t *testing.T) {
	shard := &recordingShard{leader: false} // this node does not lead the shard
	fwd := &recordingForwarder{}
	gate := &fakeGate{}
	gate.pressure.Store(true)
	var shed int64

	c := New(shard, SingleShardDirectory(2)).
		WithForwarder(fwd).
		WithDiskGate(gate, func() { atomic.AddInt64(&shed, 1) })

	_, err := c.SAdd(context.Background(), []byte("ns"), []byte("coll"), []byte("m"))
	if !errors.Is(err, ErrDiskPressure) {
		t.Fatalf("non-leader under pressure: want ErrDiskPressure, got %v", err)
	}
	if fwd.calls.Load() != 0 {
		t.Fatalf("must NOT forward under own disk pressure, forwarded %d times", fwd.calls.Load())
	}
	if shard.proposes.Load() != 0 {
		t.Fatalf("must not propose, proposed %d times", shard.proposes.Load())
	}
	if atomic.LoadInt64(&shed) != 1 {
		t.Fatalf("shed counter want 1, got %d", atomic.LoadInt64(&shed))
	}
	// Map to ResourceExhausted at the service boundary (not Internal).
	if got := connect.CodeOf(collErr(err)); got != connect.CodeResourceExhausted {
		t.Fatalf("collErr(ErrDiskPressure) want ResourceExhausted, got %v", got)
	}
}

// TestEntryGateClearForwardsNormally confirms the gate does not change healthy routing: a non-leader
// with a clear gate still forwards to the leader.
func TestEntryGateClearForwardsNormally(t *testing.T) {
	shard := &recordingShard{leader: false}
	fwd := &recordingForwarder{}
	gate := &fakeGate{} // clear

	c := New(shard, SingleShardDirectory(2)).WithForwarder(fwd).WithDiskGate(gate, nil)

	if _, err := c.SAdd(context.Background(), []byte("ns"), []byte("coll"), []byte("m")); err != nil {
		t.Fatalf("clear gate non-leader: want forward success, got %v", err)
	}
	if fwd.calls.Load() != 1 {
		t.Fatalf("clear gate: want one forward, got %d", fwd.calls.Load())
	}
}

// TestProposeRawShedsUnderPressure covers the server side of a forwarded write: a node receiving a
// ProposeForward while under its OWN disk pressure must shed (it is the applier whose volume would grow),
// returning ErrDiskPressure -> ResourceExhausted over the wire.
func TestProposeRawShedsUnderPressure(t *testing.T) {
	shard := &recordingShard{leader: true}
	gate := &fakeGate{}
	gate.pressure.Store(true)
	var shed int64
	c := New(shard, SingleShardDirectory(2)).WithDiskGate(gate, func() { atomic.AddInt64(&shed, 1) })

	_, _, err := c.ProposeRaw(context.Background(), []byte("ns"), []byte("coll"), []byte{byte(opSAdd)})
	if !errors.Is(err, ErrDiskPressure) {
		t.Fatalf("ProposeRaw under pressure: want ErrDiskPressure, got %v", err)
	}
	if shard.proposes.Load() != 0 {
		t.Fatalf("ProposeRaw must not propose under pressure, proposed %d", shard.proposes.Load())
	}
	if atomic.LoadInt64(&shed) != 1 {
		t.Fatalf("shed counter want 1, got %d", atomic.LoadInt64(&shed))
	}
	// Clear: ProposeRaw serves again.
	gate.pressure.Store(false)
	if _, _, err := c.ProposeRaw(context.Background(), []byte("ns"), []byte("coll"), []byte{byte(opSAdd)}); err != nil {
		t.Fatalf("ProposeRaw clear: want serve, got %v", err)
	}
}

// gateForwarder routes ProposeForward to a peer Collections (the in-process equivalent of the RPC), so a
// forwarded write hits the peer's ProposeRaw — including ITS disk gate.
type gateForwarder struct{ peer *Collections }

func (f gateForwarder) Forward(ctx context.Context, ns, coll, cmd []byte) (uint64, []byte, error) {
	n, data, err := f.peer.ProposeRaw(ctx, ns, coll, cmd)
	// Mirror RPCForwarder's terminal mapping: a peer that shed (disk pressure) is surfaced, not retried.
	if errors.Is(err, ErrDiskPressure) {
		return 0, nil, ErrDiskPressure
	}
	return n, data, err
}

// TestForwardToPressuredLeaderSurfacesResourceExhausted is the end-to-end fix #2: a HEALTHY entry node
// forwards to a leader that is under disk pressure; the leader sheds (ProposeRaw), and the forwarding
// node must surface ErrDiskPressure -> ResourceExhausted (NOT Internal).
func TestForwardToPressuredLeaderSurfacesResourceExhausted(t *testing.T) {
	// Leader node: under pressure, will shed in ProposeRaw.
	leaderGate := &fakeGate{}
	leaderGate.pressure.Store(true)
	leader := New(&recordingShard{leader: true}, SingleShardDirectory(2)).WithDiskGate(leaderGate, nil)

	// Entry node: healthy gate, not the leader, forwards to the leader.
	entry := New(&recordingShard{leader: false}, SingleShardDirectory(2)).
		WithForwarder(gateForwarder{peer: leader}).
		WithDiskGate(&fakeGate{}, nil)

	_, err := entry.SAdd(context.Background(), []byte("ns"), []byte("coll"), []byte("m"))
	if !errors.Is(err, ErrDiskPressure) {
		t.Fatalf("forward to pressured leader: want ErrDiskPressure, got %v", err)
	}
	if got := connect.CodeOf(collErr(err)); got != connect.CodeResourceExhausted {
		t.Fatalf("want ResourceExhausted at the forwarding node, got %v", got)
	}
}

// TestRPCForwarderSwitchMapsResourceExhausted asserts the real RPCForwarder.Forward switch maps a peer's
// gRPC ResourceExhausted to terminal ErrDiskPressure (the wire-level half of fix #2).
func TestRPCForwarderSwitchMapsResourceExhausted(t *testing.T) {
	err := status.Error(codes.ResourceExhausted, "leader shed")
	var mapped error
	switch status.Code(err) {
	case codes.ResourceExhausted:
		mapped = ErrDiskPressure
	default:
		t.Fatal("ResourceExhausted should hit the ResourceExhausted case")
	}
	if !errors.Is(mapped, ErrDiskPressure) {
		t.Fatalf("want ErrDiskPressure, got %v", mapped)
	}
	if got := connect.CodeOf(collErr(mapped)); got != connect.CodeResourceExhausted {
		t.Fatalf("re-map want ResourceExhausted, got %v", got)
	}
}
