package collections

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
)

// Manager is the dragonboat implementation of RaftShard: a NodeHost hosting replicated-collection
// shards (and the meta shard) over a shared wavesdb store (design/30 §12.5, Appendix B). It runs the
// TTL sweeper for data shards it leads (log-driven expiry, design/30 §10). The default transport is
// dragonboat's built-in TCP; pass Options to NewManagerWithOptions for the cheap-mTLS transport +
// SWIM node registry (Appendix B.4-B.5). The wavesdb-backed LogDB (Appendix B.6) remains future work.
type Manager struct {
	nh    *dragonboat.NodeHost
	store storage.LocalStore

	mu     sync.Mutex
	shards map[uint64]shardReg // shardID -> registration

	prop       *proposer // QW2: batching/pipelining write driver for data shards
	tun        Tunables
	sweepEvery time.Duration
	stopCh     chan struct{}
	doneCh     chan struct{}

	diskGate DiskGate // disk-pressure admission (design/36); nil = no gating
	onShed   func()   // optional: called once per write shed for disk pressure (metrics counter)
}

// WithDiskGate installs a disk-pressure admission gate on the Manager: while gate.UnderPressure() is
// true, every write Propose (data shards AND the meta shard) is shed with ErrDiskPressure before reaching
// Raft, and onShed (if non-nil) is called once per shed. This is the LEADER-LOCAL BACKSTOP — the primary
// gate is at the write entry (Collections.WithDiskGate), which sheds on any node before forwarding. Reads
// (Manager.Read) are never gated. Returns the Manager for chaining. The gate type is DiskGate (declared
// in collections.go); internal/health.Monitor satisfies it. Passing a nil gate disables gating.
func (m *Manager) WithDiskGate(gate DiskGate, onShed func()) *Manager {
	m.diskGate = gate
	m.onShed = onShed
	return m
}

type shardReg struct {
	replicaID uint64
	isData    bool // data shards carry datatypes + TTL; the meta shard does not
}

var _ RaftShard = (*Manager)(nil)

// Options configures optional engine integrations for a Manager (design/30 §12): a custom Raft
// transport and node registry. The zero value uses dragonboat's built-in TCP transport + static
// registry.
type Options struct {
	TransportFactory config.TransportFactory
	RegistryFactory  config.NodeRegistryFactory
	Tunables         Tunables // consensus knobs; zero fields fall back to defaults
}

// Tunables are the consensus-tier knobs (design/30 §12, §10). A zero field uses its default.
type Tunables struct {
	RTTMillisecond     uint64        // base Raft RTT unit in ms; election/heartbeat are multiples of it
	ElectionRTT        uint64        // election timeout = ElectionRTT × RTT (lower = faster failover)
	HeartbeatRTT       uint64        // leader heartbeat = HeartbeatRTT × RTT
	SnapshotEntries    uint64        // entries between snapshots (smaller = faster catch-up, more I/O)
	CompactionOverhead uint64        // log entries retained after a snapshot
	SweepEvery         time.Duration // TTL sweep interval on shard leaders
	CoalesceWindow     time.Duration // QW2: window the proposer coalesces concurrent data-shard writes over
	CoalesceMaxOps     int           // QW2: max single ops coalesced into one Raft entry
	// Quiesce lets an idle shard enter dragonboat's quiesce mode: after ~ElectionRTT×10 idle ticks it
	// stops exchanging heartbeats (near-zero idle CPU) and wakes on the next real message/proposal.
	// *bool so the non-false default (ON) survives the zero-value merge: nil = default (ON). While
	// quiesced dragonboat runs QuiescedTick (not leaderTick/nonLeaderTick), so BOTH the CheckQuorum
	// step-down timer and the election timers are frozen — quiescence and CheckQuorum coexist without a
	// re-election storm; a wake resets the election timer (becomeFollower) so no election fires on exit.
	Quiesce *bool
}

// quiesceOn is the built-in default: quiescence enabled (idle-cheap by default).
func quiesceOn() *bool { b := true; return &b }

// DefaultTunables returns the built-in consensus defaults.
func DefaultTunables() Tunables {
	return Tunables{
		RTTMillisecond: 50, ElectionRTT: 10, HeartbeatRTT: 1,
		SnapshotEntries: 1000, CompactionOverhead: 500, SweepEvery: 500 * time.Millisecond,
		CoalesceWindow: defaultCoalesceWindow, CoalesceMaxOps: defaultCoalesceMaxOps,
		Quiesce: quiesceOn(),
	}
}

func (t Tunables) withDefaults() Tunables {
	d := DefaultTunables()
	if t.RTTMillisecond == 0 {
		t.RTTMillisecond = d.RTTMillisecond
	}
	if t.ElectionRTT == 0 {
		t.ElectionRTT = d.ElectionRTT
	}
	if t.HeartbeatRTT == 0 {
		t.HeartbeatRTT = d.HeartbeatRTT
	}
	if t.SnapshotEntries == 0 {
		t.SnapshotEntries = d.SnapshotEntries
	}
	if t.CompactionOverhead == 0 {
		t.CompactionOverhead = d.CompactionOverhead
	}
	if t.SweepEvery == 0 {
		t.SweepEvery = d.SweepEvery
	}
	if t.CoalesceWindow == 0 {
		t.CoalesceWindow = d.CoalesceWindow
	}
	if t.CoalesceMaxOps == 0 {
		t.CoalesceMaxOps = d.CoalesceMaxOps
	}
	if t.Quiesce == nil {
		t.Quiesce = d.Quiesce // nil = unset → default (ON)
	}
	return t
}

// NewManager opens a NodeHost rooted at nodeHostDir, bound to raftAddr, applying shard state to store,
// using dragonboat's built-in transport.
func NewManager(nodeHostDir, raftAddr string, store storage.LocalStore) (*Manager, error) {
	return NewManagerWithOptions(nodeHostDir, raftAddr, store, Options{})
}

// NewManagerWithOptions is NewManager with a custom transport / node registry (e.g. the cheap-mTLS
// transport + SWIM registry, design/30 §12).
func NewManagerWithOptions(nodeHostDir, raftAddr string, store storage.LocalStore, opts Options) (*Manager, error) {
	tun := opts.Tunables.withDefaults()
	nhc := config.NodeHostConfig{
		NodeHostDir:    nodeHostDir,
		RTTMillisecond: tun.RTTMillisecond,
		RaftAddress:    raftAddr,
	}
	nhc.Expert.TransportFactory = opts.TransportFactory
	nhc.Expert.NodeRegistryFactory = opts.RegistryFactory
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		nh: nh, store: store, shards: map[uint64]shardReg{},
		tun:        tun,
		sweepEvery: tun.SweepEvery,
		stopCh:     make(chan struct{}), doneCh: make(chan struct{}),
	}
	m.prop = newProposer(rawProposeShard{m}, tun.CoalesceWindow, tun.CoalesceMaxOps)
	go m.sweepLoop()
	return m, nil
}

// rawProposeShard adapts the Manager's un-batched SyncPropose to the proposer's asyncShard surface,
// breaking the recursion (Manager.Propose -> proposer -> rawPropose).
type rawProposeShard struct{ m *Manager }

func (r rawProposeShard) Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) {
	return r.m.proposeRaw(ctx, shardID, cmd)
}

// Tunables returns the consensus knobs this Manager is running with.
func (m *Manager) Tunables() Tunables { return m.tun }

// ShardStatus is one shard's leadership snapshot for the operator view.
type ShardStatus struct {
	ShardID, ReplicaID, LeaderReplicaID uint64
	HasLeader, IsLeader, IsData         bool
}

// ShardStatuses returns the leadership status of every shard this node hosts, ordered by shard id.
func (m *Manager) ShardStatuses() []ShardStatus {
	m.mu.Lock()
	regs := make(map[uint64]shardReg, len(m.shards))
	for k, v := range m.shards {
		regs[k] = v
	}
	m.mu.Unlock()
	out := make([]ShardStatus, 0, len(regs))
	for sid, reg := range regs {
		lead, ok := m.LeaderID(sid)
		out = append(out, ShardStatus{
			ShardID: sid, ReplicaID: reg.replicaID, LeaderReplicaID: lead,
			HasLeader: ok, IsLeader: ok && lead == reg.replicaID, IsData: reg.isData,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

func (m *Manager) shardConfig(shardID, replicaID uint64) config.Config {
	return config.Config{
		ShardID:            shardID,
		ReplicaID:          replicaID,
		ElectionRTT:        m.tun.ElectionRTT,
		HeartbeatRTT:       m.tun.HeartbeatRTT,
		CheckQuorum:        true,
		Quiesce:            *m.tun.Quiesce, // non-nil: withDefaults() fills the default before we store m.tun
		SnapshotEntries:    m.tun.SnapshotEntries,
		CompactionOverhead: m.tun.CompactionOverhead,
	}
}

func (m *Manager) startReplica(shardID, replicaID uint64, members map[uint64]string, join bool, factory sm.CreateOnDiskStateMachineFunc, isData, nonVoting bool) error {
	cfg := m.shardConfig(shardID, replicaID)
	cfg.IsNonVoting = nonVoting
	if err := m.nh.StartOnDiskReplica(members, join, factory, cfg); err != nil {
		return err
	}
	m.mu.Lock()
	m.shards[shardID] = shardReg{replicaID: replicaID, isData: isData}
	m.mu.Unlock()
	return nil
}

// StartShard starts (or restarts) a data shard (datatype state machine).
func (m *Manager) StartShard(shardID, replicaID uint64, initialMembers map[uint64]string, join bool) error {
	factory := func(sid, _ uint64) sm.IOnDiskStateMachine { return newShardSM(m.store, sid) }
	return m.startReplica(shardID, replicaID, initialMembers, join, factory, true, false)
}

// StartMetaShard starts (or restarts) the meta shard (range directory state machine, design/30 §7).
func (m *Manager) StartMetaShard(shardID, replicaID uint64, initialMembers map[uint64]string, join bool) error {
	factory := func(sid, _ uint64) sm.IOnDiskStateMachine { return newMetaSM(m.store, sid) }
	return m.startReplica(shardID, replicaID, initialMembers, join, factory, false, false)
}

// Propose commits an encoded command through the shard leader and returns the apply result. Data-shard
// writes are driven through the batching/pipelining proposer (QW2) so concurrent single ops coalesce
// into few large Raft entries; the meta shard (control-plane, low write rate, distinct encoding) uses
// the un-batched path so its commands are never wrapped in a data-only opBatch.
func (m *Manager) Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) {
	// DISK-PRESSURE ADMISSION (design/36): shed every write BEFORE it reaches Raft when the storage volume
	// is low on free space. This is the choke point for both data-shard writes (proposer) and meta-shard
	// writes (proposeRaw) — every command that would grow the pebble LogDB passes here — so one check stops
	// the log from growing, lets compaction free space, and keeps pebble from panicking on a full volume
	// and crash-looping the voters. Reads go through Manager.Read and are never gated.
	if m.diskGate != nil && m.diskGate.UnderPressure() {
		if m.onShed != nil {
			m.onShed()
		}
		return ProposeResult{}, ErrDiskPressure
	}
	if shardID == MetaShardID || m.prop == nil || !coalescable(cmd) {
		return m.proposeRaw(ctx, shardID, cmd)
	}
	return m.prop.Propose(ctx, shardID, cmd)
}

// coalescable reports whether a data-shard command may be wrapped into an opBatch entry. Only regular
// datatype mutations (and expire/remove) are — control-plane ops (ingest/purge/freeze/unfreeze) carry a
// distinct encoding the batch sub-decoder rejects, so they take the un-batched path and stay atomic.
func coalescable(cmd []byte) bool {
	if len(cmd) == 0 {
		return false
	}
	switch opKind(cmd[0]) {
	case opSAdd, opSRem, opHSet, opHDel, opZAdd, opZRem, opHIncrBy, opHIncrByFloat, opExpire, opRemove:
		return true
	}
	return false
}

// defaultProposeTimeout bounds a consensus round-trip when the caller supplied no deadline. dragonboat's
// SyncPropose/SyncRead return ErrDeadlineNotSet for a deadline-less context, so a client that issues a
// write without a timeout (e.g. the admin-port UI) would otherwise fail spuriously — and, worse, that
// error is "forwardable", so the write gets bounced to peers instead of committing locally.
const defaultProposeTimeout = 5 * time.Second

// ensureDeadline returns ctx unchanged if it already carries a deadline; otherwise it derives one with
// defaultProposeTimeout. The returned cancel must always be called (it is a no-op when ctx was returned
// as-is).
func ensureDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultProposeTimeout)
}

// proposeRaw issues one un-batched synchronous proposal (the engine round-trip the proposer drives).
// dragonboat's own overload signals (ErrSystemBusy when the proposal pipeline is saturated, ErrShardNotReady
// when a proposal is dropped) are surfaced as the transient ErrBusy (→ ResourceExhausted) so a flood is
// shed with a retryable error — never a panic, never a node crash.
func (m *Manager) proposeRaw(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) {
	ctx, cancel := ensureDeadline(ctx)
	defer cancel()
	res, err := m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)
	if err != nil {
		if errors.Is(err, dragonboat.ErrSystemBusy) || errors.Is(err, dragonboat.ErrShardNotReady) {
			return ProposeResult{}, ErrBusy
		}
		return ProposeResult{}, err
	}
	return ProposeResult{Value: res.Value, Data: res.Data}, nil
}

// Read answers a query against shardID. linearizable routes a ReadIndex through the leader (a quorum
// confirm, no log write); otherwise it is a local stale read with no round-trip (QW3, design/32 §3.3):
// it reads the local replica directly, so it serves off any replica — a voter OR a demand-filled spot
// learner — keeping reads off the write path. The datatype/benchmark read path defaults to stale (the
// proto Linearizable flag is false by default, see Service); callers opt into linearizable per call.
func (m *Manager) Read(ctx context.Context, shardID uint64, query interface{}, linearizable bool) (interface{}, error) {
	if linearizable {
		ctx, cancel := ensureDeadline(ctx)
		defer cancel()
		return m.nh.SyncRead(ctx, shardID, query)
	}
	return m.nh.StaleRead(shardID, query)
}

// NodeHostID returns this node's dragonboat NodeHostID — the stable membership target in NodeHostID
// addressing mode (with a custom node registry, design/30 §12).
func (m *Manager) NodeHostID() string { return m.nh.ID() }

// hasLeader reports whether the shard currently has an elected leader (ready for proposes/reads).
func (m *Manager) hasLeader(shardID uint64) bool {
	_, _, ok, err := m.nh.GetLeaderID(shardID)
	return err == nil && ok
}

// LeaderID returns the current leader's replicaID for a shard and whether one is known.
func (m *Manager) LeaderID(shardID uint64) (uint64, bool) {
	id, _, ok, err := m.nh.GetLeaderID(shardID)
	if err != nil || !ok {
		return 0, false
	}
	return id, true
}

// IsLeader reports whether this node is the shard's current leader.
func (m *Manager) IsLeader(shardID uint64) bool {
	lead, ok := m.LeaderID(shardID)
	if !ok {
		return false
	}
	m.mu.Lock()
	reg, hosted := m.shards[shardID]
	m.mu.Unlock()
	return hosted && reg.replicaID == lead
}

// Stop halts the sweeper then closes the NodeHost (does not close the shared store).
func (m *Manager) Stop() {
	close(m.stopCh)
	<-m.doneCh
	m.nh.Close()
}

func (m *Manager) sweepLoop() {
	defer close(m.doneCh)
	t := time.NewTicker(m.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			m.sweepOnce()
		}
	}
}

// sweepOnce proposes expirations for due elements of every local data shard this node leads.
func (m *Manager) sweepOnce() {
	m.mu.Lock()
	local := make(map[uint64]uint64, len(m.shards))
	for s, r := range m.shards {
		if r.isData {
			local[s] = r.replicaID
		}
	}
	m.mu.Unlock()

	now := time.Now().UnixMilli()
	for shardID, replicaID := range local {
		if lead, _, ok, err := m.nh.GetLeaderID(shardID); err != nil || !ok || lead != replicaID {
			continue // only the leader sweeps
		}
		// Pass 1: TTL element expiry (design/30 §10). An empty/failed due read no longer skips the budget
		// pass below (a shard with no TTL elements still needs its timed leases swept).
		if v, err := m.nh.StaleRead(shardID, ttlDueQuery{NowMs: now, Limit: 1024}); err == nil {
			if due, _ := v.([]dueElem); len(due) > 0 {
				type ck struct{ ns, coll string }
				groups := map[ck][]item{}
				for _, d := range due {
					k := ck{string(d.NS), string(d.Coll)}
					groups[k] = append(groups[k], item{Key: d.Member, ExpiryMs: int64(d.Expiry)})
				}
				for k, items := range groups {
					cmd := encodeCommand(command{Op: opExpire, NS: []byte(k.ns), Coll: []byte(k.coll), Items: items})
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_, _ = m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)
					cancel()
				}
			}
		}
		// Pass 2: leased-budget forced expiry + tombstone GC (Stage 2 §3.5/§6).
		m.sweepBudget(shardID, now)
	}
}

// sweepBudget runs the Stage-2 leased-budget passes for one shard this node leads (caller already gated on
// leadership). It force-expires every timed lease past its REPLICATED reclaim deadline — the DEBIT
// settlement lands in applyBudExpire (§3.5) — and GCs settled tombstones whose dedup retry window has
// elapsed (§6). sweepNowMs is leader-stamped here, pre-propose, so apply reads no wall clock; the deadline
// comparison runs against the replicated ReclaimNotBeforeMs, surviving a leader change.
func (m *Manager) sweepBudget(shardID uint64, sweepNowMs int64) {
	if v, err := m.nh.StaleRead(shardID, budExpiryDueQuery{NowMs: sweepNowMs, Limit: 1024}); err == nil {
		due, _ := v.([]dueBudLease)
		for _, d := range due {
			val := make([]byte, 8)
			putI64(val, sweepNowMs)
			cmd := encodeCommand(command{Op: opBudExpire, NS: d.NS, Coll: d.Coll, Items: []item{{Key: d.LeaseID, Val: val}}})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)
			cancel()
		}
	}
	if v, err := m.nh.StaleRead(shardID, budTombGCDueQuery{NowMs: sweepNowMs, Limit: 1024}); err == nil {
		due, _ := v.([]dueBudLease)
		for _, d := range due {
			val := make([]byte, 8)
			putI64(val, d.ReclaimMs) // gcDueMs — apply recomputes the GC-index key from it
			cmd := encodeCommand(command{Op: opBudTombGC, NS: d.NS, Coll: d.Coll, Items: []item{{Key: d.LeaseID, Val: val}}})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)
			cancel()
		}
	}
}
