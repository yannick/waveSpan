package collections

import (
	"context"
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

	tun        Tunables
	sweepEvery time.Duration
	stopCh     chan struct{}
	doneCh     chan struct{}
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
}

// DefaultTunables returns the built-in consensus defaults.
func DefaultTunables() Tunables {
	return Tunables{
		RTTMillisecond: 50, ElectionRTT: 10, HeartbeatRTT: 1,
		SnapshotEntries: 1000, CompactionOverhead: 500, SweepEvery: 500 * time.Millisecond,
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
	go m.sweepLoop()
	return m, nil
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

// Propose commits an encoded command through the shard leader and returns the apply result.
func (m *Manager) Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) {
	res, err := m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)
	if err != nil {
		return ProposeResult{}, err
	}
	return ProposeResult{Value: res.Value, Data: res.Data}, nil
}

func (m *Manager) Read(ctx context.Context, shardID uint64, query interface{}, linearizable bool) (interface{}, error) {
	if linearizable {
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
		v, err := m.nh.StaleRead(shardID, ttlDueQuery{NowMs: now, Limit: 1024})
		if err != nil {
			continue
		}
		due, _ := v.([]dueElem)
		if len(due) == 0 {
			continue
		}
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
