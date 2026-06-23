package collections

import (
	"context"
	"sync"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/cwire/wavespan/internal/storage"
)

// Manager is the dragonboat implementation of RaftShard: a NodeHost hosting replicated-collection
// shards over a shared wavesdb store (design/30 §12.5, Appendix B). It also runs the TTL sweeper:
// for each local shard where this node leads, it scans the expiry index and proposes opExpire
// deletions (log-driven expiry, design/30 §10). M-A/M-D use dragonboat's built-in transport + default
// Pebble LogDB; the cheap-mTLS transport, SWIM node registry, and wavesdb-backed LogDB (Appendix
// B.4-B.6) are later milestones.
type Manager struct {
	nh    *dragonboat.NodeHost
	store storage.LocalStore

	mu     sync.Mutex
	shards map[uint64]uint64 // shardID -> this node's replicaID

	sweepEvery time.Duration
	stopCh     chan struct{}
	doneCh     chan struct{}
}

var _ RaftShard = (*Manager)(nil)

// NewManager opens a NodeHost rooted at nodeHostDir, bound to raftAddr, applying shard state to store.
func NewManager(nodeHostDir, raftAddr string, store storage.LocalStore) (*Manager, error) {
	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		NodeHostDir:    nodeHostDir,
		RTTMillisecond: 50,
		RaftAddress:    raftAddr,
	})
	if err != nil {
		return nil, err
	}
	m := &Manager{
		nh: nh, store: store, shards: map[uint64]uint64{},
		sweepEvery: 500 * time.Millisecond,
		stopCh:     make(chan struct{}), doneCh: make(chan struct{}),
	}
	go m.sweepLoop()
	return m, nil
}

func (m *Manager) StartShard(shardID, replicaID uint64, initialMembers map[uint64]string, join bool) error {
	cfg := config.Config{
		ShardID:            shardID,
		ReplicaID:          replicaID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    1000,
		CompactionOverhead: 500,
	}
	factory := func(sid, rid uint64) sm.IOnDiskStateMachine { return newShardSM(m.store, sid) }
	if err := m.nh.StartOnDiskReplica(initialMembers, join, factory, cfg); err != nil {
		return err
	}
	m.mu.Lock()
	m.shards[shardID] = replicaID
	m.mu.Unlock()
	return nil
}

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

// sweepOnce proposes expirations for due elements of every local shard this node leads.
func (m *Manager) sweepOnce() {
	m.mu.Lock()
	local := make(map[uint64]uint64, len(m.shards))
	for s, r := range m.shards {
		local[s] = r
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
