package collections

import (
	"context"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/cwire/wavespan/internal/storage"
)

// Manager wraps a dragonboat NodeHost hosting replicated-collection shards over a shared wavesdb store
// (design/30 §12.5, Appendix B). M-0: built-in transport + default Pebble LogDB; the cheap-mTLS
// transport, SWIM node registry, and wavesdb-backed LogDB are layered in later milestones.
type Manager struct {
	nh    *dragonboat.NodeHost
	store storage.LocalStore
}

// NewManager opens a NodeHost rooted at nodeHostDir, bound to raftAddr, applying shard state to store.
func NewManager(nodeHostDir, raftAddr string, store storage.LocalStore) (*Manager, error) {
	nhc := config.NodeHostConfig{
		NodeHostDir:    nodeHostDir,
		RTTMillisecond: 50,
		RaftAddress:    raftAddr,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	return &Manager{nh: nh, store: store}, nil
}

// StartShard starts (or restarts) a Raft shard whose on-disk state machine applies into CFReplData.
// initialMembers maps ReplicaID → RaftAddress; join is true when adding to an existing shard.
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
	return m.nh.StartOnDiskReplica(initialMembers, join, factory, cfg)
}

// Propose commits a mutation through the shard leader and returns the Update result value.
func (m *Manager) Propose(ctx context.Context, shardID uint64, c command) (uint64, error) {
	res, err := m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), encodeCommand(c))
	if err != nil {
		return 0, err
	}
	return res.Value, nil
}

// Get reads a key. linearizable=true routes a read-index through the leader; false serves a
// bounded-stale local read (design/30 §5.4, §13.10). Returns nil when the key is absent.
func (m *Manager) Get(ctx context.Context, shardID uint64, key []byte, linearizable bool) ([]byte, error) {
	var (
		v   interface{}
		err error
	)
	if linearizable {
		v, err = m.nh.SyncRead(ctx, shardID, key)
	} else {
		v, err = m.nh.StaleRead(shardID, key)
	}
	if err != nil || v == nil {
		return nil, err
	}
	return v.([]byte), nil
}

// Close stops the NodeHost (does not close the shared store).
func (m *Manager) Close() { m.nh.Close() }
