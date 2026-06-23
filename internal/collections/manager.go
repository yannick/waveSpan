package collections

import (
	"context"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/cwire/wavespan/internal/storage"
)

// Manager is the dragonboat implementation of RaftShard: a NodeHost hosting replicated-collection
// shards over a shared wavesdb store (design/30 §12.5, Appendix B). M-A uses dragonboat's built-in
// transport + default Pebble LogDB; the cheap-mTLS transport, SWIM node registry, and wavesdb-backed
// LogDB (Appendix B.4-B.6) are later milestones.
type Manager struct {
	nh    *dragonboat.NodeHost
	store storage.LocalStore
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
	return &Manager{nh: nh, store: store}, nil
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
	return m.nh.StartOnDiskReplica(initialMembers, join, factory, cfg)
}

func (m *Manager) Propose(ctx context.Context, shardID uint64, cmd []byte) (uint64, error) {
	res, err := m.nh.SyncPropose(ctx, m.nh.GetNoOPSession(shardID), cmd)
	if err != nil {
		return 0, err
	}
	return res.Value, nil
}

func (m *Manager) Read(ctx context.Context, shardID uint64, query interface{}, linearizable bool) (interface{}, error) {
	if linearizable {
		return m.nh.SyncRead(ctx, shardID, query)
	}
	return m.nh.StaleRead(shardID, query)
}

func (m *Manager) Stop() { m.nh.Close() }
