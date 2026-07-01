package collections

import "context"

// MetaBackupStore is the production durable BackupIntent catalog: it proposes opBackupPut/opBackupDelete
// to the meta Raft group and reads intent blobs via its Lookup (design/backup phase 3a). It implements
// the backup.MetaStore interface structurally (collections cannot import backup — that would cycle), so
// the coordinator in internal/backup uses it through that interface; main.go supplies the concrete value.
type MetaBackupStore struct{ mgr *Manager }

// NewMetaBackupStore builds the catalog over the meta shard reachable through mgr.
func NewMetaBackupStore(mgr *Manager) *MetaBackupStore { return &MetaBackupStore{mgr: mgr} }

// PutBlob durably stores a catalog blob under key (Propose routes meta-shard commands un-batched).
func (s *MetaBackupStore) PutBlob(ctx context.Context, key string, blob []byte) error {
	_, err := s.mgr.Propose(ctx, MetaShardID, encodeMetaCommand(metaCommand{Op: opBackupPut, Start: []byte(key), End: blob}))
	return err
}

// DeleteBlob removes the catalog blob under key.
func (s *MetaBackupStore) DeleteBlob(ctx context.Context, key string) error {
	_, err := s.mgr.Propose(ctx, MetaShardID, encodeMetaCommand(metaCommand{Op: opBackupDelete, Start: []byte(key)}))
	return err
}

// GetBlob reads the catalog blob under key (linearizable). found is false when absent.
func (s *MetaBackupStore) GetBlob(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := s.mgr.Read(ctx, MetaShardID, metaBackupGetQuery{Key: []byte(key)}, true)
	if err != nil {
		return nil, false, err
	}
	b, _ := v.([]byte)
	return b, b != nil, nil
}

// ListBlobs reads every catalog blob keyed by catalog key (linearizable) — the UI/RPC path.
func (s *MetaBackupStore) ListBlobs(ctx context.Context) (map[string][]byte, error) {
	return s.list(ctx, true)
}

// ListBlobsStale reads every catalog blob via a NON-linearizable (local) read — used only by the periodic
// lifecycle sweep so it does not ReadIndex-wake the meta shard every tick (letting an idle meta shard
// quiesce). A slightly-stale catalog view is fine for best-effort GC (design/backup §6).
func (s *MetaBackupStore) ListBlobsStale(ctx context.Context) (map[string][]byte, error) {
	return s.list(ctx, false)
}

func (s *MetaBackupStore) list(ctx context.Context, linearizable bool) (map[string][]byte, error) {
	v, err := s.mgr.Read(ctx, MetaShardID, metaBackupListQuery{}, linearizable)
	if err != nil {
		return nil, err
	}
	m, _ := v.(map[string][]byte)
	return m, nil
}
