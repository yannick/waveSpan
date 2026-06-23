// Package storage is WaveSpan's local-storage boundary. It defines the LocalStore interface
// that the distributed layers depend on, a wavesdb-backed implementation, and an in-memory
// implementation for tests. Distributed correctness is built above this; the engine is local
// storage only (design/02_storage_wavesdb.md).
package storage

// ColumnFamily is a logical keyspace. Each maps 1:1 to a wavesdb column family (cf.go).
type ColumnFamily int

const (
	// CFSys holds member metadata, storage UUID, and config snapshots.
	CFSys ColumnFamily = iota
	// CFKVData holds user KV versioned records.
	CFKVData
	// CFKVMeta holds latest-version pointers, holder summaries, and TTL metadata.
	CFKVMeta
	// CFGraphData holds node and relationship records.
	CFGraphData
	// CFGraphIndex holds label, property, and adjacency indexes.
	CFGraphIndex
	// CFVectorRaw holds raw vector payloads.
	CFVectorRaw
	// CFVectorIndex holds ANN/exact index metadata and segment data.
	CFVectorIndex
	// CFReplLog holds local and global mutation logs.
	CFReplLog
	// CFCacheMeta holds dynamic cache subscriptions, watermarks, and leases.
	CFCacheMeta
	// CFReplData holds the applied state of replicated-collection Raft shards (design/30).
	CFReplData
)

// StoreOp is a single write within an atomic Batch.
type StoreOp struct {
	CF     ColumnFamily
	Key    []byte
	Value  []byte
	Delete bool
	// ExpiresAtUnixMs, when > 0, sets the engine's native per-key TTL so wavesdb physically drops
	// the entry during compaction once expired (design/02 "TTL storage"). 0 means no expiry. The
	// store converts it to a remaining time.Duration at write time.
	ExpiresAtUnixMs int64
}

// StoreKV is a key/value pair yielded by a scan.
type StoreKV struct {
	Key   []byte
	Value []byte
}

// Iterator is a forward cursor over a key range. Callers must Close it. Keys are visited
// in ascending byte order. Key/Value are only valid while Valid reports true and may be
// reused on Next, so copy if retaining beyond the current position.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Err() error
	Close() error
}

// Snapshot is a read-consistent view of the store. Reads and scans taken from it observe
// the same point-in-time state regardless of concurrent writes (design/02 invariants).
type Snapshot interface {
	Get(cf ColumnFamily, key []byte) ([]byte, bool, error)
	Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error)
	Close() error
}

// LocalStore is the storage abstraction the distributed layers build on. It is the Go form
// of the trait in design/02_storage_wavesdb.md; the engine beneath it is wavesdb.
type LocalStore interface {
	// Put writes a single key/value (auto-committed).
	Put(cf ColumnFamily, key, value []byte) error
	// Get returns the value for key; found is false when the key is absent.
	Get(cf ColumnFamily, key []byte) (value []byte, found bool, err error)
	// Delete removes key (auto-committed). Deleting an absent key is not an error.
	Delete(cf ColumnFamily, key []byte) error
	// Batch applies all ops atomically in a single transaction (all-or-nothing).
	Batch(ops []StoreOp) error
	// BatchRC applies all ops atomically at ReadCommitted isolation (skips the write-write conflict
	// check so independent keys commit in parallel; the caller orders same-key writes itself).
	BatchRC(ops []StoreOp) error
	// Scan returns a forward iterator over [start, end) limited to limit rows (0 = unlimited).
	Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error)
	// Snapshot returns a read-consistent view.
	Snapshot() (Snapshot, error)
	// Flush forces the column family's memtable to disk.
	Flush(cf ColumnFamily) error
	// CompactRange compacts the column family over [start, end) (approximated by full-CF
	// compaction where the engine lacks range compaction).
	CompactRange(cf ColumnFamily, start, end []byte) error
	// Close releases the store.
	Close() error
}
