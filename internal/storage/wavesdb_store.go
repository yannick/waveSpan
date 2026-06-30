package storage

import (
	"bytes"
	"errors"
	"strings"
	"time"

	"wavesdb"
)

// EngineOptions carries the wavesdb tunables (resolved from the tunables registry by the caller) into
// the engine at open time. The zero value is valid and reproduces wavesdb's built-in defaults; the
// node populates it from config so the storage.engine.* tunables actually take effect (previously
// OpenWavesdb opened with Options{Path} only and every knob used the library default).
type EngineOptions struct {
	// DB-level (wavesdb.Options)
	BlockCacheSize       int64
	MaxOpenSSTables      int
	MaxMemoryUsage       int64
	NumFlushThreads      int
	NumCompactionThreads int

	// Column-family level (wavesdb.ColumnFamilyOptions)
	WriteBufferSize     int
	LevelSizeRatio      int
	MinLevels           int
	KlogValueThreshold  int
	Compression         string // none|snappy|lz4|zstd|flate
	EnableBloomFilter   bool
	BloomFPR            float64
	EnableBlockIndex    bool
	IndexSampleRatio    int
	BlockIndexPrefixLen int
	SyncMode            string // none|full|interval
	SyncInterval        time.Duration
	SkipListMaxLevel    int
	SkipListProbability float64
	DefaultIsolation    string // read-uncommitted|read-committed|repeatable-read|snapshot|serializable
	L1FileCountTrigger  int
	L0StallThreshold    int
	UseBTree            bool
}

func (e EngineOptions) dbOptions(path string) wavesdb.Options {
	return wavesdb.Options{
		Path:                 path,
		BlockCacheSize:       int(e.BlockCacheSize),
		MaxOpenSSTables:      e.MaxOpenSSTables,
		MaxMemoryUsage:       e.MaxMemoryUsage,
		NumFlushThreads:      e.NumFlushThreads,
		NumCompactionThreads: e.NumCompactionThreads,
	}
}

func (e EngineOptions) cfOptions() wavesdb.ColumnFamilyOptions {
	o := wavesdb.DefaultColumnFamilyOptions()
	o.WriteBufferSize = e.WriteBufferSize
	o.LevelSizeRatio = e.LevelSizeRatio
	o.MinLevels = e.MinLevels
	o.KlogValueThreshold = e.KlogValueThreshold
	o.Compression = parseCompression(e.Compression)
	o.EnableBloomFilter = e.EnableBloomFilter
	o.BloomFPR = e.BloomFPR
	o.EnableBlockIndex = e.EnableBlockIndex
	o.IndexSampleRatio = e.IndexSampleRatio
	o.BlockIndexPrefixLen = e.BlockIndexPrefixLen
	o.Sync = parseSyncMode(e.SyncMode)
	o.SyncInterval = e.SyncInterval
	o.SkipListMaxLevel = e.SkipListMaxLevel
	o.SkipListProbability = e.SkipListProbability
	o.DefaultIsolation = parseIsolation(e.DefaultIsolation)
	o.L1FileCountTrigger = e.L1FileCountTrigger
	o.L0StallThreshold = e.L0StallThreshold
	o.UseBTree = e.UseBTree
	return o
}

func parseCompression(s string) wavesdb.Compression {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "snappy":
		return wavesdb.CompressionSnappy
	case "lz4":
		return wavesdb.CompressionLZ4
	case "zstd":
		return wavesdb.CompressionZstd
	case "flate":
		return wavesdb.CompressionFlate
	default:
		return wavesdb.CompressionNone
	}
}

func parseSyncMode(s string) wavesdb.SyncMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "full":
		return wavesdb.SyncFull
	case "interval":
		return wavesdb.SyncInterval
	default:
		return wavesdb.SyncNone
	}
}

func parseIsolation(s string) wavesdb.IsolationLevel {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "read-uncommitted":
		return wavesdb.ReadUncommitted
	case "read-committed":
		return wavesdb.ReadCommitted
	case "repeatable-read":
		return wavesdb.RepeatableRead
	case "serializable":
		return wavesdb.Serializable
	default:
		return wavesdb.Snapshot
	}
}

// ttlForExpiry converts an absolute expires-at unix-ms into the remaining lifetime wavesdb expects
// as a per-key TTL. 0 (or unset) means no TTL. An already-past expiry yields a minimal positive
// duration so the engine treats the entry as immediately expired rather than as "no TTL".
func ttlForExpiry(expiresAtUnixMs int64) time.Duration {
	if expiresAtUnixMs <= 0 {
		return 0
	}
	d := time.Until(time.UnixMilli(expiresAtUnixMs))
	if d <= 0 {
		return time.Millisecond
	}
	return d
}

// WavesdbStore is the production LocalStore backed by the wavesdb LSM engine. It is the only
// package that imports wavesdb (design/17 dependency direction).
type WavesdbStore struct {
	db  *wavesdb.DB
	cfs map[ColumnFamily]*wavesdb.ColumnFamily
}

// OpenWavesdb opens (or creates) a wavesdb database at path with the library's default engine
// tunables. Used by tests and callers that don't tune the engine.
func OpenWavesdb(path string) (*WavesdbStore, error) {
	return OpenWavesdbWith(path, defaultEngineOptions())
}

// OpenWavesdbWith opens (or creates) a wavesdb database at path with the supplied engine tunables and
// ensures all logical column families exist. The node builds opts from the storage.engine.* tunables.
func OpenWavesdbWith(path string, opts EngineOptions) (*WavesdbStore, error) {
	db, err := wavesdb.Open(opts.dbOptions(path))
	if err != nil {
		return nil, err
	}
	cfOpts := opts.cfOptions()
	s := &WavesdbStore{db: db, cfs: make(map[ColumnFamily]*wavesdb.ColumnFamily, len(allColumnFamilies))}
	for _, cf := range allColumnFamilies {
		h := db.GetColumnFamily(cf.Name())
		if h == nil {
			h, err = db.CreateColumnFamily(cf.Name(), cfOpts)
			if err != nil {
				_ = db.Close()
				return nil, err
			}
		}
		s.cfs[cf] = h
	}
	return s, nil
}

// defaultEngineOptions mirrors wavesdb's DefaultColumnFamilyOptions so OpenWavesdb (no tunables)
// behaves exactly as before.
func defaultEngineOptions() EngineOptions {
	d := wavesdb.DefaultColumnFamilyOptions()
	return EngineOptions{
		WriteBufferSize:     d.WriteBufferSize,
		LevelSizeRatio:      d.LevelSizeRatio,
		MinLevels:           d.MinLevels,
		KlogValueThreshold:  d.KlogValueThreshold,
		Compression:         "none",
		EnableBloomFilter:   d.EnableBloomFilter,
		BloomFPR:            d.BloomFPR,
		EnableBlockIndex:    d.EnableBlockIndex,
		IndexSampleRatio:    d.IndexSampleRatio,
		BlockIndexPrefixLen: d.BlockIndexPrefixLen,
		SyncMode:            "none",
		SyncInterval:        d.SyncInterval,
		SkipListMaxLevel:    d.SkipListMaxLevel,
		SkipListProbability: d.SkipListProbability,
		DefaultIsolation:    "snapshot",
		L1FileCountTrigger:  d.L1FileCountTrigger,
		L0StallThreshold:    d.L0StallThreshold,
		UseBTree:            d.UseBTree,
	}
}

func (s *WavesdbStore) handle(cf ColumnFamily) (*wavesdb.ColumnFamily, error) {
	h, ok := s.cfs[cf]
	if !ok {
		return nil, ErrUnknownColumnFamily
	}
	return h, nil
}

func (s *WavesdbStore) Put(cf ColumnFamily, key, value []byte) error {
	h, err := s.handle(cf)
	if err != nil {
		return err
	}
	return mapErr(s.db.Put(h, key, value, 0))
}

func (s *WavesdbStore) Get(cf ColumnFamily, key []byte) ([]byte, bool, error) {
	h, err := s.handle(cf)
	if err != nil {
		return nil, false, err
	}
	v, err := s.db.Get(h, key)
	if errors.Is(err, wavesdb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, mapErr(err)
	}
	return v, true, nil
}

func (s *WavesdbStore) Delete(cf ColumnFamily, key []byte) error {
	h, err := s.handle(cf)
	if err != nil {
		return err
	}
	return mapErr(s.db.Delete(h, key))
}

func (s *WavesdbStore) Batch(ops []StoreOp) error {
	return s.batch(ops, wavesdb.Snapshot)
}

// BatchRC applies ops at ReadCommitted isolation: it skips wavesdb's write-write conflict check,
// which takes a global write lock and serializes ALL commits on a node. The recordstore guarantees
// its own per-key ordering (a striped lock around the latest-pointer read-modify-write), so
// independent keys can commit fully in parallel — the write-throughput unlock (design/05).
func (s *WavesdbStore) BatchRC(ops []StoreOp) error {
	return s.batch(ops, wavesdb.ReadCommitted)
}

func (s *WavesdbStore) batch(ops []StoreOp, isolation wavesdb.IsolationLevel) error {
	for _, op := range ops {
		if !op.CF.valid() {
			return ErrUnknownColumnFamily
		}
	}
	txn, err := s.db.BeginWithIsolation(isolation)
	if err != nil {
		return mapErr(err)
	}
	for _, op := range ops {
		h := s.cfs[op.CF]
		if op.Delete {
			err = txn.Delete(h, op.Key)
		} else {
			err = txn.Put(h, op.Key, op.Value, ttlForExpiry(op.ExpiresAtUnixMs))
		}
		if err != nil {
			_ = txn.Rollback()
			return mapErr(err)
		}
	}
	return mapErr(txn.Commit())
}

func (s *WavesdbStore) Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error) {
	h, err := s.handle(cf)
	if err != nil {
		return nil, err
	}
	txn, err := s.db.BeginWithIsolation(wavesdb.Snapshot)
	if err != nil {
		return nil, mapErr(err)
	}
	it := txn.NewIterator(h)
	return newWavesdbIterator(txn, it, start, end, limit), nil
}

func (s *WavesdbStore) Snapshot() (Snapshot, error) {
	txn, err := s.db.BeginWithIsolation(wavesdb.Snapshot)
	if err != nil {
		return nil, mapErr(err)
	}
	return &wavesdbSnapshot{store: s, txn: txn}, nil
}

func (s *WavesdbStore) Flush(cf ColumnFamily) error {
	h, err := s.handle(cf)
	if err != nil {
		return err
	}
	return mapErr(s.db.FlushMemtable(h))
}

// CompactRange compacts the whole column family; wavesdb does not expose range compaction,
// so the [start, end) bounds are advisory (design/02 / TS-011 "compaction hook").
func (s *WavesdbStore) CompactRange(cf ColumnFamily, _, _ []byte) error {
	h, err := s.handle(cf)
	if err != nil {
		return err
	}
	return mapErr(s.db.Compact(h))
}

func (s *WavesdbStore) Close() error { return mapErr(s.db.Close()) }

// UnderlyingDB exposes the wavesdb engine handle for capabilities the LocalStore interface does not
// surface — notably physical-plane backups (CheckpointToObjectStore / SSTablesSince), which operate on
// SSTables below the KV abstraction. The backup layer type-asserts for this accessor and gates the
// physical plane on it (a store without it — e.g. MemStore — cannot take a physical backup). This is a
// deliberate, narrow break in the design/17 "only storage imports wavesdb" rule: it hands back the same
// engine handle rather than widening the LocalStore interface for every caller.
func (s *WavesdbStore) UnderlyingDB() *wavesdb.DB { return s.db }

// wavesdbSnapshot is a read-consistent view over a pinned snapshot transaction.
type wavesdbSnapshot struct {
	store *WavesdbStore
	txn   *wavesdb.Txn
}

func (sn *wavesdbSnapshot) Get(cf ColumnFamily, key []byte) ([]byte, bool, error) {
	h, err := sn.store.handle(cf)
	if err != nil {
		return nil, false, err
	}
	v, err := sn.txn.Get(h, key)
	if errors.Is(err, wavesdb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, mapErr(err)
	}
	return v, true, nil
}

// Scan returns a snapshot-consistent iterator. Writes committed after the snapshot are not
// observed (wavesdb commit 1ca7892 fixed a snapshot-iterator off-by-one that previously
// leaked the immediate-next write into read-only snapshot scans).
func (sn *wavesdbSnapshot) Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error) {
	h, err := sn.store.handle(cf)
	if err != nil {
		return nil, err
	}
	it := sn.txn.NewIterator(h)
	// nil txn: the iterator must not roll back the shared snapshot transaction on Close.
	return newWavesdbIterator(nil, it, start, end, limit), nil
}

func (sn *wavesdbSnapshot) Close() error { return mapErr(sn.txn.Rollback()) }

// wavesdbIterator bounds a wavesdb iterator by an exclusive end key and a row limit. When
// txn is non-nil it owns that transaction and rolls it back on Close.
type wavesdbIterator struct {
	txn   *wavesdb.Txn
	it    *wavesdb.Iterator
	end   []byte
	limit int
	count int
	valid bool
}

func newWavesdbIterator(txn *wavesdb.Txn, it *wavesdb.Iterator, start, end []byte, limit int) *wavesdbIterator {
	w := &wavesdbIterator{txn: txn, it: it, end: end, limit: limit}
	if start == nil {
		it.SeekToFirst()
	} else {
		it.Seek(start)
	}
	w.sync()
	return w
}

func (w *wavesdbIterator) sync() {
	switch {
	case !w.it.Valid():
		w.valid = false
	case w.limit > 0 && w.count >= w.limit:
		w.valid = false
	case w.end != nil && bytes.Compare(w.it.Key(), w.end) >= 0:
		w.valid = false
	default:
		w.valid = true
	}
}

func (w *wavesdbIterator) Valid() bool { return w.valid }

func (w *wavesdbIterator) Next() {
	if !w.valid {
		return
	}
	w.count++
	w.it.Next()
	w.sync()
}

func (w *wavesdbIterator) Key() []byte {
	if !w.valid {
		return nil
	}
	return w.it.Key()
}

func (w *wavesdbIterator) Value() []byte {
	if !w.valid {
		return nil
	}
	return w.it.Value()
}

func (w *wavesdbIterator) Err() error { return w.it.Err() }

func (w *wavesdbIterator) Close() error {
	w.it.Close()
	if w.txn != nil {
		return mapErr(w.txn.Rollback())
	}
	return nil
}

// mapErr translates wavesdb sentinel errors into storage errors.
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, wavesdb.ErrConflict):
		return ErrConflict
	case errors.Is(err, wavesdb.ErrClosed):
		return ErrClosed
	default:
		return err
	}
}
