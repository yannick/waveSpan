package storage

import (
	"bytes"
	"errors"

	"wavesdb"
)

// WavesdbStore is the production LocalStore backed by the wavesdb LSM engine. It is the only
// package that imports wavesdb (design/17 dependency direction).
type WavesdbStore struct {
	db  *wavesdb.DB
	cfs map[ColumnFamily]*wavesdb.ColumnFamily
}

// OpenWavesdb opens (or creates) a wavesdb database at path and ensures all logical column
// families exist.
func OpenWavesdb(path string) (*WavesdbStore, error) {
	db, err := wavesdb.Open(wavesdb.Options{Path: path})
	if err != nil {
		return nil, err
	}
	s := &WavesdbStore{db: db, cfs: make(map[ColumnFamily]*wavesdb.ColumnFamily, len(allColumnFamilies))}
	for _, cf := range allColumnFamilies {
		h := db.GetColumnFamily(cf.Name())
		if h == nil {
			h, err = db.CreateColumnFamily(cf.Name(), wavesdb.DefaultColumnFamilyOptions())
			if err != nil {
				_ = db.Close()
				return nil, err
			}
		}
		s.cfs[cf] = h
	}
	return s, nil
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
			err = txn.Put(h, op.Key, op.Value, 0)
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
