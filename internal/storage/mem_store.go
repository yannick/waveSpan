package storage

import (
	"bytes"
	"sort"
	"sync"
)

// MemStore is an in-memory LocalStore used by unit tests. It keeps an ordered view per
// column family and provides atomic Batch and read-consistent Snapshot semantics with the
// same contract as the wavesdb-backed store.
type MemStore struct {
	mu     sync.RWMutex
	cfs    map[ColumnFamily]map[string][]byte
	closed bool
}

// NewMemStore returns an empty in-memory store with all column families initialised.
func NewMemStore() *MemStore {
	cfs := make(map[ColumnFamily]map[string][]byte, len(allColumnFamilies))
	for _, cf := range allColumnFamilies {
		cfs[cf] = make(map[string][]byte)
	}
	return &MemStore{cfs: cfs}
}

func (s *MemStore) Put(cf ColumnFamily, key, value []byte) error {
	return s.Batch([]StoreOp{{CF: cf, Key: key, Value: value}})
}

func (s *MemStore) Delete(cf ColumnFamily, key []byte) error {
	return s.Batch([]StoreOp{{CF: cf, Key: key, Delete: true}})
}

func (s *MemStore) Get(cf ColumnFamily, key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, ErrClosed
	}
	if !cf.valid() {
		return nil, false, ErrUnknownColumnFamily
	}
	v, ok := s.cfs[cf][string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (s *MemStore) Batch(ops []StoreOp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	// Validate everything before mutating: Batch is all-or-nothing.
	for _, op := range ops {
		if !op.CF.valid() {
			return ErrUnknownColumnFamily
		}
	}
	for _, op := range ops {
		m := s.cfs[op.CF]
		if op.Delete {
			delete(m, string(op.Key))
			continue
		}
		m[string(op.Key)] = append([]byte(nil), op.Value...)
	}
	return nil
}

// BatchRC is identical to Batch for the in-memory store (isolation is moot without an LSM engine).
func (s *MemStore) BatchRC(ops []StoreOp) error { return s.Batch(ops) }

func (s *MemStore) Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return newSliceIterator(s.cfs[cf], start, end, limit), nil
}

func (s *MemStore) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	// Copy-on-snapshot: a frozen view unaffected by later writes.
	cfs := make(map[ColumnFamily]map[string][]byte, len(s.cfs))
	for cf, m := range s.cfs {
		c := make(map[string][]byte, len(m))
		for k, v := range m {
			c[k] = append([]byte(nil), v...)
		}
		cfs[cf] = c
	}
	return &memSnapshot{cfs: cfs}, nil
}

func (s *MemStore) Flush(ColumnFamily) error                        { return nil }
func (s *MemStore) CompactRange(ColumnFamily, []byte, []byte) error { return nil }

func (s *MemStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

type memSnapshot struct {
	cfs map[ColumnFamily]map[string][]byte
}

func (snap *memSnapshot) Get(cf ColumnFamily, key []byte) ([]byte, bool, error) {
	if !cf.valid() {
		return nil, false, ErrUnknownColumnFamily
	}
	v, ok := snap.cfs[cf][string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (snap *memSnapshot) Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error) {
	return newSliceIterator(snap.cfs[cf], start, end, limit), nil
}

func (snap *memSnapshot) Close() error { return nil }

// sliceIterator is a forward cursor over a materialised, sorted slice of KV pairs.
type sliceIterator struct {
	rows []StoreKV
	idx  int
}

func newSliceIterator(m map[string][]byte, start, end []byte, limit int) *sliceIterator {
	keys := make([]string, 0, len(m))
	for k := range m {
		kb := []byte(k)
		if start != nil && bytes.Compare(kb, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(kb, end) >= 0 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	rows := make([]StoreKV, len(keys))
	for i, k := range keys {
		rows[i] = StoreKV{Key: []byte(k), Value: append([]byte(nil), m[k]...)}
	}
	return &sliceIterator{rows: rows}
}

func (it *sliceIterator) Valid() bool { return it.idx < len(it.rows) }
func (it *sliceIterator) Next()       { it.idx++ }
func (it *sliceIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.rows[it.idx].Key
}
func (it *sliceIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.rows[it.idx].Value
}
func (it *sliceIterator) Err() error   { return nil }
func (it *sliceIterator) Close() error { return nil }
