package vector

import (
	"encoding/binary"

	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// key prefixes: raw vectors in CFVectorRaw ("vr"); meta + graph-attachment index in CFVectorIndex.
const (
	pfxRaw    = "vr"
	pfxMeta   = "vm"
	pfxAttach = "va"
)

func lp(dst []byte, s string) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(s)))
	dst = append(dst, tmp[:n]...)
	return append(dst, s...)
}

func decodeLP(b []byte) (string, []byte) {
	n, sz := binary.Uvarint(b)
	// Unsigned compare (no lossy int() cast): a length in (2^63, 2^64) would go
	// negative as an int and slip past a signed guard, panicking the slice below.
	if sz <= 0 || n > uint64(len(b)-sz) {
		return "", nil
	}
	return string(b[sz : sz+int(n)]), b[sz+int(n):]
}

func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func rawKey(collection, vectorID string) []byte {
	return lp(lp([]byte(pfxRaw), collection), vectorID)
}

func metaKey(collection, vectorID string) []byte {
	return lp(lp([]byte(pfxMeta), collection), vectorID)
}

func attachKey(nodeID, collection, vectorID string) []byte {
	return lp(lp(lp([]byte(pfxAttach), nodeID), collection), vectorID)
}

func rawPrefix(collection string) []byte { return lp([]byte(pfxRaw), collection) }
func attachPrefix(nodeID string) []byte  { return lp([]byte(pfxAttach), nodeID) }

// Store persists raw vectors in wavesdb (design/08 "Vector storage"). Records resolve record-level
// last-write-wins; tombstoned/losing records are filtered at read time (winner-only).
type Store struct {
	local storage.LocalStore
}

// NewStore wires a vector store over a local store.
func NewStore(local storage.LocalStore) *Store { return &Store{local: local} }

// Put writes a vector record, its meta entry, and (if attached) a graph-node index entry, atomically.
func (s *Store) Put(v *wavespanv1.VectorRecord) error {
	enc, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	ops := []storage.StoreOp{
		{CF: storage.CFVectorRaw, Key: rawKey(v.GetCollection(), v.GetVectorId()), Value: enc},
	}
	if !v.GetTombstone() {
		metaEnc, _ := proto.Marshal(&wavespanv1.VectorMeta{
			Collection: v.GetCollection(), VectorId: v.GetVectorId(), Dimensions: v.GetDimensions(), GraphNodeId: v.GetGraphNodeId(),
		})
		ops = append(ops, storage.StoreOp{CF: storage.CFVectorIndex, Key: metaKey(v.GetCollection(), v.GetVectorId()), Value: metaEnc})
		if v.GetGraphNodeId() != "" {
			ops = append(ops, storage.StoreOp{CF: storage.CFVectorIndex, Key: attachKey(v.GetGraphNodeId(), v.GetCollection(), v.GetVectorId()), Value: []byte(v.GetVectorId())})
		}
	}
	return s.local.Batch(ops)
}

// Get returns a live vector by (collection, vector_id); nil,false if absent or tombstoned.
func (s *Store) Get(collection, vectorID string) (*wavespanv1.VectorRecord, bool, error) {
	b, found, err := s.local.Get(storage.CFVectorRaw, rawKey(collection, vectorID))
	if err != nil || !found {
		return nil, false, err
	}
	v := &wavespanv1.VectorRecord{}
	if err := proto.Unmarshal(b, v); err != nil || v.GetTombstone() {
		return nil, false, err
	}
	return v, true, nil
}

// Delete writes a tombstone for a vector.
func (s *Store) Delete(collection, vectorID string, version *wavespanv1.Version) error {
	return s.Put(&wavespanv1.VectorRecord{Collection: collection, VectorId: vectorID, Tombstone: true, Version: version})
}

// ScanCollection returns the live vector records in a collection (candidates for exact search).
func (s *Store) ScanCollection(collection string) ([]*wavespanv1.VectorRecord, error) {
	prefix := rawPrefix(collection)
	it, err := s.local.Scan(storage.CFVectorRaw, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []*wavespanv1.VectorRecord
	for it.Valid() {
		v := &wavespanv1.VectorRecord{}
		if proto.Unmarshal(it.Value(), v) == nil && !v.GetTombstone() {
			out = append(out, v)
		}
		it.Next()
	}
	return out, it.Err()
}

// GetByNode returns the live vectors attached to a graph node.
func (s *Store) GetByNode(nodeID string) ([]*wavespanv1.VectorRecord, error) {
	prefix := attachPrefix(nodeID)
	it, err := s.local.Scan(storage.CFVectorIndex, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	type ref struct{ collection, vectorID string }
	var refs []ref
	for it.Valid() {
		rest := it.Key()[len(prefix):] // lp(collection) || lp(vectorID)
		collection, rest2 := decodeLP(rest)
		vectorID, _ := decodeLP(rest2)
		refs = append(refs, ref{collection, vectorID})
		it.Next()
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	var out []*wavespanv1.VectorRecord
	for _, r := range refs {
		if v, found, _ := s.Get(r.collection, r.vectorID); found {
			out = append(out, v)
		}
	}
	return out, nil
}
