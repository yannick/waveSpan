package graph

import (
	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Store persists property-graph nodes and edges in wavesdb. Node/edge records are authoritative in
// CFGraphData; label, property, and adjacency entries are derived in CFGraphIndex. Mutation batches
// commit atomically in a single underlying Txn (design/07 "Graph mutation atomicity").
type Store struct {
	local storage.LocalStore
}

// NewStore wires a graph store over a local store.
func NewStore(local storage.LocalStore) *Store { return &Store{local: local} }

// Batch accumulates graph mutations that commit atomically together (one coordinator Txn).
type Batch struct {
	ops []storage.StoreOp
}

// NewBatch starts a mutation batch.
func (s *Store) NewBatch() *Batch { return &Batch{} }

// PutNode writes a node record plus its label and property index entries.
func (b *Batch) PutNode(n *wavespanv1.NodeRecord) error {
	rec, err := EncodeNode(n)
	if err != nil {
		return err
	}
	b.ops = append(b.ops, storage.StoreOp{CF: storage.CFGraphData, Key: NodeKey(n.GetGraphId(), n.GetNodeId()), Value: rec})
	if n.GetTombstone() {
		return nil // index entries left stale; filtered at read time (design/07)
	}
	for _, label := range n.GetLabels() {
		b.ops = append(b.ops, storage.StoreOp{CF: storage.CFGraphIndex, Key: LabelKey(n.GetGraphId(), label, n.GetNodeId())})
		for prop, val := range n.GetProperties() {
			b.ops = append(b.ops, storage.StoreOp{CF: storage.CFGraphIndex, Key: PropKey(n.GetGraphId(), label, prop, val, n.GetNodeId())})
		}
	}
	return nil
}

// PutEdge writes an edge record plus its out/in adjacency entries (value = edge id).
func (b *Batch) PutEdge(e *wavespanv1.EdgeRecord) error {
	rec, err := EncodeEdge(e)
	if err != nil {
		return err
	}
	b.ops = append(b.ops, storage.StoreOp{CF: storage.CFGraphData, Key: EdgeKey(e.GetGraphId(), e.GetEdgeId()), Value: rec})
	if e.GetTombstone() {
		return nil
	}
	id := []byte(e.GetEdgeId())
	b.ops = append(b.ops,
		storage.StoreOp{CF: storage.CFGraphIndex, Key: OutAdjKey(e.GetGraphId(), e.GetStartNode(), e.GetType(), e.GetEndNode(), e.GetEdgeId()), Value: id},
		storage.StoreOp{CF: storage.CFGraphIndex, Key: InAdjKey(e.GetGraphId(), e.GetEndNode(), e.GetType(), e.GetStartNode(), e.GetEdgeId()), Value: id},
	)
	return nil
}

// Commit applies the whole batch atomically.
func (b *Batch) Commit(s *Store) error { return s.local.Batch(b.ops) }

// Len reports the number of pending storage ops (for tests/guardrails).
func (b *Batch) Len() int { return len(b.ops) }

// CreateNode writes a single node atomically.
func (s *Store) CreateNode(n *wavespanv1.NodeRecord) error {
	b := s.NewBatch()
	if err := b.PutNode(n); err != nil {
		return err
	}
	return b.Commit(s)
}

// CreateEdge writes a single edge atomically.
func (s *Store) CreateEdge(e *wavespanv1.EdgeRecord) error {
	b := s.NewBatch()
	if err := b.PutEdge(e); err != nil {
		return err
	}
	return b.Commit(s)
}

// GetNode returns a live node by id (nil, false on absent or tombstoned).
func (s *Store) GetNode(graph, nodeID string) (*wavespanv1.NodeRecord, bool, error) {
	v, found, err := s.local.Get(storage.CFGraphData, NodeKey(graph, nodeID))
	if err != nil || !found {
		return nil, false, err
	}
	n, err := DecodeNode(v)
	if err != nil || n.GetTombstone() {
		return nil, false, err
	}
	return n, true, nil
}

// GetEdge returns a live edge by id.
func (s *Store) GetEdge(graph, edgeID string) (*wavespanv1.EdgeRecord, bool, error) {
	v, found, err := s.local.Get(storage.CFGraphData, EdgeKey(graph, edgeID))
	if err != nil || !found {
		return nil, false, err
	}
	e, err := DecodeEdge(v)
	if err != nil || e.GetTombstone() {
		return nil, false, err
	}
	return e, true, nil
}

// ScanOutgoing returns live outgoing edges from src, optionally filtered to edgeType ("" = any).
func (s *Store) ScanOutgoing(graph, src, edgeType string) ([]*wavespanv1.EdgeRecord, error) {
	prefix := OutAdjPrefix(graph, src)
	if edgeType != "" {
		prefix = append(prefix, lp(nil, edgeType)...)
	}
	return s.scanAdjacency(graph, prefix)
}

// ScanIncoming returns live incoming edges to dst, optionally filtered to edgeType.
func (s *Store) ScanIncoming(graph, dst, edgeType string) ([]*wavespanv1.EdgeRecord, error) {
	prefix := InAdjPrefix(graph, dst)
	if edgeType != "" {
		prefix = append(prefix, lp(nil, edgeType)...)
	}
	return s.scanAdjacency(graph, prefix)
}

func (s *Store) scanAdjacency(graph string, prefix []byte) ([]*wavespanv1.EdgeRecord, error) {
	it, err := s.local.Scan(storage.CFGraphIndex, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []*wavespanv1.EdgeRecord
	for it.Valid() {
		edgeID := string(it.Value())
		it.Next()
		// resolve the authoritative edge record; skip stale adjacency entries (design/07 filtering)
		if e, found, gerr := s.GetEdge(graph, edgeID); gerr == nil && found {
			out = append(out, e)
		}
	}
	return out, it.Err()
}
