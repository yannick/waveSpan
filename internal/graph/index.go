package graph

import (
	"github.com/cwire/wavespan/internal/storage"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// ScanLabel returns the ids of live nodes carrying a label. Index hits are filtered against the
// current node record (a stale/tombstoned entry is excluded — design/07 "indexes are derived").
func (s *Store) ScanLabel(graph, label string) ([]string, error) {
	prefix := LabelPrefix(graph, label)
	it, err := s.local.Scan(storage.CFGraphIndex, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	seen := map[string]bool{}
	var out []string
	for it.Valid() {
		nodeID, _ := decodeLP(it.Key()[len(prefix):])
		it.Next()
		if seen[nodeID] {
			continue
		}
		if n, found, _ := s.GetNode(graph, nodeID); found && hasLabel(n, label) {
			seen[nodeID] = true
			out = append(out, nodeID)
		}
	}
	return out, it.Err()
}

// SeekProperty returns the ids of live nodes whose (label, prop) equals val.
func (s *Store) SeekProperty(graph, label, prop string, val *wavespanv1.Value) ([]string, error) {
	prefix := propValuePrefix(graph, label, prop, val)
	return s.seekProp(graph, label, prop, prefix, prefixEnd(prefix), len(prefix), val, nil)
}

// SeekPropertyGTE returns live nodes whose integer (label, prop) is >= from (range seek).
func (s *Store) SeekPropertyGTE(graph, label, prop string, from int64) ([]string, error) {
	fromVal := &wavespanv1.Value{Value: &wavespanv1.Value_IntValue{IntValue: from}}
	start := propValuePrefix(graph, label, prop, fromVal) // includes the value bytes
	typePrefix := PropTypePrefix(graph, label, prop, tagInt)
	end := prefixEnd(typePrefix)
	nodeOffset := len(typePrefix) + 8 // tag + 8-byte int payload, then the node id
	minVal := from
	return s.seekProp(graph, label, prop, start, end, nodeOffset, nil, &minVal)
}

// seekProp scans the property index in [start, end), extracts the node id at nodeOffset, and keeps
// only live nodes whose current property still matches (equality eq, or integer >= gte).
func (s *Store) seekProp(graph, label, prop string, start, end []byte, nodeOffset int, eq *wavespanv1.Value, gte *int64) ([]string, error) {
	it, err := s.local.Scan(storage.CFGraphIndex, start, end, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	seen := map[string]bool{}
	var out []string
	for it.Valid() {
		k := it.Key()
		it.Next()
		if len(k) < nodeOffset {
			continue
		}
		nodeID := string(k[nodeOffset:])
		if seen[nodeID] {
			continue
		}
		n, found, _ := s.GetNode(graph, nodeID)
		if !found || !hasLabel(n, label) {
			continue
		}
		cur, ok := n.GetProperties()[prop]
		if !ok || !propMatches(cur, eq, gte) {
			continue // stale index entry: current value no longer matches
		}
		seen[nodeID] = true
		out = append(out, nodeID)
	}
	return out, it.Err()
}

func propMatches(cur, eq *wavespanv1.Value, gte *int64) bool {
	if eq != nil {
		return valueEqual(cur, eq)
	}
	if gte != nil {
		iv, ok := cur.GetValue().(*wavespanv1.Value_IntValue)
		return ok && iv.IntValue >= *gte
	}
	return true
}

// RebuildIndexes wipes the graph's derived index entries and reconstructs them from authoritative
// records, proving indexes are derived (design/07).
func (s *Store) RebuildIndexes(graph string) error {
	if err := s.wipeIndex(); err != nil {
		return err
	}
	b := s.NewBatch()
	if err := s.forEachNode(graph, func(n *wavespanv1.NodeRecord) error {
		if n.GetTombstone() {
			return nil
		}
		for _, label := range n.GetLabels() {
			b.ops = append(b.ops, storage.StoreOp{CF: storage.CFGraphIndex, Key: LabelKey(graph, label, n.GetNodeId())})
			for prop, val := range n.GetProperties() {
				b.ops = append(b.ops, storage.StoreOp{CF: storage.CFGraphIndex, Key: PropKey(graph, label, prop, val, n.GetNodeId())})
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := s.forEachEdge(graph, func(e *wavespanv1.EdgeRecord) error {
		if e.GetTombstone() {
			return nil
		}
		id := []byte(e.GetEdgeId())
		b.ops = append(b.ops,
			storage.StoreOp{CF: storage.CFGraphIndex, Key: OutAdjKey(graph, e.GetStartNode(), e.GetType(), e.GetEndNode(), e.GetEdgeId()), Value: id},
			storage.StoreOp{CF: storage.CFGraphIndex, Key: InAdjKey(graph, e.GetEndNode(), e.GetType(), e.GetStartNode(), e.GetEdgeId()), Value: id},
		)
		return nil
	}); err != nil {
		return err
	}
	return b.Commit(s)
}

func (s *Store) wipeIndex() error {
	it, err := s.local.Scan(storage.CFGraphIndex, nil, nil, 0)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	var ops []storage.StoreOp
	for it.Valid() {
		ops = append(ops, storage.StoreOp{CF: storage.CFGraphIndex, Key: append([]byte(nil), it.Key()...), Delete: true})
		it.Next()
	}
	if err := it.Err(); err != nil {
		return err
	}
	return s.local.Batch(ops)
}

// AllNodes returns every live node record of a graph (used by the AllNodesScan operator).
func (s *Store) AllNodes(graph string) ([]*wavespanv1.NodeRecord, error) {
	var out []*wavespanv1.NodeRecord
	err := s.forEachNode(graph, func(n *wavespanv1.NodeRecord) error {
		if !n.GetTombstone() {
			out = append(out, n)
		}
		return nil
	})
	return out, err
}

func (s *Store) forEachNode(graph string, fn func(*wavespanv1.NodeRecord) error) error {
	prefix := NodePrefix(graph)
	it, err := s.local.Scan(storage.CFGraphData, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if n, derr := DecodeNode(it.Value()); derr == nil {
			if ferr := fn(n); ferr != nil {
				return ferr
			}
		}
		it.Next()
	}
	return it.Err()
}

func (s *Store) forEachEdge(graph string, fn func(*wavespanv1.EdgeRecord) error) error {
	prefix := EdgePrefix(graph)
	it, err := s.local.Scan(storage.CFGraphData, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if e, derr := DecodeEdge(it.Value()); derr == nil {
			if ferr := fn(e); ferr != nil {
				return ferr
			}
		}
		it.Next()
	}
	return it.Err()
}

func hasLabel(n *wavespanv1.NodeRecord, label string) bool {
	for _, l := range n.GetLabels() {
		if l == label {
			return true
		}
	}
	return false
}

func valueEqual(a, b *wavespanv1.Value) bool {
	switch av := a.GetValue().(type) {
	case *wavespanv1.Value_IntValue:
		bv, ok := b.GetValue().(*wavespanv1.Value_IntValue)
		return ok && av.IntValue == bv.IntValue
	case *wavespanv1.Value_StringValue:
		bv, ok := b.GetValue().(*wavespanv1.Value_StringValue)
		return ok && av.StringValue == bv.StringValue
	case *wavespanv1.Value_DoubleValue:
		bv, ok := b.GetValue().(*wavespanv1.Value_DoubleValue)
		return ok && av.DoubleValue == bv.DoubleValue
	case *wavespanv1.Value_BoolValue:
		bv, ok := b.GetValue().(*wavespanv1.Value_BoolValue)
		return ok && av.BoolValue == bv.BoolValue
	default:
		return false
	}
}
