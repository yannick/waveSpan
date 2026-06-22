package planner

import (
	"fmt"

	"github.com/cwire/wavespan/internal/cypher/parser"
	"github.com/cwire/wavespan/internal/graph"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func (e *Executor) version() *wavespanv1.Version {
	if e.NewVersion != nil {
		return e.NewVersion()
	}
	return &wavespanv1.Version{WriterClusterId: e.SelfCluster, WriterMemberId: e.SelfMember}
}

// create writes the CREATE patterns. Each binding row (from any preceding MATCH) produces its own
// nodes/edges; an already-bound variable is reused rather than recreated. The whole clause commits
// in a single atomic batch (design/07 "Graph mutation atomicity").
func (e *Executor) create(o *CreatePatterns, rows []bindingRow) error {
	if len(rows) == 0 {
		rows = []bindingRow{{}}
	}
	b := e.Store.NewBatch()
	seq := 0
	for _, row := range rows {
		for _, part := range o.Patterns {
			startID, err := e.emitNode(b, part.Node, row, &seq)
			if err != nil {
				return err
			}
			from := startID
			for i, rel := range part.Rels {
				endID, err := e.emitNode(b, part.Nodes[i], row, &seq)
				if err != nil {
					return err
				}
				typ := ""
				if len(rel.Types) > 0 {
					typ = rel.Types[0]
				}
				start, end := from, endID
				if rel.Direction == parser.DirIn {
					start, end = endID, from
				}
				edgeID := fmt.Sprintf("%s|%s|%s", start, typ, end)
				if err := b.PutEdge(&wavespanv1.EdgeRecord{
					GraphId: e.GraphID, EdgeId: edgeID, StartNode: start, EndNode: end, Type: typ,
					Properties: e.evalProps(rel.Properties, row), Version: e.version(),
				}); err != nil {
					return err
				}
				from = endID
			}
		}
	}
	return b.Commit(e.Store)
}

// emitNode reuses an already-bound node variable, otherwise creates a new node (id taken from the
// `id` property when present, else generated).
func (e *Executor) emitNode(b *graph.Batch, np parser.NodePattern, row bindingRow, seq *int) (string, error) {
	if np.Variable != "" {
		if bound, ok := row[np.Variable].(*wavespanv1.NodeRecord); ok {
			return bound.GetNodeId(), nil
		}
	}
	props := e.evalProps(np.Properties, row)
	id := ""
	if v, ok := props["id"]; ok {
		id = v.GetStringValue()
	}
	if id == "" {
		*seq++
		id = fmt.Sprintf("_n%d", *seq)
	}
	if err := b.PutNode(&wavespanv1.NodeRecord{
		GraphId: e.GraphID, NodeId: id, Labels: np.Labels, Properties: props, Version: e.version(),
	}); err != nil {
		return "", err
	}
	return id, nil
}

func (e *Executor) evalProps(props map[string]parser.Expr, row bindingRow) map[string]*wavespanv1.Value {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]*wavespanv1.Value, len(props))
	for k, ex := range props {
		out[k] = e.evalScalar(ex, row)
	}
	return out
}

// set updates the bound nodes' properties (record-level LWW, design/07). Distinct nodes are written
// once.
func (e *Executor) set(o *SetItems, rows []bindingRow) error {
	updated := map[string]*wavespanv1.NodeRecord{}
	for _, r := range rows {
		for _, item := range o.Items {
			n, ok := r[item.Variable].(*wavespanv1.NodeRecord)
			if !ok {
				continue
			}
			cur, found := updated[n.GetNodeId()]
			if !found {
				cur = cloneNode(n)
				updated[n.GetNodeId()] = cur
			}
			if cur.Properties == nil {
				cur.Properties = map[string]*wavespanv1.Value{}
			}
			cur.Properties[item.Property] = e.evalScalar(item.Value, r)
		}
	}
	b := e.Store.NewBatch()
	for _, n := range updated {
		n.Version = e.version()
		if err := b.PutNode(n); err != nil {
			return err
		}
	}
	return b.Commit(e.Store)
}

// del tombstones the bound nodes/edges (design/03 delete = tombstone write).
func (e *Executor) del(o *DeleteVars, rows []bindingRow) error {
	nodes := map[string]bool{}
	edges := map[string]bool{}
	for _, r := range rows {
		for _, v := range o.Variables {
			switch b := r[v].(type) {
			case *wavespanv1.NodeRecord:
				nodes[b.GetNodeId()] = true
			case *wavespanv1.EdgeRecord:
				edges[b.GetEdgeId()] = true
			}
		}
	}
	b := e.Store.NewBatch()
	for id := range nodes {
		if err := b.PutNode(&wavespanv1.NodeRecord{GraphId: e.GraphID, NodeId: id, Tombstone: true, Version: e.version()}); err != nil {
			return err
		}
	}
	for id := range edges {
		if err := b.PutEdge(&wavespanv1.EdgeRecord{GraphId: e.GraphID, EdgeId: id, Tombstone: true, Version: e.version()}); err != nil {
			return err
		}
	}
	return b.Commit(e.Store)
}

func cloneNode(n *wavespanv1.NodeRecord) *wavespanv1.NodeRecord {
	props := make(map[string]*wavespanv1.Value, len(n.GetProperties()))
	for k, v := range n.GetProperties() {
		props[k] = v
	}
	return &wavespanv1.NodeRecord{
		GraphId: n.GetGraphId(), NodeId: n.GetNodeId(), Labels: append([]string(nil), n.GetLabels()...),
		Properties: props,
	}
}
