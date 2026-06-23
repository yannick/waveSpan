package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/security"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// GraphSubgraph resolves the sub-graph induced by an explicit set of node ids (design/26). It backs
// "render Cypher results in the explorer": a Cypher RETURN yields node ids as strings, the UI
// collects them, and this returns the live nodes plus the edges among them. With neighbor_depth>0 it
// also pulls that many hops of neighbours. Ids that don't resolve to a live node are ignored.
// Property values are redacted unless include_value AND the caller is admin.
func (s *ObsService) GraphSubgraph(ctx context.Context, req *connect.Request[wavespanv1.GraphSubgraphRequest]) (*connect.Response[wavespanv1.GraphExploreResponse], error) {
	if s.graph == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errGraphDisabled)
	}
	m := req.Msg
	graphID := m.GetGraphId()
	maxCap := s.capFor("observability.graphExploreCap", graphExploreCap)
	reveal := m.GetIncludeValue() && security.RoleFrom(ctx) == security.RoleAdmin

	visited := map[string]*wavespanv1.NodeRecord{}
	var order []string
	truncated := false
	add := func(n *wavespanv1.NodeRecord) bool {
		if n == nil {
			return false
		}
		if _, ok := visited[n.GetNodeId()]; ok {
			return true
		}
		if len(visited) >= maxCap {
			truncated = true
			return false
		}
		visited[n.GetNodeId()] = n
		order = append(order, n.GetNodeId())
		return true
	}

	// Seed with the requested ids that resolve to a live node.
	var frontier []string
	for _, id := range m.GetNodeIds() {
		if _, ok := visited[id]; ok {
			continue
		}
		if n, found, _ := s.graph.GetNode(graphID, id); found {
			if add(n) {
				frontier = append(frontier, id)
			}
		}
	}

	// Optional neighbour expansion (BFS). collectEdges is used only for neighbour discovery here; the
	// returned edges are recomputed below so every edge endpoint is guaranteed to be in the node set.
	discard := func(*wavespanv1.EdgeRecord) {}
	depth := int(m.GetNeighborDepth())
	for d := 0; d < depth && len(visited) < maxCap; d++ {
		var next []string
		for _, id := range frontier {
			for _, nbID := range s.collectEdges(graphID, id, nil, discard) {
				if _, seen := visited[nbID]; seen {
					continue
				}
				if n, found, _ := s.graph.GetNode(graphID, nbID); found {
					if add(n) {
						next = append(next, nbID)
					}
				}
			}
		}
		frontier = next
	}

	// Edges among the final node set (covers neighbor_depth==0 and intra-set links).
	edgeSet := map[string]*wavespanv1.GraphEdge{}
	addEdge := func(e *wavespanv1.EdgeRecord) {
		edgeSet[e.GetEdgeId()] = &wavespanv1.GraphEdge{
			EdgeId: e.GetEdgeId(), Source: e.GetStartNode(), Target: e.GetEndNode(), Type: e.GetType(),
		}
	}
	for id := range visited {
		s.collectEdges(graphID, id, visited, addEdge)
	}

	resp := &wavespanv1.GraphExploreResponse{Truncated: truncated}
	for _, id := range order {
		n := visited[id]
		gn := &wavespanv1.GraphNode{NodeId: id, Labels: n.GetLabels()}
		if reveal {
			gn.Properties = n.GetProperties()
		}
		resp.Nodes = append(resp.Nodes, gn)
	}
	for _, e := range edgeSet {
		resp.Edges = append(resp.Edges, e)
	}
	return connect.NewResponse(resp), nil
}
