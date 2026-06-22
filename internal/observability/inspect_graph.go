package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/security"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

const graphExploreCap = 500

// WithGraph enables the visual node explorer (GraphExplore) over the local graph store.
func (s *ObsService) WithGraph(g *graph.Store) *ObsService {
	s.graph = g
	return s
}

// GraphExplore returns a bounded sub-graph for the visual node explorer (design/26): either the
// whole graph (capped) or a BFS expansion from a seed node up to depth hops. Property values are
// redacted unless include_value AND the caller is admin; node ids and labels are always returned.
func (s *ObsService) GraphExplore(ctx context.Context, req *connect.Request[wavespanv1.GraphExploreRequest]) (*connect.Response[wavespanv1.GraphExploreResponse], error) {
	if s.graph == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errGraphDisabled)
	}
	m := req.Msg
	graphID := m.GetGraphId()
	limit := int(m.GetLimit())
	maxCap := s.capFor("observability.graphExploreCap", graphExploreCap)
	if limit <= 0 || limit > maxCap {
		limit = maxCap
	}
	reveal := m.GetIncludeValue() && security.RoleFrom(ctx) == security.RoleAdmin

	visited := map[string]*wavespanv1.NodeRecord{}
	var order []string
	add := func(n *wavespanv1.NodeRecord) bool {
		if n == nil {
			return false
		}
		if _, ok := visited[n.GetNodeId()]; ok {
			return true
		}
		if len(visited) >= limit {
			return false
		}
		visited[n.GetNodeId()] = n
		order = append(order, n.GetNodeId())
		return true
	}

	edgeSet := map[string]*wavespanv1.GraphEdge{}
	addEdge := func(e *wavespanv1.EdgeRecord) {
		edgeSet[e.GetEdgeId()] = &wavespanv1.GraphEdge{
			EdgeId: e.GetEdgeId(), Source: e.GetStartNode(), Target: e.GetEndNode(), Type: e.GetType(),
		}
	}

	truncated := false
	if m.GetSeedNodeId() == "" {
		all, err := s.graph.AllNodes(graphID)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for _, n := range all {
			if !add(n) {
				truncated = true
				break
			}
		}
		// edges among the returned node set
		for id := range visited {
			s.collectEdges(graphID, id, visited, addEdge)
		}
	} else {
		if n, found, _ := s.graph.GetNode(graphID, m.GetSeedNodeId()); found {
			add(n)
		}
		frontier := []string{m.GetSeedNodeId()}
		depth := int(m.GetDepth())
		for d := 0; d < depth && len(visited) < limit; d++ {
			var next []string
			for _, id := range frontier {
				for _, nbID := range s.collectEdges(graphID, id, nil, addEdge) {
					if n, found, _ := s.graph.GetNode(graphID, nbID); found {
						if _, seen := visited[nbID]; !seen {
							if add(n) {
								next = append(next, nbID)
							} else {
								truncated = true
							}
						}
					}
				}
			}
			frontier = next
		}
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

// collectEdges records outgoing + incoming edges of node id and returns the neighbor ids. When
// withinSet is non-nil, only edges whose other endpoint is already in the set are recorded.
func (s *ObsService) collectEdges(graphID, id string, withinSet map[string]*wavespanv1.NodeRecord, addEdge func(*wavespanv1.EdgeRecord)) []string {
	var neighbors []string
	out, _ := s.graph.ScanOutgoing(graphID, id, "")
	for _, e := range out {
		if withinSet != nil {
			if _, ok := withinSet[e.GetEndNode()]; !ok {
				continue
			}
		}
		addEdge(e)
		neighbors = append(neighbors, e.GetEndNode())
	}
	in, _ := s.graph.ScanIncoming(graphID, id, "")
	for _, e := range in {
		if withinSet != nil {
			if _, ok := withinSet[e.GetStartNode()]; !ok {
				continue
			}
		}
		addEdge(e)
		neighbors = append(neighbors, e.GetStartNode())
	}
	return neighbors
}

type graphDisabledError struct{}

func (graphDisabledError) Error() string { return "observability: graph explorer not configured" }

var errGraphDisabled = graphDisabledError{}
