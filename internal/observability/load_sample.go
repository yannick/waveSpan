package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/sampledata"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// LoadSampleDataset loads a small, open-licensed demo graph (sampledata.Movies) into graph_id so the
// node UI can be explored without first authoring data (design/26). It is admin-gated by the surface
// classifier and additionally gated by WAVESPAN_DISABLE_SAMPLE_DATASET: when disabled it returns
// ok=false with an explanatory error rather than a transport failure. The load is idempotent — the
// node/edge ids are stable, so re-running overwrites in place.
func (s *ObsService) LoadSampleDataset(_ context.Context, req *connect.Request[wavespanv1.LoadSampleDatasetRequest]) (*connect.Response[wavespanv1.LoadSampleDatasetResponse], error) {
	if s.graph == nil || s.newGraphVersion == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errGraphDisabled)
	}
	if !s.sampleEnabled {
		return connect.NewResponse(&wavespanv1.LoadSampleDatasetResponse{
			Ok:    false,
			Error: "sample dataset loading is disabled on this node (WAVESPAN_DISABLE_SAMPLE_DATASET)",
		}), nil
	}

	graphID := req.Msg.GetGraphId()
	if graphID == "" {
		graphID = "g"
	}

	ds := sampledata.Movies()
	b := s.graph.NewBatch()
	for _, n := range ds.Nodes {
		if err := b.PutNode(&wavespanv1.NodeRecord{
			GraphId:    graphID,
			NodeId:     n.ID,
			Labels:     n.Labels,
			Properties: n.Props,
			Version:    s.newGraphVersion(),
		}); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	for _, e := range ds.Edges {
		if err := b.PutEdge(&wavespanv1.EdgeRecord{
			GraphId:    graphID,
			EdgeId:     e.Start + "|" + e.Type + "|" + e.End, // matches the Cypher CREATE edge-id convention
			StartNode:  e.Start,
			EndNode:    e.End,
			Type:       e.Type,
			Properties: e.Props,
			Version:    s.newGraphVersion(),
		}); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	if err := b.Commit(s.graph); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&wavespanv1.LoadSampleDatasetResponse{
		Ok:           true,
		NodesCreated: uint32(len(ds.Nodes)),
		EdgesCreated: uint32(len(ds.Edges)),
		DatasetName:  ds.Name,
		License:      ds.License,
		Attribution:  ds.Attribution,
	}), nil
}
