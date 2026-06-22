// Package cypher exposes the Cypher query service: it parses a query, plans and executes it against
// the local graph store, and streams rows followed by QueryMeta (design/07).
package cypher

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/cypher/parser"
	"github.com/cwire/wavespan/internal/cypher/planner"
	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/vector"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Service is the Cypher Connect handler over the local graph store, with optional vector search.
type Service struct {
	store       *graph.Store
	clusterID   string
	memberID    string
	newVersion  func() *wavespanv1.Version
	vectorStore *vector.Store
	vectorIndex func(name string) (*vector.IndexMeta, bool)
	vectorLive  func(name string) (*vector.LiveIndex, bool)
}

// NewService wires the Cypher service. newVersion supplies an HLC version for graph mutations
// (shared with the node's clock).
func NewService(store *graph.Store, clusterID, memberID string, newVersion func() *wavespanv1.Version) *Service {
	return &Service{store: store, clusterID: clusterID, memberID: memberID, newVersion: newVersion}
}

// WithVector enables vector.searchExact (M9) and vector.searchApprox (M10) over the given vector
// store, index-metadata resolver, and live-index resolver.
func (s *Service) WithVector(vstore *vector.Store, index func(name string) (*vector.IndexMeta, bool), live func(name string) (*vector.LiveIndex, bool)) *Service {
	s.vectorStore = vstore
	s.vectorIndex = index
	s.vectorLive = live
	return s
}

// Handler returns the mountable Connect handler for the data port.
func (s *Service) Handler() (string, http.Handler) {
	return wavespanv1connect.NewCypherHandler(s)
}

// Query parses, plans, executes, and streams the result of a Cypher query.
func (s *Service) Query(_ context.Context, req *connect.Request[wavespanv1.CypherRequest], stream *connect.ServerStream[wavespanv1.CypherResult]) error {
	ast, err := parser.Parse(req.Msg.GetQuery())
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	exec := &planner.Executor{
		Store: s.store, GraphID: req.Msg.GetGraphId(), Limits: planner.DefaultLimits(),
		Router: planner.LocalRouter{Self: s.memberID}, SelfCluster: s.clusterID, SelfMember: s.memberID,
		Params: req.Msg.GetParameters(), NewVersion: s.newVersion,
		VectorStore: s.vectorStore, VectorIndex: s.vectorIndex, VectorLive: s.vectorLive,
	}
	res, err := exec.Execute(ast)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	for _, row := range res.Rows {
		if err := stream.Send(&wavespanv1.CypherResult{Msg: &wavespanv1.CypherResult_Row{Row: &wavespanv1.CypherRow{Columns: row}}}); err != nil {
			return err
		}
	}
	return stream.Send(&wavespanv1.CypherResult{Msg: &wavespanv1.CypherResult_Meta{Meta: res.Meta}})
}
