package grpcsrv

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/cypher"
	"github.com/yannick/wavespan/internal/cypher/parser"
	"github.com/yannick/wavespan/internal/cypher/planner"
	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/vector"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Cypher is the gRPC Cypher adapter. It mirrors internal/cypher.Service: it parses, plans, and
// executes a query against the same local graph store (with optional vector/KV/collections built-ins)
// and streams rows followed by QueryMeta. Only the transport differs.
type Cypher struct {
	wavespanv1.UnimplementedCypherServer

	store         *graph.Store
	clusterID     string
	memberID      string
	newVersion    func() *wavespanv1.Version
	vectorStore   *vector.Store
	vectorIndex   func(name string) (*vector.IndexMeta, bool)
	vectorLive    func(name string) (*vector.LiveIndex, bool)
	vectorScatter cypher.ScatterFunc
	kv            planner.KVAccess
	collections   planner.CollectionsAccess
}

// NewCypher wires the gRPC Cypher adapter. Mirrors cypher.NewService.
func NewCypher(store *graph.Store, clusterID, memberID string, newVersion func() *wavespanv1.Version) *Cypher {
	return &Cypher{store: store, clusterID: clusterID, memberID: memberID, newVersion: newVersion}
}

// WithVector enables vector.search* built-ins. Mirrors cypher.Service.WithVector.
func (s *Cypher) WithVector(vstore *vector.Store, index func(name string) (*vector.IndexMeta, bool), live func(name string) (*vector.LiveIndex, bool)) *Cypher {
	s.vectorStore = vstore
	s.vectorIndex = index
	s.vectorLive = live
	return s
}

// WithVectorScatter makes vector search cluster-wide. Mirrors cypher.Service.WithVectorScatter.
func (s *Cypher) WithVectorScatter(scatter cypher.ScatterFunc) *Cypher {
	s.vectorScatter = scatter
	return s
}

// WithKV enables the kv.* built-ins. Mirrors cypher.Service.WithKV.
func (s *Cypher) WithKV(kv planner.KVAccess) *Cypher {
	s.kv = kv
	return s
}

// WithCollections enables the set.*/hash.*/zset.* built-ins. Mirrors cypher.Service.WithCollections.
func (s *Cypher) WithCollections(c planner.CollectionsAccess) *Cypher {
	s.collections = c
	return s
}

// Query parses, plans, executes, and streams the result of a Cypher query. It delegates to the same
// parser + planner.Executor cores; rows are sent on the gRPC stream followed by QueryMeta. The
// executor runs against stream.Context() so client cancellation propagates.
func (s *Cypher) Query(req *wavespanv1.CypherRequest, stream grpc.ServerStreamingServer[wavespanv1.CypherResult]) error {
	ast, err := parser.Parse(req.GetQuery())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	exec := &planner.Executor{
		Store: s.store, GraphID: req.GetGraphId(), Limits: planner.DefaultLimits(),
		Router: planner.LocalRouter{Self: s.memberID}, SelfCluster: s.clusterID, SelfMember: s.memberID,
		Params: req.GetParameters(), NewVersion: s.newVersion,
		VectorStore: s.vectorStore, VectorIndex: s.vectorIndex, VectorLive: s.vectorLive,
		Ctx: stream.Context(), VectorScatter: s.vectorScatter,
		KV: s.kv, Collections: s.collections,
	}
	res, err := exec.Execute(ast)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	for _, row := range res.Rows {
		if err := stream.Send(&wavespanv1.CypherResult{Msg: &wavespanv1.CypherResult_Row{Row: &wavespanv1.CypherRow{Columns: row}}}); err != nil {
			return err
		}
	}
	return stream.Send(&wavespanv1.CypherResult{Msg: &wavespanv1.CypherResult_Meta{Meta: res.Meta}})
}
