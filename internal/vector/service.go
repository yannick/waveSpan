package vector

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Service is the VectorService Connect handler: it ingests raw vectors into the local store
// (design/08). Search is served via the Cypher vector.search* procedures.
type Service struct {
	store      *Store
	newVersion func() *wavespanv1.Version
	onWrite    func(*wavespanv1.VectorRecord)                            // update the local live index
	globalTap  func(ns string, key []byte, rec *wavespanv1.StoredRecord) // ship to peer clusters
	index      func(name string) (*IndexMeta, bool)                      // resolve an index (for SearchLocal)
	live       func(name string) (*LiveIndex, bool)                      // resolve the live ANN index

	// replicate routes a vector write through the KV origin+1 coordinator (intra-cluster replication +
	// cross-cluster tap); the holders' recordstore apply-observer feeds each HNSW. It receives the
	// collection + embedding so the node can place the write on the bucket's affinity ring. nil =
	// local-only (single-node tests). dims, when set, validates a Put against the declared dimensions.
	replicate func(ctx context.Context, ns string, key, value []byte, collection string, vec []float32) error
	dims      func(collection string) (int, bool)

	// Coordinator ops for the vector-as-key API (design/29): resolve a collection's index, scatter a
	// kNN search to the relevant holders (routed by probed buckets when nprobe>0; all holders
	// otherwise), read a record's bytes (for exact Get + neighbour payloads), and delete. The scatter
	// queries PEER holders only; the coordinator adds its own local fragment.
	collIndex func(collection string) (string, bool)
	scatter   func(ctx context.Context, collection, indexName string, query []float32, k, efSearch, nprobe int, rerank bool) (fragments [][]Hit, unreachable int)
	kvRead    func(ctx context.Context, ns string, key []byte) (value []byte, found bool, err error)
	kvDelete  func(ctx context.Context, ns string, key []byte) error
}

// WithCoordinator wires the cluster-wide vector-as-key operations (VectorGet/Delete/Search).
func (s *Service) WithCoordinator(
	collIndex func(collection string) (string, bool),
	scatter func(ctx context.Context, collection, indexName string, query []float32, k, efSearch, nprobe int, rerank bool) ([][]Hit, int),
	kvRead func(ctx context.Context, ns string, key []byte) ([]byte, bool, error),
	kvDelete func(ctx context.Context, ns string, key []byte) error,
) *Service {
	s.collIndex = collIndex
	s.scatter = scatter
	s.kvRead = kvRead
	s.kvDelete = kvDelete
	return s
}

// NewService wires the vector ingest service.
func NewService(store *Store, newVersion func() *wavespanv1.Version) *Service {
	return &Service{store: store, newVersion: newVersion}
}

// WithHooks installs the local-index updater and the global-replication tap (M10).
func (s *Service) WithHooks(onWrite func(*wavespanv1.VectorRecord), globalTap func(ns string, key []byte, rec *wavespanv1.StoredRecord)) *Service {
	s.onWrite = onWrite
	s.globalTap = globalTap
	return s
}

// WithSearch enables the SearchLocal RPC (the per-node fragment search a query coordinator scatters
// to holders, design/08).
func (s *Service) WithSearch(index func(name string) (*IndexMeta, bool), live func(name string) (*LiveIndex, bool)) *Service {
	s.index = index
	s.live = live
	return s
}

// WithReplication routes vector writes through the cluster (origin+1 + holders + cross-cluster tap) so
// a vector is searchable on every holder and survives reboots, instead of living only on the ingest
// node. dims validates Put requests against the collection's declared dimensionality.
func (s *Service) WithReplication(replicate func(ctx context.Context, ns string, key, value []byte, collection string, vec []float32) error, dims func(collection string) (int, bool)) *Service {
	s.replicate = replicate
	s.dims = dims
	return s
}

// SearchLocal searches only the vectors this node holds and returns its fragment of the top-k.
func (s *Service) SearchLocal(_ context.Context, req *connect.Request[wavespanv1.SearchLocalRequest]) (*connect.Response[wavespanv1.SearchLocalResponse], error) {
	if s.index == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, connectError("vector: search not configured"))
	}
	m := req.Msg
	meta, ok := s.index(m.GetIndexName())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, connectError("vector: unknown index "+m.GetIndexName()))
	}
	var live *LiveIndex
	if s.live != nil {
		live, _ = s.live(m.GetIndexName())
	}
	hits := LocalSearch(s.store, meta, live, m.GetQuery(), int(m.GetK()), int(m.GetEfSearch()), m.GetExact(), m.GetRerank())
	resp := &wavespanv1.SearchLocalResponse{Hits: make([]*wavespanv1.VectorHit, 0, len(hits))}
	for _, h := range hits {
		resp.Hits = append(resp.Hits, &wavespanv1.VectorHit{
			Collection: h.Collection, VectorId: h.VectorID, GraphNodeId: h.GraphNodeID, Distance: h.Distance, Score: h.Score,
		})
	}
	return connect.NewResponse(resp), nil
}

// Handler returns the mountable Connect handler for the data port.
func (s *Service) Handler() (string, http.Handler) {
	return wavespanv1connect.NewVectorServiceHandler(s, rpcopts.Handler()...)
}

// Put ingests a vector record (legacy ingest RPC). See putRecord for the shared write path.
func (s *Service) Put(ctx context.Context, req *connect.Request[wavespanv1.PutVectorRequest]) (*connect.Response[wavespanv1.PutVectorResponse], error) {
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errNoRecord)
	}
	if _, err := s.putRecord(ctx, rec); err != nil {
		return nil, err
	}
	return connect.NewResponse(&wavespanv1.PutVectorResponse{}), nil
}

// putRecord validates dimensions, derives the vector id from the embedding when none is supplied
// ("the vector is the key"), and — when replication is wired — routes the write through the cluster
// so every holder's HNSW is fed via the recordstore apply-observer and the vector survives reboot.
// Without replication (single-node tests) it falls back to a local write. Returns the record's version.
func (s *Service) putRecord(ctx context.Context, rec *wavespanv1.VectorRecord) (*wavespanv1.Version, error) {
	if s.dims != nil {
		if d, ok := s.dims(rec.GetCollection()); ok && len(rec.GetValues()) != d {
			return nil, connect.NewError(connect.CodeInvalidArgument, connectError("vector: dimension mismatch for collection "+rec.GetCollection()))
		}
	}
	rec.Dimensions = uint32(len(rec.GetValues()))
	if rec.GetVectorId() == "" {
		rec.VectorId = VecHash(rec.GetValues())
	}
	if rec.Version == nil && s.newVersion != nil {
		rec.Version = s.newVersion()
	}

	if s.replicate != nil {
		sr, werr := Wrap(rec)
		if werr != nil {
			return nil, connect.NewError(connect.CodeInternal, werr)
		}
		if err := s.replicate(ctx, sr.GetNamespace(), sr.GetLogicalKey(), sr.GetValue().GetInline(), rec.GetCollection(), rec.GetValues()); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		return rec.GetVersion(), nil
	}

	// Local-only fallback (no cluster wired): store + index this node only.
	if err := s.store.Put(rec); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if s.onWrite != nil {
		s.onWrite(rec)
	}
	if s.globalTap != nil {
		if sr, werr := Wrap(rec); werr == nil {
			s.globalTap(sr.GetNamespace(), sr.GetLogicalKey(), sr)
		}
	}
	return rec.GetVersion(), nil
}

var errNoRecord = connectError("vector: put requires a record")

type connectError string

func (e connectError) Error() string { return string(e) }
