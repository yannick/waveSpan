package grpcsrv

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/yannick/wavespan/internal/vector"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Vector is the gRPC VectorService adapter. It mirrors internal/vector.Service, holding the same
// dependencies and reusing the same exported vector cores (Store, LocalSearch, MergeTopK, Wrap, …).
// Only the transport differs.
type Vector struct {
	wavespanv1.UnimplementedVectorServiceServer

	store      *vector.Store
	newVersion func() *wavespanv1.Version
	onWrite    func(*wavespanv1.VectorRecord)
	globalTap  func(ns string, key []byte, rec *wavespanv1.StoredRecord)
	index      func(name string) (*vector.IndexMeta, bool)
	live       func(name string) (*vector.LiveIndex, bool)

	replicate func(ctx context.Context, ns string, key, value []byte, collection string, vec []float32) error
	dims      func(collection string) (int, bool)

	collIndex func(collection string) (string, bool)
	scatter   func(ctx context.Context, collection, indexName string, query []float32, k, efSearch, nprobe int, rerank bool) (fragments [][]vector.Hit, unreachable int)
	kvRead    func(ctx context.Context, ns string, key []byte) (value []byte, found bool, err error)
	kvDelete  func(ctx context.Context, ns string, key []byte) error
}

// NewVector wires the gRPC vector ingest adapter. Mirrors vector.NewService.
func NewVector(store *vector.Store, newVersion func() *wavespanv1.Version) *Vector {
	return &Vector{store: store, newVersion: newVersion}
}

// WithHooks installs the local-index updater and the global-replication tap. Mirrors
// vector.Service.WithHooks.
func (s *Vector) WithHooks(onWrite func(*wavespanv1.VectorRecord), globalTap func(ns string, key []byte, rec *wavespanv1.StoredRecord)) *Vector {
	s.onWrite = onWrite
	s.globalTap = globalTap
	return s
}

// WithSearch enables the SearchLocal RPC. Mirrors vector.Service.WithSearch.
func (s *Vector) WithSearch(index func(name string) (*vector.IndexMeta, bool), live func(name string) (*vector.LiveIndex, bool)) *Vector {
	s.index = index
	s.live = live
	return s
}

// WithReplication routes vector writes through the cluster. Mirrors vector.Service.WithReplication.
func (s *Vector) WithReplication(replicate func(ctx context.Context, ns string, key, value []byte, collection string, vec []float32) error, dims func(collection string) (int, bool)) *Vector {
	s.replicate = replicate
	s.dims = dims
	return s
}

// WithCoordinator wires the cluster-wide vector-as-key operations. Mirrors
// vector.Service.WithCoordinator.
func (s *Vector) WithCoordinator(
	collIndex func(collection string) (string, bool),
	scatter func(ctx context.Context, collection, indexName string, query []float32, k, efSearch, nprobe int, rerank bool) ([][]vector.Hit, int),
	kvRead func(ctx context.Context, ns string, key []byte) ([]byte, bool, error),
	kvDelete func(ctx context.Context, ns string, key []byte) error,
) *Vector {
	s.collIndex = collIndex
	s.scatter = scatter
	s.kvRead = kvRead
	s.kvDelete = kvDelete
	return s
}

// putRecord mirrors vector.Service.putRecord, using the same exported cores (Wrap, Store.Put, VecHash).
func (s *Vector) putRecord(ctx context.Context, rec *wavespanv1.VectorRecord) (*wavespanv1.Version, error) {
	if s.dims != nil {
		if d, ok := s.dims(rec.GetCollection()); ok && len(rec.GetValues()) != d {
			return nil, status.Error(codes.InvalidArgument, "vector: dimension mismatch for collection "+rec.GetCollection())
		}
	}
	rec.Dimensions = uint32(len(rec.GetValues()))
	if rec.GetVectorId() == "" {
		rec.VectorId = vector.VecHash(rec.GetValues())
	}
	if rec.Version == nil && s.newVersion != nil {
		rec.Version = s.newVersion()
	}

	if s.replicate != nil {
		sr, werr := vector.Wrap(rec)
		if werr != nil {
			return nil, status.Error(codes.Internal, werr.Error())
		}
		if err := s.replicate(ctx, sr.GetNamespace(), sr.GetLogicalKey(), sr.GetValue().GetInline(), rec.GetCollection(), rec.GetValues()); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return rec.GetVersion(), nil
	}

	if err := s.store.Put(rec); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if s.onWrite != nil {
		s.onWrite(rec)
	}
	if s.globalTap != nil {
		if sr, werr := vector.Wrap(rec); werr == nil {
			s.globalTap(sr.GetNamespace(), sr.GetLogicalKey(), sr)
		}
	}
	return rec.GetVersion(), nil
}

// Put ingests a vector record (legacy ingest RPC).
func (s *Vector) Put(ctx context.Context, m *wavespanv1.PutVectorRequest) (*wavespanv1.PutVectorResponse, error) {
	rec := m.GetRecord()
	if rec == nil {
		return nil, status.Error(codes.InvalidArgument, "vector: put requires a record")
	}
	if _, err := s.putRecord(ctx, rec); err != nil {
		return nil, err
	}
	return &wavespanv1.PutVectorResponse{}, nil
}

// SearchLocal searches only the vectors this node holds and returns its fragment of the top-k.
func (s *Vector) SearchLocal(_ context.Context, m *wavespanv1.SearchLocalRequest) (*wavespanv1.SearchLocalResponse, error) {
	if s.index == nil {
		return nil, status.Error(codes.Unimplemented, "vector: search not configured")
	}
	meta, ok := s.index(m.GetIndexName())
	if !ok {
		return nil, status.Error(codes.NotFound, "vector: unknown index "+m.GetIndexName())
	}
	var live *vector.LiveIndex
	if s.live != nil {
		live, _ = s.live(m.GetIndexName())
	}
	hits := vector.LocalSearch(s.store, meta, live, m.GetQuery(), int(m.GetK()), int(m.GetEfSearch()), m.GetExact(), m.GetRerank())
	resp := &wavespanv1.SearchLocalResponse{Hits: make([]*wavespanv1.VectorHit, 0, len(hits))}
	for _, h := range hits {
		resp.Hits = append(resp.Hits, &wavespanv1.VectorHit{
			Collection: h.Collection, VectorId: h.VectorID, GraphNodeId: h.GraphNodeID, Distance: h.Distance, Score: h.Score,
		})
	}
	return resp, nil
}

// SampleVectors returns a reservoir sample of this node's local vectors for a collection.
func (s *Vector) SampleVectors(_ context.Context, m *wavespanv1.SampleVectorsReq) (*wavespanv1.SampleVectorsRes, error) {
	limit := int(m.GetLimit())
	if limit <= 0 {
		limit = 1000
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	sample := vector.ReservoirSample(s.store, m.GetCollection(), limit, rng)
	res := &wavespanv1.SampleVectorsRes{Vectors: make([]*wavespanv1.FloatVector, len(sample))}
	for i, v := range sample {
		res.Vectors[i] = &wavespanv1.FloatVector{Values: v}
	}
	return res, nil
}

// VectorPut stores an embedding (the key) with an opaque payload (the value), replicated to holders.
func (s *Vector) VectorPut(ctx context.Context, m *wavespanv1.VectorPutReq) (*wavespanv1.VectorPutRes, error) {
	rec := &wavespanv1.VectorRecord{Collection: m.GetCollection(), Values: m.GetVector(), Payload: m.GetPayload()}
	ver, err := s.putRecord(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &wavespanv1.VectorPutRes{Version: ver}, nil
}

// VectorGet returns the payload stored under an exact embedding, read via the KV path.
func (s *Vector) VectorGet(ctx context.Context, m *wavespanv1.VectorGetReq) (*wavespanv1.VectorGetRes, error) {
	if s.kvRead == nil {
		return nil, status.Error(codes.Unimplemented, "vector: cluster ops not configured")
	}
	id := vector.VecHash(m.GetVector())
	val, found, err := s.kvRead(ctx, vector.MutationNamespace(m.GetCollection()), []byte(id))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !found {
		return &wavespanv1.VectorGetRes{Found: false}, nil
	}
	v := &wavespanv1.VectorRecord{}
	if uerr := proto.Unmarshal(val, v); uerr != nil {
		return nil, status.Error(codes.Internal, uerr.Error())
	}
	return &wavespanv1.VectorGetRes{Found: !v.GetTombstone(), Payload: v.GetPayload()}, nil
}

// VectorDelete tombstones the record under an exact embedding.
func (s *Vector) VectorDelete(ctx context.Context, m *wavespanv1.VectorDeleteReq) (*wavespanv1.VectorDeleteRes, error) {
	if s.kvDelete == nil {
		return nil, status.Error(codes.Unimplemented, "vector: cluster ops not configured")
	}
	id := vector.VecHash(m.GetVector())
	if err := s.kvDelete(ctx, vector.MutationNamespace(m.GetCollection()), []byte(id)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wavespanv1.VectorDeleteRes{}, nil
}

// VectorSearch runs a cluster-wide kNN: it scatters SearchLocal to holders, merges the global top-k,
// and (optionally) attaches each neighbour's embedding + payload read via the KV path.
func (s *Vector) VectorSearch(ctx context.Context, m *wavespanv1.VectorSearchReq) (*wavespanv1.VectorSearchRes, error) {
	if s.scatter == nil || s.collIndex == nil {
		return nil, status.Error(codes.Unimplemented, "vector: cluster search not configured")
	}
	if s.dims != nil {
		if d, ok := s.dims(m.GetCollection()); ok && len(m.GetQuery()) != d {
			return nil, status.Error(codes.InvalidArgument, "vector: query dimension mismatch for collection "+m.GetCollection())
		}
	}
	idxName, ok := s.collIndex(m.GetCollection())
	if !ok {
		return nil, status.Error(codes.NotFound, "vector: no index for collection "+m.GetCollection())
	}
	k := int(m.GetK())
	if k <= 0 {
		k = 10
	}
	ef := int(m.GetEfSearch())
	if ef <= 0 {
		ef = 64
	}
	var fragments [][]vector.Hit
	if meta, ok := s.index(idxName); ok {
		var live *vector.LiveIndex
		if s.live != nil {
			live, _ = s.live(idxName)
		}
		if local := vector.LocalSearch(s.store, meta, live, m.GetQuery(), k, ef, false, m.GetRerank()); len(local) > 0 {
			fragments = append(fragments, local)
		}
	}
	remote, unreachable := s.scatter(ctx, m.GetCollection(), idxName, m.GetQuery(), k, ef, int(m.GetNprobe()), m.GetRerank())
	fragments = append(fragments, remote...)
	merged := vector.MergeTopK(fragments, k)

	res := &wavespanv1.VectorSearchRes{
		Completeness: wavespanv1.Completeness_COMPLETE,
		Meta:         &wavespanv1.ResponseMeta{Source: wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER},
	}
	if unreachable > 0 {
		res.Completeness = wavespanv1.Completeness_PARTIAL
		res.Meta.Warnings = append(res.Meta.Warnings, "some holders were unreachable")
	}
	for _, h := range merged {
		n := &wavespanv1.Neighbor{VectorId: h.VectorID, Distance: h.Distance, Score: h.Score}
		if s.kvRead != nil {
			if val, found, err := s.kvRead(ctx, vector.MutationNamespace(h.Collection), []byte(h.VectorID)); err == nil && found {
				v := &wavespanv1.VectorRecord{}
				if proto.Unmarshal(val, v) == nil {
					n.Vector = v.GetValues()
					if m.GetIncludePayload() {
						n.Payload = v.GetPayload()
					}
				}
			}
		}
		res.Neighbors = append(res.Neighbors, n)
	}
	sort.SliceStable(res.Neighbors, func(i, j int) bool { return res.Neighbors[i].Distance < res.Neighbors[j].Distance })
	return res, nil
}
