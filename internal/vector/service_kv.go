package vector

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// SampleVectors returns a reservoir sample of this node's local vectors for a collection — the
// per-node contribution the IVF trainer aggregates (design/29 Phase 3.5).
func (s *Service) SampleVectors(_ context.Context, req *connect.Request[wavespanv1.SampleVectorsReq]) (*connect.Response[wavespanv1.SampleVectorsRes], error) {
	m := req.Msg
	limit := int(m.GetLimit())
	if limit <= 0 {
		limit = 1000
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	sample := ReservoirSample(s.store, m.GetCollection(), limit, rng)
	res := &wavespanv1.SampleVectorsRes{Vectors: make([]*wavespanv1.FloatVector, len(sample))}
	for i, v := range sample {
		res.Vectors[i] = &wavespanv1.FloatVector{Values: v}
	}
	return connect.NewResponse(res), nil
}

// VectorPut stores an embedding (the key) with an opaque payload (the value), replicated to holders.
func (s *Service) VectorPut(ctx context.Context, req *connect.Request[wavespanv1.VectorPutReq]) (*connect.Response[wavespanv1.VectorPutRes], error) {
	m := req.Msg
	rec := &wavespanv1.VectorRecord{Collection: m.GetCollection(), Values: m.GetVector(), Payload: m.GetPayload()}
	ver, err := s.putRecord(ctx, rec)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&wavespanv1.VectorPutRes{Version: ver}), nil
}

// VectorGet returns the payload stored under an exact embedding, read via the KV path (local or the
// closest holder).
func (s *Service) VectorGet(ctx context.Context, req *connect.Request[wavespanv1.VectorGetReq]) (*connect.Response[wavespanv1.VectorGetRes], error) {
	if s.kvRead == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, connectError("vector: cluster ops not configured"))
	}
	m := req.Msg
	id := VecHash(m.GetVector())
	val, found, err := s.kvRead(ctx, MutationNamespace(m.GetCollection()), []byte(id))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if !found {
		return connect.NewResponse(&wavespanv1.VectorGetRes{Found: false}), nil
	}
	v := &wavespanv1.VectorRecord{}
	if uerr := proto.Unmarshal(val, v); uerr != nil {
		return nil, connect.NewError(connect.CodeInternal, uerr)
	}
	return connect.NewResponse(&wavespanv1.VectorGetRes{Found: !v.GetTombstone(), Payload: v.GetPayload()}), nil
}

// VectorDelete tombstones the record under an exact embedding (the holders' apply-observer purges it
// from each HNSW).
func (s *Service) VectorDelete(ctx context.Context, req *connect.Request[wavespanv1.VectorDeleteReq]) (*connect.Response[wavespanv1.VectorDeleteRes], error) {
	if s.kvDelete == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, connectError("vector: cluster ops not configured"))
	}
	m := req.Msg
	id := VecHash(m.GetVector())
	if err := s.kvDelete(ctx, MutationNamespace(m.GetCollection()), []byte(id)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&wavespanv1.VectorDeleteRes{}), nil
}

// VectorSearch runs a cluster-wide kNN: it scatters SearchLocal to holders, merges the global top-k,
// and (optionally) attaches each neighbour's embedding + payload read via the KV path.
func (s *Service) VectorSearch(ctx context.Context, req *connect.Request[wavespanv1.VectorSearchReq]) (*connect.Response[wavespanv1.VectorSearchRes], error) {
	if s.scatter == nil || s.collIndex == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, connectError("vector: cluster search not configured"))
	}
	m := req.Msg
	if s.dims != nil {
		if d, ok := s.dims(m.GetCollection()); ok && len(m.GetQuery()) != d {
			return nil, connect.NewError(connect.CodeInvalidArgument, connectError("vector: query dimension mismatch for collection "+m.GetCollection()))
		}
	}
	idxName, ok := s.collIndex(m.GetCollection())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, connectError("vector: no index for collection "+m.GetCollection()))
	}
	k := int(m.GetK())
	if k <= 0 {
		k = 10
	}
	ef := int(m.GetEfSearch())
	if ef <= 0 {
		ef = 64
	}
	// Local fragment: the coordinator also holds vectors, and the peer scatter skips self.
	var fragments [][]Hit
	if meta, ok := s.index(idxName); ok {
		var live *LiveIndex
		if s.live != nil {
			live, _ = s.live(idxName)
		}
		if local := LocalSearch(s.store, meta, live, m.GetQuery(), k, ef, false, m.GetRerank()); len(local) > 0 {
			fragments = append(fragments, local)
		}
	}
	// Remote fragments from peer holders (routed to probed-bucket holders when nprobe>0).
	remote, unreachable := s.scatter(ctx, m.GetCollection(), idxName, m.GetQuery(), k, ef, int(m.GetNprobe()), m.GetRerank())
	fragments = append(fragments, remote...)
	merged := MergeTopK(fragments, k)

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
		// Attach the embedding (always) + payload (when requested) by reading the record.
		if s.kvRead != nil {
			if val, found, err := s.kvRead(ctx, MutationNamespace(h.Collection), []byte(h.VectorID)); err == nil && found {
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
	// merged is already distance-ordered, but guard determinism after the payload join.
	sort.SliceStable(res.Neighbors, func(i, j int) bool { return res.Neighbors[i].Distance < res.Neighbors[j].Distance })
	return connect.NewResponse(res), nil
}
