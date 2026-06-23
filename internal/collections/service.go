package collections

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// learnerAdmitTarget is the subset of Manager the AdmitLearner RPC needs (admit a learner to a shard
// this node hosts).
type learnerAdmitTarget interface {
	AddLearner(ctx context.Context, shardID, replicaID uint64, addr string) error
}

// Service is the public CollectionService Connect handler over the typed Collections engine
// (design/30 §13). Writes go through the owning shard's leader; reads honour the request's
// linearizable flag. WRONGTYPE maps to FailedPrecondition.
type Service struct {
	cols  *Collections
	self  membership.Member
	admit learnerAdmitTarget
}

// NewService wires the CollectionService Connect handler.
func NewService(cols *Collections, self membership.Member) *Service {
	return &Service{cols: cols, self: self}
}

// WithLearnerAdmit enables the AdmitLearner RPC: this node will admit learners to shards it hosts (the
// server side of demand-fill, design/30 §9).
func (s *Service) WithLearnerAdmit(a learnerAdmitTarget) *Service {
	s.admit = a
	return s
}

// Handler returns the mountable Connect handler (path, handler) for the data port.
func (s *Service) Handler() (string, http.Handler) {
	return wavespanv1connect.NewCollectionServiceHandler(s, rpcopts.Handler()...)
}

func (s *Service) meta() *wavespanv1.ResponseMeta {
	return &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID,
		ServedByMemberId:  s.self.MemberID,
		Source:            wavespanv1.ReadSource_LOCAL_DURABLE,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      wavespanv1.Completeness_COMPLETE,
		ObservedAtUnixMs:  time.Now().UnixMilli(),
	}
}

func collErr(err error) error {
	if errors.Is(err, ErrWrongType) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func (s *Service) count(n uint64, err error) (*connect.Response[wavespanv1.CountResult], error) {
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.CountResult{Meta: s.meta(), Count: n}), nil
}

// --- Set ---

// SAdd adds set members (optionally with a TTL).
func (s *Service) SAdd(ctx context.Context, req *connect.Request[wavespanv1.SAddRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	ns, coll := []byte(m.GetNamespace()), m.GetCollection()
	if m.TtlMs != nil {
		return s.count(s.cols.SAddTTL(ctx, ns, coll, m.GetTtlMs(), m.GetMembers()...))
	}
	return s.count(s.cols.SAdd(ctx, ns, coll, m.GetMembers()...))
}

// SRem removes set members.
func (s *Service) SRem(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.SRem(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetKeys()...))
}

// SIsMember reports set membership.
func (s *Service) SIsMember(ctx context.Context, req *connect.Request[wavespanv1.MemberRequest]) (*connect.Response[wavespanv1.BoolResult], error) {
	m := req.Msg
	ok, err := s.cols.SIsMember(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetMember(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.BoolResult{Meta: s.meta(), Value: ok}), nil
}

// SCard returns the set cardinality.
func (s *Service) SCard(ctx context.Context, req *connect.Request[wavespanv1.CardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.SCard(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetLinearizable()))
}

// SMembers enumerates set members.
func (s *Service) SMembers(ctx context.Context, req *connect.Request[wavespanv1.RangeRequest]) (*connect.Response[wavespanv1.MembersResult], error) {
	m := req.Msg
	out, err := s.cols.SMembers(ctx, []byte(m.GetNamespace()), m.GetCollection(), int(m.GetLimit()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.MembersResult{Meta: s.meta(), Members: out}), nil
}

// --- Hash ---

// HSet sets hash fields.
func (s *Service) HSet(ctx context.Context, req *connect.Request[wavespanv1.HSetRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	fields := make([]FieldValue, len(m.GetFields()))
	for i, f := range m.GetFields() {
		fields[i] = FieldValue{Field: f.GetField(), Value: f.GetValue()}
	}
	return s.count(s.cols.HSet(ctx, []byte(m.GetNamespace()), m.GetCollection(), fields...))
}

// HDel deletes hash fields.
func (s *Service) HDel(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.HDel(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetKeys()...))
}

// HGet returns a hash field value.
func (s *Service) HGet(ctx context.Context, req *connect.Request[wavespanv1.MemberRequest]) (*connect.Response[wavespanv1.ValueResult], error) {
	m := req.Msg
	v, found, err := s.cols.HGet(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetMember(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.ValueResult{Meta: s.meta(), Found: found, Value: v}), nil
}

// HLen returns the number of hash fields.
func (s *Service) HLen(ctx context.Context, req *connect.Request[wavespanv1.CardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.HLen(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetLinearizable()))
}

// HGetAll returns all hash field/value pairs.
func (s *Service) HGetAll(ctx context.Context, req *connect.Request[wavespanv1.RangeRequest]) (*connect.Response[wavespanv1.FieldsResult], error) {
	m := req.Msg
	out, err := s.cols.HGetAll(ctx, []byte(m.GetNamespace()), m.GetCollection(), int(m.GetLimit()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	fields := make([]*wavespanv1.FieldValue, len(out))
	for i, f := range out {
		fields[i] = &wavespanv1.FieldValue{Field: f.Field, Value: f.Value}
	}
	return connect.NewResponse(&wavespanv1.FieldsResult{Meta: s.meta(), Fields: fields}), nil
}

// --- Sorted set ---

// ZAdd adds or updates sorted-set members.
func (s *Service) ZAdd(ctx context.Context, req *connect.Request[wavespanv1.ZAddRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	members := make([]ScoredMember, len(m.GetMembers()))
	for i, sm := range m.GetMembers() {
		members[i] = ScoredMember{Member: sm.GetMember(), Score: sm.GetScore()}
	}
	return s.count(s.cols.ZAdd(ctx, []byte(m.GetNamespace()), m.GetCollection(), members...))
}

// ZRem removes sorted-set members.
func (s *Service) ZRem(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.ZRem(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetKeys()...))
}

// ZScore returns a member's score.
func (s *Service) ZScore(ctx context.Context, req *connect.Request[wavespanv1.MemberRequest]) (*connect.Response[wavespanv1.ScoreResult], error) {
	m := req.Msg
	sc, found, err := s.cols.ZScore(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetMember(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.ScoreResult{Meta: s.meta(), Found: found, Score: sc}), nil
}

// ZCard returns the sorted-set cardinality.
func (s *Service) ZCard(ctx context.Context, req *connect.Request[wavespanv1.CardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.ZCard(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetLinearizable()))
}

// ZRange returns members in ascending score order.
func (s *Service) ZRange(ctx context.Context, req *connect.Request[wavespanv1.RangeRequest]) (*connect.Response[wavespanv1.ScoredMembersResult], error) {
	m := req.Msg
	out, err := s.cols.ZRange(ctx, []byte(m.GetNamespace()), m.GetCollection(), int(m.GetLimit()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	members := make([]*wavespanv1.ScoredMember, len(out))
	for i, sm := range out {
		members[i] = &wavespanv1.ScoredMember{Member: sm.Member, Score: sm.Score}
	}
	return connect.NewResponse(&wavespanv1.ScoredMembersResult{Meta: s.meta(), Members: members}), nil
}

// AdmitLearner admits a peer as a non-voting learner of a shard this node hosts (demand-fill server).
func (s *Service) AdmitLearner(ctx context.Context, req *connect.Request[wavespanv1.AdmitLearnerRequest]) (*connect.Response[wavespanv1.AdmitLearnerResponse], error) {
	if s.admit == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("collections: this node hosts no shards"))
	}
	m := req.Msg
	if err := s.admit.AddLearner(ctx, m.GetShardId(), m.GetReplicaId(), m.GetTarget()); err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.AdmitLearnerResponse{Meta: s.meta()}), nil
}
