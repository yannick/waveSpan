package collections

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
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
	switch {
	case errors.Is(err, ErrWrongType):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrNotNumber):
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func (s *Service) count(n uint64, err error) (*connect.Response[wavespanv1.CountResult], error) {
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.CountResult{Meta: s.meta(), Count: n}), nil
}

// write builds a mutation carrying the idempotency key and proposes it (forwarding to the leader if
// this node isn't it). idem == "" means no dedup.
func (s *Service) write(ctx context.Context, op opKind, ns, coll []byte, idem string, items []item) (*connect.Response[wavespanv1.CountResult], error) {
	return s.count(s.cols.proposeCount(ctx, command{Op: op, NS: ns, Coll: coll, Idem: []byte(idem), Items: items}))
}

// --- Set ---

// SAdd adds set members (optionally with a TTL).
func (s *Service) SAdd(ctx context.Context, req *connect.Request[wavespanv1.SAddRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	items := itemsFromKeys(m.GetMembers())
	if m.TtlMs != nil {
		exp := time.Now().UnixMilli() + m.GetTtlMs()
		for i := range items {
			items[i].ExpiryMs = exp
		}
	}
	return s.write(ctx, opSAdd, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), items)
}

// SRem removes set members.
func (s *Service) SRem(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.write(ctx, opSRem, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), itemsFromKeys(m.GetKeys()))
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
	items := make([]item, len(m.GetFields()))
	for i, f := range m.GetFields() {
		items[i] = item{Key: f.GetField(), Val: f.GetValue()}
	}
	return s.write(ctx, opHSet, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), items)
}

// HDel deletes hash fields.
func (s *Service) HDel(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.write(ctx, opHDel, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), itemsFromKeys(m.GetKeys()))
}

// HIncrBy atomically increments an integer hash field and returns the new value.
func (s *Service) HIncrBy(ctx context.Context, req *connect.Request[wavespanv1.HIncrByRequest]) (*connect.Response[wavespanv1.Int64Result], error) {
	m := req.Msg
	d := make([]byte, 8)
	binary.BigEndian.PutUint64(d, uint64(m.GetDelta()))
	_, data, err := s.cols.proposeCmd(ctx, command{Op: opHIncrBy, NS: []byte(m.GetNamespace()), Coll: m.GetCollection(),
		Idem: []byte(m.GetIdempotencyKey()), Items: []item{{Key: m.GetField(), Val: d}}})
	if err != nil {
		return nil, collErr(err)
	}
	if len(data) != 8 {
		return nil, collErr(ErrNotNumber)
	}
	return connect.NewResponse(&wavespanv1.Int64Result{Meta: s.meta(), Value: int64(binary.BigEndian.Uint64(data))}), nil
}

// HIncrByFloat atomically increments a float hash field and returns the new value.
func (s *Service) HIncrByFloat(ctx context.Context, req *connect.Request[wavespanv1.HIncrByFloatRequest]) (*connect.Response[wavespanv1.DoubleResult], error) {
	m := req.Msg
	_, data, err := s.cols.proposeCmd(ctx, command{Op: opHIncrByFloat, NS: []byte(m.GetNamespace()), Coll: m.GetCollection(),
		Idem: []byte(m.GetIdempotencyKey()), Items: []item{{Key: m.GetField(), Score: m.GetDelta()}}})
	if err != nil {
		return nil, collErr(err)
	}
	if len(data) != 8 {
		return nil, collErr(ErrNotNumber)
	}
	return connect.NewResponse(&wavespanv1.DoubleResult{Meta: s.meta(), Value: math.Float64frombits(binary.BigEndian.Uint64(data))}), nil
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
	items := make([]item, len(m.GetMembers()))
	for i, sm := range m.GetMembers() {
		items[i] = item{Key: sm.GetMember(), Score: sm.GetScore()}
	}
	return s.write(ctx, opZAdd, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), items)
}

// ZRem removes sorted-set members.
func (s *Service) ZRem(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.write(ctx, opZRem, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), itemsFromKeys(m.GetKeys()))
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

// ProposeForward applies a write forwarded by a peer that was not the shard's leader (node-side leader
// routing, design/30 §13.13). It commits locally only — if this node also isn't the leader, the error
// propagates and the forwarder tries the next peer.
func (s *Service) ProposeForward(ctx context.Context, req *connect.Request[wavespanv1.ProposeForwardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	n, data, err := s.cols.ProposeRaw(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetCommand())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.CountResult{Meta: s.meta(), Count: n, Data: data}), nil
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
