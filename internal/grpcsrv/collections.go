package grpcsrv

import (
	"context"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/collections"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Collections is the gRPC CollectionService adapter. The collections write/read path (idempotency-key
// dedup, leader forwarding, WRONGTYPE→FailedPrecondition mapping, ResponseMeta construction) lives in
// internal/collections.Service over private engine primitives (command/item/op codes, proposeCount).
// Rather than duplicate that body — and the private helpers it needs — this adapter delegates to the
// same *collections.Service, translating Connect codes to gRPC status codes. Same cores, same logic.
type Collections struct {
	wavespanv1.UnimplementedCollectionServiceServer
	svc *collections.Service
}

// NewCollections wires the gRPC CollectionService adapter over the existing collections service core.
func NewCollections(svc *collections.Service) *Collections {
	return &Collections{svc: svc}
}

// --- Set ---

func (s *Collections) SAdd(ctx context.Context, m *wavespanv1.SAddRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.SAdd(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) SRem(ctx context.Context, m *wavespanv1.KeysRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.SRem(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) SIsMember(ctx context.Context, m *wavespanv1.MemberRequest) (*wavespanv1.BoolResult, error) {
	res, err := s.svc.SIsMember(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) SCard(ctx context.Context, m *wavespanv1.CardRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.SCard(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) SMembers(ctx context.Context, m *wavespanv1.RangeRequest) (*wavespanv1.MembersResult, error) {
	res, err := s.svc.SMembers(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// --- Hash ---

func (s *Collections) HSet(ctx context.Context, m *wavespanv1.HSetRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.HSet(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) HDel(ctx context.Context, m *wavespanv1.KeysRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.HDel(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) HGet(ctx context.Context, m *wavespanv1.MemberRequest) (*wavespanv1.ValueResult, error) {
	res, err := s.svc.HGet(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) HLen(ctx context.Context, m *wavespanv1.CardRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.HLen(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) HGetAll(ctx context.Context, m *wavespanv1.RangeRequest) (*wavespanv1.FieldsResult, error) {
	res, err := s.svc.HGetAll(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) HIncrBy(ctx context.Context, m *wavespanv1.HIncrByRequest) (*wavespanv1.Int64Result, error) {
	res, err := s.svc.HIncrBy(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) HIncrByFloat(ctx context.Context, m *wavespanv1.HIncrByFloatRequest) (*wavespanv1.DoubleResult, error) {
	res, err := s.svc.HIncrByFloat(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// --- Sorted set ---

func (s *Collections) ZAdd(ctx context.Context, m *wavespanv1.ZAddRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.ZAdd(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) ZRem(ctx context.Context, m *wavespanv1.KeysRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.ZRem(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) ZScore(ctx context.Context, m *wavespanv1.MemberRequest) (*wavespanv1.ScoreResult, error) {
	res, err := s.svc.ZScore(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) ZCard(ctx context.Context, m *wavespanv1.CardRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.ZCard(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) ZRange(ctx context.Context, m *wavespanv1.RangeRequest) (*wavespanv1.ScoredMembersResult, error) {
	res, err := s.svc.ZRange(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// --- Bulk / namespace / operator ---

func (s *Collections) BulkRemove(ctx context.Context, m *wavespanv1.BulkRemoveRequest) (*wavespanv1.BulkRemoveResult, error) {
	res, err := s.svc.BulkRemove(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) TierInfo(ctx context.Context, m *wavespanv1.TierInfoRequest) (*wavespanv1.TierInfoResult, error) {
	res, err := s.svc.TierInfo(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) AdmitLearner(ctx context.Context, m *wavespanv1.AdmitLearnerRequest) (*wavespanv1.AdmitLearnerResponse, error) {
	res, err := s.svc.AdmitLearner(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

func (s *Collections) ProposeForward(ctx context.Context, m *wavespanv1.ProposeForwardRequest) (*wavespanv1.CountResult, error) {
	res, err := s.svc.ProposeForward(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}
