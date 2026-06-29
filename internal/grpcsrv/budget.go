package grpcsrv

import (
	"context"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/collections"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Budget is the gRPC BudgetServiceServer adapter (design/35). Like the CollectionService adapter, it
// delegates to the same *collections.Service Connect core — the leased-budget engine, idempotency, error
// mapping, and ResponseMeta all live there — translating Connect codes to gRPC status codes. One core,
// one set of semantics, exposed on both the Connect (UI) and gRPC (data-plane) transports.
type Budget struct {
	wavespanv1.UnimplementedBudgetServiceServer
	svc *collections.Service
}

// NewBudget wires the gRPC BudgetService adapter over the existing collections service core.
func NewBudget(svc *collections.Service) *Budget {
	return &Budget{svc: svc}
}

// BudgetDefine implements the BudgetServiceServer gRPC method by delegating to the Connect service.
func (s *Budget) BudgetDefine(ctx context.Context, m *wavespanv1.BudgetDefineRequest) (*wavespanv1.BudgetStatResult, error) {
	res, err := s.svc.BudgetDefine(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// BudgetGrant implements the BudgetServiceServer gRPC method by delegating to the Connect service.
func (s *Budget) BudgetGrant(ctx context.Context, m *wavespanv1.BudgetGrantRequest) (*wavespanv1.BudgetGrantResult, error) {
	res, err := s.svc.BudgetGrant(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// BudgetReport implements the BudgetServiceServer gRPC method by delegating to the Connect service.
func (s *Budget) BudgetReport(ctx context.Context, m *wavespanv1.BudgetReportRequest) (*wavespanv1.BudgetStatResult, error) {
	res, err := s.svc.BudgetReport(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// BudgetReturn implements the BudgetServiceServer gRPC method by delegating to the Connect service.
func (s *Budget) BudgetReturn(ctx context.Context, m *wavespanv1.BudgetReturnRequest) (*wavespanv1.BudgetStatResult, error) {
	res, err := s.svc.BudgetReturn(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// BudgetReconcile implements the BudgetServiceServer gRPC method by delegating to the Connect service.
func (s *Budget) BudgetReconcile(ctx context.Context, m *wavespanv1.BudgetReconcileRequest) (*wavespanv1.BudgetStatResult, error) {
	res, err := s.svc.BudgetReconcile(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// BudgetStat implements the BudgetServiceServer gRPC method by delegating to the Connect service.
func (s *Budget) BudgetStat(ctx context.Context, m *wavespanv1.BudgetStatRequest) (*wavespanv1.BudgetStatResult, error) {
	res, err := s.svc.BudgetStat(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}
