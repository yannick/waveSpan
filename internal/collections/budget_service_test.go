package collections

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestCollErrBudgetCodes pins the Connect-code mapping for every LeasedBudget error (Task 10): no
// capacity is ResourceExhausted; an existing/missing pool and an unknown lease are FailedPrecondition;
// an unsupported mode is InvalidArgument.
func TestCollErrBudgetCodes(t *testing.T) {
	cases := []struct {
		err  error
		want connect.Code
	}{
		{ErrNoCapacity, connect.CodeResourceExhausted},
		{ErrBudgetExists, connect.CodeFailedPrecondition},
		{ErrBudgetNotFound, connect.CodeFailedPrecondition},
		{ErrLeaseUnknown, connect.CodeFailedPrecondition},
		{ErrUnsupportedMode, connect.CodeInvalidArgument},
	}
	for _, c := range cases {
		if got := connect.CodeOf(collErr(c.err)); got != c.want {
			t.Fatalf("collErr(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// TestBudgetServiceHandlers drives the Connect handlers over a real Manager + shard (mirrors
// hincr_test.go / TestBudgetEndToEnd): it asserts the define/grant responses, that a depleted STRICT
// pool yields a no_capacity result (not an error), and that re-defining a pool maps ErrBudgetExists to
// FailedPrecondition.
func TestBudgetServiceHandlers(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	m := newMgr(t, t.TempDir(), addr, mem)
	if err := m.StartShard(2, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	defer m.Stop()
	cols := New(m, SingleShardDirectory(2))
	waitReady(t, cols)
	svc := NewService(cols, membership.Member{})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ns, budget := "pacing", []byte("li/7/total")

	// Define cap=500 STRICT; the response carries the fresh accounting.
	defRes, err := svc.BudgetDefine(ctx, connect.NewRequest(&wavespanv1.BudgetDefineRequest{
		Namespace: ns, Budget: budget, CapUnits: 500, Mode: wavespanv1.BudgetMode_BUDGET_MODE_STRICT,
	}))
	if err != nil {
		t.Fatalf("BudgetDefine: %v", err)
	}
	if d := defRes.Msg; !d.GetExists() || d.GetCapUnits() != 500 || d.GetAvailableUnits() != 500 {
		t.Fatalf("define result = %+v, want exists cap500 avail500", d)
	}

	// Grant 500 (whole pool) to lease-1.
	gRes, err := svc.BudgetGrant(ctx, connect.NewRequest(&wavespanv1.BudgetGrantRequest{
		Namespace: ns, Budget: budget, HolderId: "node-A", AmountUnits: 500, LeaseId: []byte("lease-1"),
	}))
	if err != nil {
		t.Fatalf("BudgetGrant: %v", err)
	}
	if g := gRes.Msg; g.GetGrantedUnits() != 500 || g.GetNoCapacity() {
		t.Fatalf("grant result = %+v, want granted500 noCapacity=false", g)
	}

	// A second grant against the now-empty pool is a normal no_capacity RESULT, not an error.
	emptyRes, err := svc.BudgetGrant(ctx, connect.NewRequest(&wavespanv1.BudgetGrantRequest{
		Namespace: ns, Budget: budget, HolderId: "node-B", AmountUnits: 100, LeaseId: []byte("lease-2"),
	}))
	if err != nil {
		t.Fatalf("BudgetGrant(empty): %v", err)
	}
	if e := emptyRes.Msg; !e.GetNoCapacity() || e.GetGrantedUnits() != 0 {
		t.Fatalf("depleted grant result = %+v, want noCapacity granted0", e)
	}

	// Re-defining an existing pool maps ErrBudgetExists -> FailedPrecondition.
	_, err = svc.BudgetDefine(ctx, connect.NewRequest(&wavespanv1.BudgetDefineRequest{
		Namespace: ns, Budget: budget, CapUnits: 999, Mode: wavespanv1.BudgetMode_BUDGET_MODE_STRICT,
	}))
	if got := connect.CodeOf(err); err == nil || got != connect.CodeFailedPrecondition {
		t.Fatalf("re-define err = %v (code %v), want FailedPrecondition", err, got)
	}
}
