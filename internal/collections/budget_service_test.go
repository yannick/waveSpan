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
		{ErrBudgetBadParam, connect.CodeInvalidArgument},
		// 2c.4: a settled/tombstoned lease_id maps to AlreadyExists so the node lease cache mints a fresh
		// nonce instead of retrying the same (tombstoned) id.
		{ErrLeaseSettled, connect.CodeAlreadyExists},
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

// TestBudgetGrantEchoesTiming asserts the 2c.1 grant-result timing echo: a grant against a timed budget
// returns the effective leader-stamped granted_ms plus the budget's self_guard/max_pause config and the
// resolved ttl, so the node lease cache can run its freshness gate and stamp its deadline (§5 M-4). The
// per-grant ttl_ms override wins over the budget default.
func TestBudgetGrantEchoesTiming(t *testing.T) {
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
	ns, budget := "pacing", []byte("li/echo/total")

	// A timed budget: default_ttl 60s, self_guard 700ms (>= maxClockSkewMs), max_pause 2s, dedup 30s.
	if _, err := svc.BudgetDefine(ctx, connect.NewRequest(&wavespanv1.BudgetDefineRequest{
		Namespace: ns, Budget: budget, CapUnits: 1000, Mode: wavespanv1.BudgetMode_BUDGET_MODE_STRICT,
		SelfGuardMs: 700, MaxPauseMs: 2000, DefaultTtlMs: 60_000, DedupRetryWindowMs: 30_000,
	})); err != nil {
		t.Fatalf("BudgetDefine: %v", err)
	}

	before := time.Now().UnixMilli()
	// Grant with a per-grant ttl override of 30s (wins over the 60s default).
	gRes, err := svc.BudgetGrant(ctx, connect.NewRequest(&wavespanv1.BudgetGrantRequest{
		Namespace: ns, Budget: budget, HolderId: "node-A", AmountUnits: 100, LeaseId: []byte("lease-echo"),
		TtlMs: 30_000,
	}))
	if err != nil {
		t.Fatalf("BudgetGrant: %v", err)
	}
	after := time.Now().UnixMilli()
	g := gRes.Msg
	if g.GetGrantedUnits() != 100 {
		t.Fatalf("granted = %d, want 100", g.GetGrantedUnits())
	}
	if g.GetGrantedMs() < before || g.GetGrantedMs() > after {
		t.Fatalf("granted_ms = %d, want within [%d,%d]", g.GetGrantedMs(), before, after)
	}
	if g.GetTtlMs() != 30_000 {
		t.Fatalf("ttl_ms echo = %d, want 30000 (per-grant override)", g.GetTtlMs())
	}
	if g.GetSelfGuardMs() != 700 {
		t.Fatalf("self_guard_ms echo = %d, want 700", g.GetSelfGuardMs())
	}
	if g.GetMaxPauseBudgetMs() != 2000 {
		t.Fatalf("max_pause_budget_ms echo = %d, want 2000", g.GetMaxPauseBudgetMs())
	}

	// An idempotent retry (same lease_id) re-echoes byte-identical timing (Gap#2, load-bearing for 2c.4).
	rRes, err := svc.BudgetGrant(ctx, connect.NewRequest(&wavespanv1.BudgetGrantRequest{
		Namespace: ns, Budget: budget, HolderId: "node-A", AmountUnits: 100, LeaseId: []byte("lease-echo"),
		TtlMs: 30_000,
	}))
	if err != nil {
		t.Fatalf("BudgetGrant(retry): %v", err)
	}
	if r := rRes.Msg; r.GetGrantedMs() != g.GetGrantedMs() || r.GetTtlMs() != g.GetTtlMs() ||
		r.GetSelfGuardMs() != g.GetSelfGuardMs() || r.GetMaxPauseBudgetMs() != g.GetMaxPauseBudgetMs() {
		t.Fatalf("retry echo = %+v, want identical to %+v", r, g)
	}
}
