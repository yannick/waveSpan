package checker

import "testing"

// TestBudgetCapCheckerPasses: a ledger whose total acked spend stays at/under the cap is clean.
func TestBudgetCapCheckerPasses(t *testing.T) {
	ev := []SpendEvent{
		{Holder: "A", LeaseID: "a1", Units: 400, AtMs: 10},
		{Holder: "B", LeaseID: "b1", Units: 300, AtMs: 20},
		{Holder: "A", LeaseID: "a2", Units: 300, AtMs: 30}, // total 1000 == cap, exactly safe
	}
	if v := (BudgetCapChecker{Cap: 1000}).Check(ev); len(v) != 0 {
		t.Fatalf("clean ledger flagged: %+v", v)
	}
}

// TestBudgetCapCheckerCatchesOverspend: the moment cumulative acked spend exceeds cap, the checker flags it
// — this is the constructed C2 debit→credit overspend the equality probe alone cannot see.
func TestBudgetCapCheckerCatchesOverspend(t *testing.T) {
	ev := []SpendEvent{
		{Holder: "A", LeaseID: "a1", Units: 600, AtMs: 10},
		{Holder: "B", LeaseID: "b1", Units: 600, AtMs: 20}, // 1200 > cap 1000 (the overspend)
	}
	v := (BudgetCapChecker{Cap: 1000}).Check(ev)
	if len(v) != 1 || v[0].TotalAcked != 1200 || v[0].AtMs != 20 {
		t.Fatalf("overspend not caught correctly: %+v", v)
	}
}

// TestBudgetCapCheckerOrdersByTime: events are summed in AtMs order, so an out-of-order slice still flags at
// the correct instant (a transient overspend that a later credit would mask in the pool's `spent`).
func TestBudgetCapCheckerOrdersByTime(t *testing.T) {
	ev := []SpendEvent{
		{Holder: "B", LeaseID: "b1", Units: 600, AtMs: 30}, // recorded out of order
		{Holder: "A", LeaseID: "a1", Units: 600, AtMs: 10},
	}
	v := (BudgetCapChecker{Cap: 1000}).Check(ev)
	if len(v) != 1 || v[0].AtMs != 30 {
		t.Fatalf("time-ordering broken: %+v", v)
	}
}

// TestLeaseDisjointnessClean: every impression served strictly before its lease's reclaim deadline is clean.
func TestLeaseDisjointnessClean(t *testing.T) {
	ev := []SpendEvent{
		{Holder: "A", LeaseID: "a1", Units: 1, AtMs: 100, ReclaimMs: 5000},
		{Holder: "A", LeaseID: "a1", Units: 1, AtMs: 4999, ReclaimMs: 5000},
	}
	if v := (LeaseDisjointnessChecker{}).Check(ev); len(v) != 0 {
		t.Fatalf("in-window serves flagged: %+v", v)
	}
}

// TestLeaseDisjointnessCatchesPastReclaim: an impression served at/after the reclaim deadline is a window
// overlap — the hole the freshness gate + boottime self-fence close.
func TestLeaseDisjointnessCatchesPastReclaim(t *testing.T) {
	ev := []SpendEvent{
		{Holder: "A", LeaseID: "a1", Units: 1, AtMs: 5200, ReclaimMs: 5000}, // 200ms past reclaim
	}
	v := (LeaseDisjointnessChecker{}).Check(ev)
	if len(v) != 1 || v[0].OverMs != 200 {
		t.Fatalf("past-reclaim serve not caught: %+v", v)
	}
}
