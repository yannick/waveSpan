package checker

import (
	"fmt"
	"sort"
)

// SpendEvent is one externally-ACKED delivery the LeasedBudget soak recorded AFTER a holder's Spend
// returned nil — i.e. an impression the adserver REALLY delivered (units = impressions × price). It is the
// harness's GROUND TRUTH, deliberately distinct from the pool's internal `spent`: the §7/§16 mandate is
// that the equality probe (cap == available+leasedOut+spent) alone cannot catch a credit-instead-of-debit
// overspend, so the soak checks the holders' actual deliveries instead (design/35 §7, C2).
type SpendEvent struct {
	Holder    string // node identity that served the impression
	LeaseID   string // server lease_id the units were drawn under
	Units     int64  // impressions × price actually delivered (the acked spend)
	AtMs      int64  // real (suspend-INCLUSIVE) time the impression was served
	ReclaimMs int64  // grantor's REPLICATED reclaimNotBeforeMs for this lease (0 = non-timed)
}

// CapViolation is a point where the cumulative externally-acked spend exceeded the cap.
type CapViolation struct {
	Cap        int64
	TotalAcked int64
	AtMs       int64
	Detail     string
}

// DisjointViolation is an impression served at/after the grantor's reclaim deadline for its lease — the
// holder's serving window overlapped the grantor's reclaim window (§2 lease disjointness broken).
type DisjointViolation struct {
	Holder  string
	LeaseID string
	AtMs    int64
	OverMs  int64 // how far past reclaimNotBeforeMs the impression was served
	Detail  string
}

// BudgetCapChecker asserts the §7 mandate: the cumulative externally-ACKED spend never exceeds the cap, at
// EVERY prefix of the time-ordered ledger (so a transient mid-run overspend is caught, not merely the final
// total). It NEVER reads the pool's internal accounting — only the holders' real deliveries. This is the
// checker that catches the C2 debit→credit regression: forced expiry must DEBIT the un-attested remainder,
// never credit it back to available (which would let the same money be granted and served twice).
type BudgetCapChecker struct{ Cap int64 }

// Name returns the property name.
func (BudgetCapChecker) Name() string { return "budget-strict-cap" }

// Check sorts a COPY of the ledger by AtMs (stable) and walks the running sum, flagging the FIRST instant
// the cumulative acked spend exceeds the cap (every later event would also flag; one is enough to fail).
func (c BudgetCapChecker) Check(events []SpendEvent) []CapViolation { return checkCap(events, c.Cap) }

func checkCap(events []SpendEvent, capUnits int64) []CapViolation {
	ev := append([]SpendEvent(nil), events...)
	sort.SliceStable(ev, func(i, j int) bool { return ev[i].AtMs < ev[j].AtMs })
	var sum int64
	var out []CapViolation
	for _, e := range ev {
		if e.Units <= 0 {
			continue
		}
		sum += e.Units
		if sum > capUnits {
			out = append(out, CapViolation{
				Cap: capUnits, TotalAcked: sum, AtMs: e.AtMs,
				Detail: fmt.Sprintf("Σ acked spend %d exceeds cap %d at t=%dms (overspend of %d)", sum, capUnits, e.AtMs, sum-capUnits),
			})
			return out // one is a failure; stop (the rest would all flag)
		}
	}
	return out
}

// LeaseDisjointnessChecker asserts the §2 window-disjointness property the freshness gate (§16 edit #2) and
// the suspend-aware boottime self-fence (C1 / edit #6) exist to preserve: a holder must STOP serving a lease
// strictly before the grantor's earliest reclaim instant (its replicated reclaimNotBeforeMs). An impression
// served at/after that instant means the holder's serving window overlapped the grantor's reclaim window —
// the precise hole those two protections close. (With debit-on-forced-expiry standing, such an overlap does
// NOT by itself overspend in single-cluster — debit is the cap backstop — so this checker, not the cap
// checker, is the one those two controls trip; see the soak's NEGATIVE-CONTROL notes.)
type LeaseDisjointnessChecker struct{}

// Name returns the property name.
func (LeaseDisjointnessChecker) Name() string { return "budget-lease-disjointness" }

// Check flags every acked impression served at or after its lease's replicated reclaim deadline.
func (LeaseDisjointnessChecker) Check(events []SpendEvent) []DisjointViolation {
	var out []DisjointViolation
	for _, e := range events {
		if e.ReclaimMs > 0 && e.AtMs >= e.ReclaimMs {
			out = append(out, DisjointViolation{
				Holder: e.Holder, LeaseID: e.LeaseID, AtMs: e.AtMs, OverMs: e.AtMs - e.ReclaimMs,
				Detail: fmt.Sprintf("holder %s served lease %s at t=%dms, %dms past its reclaim deadline %dms",
					e.Holder, e.LeaseID, e.AtMs, e.AtMs-e.ReclaimMs, e.ReclaimMs),
			})
		}
	}
	return out
}
