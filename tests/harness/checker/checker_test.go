package checker

import (
	"testing"

	"github.com/cwire/wavespan/internal/version"
	"github.com/cwire/wavespan/tests/harness/runner"
)

func v(phys uint64, logical uint32, member string, seq uint64) *version.Version {
	return &version.Version{HLCPhysicalMs: phys, HLCLogical: logical, WriterClusterID: "dev", WriterMemberID: member, WriterSequence: seq}
}

// healed adds a fault that ends at 50ms so reads at start>=50 are post-heal.
func healed(h *runner.History) {
	h.AppendFault(runner.Fault{Kind: "partition-halves", Targets: []string{"n2"}, StartMs: 10, EndMs: 50})
}

func hasViolation(vs []runner.Violation, property string) bool {
	for _, x := range vs {
		if x.Property == property {
			return true
		}
	}
	return false
}

func TestCleanHistoryHasNoViolations(t *testing.T) {
	h := &runner.History{}
	healed(h)
	h.Append(runner.Op{Kind: runner.OpPut, Key: "a", Value: "1", Ack: true, WriterVersion: v(100, 0, "n1", 1)})
	h.Append(runner.Op{Kind: runner.OpGet, Key: "a", Value: "1", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: v(100, 0, "n1", 1)})
	h.Append(runner.Op{Kind: runner.OpGet, Key: "a", Value: "1", Ack: true, ServedBy: "n2", StartMs: 100, ObservedVersion: v(100, 0, "n1", 1)})
	if vs := Run(h, All()); len(vs) != 0 {
		t.Fatalf("clean history flagged: %+v", vs)
	}
}

func TestConvergence(t *testing.T) {
	// post-heal disagreement -> violation
	bad := &runner.History{}
	healed(bad)
	bad.Append(runner.Op{Kind: runner.OpGet, Key: "a", Value: "1", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: v(100, 0, "n1", 1)})
	bad.Append(runner.Op{Kind: runner.OpGet, Key: "a", Value: "2", Ack: true, ServedBy: "n2", StartMs: 100, ObservedVersion: v(200, 0, "n2", 1)})
	if !hasViolation(Convergence{}.Check(bad), "convergence") {
		t.Fatal("post-heal disagreement must be flagged")
	}
	// disagreement DURING the partition (before heal at 50) -> NOT flagged
	dur := &runner.History{}
	dur.AppendFault(runner.Fault{Kind: "partition-halves", StartMs: 10, EndMs: 200})
	dur.Append(runner.Op{Kind: runner.OpGet, Key: "a", Value: "1", Ack: true, ServedBy: "n1", StartMs: 30, ObservedVersion: v(100, 0, "n1", 1)})
	dur.Append(runner.Op{Kind: runner.OpGet, Key: "a", Value: "2", Ack: true, ServedBy: "n2", StartMs: 30, ObservedVersion: v(50, 0, "n2", 1)})
	if hasViolation(Convergence{}.Check(dur), "convergence") {
		t.Fatal("mid-partition divergence must NOT be flagged")
	}
}

func TestLWWDeterminism(t *testing.T) {
	mk := func(readVer *version.Version) *runner.History {
		h := &runner.History{}
		healed(h)
		h.Append(runner.Op{Kind: runner.OpPut, Key: "a", Ack: true, WriterVersion: v(100, 0, "n1", 1)})
		h.Append(runner.Op{Kind: runner.OpPut, Key: "a", Ack: true, WriterVersion: v(300, 0, "n2", 1)}) // maximal
		h.Append(runner.Op{Kind: runner.OpPut, Key: "a", Ack: true, WriterVersion: v(200, 0, "n1", 2)})
		h.Append(runner.Op{Kind: runner.OpGet, Key: "a", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: readVer})
		return h
	}
	if hasViolation(LWWDeterminism{}.Check(mk(v(300, 0, "n2", 1))), "lww-determinism") {
		t.Fatal("a read of the maximal winner must not be flagged")
	}
	if !hasViolation(LWWDeterminism{}.Check(mk(v(200, 0, "n1", 2))), "lww-determinism") {
		t.Fatal("a read of a non-maximal version must be flagged")
	}
}

func TestDurability(t *testing.T) {
	winner := v(300, 0, "n2", 1)
	base := func() *runner.History {
		h := &runner.History{}
		healed(h)
		h.Append(runner.Op{Kind: runner.OpPut, Key: "a", Ack: true, WriterVersion: winner})
		return h
	}
	lost := base()
	lost.Append(runner.Op{Kind: runner.OpGet, Key: "a", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: v(100, 0, "n1", 1)})
	if !hasViolation(Durability{}.Check(lost), "durability") {
		t.Fatal("a vanished acked winner must be flagged")
	}
	// the same loss is excused when both holders died
	excused := base()
	excused.AppendFault(runner.Fault{Kind: "double-holder-loss", Targets: []string{"a"}, StartMs: 10, EndMs: 40})
	excused.Append(runner.Op{Kind: runner.OpGet, Key: "a", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: v(100, 0, "n1", 1)})
	if hasViolation(Durability{}.Check(excused), "durability") {
		t.Fatal("double-holder-loss must excuse the loss")
	}
}

func TestCompletenessHonesty(t *testing.T) {
	mk := func(comp string, hasCert bool, validUntil int64) *runner.History {
		h := &runner.History{}
		h.Append(runner.Op{Kind: runner.OpScan, Key: "ns", Completeness: comp, HasCertificate: hasCert, CertValidUntilMs: validUntil, StartMs: 100, EndMs: 101})
		return h
	}
	if !hasViolation(CompletenessHonesty{}.Check(mk("COMPLETE", false, 0)), "completeness-honesty") {
		t.Fatal("COMPLETE without a certificate must be flagged")
	}
	if !hasViolation(CompletenessHonesty{}.Check(mk("COMPLETE", true, 50)), "completeness-honesty") {
		t.Fatal("COMPLETE with an expired certificate must be flagged")
	}
	if hasViolation(CompletenessHonesty{}.Check(mk("COMPLETE", true, 5000)), "completeness-honesty") {
		t.Fatal("COMPLETE with a valid certificate must be honest")
	}
	if hasViolation(CompletenessHonesty{}.Check(mk("PARTIAL", false, 0)), "completeness-honesty") {
		t.Fatal("PARTIAL is always honest")
	}
}

func TestIdempotency(t *testing.T) {
	deduped := &runner.History{}
	deduped.Append(runner.Op{Kind: runner.OpPut, Key: "c", RequestID: "r1", Ack: true, WriterVersion: v(100, 0, "n1", 1)})
	deduped.Append(runner.Op{Kind: runner.OpPut, Key: "c", RequestID: "r1", Ack: true, WriterVersion: v(100, 0, "n1", 1)}) // same version (retry deduped)
	if hasViolation(Idempotency{}.Check(deduped), "idempotency") {
		t.Fatal("a deduped retry (same version) must not be flagged")
	}
	doubled := &runner.History{}
	doubled.Append(runner.Op{Kind: runner.OpAppend, Key: "c", RequestID: "r1", Ack: true, WriterVersion: v(100, 0, "n1", 1)})
	doubled.Append(runner.Op{Kind: runner.OpAppend, Key: "c", RequestID: "r1", Ack: true, WriterVersion: v(200, 0, "n1", 2)}) // applied twice
	if !hasViolation(Idempotency{}.Check(doubled), "idempotency") {
		t.Fatal("a request_id applied as two distinct mutations must be flagged")
	}
}

func TestNoLostUpdatePerPolicy(t *testing.T) {
	wa, wb := v(100, 0, "n1", 1), v(100, 0, "n2", 1) // concurrent
	// keep-siblings, both present -> ok
	ok := &runner.History{}
	healed(ok)
	ok.Append(runner.Op{Kind: runner.OpPut, Key: "s", Policy: "keep-siblings", Ack: true, WriterVersion: wa})
	ok.Append(runner.Op{Kind: runner.OpPut, Key: "s", Policy: "keep-siblings", Ack: true, WriterVersion: wb})
	ok.Append(runner.Op{Kind: runner.OpGet, Key: "s", Policy: "keep-siblings", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: wa, Siblings: []*version.Version{wb}})
	if hasViolation(NoLostUpdatePerPolicy{}.Check(ok), "no-lost-update-per-policy") {
		t.Fatal("keep-siblings with both siblings present must not be flagged")
	}
	// keep-siblings, one dropped -> violation
	dropped := &runner.History{}
	healed(dropped)
	dropped.Append(runner.Op{Kind: runner.OpPut, Key: "s", Policy: "keep-siblings", Ack: true, WriterVersion: wa})
	dropped.Append(runner.Op{Kind: runner.OpPut, Key: "s", Policy: "keep-siblings", Ack: true, WriterVersion: wb})
	dropped.Append(runner.Op{Kind: runner.OpGet, Key: "s", Policy: "keep-siblings", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: wa}) // wb missing
	if !hasViolation(NoLostUpdatePerPolicy{}.Check(dropped), "no-lost-update-per-policy") {
		t.Fatal("a dropped keep-siblings concurrent write must be flagged")
	}
	// LWW with a dropped overwrite -> NOT flagged (policy-legal)
	lww := &runner.History{}
	healed(lww)
	lww.Append(runner.Op{Kind: runner.OpPut, Key: "k", Ack: true, WriterVersion: wa})
	lww.Append(runner.Op{Kind: runner.OpPut, Key: "k", Ack: true, WriterVersion: wb})
	lww.Append(runner.Op{Kind: runner.OpGet, Key: "k", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: wb})
	if hasViolation(NoLostUpdatePerPolicy{}.Check(lww), "no-lost-update-per-policy") {
		t.Fatal("LWW concurrent-overwrite loss must NOT be flagged")
	}
}

func TestSessionMonotonicity(t *testing.T) {
	mono := &runner.History{}
	mono.Append(runner.Op{Kind: runner.OpGet, Key: "a", Session: "s1", Ack: true, StartMs: 10, ObservedVersion: v(100, 0, "n1", 1)})
	mono.Append(runner.Op{Kind: runner.OpGet, Key: "a", Session: "s1", Ack: true, StartMs: 20, ObservedVersion: v(200, 0, "n1", 2)})
	if hasViolation(SessionMonotonicity{}.Check(mono), "session-monotonicity") {
		t.Fatal("monotonic session reads must not be flagged")
	}
	regress := &runner.History{}
	regress.Append(runner.Op{Kind: runner.OpGet, Key: "a", Session: "s1", Ack: true, StartMs: 10, ObservedVersion: v(200, 0, "n1", 2)})
	regress.Append(runner.Op{Kind: runner.OpGet, Key: "a", Session: "s1", Ack: true, StartMs: 20, ObservedVersion: v(100, 0, "n1", 1)}) // older!
	if !hasViolation(SessionMonotonicity{}.Check(regress), "session-monotonicity") {
		t.Fatal("a session read regressing to an older version must be flagged")
	}
}

func TestTTLBound(t *testing.T) {
	c := TTLBound{DefaultMaxVisibilityMs: 1000}
	// read AFTER the bound still live -> violation
	late := &runner.History{}
	late.Append(runner.Op{Kind: runner.OpPut, Key: "t", Ack: true, ExpiresAtMs: 1000, WriterVersion: v(1, 0, "n1", 1)})
	late.Append(runner.Op{Kind: runner.OpGet, Key: "t", Value: "v", Ack: true, StartMs: 3000}) // 3000 > 1000+1000
	if !hasViolation(c.Check(late), "ttl-bound") {
		t.Fatal("an expired key live past the bound must be flagged")
	}
	// read BEFORE the bound still live -> fine (lazy TTL)
	early := &runner.History{}
	early.Append(runner.Op{Kind: runner.OpPut, Key: "t", Ack: true, ExpiresAtMs: 1000})
	early.Append(runner.Op{Kind: runner.OpGet, Key: "t", Value: "v", Ack: true, StartMs: 1500}) // within the bound
	if hasViolation(c.Check(early), "ttl-bound") {
		t.Fatal("an expired key visible within the bound must NOT be flagged")
	}
	// tombstone after the bound -> fine
	gone := &runner.History{}
	gone.Append(runner.Op{Kind: runner.OpPut, Key: "t", Ack: true, ExpiresAtMs: 1000})
	gone.Append(runner.Op{Kind: runner.OpGet, Key: "t", Ack: true, Tombstone: true, StartMs: 3000})
	if hasViolation(c.Check(gone), "ttl-bound") {
		t.Fatal("a tombstoned expired key must not be flagged")
	}
}
