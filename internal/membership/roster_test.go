package membership

import (
	"testing"
	"time"
)

func testCfg() LivenessConfig {
	return LivenessConfig{
		SuspicionTimeout:   3 * time.Second,
		UnreachableTimeout: 10 * time.Second,
		DeadRetention:      1 * time.Minute,
		ForgottenRetention: 1 * time.Minute,
	}
}

func mem(id string) Member { return Member{ClusterID: "dev", MemberID: id, NodeName: "node-" + id} }

func at(base time.Time, d time.Duration) time.Time { return base.Add(d) }

func stateOf(t *testing.T, r *Roster, id string) State {
	t.Helper()
	v, ok := r.Get(id)
	if !ok {
		t.Fatalf("member %s not in roster", id)
	}
	return v.State
}

func TestLivenessSuspectToUnreachableToDead(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	r.Upsert(mem("p1"), base)

	if got := stateOf(t, r, "p1"); got != StateAlive {
		t.Fatalf("new member should be ALIVE, got %s", got)
	}

	// failed probe -> SUSPECT
	r.Suspect("p1", at(base, time.Second))
	if got := stateOf(t, r, "p1"); got != StateSuspect {
		t.Fatalf("after Suspect want SUSPECT, got %s", got)
	}

	// before suspicion timeout: still SUSPECT
	r.Tick(at(base, 2*time.Second))
	if got := stateOf(t, r, "p1"); got != StateSuspect {
		t.Fatalf("before timeout want SUSPECT, got %s", got)
	}

	// after suspicion timeout (suspectSince=1s, +3s = 4s) -> UNREACHABLE
	r.Tick(at(base, 5*time.Second))
	if got := stateOf(t, r, "p1"); got != StateUnreachable {
		t.Fatalf("after suspicion timeout want UNREACHABLE, got %s", got)
	}

	// after unreachable timeout -> DEAD
	r.Tick(at(base, 20*time.Second))
	if got := stateOf(t, r, "p1"); got != StateDead {
		t.Fatalf("after unreachable timeout want DEAD, got %s", got)
	}
}

// drivePeerToDead advances p1 through SUSPECT->UNREACHABLE->DEAD via Tick.
func drivePeerToDead(t *testing.T, r *Roster, base time.Time) {
	t.Helper()
	r.Upsert(mem("p1"), base)
	r.Suspect("p1", base)
	r.Tick(at(base, 4*time.Second))  // -> UNREACHABLE
	r.Tick(at(base, 15*time.Second)) // -> DEAD
	if stateOf(t, r, "p1") != StateDead {
		t.Fatal("expected DEAD")
	}
}

func notInMembers(t *testing.T, r *Roster, id string) {
	t.Helper()
	for _, m := range r.Members() {
		if m.Member.MemberID == id {
			t.Fatalf("%s must not appear in Members()", id)
		}
	}
	for _, m := range r.Live() {
		if m.Member.MemberID == id {
			t.Fatalf("%s must not appear in Live()", id)
		}
	}
}

// TestDeadToForgottenToDeleteTwoStage: a dead member (no repair) transitions DEAD -> FORGOTTEN (a retained,
// non-gossiped tombstone excluded from Members()/Live()) after DeadRetention, then is DELETED from the map
// after a further ForgottenRetention (bounded growth). This keeps bug-B's guarantee (a dead member IS
// eventually evicted) while retaining the tombstone to block resurrection during the convergence window.
func TestDeadToForgottenToDeleteTwoStage(t *testing.T) {
	base := time.Unix(2_000_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	drivePeerToDead(t, r, base) // DEAD at ~base+15s, no MarkRepairComplete

	// Before DeadRetention (1m): still DEAD, still present.
	r.Tick(at(base, 30*time.Second))
	if stateOf(t, r, "p1") != StateDead {
		t.Fatal("before retention the dead member must still be present as DEAD")
	}
	// After DeadRetention: FORGOTTEN tombstone — still in the map (Get resolves) but out of Members()/Live().
	r.Tick(at(base, 90*time.Second))
	if stateOf(t, r, "p1") != StateForgotten {
		t.Fatalf("after DeadRetention want FORGOTTEN tombstone, got %s", stateOf(t, r, "p1"))
	}
	notInMembers(t, r, "p1")
	// After a further ForgottenRetention: deleted from the map (bounded growth).
	r.Tick(at(base, 160*time.Second))
	if _, ok := r.Get("p1"); ok {
		t.Fatal("the forgotten tombstone must be DELETED after ForgottenRetention")
	}
}

// TestDeadForgottenEarlyOnRepairComplete: MarkRepairComplete is the optional EARLIER Dead->Forgotten path —
// a repair-complete dead member becomes a FORGOTTEN tombstone before DeadRetention elapses (still retained,
// still out of Members()).
func TestDeadForgottenEarlyOnRepairComplete(t *testing.T) {
	base := time.Unix(2_500_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	drivePeerToDead(t, r, base) // DEAD at ~base+15s

	r.MarkRepairComplete("p1")
	r.Tick(at(base, 20*time.Second)) // well within DeadRetention (1m), but repair done → forget early
	if stateOf(t, r, "p1") != StateForgotten {
		t.Fatalf("a repair-complete dead member must be FORGOTTEN early, got %s", stateOf(t, r, "p1"))
	}
	notInMembers(t, r, "p1")
}

// driveMemberToForgotten drives m through to a FORGOTTEN tombstone (past DeadRetention) and asserts it.
func driveMemberToForgotten(t *testing.T, r *Roster, m Member, base time.Time) {
	t.Helper()
	r.Upsert(m, base)
	r.Suspect(m.MemberID, base)
	r.Tick(at(base, 4*time.Second))  // UNREACHABLE
	r.Tick(at(base, 15*time.Second)) // DEAD
	r.Tick(at(base, 90*time.Second)) // past DeadRetention → FORGOTTEN tombstone
	if stateOf(t, r, m.MemberID) != StateForgotten {
		t.Fatalf("expected FORGOTTEN tombstone, got %s", stateOf(t, r, m.MemberID))
	}
}

// TestForgottenTombstoneBlocksStaleAliveResurrection (Risk 1): a lagging/partitioned peer's stale ALIVE at
// the member's dead old address (incarnation <= the tombstone's) must NOT resurrect it — the retained
// FORGOTTEN tombstone's higher state ordinal makes ApplyGossip's existing ordering reject it.
func TestForgottenTombstoneBlocksStaleAliveResurrection(t *testing.T) {
	base := time.Unix(9_500_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	old := addrMember("p1", "10.0.0.1")
	driveMemberToForgotten(t, r, old, base)

	r.ApplyGossip(MemberView{Member: old, State: StateAlive, Incarnation: 0}, at(base, 91*time.Second))
	if v, _ := r.Get("p1"); v.State != StateForgotten {
		t.Fatalf("stale ALIVE must NOT resurrect a forgotten member: state=%s", v.State)
	}
	notInMembers(t, r, "p1") // not resurrected into Members()/Live() at the dead old address
}

// TestForgottenMemberRevivesOnHigherIncarnation (Risk 1): a GENUINE restart-after-forget — a higher-
// incarnation ALIVE at a new address — still supersedes the tombstone and revives the member.
func TestForgottenMemberRevivesOnHigherIncarnation(t *testing.T) {
	base := time.Unix(9_600_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	driveMemberToForgotten(t, r, addrMember("p1", "10.0.0.1"), base)

	r.ApplyGossip(MemberView{Member: addrMember("p1", "10.0.0.9"), State: StateAlive, Incarnation: 100}, at(base, 91*time.Second))
	v, ok := r.Get("p1")
	if !ok || v.State != StateAlive || v.Member.DataAddr != "10.0.0.9:7800" {
		t.Fatalf("a genuine higher-incarnation revival must supersede the tombstone: ok=%v %+v", ok, v)
	}
	found := false
	for _, m := range r.Live() {
		if m.Member.MemberID == "p1" {
			found = true
		}
	}
	if !found {
		t.Fatal("revived member must be back in Live()")
	}
}

func TestObserveAckRevivesSuspect(t *testing.T) {
	base := time.Unix(3_000_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	r.Upsert(mem("p1"), base)
	r.Suspect("p1", base)
	before, _ := r.Get("p1")
	r.ObserveAck("p1", at(base, time.Second))
	after, _ := r.Get("p1")
	if after.State != StateAlive {
		t.Fatalf("ack should revive to ALIVE, got %s", after.State)
	}
	if after.Incarnation <= before.Incarnation {
		t.Fatalf("revival should bump incarnation to refute suspicion: %d -> %d", before.Incarnation, after.Incarnation)
	}
}

func TestApplyGossipIncarnationRules(t *testing.T) {
	base := time.Unix(4_000_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	r.Upsert(mem("p1"), base)

	// equal incarnation, more severe state is adopted
	r.ApplyGossip(MemberView{Member: mem("p1"), State: StateSuspect, Incarnation: 0}, base)
	if stateOf(t, r, "p1") != StateSuspect {
		t.Fatal("equal-incarnation more-severe state should be adopted")
	}
	// higher incarnation ALIVE overrides suspicion (refutation propagated)
	r.ApplyGossip(MemberView{Member: mem("p1"), State: StateAlive, Incarnation: 1}, base)
	if stateOf(t, r, "p1") != StateAlive {
		t.Fatal("higher-incarnation ALIVE should override suspicion")
	}
	// stale lower incarnation is ignored
	r.ApplyGossip(MemberView{Member: mem("p1"), State: StateDead, Incarnation: 0}, base)
	if stateOf(t, r, "p1") != StateAlive {
		t.Fatal("stale lower incarnation must be ignored")
	}
}

func TestApplyGossipSelfRefutation(t *testing.T) {
	base := time.Unix(5_000_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	selfBefore, _ := r.Get("self")
	// a peer claims we are suspect
	r.ApplyGossip(MemberView{Member: mem("self"), State: StateSuspect, Incarnation: selfBefore.Incarnation + 5}, base)
	selfAfter, _ := r.Get("self")
	if selfAfter.State != StateAlive {
		t.Fatal("self must refute suspicion and stay ALIVE")
	}
	if selfAfter.Incarnation <= selfBefore.Incarnation+5 {
		t.Fatalf("self refutation must out-incarnate the suspicion: %d", selfAfter.Incarnation)
	}
}

func addrMember(id, host string) Member {
	return Member{ClusterID: "dev", MemberID: id, NodeName: "node-" + id, GossipAddr: host + ":7700", DataAddr: host + ":7800"}
}

// TestNewRosterSeedsSelfIncarnation: the injected seed becomes self's starting incarnation (Bug A: a
// monotonic seed lets a restart out-incarnate its prior generation).
func TestNewRosterSeedsSelfIncarnation(t *testing.T) {
	r := NewRoster(mem("self"), testCfg(), 12345)
	v, _ := r.Get("self")
	if v.Incarnation != 12345 {
		t.Fatalf("self incarnation = %d, want the seed 12345", v.Incarnation)
	}
}

// TestApplyGossipChangesAddressOnlyOnHigherIncarnation documents WHY the monotonic seed is needed: an
// equal-incarnation gossip does NOT adopt a changed address (so a restart re-announcing at the same/lower
// incarnation is ignored); a strictly higher incarnation does.
func TestApplyGossipChangesAddressOnlyOnHigherIncarnation(t *testing.T) {
	base := time.Unix(6_000_000, 0)
	r := NewRoster(mem("self"), testCfg(), 0)
	r.ApplyGossip(MemberView{Member: addrMember("p1", "10.0.0.1"), State: StateAlive, Incarnation: 5}, base)

	// same incarnation, different address → NOT adopted (the bug's trigger).
	r.ApplyGossip(MemberView{Member: addrMember("p1", "10.0.0.2"), State: StateAlive, Incarnation: 5}, base)
	if got, _ := r.Get("p1"); got.Member.DataAddr != "10.0.0.1:7800" {
		t.Fatalf("equal-incarnation must NOT change address, got %s", got.Member.DataAddr)
	}
	// higher incarnation → adopted.
	r.ApplyGossip(MemberView{Member: addrMember("p1", "10.0.0.2"), State: StateAlive, Incarnation: 6}, base)
	if got, _ := r.Get("p1"); got.Member.DataAddr != "10.0.0.2:7800" {
		t.Fatalf("higher-incarnation must adopt the new address, got %s", got.Member.DataAddr)
	}
}

// TestRestartedMemberAddressPropagatesOnMonotonicIncarnation is the Bug A gate: a peer knows node1 at its
// old address/incarnation; node1 restarts on a new IP with a MONOTONIC-higher seed; when its restarted
// self-view reaches the peer via gossip, the peer adopts the NEW address. With the old behavior (seed
// ignored → incarnation 0 ≤ old) the peer would keep probing the dead old address.
func TestRestartedMemberAddressPropagatesOnMonotonicIncarnation(t *testing.T) {
	base := time.Unix(7_000_000, 0)
	peer := NewRoster(mem("peer"), testCfg(), 0)
	// The peer already knows node1 at its old address with incarnation 100 (from prior gossip).
	peer.ApplyGossip(MemberView{Member: addrMember("node1", "10.0.0.1"), State: StateAlive, Incarnation: 100}, base)

	// node1 restarts on a fresh volume with a new IP; its roster is seeded with a monotonic-higher
	// incarnation (boot-time millis in production; 200 here).
	restarted := NewRoster(addrMember("node1", "10.0.0.9"), testCfg(), 200)
	selfView, _ := restarted.Get("node1")

	peer.ApplyGossip(selfView, base) // the peer receives node1's restarted announcement

	got, _ := peer.Get("node1")
	if got.Member.DataAddr != "10.0.0.9:7800" || got.Member.GossipAddr != "10.0.0.9:7700" {
		t.Fatalf("peer must adopt the restarted member's NEW address, got %+v", got.Member)
	}
	if got.State != StateAlive {
		t.Fatalf("restarted member must be ALIVE at the peer, got %s", got.State)
	}
}

// TestSelfIncarnationNotLoweredByStaleSuspicion locks the monotonic guarantee: a peer's LOWER-incarnation
// suspicion about self must never lower self's incarnation (else a regressed seed could be dragged down).
func TestSelfIncarnationNotLoweredByStaleSuspicion(t *testing.T) {
	base := time.Unix(9_000_000, 0)
	r := NewRoster(addrMember("self", "10.0.0.1"), testCfg(), 1000) // seeded HIGH
	r.ApplyGossip(MemberView{Member: addrMember("self", "10.0.0.1"), State: StateSuspect, Incarnation: 5}, base)
	v, _ := r.Get("self")
	if v.Incarnation < 1000 {
		t.Fatalf("self incarnation lowered to %d by a stale lower-incarnation suspicion (must stay >= 1000)", v.Incarnation)
	}
	if v.State != StateAlive {
		t.Fatalf("self must stay ALIVE, got %s", v.State)
	}
}

// TestSelfRefutesStaleAddressRecord is the risk-2 (clock-regression) gate: a peer that still remembers
// self at its OLD address — even at an incarnation >= self's (a regressed boot-millis seed) — is refuted:
// self bumps ABOVE the stale record and re-asserts its true current address, with no death window.
func TestSelfRefutesStaleAddressRecord(t *testing.T) {
	base := time.Unix(9_100_000, 0)
	r := NewRoster(addrMember("self", "10.0.0.9"), testCfg(), 100) // true current addr .9
	// stale self-record at the OLD address .1, Alive, at self's incarnation (regressed-seed case).
	r.ApplyGossip(MemberView{Member: addrMember("self", "10.0.0.1"), State: StateAlive, Incarnation: 100}, base)
	v, _ := r.Get("self")
	if v.Incarnation <= 100 {
		t.Fatalf("self must bump ABOVE a stale self-record's incarnation to supersede it, got %d", v.Incarnation)
	}
	if v.Member.DataAddr != "10.0.0.9:7800" || v.Member.GossipAddr != "10.0.0.9:7700" {
		t.Fatalf("self must re-assert its TRUE current address, got %+v", v.Member)
	}
}

// TestSelfNoInflationOnMatchingRecord is the critical inflation guard: a self-record that already MATCHES
// self (Alive + same address) at equal incarnation triggers NO bump — a converged cluster must not inflate
// self's incarnation every gossip round.
func TestSelfNoInflationOnMatchingRecord(t *testing.T) {
	base := time.Unix(9_200_000, 0)
	self := addrMember("self", "10.0.0.9")
	r := NewRoster(self, testCfg(), 100)
	for i := 0; i < 3; i++ {
		r.ApplyGossip(MemberView{Member: self, State: StateAlive, Incarnation: 100}, base)
		if v, _ := r.Get("self"); v.Incarnation != 100 {
			t.Fatalf("matching self-record must NOT inflate incarnation (round %d): got %d, want 100", i, v.Incarnation)
		}
	}
}

// TestMembersSnapshotCachedAndImmutable verifies Members()/Live() return a cached snapshot that is
// reused between mutations and never mutated in place (a previously-returned slice stays valid).
func TestMembersSnapshotCachedAndImmutable(t *testing.T) {
	now := time.Now()
	r := NewRoster(mem("self"), testCfg(), 0)
	r.Upsert(mem("a"), now)

	s1 := r.Members()
	if len(s1) != 2 {
		t.Fatalf("Members = %d, want 2 (self+a)", len(s1))
	}
	if &r.Members()[0] != &s1[0] {
		t.Fatal("Members() reallocated the snapshot without a mutation")
	}

	r.Upsert(mem("b"), now) // mutation: must not touch the already-returned s1
	if len(s1) != 2 {
		t.Fatalf("previously-returned snapshot changed to len %d", len(s1))
	}
	if got := r.Members(); len(got) != 3 {
		t.Fatalf("Members after upsert = %d, want 3", len(got))
	}

	if got := r.Live(); len(got) != 3 {
		t.Fatalf("Live = %d, want 3 alive", len(got))
	}
	r.Suspect("a", now)
	if got := r.Live(); len(got) != 2 {
		t.Fatalf("Live after suspecting a = %d, want 2", len(got))
	}
	if got := r.Members(); len(got) != 3 {
		t.Fatalf("Members after suspecting a = %d, want 3 (suspect still listed)", len(got))
	}
}
