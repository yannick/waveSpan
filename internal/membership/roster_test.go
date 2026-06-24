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
	r := NewRoster(mem("self"), testCfg())
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

func TestDeadRetainedUntilRepairAndRetention(t *testing.T) {
	base := time.Unix(2_000_000, 0)
	r := NewRoster(mem("self"), testCfg())
	r.Upsert(mem("p1"), base)
	r.Suspect("p1", base)
	r.Tick(at(base, 4*time.Second))  // -> UNREACHABLE
	r.Tick(at(base, 15*time.Second)) // -> DEAD
	if stateOf(t, r, "p1") != StateDead {
		t.Fatal("expected DEAD")
	}
	// retention elapsed but repair NOT complete: must stay DEAD (holder records needed)
	r.Tick(at(base, 2*time.Minute))
	if got := stateOf(t, r, "p1"); got != StateDead {
		t.Fatalf("dead member forgotten before repair complete: %s", got)
	}
	// repair complete + retention -> FORGOTTEN (drops out of Members())
	r.MarkRepairComplete("p1")
	r.Tick(at(base, 4*time.Minute))
	if _, ok := r.Get("p1"); !ok {
		t.Fatal("get should still resolve")
	}
	if stateOf(t, r, "p1") != StateForgotten {
		t.Fatal("expected FORGOTTEN after repair+retention")
	}
	for _, m := range r.Members() {
		if m.Member.MemberID == "p1" {
			t.Fatal("forgotten member should not appear in Members()")
		}
	}
}

func TestObserveAckRevivesSuspect(t *testing.T) {
	base := time.Unix(3_000_000, 0)
	r := NewRoster(mem("self"), testCfg())
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
	r := NewRoster(mem("self"), testCfg())
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
	r := NewRoster(mem("self"), testCfg())
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

// TestMembersSnapshotCachedAndImmutable verifies Members()/Live() return a cached snapshot that is
// reused between mutations and never mutated in place (a previously-returned slice stays valid).
func TestMembersSnapshotCachedAndImmutable(t *testing.T) {
	now := time.Now()
	r := NewRoster(mem("self"), testCfg())
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
