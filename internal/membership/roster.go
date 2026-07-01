package membership

import (
	"sort"
	"sync"
	"time"
)

// memberState is the roster's mutable record for one member.
type memberState struct {
	member      Member
	state       State
	incarnation uint64
	lastSeenMs  int64
	stateSince  int64
	repairDone  bool
}

// MemberView is an immutable snapshot of a member's roster record.
type MemberView struct {
	Member      Member
	State       State
	Incarnation uint64
	LastSeenMs  int64
}

// Roster tracks cluster members and their liveness. It is safe for concurrent use. Liveness
// advances on Tick by the configured timeouts; suspicion and refutation follow SWIM incarnation
// rules (design/04 "Liveness states").
type Roster struct {
	mu       sync.RWMutex
	selfID   string
	members  map[string]*memberState
	cfg      LivenessConfig
	observer func(memberID string, newState State) // optional liveness-transition observer (M13)

	// Cached, sorted snapshots returned by Members()/Live(). Rebuilt under the write lock on every
	// mutation, so the hot read path (per-request placement/fanout/fetch) returns a shared immutable
	// slice with no per-call allocation or sort. Each rebuild creates fresh slices, so references
	// handed to callers stay valid after a later mutation.
	membersSnap []MemberView
	liveSnap    []MemberView
}

// SetStateObserver installs a callback invoked on every liveness transition (for the observability
// gossip tap). It is called without the roster lock held.
func (r *Roster) SetStateObserver(fn func(memberID string, newState State)) {
	r.mu.Lock()
	r.observer = fn
	r.mu.Unlock()
}

// NewRoster creates a roster seeded with the local member as ALIVE at selfIncarnation.
//
// selfIncarnation MUST be monotonic across restarts of the same MemberID (production seeds it from
// boot-time unix-millis; see NewService). SWIM propagates an address change only on a STRICTLY HIGHER
// incarnation (ApplyGossip), so a restarted pod that re-announces at incarnation 0 (a fresh counter) is
// rejected by peers still holding the old (higher) incarnation — they keep probing its dead old address.
// A monotonic seed guarantees the restart out-incarnates the prior generation, so the new address is
// accepted and epidemically propagated. Boot-time millis (not a persisted counter) is used deliberately:
// on a spot node the storage volume — and any counter persisted beside the storage UUID — is fresh on
// reschedule, which would reset the counter on exactly the restart that needs a higher incarnation; the
// wall clock is not. Tests pass 0 to preserve their controlled incarnation assumptions.
func NewRoster(self Member, cfg LivenessConfig, selfIncarnation uint64) *Roster {
	r := &Roster{selfID: self.MemberID, members: map[string]*memberState{}, cfg: cfg}
	r.members[self.MemberID] = &memberState{member: self, state: StateAlive, incarnation: selfIncarnation}
	r.rebuildSnapshots()
	return r
}

// rebuildSnapshots recomputes the cached Members()/Live() slices. The caller must hold r.mu (write
// lock). Fresh slices are allocated each time so previously-returned snapshots remain immutable.
func (r *Roster) rebuildSnapshots() {
	members := make([]MemberView, 0, len(r.members))
	live := make([]MemberView, 0, len(r.members))
	for _, ms := range r.members {
		if ms.state == StateForgotten {
			continue
		}
		v := view(ms)
		members = append(members, v)
		if ms.state == StateAlive {
			live = append(live, v)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Member.MemberID < members[j].Member.MemberID })
	sort.Slice(live, func(i, j int) bool { return live[i].Member.MemberID < live[j].Member.MemberID })
	r.membersSnap = members
	r.liveSnap = live
}

// Self returns the local member.
func (r *Roster) Self() Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.members[r.selfID].member
}

// Upsert adds a member discovered via seeds or gossip, as ALIVE if not already known. Existing
// members keep their state; only addressing/topology metadata is refreshed.
func (r *Roster) Upsert(m Member, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.rebuildSnapshots()
	ms, ok := r.members[m.MemberID]
	if !ok {
		r.members[m.MemberID] = &memberState{member: m, state: StateAlive, lastSeenMs: unixMs(now), stateSince: unixMs(now)}
		return
	}
	// NOTE: this refreshes an existing member's address UNCONDITIONALLY (no incarnation check), unlike
	// ApplyGossip. It is fed from a live gossip contact's From (HandleGossip), so it tracks the sender's
	// current address; the incarnation-gated propagation to third parties is ApplyGossip's job. A stale
	// duplicate from an old address could momentarily flap the address here, but the owner's monotonic
	// self-incarnation (ApplyGossip self branch) re-asserts the truth on the next round.
	ms.member = m
}

// ObserveAck records a successful direct/indirect ping: the member is ALIVE and any local
// suspicion is refuted by bumping its incarnation so the refutation propagates.
func (r *Roster) ObserveAck(id string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.rebuildSnapshots()
	ms, ok := r.members[id]
	if !ok {
		return
	}
	ms.lastSeenMs = unixMs(now)
	if ms.state != StateAlive {
		ms.incarnation++
		r.setState(ms, StateAlive, now)
	}
}

// Suspect records a failed probe: an ALIVE member becomes SUSPECT (design/04 ALIVE->SUSPECT).
func (r *Roster) Suspect(id string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.rebuildSnapshots()
	ms, ok := r.members[id]
	if !ok || id == r.selfID {
		return
	}
	if ms.state == StateAlive {
		r.setState(ms, StateSuspect, now)
	}
}

// ApplyGossip merges a member's state learned from a peer's delta. Higher incarnation always
// wins; equal incarnation adopts the more severe state. A stale record about self — a non-Alive claim
// OR a stale address — is refuted by out-incarnating it (see the self branch).
func (r *Roster) ApplyGossip(u MemberView, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.rebuildSnapshots()

	if u.Member.MemberID == r.selfID {
		self := r.members[r.selfID]
		// Refute a STALE record about ourselves: a non-Alive claim (classic SWIM suspicion) OR a stale
		// ADDRESS (a peer still remembering our pre-restart address). Bumping ABOVE the stale incarnation
		// makes our true {Alive, current Member} record supersede it when we next gossip — so even a
		// regressed/low boot-millis seed self-heals immediately, with no death window (risk-2 hardening).
		// The bump requires u.Incarnation >= ours; a strictly-lower stale record is already dominated, so
		// we never LOWER our incarnation (monotonic guarantee).
		//
		// Inflation guard (critical): a record that already MATCHES us — Alive AND our current Member — is
		// NOT stale, so it triggers no bump. Once peers converge on our {incarnation, addr}, gossip rounds
		// stop bumping; otherwise self-incarnation would inflate every round.
		stale := u.State != StateAlive || u.Member != self.member
		if stale && u.Incarnation >= self.incarnation {
			self.incarnation = u.Incarnation + 1
			r.setState(self, StateAlive, now) // re-assert Alive (self.member is already our true identity)
		}
		return
	}

	ms, ok := r.members[u.Member.MemberID]
	if !ok {
		r.members[u.Member.MemberID] = &memberState{
			member: u.Member, state: u.State, incarnation: u.Incarnation,
			lastSeenMs: unixMs(now), stateSince: unixMs(now),
		}
		return
	}
	switch {
	case u.Incarnation > ms.incarnation:
		ms.incarnation = u.Incarnation
		ms.member = u.Member
		r.setState(ms, u.State, now)
	case u.Incarnation == ms.incarnation && u.State > ms.state:
		r.setState(ms, u.State, now)
	}
}

// Tick advances timeout-driven transitions: SUSPECT->UNREACHABLE->DEAD->FORGOTTEN (a retained,
// non-gossiped tombstone)->deleted (after a further ForgottenRetention).
func (r *Roster) Tick(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.rebuildSnapshots()
	nowMs := unixMs(now)
	for id, ms := range r.members {
		if id == r.selfID {
			continue
		}
		switch ms.state {
		case StateSuspect:
			if nowMs-ms.stateSince >= r.cfg.SuspicionTimeout.Milliseconds() {
				r.setState(ms, StateUnreachable, now)
			}
		case StateUnreachable:
			if nowMs-ms.stateSince >= r.cfg.UnreachableTimeout.Milliseconds() {
				r.setState(ms, StateDead, now)
			}
		case StateDead:
			// Stage 1: DEAD -> FORGOTTEN after DeadRetention (the guaranteed time-based backstop) OR earlier
			// once repair has released this member's holder records (repairDone). The entry is RETAINED as a
			// non-gossiped tombstone — rebuildSnapshots excludes FORGOTTEN from Members()/Live(), and
			// outgoing gossip is sourced from Members(), so we hold it locally without spreading it. This
			// stops a churned cluster's dead members from lingering in Members() forever (which staled the
			// roster and forced backups to PARTIAL) while keeping the tombstone that blocks resurrection.
			if nowMs-ms.stateSince >= r.cfg.DeadRetention.Milliseconds() || ms.repairDone {
				r.setState(ms, StateForgotten, now)
			}
		case StateForgotten:
			// Stage 2: FORGOTTEN -> deleted after a further ForgottenRetention, reclaiming the map entry so
			// the roster can't grow unbounded (total tombstone lifetime ≈ DeadRetention + ForgottenRetention).
			// While the tombstone lives, ApplyGossip's ordering rejects a stale ALIVE (state ordinal < the
			// FORGOTTEN tombstone's, incarnation <= it) at the dead old address; a genuine higher-incarnation
			// revival still supersedes it. Residual (inherent to SWIM): a peer partitioned LONGER than this
			// full window can still transiently re-add the member — it re-dies within ~one liveness window
			// (SUSPECT+UNREACHABLE), and F5 keeps backups tolerant (a stale member is skipped → PARTIAL, not
			// fatal). A truly-dead member cannot refute for itself, so no finite tombstone fully closes this.
			if nowMs-ms.stateSince >= r.cfg.ForgottenRetention.Milliseconds() {
				delete(r.members, id)
			}
		}
	}
}

// MarkRepairComplete signals that repair no longer needs a dead member's holder records, so it may be
// forgotten EARLY — before the DeadRetention backstop that evicts it regardless (see Tick). Optional: a
// dead member is always evicted after retention even if this is never called.
func (r *Roster) MarkRepairComplete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.rebuildSnapshots()
	if ms, ok := r.members[id]; ok {
		ms.repairDone = true
	}
}

// Members returns all members except those forgotten, sorted by memberId. The returned slice is a
// shared immutable snapshot (rebuilt only on roster mutation) — callers must not mutate it.
func (r *Roster) Members() []MemberView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.membersSnap
}

// Live returns members currently ALIVE, sorted by memberId. Shared immutable snapshot (see Members).
func (r *Roster) Live() []MemberView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.liveSnap
}

// Get returns a member's view by id.
func (r *Roster) Get(id string) (MemberView, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ms, ok := r.members[id]
	if !ok {
		return MemberView{}, false
	}
	return view(ms), true
}

func (r *Roster) setState(ms *memberState, s State, now time.Time) {
	if ms.state != s {
		ms.state = s
		ms.stateSince = unixMs(now)
		if r.observer != nil {
			// the observer (gossip tap -> ring) is non-blocking by contract, so calling it while
			// the roster lock is held is safe (it never blocks or re-enters the roster).
			r.observer(ms.member.MemberID, s)
		}
	}
}

func view(ms *memberState) MemberView {
	return MemberView{Member: ms.member, State: ms.state, Incarnation: ms.incarnation, LastSeenMs: ms.lastSeenMs}
}

func unixMs(t time.Time) int64 { return t.UnixMilli() }
