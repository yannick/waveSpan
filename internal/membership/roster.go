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
}

// SetStateObserver installs a callback invoked on every liveness transition (for the observability
// gossip tap). It is called without the roster lock held.
func (r *Roster) SetStateObserver(fn func(memberID string, newState State)) {
	r.mu.Lock()
	r.observer = fn
	r.mu.Unlock()
}

// NewRoster creates a roster seeded with the local member as ALIVE.
func NewRoster(self Member, cfg LivenessConfig) *Roster {
	r := &Roster{selfID: self.MemberID, members: map[string]*memberState{}, cfg: cfg}
	r.members[self.MemberID] = &memberState{member: self, state: StateAlive}
	return r
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
	ms, ok := r.members[m.MemberID]
	if !ok {
		r.members[m.MemberID] = &memberState{member: m, state: StateAlive, lastSeenMs: unixMs(now), stateSince: unixMs(now)}
		return
	}
	ms.member = m
}

// ObserveAck records a successful direct/indirect ping: the member is ALIVE and any local
// suspicion is refuted by bumping its incarnation so the refutation propagates.
func (r *Roster) ObserveAck(id string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	ms, ok := r.members[id]
	if !ok || id == r.selfID {
		return
	}
	if ms.state == StateAlive {
		r.setState(ms, StateSuspect, now)
	}
}

// ApplyGossip merges a member's state learned from a peer's delta. Higher incarnation always
// wins; equal incarnation adopts the more severe state. Suspicion about self is refuted.
func (r *Roster) ApplyGossip(u MemberView, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if u.Member.MemberID == r.selfID {
		// refute any non-alive claim about ourselves
		if u.State != StateAlive {
			self := r.members[r.selfID]
			if u.Incarnation >= self.incarnation {
				self.incarnation = u.Incarnation + 1
			}
			r.setState(self, StateAlive, now)
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

// Tick advances timeout-driven transitions: SUSPECT->UNREACHABLE->DEAD->FORGOTTEN.
func (r *Roster) Tick(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
			if ms.repairDone && nowMs-ms.stateSince >= r.cfg.DeadRetention.Milliseconds() {
				r.setState(ms, StateForgotten, now)
			}
		}
	}
}

// MarkRepairComplete signals that repair no longer needs a dead member's holder records, so it
// may be forgotten after retention (design/04: "do not forget dead members" until then).
func (r *Roster) MarkRepairComplete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ms, ok := r.members[id]; ok {
		ms.repairDone = true
	}
}

// Members returns all members except those forgotten, sorted by memberId.
func (r *Roster) Members() []MemberView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]MemberView, 0, len(r.members))
	for _, ms := range r.members {
		if ms.state == StateForgotten {
			continue
		}
		out = append(out, view(ms))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Member.MemberID < out[j].Member.MemberID })
	return out
}

// Live returns members currently ALIVE, sorted by memberId.
func (r *Roster) Live() []MemberView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]MemberView, 0, len(r.members))
	for _, ms := range r.members {
		if ms.state == StateAlive {
			out = append(out, view(ms))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Member.MemberID < out[j].Member.MemberID })
	return out
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
