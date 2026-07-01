package membership

import "time"

// State is a member's liveness state (design/04_membership_latency_gossip.md "Liveness states").
//
//	ALIVE -> SUSPECT -> UNREACHABLE -> DEAD -> FORGOTTEN
type State int

const (
	// StateAlive is reachable and healthy.
	StateAlive State = iota
	// StateSuspect has missed pings; not yet confirmed gone.
	StateSuspect
	// StateUnreachable has timed out of suspicion; excluded from new durable placement.
	StateUnreachable
	// StateDead is confirmed gone, but retained because repair still needs its holder records.
	StateDead
	// StateForgotten has elapsed retention with repair complete; safe to drop.
	StateForgotten
)

func (s State) String() string {
	switch s {
	case StateAlive:
		return "ALIVE"
	case StateSuspect:
		return "SUSPECT"
	case StateUnreachable:
		return "UNREACHABLE"
	case StateDead:
		return "DEAD"
	case StateForgotten:
		return "FORGOTTEN"
	default:
		return "UNKNOWN"
	}
}

// LivenessConfig holds the timeouts that drive state advancement.
type LivenessConfig struct {
	// SuspicionTimeout: SUSPECT -> UNREACHABLE after this long without a successful ping.
	SuspicionTimeout time.Duration
	// UnreachableTimeout: UNREACHABLE -> DEAD after this much additional time.
	UnreachableTimeout time.Duration
	// DeadRetention: DEAD -> FORGOTTEN after this long (or earlier once repair is complete).
	DeadRetention time.Duration
	// ForgottenRetention: FORGOTTEN -> deleted after this much ADDITIONAL time. The FORGOTTEN entry is a
	// non-gossiped tombstone (excluded from Members()/Live() and thus outgoing gossip) that blocks a
	// lagging peer's stale ALIVE from resurrecting the member at its dead old address; deleting it after
	// this window bounds roster growth. Total tombstone lifetime ≈ DeadRetention + ForgottenRetention.
	ForgottenRetention time.Duration
}

// DefaultLivenessConfig returns timeouts tuned for fast spot-churn detection (design/04
// "Spot node handling": mark SUSPECT quickly).
func DefaultLivenessConfig() LivenessConfig {
	return LivenessConfig{
		SuspicionTimeout:   3 * time.Second,
		UnreachableTimeout: 10 * time.Second,
		DeadRetention:      5 * time.Minute,
		ForgottenRetention: 5 * time.Minute,
	}
}
