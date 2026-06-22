package conflict

import wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"

// Deferred conflict policies. v1 ships only hlc-last-write-wins and keep-siblings (ADR-0004,
// design/06 "CRDT policies"/"Application resolver"). The CRDT and application/WASM resolvers are
// specified as part of the Resolver surface but intentionally NOT implemented in v1; these
// stubs keep the policy names visible and fail loudly if ever wired up by mistake.

// Deferred policy names (not registered; not invoked in v1 paths).
const (
	PolicyCRDTGrowOnlyCounter = "crdt-g-counter"
	PolicyCRDTPNCounter       = "crdt-pn-counter"
	PolicyCRDTORSet           = "crdt-or-set"
	PolicyCRDTLWWRegister     = "crdt-lww-register"
	PolicyApplicationResolver = "application"
)

// deferredResolver panics if invoked — it documents an unimplemented v1 policy.
type deferredResolver struct{ policy string }

func (d deferredResolver) Resolve([]*wavespanv1.StoredRecord, *wavespanv1.StoredRecord) ResolveResult {
	panic("conflict: policy " + d.policy + " is deferred — v1 ships hlc-last-write-wins + keep-siblings only")
}

var _ Resolver = deferredResolver{} // the deferred stub still satisfies the Resolver surface

// DeferredPolicies lists the conflict policies specified but not implemented in v1. They are not
// registered, so requesting one falls back to hlc-last-write-wins rather than panicking.
func DeferredPolicies() []string {
	return []string{
		PolicyCRDTGrowOnlyCounter, PolicyCRDTPNCounter, PolicyCRDTORSet,
		PolicyCRDTLWWRegister, PolicyApplicationResolver,
	}
}
