// Package conflict resolves concurrent versions of a record into a single outcome (design/06
// "Conflict policies"). v1 ships two resolvers — hlc-last-write-wins (default) and keep-siblings;
// CRDT and application resolvers are interface-only/deferred (deferred.go).
package conflict

import (
	"sync"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// ResultKind is the kind of a resolution outcome (mirrors design/06's ResolveResult enum).
type ResultKind int

const (
	// KindWinner is a single record winning.
	KindWinner ResultKind = iota
	// KindTombstone is a winning tombstone (delete wins).
	KindTombstone
	// KindSiblings keeps multiple concurrent records.
	KindSiblings
	// KindReject rejects the incoming mutation.
	KindReject
)

// ResolveResult is the outcome of a conflict resolution.
type ResolveResult struct {
	Kind     ResultKind
	Record   *wavespanv1.StoredRecord   // for KindWinner / KindTombstone
	Siblings []*wavespanv1.StoredRecord // for KindSiblings (sorted, deterministic)
	Reason   string                     // for KindReject
}

// Resolver merges the existing local records for a key with an incoming record into a
// single deterministic result. Implementations must be deterministic: the same inputs (in any
// order) yield the same result on every node, so all clusters converge.
type Resolver interface {
	Resolve(existing []*wavespanv1.StoredRecord, incoming *wavespanv1.StoredRecord) ResolveResult
}

// Policy names registered in v1.
const (
	PolicyHLCLastWriteWins = "hlc-last-write-wins"
	PolicyKeepSiblings     = "keep-siblings"
)

// Registry maps a policy name to a resolver.
type Registry struct {
	mu        sync.RWMutex
	resolvers map[string]Resolver
	fallback  Resolver
}

// NewRegistry builds a registry with the v1 resolvers registered and hlc-last-write-wins as the
// default fallback for unknown/unset policies.
func NewRegistry() *Registry {
	r := &Registry{resolvers: map[string]Resolver{}}
	lww := HLCLastWriteWins{}
	r.resolvers[PolicyHLCLastWriteWins] = lww
	r.resolvers[PolicyKeepSiblings] = KeepSiblings{}
	r.fallback = lww
	return r
}

// Register adds or replaces a resolver for a policy name.
func (r *Registry) Register(policy string, resolver Resolver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolvers[policy] = resolver
}

// Resolver returns the resolver for a policy, or the default (hlc-last-write-wins) when the policy
// is empty or unknown.
func (r *Registry) Resolver(policy string) Resolver {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if res, ok := r.resolvers[policy]; ok {
		return res
	}
	return r.fallback
}
