package global

import (
	"strconv"
	"sync"

	"github.com/cwire/wavespan/internal/conflict"
	"github.com/cwire/wavespan/internal/vector"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// RecordStore is the local record primitive the applier writes through (recordstore.Store).
type RecordStore interface {
	GetRecord(namespace string, key []byte) (*wavespanv1.StoredRecord, bool, error)
	Apply(rec *wavespanv1.StoredRecord, kind wavespanv1.MutationKind) (version.Version, error)
	ApplySiblings(namespace string, key []byte, siblings []*wavespanv1.StoredRecord) error
}

// Applier applies inbound GlobalMutations into the local store through the namespace's conflict
// resolver, idempotently by mutation id (design/06). A replayed mutation is a no-op.
type Applier struct {
	store     RecordStore
	registry  *conflict.Registry
	policyFor func(namespace string) string

	mu         sync.Mutex
	applied    map[string]bool // mutation_id -> applied (dedupe / replay protection)
	vectorSink func(*wavespanv1.VectorRecord)
	onApply    func(namespace string, key []byte, rec *wavespanv1.StoredRecord)
}

// SetOnApply installs a hook invoked after a successful inbound KV apply, so the receiving node can
// spread the record within its local cluster (fanout) and advertise the holder — making a
// replicate-everywhere namespace stay everywhere across the global boundary.
func (a *Applier) SetOnApply(fn func(namespace string, key []byte, rec *wavespanv1.StoredRecord)) {
	a.onApply = fn
}

// SetVectorSink routes applied raw-vector mutations (reserved namespace) to the local vector store
// and index instead of the KV recordstore (design/08 TS-084). Only raw records cross the wire.
func (a *Applier) SetVectorSink(sink func(*wavespanv1.VectorRecord)) { a.vectorSink = sink }

// NewApplier wires an applier. policyFor maps a namespace to its conflict-policy name (empty =>
// hlc-last-write-wins).
func NewApplier(store RecordStore, registry *conflict.Registry, policyFor func(namespace string) string) *Applier {
	if registry == nil {
		registry = conflict.NewRegistry()
	}
	if policyFor == nil {
		policyFor = func(string) string { return conflict.PolicyHLCLastWriteWins }
	}
	return &Applier{store: store, registry: registry, policyFor: policyFor, applied: map[string]bool{}}
}

func mutationKey(id *wavespanv1.GlobalMutationId) string {
	return id.GetClusterId() + "\x00" + id.GetMemberId() + "\x00" + strconv.FormatUint(id.GetWriterSequence(), 10)
}

// Applied reports whether a mutation id has already been applied.
func (a *Applier) Applied(id *wavespanv1.GlobalMutationId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applied[mutationKey(id)]
}

// Apply applies one inbound mutation. It returns true if it was newly applied (false on a replay).
// The applied record keeps the origin expiry — TTL is never recomputed from apply time (design/06).
func (a *Applier) Apply(m *wavespanv1.GlobalMutation) (bool, error) {
	id := mutationKey(m.GetId())
	a.mu.Lock()
	if a.applied[id] {
		a.mu.Unlock()
		return false, nil
	}
	a.mu.Unlock()

	ns, key := m.GetNamespace(), m.GetKey()
	incoming := m.GetRecord()

	// raw vectors route to the vector store/index, not the KV recordstore (TS-084).
	if vector.IsMutationNamespace(ns) {
		if a.vectorSink != nil {
			if vrec, derr := vector.Unwrap(incoming); derr == nil {
				a.vectorSink(vrec)
			}
		}
		a.mu.Lock()
		a.applied[id] = true
		a.mu.Unlock()
		return true, nil
	}

	var existing []*wavespanv1.StoredRecord
	if rec, found, err := a.store.GetRecord(ns, key); err == nil && found {
		existing = []*wavespanv1.StoredRecord{rec}
	}

	res := a.registry.Resolver(a.policyFor(ns)).Resolve(existing, incoming)
	var err error
	switch res.Kind {
	case conflict.KindWinner:
		_, err = a.store.Apply(res.Record, wavespanv1.MutationKind_MUTATION_KIND_PUT)
	case conflict.KindTombstone:
		_, err = a.store.Apply(res.Record, wavespanv1.MutationKind_MUTATION_KIND_DELETE)
	case conflict.KindSiblings:
		err = a.store.ApplySiblings(ns, key, res.Siblings)
	case conflict.KindReject:
		// nothing to apply
	}
	if err != nil {
		return false, err
	}
	a.mu.Lock()
	a.applied[id] = true
	a.mu.Unlock()
	// Inbound cross-cluster writes land on ONE local node; the hook lets the node spread them within
	// the local cluster (e.g. fanout an "everywhere" namespace to all local nodes) + advertise the
	// holder, exactly as a locally-originated write does. Without it, a replicate-everywhere namespace
	// would not stay "everywhere" across the global boundary.
	if a.onApply != nil && res.Kind != conflict.KindReject {
		a.onApply(ns, key, res.Record)
	}
	return true, nil
}
