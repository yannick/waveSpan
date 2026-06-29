package backup

import "github.com/yannick/wavespan/internal/storage"

// funcContributor is a small generic Contributor: a name, its CF specs, and an
// optional rebuild hook. In Phase 2a every rebuild hook is nil (no-op) — derived
// index CFs are copied verbatim; Phase 2b flips graph/vector index CFs to derived
// and wires real rebuild hooks here.
type funcContributor struct {
	name    string
	cfs     []CFSpec
	rebuild func(dst storage.LocalStore, ri RestoreInfo) error
}

func (f funcContributor) Name() string  { return f.name }
func (f funcContributor) CFs() []CFSpec { return f.cfs }
func (f funcContributor) RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error {
	if f.rebuild == nil {
		return nil
	}
	return f.rebuild(dst, ri)
}

// DefaultRegistry registers the five built-in contributors covering every
// non-transient column family. CFReplLog and CFCacheMeta are owned by no
// contributor and therefore never exported.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(funcContributor{name: "system", cfs: []CFSpec{{storage.CFSys, true}}})
	r.Register(funcContributor{name: "kv", cfs: []CFSpec{{storage.CFKVData, true}, {storage.CFKVMeta, true}}})
	r.Register(funcContributor{name: "collections", cfs: []CFSpec{{storage.CFReplData, true}}})
	// TODO(phase2b): flip CFGraphIndex to derived (Authoritative: false) + wire rebuild here.
	r.Register(funcContributor{name: "graph", cfs: []CFSpec{{storage.CFGraphData, true}, {storage.CFGraphIndex, true}}})
	// TODO(phase2b): flip CFVectorIndex to derived (Authoritative: false) + wire rebuild here
	// (also solve vector index-spec capture, which lives in config/CRD not in the backed-up data).
	r.Register(funcContributor{name: "vector", cfs: []CFSpec{{storage.CFVectorRaw, true}, {storage.CFVectorIndex, true}}})
	return r
}
