package backup

import (
	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/vector"
)

// funcContributor is a small generic Contributor: a name, its CF specs, an
// optional rebuild hook, and an optional selection matcher. In Phase 2a every
// rebuild hook is nil (no-op) — derived index CFs are copied verbatim; Phase 2b
// flips graph/vector index CFs to derived and wires real rebuild hooks here.
type funcContributor struct {
	name    string
	cfs     []CFSpec
	rebuild func(dst storage.LocalStore, ri RestoreInfo) error
	// selects decides partial-backup inclusion for a key. nil means "always
	// include" (e.g. system config). Consulted only when the selector is non-empty.
	selects func(cf storage.ColumnFamily, key []byte, sel Selector) bool
}

func (f funcContributor) Name() string  { return f.name }
func (f funcContributor) CFs() []CFSpec { return f.cfs }
func (f funcContributor) RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error {
	if f.rebuild == nil {
		return nil
	}
	return f.rebuild(dst, ri)
}
func (f funcContributor) Selects(cf storage.ColumnFamily, key []byte, sel Selector) bool {
	if f.selects == nil {
		return true
	}
	return f.selects(cf, key, sel)
}

// DefaultRegistry registers the five built-in contributors covering every
// non-transient column family. CFReplLog and CFCacheMeta are owned by no
// contributor and therefore never exported.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	// system: CFSys is cluster config/identity — always backed up (selects nil).
	r.Register(funcContributor{name: "system", cfs: []CFSpec{{storage.CFSys, true}}})
	r.Register(funcContributor{
		name: "kv", cfs: []CFSpec{{storage.CFKVData, true}, {storage.CFKVMeta, true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			ns, ok := recordstore.NamespaceOfKey(key)
			return ok && contains(sel.Namespaces, ns)
		},
	})
	r.Register(funcContributor{
		name: "collections", cfs: []CFSpec{{storage.CFReplData, true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			ns, _, ok := collections.NamespaceCollectionOfKey(key)
			return ok && contains(sel.Namespaces, ns)
		},
	})
	// TODO(phase2b): flip CFGraphIndex to derived (Authoritative: false) + wire rebuild here.
	r.Register(funcContributor{
		name: "graph", cfs: []CFSpec{{storage.CFGraphData, true}, {storage.CFGraphIndex, true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			g, ok := graph.OfKey(key)
			return ok && contains(sel.Graphs, g)
		},
	})
	// TODO(phase2b): flip CFVectorIndex to derived (Authoritative: false) + wire rebuild here
	// (also solve vector index-spec capture, which lives in config/CRD not in the backed-up data).
	r.Register(funcContributor{
		name: "vector", cfs: []CFSpec{{storage.CFVectorRaw, true}, {storage.CFVectorIndex, true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			c, ok := vector.CollectionOfKey(key)
			return ok && contains(sel.VectorCollections, c)
		},
	})
	return r
}
