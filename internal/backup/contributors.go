package backup

import (
	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/vector"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
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
	// versionOf decodes a record's HLC version (Phase 3a.1). nil means "no version"
	// (→ ok=false): system config, derived/index CFs, and the raft-consistent
	// collections CF.
	versionOf func(cf storage.ColumnFamily, key, value []byte) (version.Version, bool)
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
func (f funcContributor) VersionOf(cf storage.ColumnFamily, key, value []byte) (version.Version, bool) {
	if f.versionOf == nil {
		return version.Version{}, false
	}
	return f.versionOf(cf, key, value)
}

// kvVersionOf decodes the HLC version from a CFKVData StoredRecord value. CFKVMeta carries no per-record
// version (it is the derived latest-pointer index) → ok=false.
func kvVersionOf(cf storage.ColumnFamily, _ []byte, value []byte) (version.Version, bool) {
	if cf != storage.CFKVData {
		return version.Version{}, false
	}
	rec, err := storage.DecodeStoredRecord(value)
	if err != nil {
		return version.Version{}, false
	}
	return version.FromProto(rec.GetVersion()), true
}

// graphVersionOf decodes a CFGraphData node/edge record's version (classified by the key's leading byte:
// 'n' = node, 'e' = edge — see graph.NodeKey/EdgeKey). The derived CFGraphIndex (label/property/adjacency
// keys) carries no record version → ok=false.
func graphVersionOf(cf storage.ColumnFamily, key, value []byte) (version.Version, bool) {
	if cf != storage.CFGraphData || len(key) == 0 {
		return version.Version{}, false
	}
	switch key[0] {
	case 'n':
		var n wavespanv1.NodeRecord
		if proto.Unmarshal(value, &n) == nil {
			return version.FromProto(n.GetVersion()), true
		}
	case 'e':
		var e wavespanv1.EdgeRecord
		if proto.Unmarshal(value, &e) == nil {
			return version.FromProto(e.GetVersion()), true
		}
	}
	return version.Version{}, false
}

// vectorVersionOf decodes a CFVectorRaw VectorRecord's version. The derived CFVectorIndex → ok=false.
func vectorVersionOf(cf storage.ColumnFamily, _ []byte, value []byte) (version.Version, bool) {
	if cf != storage.CFVectorRaw {
		return version.Version{}, false
	}
	var v wavespanv1.VectorRecord
	if proto.Unmarshal(value, &v) == nil {
		return version.FromProto(v.GetVersion()), true
	}
	return version.Version{}, false
}

// DefaultRegistry registers the five built-in contributors covering every
// non-transient column family. CFReplLog and CFCacheMeta are owned by no
// contributor and therefore never exported.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	// system: CFSys is cluster config/identity — always backed up (selects nil).
	r.Register(funcContributor{name: "system", cfs: []CFSpec{{CF: storage.CFSys, Authoritative: true}}})
	// kv: CFKVData is authoritative. CFKVMeta is authoritative too — a FULL backup exports it verbatim so
	// the latest-pointer's SiblingVersions / conflict-tracking state survives (the LWW rebuild cannot
	// reconstruct it). It is flagged RebuildWhenCut: an HLC ≤T cut skips it on export and rebuilds it on
	// restore from the surviving (≤T) CFKVData via recordstore.RebuildMetaIfAbsent — copying the pointers
	// verbatim while the cut dropped their >T winners would dangle the pointer → silent key loss. A ≤T cut
	// therefore collapses concurrent siblings to the LWW winner (documented limitation, design/backup §5.2);
	// full backups preserve them.
	r.Register(funcContributor{
		name: "kv", cfs: []CFSpec{{CF: storage.CFKVData, Authoritative: true}, {CF: storage.CFKVMeta, Authoritative: true, RebuildWhenCut: true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			ns, ok := recordstore.NamespaceOfKey(key)
			return ok && contains(sel.Namespaces, ns)
		},
		versionOf: kvVersionOf,
		// Rebuild only when CFKVMeta is ABSENT (a ≤T cut omitted it). A full backup restored CFKVMeta
		// verbatim → present → this is a no-op, preserving siblings/conflict state.
		rebuild: func(dst storage.LocalStore, _ RestoreInfo) error { return recordstore.RebuildMetaIfAbsent(dst) },
	})
	r.Register(funcContributor{
		name: "collections", cfs: []CFSpec{{CF: storage.CFReplData, Authoritative: true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			ns, _, ok := collections.NamespaceCollectionOfKey(key)
			return ok && contains(sel.Namespaces, ns)
		},
	})
	// TODO(phase2b): flip CFGraphIndex to derived (Authoritative: false) + wire rebuild here.
	r.Register(funcContributor{
		name: "graph", cfs: []CFSpec{{CF: storage.CFGraphData, Authoritative: true}, {CF: storage.CFGraphIndex, Authoritative: true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			g, ok := graph.OfKey(key)
			return ok && contains(sel.Graphs, g)
		},
		versionOf: graphVersionOf,
	})
	// TODO(phase2b): flip CFVectorIndex to derived (Authoritative: false) + wire rebuild here
	// (also solve vector index-spec capture, which lives in config/CRD not in the backed-up data).
	r.Register(funcContributor{
		name: "vector", cfs: []CFSpec{{CF: storage.CFVectorRaw, Authoritative: true}, {CF: storage.CFVectorIndex, Authoritative: true}},
		selects: func(_ storage.ColumnFamily, key []byte, sel Selector) bool {
			c, ok := vector.CollectionOfKey(key)
			return ok && contains(sel.VectorCollections, c)
		},
		versionOf: vectorVersionOf,
	})
	return r
}
