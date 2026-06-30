package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
)

func TestDefaultRegistryCoverage(t *testing.T) {
	reg := DefaultRegistry()
	auth := map[storage.ColumnFamily]bool{}
	for _, cf := range reg.AuthoritativeCFs() {
		auth[cf] = true
	}
	owned := map[storage.ColumnFamily]bool{}
	for _, c := range reg.Contributors() {
		for _, s := range c.CFs() {
			owned[s.CF] = true
		}
	}
	// Every authoritative (backed-up) CF must be covered. CFKVMeta is authoritative and exported VERBATIM
	// (preserving the latest pointer's siblings/conflict state); the ≤T cut filters CFKVData only, and a
	// restore-time repair pass (RepairCutMeta) repoints just the pointers whose winner was after T.
	for _, cf := range []storage.ColumnFamily{
		storage.CFSys, storage.CFKVData, storage.CFKVMeta,
		storage.CFGraphData, storage.CFGraphIndex,
		storage.CFVectorRaw, storage.CFVectorIndex, storage.CFReplData,
	} {
		if !auth[cf] {
			t.Errorf("CF %v not authoritative in DefaultRegistry", cf)
		}
	}
	if !owned[storage.CFKVMeta] {
		t.Error("CFKVMeta must be owned by the kv contributor")
	}
	// Transient CFs are owned by nobody (never backed up).
	for _, cf := range []storage.ColumnFamily{storage.CFReplLog, storage.CFCacheMeta} {
		if auth[cf] {
			t.Errorf("transient CF %v must not be backed up", cf)
		}
	}
}
