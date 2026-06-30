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
	// Every authoritative (backed-up) CF must be covered.
	for _, cf := range []storage.ColumnFamily{
		storage.CFSys, storage.CFKVData,
		storage.CFGraphData, storage.CFGraphIndex,
		storage.CFVectorRaw, storage.CFVectorIndex, storage.CFReplData,
	} {
		if !auth[cf] {
			t.Errorf("CF %v not authoritative in DefaultRegistry", cf)
		}
	}
	// CFKVMeta is DERIVED (3a.1: rebuilt on restore from the surviving ≤T CFKVData, not exported), so it
	// is OWNED by the kv contributor but NOT authoritative.
	if auth[storage.CFKVMeta] {
		t.Error("CFKVMeta must be derived (non-authoritative) in 3a.1, not backed up verbatim")
	}
	if !owned[storage.CFKVMeta] {
		t.Error("CFKVMeta must still be owned by the kv contributor (it drives the rebuild hook)")
	}
	// Transient CFs are owned by nobody (never backed up).
	for _, cf := range []storage.ColumnFamily{storage.CFReplLog, storage.CFCacheMeta} {
		if auth[cf] {
			t.Errorf("transient CF %v must not be backed up", cf)
		}
	}
}
