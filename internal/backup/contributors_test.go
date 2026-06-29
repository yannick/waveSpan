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
	// Every non-transient CF must be covered (2a backs up derived indexes verbatim).
	for _, cf := range []storage.ColumnFamily{
		storage.CFSys, storage.CFKVData, storage.CFKVMeta,
		storage.CFGraphData, storage.CFGraphIndex,
		storage.CFVectorRaw, storage.CFVectorIndex, storage.CFReplData,
	} {
		if !auth[cf] {
			t.Errorf("CF %v not covered by DefaultRegistry", cf)
		}
	}
	// Transient CFs are owned by nobody (never backed up).
	for _, cf := range []storage.ColumnFamily{storage.CFReplLog, storage.CFCacheMeta} {
		if auth[cf] {
			t.Errorf("transient CF %v must not be backed up", cf)
		}
	}
}
