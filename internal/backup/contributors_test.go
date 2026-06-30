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
	// Every authoritative (backed-up) CF must be covered. CFKVMeta is authoritative too: a full backup
	// exports it verbatim (preserving siblings/conflict state) — it is only SKIPPED while an HLC ≤T cut is
	// active, then rebuilt on restore (RebuildWhenCut).
	for _, cf := range []storage.ColumnFamily{
		storage.CFSys, storage.CFKVData, storage.CFKVMeta,
		storage.CFGraphData, storage.CFGraphIndex,
		storage.CFVectorRaw, storage.CFVectorIndex, storage.CFReplData,
	} {
		if !auth[cf] {
			t.Errorf("CF %v not authoritative in DefaultRegistry", cf)
		}
	}
	// CFKVMeta must be flagged RebuildWhenCut: exported on full backups, skipped + rebuilt on a ≤T cut.
	if !owned[storage.CFKVMeta] {
		t.Error("CFKVMeta must be owned by the kv contributor")
	}
	if !reg.CutDerivedCFs()[storage.CFKVMeta] {
		t.Error("CFKVMeta must be RebuildWhenCut (skipped + rebuilt only when a ≤T cut is active)")
	}
	// Transient CFs are owned by nobody (never backed up).
	for _, cf := range []storage.ColumnFamily{storage.CFReplLog, storage.CFCacheMeta} {
		if auth[cf] {
			t.Errorf("transient CF %v must not be backed up", cf)
		}
	}
}
