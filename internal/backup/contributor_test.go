package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
)

func TestRegistryRegisterAndList(t *testing.T) {
	reg := NewRegistry()
	reg.Register(staticContributor{
		name: "demo",
		cfs:  []CFSpec{{CF: storage.CFKVData, Authoritative: true}, {CF: storage.CFGraphIndex, Authoritative: false}},
	})
	got := reg.Contributors()
	if len(got) != 1 || got[0].Name() != "demo" {
		t.Fatalf("want 1 contributor 'demo', got %+v", got)
	}
	// Authoritative CFs across the registry exclude the derived one.
	auth := reg.AuthoritativeCFs()
	if len(auth) != 1 || auth[0] != storage.CFKVData {
		t.Fatalf("want [CFKVData] authoritative, got %v", auth)
	}
}

// staticContributor is a test-only Contributor.
type staticContributor struct {
	name string
	cfs  []CFSpec
}

func (s staticContributor) Name() string { return s.name }
func (s staticContributor) CFs() []CFSpec { return s.cfs }
func (s staticContributor) RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error {
	return nil
}
func (s staticContributor) Selects(cf storage.ColumnFamily, key []byte, sel Selector) bool {
	return true
}
