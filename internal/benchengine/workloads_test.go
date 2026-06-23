package benchengine

import "testing"

func TestOpsForCollectionWorkloads(t *testing.T) {
	cfg := Config{DataAddr: "x:1"}
	for _, kind := range []string{"set", "hash", "zset", "bulkremove"} {
		op, label, err := opsFor(WorkloadSpec{Kind: kind, Params: map[string]any{}}, cfg)
		if err != nil {
			t.Fatalf("opsFor(%q) err=%v", kind, err)
		}
		if op == nil {
			t.Fatalf("opsFor(%q) returned nil op", kind)
		}
		if label != kind {
			t.Fatalf("opsFor(%q) label=%q", kind, label)
		}
	}
}

func TestOpsForUnknownKind(t *testing.T) {
	_, _, err := opsFor(WorkloadSpec{Kind: "nope", Params: map[string]any{}}, Config{DataAddr: "x:1"})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}
