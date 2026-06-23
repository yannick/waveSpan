package rpcopts

import "testing"

func TestShortMethod(t *testing.T) {
	cases := map[string]string{
		"/wavespan.v1.KvService/Put":          "Put",
		"/wavespan.v1.VectorService/VectorGet": "VectorGet",
		"Bare":                                "Bare",
		"":                                    "",
	}
	for in, want := range cases {
		if got := shortMethod(in); got != want {
			t.Errorf("shortMethod(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClassify(t *testing.T) {
	reads := []string{"Get", "MultiGet", "Scan", "Search", "VectorGet", "VectorSearch", "Query", "SampleVectors", "GetClusterView", "GetConfig", "GraphExplore", "GraphSubgraph"}
	writes := []string{"Put", "Delete", "VectorPut", "VectorDelete", "SetTunable"}
	// Internal cluster machinery + per-node fan-out fragments are excluded from client throughput.
	others := []string{"Exchange", "StoreReplica", "FetchReplica", "Subscribe", "SearchLocal", "ScanLocal", "InspectLocal", "Healthz"}

	for _, m := range reads {
		if got := classify(m); got != "read" {
			t.Errorf("classify(%q) = %q, want read", m, got)
		}
	}
	for _, m := range writes {
		if got := classify(m); got != "write" {
			t.Errorf("classify(%q) = %q, want write", m, got)
		}
	}
	for _, m := range others {
		if got := classify(m); got != "other" {
			t.Errorf("classify(%q) = %q, want other", m, got)
		}
	}
}
