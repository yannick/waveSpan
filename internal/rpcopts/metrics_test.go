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
	reads := []string{"Get", "MultiGet", "Scan", "ScanLocal", "Search", "SearchLocal", "VectorGet", "VectorSearch", "Query", "SampleVectors", "GetClusterView", "GetConfig", "InspectLocal", "GraphExplore", "GraphSubgraph"}
	writes := []string{"Put", "Delete", "VectorPut", "VectorDelete", "StoreReplica", "SetTunable", "Apply"}
	others := []string{"GossipExchange", "Subscribe", "Healthz"}

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
