//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestVectorReplicatesAcrossClustersAndIsSearchable asserts a raw vector written in cluster A is
// found by vector.searchApprox in cluster B after global apply (TS-084) — only raw records cross
// the wire; each cluster rebuilds its own ANN index.
func TestVectorReplicatesAcrossClustersAndIsSearchable(t *testing.T) {
	composeGlobal(t, "up", "-d")
	t.Cleanup(func() { composeGlobal(t, "down", "-v") })
	waitFor(t, "clusters form", 90*time.Second, func() bool {
		return len(membership(t, "7951")) == 2 && len(membership(t, "7953")) == 2
	})
	const aPort, bPort = "7861", "7863"

	// write a distinctive vector into cluster A
	if _, err := vectorClient(aPort).Put(context.Background(), connect.NewRequest(&wavespanv1.PutVectorRequest{
		Record: &wavespanv1.VectorRecord{Collection: "docs", VectorId: "from-a", Values: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
	})); err != nil {
		t.Fatalf("ingest in A: %v", err)
	}

	// it becomes searchable in cluster B once the raw vector is replicated and applied
	waitFor(t, "A->B vector replication", 30*time.Second, func() bool {
		return contains(approxSearch(t, bPort, []float32{1, 0, 0, 0, 0, 0, 0, 0}, 1), "from-a")
	})
}
