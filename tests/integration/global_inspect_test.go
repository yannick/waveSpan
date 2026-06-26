//go:build integration

// NOTE: docker-compose.global.yaml builds the image from the sibling `waveSpan/` directory (the
// Dockerfile COPYs `waveSpan/`). So `make test-integration` builds the code under test only when run
// from the canonical `waveSpan/` checkout — i.e. on main / post-merge, which is how CI runs it. From
// a differently-named worktree, pre-build `wavespan/node:dev` from that worktree's source before
// running (the test itself only does `compose up`, never `build`).

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// inspectGlobalKey runs ObservabilityService.InspectGlobal against the given admin port and returns
// the single resolved InspectKey row plus the trailer. It requests admin role (dev mode) so values
// are revealed.
func inspectGlobalKey(t *testing.T, adminPort, ns, key string, includePeers bool) (*wavespanv1.InspectKey, *wavespanv1.InspectTrailer) {
	t.Helper()
	req := connect.NewRequest(&wavespanv1.InspectGlobalRequest{
		Keyspace:            wavespanv1.Keyspace_KEYSPACE_KV,
		Namespace:           ns,
		Key:                 []byte(key),
		IncludeValue:        true,
		IncludePeerClusters: includePeers,
	})
	req.Header().Set("X-WaveSpan-Role", "admin")
	stream, err := obsClient(adminPort).InspectGlobal(context.Background(), req)
	if err != nil {
		t.Fatalf("InspectGlobal on %s: %v", adminPort, err)
	}
	var ik *wavespanv1.InspectKey
	var tr *wavespanv1.InspectTrailer
	for stream.Receive() {
		if k := stream.Msg().GetKey(); k != nil {
			ik = k
		}
		if x := stream.Msg().GetTrailer(); x != nil {
			tr = x
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("InspectGlobal recv on %s: %v", adminPort, err)
	}
	return ik, tr
}

func hasHolder(ik *wavespanv1.InspectKey, member, peerCluster string) bool {
	for _, h := range ik.GetHolders() {
		if h.GetMemberId() == member && h.GetPeerClusterId() == peerCluster {
			return true
		}
	}
	return false
}

func hasPeerClusterHolder(ik *wavespanv1.InspectKey, peerCluster string) bool {
	for _, h := range ik.GetHolders() {
		if h.GetPeerClusterId() == peerCluster {
			return true
		}
	}
	return false
}

// TestGlobalInspectAcrossClusters verifies the Global Data Browser end to end on the two-cluster
// active-active topology (test-a: a1,a2 ↔ test-b: b1,b2):
//   - cross-cluster: a key written on test-a, once replicated, is resolved from test-b with a
//     test-a-tagged peer holder, a local holder, the value surfaced, and COMPLETE completeness;
//   - single-cluster (Layer 1 regression): a key written on test-a (origin+1 → a1 AND a2) is
//     resolved on test-a WITHOUT peers listing BOTH a1 and a2 — the old stub listed only the
//     serving node and always reported PARTIAL.
func TestGlobalInspectAcrossClusters(t *testing.T) {
	composeGlobal(t, "up", "-d")
	t.Cleanup(func() { composeGlobal(t, "down", "-v") })

	// KV writes/reads go to the ADMIN port: post-gRPC-migration the data port (7800) speaks gRPC
	// over HTTP/2, while the browser-facing Connect KvService is mounted on the admin port (7900).
	const a1Admin, b1Admin = "7951", "7953"

	waitFor(t, "clusters form", 90*time.Second, func() bool {
		return len(membership(t, a1Admin)) == 2 && len(membership(t, b1Admin)) == 2
	})

	// --- cross-cluster ---
	putOn(t, a1Admin, "default", "gkey", "gval")
	waitFor(t, "A->B replication", 30*time.Second, func() bool {
		return getOn(t, b1Admin, "default", "gkey").GetFound()
	})
	waitFor(t, "B resolves A as a peer holder", 30*time.Second, func() bool {
		ik, _ := inspectGlobalKey(t, b1Admin, "default", "gkey", true)
		return hasPeerClusterHolder(ik, "test-a")
	})

	ik, tr := inspectGlobalKey(t, b1Admin, "default", "gkey", true)
	hasLocal := false
	for _, h := range ik.GetHolders() {
		if h.GetPeerClusterId() == "" {
			hasLocal = true
		}
	}
	if !hasLocal {
		t.Errorf("expected a local (test-b) holder, got %+v", ik.GetHolders())
	}
	if !hasPeerClusterHolder(ik, "test-a") {
		t.Errorf("expected a test-a peer holder, got %+v", ik.GetHolders())
	}
	if string(ik.GetValue()) != "gval" {
		t.Errorf("expected value gval surfaced cross-cluster, got %q", ik.GetValue())
	}
	if tr.GetFinalCompleteness() != wavespanv1.Completeness_COMPLETE {
		t.Errorf("expected COMPLETE, got %v warnings=%v", tr.GetFinalCompleteness(), tr.GetWarnings())
	}

	// --- single-cluster Layer 1 regression ---
	putOn(t, a1Admin, "default", "lkey", "lval")
	waitFor(t, "lkey on both test-a nodes", 30*time.Second, func() bool {
		ik, _ := inspectGlobalKey(t, a1Admin, "default", "lkey", false)
		return hasHolder(ik, "a1", "") && hasHolder(ik, "a2", "")
	})
	ik2, tr2 := inspectGlobalKey(t, a1Admin, "default", "lkey", false)
	if !hasHolder(ik2, "a1", "") || !hasHolder(ik2, "a2", "") {
		t.Fatalf("Layer 1 must list BOTH a1 and a2, got %+v", ik2.GetHolders())
	}
	if tr2.GetFinalCompleteness() != wavespanv1.Completeness_COMPLETE {
		t.Errorf("single-cluster resolution should be COMPLETE, got %v warnings=%v", tr2.GetFinalCompleteness(), tr2.GetWarnings())
	}
}
