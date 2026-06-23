//go:build integration

package integration

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func composeGlobal(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", "../../docker/docker-compose.global.yaml"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func putOn(t *testing.T, port, ns, key, val string) {
	t.Helper()
	if _, err := kvClient(port).Put(context.Background(), connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: ns, Key: []byte(key), Value: []byte(val), RequireOriginPlusOne: true,
	})); err != nil {
		t.Fatalf("put %s/%s on %s: %v", ns, key, port, err)
	}
}

func getOn(t *testing.T, port, ns, key string) *wavespanv1.GetResult {
	t.Helper()
	resp, err := kvClient(port).Get(context.Background(), connect.NewRequest(&wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)}))
	if err != nil {
		return &wavespanv1.GetResult{}
	}
	return resp.Msg
}

func TestGlobalReplicationAcrossClusters(t *testing.T) {
	composeGlobal(t, "up", "-d")
	t.Cleanup(func() { composeGlobal(t, "down", "-v") })

	// each cluster must form (2 members) before writes can satisfy origin+1
	waitFor(t, "clusters form", 90*time.Second, func() bool {
		return len(membership(t, "7951")) == 2 && len(membership(t, "7953")) == 2
	})
	const a1, b1 = "7861", "7863"

	// (TestGlobalBidirectional) a write in A appears in B, and vice versa
	putOn(t, a1, "default", "from-a", "hello-a")
	waitFor(t, "A->B replication", 30*time.Second, func() bool {
		return getOn(t, b1, "default", "from-a").GetFound() && string(getOn(t, b1, "default", "from-a").GetValue()) == "hello-a"
	})
	putOn(t, b1, "default", "from-b", "hello-b")
	waitFor(t, "B->A replication", 30*time.Second, func() bool {
		return getOn(t, a1, "default", "from-b").GetFound() && string(getOn(t, a1, "default", "from-b").GetValue()) == "hello-b"
	})

	// (TestGlobalConvergence) concurrent writes to the same key converge to the SAME HLC-LWW winner
	putOn(t, a1, "default", "ck", "val-from-a")
	putOn(t, b1, "default", "ck", "val-from-b")
	waitFor(t, "convergence to one winner", 30*time.Second, func() bool {
		va := string(getOn(t, a1, "default", "ck").GetValue())
		vb := string(getOn(t, b1, "default", "ck").GetValue())
		return va != "" && va == vb
	})

	// (TestKeepSiblings) concurrent writes in a keep-siblings namespace surface SIBLINGS_PRESENT
	putOn(t, a1, "siblings", "sk", "sib-a")
	putOn(t, b1, "siblings", "sk", "sib-b")
	waitFor(t, "siblings recorded", 30*time.Second, func() bool {
		// after cross-replication both sides should detect concurrent siblings
		ca := getOn(t, a1, "siblings", "sk").GetMeta().GetConflictState()
		cb := getOn(t, b1, "siblings", "sk").GetMeta().GetConflictState()
		return ca == wavespanv1.ConflictState_CONFLICT_SIBLINGS_PRESENT || cb == wavespanv1.ConflictState_CONFLICT_SIBLINGS_PRESENT
	})
}
