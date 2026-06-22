//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func replClient(port string) wavespanv1connect.ReplicationServiceClient {
	return wavespanv1connect.NewReplicationServiceClient(http.DefaultClient, "http://localhost:"+port)
}

// localCount returns how many records the node at dataPort holds LOCALLY for a namespace (ScanLocal
// never fetches from peers, so this proves what the node physically holds, not what a read warms).
func localCount(t *testing.T, dataPort, namespace string) int {
	t.Helper()
	resp, err := replClient(dataPort).ScanLocal(context.Background(), connect.NewRequest(&wavespanv1.ScanLocalRequest{Namespace: namespace}))
	if err != nil {
		t.Fatalf("ScanLocal %s: %v", dataPort, err)
	}
	return len(resp.Msg.GetRows())
}

// TestReplicateEverywhereAndBackfill: writes to an "everywhere" namespace land on EVERY node, and a
// wiped node that rejoins streams the records back via bootstrap (design/05 node sync).
func TestReplicateEverywhereAndBackfill(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "cluster up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const n = 50
	kvc := kvClient("7811") // node1 data port
	for i := 0; i < n; i++ {
		if _, err := kvc.Put(context.Background(), connect.NewRequest(&wavespanv1.PutRequest{
			Namespace: "ref", Key: []byte(fmt.Sprintf("ref/%d", i)), Value: []byte(fmt.Sprintf("v%d", i)), RequireOriginPlusOne: true,
		})); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// (1) replicate-everywhere: all three nodes hold every record locally.
	waitFor(t, "everywhere fanout", 30*time.Second, func() bool {
		return localCount(t, "7811", "ref") == n && localCount(t, "7812", "ref") == n && localCount(t, "7813", "ref") == n
	})

	// (2) wipe node3 entirely (container + its data volume), then bring it back empty.
	compose(t, "rm", "-sf", "node3")
	rmVolume(t, "wavespan-dev_node3-data")
	compose(t, "up", "-d", "node3")
	waitFor(t, "node3 rejoined", 60*time.Second, func() bool { return len(membership(t, "7903")) == 3 })

	// (3) backfill: node3 streams the everywhere namespace from a peer on join — WITHOUT any read
	// warming it, ScanLocal must eventually show all n records present locally again.
	waitFor(t, "node3 backfilled", 60*time.Second, func() bool { return localCount(t, "7813", "ref") == n })
}

func rmVolume(t *testing.T, name string) {
	t.Helper()
	// best-effort; the volume may already be gone if `rm` removed it
	_ = exec.Command("docker", "volume", "rm", "-f", name).Run()
}
