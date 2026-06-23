//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestRepairRestoresReplicaAfterHolderKill verifies that killing a (non-coordinator) holder
// triggers the repair engine on the coordinator to re-replicate the key onto a fresh node, with
// no manual action (M4/TS-035). The compose cluster runs with WAVESPAN_TARGET_NEARBY_REPLICAS=1
// (2 durable holders), so initially one of the three nodes does not hold the key.
func TestRepairRestoresReplicaAfterHolderKill(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })

	adminPorts := map[string]string{"node1": "7901", "node2": "7902", "node3": "7903"}
	dataPorts := map[string]string{"node1": "7811", "node2": "7812", "node3": "7813"}

	waitFor(t, "form-up", 60*time.Second, func() bool {
		for _, p := range adminPorts {
			if len(membership(t, p)) != 3 {
				return false
			}
		}
		return true
	})

	ctx := context.Background()
	holds := func(port string) bool {
		resp, err := kvClient(port).Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("rk")}))
		return err == nil && resp.Msg.GetFound()
	}

	// write on node1 (coordinator); origin+1 acks one nearby replica, fanout fills to 2 holders
	if _, err := kvClient(dataPorts["node1"]).Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: "default", Key: []byte("rk"), Value: []byte("rv"), RequireOriginPlusOne: true,
	})); err != nil {
		t.Fatalf("put: %v", err)
	}

	// find the peer that does NOT hold the key (exactly one of node2/node3 is empty at target=2)
	var emptyNode, replicaNode string
	waitFor(t, "replica layout settles", 30*time.Second, func() bool {
		emptyNode, replicaNode = "", ""
		for _, name := range []string{"node2", "node3"} {
			if holds(dataPorts[name]) {
				replicaNode = name
			} else {
				emptyNode = name
			}
		}
		return emptyNode != "" && replicaNode != ""
	})

	// kill the replica holder; the coordinator must repair onto the previously-empty node
	compose(t, "kill", replicaNode)
	waitFor(t, "repair re-replicates onto a fresh node", 90*time.Second, func() bool {
		return holds(dataPorts[emptyNode])
	})
}
