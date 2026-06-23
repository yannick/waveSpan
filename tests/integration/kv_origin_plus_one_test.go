//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func kvClient(dataPort string) wavespanv1connect.KvServiceClient {
	return wavespanv1connect.NewKvServiceClient(http.DefaultClient, "http://localhost:"+dataPort)
}

func TestKVOriginPlusOneAndOriginKillSurvival(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })

	adminPorts := map[string]string{"node1": "7901", "node2": "7902", "node3": "7903"}
	dataPorts := []string{"7811", "7812", "7813"}

	// wait for the cluster to form so placement has candidates
	waitFor(t, "form-up", 60*time.Second, func() bool {
		for _, p := range adminPorts {
			m := membership(t, p)
			if len(m) != 3 {
				return false
			}
		}
		return true
	})

	ctx := context.Background()

	// (TS-032) put on node1 acknowledges origin+1 with a nearby durable replica
	putResp, err := kvClient("7811").Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: "default", Key: []byte("foo"), Value: []byte("bar"), RequireOriginPlusOne: true,
	}))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if putResp.Msg.GetAckedNearbyReplicas() < 1 {
		t.Fatalf("ackedNearbyReplicas = %d, want >= 1", putResp.Msg.GetAckedNearbyReplicas())
	}

	// local read on the origin returns the value
	getResp, err := kvClient("7811").Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("foo")}))
	if err != nil || !getResp.Msg.GetFound() || string(getResp.Msg.GetValue()) != "bar" {
		t.Fatalf("get on origin = (%q, found=%v, %v)", getResp.Msg.GetValue(), getResp.Msg.GetFound(), err)
	}

	// (M3) killing the origin leaves the value on a replica: at least one survivor has it
	compose(t, "kill", "node1")
	waitFor(t, "value survives on a replica", 60*time.Second, func() bool {
		for _, p := range dataPorts[1:] { // node2, node3
			resp, err := kvClient(p).Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("foo")}))
			if err == nil && resp.Msg.GetFound() && string(resp.Msg.GetValue()) == "bar" {
				return true
			}
		}
		return false
	})
}
