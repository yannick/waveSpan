//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestCacheMissFetchAndDynamicCacheHit verifies read locality (M5/TS-040, TS-041): a node that
// did not coordinate or replicate a key resolves a holder via the gossiped directory, fetches it,
// caches it, and serves the second read from the dynamic cache.
func TestCacheMissFetchAndDynamicCacheHit(t *testing.T) {
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
	if _, err := kvClient(dataPorts["node1"]).Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: "default", Key: []byte("ck"), Value: []byte("cv"), RequireOriginPlusOne: true,
	})); err != nil {
		t.Fatalf("put: %v", err)
	}

	// pick a node that does not already hold the key (target=2 holders -> one node is empty)
	var miss string
	waitFor(t, "find a non-holder + holder bloom gossiped", 40*time.Second, func() bool {
		for _, name := range []string{"node2", "node3"} {
			resp, err := kvClient(dataPorts[name]).Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("ck")}))
			if err != nil {
				return false
			}
			// a node that returns FETCHED_CLOSEST_HOLDER on first read is the cache client we want
			if resp.Msg.GetFound() && resp.Msg.GetMeta().GetSource() == wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER {
				miss = name
				return true
			}
		}
		return false
	})

	// second read on that node is served from the dynamic cache
	resp, err := kvClient(dataPorts[miss]).Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("ck")}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetMeta().GetSource() != wavespanv1.ReadSource_LOCAL_DYNAMIC_CACHE {
		t.Fatalf("second read on %s source = %v, want LOCAL_DYNAMIC_CACHE", miss, resp.Msg.GetMeta().GetSource())
	}
	if string(resp.Msg.GetValue()) != "cv" {
		t.Fatalf("cached value = %q, want cv", resp.Msg.GetValue())
	}

	// (TS-042) update on the holder propagates to the subscribed cache
	if _, err := kvClient(dataPorts["node1"]).Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: "default", Key: []byte("ck"), Value: []byte("cv2"), RequireOriginPlusOne: true,
	})); err != nil {
		t.Fatalf("update put: %v", err)
	}
	waitFor(t, "cache receives the update via subscription", 30*time.Second, func() bool {
		r, err := kvClient(dataPorts[miss]).Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("ck")}))
		return err == nil && string(r.Msg.GetValue()) == "cv2"
	})
}
