//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// runScan collects the header completeness and ordered keys from a streaming scan.
func runScan(t *testing.T, port string, mode wavespanv1.ScanMode) (wavespanv1.Completeness, []string) {
	t.Helper()
	stream, err := kvClient(port).Scan(context.Background(), connect.NewRequest(&wavespanv1.ScanRequest{Namespace: "default", Mode: mode}))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var comp wavespanv1.Completeness
	var keys []string
	for stream.Receive() {
		switch m := stream.Msg().Msg.(type) {
		case *wavespanv1.ScanResponse_Header:
			comp = m.Header.GetCompleteness()
		case *wavespanv1.ScanResponse_Row:
			keys = append(keys, string(m.Row.GetKey()))
		}
	}
	return comp, keys
}

func TestScanCompletenessAndLazyTTL(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })

	adminPorts := map[string]string{"node1": "7901", "node2": "7902", "node3": "7903"}
	waitFor(t, "form-up", 60*time.Second, func() bool {
		for _, p := range adminPorts {
			if len(membership(t, p)) != 3 {
				return false
			}
		}
		return true
	})

	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := kvClient("7811").Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
			Namespace: "default", Key: []byte(k), Value: []byte("V" + k), RequireOriginPlusOne: true,
		})); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// (TS-050) a cache-fast scan is BEST_EFFORT, never COMPLETE
	if comp, _ := runScan(t, "7811", wavespanv1.ScanMode_CACHE_FAST); comp != wavespanv1.Completeness_BEST_EFFORT {
		t.Fatalf("cache-fast completeness = %v, want BEST_EFFORT", comp)
	}

	// (TS-051) a routed-eventual scan merges holders into a sorted, deduped key set
	waitFor(t, "routed scan sees all keys", 30*time.Second, func() bool {
		comp, keys := runScan(t, "7811", wavespanv1.ScanMode_ROUTED_EVENTUAL)
		return comp == wavespanv1.Completeness_PARTIAL && len(keys) == 3 &&
			keys[0] == "a" && keys[1] == "b" && keys[2] == "c"
	})

	// (TS-053) a key with a short TTL eventually stops being returned (best-effort hide-expired)
	ttl := int64(2000)
	if _, err := kvClient("7811").Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: "default", Key: []byte("tmp"), Value: []byte("x"), TtlMs: &ttl, RequireOriginPlusOne: true,
	})); err != nil {
		t.Fatalf("ttl put: %v", err)
	}
	if r, _ := kvClient("7811").Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("tmp")})); !r.Msg.GetFound() {
		t.Fatal("ttl key should be readable before expiry")
	}
	waitFor(t, "ttl key disappears after expiry", 30*time.Second, func() bool {
		r, err := kvClient("7811").Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte("tmp")}))
		return err == nil && !r.Msg.GetFound()
	})
}
