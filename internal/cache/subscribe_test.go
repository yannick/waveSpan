package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	local "github.com/yannick/wavespan/internal/replication/local"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func newStoreFor(t *testing.T, member string) *recordstore.Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return recordstore.NewStore(mem, "dev", member, version.NewClock(nil, 500), version.NewSequencer(0))
}

func applyKV(t *testing.T, rec *recordstore.Store, ns string, key, val []byte) {
	t.Helper()
	v := rec.NextVersion()
	if _, err := rec.Apply(rec.BuildRecord(ns, key, val, v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

// TestCacheSubscriptionPropagatesUpdate fetches a key, subscribes, then updates the key on the
// holder and asserts the update streams to the subscriber's dynamic cache (M5/TS-042).
func TestCacheSubscriptionPropagatesUpdate(t *testing.T) {
	// holder node1 with default/foo = v1
	rec1 := newStoreFor(t, "node1")
	applyKV(t, rec1, "default", []byte("foo"), []byte("v1"))
	source := NewSubscriptionSource(rec1)

	mux := http.NewServeMux()
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	addr1 := strings.TrimPrefix(ts.URL, "http://")
	server := local.NewReplicaServer(local.NewReceiver(rec1, "node1", local.NewIdempotency(0)), rec1, "node1", addr1, source)
	mux.Handle(server.Handler())

	// subscriber node3
	rec3 := newStoreFor(t, "node3")
	now := int64(1)
	cacheStore := NewStore(rec3, func() int64 { return now })
	dir3 := NewDirectory("node3", func() int64 { return now })
	srcDir := NewDirectory("node1", func() int64 { return now })
	srcDir.AddHeldKey("default", []byte("foo"))
	dir3.ApplyPeerSummary(srcDir.OwnSummary())

	self3 := membership.Member{ClusterID: "dev", MemberID: "node3"}
	cluster := staticCluster{{Member: membership.Member{MemberID: "node1", DataAddr: addr1}, State: membership.StateAlive}}
	fetcher := NewFetcher(self3, dir3, cluster, latencygraph.New(latencygraph.DefaultConfig()), http.DefaultClient)
	subscriber := NewSubscriber(self3, cacheStore, fetcher, http.DefaultClient)
	// cancel the subscription before the httptest server closes, so the streaming handler returns
	// and Close() does not block (cleanup runs LIFO: this is registered after ts.Close).
	subCtx, subCancel := context.WithCancel(context.Background())
	subscriber.SetBaseContext(subCtx)
	t.Cleanup(subCancel)

	// fetch foo on node3 -> cache v1 + subscribe
	fr, err := fetcher.Fetch(context.Background(), "default", []byte("foo"))
	if err != nil || !fr.Found {
		t.Fatalf("fetch: %+v %v", fr, err)
	}
	if err := cacheStore.Put(fr.Record); err != nil {
		t.Fatal(err)
	}
	if fr.Offer == nil {
		t.Fatal("fetch should return a subscription offer")
	}
	subscriber.Ensure(context.Background(), "default", []byte("foo"), fr.Offer)

	// update foo on node1 -> v2, then notify subscribers
	applyKV(t, rec1, "default", []byte("foo"), []byte("v2"))
	source.Notify("default", []byte("foo"))

	// the subscriber's cache should converge to v2
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if out, _ := rec3.Get("default", []byte("foo")); out.Found && string(out.Value) == "v2" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	out, _ := rec3.Get("default", []byte("foo"))
	t.Fatalf("subscription did not propagate update: cache value = %q, want v2", out.Value)
}
