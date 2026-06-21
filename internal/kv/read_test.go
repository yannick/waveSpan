package kv

import (
	"context"
	"testing"

	"github.com/cwire/wavespan/internal/cache"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// fakeFetcher returns a fixed record for a key, simulating a closest-holder fetch.
type fakeFetcher struct {
	rec   *wavespanv1.StoredRecord
	calls int
}

func (f *fakeFetcher) Fetch(_ context.Context, _ string, _ []byte) (cache.FetchResult, error) {
	f.calls++
	if f.rec == nil {
		return cache.FetchResult{Found: false}, nil
	}
	return cache.FetchResult{Found: true, Record: f.rec, Source: "node1"}, nil
}

func TestReadMissFetchesAndCaches(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rstore := recordstore.NewStore(mem, "dev", "node3", version.NewClock(nil, 500), version.NewSequencer(0))
	self := membership.Member{ClusterID: "dev", MemberID: "node3"}

	// the record a holder would return
	v := version.Version{HLCPhysicalMs: 100, WriterClusterID: "dev", WriterMemberID: "node1", WriterSequence: 1}
	rec := &wavespanv1.StoredRecord{
		LogicalKey: []byte("foo"), Namespace: "default", Version: v.ToProto(),
		Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("bar")}},
	}
	ff := &fakeFetcher{rec: rec}
	cs := cache.NewStore(rstore, func() int64 { return 1 })
	reader := NewReader(rstore, self).WithCache(ff, cs)

	// first read: local miss -> fetch -> cache; source FETCHED_CLOSEST_HOLDER
	res, err := reader.Get(context.Background(), "default", []byte("foo"), false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetFound() || string(res.GetValue()) != "bar" {
		t.Fatalf("first read = (found=%v, %q)", res.GetFound(), res.GetValue())
	}
	if res.GetMeta().GetSource() != wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER {
		t.Fatalf("first read source = %v, want FETCHED_CLOSEST_HOLDER", res.GetMeta().GetSource())
	}

	// second read: served locally from the dynamic cache; no fetch
	res2, err := reader.Get(context.Background(), "default", []byte("foo"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res2.GetMeta().GetSource() != wavespanv1.ReadSource_LOCAL_DYNAMIC_CACHE {
		t.Fatalf("second read source = %v, want LOCAL_DYNAMIC_CACHE", res2.GetMeta().GetSource())
	}
	if ff.calls != 1 {
		t.Fatalf("second read should be served from cache without a fetch; fetch calls=%d", ff.calls)
	}
}
