package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/yannick/wavespan/internal/cache"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
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

// errFetcher simulates an unreachable holder: the closest-holder fetch fails.
type errFetcher struct{ calls int }

func (f *errFetcher) Fetch(_ context.Context, _ string, _ []byte) (cache.FetchResult, error) {
	f.calls++
	return cache.FetchResult{}, errors.New("holder unreachable")
}

// An unreachable holder on a local miss must mark the read PARTIAL: the not-found may be a false
// negative (the value could live on the holder we could not reach), so callers can react.
func TestReadMissUnreachableHolderIsPartial(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rstore := recordstore.NewStore(mem, "dev", "node3", version.NewClock(nil, 500), version.NewSequencer(0))
	self := membership.Member{ClusterID: "dev", MemberID: "node3"}
	cs := cache.NewStore(rstore, func() int64 { return 1 })
	reader := NewReader(rstore, self).WithCache(&errFetcher{}, cs)

	res, err := reader.Get(context.Background(), "default", []byte("absent"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.GetFound() {
		t.Fatal("expected not found")
	}
	if res.GetMeta().GetCompleteness() != wavespanv1.Completeness_PARTIAL {
		t.Fatalf("unreachable-holder miss must be PARTIAL, got %v", res.GetMeta().GetCompleteness())
	}
}

// A reachable holder that reports the key absent is a genuine, COMPLETE miss (not partial).
func TestReadMissReachableHolderAbsentIsComplete(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rstore := recordstore.NewStore(mem, "dev", "node3", version.NewClock(nil, 500), version.NewSequencer(0))
	self := membership.Member{ClusterID: "dev", MemberID: "node3"}
	cs := cache.NewStore(rstore, func() int64 { return 1 })
	reader := NewReader(rstore, self).WithCache(&fakeFetcher{rec: nil}, cs)

	res, err := reader.Get(context.Background(), "default", []byte("absent"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.GetMeta().GetCompleteness() != wavespanv1.Completeness_COMPLETE {
		t.Fatalf("reachable-holder absent must be COMPLETE, got %v", res.GetMeta().GetCompleteness())
	}
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
