package cache

import (
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func TestEvictorDropsIdleCacheButNotDurable(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rec := recordstore.NewStore(mem, "dev", "node3", version.NewClock(nil, 500), version.NewSequencer(0))

	now := int64(1_000_000)
	cs := NewStore(rec, func() int64 { return now })

	// a dynamic cache replica (subject to eviction)
	cv := version.Version{HLCPhysicalMs: 1, WriterClusterID: "dev", WriterMemberID: "node1", WriterSequence: 1}
	cacheRec := &wavespanv1.StoredRecord{
		LogicalKey: []byte("foo"), Namespace: "default", Version: cv.ToProto(),
		Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("bar")}},
	}
	if err := cs.Put(cacheRec); err != nil {
		t.Fatal(err)
	}
	// a durable replica written straight to the record store (NOT a cache replica)
	dv := rec.NextVersion()
	if _, err := rec.Apply(rec.BuildRecord("default", []byte("dur"), []byte("v"), dv, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}

	ev := NewEvictor(cs, 5*time.Minute, func() int64 { return now })

	// not idle yet -> nothing evicted
	if n := ev.EvictIdle(); n != 0 {
		t.Fatalf("nothing should be evicted yet, evicted %d", n)
	}

	// advance past the idle TTL
	now += (6 * time.Minute).Milliseconds()
	if n := ev.EvictIdle(); n != 1 {
		t.Fatalf("the idle cache replica should be evicted, evicted %d", n)
	}

	// the cache replica is physically gone
	if out, _ := rec.Get("default", []byte("foo")); out.Found {
		t.Fatal("evicted cache replica should not be readable")
	}
	if cs.IsCacheReplica("default", []byte("foo")) {
		t.Fatal("evicted key should no longer be a cache replica")
	}
	// the durable replica is untouched (never evicted by the dynamic-cache evictor)
	if out, _ := rec.Get("default", []byte("dur")); !out.Found {
		t.Fatal("durable replica must NOT be evicted by the cache evictor")
	}
}
