package kv

import (
	"context"
	"sync"
	"time"

	"github.com/yannick/wavespan/internal/cache"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// fetcher resolves and fetches a record from the closest holder on a local miss (M5).
type fetcher interface {
	Fetch(ctx context.Context, namespace string, key []byte) (cache.FetchResult, error)
}

// cacheStore persists a fetched record as a dynamic cache replica and reports cache membership.
type cacheStore interface {
	Put(rec *wavespanv1.StoredRecord) error
	IsCacheReplica(namespace string, key []byte) bool
}

// subscriber opens a live update subscription for a cached key (M5).
type subscriber interface {
	Ensure(ctx context.Context, namespace string, key []byte, offer *wavespanv1.SubscriptionOffer)
}

// Reader serves reads: local-first, then (M5) a closest-holder fetch that populates the dynamic
// cache so later reads are local (design/03 "Get path").
type Reader struct {
	store   *recordstore.Store
	self    membership.Member
	fetcher fetcher
	cache   cacheStore
	sub     subscriber
}

// NewReader builds a local reader (no cache fetch path).
func NewReader(store *recordstore.Store, self membership.Member) *Reader {
	return &Reader{store: store, self: self}
}

// WithCache enables the miss -> fetch -> cache path (M5).
func (r *Reader) WithCache(f fetcher, cs cacheStore) *Reader {
	r.fetcher = f
	r.cache = cs
	return r
}

// WithSubscriber enables live cache invalidation: after caching a fetched key, open a
// SubscribeKey stream so updates on the holder propagate to this cache (M5).
func (r *Reader) WithSubscriber(s subscriber) *Reader {
	r.sub = s
	return r
}

// MultiGet reads many keys of one namespace, returning one GetResult per key in request order. It
// amortizes the RPC/HTTP-2/serialization overhead of a per-key Get across the whole batch — the
// dominant read-path cost. Reads are issued concurrently (bounded) so a batch with cache misses
// (network fetch-on-miss) does not serialize those round-trips.
func (r *Reader) MultiGet(ctx context.Context, namespace string, keys [][]byte, hideExpired bool) ([]*wavespanv1.GetResult, error) {
	results := make([]*wavespanv1.GetResult, len(keys))
	const maxConcurrent = 32
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for i, k := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, k []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := r.Get(ctx, namespace, k, hideExpired)
			if err != nil {
				// degrade a single-key error to not-found so one bad key never fails the batch.
				res = &wavespanv1.GetResult{
					Meta:  &wavespanv1.ResponseMeta{ServedByMemberId: r.self.MemberID, Completeness: wavespanv1.Completeness_PARTIAL},
					Found: false,
				}
			}
			results[i] = res
		}(i, k)
	}
	wg.Wait()
	return results, nil
}

// Get reads a key. On a local miss it fetches from the closest holder, caches the result, and
// reports the read source in the response metadata.
func (r *Reader) Get(ctx context.Context, namespace string, key []byte, hideExpired bool) (*wavespanv1.GetResult, error) {
	out, err := r.store.Get(namespace, key)
	if err != nil {
		return nil, err
	}

	source := wavespanv1.ReadSource_LOCAL_DURABLE
	completeness := wavespanv1.Completeness_COMPLETE
	if out.Found || out.Tombstone {
		if r.cache != nil && r.cache.IsCacheReplica(namespace, key) {
			source = wavespanv1.ReadSource_LOCAL_DYNAMIC_CACHE
		}
	} else if r.fetcher != nil {
		// local miss: fetch from the closest holder and cache it (design/05 read path)
		fr, ferr := r.fetcher.Fetch(ctx, namespace, key)
		switch {
		case ferr == nil && fr.Found:
			if r.cache != nil {
				_ = r.cache.Put(fr.Record)
			}
			if r.sub != nil && fr.Offer != nil {
				r.sub.Ensure(ctx, namespace, key, fr.Offer)
			}
			out, err = r.store.Get(namespace, key)
			if err != nil {
				return nil, err
			}
			source = wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER
		case ferr != nil:
			// Holder unreachable: this not-found may be a false negative (the value could live on
			// the holder we could not reach), so the read is incomplete. A reachable holder that
			// reports absent (ferr == nil, !fr.Found) stays COMPLETE — that is a genuine miss.
			completeness = wavespanv1.Completeness_PARTIAL
		}
	}

	meta := &wavespanv1.ResponseMeta{
		ServedByClusterId: r.self.ClusterID,
		ServedByMemberId:  r.self.MemberID,
		Source:            source,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      completeness,
		ObservedAtUnixMs:  time.Now().UnixMilli(),
	}
	if !out.ConflictNone {
		meta.ConflictState = wavespanv1.ConflictState_CONFLICT_SIBLINGS_PRESENT
	}
	res := &wavespanv1.GetResult{Meta: meta, Found: out.Found, Value: out.Value}
	if out.Found || out.Tombstone {
		meta.ObservedVersion = out.Version.ToProto()
	}
	if out.ExpiresAtMs != nil {
		res.ExpiresAtUnixMs = out.ExpiresAtMs
		// best-effort hide-expired on read (design/03 "TTL semantics"): a node may hide a record
		// it detects as expired. hideExpired only tightens the contract; default already hides.
		_ = hideExpired
		if *out.ExpiresAtMs <= time.Now().UnixMilli() {
			res.Found = false
			res.Value = nil
		}
	}
	return res, nil
}
