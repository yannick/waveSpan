package kv

import (
	"context"
	"time"

	"github.com/cwire/wavespan/internal/cache"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
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

// Reader serves reads: local-first, then (M5) a closest-holder fetch that populates the dynamic
// cache so later reads are local (design/03 "Get path").
type Reader struct {
	store   *recordstore.Store
	self    membership.Member
	fetcher fetcher
	cache   cacheStore
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

// Get reads a key. On a local miss it fetches from the closest holder, caches the result, and
// reports the read source in the response metadata.
func (r *Reader) Get(ctx context.Context, namespace string, key []byte, hideExpired bool) (*wavespanv1.GetResult, error) {
	out, err := r.store.Get(namespace, key)
	if err != nil {
		return nil, err
	}

	source := wavespanv1.ReadSource_LOCAL_DURABLE
	if out.Found || out.Tombstone {
		if r.cache != nil && r.cache.IsCacheReplica(namespace, key) {
			source = wavespanv1.ReadSource_LOCAL_DYNAMIC_CACHE
		}
	} else if r.fetcher != nil {
		// local miss: fetch from the closest holder and cache it (design/05 read path)
		if fr, ferr := r.fetcher.Fetch(ctx, namespace, key); ferr == nil && fr.Found {
			if r.cache != nil {
				_ = r.cache.Put(fr.Record)
			}
			out, err = r.store.Get(namespace, key)
			if err != nil {
				return nil, err
			}
			source = wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER
		}
	}

	meta := &wavespanv1.ResponseMeta{
		ServedByClusterId: r.self.ClusterID,
		ServedByMemberId:  r.self.MemberID,
		Source:            source,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      wavespanv1.Completeness_COMPLETE,
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
		if hideExpired && *out.ExpiresAtMs <= time.Now().UnixMilli() {
			res.Found = false
			res.Value = nil
		}
	}
	return res, nil
}
