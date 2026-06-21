# M05 - Dynamic cache: FetchReplica, cache store, SubscribeKey, resync, eviction

**Milestone:** M5 (`design/18_implementation_roadmap.md` "Milestone 5")
**Tickets:** TS-040, TS-041, TS-042, TS-043 (`design/19_agent_work_items.md`)
**Depends on:** M4 (holder directory for closest-holder fetch)
**Enables:** M6 (cache-fast scans build on the cache store and certificates)
**Parallel with:** M6, and the M7/M8 tracks (`IMPLEMENTATION_STRATEGY.md` section 2)

## Context

M5 gives reads progressive locality. On a local miss, a pod fetches from the **closest known
holder** (resolved via the M4 holder directory, never by broadcast — `design/README.md` hard
rule 3), stores the value as a **dynamic cache replica**, and subscribes to future updates so
later reads are served locally while the subscription is healthy
(`design/05_special_cache_replication.md` "Dynamic cache read path"/"Subscription protocol").

Dynamic cache replicas are **derived, disposable, and not counted for write durability** (ADR
`design/adr/0003_cache_replicas_are_derived.md`; `design/05` "Replica types"). They are bounded
by memory/disk/subscriber-count/lag/idle-time (`design/README.md` hard rule 4) and must
**resync or downgrade** on a broken subscription rather than silently serve stale data.

## Files to create

```
proto/wavespan/v1/replication.proto   EXTEND: FetchReplicaRequest/Response, SubscribeKeyRequest/Response,
                                       CacheUpdate, CacheUpdateAck, SubscriptionOffer
internal/cache/fetch.go               FetchReplica client (closest holder) + server (holder side)
internal/cache/store.go               dynamic cache store: cache replica class, latest-pointer integration
internal/cache/subscribe.go           SubscribeKey stream: server (source) + client (subscriber)
internal/cache/subscription_sm.go     INACTIVE->SNAPSHOTTING->ACTIVE->LAGGING->RESYNCING->EXPIRED->EVICTED
internal/cache/eviction.go            eviction-score loop; idle TTL; cache-pressure eviction
internal/cache/deadsub.go             dead/slow subscriber detection on the source side
internal/kv/read.go                   EXTEND: miss -> fetch -> cache -> subscribe; metadata says cache source
internal/observability/metrics.go     EXTEND: cache hit/miss, subscription lag/active, evictions
internal/cache/*_test.go              fetch, cache hit, update propagation, resync, eviction
tests/integration/cache_test.go       miss-fetch, second-read-hit, update-propagates, broken-sub-resync
```

## Steps

1. **Protos.** Extend `replication.proto` with `FetchReplicaRequest`
   (namespace, key, min_version?, want_subscription_offer) / `FetchReplicaResponse`
   (found, `StoredRecord`, alternate_holder_member_ids, subscription_offer?),
   `SubscribeKeyRequest` (namespace, key, from_version, subscriber_member_id,
   requested_lease_ms) / `SubscribeKeyResponse` (subscription_id, lease_until, current_version,
   snapshot_required), `CacheUpdate` (subscription_id, key, `StoredRecord`, stream_sequence,
   snapshot_required), `CacheUpdateAck`, and `SubscriptionOffer` — exactly per `design/05`
   "FetchReplica protocol" and "Subscription protocol".

2. **FetchReplica (TS-040), `internal/cache/fetch.go`.** Client: resolve holders from the M4
   holder directory / range directory, pick the closest by the M2 latency graph, and
   `FetchReplica` (`design/05` "Dynamic cache read path"). If the chosen holder is stale it
   returns `alternate_holder_member_ids` for fallback. Server (holder side): return the local
   `StoredRecord` + a `SubscriptionOffer` when `want_subscription_offer`. TS-040 acceptance: a
   read miss fetches the value from a known holder (no broadcast).

3. **Dynamic cache store (TS-041), `internal/cache/store.go`.** Persist the fetched record as
   a **dynamic cache replica** (replica class distinct from nearby-durable; stored in CF
   `kv_data`/`kv_meta` but flagged derived, with cache metadata in CF `cache_meta`). Integrate
   with the local latest pointer so the next `Get` is served locally. Reads served from cache
   set `ResponseMeta.source = LOCAL_DYNAMIC_CACHE` (TS-041 acceptance: second read served
   locally; metadata says `LOCAL_DYNAMIC_CACHE`). Cache replicas **never** count toward write
   ACK or target-N (ADR 0003).

4. **SubscribeKey stream (TS-042), `internal/cache/subscribe.go` + `subscription_sm.go`.**
   Subscriber opens a `SubscribeKey` stream to the best source (preference order in `design/05`
   "Subscription protocol": latest write coordinator, then freshest nearby durable replica,
   then range-summary owner, then global applier). Drive the subscription state machine
   `INACTIVE -> SNAPSHOTTING -> ACTIVE -> LAGGING -> RESYNCING -> ACTIVE -> EXPIRED -> EVICTED`
   (`design/05` "Subscription state machine"). On the source side, when a new mutation is
   applied (M3/M4 update propagation), push a `CacheUpdate` to active subscribers; the
   subscriber applies it if the version wins or creates a sibling, then acks the stream
   sequence. TS-042 acceptance: an update on the origin reaches the subscribed dynamic cache.

5. **Resync + dead-subscriber detection (TS-043), `internal/cache/deadsub.go` +
   `subscription_sm.go`.** Gap detection: a missed `stream_sequence` or lag over
   `maxSubscriptionLagMs` moves the subscription `ACTIVE -> LAGGING`; the source requests a
   snapshot (`snapshot_required=true`) moving it to `RESYNCING`, and after the snapshot applies
   it returns to `ACTIVE` (`design/05` "Subscription state machine"). The source detects slow/
   dead subscribers and **drops** them after a threshold (`design/05` "Update propagation" 7).
   A missed update is never fatal — the subscriber refetches/resyncs (TS-043 acceptance: a lost
   update triggers resync).

6. **Eviction (TS-043), `internal/cache/eviction.go`.** Evict dynamic caches by the
   `eviction_score` from `design/05` "Cache eviction" (idle time, size, lag, low hit rate,
   update-fanout cost). Enforce idle-subscription TTL (`idleSubscriptionTtlSeconds`) and
   cache-pressure eviction (`maxDynamicCacheBytesPerPod`). **Never evict nearby durable
   replicas with the dynamic-cache evictor** (`design/05` "Cache eviction"). TS-043 acceptance:
   an idle cache is evicted.

7. **Metrics.** Add `kv_dynamic_cache_hits_total`, `kv_dynamic_cache_misses_total`,
   `kv_cache_subscription_lag_ms`, `kv_cache_subscriptions_active`, `kv_cache_evictions_total`
   (`design/05` "Metrics"). The lag metric is the watch point for the Go-GC scan/lag risk
   (`IMPLEMENTATION_STRATEGY.md` section 4).

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 5 and the TS tickets:

- A read miss fetches from the closest holder. (M5; TS-040)
- A second read hits the local dynamic cache; metadata says `LOCAL_DYNAMIC_CACHE`. (M5; TS-041)
- An update on the origin propagates to the cache. (M5; TS-042)
- A broken subscription resyncs or downgrades; an idle cache is evicted. (M5; TS-043)

## Verification

1. **Unit:** `fetch` test resolves the closest holder via the directory with no broadcast;
   `store` test asserts a fetched key is served locally with `LOCAL_DYNAMIC_CACHE` metadata;
   `subscribe` test drives every state-machine transition; `deadsub` test asserts a forced
   sequence gap triggers `LAGGING -> RESYNCING -> ACTIVE`; `eviction` test asserts idle and
   cache-pressure eviction and that durable replicas are never evicted by this loop.
2. **Docker integration (`tests/integration/cache_test.go`)** on the 3-node cluster
   (`design/16_testing_strategy.md` "Docker cluster tests" + `design/10_docker_dev.md`
   acceptance):
   - Put `foo` on node1; `wavespanctl --addr node3 kv get default foo` is a miss that fetches
     from a holder and returns the value (TS-040).
   - A second `get` on node3 is served from the dynamic cache; metadata reports
     `LOCAL_DYNAMIC_CACHE` (TS-041).
   - Update `foo` on node1; assert node3's cache receives the `CacheUpdate` and the next read
     reflects the new value (TS-042).
   - Force a subscription gap (pause the source briefly, then resume) and assert the subscriber
     resyncs rather than serving stale data (TS-043); leave the cache idle past
     `idleSubscriptionTtlSeconds` and assert eviction.
3. **Property 4 prep:** confirm cache reads never claim more completeness than a point read —
   the range-coverage `COMPLETE` guarantee is delivered in M6, but M5 must not let a cached
   point read masquerade as a complete range.
