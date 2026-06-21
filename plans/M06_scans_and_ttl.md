# M06 - Range scans and lazy TTL

**Milestone:** M6 (`design/18_implementation_roadmap.md` "Milestone 6")
**Tickets:** TS-050, TS-051, TS-052, TS-053 (`design/19_agent_work_items.md`)
**Depends on:** M4 (holder directory for routed scans), M5 (dynamic cache store for cache-fast
scans and range certificates)
**Parallel with:** M7 and the graph/vector track (`IMPLEMENTATION_STRATEGY.md` section 2)

## Context

M6 adds eventually-consistent range scans with **honest completeness metadata** and lazy TTL.
Scans must never silently return a partial cache scan as complete
(`design/03_kv_store.md` "Range scans"; `design/README.md` "Recommended initial defaults").
The cache-fast mode reads local cache/durable copies (fast, may be incomplete); the
routed-eventual mode contacts known holders per subrange and merges; a `cache-complete` scan
is allowed **only** when a valid `RangeCoverageCertificate` exists
(`design/03` "Cache range coverage certificate"). This is property 4 in
`IMPLEMENTATION_STRATEGY.md` section 3 and a CI merge gate.

TTL is lazy and best-effort: writes stamp `expires_at`, reads may hide detected-expired
records, and a background sweeper emits tombstones in coarse buckets that later compaction
removes (`design/03` "TTL semantics"; `design/02_storage_wavesdb.md` "TTL storage"). Expired
records must not break conflict convergence.

## Files to create

```
proto/wavespan/v1/kv.proto            FINALIZE Scan RPC + ScanHeader/Completeness; SubscribeRange; RangeCoverageCertificate
internal/kv/scan.go                   scan dispatcher: cache-fast | cache-complete | routed-eventual | local-only
internal/kv/scan_merge.go             k-way sorted merge of holder subrange results (bounded batches)
internal/cache/range_cert.go          RangeCoverageCertificate issue/validate; SubscribeRange (range cache)
internal/ttl/bucket.go                TTL bucket assignment (/kv_meta/ttl/{bucket}/...); expires_at math
internal/ttl/sweeper.go               background sweeper: scan old buckets, emit tombstones
internal/ttl/read_filter.go           best-effort hide-expired on read; strict hideExpiredOnRead
internal/observability/metrics.go     EXTEND: range cache coverage count, ttl sweep metrics
internal/kv/scan_test.go              partial never marked complete; cert gates COMPLETE
internal/ttl/ttl_test.go              bucket assignment, hide-expired, tombstone emission, compaction eligibility
tests/integration/scan_ttl_test.go    cache-fast BEST_EFFORT; certified range COMPLETE; TTL disappears
```

## Steps

1. **Finalize scan protos.** Complete the `Scan` streaming RPC and the `ScanHeader`
   (`mode`, `completeness`, `coverage[]`, low/high watermark) + `Completeness` enum
   (`COMPLETENESS_UNKNOWN`, `COMPLETE`, `PARTIAL`, `BEST_EFFORT`) from `design/03` "Range
   scans". Add `SubscribeRangeRequest` and `RangeCoverageCertificate`
   (namespace, start_key, end_key, owner_member_id, owner_epoch, high_watermark, valid_until)
   from `design/03`/`design/05`.

2. **Scan dispatcher (TS-050/051), `internal/kv/scan.go` + `scan_merge.go`.** Implement the
   four modes from `design/03` "Range scans":
   - `local-only`: iterate local `wavesdb` only (debugging/analytics).
   - `cache-fast` (default): scan local cache + local durable copies only; emit
     `BEST_EFFORT`/`PARTIAL` — **never `COMPLETE`** (TS-050: partial never marked complete;
     `design/16` "scan cache-fast returns metadata BEST_EFFORT").
   - `routed-eventual`: split `[start,end)` by range, ask known holders (M4 holder/range
     directory) for each subrange, and k-way merge sorted keys in **bounded batches** with
     reused buffers to avoid GC pressure on large scans (GC-pause risk,
     `IMPLEMENTATION_STRATEGY.md` section 4). TS-051: scan contacts known holders and merges
     sorted keys.
   - `cache-complete`: serve from the range cache only if a valid certificate covers
     `[start,end)`; otherwise downgrade to `best_effort` or fall back to routed
     (`design/03` certificate rule).
   Every scan stream emits the `ScanHeader` first (`design/03` "Do not silently return partial
   cache scans as complete").

3. **Range coverage certificate (TS-052), `internal/cache/range_cert.go`.** A range owner
   issues a `RangeCoverageCertificate` to a subscriber that holds an active `SubscribeRange`
   subscription or a recent full snapshot (`design/03`/`design/05` "Range cache"). A
   `cache-complete` scan validates the certificate (owner epoch current, not expired, covers
   the requested range) before reporting `COMPLETE`; on expiry it downgrades. TS-052: a
   complete cache scan works only with a valid certificate — this is **property 4** and a CI
   merge gate.

4. **TTL buckets (TS-053), `internal/ttl/bucket.go`.** On write, compute
   `expires_at = local_hlc_physical + ttl` and index the key into a coarse bucket
   `/kv_meta/ttl/{bucket_start_unix_ms}/{namespace}/{key_hash}/{key}` with default
   `ttlBucketSeconds=60` (`design/02` "TTL storage"; `design/03` "TTL semantics"). `wavesdb`'s
   native `Txn.Put` ttl arg may back per-record expiry, but the bucket index is what the
   sweeper scans to bound write amplification.

5. **Read filter, `internal/ttl/read_filter.go`.** Default: a node **may** hide a record it
   detects as expired but stale reads are allowed (no promise all nodes detect expiry
   simultaneously). Strict namespaces with `hideExpiredOnRead=true` hide detected-expired
   records on read (`design/03` "TTL semantics"). Applies to both point reads (M3) and scans.

6. **Sweeper (TS-053), `internal/ttl/sweeper.go`.** Background loop scans old TTL buckets,
   emits **tombstone mutations** for expired current versions (which replicate and participate
   in conflict resolution like any tombstone, M3/M7), and marks obsolete versions for later
   compaction via `storage.CompactRange` (`design/02` "Compaction behavior"; `design/03`
   "Physical GC"). The sweeper coordinates with the M4 repair engine: do **not** repair an
   expired key unless a tombstone is needed for convergence (`design/05` "Repair loop").

7. **Metrics.** Add `kv_range_cache_coverage_count` and TTL sweep counters (`design/05`
   "Metrics"; `design/14_observability.md`).

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 6 and the TS tickets:

- A cache scan returns completeness metadata; a partial result is never marked complete.
  (M6; TS-050)
- A routed-eventual scan contacts known holders and merges sorted keys. (TS-051)
- A complete-range cache scan reports `COMPLETE` only with a valid coverage certificate.
  (M6; TS-052)
- TTL eventually hides/deletes records; physical cleanup happens after a grace period; expired
  records do not break conflict convergence. (M6; TS-053)

## Verification

1. **Unit:** `scan_test` asserts a `cache-fast` scan over a partially-cached range emits
   `BEST_EFFORT`/`PARTIAL` and never `COMPLETE` (TS-050, property 4), and that
   `cache-complete` returns `COMPLETE` only when a valid certificate is present and downgrades
   on expiry (TS-052); `ttl_test` covers bucket assignment, best-effort hide-expired, tombstone
   emission, and compaction eligibility (TS-053, `design/16` "TTL" unit list).
2. **Docker integration (`tests/integration/scan_ttl_test.go`)** on the 3-node cluster
   (`design/16` "scan cache-fast returns BEST_EFFORT", "range cache with certificate returns
   COMPLETE"):
   - Populate a range, scan `cache-fast` from a node holding only part of it, assert the header
     is `BEST_EFFORT` (TS-050).
   - `routed-eventual` scan from a node and assert merged, sorted, de-duplicated keys across
     holders (TS-051).
   - Establish a `SubscribeRange` + certificate, run `cache-complete`, assert `COMPLETE`; let
     the certificate expire and assert the next scan downgrades (TS-052).
   - Put keys with a short TTL; assert reads eventually stop returning them and the sweeper
     emits tombstones; verify a concurrent conflicting write still converges deterministically
     with the expired record present (TS-053: "expired records do not break conflict
     convergence").
3. **CI gate enabled:** this milestone turns on the "dynamic cache mislabels partial scan as
   complete" merge gate (`IMPLEMENTATION_STRATEGY.md` section 3 / `design/16` CI gates).
4. **Bank invariant:** add a TTL-bearing account variant and confirm at quiescence that
   expired-key tombstones did not corrupt the conserved-total invariant.
