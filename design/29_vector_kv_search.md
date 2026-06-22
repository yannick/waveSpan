# 29 — Vector KV: nearest-neighbour search in the KV surface

## Goal

Expose a **vector-as-key KV** interface: the fixed-length embedding is the key, an arbitrary
proto is the value/payload, and the cluster answers approximate **k-nearest-neighbour** queries
fast. We optimise lookup latency *within* a cluster by routing queries to only the nodes that hold
the relevant region of the vector space — leveraging, and extending, WaveSpan's gossiped holder
knowledge of "which key prefixes live on which node".

This builds on the existing per-node HNSW engine and scatter-gather; it does **not** reinvent them.

### Decisions (locked)

1. **Quantizer: LSH *and* IVF, both available now.** A collection declares which it uses. LSH needs
   no training (cold-start friendly); IVF (k-means centroids) gives balanced buckets and better
   routing selectivity once trained. Bucket ids are versioned so a quantizer change re-buckets lazily.
2. **Bucket-affinity placement up front.** A bucket's primary holders are chosen by consistent-hashing
   the bucket id onto the membership ring (geo/distinct-node still honoured), so a bucket concentrates
   on a small, *computable* node-set and per-query fan-out is minimal from day one.
3. **Extend `VectorService`** (which already has `Put` + `SearchLocal`) with the coordinator-level
   vector-as-key operations, rather than a new service or folding into `KvService`.

> **Foundation first (see the plan).** An adversarial review established that vectors are *local-only*
> today (`VectorService.Put` does not replicate intra-cluster), the per-node HNSW is not rebuilt on
> reboot, and the delta merger never runs. Those must be fixed (plan Phase 0) **before** any routing —
> the statement elsewhere that this needs "no change to the replication/repair engines" is **false** for
> the follower-HNSW apply path. See `29_vector_kv_search_plan.md`.

## Reused building blocks (ground truth)

| Component | Location | Role |
|-----------|----------|------|
| HNSW index | `internal/vector/ann/hnsw.go` (`Insert`/`Delete`/`Search(q,k,efSearch)`) | per-collection, per-node ANN |
| Local fragment search | `internal/vector/search.go` `LocalSearch(...)` | exact or ANN top-k on a node |
| Top-k merge + dedup | `internal/vector/exact.go` `MergeTopK` (dedup by `collection+vector_id`) | global merge |
| Scatter closure | `internal/cypher/scatter.go` `NewVectorScatter` | per-peer `SearchLocal` fan-out |
| Delta index + merger | `internal/vector/{delta,merge,segment}.go` (`vector.mergeInterval`, 5s) | fresh-write visibility |
| Holder directory | `internal/cache/directory.go` (`AddHeldKey`/`ResolveHolders`, 16 KiB add-only bloom, ~13k-key capacity) | who-holds-what, gossiped |
| Placement | `internal/placement/placement.go` `Select(...)` (latency/geo; **no hash affinity today**) | replica choice |
| Collection config | `internal/config/vector.go` (`WAVESPAN_VECTOR_INDEXES=name:collection:metric:dims`) | index declaration |
| Vector proto | `proto/wavespan/v1/vector.proto` (`VectorService`, `VectorRecord`, `SearchLocal*`) | RPC surface |

## Data model

A vector record is a KV record with a structured key:

```
/vec/{collection}/{qver}/{bucket}/{vechash}     value = VectorRecord{ values[], payload, version, tombstone }
```

- `qver` — quantizer version (so a re-trained IVF or rotated LSH produces a disjoint key range and the
  old buckets age out via TTL/repair without a stop-the-world rebuild).
- `bucket` — coarse quantizer output (uint16/uint32), the **routing prefix**.
- `vechash` — `hash(vector_bytes)`; this is the record identity, so identical vectors dedup. Exact
  `VectorGet` is by exact bytes (document the float-equality caveat).
- `VectorRecord` gains `bytes payload = 10;` (today it carries only a `metadata` map).

The bucket count B is a per-collection parameter (256–4096 typical): small enough that a node's
held-bucket set is tiny, large enough that a bucket is a small slice of the space.

## Quantizers (both implemented; per-collection)

`Quantizer` interface: `Bucket(vec) uint32` and `TopBuckets(vec, nprobe) []uint32`, plus a `Version()`.

- **LSH** — `b = sign-bits of (R · vec)` for a fixed random projection `R` (seed is the only artifact).
  `TopBuckets` flips the lowest-margin bits to enumerate nearby buckets. Zero training; works on the
  first write.
- **IVF** — `b = argmin_c ||vec − centroid_c||`; `TopBuckets` = the `nprobe` nearest centroids. Centroids
  are trained from a sample once a collection passes a size threshold and stored as a small, versioned
  artifact in an `all`/`global` namespace (replicated everywhere, gossip-friendly, exactly like the
  config snapshot). Retraining bumps `qver`.

Collection config grows: `quantizer ∈ {lsh, ivf}`, `numBuckets`, `nprobe` default, HNSW `M` /
`efConstruction` / `efSearch`. The recall↔fan-out dials (`nprobe`, `efSearch`) are **tunables**
(`internal/tunables`) so they're adjustable live cluster-wide via the config plane.

## Bucket-affinity placement (up front)

New placement mode for vector records: the **primary** holder-set is `ring(bucket, qver) → N nodes`
via consistent hashing (rendezvous/HRW over alive members), filtered by the existing geo/distinct-node
rules; latency only tie-breaks *within* the affinity set. Consequences:

- A bucket concentrates on a deterministic, small node-set → minimal per-probe fan-out.
- Routing can **compute** candidate holders from the ring directly, then *confirm/augment* with the
  holder directory (which still catches dynamic cache replicas and in-flight migrations).
- On membership change the ring shifts for a minority of buckets; the existing **repair engine**
  re-replicates affected buckets to their new primaries (re-using anti-entropy), and old copies age
  out. Consistent hashing bounds the churn.

`placement.Select` gains a `BucketAffinity{ Bucket, QVer }` option; the KV coordinator passes it for
`/vec/...` writes. Non-vector writes are unchanged (latency-only).

## Bucket advertisement (the "prefix → node" knowledge)

We advertise the **bucket id**, not every vector. v1 proposed reusing the holder bloom for this; the
review killed that idea — the bloom is a shared 16 KiB add-only filter (polluting it raises the FP rate
for normal KV reads, and it can never *remove* a migrated/emptied bucket). Instead, a node gossips an
**explicit per-collection held-bucket set** — a small sorted `[]uint32` of `(qver,bucket)` ids,
piggybacked on gossip like holder summaries / config deltas. It is exact (no false positives), supports
**removal**, and is **periodically recomputed from the local store**, so a bucket that migrates away or
empties is de-advertised. A query resolves holders per probed bucket against this set (and, in Phase 3,
against the affinity ring computed locally — independent of gossip staleness). This is the requested
"which prefixes are on which node", done right.

## Write path

`VectorPut(collection, vector, payload, ttl?)` on the origin node:
1. `qver, b = quantizer.Bucket(vector)`; `id = hash(vector)`.
2. Store `VectorRecord{values, payload, version}` at `/vec/{coll}/{qver}/{b}/{id}` via the normal
   origin+1 coordinator, but with `BucketAffinity{b,qver}` placement.
3. Insert into the local HNSW **delta** (searchable within `vector.mergeInterval`).
4. On each holder ack, advertise the bucket (`AddHeldKey(__vecbuckets__/coll, …)`).

The replication/repair *engines* are reused unchanged, but a **follower-side apply hook** (feed each
holder's HNSW from replicated vector records, à la `applier.SetVectorSink`) and a **boot rebuild** of
the HNSW from the local store are required (plan Phase 0) — otherwise a replicated vector persists as
bytes but is never searchable on a follower or after reboot.

## Search path

`VectorSearch(collection, query, k, nprobe, efSearch, rerank, include_payload)`:
```
qver = current quantizer version for collection
buckets = quantizer.TopBuckets(query, nprobe)
nodes   = ⋃ over buckets:  ring(bucket,qver) ∪ ResolveHolders("__vecbuckets__/coll", bucket)
nodes   = dedupe; for each bucket keep the lowest-latency holder (latency graph)
frags   = scatter SearchLocal(query, k', efSearch, buckets, rerank) → nodes      # parallel, partial-tolerant
result  = MergeTopK(frags, k)   # dedup by collection+vector_id
if include_payload: payloads returned inline by SearchLocal (size-capped) → no 2nd round-trip
meta.completeness = COMPLETE iff every probed bucket had a reachable, merged holder; else PARTIAL
```

`SearchLocalRequest` gains `repeated uint32 buckets` (restrict the local HNSW scan to probed buckets;
a node may ignore it and search its whole index — still correct). `VectorHit` gains optional inline
`bytes payload`.

## Consistency & recall

Eventual, consistent with the rest of WaveSpan: a fresh vector is visible once merged (≤ merge
interval) and replicated async; `VectorSearch` declares `Completeness` (PARTIAL on an unreachable
probed-bucket holder or unmerged delta). **Recall is bounded by `nprobe` bucket coverage, not by
`rerank`:** a true nearest neighbour in an un-probed bucket (e.g. just across a quantization boundary)
is never retrieved, so rerank — which exact-rescores only the *already-retrieved* `k×4` candidates —
cannot recover it. `nprobe` is the recall dial; `efSearch`/`rerank` refine *within* probed buckets. A vector
replicated on several holders is de-duplicated in `MergeTopK`.

## API (extend VectorService)

**Vector representation.** Every vector field in the API is `repeated float` (the query, the stored
vector, the returned neighbour) — self-describing (dimension = `len`, validated against the
collection's declared `dimensions`) and consistent with the existing `VectorRecord.values` /
`SearchLocalRequest.query`. "Vector as key" is an *internal* encoding: the server canonicalizes the
floats to little-endian float32 bytes and derives `vechash = hash(bytes)` for the storage key — clients
never handle the byte form. Only the **payload** is `bytes` (the caller's arbitrary proto).

```proto
service VectorService {
  rpc VectorPut(VectorPutReq) returns (VectorPutRes);
  rpc VectorGet(VectorGetReq) returns (VectorGetRes);
  rpc VectorDelete(VectorDeleteReq) returns (VectorDeleteRes);
  rpc VectorSearch(VectorSearchReq) returns (VectorSearchRes); // NEW coordinator scatter-gather
  rpc SearchLocal(SearchLocalRequest) returns (SearchLocalResponse); // existing per-node fragment (+buckets,+payload)
}

// Search: the query vector is `query` (field 2). len(query) must equal the collection's dimensions.
message VectorSearchReq { string collection=1; repeated float query=2; uint32 k=3; uint32 nprobe=4;
                          uint32 ef_search=5; bool rerank=6; bool include_payload=7; }
message Neighbor        { repeated float vector=1; bytes payload=2; float distance=3; string holder=4; }
message VectorSearchRes { repeated Neighbor neighbors=1; ResponseMeta meta=2; Completeness completeness=3; }

// Vector-as-key CRUD: the vector is `vector` (repeated float); payload is the caller's proto bytes.
message VectorPutReq    { string collection=1; repeated float vector=2; bytes payload=3; optional int64 ttl_ms=4; }
message VectorPutRes    { Version version=1; ResponseMeta meta=2; }
message VectorGetReq    { string collection=1; repeated float vector=2; }
message VectorGetRes    { bool found=1; bytes payload=2; ResponseMeta meta=3; }
message VectorDeleteReq { string collection=1; repeated float vector=2; }
message VectorDeleteRes { ResponseMeta meta=1; }
```
`VectorRecord` += `bytes payload = 10;`. The embedding is the key throughout; the API speaks floats.

## Implementation phases

1. **Vector-as-key API + baseline kNN** — `VectorPut/Get/Delete/Search` + payload field, wired over the
   existing scatter-to-holders + `MergeTopK` + inline payloads. No routing yet (correct, full fan-out).
2. **Quantizers + bucket keys + advertisement** — LSH and IVF behind the `Quantizer` interface;
   bucket-prefixed keys; `__vecbuckets__` advertisement; routed scatter (directory + nprobe).
3. **Bucket-affinity placement** — consistent-hash `bucket→nodes` in `placement.Select`; ring-based
   routing + directory confirmation; repair-driven re-placement on membership change; IVF training +
   `qver` versioning / re-bucketing.
4. **Optimisation & tuning** — closest-replica pick, hot-bucket/query caching (dynamic cache replicas),
   `nprobe`/`efSearch`/HNSW params as live tunables, metrics (recall proxy, fan-out, per-bucket load).

## Risks / non-goals

- **Skew:** a hot bucket on few nodes can hotspot; mitigate with replica fan-out within the affinity
  set and closest-replica routing; surface per-bucket load in metrics.
- **Rebalancing cost:** affinity churn on membership change is bounded by consistent hashing but
  non-zero; piggybacks on repair/anti-entropy.
- **Not** exact kNN, **not** linearizable, **not** cross-cluster global search in v1 (per-cluster;
  global is a later composition over the existing active-active path).
