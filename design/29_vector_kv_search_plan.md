# 29 — Vector KV Search — Implementation Plan (v2, post-critique)

Implements `design/29_vector_kv_search.md`. **v2 incorporates an adversarial code review** that
overturned v1's central assumption. Corrected ground truth and a new foundational **Phase 0** lead.

## Corrected ground truth (from the review — verify-or-it's-a-bug)

| Claim in v1 / design | Reality (file:line) | Impact |
|---|---|---|
| "route VectorPut through the coordinator so vectors replicate" | **Vectors are local-only.** `VectorService.Put` (`internal/vector/service.go:73-95`) does `store.Put` (local `Batch`) + local `onWrite` (this node's HNSW) + optional cross-**cluster** `globalTap`. No origin+1. Integration test confirms: *"a raw vector lives only on the node that ingested it (no origin+1 fanout)."* | **BLOCKER.** Intra-cluster replication must be built. |
| "insert into local HNSW delta on each holder" | The follower `onStored` hook (`cmd/wavespan-node/main.go:192-196`) only does `cacheDir.AddHeldKey` + `subSource.Notify` — **it never decodes the record or touches a vector index.** | **BLOCKER.** No follower-HNSW path exists. |
| HNSW survives reboot | Boot builds **empty** indexes (`indexset.go` `buildSegment(..., nil)`); `RebuildLiveIndex` (`internal/vector/rebuild.go:8`) exists but is **only called by backup/restore + tests**, never in `main.go`. | **BLOCKER (latent bug today).** Stored vectors invisible after restart. |
| "delta + 5s merge works" | `Merger.Run` (`internal/vector/merge.go`) has **zero non-test callers**; the delta is brute-forced (`delta.go:48`). `vector.mergeInterval` tunable drives nothing. | **MAJOR.** Per-node search is O(n) over all local vectors. |
| "128 KiB bloom" (design L33) | `bloomBits = 1<<17` = **16 KiB** (`internal/cache/bloom.go:12`). ~13k-key capacity, no overflow handling, FP degrades silently. | **MAJOR.** Re-do capacity math; don't pollute it. |
| holder bloom can advertise buckets | `Bloom` is **add-only** (no delete/clear); `Directory` has `AddHeldKey` but **no remove**. | **MAJOR.** A migrated/emptied bucket is advertised forever. |
| affinity is compatible with origin+1 | Coordinator records **origin as a durable holder before placement runs** (`coordinator.go` `recordHolder` precedes `placement.Select`). | **MAJOR.** Off-affinity origin copies; needs explicit reconciliation. |
| "no change to replication/repair engines" (design L107) | **False** for the follower-HNSW path. | Fix the design doc. |
| cross-cluster apply hook | `applier.SetVectorSink(func(v){ vstore.Put(v); indexSet.OnWrite(v) })` (`main.go:305-308`). | **The template** for the intra-cluster apply hook. |
| `Put` validates dimensions | It **overwrites**: `rec.Dimensions = len(values)` (`service.go:82`). No check vs collection. | Build validation. |
| collections are dynamic | **Static boot config only** (`WAVESPAN_VECTOR_INDEXES`, `config/vector.go`, consumed once `main.go:284-294`). No DDL. | Need a collection registry for per-collection quantizer params. |
| `VectorRecord` has TTL | It does **not**; `Wrap` never sets `expires_at` (`global.go:25`). | `VectorPut.ttl_ms` is all-new plumbing → defer to a later phase. |

**The one question that gates everything:** *how does a vector written on node A get into node B's HNSW
and survive B's reboot?* Today: it doesn't. Phases 2–4 are premature until that path exists.

---

## Phase 0 — Correct distributed vector store (the real foundation)

Goal: a vector written anywhere is **replicated to its holders**, each holder **maintains its HNSW**
from the records it receives, the index **survives reboot**, the **delta is drained**, and search
**scatters to holders** (not all-alive). No quantizer/routing yet — just *correct*.

0.1 **Boot rebuild.** In `cmd/wavespan-node/main.go` after `NewIndexSet`, call
    `vector.RebuildLiveIndex(store, collection, meta, params)` per configured collection so the HNSW is
    repopulated from `CFVectorRaw`. Fixes the latent reboot bug. (Also call after a repair/backfill
    stream lands a batch of vectors.)

0.2 **Run the merger.** Per live index, `go merger.Run(ctx, tun.Get("vector.mergeInterval").Duration())`
    (and re-read on the Hot tunable's `OnApply`). Drains the delta into the HNSW segment so search is
    not O(n). Wires the dead tunable.

0.3 **Intra-cluster replication + follower apply hook (the blocker).**
    - VectorPut writes a `StoredRecord{Kind: RECORD_KIND_VECTOR, value: marshal(VectorRecord)}` through
      the **KV coordinator** (`internal/kv/coordinator.go`) so it gets origin+1 + target-N placement +
      repair *for free* (it's a KV record with a vector kind). `recordstore.BuildRecord` must learn the
      `RECORD_KIND_VECTOR` kind (enum already exists, `proto/common.proto:67`).
    - Add an **intra-cluster vector sink**: extend the follower `onStored(ns, key)` hook so that when
      `ns` is the vector namespace it decodes the `StoredRecord` → `VectorRecord` and calls
      `indexSet.OnWrite(v)` (insert) or `indexSet.OnDelete(v)` (tombstone). This is the missing path;
      **mirror `applier.SetVectorSink`** (`main.go:305-308`) which already does exactly this for the
      cross-cluster applier. Deletes/TTL-tombstones must call `hnsw.Delete` on every holder, not just
      the origin.
    - Decommission the local-only `vector.Store.Put` path on the write side (Put now goes through the
      coordinator); `Store` remains the read/scan/rebuild source of truth on each holder.

0.4 **Scatter to holders, not all-alive.** Replace `vectorPeers()` (`main.go:359-367`, every
    `StateAlive` member) with a holder-aware peer set: for the (un-bucketed) collection in Phase 0,
    that's still "members that hold the collection" — use the directory / member set that actually has
    vectors. (Bucket-precise routing arrives in Phase 2; Phase 0 just stops blindly hitting nodes that
    hold nothing.)

0.5 **Identity + validation.**
    - `vector_id = vechash = blake3(canonical float32-LE)`, set **server-side** (replaces caller-supplied
      ids) so `MergeTopK`'s `(collection, vector_id)` dedup actually dedups identical embeddings
      (review #12). One atomic change.
    - **Validate** `len(values) == collection.Dimensions` (reject, don't overwrite — review #11) and
      reject non-finite floats.

**Phase 0 deliverable:** a correct, replicated, reboot-surviving, delta-drained vector store whose kNN
scatters to holders. Memory is now bounded by replication factor per node (not all-nodes). This is the
true "baseline kNN" — v1 mislabeled it Phase 1.

**Phase 0 tests:** write on node A → search on B/C returns it (replication); restart a node → vectors
still searchable (rebuild); write 10k vectors → delta stays bounded, latency stable (merger); delete →
gone from every holder's HNSW (follower delete); dimension mismatch rejected.

---

## Phase 1 — Vector-as-key API surface

On the now-correct foundation, expose the clean API (`design/29` "API" section, `repeated float`
vectors, `bytes` payload):

1.1 **Proto** (`vector.proto`): `VectorPut/VectorGet/VectorDelete/VectorSearch` + messages; `bytes
    payload = 10` on `VectorRecord`; `repeated uint32 buckets` + inline `bytes payload` on `SearchLocal*`.
    Regen Go+TS with `PATH=$HOME/go/bin` (protoc-gen-go **v1.36.11** — older shadows downgrade all stubs).

1.2 **Coordinator `VectorSearch`** reuses the scatter (`NewVectorScatter`) → `SearchLocal` → `MergeTopK`
    → payloads inline if `include_payload`. `VectorGet/Delete` = hash → KV `Get`/`Delete(tombstone)`.

1.3 **Collection registry / minimal DDL.** Per-collection quantizer params (Phase 2) need a registry
    beyond static boot env. Phase 1 adds a `CreateCollection(name, dim, metric, quantizer, numBuckets,
    nprobe)` admin RPC writing a versioned collection-config record into an `all`/`global` namespace
    (replicated like the IVF centroid artifact). Boot env remains a bootstrap shortcut. Metric is
    inherited from the collection (not per-request); LSH/IVF must use the **same** metric.

1.4 **Tests:** API round-trip (put→get→search) with payloads; dim validation surfaced via the API.

---

## Phase 2 — Quantizers + bucket advertisement + routed scatter

2.1 **`Quantizer` interface** (`internal/vector/quantizer`): `Bucket(vec) (qver uint32, bucket uint32)`,
    `Probe(vec, nprobe) []uint32`, `Version()`.
    - **LSH** (random hyperplanes): bucket = sign bits of `R·vec`; `Probe` = **multi-probe** — perturb
      the lowest-|projection| bits in a defined order to yield `nprobe` buckets. **Define `nprobe`
      semantics explicitly for LSH** (number of perturbation vectors), and document that small-nprobe
      LSH recall is weaker than IVF (review #9).
    - **IVF** (k-means): bucket = argmin centroid; `Probe` = `nprobe` nearest centroids.
    Both use the collection's metric.

2.2 **Bucket-prefixed keys**: `/vec/{collection}/{qver}/{bucket}/{vechash}`. Quantize on the write path
    (in the coordinator wrapper) so the bucket prefix participates in placement (Phase 3) and routing.

2.3 **Bucket advertisement — explicit set, NOT the per-key bloom** (fixes review #5/#6/#7). Add a new
    gossiped per-node, per-collection **held-bucket set** (a small sorted `[]uint32` of qver+bucket
    ids), piggybacked on gossip exactly like holder summaries / config deltas (reuse the
    `SetConfigHooks`-style provide/consume plumbing I added). It is **exact** (no bloom FP), **supports
    removal**, and is **periodically recomputed from the local store** so an emptied/migrated bucket is
    de-advertised. This replaces v1's "abuse the holder bloom" idea entirely.

2.4 **Routed scatter**: `buckets = quantizer.Probe(query, nprobe)`; `nodes = ⋃ heldBucketDir.holders(coll,
    b)`; scatter `SearchLocal(query, k', efSearch, buckets, rerank)` only there. **Completeness is honest:
    PARTIAL unless every probed bucket had a reachable advertised holder** — and because the set is exact
    (not bloom), a missing holder is detectable (review #6). For freshly-joined holders not yet gossiped,
    Phase 3's ring fallback closes the gap.

2.5 **IVF training**: train centroids from a reservoir sample once a collection passes a size threshold;
    store the centroid set as a versioned artifact in an `all`/`global` namespace; bump `qver`. Until
    trained, the collection uses LSH (cold-start).

2.6 **Tests:** quantizer recall sanity (LSH vs IVF); routing selectivity (fan-out = #holders of probed
    buckets, asserted via metric); de-advertisement after delete/migration; PARTIAL on missing holder.

---

## Phase 3 — Bucket-affinity placement + ring routing (reconciled with origin+1)

3.1 **Affinity primaries**: `ring(qver, bucket) → N alive members` via rendezvous (HRW) hashing,
    filtered by geo/distinct-node; latency tie-breaks within the set.

3.2 **Origin+1 reconciliation (fixes review #8): forward vector writes to a ring-primary.** The node that
    receives `VectorPut` does **not** originate; it forwards to `ring(bucket)[0]` (mirroring the
    `AdminPut`/`kvWriter` forward to a chosen coordinator), which becomes the origin and does origin+1
    **within the affinity set**. So `origin ∈ affinity set` and there are no off-affinity origin copies.
    (Alternative kept in reserve: origin keeps a copy and repair removes it once the affinity set is
    durable — but that needs the de-advertisement from 2.3, which we now have.)

3.3 **Ring routing**: candidate holders = `ring(bucket)` (computed locally, **independent of gossip** — no
    staleness false-negative) ∪ advertised held-bucket holders (catches dynamic cache replicas /
    in-flight migration). Per bucket, query the **closest** holder (latency graph), dedup.

3.4 **Rebalancing**: on membership change the ring shifts for a minority of buckets; the **repair engine**
    re-replicates affected buckets to new primaries; old holders **remove** the bucket from their
    advertised set (2.3) and their copies age out / are GC'd. Bound churn via HRW.

3.5 **qver re-bucketing**: a new quantizer version writes a disjoint key range; a background migration
    re-quantizes existing vectors and the old `qver` range ages out. Reads union both qvers during
    migration.

---

## Phase 4 — Optimisation, tunables, metrics, memory

- **Tunables** (live via the config plane): `vector.nprobe`, `vector.efSearch`, `vector.hnsw.m`,
  `vector.hnsw.efConstruction` (today hardcoded `M:16,EfConstruction:200,EfSearchDefault:64`).
- **Memory** (review #11): per-node HNSW is in-memory + unbounded. Phase 0 bounds it by RF; affinity
  bounds it by bucket share. Add a per-collection HNSW memory metric + a soft cap / spill policy
  (or document the operating envelope).
- **TTL** (deferred): add `expires_at_unix_ms` to `VectorRecord` + sweeper-driven `hnsw.Delete` on
  expiry. Explicitly out of Phases 0–3.
- **Metrics**: query fan-out (#nodes scattered), per-bucket load/skew, recall proxy, delta depth,
  merger lag, scatter-unreachable, HNSW size. These also gate the affinity/skew tuning.

---

## Recall honesty (review #9, into the contract)

Recall is bounded by **`nprobe` bucket coverage**, *not* by `rerank`: a true nearest neighbour in an
un-probed bucket (e.g. just across a quantization boundary) is never a candidate, so rerank cannot
recover it (`search.go:17-30` rescoring `k*4` *retrieved* candidates only). The API/`ResponseMeta`
states this; `nprobe` is the recall dial, `efSearch`/`rerank` refine *within* probed buckets.

## Revised sequencing & risk

- **Phase 0 is non-negotiable and first** — replication + follower-HNSW + boot-rebuild + merger. It also
  fixes two latent production bugs (no reboot rebuild; dead merger) independent of this feature.
- Phases 1–4 are each shippable on top. Affinity (3) depends on the explicit held-bucket set (2.3) for
  de-advertisement, so 2 precedes 3.
- **Top residual risks:** memory blow-up if Phase 0 ships before affinity on large collections (mitigate
  with RF, not replicate-everywhere); hot-bucket skew; IVF retrain cost/timing; recall under partition
  (PARTIAL + ring fallback). Each has a metric in Phase 4.
