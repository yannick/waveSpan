---
title: Vector Search
section: Reference
order: 9
summary: The vector-as-key API (Put/Get/Delete/Search), distributed HNSW with per-node indexes + scatter-gather, exact vs approximate search, and the delta index.
---

# Vector Search

WaveSpan embeds a vector engine (`internal/vector`) so embeddings live next to your graph and KV data. You can query them through the Cypher surface **or** the dedicated vector-as-key API where the embedding *is* the key and an arbitrary proto is the payload.

## Vector-as-key API

`VectorService` exposes a small KV-shaped surface (the embedding is `repeated float`; the payload is opaque bytes):

| RPC | Meaning |
|-----|---------|
| `VectorPut(collection, vector, payload)` | Store an embedding (key) + payload (value). Replicated to holders. |
| `VectorGet(collection, vector)` | Exact lookup of the payload for a stored embedding (local or closest holder). |
| `VectorDelete(collection, vector)` | Tombstone the record; every holder's index is purged. |
| `VectorSearch(collection, query, k, …)` | Cluster-wide **k-nearest-neighbour** search. |

```jsonc
// VectorSearch — the query vector goes in `query`
{ "collection": "docs", "query": [0.12, -0.04, …], "k": 10, "includePayload": true }
// → neighbors: [{ vector, payload, distance, score, vectorId }, …], completeness
```

The identity of a vector is derived from the embedding itself (a hash of its bytes), so identical embeddings dedupe and an exact `VectorGet`/`VectorDelete` addresses the same record.

## Distributed architecture

- **Per-node HNSW.** Every holder maintains a live ANN index over just the vectors it holds. A write replicates through the origin+1 coordinator; each holder's index is fed from the record-apply path, and is **rebuilt from the store on reboot**.
- **Scatter-gather search.** `VectorSearch` fans `SearchLocal` out to holders, each returns its local top-k fragment, and the coordinator merges the global top-k (dedup by vector id) and attaches payloads. It declares `Completeness` — `PARTIAL` if a holder was unreachable.
- **Bucket routing.** Each vector is assigned a coarse **bucket** by a per-collection quantizer (LSH today; IVF available). Nodes gossip which buckets they hold, so a search with `nprobe > 0` quantizes the query, finds its nearest buckets, and scatters **only to the nodes holding those buckets** — not the whole cluster. `nprobe` trades recall for fan-out; `nprobe = 0` scatters to all holders.
- **Eventual consistency.** A freshly-written vector is searchable within the merge interval; deletes converge across holders via intra-cluster anti-entropy.

> Roadmap (design 29): bucket-**affinity placement** — consistent-hash a bucket onto a small node-set so a bucket concentrates on few nodes and routing fan-out is minimal, plus IVF training/versioning.

## Storage model

In v1, raw vectors are stored **inline in `wavesdb`** (object-storage offload is a future enhancement):

```text
/vector/{collection}/raw/{vector_id}        # the raw float vector
/vector/{collection}/meta/{vector_id}        # dimensions, metadata
/vector/{collection}/ann/{index}/{segment}/… # ANN index segments
/vector/{collection}/delta/{index}/{seq}      # pending index mutations
```

Because vectors are ordinary keyspace records, they replicate and cache with the same per-namespace policy as everything else.

## Exact vs approximate

| Mode | Procedure | Behaviour |
|------|-----------|-----------|
| Exact | `vector.searchExact` | Scans candidates and computes true distances. Accurate, slower. |
| Approximate | `vector.searchApprox` | Uses the ANN index (HNSW, pure Go, no cgo). Fast, approximate recall. |
| Auto | `vector.search` | Picks based on index availability and collection size. |

```cypher
CALL vector.search('docs', $embedding, 8)
YIELD id, score
RETURN id, score
```

## The ANN index

Approximate search is backed by an **HNSW** index implemented in pure Go (no cgo, keeping the static-binary build). The index is organized into **segments** that are built and compacted asynchronously.

## The delta index — staying fresh

Vectors change. Rather than rebuild the whole index on every write, WaveSpan tracks mutations in a **delta index**:

1. Each vector write appends to `/vector/{collection}/delta/{index}/{seq}` via the mutation log.
2. A background builder folds deltas into ANN segments incrementally.
3. Queries consult both the built segments **and** the unmerged delta, so recently-written vectors are searchable before a full rebuild.

This means index freshness is **eventual**, consistent with the rest of the system. The `vector.*` procedures report freshness metadata so a query can tell how current the index is.

## Performance notes

- Keep collections scoped — a `replicationFactor` of `"all"` on a large vector namespace replicates every vector to every node, which is rarely what you want for big embedding sets.
- Exact search cost scales with candidate count; prefer ANN for large collections and reserve exact search for small or high-precision cases.
- Index freshness lag is observable via metrics (see [Operations & Observability](doc:operations-observability)).
