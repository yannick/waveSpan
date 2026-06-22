---
title: Vector Search
section: Reference
order: 9
summary: Storing vectors inside wavesdb, exact vs approximate (ANN) search, the delta index for incremental updates, and the search procedures.
---

# Vector Search

WaveSpan embeds a vector engine (`internal/vector`) so embeddings live next to your graph and KV data, queryable through the same Cypher surface.

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
