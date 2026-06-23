---
title: Vector Search
section: Reference
order: 9
summary: The vector-as-key API, distributed HNSW with replication + scatter-gather, coarse-bucket routing (LSH/IVF), affinity placement, consistency, and tuning.
---

# Vector Search

WaveSpan embeds a vector engine (`internal/vector`) so embeddings live next to your graph and KV
data. You can query them through the Cypher surface **or** the dedicated **vector-as-key API** where
the embedding *is* the key and an arbitrary proto is the payload — a distributed, approximate
k-nearest-neighbour store with the same eventual-consistency contract as the rest of WaveSpan.

## At a glance

- **Vector as key, proto as value.** `VectorPut(collection, vector, payload)` stores an embedding with
  an opaque payload; `VectorGet` / `VectorDelete` address it by the exact embedding; `VectorSearch`
  returns the nearest neighbours.
- **Distributed & durable.** Vectors replicate (origin+1) and every holder maintains its own HNSW
  index, rebuilt from the store on restart.
- **Routed by buckets.** A coarse quantizer assigns each vector a bucket; a query reaches only the
  nodes holding its nearest buckets, not the whole cluster.
- **Affinity placement.** A bucket's vectors concentrate on a deterministic node-set (rendezvous
  hashing), so routing is maximally selective.

---

## The API

`VectorService` (data port) exposes the vector-as-key surface. Every vector field is `repeated float`
(self-describing, validated against the collection's `dimensions`); the byte key form is internal.
Only the **payload** is opaque bytes.

| RPC | Meaning |
|-----|---------|
| `VectorPut(collection, vector, payload)` | Store an embedding (key) + payload (value). Replicated. |
| `VectorGet(collection, vector)` | Exact payload lookup for a stored embedding. |
| `VectorDelete(collection, vector)` | Tombstone; every holder's index is purged. |
| `VectorSearch(collection, query, k, nprobe, …)` | Cluster-wide kNN. |

```jsonc
// VectorSearch — the query vector goes in `query`; len(query) must equal the collection dimension.
{
  "collection": "docs",
  "query": [0.12, -0.04, 0.31, …],
  "k": 10,
  "nprobe": 16,          // buckets to probe (recall vs fan-out); 0 = scatter to all holders
  "includePayload": true
}
// → { neighbors: [ { vector, payload, distance, score, vectorId }, … ],
//     completeness: COMPLETE | PARTIAL }
```

The **identity** of a vector is a hash of its embedding (`vechash`), so identical embeddings dedupe
and `VectorGet`/`VectorDelete` address the same record. Search results are de-duplicated by id when a
vector is held on several nodes.

You can also search via Cypher procedures (`vector.search`, `vector.searchExact`, `vector.searchApprox`)
which compose with graph queries.

---

## Distributed architecture

### Per-node HNSW + lifecycle
Every holder maintains a live ANN index (HNSW, pure Go — no cgo) over just the vectors it holds:

1. **Write → replicate.** `VectorPut` routes through the origin+1 coordinator, so the vector replicates
   to its holders and (if enabled) taps cross-cluster.
2. **Apply → index.** A single record-apply observer feeds the vector store + HNSW on **every** holder,
   from every path (origin, replica, anti-entropy, bootstrap, cross-cluster), applying only the LWW
   winner. Replicated deletes purge the index too.
3. **Boot → rebuild.** On startup each index is rebuilt from the authoritative raw vectors, so a
   restart never loses searchability.
4. **Delta → merge.** New writes land in a small delta for immediate visibility; a background merger
   folds the delta into the main HNSW segment on the `vector.mergeInterval` tunable.

### Scatter-gather search
`VectorSearch` runs a local fragment on the coordinator, scatters `SearchLocal` to the relevant
holders, merges the global top-k (dedup by id), and attaches each neighbour's embedding + payload.
It declares **`Completeness`** — `PARTIAL` if a holder was unreachable.

---

## Bucket routing

The trick that avoids querying every node: a coarse **bucket** stamped on each vector, and knowledge
of which node holds which buckets.

### Quantizers
Each collection has a quantizer that maps a vector to a small bucket id (`Bucket`) and, for a query,
the buckets to probe (`Probe`):

| Quantizer | How | When |
|-----------|-----|------|
| **LSH** | sign pattern against random hyperplanes; multi-probe flips the lowest-margin bits | the cold-start default — zero training, angular/cosine |
| **IVF** | nearest of k k-means centroids; probe = nprobe nearest centroids | balanced buckets; installed automatically once a collection has enough vectors |

The LSH planes are seeded from the collection name, so **every node derives identical buckets** — no
coordination needed.

**IVF is trained automatically.** A single elected node (lowest member id) periodically gathers a
cross-node sample, trains k-means centroids, and publishes a **versioned, replicated centroid
artifact**; every node reads and installs it, so the whole cluster agrees on buckets. A retrain bumps
the version (`qver`) and starts new buckets — old vectors stay put and simply re-advertise under the
new version, so **no data migration is needed** and routing stays correct across the change.

`nprobe` is the recall dial: more probed buckets → more holders queried → higher recall.

### Held-bucket directory
Each node tracks the set of buckets it holds per collection and **gossips it** (an explicit,
removable set — recomputed periodically from the store so emptied/migrated buckets are
de-advertised). A query consults this directory to find the holders of its probed buckets.

### Affinity placement
A bucket's vectors are placed on a deterministic node-set chosen by **rendezvous (HRW) hashing** of the
bucket id. Consequences:

- All vectors in a bucket concentrate on the **same few nodes**, so routing reaches exactly them.
- Holders are **computable locally** from the ring (gossip-independent) — covering nodes that just
  joined and haven't advertised yet — *and* confirmed by the gossiped directory.
- A membership change moves only **~1/N of buckets** (HRW), so rebalancing is cheap.
- **Closest-replica routing:** the query hits just the lowest-latency *ring* member of each probed
  bucket (a full replica), so it's a single hop per bucket instead of querying every replica.
- **Background re-bucketing:** each node continuously migrates any vector it holds that's off its
  current ring back onto the ring, then drops the local copy — reclaiming the off-ring origin copies
  affinity placement leaves behind, and re-concentrating after an IVF retrain reassigns buckets. The
  durable record is kept until the ring has acknowledged the move, so there's no data loss. (While a
  bucket is mid-migration, routing also queries the partial holders, so results stay correct.)

Putting it together, a search:

```text
buckets = quantizer.Probe(query, nprobe)
holders = (gossiped held-bucket holders)  ∪  (HRW ring of each bucket)
scatter SearchLocal(query, …) → holders  →  merge top-k  →  attach payloads
```

---

## Consistency & recall

- **Eventual**, like the rest of WaveSpan. A freshly-written vector is searchable within the merge
  interval and replicates async; deletes converge across holders via intra-cluster anti-entropy.
- **Recall is bounded by `nprobe` coverage, not by reranking.** A true nearest neighbour in an
  un-probed bucket (e.g. just across a quantization boundary) is never retrieved — `rerank` only
  exact-rescores the candidates already pulled from probed buckets. Raise `nprobe` for higher recall
  at the cost of more fan-out; `nprobe = 0` scatters to all holders (exact over the candidate union).
- `Completeness` on the response is honest: `PARTIAL` when a holder was unreachable.

---

## Tuning

| Tunable | Default | Effect |
|---------|---------|--------|
| `vector.mergeInterval` | `5s` | how often the delta folds into the HNSW segment (freshness vs rebuild cost) |
| `nprobe` (per query) | — | buckets probed → recall vs fan-out |
| `ef_search` (per query) | `64` | HNSW beam width → recall vs CPU within a node |
| `rerank` (per query) | off | exact-rescore ANN candidates from probed buckets |

Operational notes:
- Keep big embedding collections off `replicationFactor: "all"` — that copies every vector to every
  node (the worst case for the per-node HNSW's memory).
- Per-node HNSW is in-memory; affinity placement bounds each node to its share of buckets.

### Metrics
Per-collection gauges on each node's `/metrics`:

| Metric | Meaning |
|--------|---------|
| `wavespan_vector_local_vectors` | live vectors held locally |
| `wavespan_vector_held_buckets` | distinct buckets held locally |
| `wavespan_vector_bucket_skew` | local bucket-size skew (max/mean; `1.0` = balanced, higher = a hot bucket) |
| `wavespan_vector_quantizer_version` | live quantizer version (rises when an IVF retrain installs) |
| `wavespan_vector_search_scattered_nodes` | histogram of peer nodes a query scattered to (routing fan-out) |

---

See design docs 08 (vector engine) and 29 (vector KV search) for the full specification.
