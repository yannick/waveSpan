# 08. Vector engine

## Goal

Support exact and approximate vector search, integrated with the Cypher graph layer and replication system.

## Storage model

V1 stores raw vectors inside WavesDB.

Vector record:

```protobuf
message VectorRecord {
  string collection = 1;
  bytes vector_id = 2;
  repeated float values = 3;
  string dtype = 4; // float32 in v1
  uint32 dimensions = 5;
  map<string, Value> metadata = 6;
  optional bytes graph_node_id = 7;
  Version version = 8;
  bool tombstone = 9;
}
```

Keys:

```text
/vector/{collection}/raw/{vector_id} -> VectorRecord
/vector/{collection}/meta/{vector_id} -> VectorMeta
/vector/{collection}/delta/{index_id}/{seq} -> VectorIndexMutation
/vector/{collection}/ann/{index_id}/{segment_id}/... -> ANN segment data
```

## Index types

### Exact index

Exact search scans raw vectors or compact vector blocks and computes distance exactly.

Use exact search for:

- small collections;
- correctness tests;
- reranking ANN candidates;
- filtered queries where candidate set is small;
- debugging recall.

### Approximate index

V1 approximate index should use HNSW unless implementation constraints push to a simpler ANN first.

Use approximate search for:

- large collections;
- low-latency semantic search;
- graph/vector hybrid search.

## Distance metrics

Required:

- cosine;
- dot product;
- Euclidean/L2.

Normalize vectors at ingest when policy requires cosine optimization.

## Visibility model

Default:

```yaml
vectorVisibility: write-visible-with-delta
```

Write path:

1. write raw vector record;
2. append mutation log;
3. replicate origin+1 locally;
4. insert into local delta index;
5. return success;
6. background merges delta into main ANN segment;
7. global replication applies raw record and updates remote local delta indexes.

Search path:

```text
search main ANN segments
search delta index
merge candidates
filter tombstones/TTL/conflicts
optional exact rerank
return top-k
```

## Exact search algorithm

For each shard/partition:

1. use graph/property filters if provided;
2. scan candidate raw vectors;
3. compute distance with SIMD if available;
4. maintain top-k heap;
5. return local top-k to coordinator;
6. coordinator merges top-k from fragments.

Exact search must be correct over records visible to the local node under eventual consistency. It is not globally fresh.

## Approximate search algorithm

For HNSW-like index:

1. choose local index partitions from vector index metadata;
2. search main graph with `efSearch`;
3. search mutable delta index;
4. merge candidates;
5. fetch raw vector records for candidate validation;
6. filter deleted/stale records;
7. exact rerank if requested.

## Cypher procedures

```cypher
CALL vector.search('doc_embedding', $query, 10)
YIELD node, score
RETURN node.id, score
```

```cypher
CALL vector.searchExact('doc_embedding', $query, 10)
YIELD node, score
RETURN node, score
```

```cypher
CALL vector.searchApprox('doc_embedding', $query, 50, {efSearch: 200})
YIELD node, score
MATCH (node)-[:IN_DOCUMENT]->(doc:Document)
WHERE doc.status = 'published'
RETURN doc.title, node.text, score
ORDER BY score DESC
LIMIT 10
```

## Vector geo-replication

Replicate raw vectors, not ANN internals, by default.

Local cluster:

- origin+1 for raw vector write;
- target-N local fanout;
- local delta index update;
- dynamic cache for hot vector payloads.

Global active-active:

- stream raw vector mutations;
- apply conflict policy;
- update local delta index;
- background merge into local ANN segments.

This keeps replication fast and avoids cross-cluster index coupling.

## Dynamic vector caching

Vector query results can warm:

- raw vector payloads;
- graph nodes returned by vector search;
- metadata filters;
- small ANN entrypoint metadata.

Do not blindly subscribe every vector result to updates. High fanout can be explosive.

Default dynamic cache behavior:

```yaml
vectorCache:
  cacheResultPayloads: true
  subscribeTopK: false
  subscribeGraphNeighborhood: optional
  maxCachedVectorsPerPod: 1000000
```

If a vector is frequently read or part of a hot graph neighborhood, promote it to a durable nearby replica through repair/promotion.

## Conflict handling

Vector records use record-level conflict policy.

Default:

```yaml
conflictPolicy: hlc-last-write-wins
```

For graph-attached vectors, node property conflict policy may determine whether the vector property wins.

If siblings exist, vector search must either:

- search winning sibling only;
- search all siblings and mark conflict;
- exclude conflicted records.

Default:

```yaml
vectorConflictReadPolicy: winner-only
```

## Vector partitioning

Vectors are assigned to a partition by their identity, and ANN indexes are built per
partition. There are two partitioning rules depending on whether the vector is attached to
a graph node:

```text
graph-attached vector (graph_node_id set):
    partition = hash(graph_id + node_id)

bare vector (no graph node):
    partition = hash(collection_id + vector_id)
```

Graph-attached vectors deliberately share the partition function with the graph layer
(`hash(graph_id + node_id)`, doc 07), so a vector and its node land on the same holders and
hybrid graph+vector queries stay local. Bare vectors have no node, so they partition by
their own collection and id.

### ANN indexes are per-partition, derived, and never authoritative

Each partition has its own ANN index (and exact block index). These indexes are **derived
local state**: they are never replicated as authoritative data. Global replication carries
only raw vector records and metadata (doc 06); **each cluster rebuilds its own ANN indexes
locally** from those raw records. This keeps the wire format free of index internals and
avoids cross-cluster index coupling.

### Per-partition rebuild and atomic segment publish

Rebuild operates one partition at a time so a rebuild of one partition never blocks queries
against others:

```text
for each partition needing (re)build:
    1. snapshot the partition's raw vector records at a build watermark;
    2. skip tombstones and observed-expired records;
    3. build an immutable segment off to the side (new segment_id);
    4. atomically publish the segment by swapping the partition's active-segment pointer
       to segment_id (single metadata write; readers see old-or-new, never half-built);
    5. continue serving queries from the previous segment until the swap commits;
    6. garbage collect the previous segment once no in-flight query references it.
```

The active-segment pointer is the only mutable handle; publishing is the atomic pointer
swap, so concurrent searches always read a complete segment. Delta-index inserts continue
during rebuild and are folded in at the next merge (see visibility model).

## Index rebuild

ANN and exact block indexes are derived. Rebuild from raw vector records, partition by
partition, using the per-partition atomic segment-publish protocol above.

Rebuild steps:

1. scan raw vector records;
2. skip tombstones and expired records if observed;
3. group by index partition;
4. build immutable segment;
5. atomically publish segment metadata (active-segment pointer swap);
6. garbage collect old segment after no queries reference it.

## Metrics

```text
vector_raw_put_latency_ms
vector_delta_index_lag_ms
vector_ann_query_latency_ms
vector_exact_query_latency_ms
vector_ann_recall_sampled
vector_index_rebuild_seconds
vector_index_segments_count
vector_candidates_filtered_total
vector_global_apply_lag_seconds
```

## Implementation checklist

- [ ] Raw vector record format implemented.
- [ ] Exact search implemented.
- [ ] Mutable delta index implemented.
- [ ] ANN index abstraction implemented.
- [ ] HNSW implementation or binding selected.
- [ ] Cypher vector procedures implemented.
- [ ] Vector result exact rerank implemented.
- [ ] Vector mutation log implemented.
- [ ] Global raw vector replication implemented.
- [ ] Index rebuild implemented.

