# 07. Graph and Cypher layer

## Goal

Expose graph database functionality through a production subset of Cypher. Do not expose SQL.

## Graph model

WaveSpan uses a property graph model:

```text
Node:
  id: bytes/string
  labels: set<string>
  properties: map<string, Value>

Relationship:
  id: bytes/string
  type: string
  start_node_id: NodeId
  end_node_id: NodeId
  properties: map<string, Value>
```

## Supported Cypher subset for v1

Required:

```cypher
MATCH
OPTIONAL MATCH
WHERE
CREATE
SET
DELETE
RETURN
WITH
UNWIND
ORDER BY
LIMIT
SKIP
```

Recommended after core works:

```cypher
MERGE
REMOVE
DETACH DELETE
```

Not required in v1:

```cypher
LOAD CSV
CALL arbitrary procedure except built-in vector procedures
subqueries beyond simple CALL/YIELD
schema DDL beyond WaveSpan CRDs/Admin API
full openCypher compatibility
```

## Vector extensions

Expose vector search as built-in procedures:

```cypher
CALL vector.search(indexName, queryVector, k)
YIELD node, score
RETURN node, score
```

```cypher
CALL vector.searchExact(indexName, queryVector, k)
YIELD node, score
RETURN node, score
```

```cypher
CALL vector.searchApprox(indexName, queryVector, k, {efSearch: 100})
YIELD node, score
MATCH (node)-[:PART_OF]->(d:Document)
RETURN d.title, node.text, score
```

## KV built-ins

Expose the namespaced, versioned, replicated KV store (the same store the gRPC KV API
exposes) directly from Cypher. Reads route through the same routed reader (local-first +
closest-holder fetch); writes go through the same coordinator (origin+1 durability +
replication fanout). This is one coherent store, not a side channel.

### `kv.get(namespace, key)` — scalar function

Usable inline in any expression (`RETURN`, `WHERE`, computed properties, etc.).

Returns the value as a string, or `null` if the key is absent, tombstoned, or expired.

A bad call (wrong arity, non-string arguments, unconfigured backend) is a hard query error,
never `null`. `null` is reserved for "the read found no live value."

Reads are eventual, exactly like the gRPC KV `Get`: local-first, with a closest-holder
fetch on a local miss. If that holder fetch fails (holder unreachable), the read currently
surfaces as `null` — the same behavior the gRPC KV API exhibits, so the two stay coherent,
but it means a transient `null` can in principle hide an unreachable holder rather than a
genuinely absent key. (A future enhancement should mark such a read partial via
`partial_graph_possible`; today it does not.)

`kv.get` exposes **string** values only. A value the gRPC KV API wrote as non-UTF8 bytes is
not representable as a Cypher string and yields a hard error (not silent truncation); read
such keys via the gRPC KV API, or a future `kv.getBytes` built-in.

### `CALL kv.put(namespace, key, value [, options])` — procedure

Writes `value` (a UTF-8 string) under `namespace`/`key`. Yields a single `version` column
containing the committed HLC version's stable id.

The optional fourth argument is an options map:

```text
{ttlMs: <integer>}   // per-key TTL using the engine's native TTL mechanism
```

### `CALL kv.delete(namespace, key)` — procedure

Tombstones the key. Yields a single `version` column.

### Semantics and constraints

**Namespace is always explicit.** Matches the KV API's namespaced addressing; there is no
implicit default namespace.

**Each write is independent.** `kv.put` and `kv.delete` each issue their own KV write
(origin+1 + replication fanout), committed independently of any graph mutation in the same
Cypher statement. A statement that both `CREATE`s graph data and calls `kv.put` commits the
two separately — the KV write does NOT join the graph mutation's `wavesdb` transaction.

**Argument validation is strict.** `namespace`, `key`, and `value` must be strings;
`ttlMs` must be an integer. A wrong type is a hard query error, not a silent null.

**Values are strings** (UTF-8 bytes). Structured data must be serialised by the caller.

### Examples

```cypher
// read inline, joined against a graph match
MATCH (u:User {id: 'u1'})
RETURN u.name, kv.get('profile', u.id) AS profile

// filter graph rows on KV state
MATCH (u:User) WHERE kv.get('flags', u.id) = 'banned' RETURN u

// write (yields version) and delete
CALL kv.put('profile', 'u1', '{"v":2}') YIELD version RETURN version
CALL kv.delete('profile', 'u1')

// write with TTL
CALL kv.put('session', 'tok-abc', 'active', {ttlMs: 3600000}) YIELD version RETURN version
```

## Key encoding

Node record:

```text
/graph/{graph}/node/{node_id} -> NodeRecord
```

Labels:

```text
/graph/{graph}/label/{label}/{node_id} -> empty
```

Outgoing adjacency:

```text
/graph/{graph}/edge/out/{src_id}/{type}/{dst_id}/{edge_id} -> EdgeRecord
```

Incoming adjacency:

```text
/graph/{graph}/edge/in/{dst_id}/{type}/{src_id}/{edge_id} -> EdgeRecord
```

Relationship by ID:

```text
/graph/{graph}/edge/by_id/{edge_id} -> EdgeRecord
```

Property index:

```text
/graph/{graph}/prop/{label}/{property}/{encoded_value}/{node_id} -> empty
```

## Records

```protobuf
message NodeRecord {
  bytes node_id = 1;
  repeated string labels = 2;
  map<string, Value> properties = 3;
  Version version = 4;
  bool tombstone = 5;
}

message EdgeRecord {
  bytes edge_id = 1;
  bytes start_node_id = 2;
  bytes end_node_id = 3;
  string type = 4;
  map<string, Value> properties = 5;
  Version version = 6;
  bool tombstone = 7;
}
```

## Graph mutation atomicity

A graph write touches multiple records and indexes.

Example:

```cypher
CREATE (a:User {id: '1'})-[:FOLLOWS]->(b:User {id: '2'})
```

Writes:

- node record for `a` if created;
- node record for `b` if created;
- label index entries;
- property index entries;
- edge by ID;
- outgoing adjacency;
- incoming adjacency;
- mutation log entries.

### Coordinator-local atomicity

V1 graph mutation atomicity is **local-batch atomic on the coordinator pod**. All records
and index entries for one graph mutation — node records, label/property index entries, edge
by-id, outgoing and incoming adjacency, and the mutation-log entries — are written in a
single `wavesdb` `Txn` spanning the graph column families:

```text
txn := store.NewTxn()
txn.Put(nodeRecordKey, ...)        // graph node CF
txn.Put(labelIndexKey, ...)        // graph index CF
txn.Put(propIndexKey, ...)         // graph index CF
txn.Put(edgeByIdKey, ...)          // graph edge CF
txn.Put(edgeOutKey, ...)           // graph adjacency CF
txn.Put(edgeInKey, ...)            // graph adjacency CF
txn.Put(mutationLogKey, ...)       // mutation-log CF
txn.Commit()                       // all-or-nothing on this pod
```

The `Txn` either commits the whole batch or none of it, so a single-pod observer never sees
half-applied graph state (e.g. an edge whose adjacency entries are missing).

### Cross-pod coherence is eventual

Distributed global atomicity is **not** guaranteed. If related graph records map to
different pods, the coordinator commits its local batch and replication carries the pieces
to other holders independently, converging eventually.

Therefore a query reading across pods may transiently observe partial graph state:

- an edge whose endpoint node has not yet been applied on the reader's holders;
- a node visible before all of its adjacency entries arrive;
- an index entry pointing at a record version not yet present locally (the index filter in
  "Index maintenance" suppresses such rows).

This is acceptable under the eventual-consistency product contract but **must be visible to
the client**. The coordinator sets:

```text
QueryMeta.partial_graph_possible = true
```

whenever any participating member's `high_watermark` predates the query's start version —
i.e. some holder has not yet caught up to the point in time the query began at, so the
read may straddle an in-flight cross-pod mutation.

**Client contract:** when `partial_graph_possible = true`, results are a coherent snapshot
per pod but not necessarily globally coherent. Clients that require referential integrity
(every edge has both endpoints) must either tolerate transient gaps and re-query, or
restrict the query to a single graph partition.

## Query planner

Pipeline:

```text
parse -> semantic check -> logical plan -> physical plan -> execute fragments -> merge rows
```

Logical operators:

- node label scan;
- node property seek;
- expand outgoing;
- expand incoming;
- relationship type filter;
- property filter;
- projection;
- aggregation later;
- sort;
- limit;
- vector procedure call.

Physical operators:

- local WavesDB range scan;
- holder fetch;
- cache scan;
- routed scan;
- adjacency expansion;
- vector index search;
- remote fragment call;
- row merge.

## Fragment routing

The planner splits a query into fragments and must place each fragment without ever
falling back to a global broadcast.

Routing is by **range-directory affinity**. Each fragment scans or expands within some key
range (a label scan, a property seek, or an adjacency expansion under a node id). The
planner resolves that range to its known holders through the holder directory:

```text
1. derive the fragment's target range_id from its scan/expand bounds
   (e.g. hash(graph_id + node_id) for adjacency, label/property prefix for index scans);
2. look up range_id in the local range directory:
     known holders with fresh holder summaries -> route the fragment to the closest such
     holder by the latency graph (affinity hit);
3. if affinity is UNKNOWN (no directory entry, or only stale summaries):
     fan out to that range's candidate holders, bounded by maxRemoteFragments (default 128);
4. NEVER broadcast to all members.
```

```yaml
cypher:
  maxRemoteFragments: 128   # hard cap on fan-out when affinity is unknown
```

Affinity lookup:

```text
affinity(range_id) =
    range_directory[range_id].known_holders filtered to fresh holder summaries
```

Fallback path (affinity unknown):

```text
candidates(range_id) =
    holders implied by the range partition function + any summary-only holders;
    truncated to maxRemoteFragments closest by latency graph;
    if still empty, resolve via anti-entropy / range-summary exchange (doc 04),
    then retry — do not broadcast.
```

A fan-out that would exceed `maxRemoteFragments` is truncated to the closest candidates and
the query is marked partial in `QueryMeta` rather than widened to a broadcast. This bounds
the blast radius of an unrouted fragment to a single range's holders.

## Partitioning

Single tenant simplifies partitioning.

Default graph partition key:

```text
hash(graph_id + node_id)
```

Locality optimization:

- put adjacency lists close to source node;
- cache hot neighbor nodes dynamically;
- replicate hot graph neighborhoods using range subscriptions;
- for geo-aware graphs, allow a `homeGeo` property or label policy.

## Graph replication

Graph records use the same replication layer as KV:

- origin+1 local write acknowledgement;
- target-N local fanout;
- dynamic cache on read;
- optional global active-active mutation stream.

Conflict policy defaults:

| Record | Default policy |
|---|---|
| Node properties | HLC-LWW per property if possible, else record-level LWW |
| Labels | OR-set CRDT recommended |
| Relationships | edge ID is unique; concurrent creates are independent |
| Deletes | tombstone with HLC-LWW unless CRDT delete policy configured |

## Index maintenance

Indexes are derived state.

On mutation:

1. write authoritative node/edge record;
2. append graph index mutation log;
3. update local indexes synchronously if cheap;
4. otherwise update asynchronously;
5. query must filter index results against current record state.

## Read metadata

Cypher responses include:

```protobuf
message QueryMeta {
  string served_by_cluster_id = 1;
  repeated string participating_members = 2;
  QueryConsistency consistency = 3;
  Completeness completeness = 4;
  bool used_cache = 5;
  bool partial_graph_possible = 6;
  repeated string warnings = 7;
}
```

## Query guardrails

Default limits:

```yaml
cypher:
  maxRowsReturned: 10000
  maxIntermediateRows: 1000000
  maxTraversalDepth: 8
  maxRemoteFragments: 128
  queryTimeoutMs: 30000
  maxMemoryBytes: 1073741824
```

Unbounded graph traversal will destroy the system. Enforce guardrails from day one.

## Implementation checklist

- [ ] Cypher parser subset selected and tested.
- [ ] AST and logical plan implemented.
- [ ] Node/edge encoding implemented.
- [ ] Label index implemented.
- [ ] Property index implemented.
- [ ] Adjacency scans implemented.
- [ ] Basic MATCH/WHERE/RETURN implemented.
- [ ] CREATE/SET/DELETE implemented.
- [ ] Vector procedure call hook implemented.
- [ ] KV built-in function (`kv.get`) and procedures (`kv.put`, `kv.delete`) implemented.
- [ ] Distributed fragment execution implemented.
- [ ] Query guardrails implemented.

