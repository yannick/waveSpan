---
title: Cypher & Graph
section: Reference
order: 8
summary: The property-graph model encoded into the keyspace, and the production subset of Cypher WaveSpan supports for queries.
---

# Cypher & Graph

Alongside the KV API, WaveSpan exposes a **property graph** queried with a subset of Cypher. The graph is not a separate store — it is encoded into the same ordered `wavesdb` keyspace as everything else.

## The graph model

A standard property graph:

- **Nodes** carry labels and properties.
- **Relationships** are directed, typed, and carry properties.
- Edges are indexed in **both directions** for efficient traversal.

### Keyspace encoding

```text
/graph/{graph}/node/{node_id}
/graph/{graph}/label/{label}/{node_id}
/graph/{graph}/edge/out/{src}/{type}/{dst}/{edge_id}
/graph/{graph}/edge/in/{dst}/{type}/{src}/{edge_id}
/graph/{graph}/prop/{label}/{prop}/{encoded_value}/{node_id}
```

Because graph data lives in the KV keyspace, it inherits the same replication, caching, and consistency semantics — including the eventual-consistency contract and `Completeness` metadata.

## Supported Cypher subset

WaveSpan implements a **production-safe subset** of openCypher (`internal/cypher`), not full compatibility.

**Supported:**

```cypher
MATCH (n:User)-[:FOLLOWS]->(m:User)
WHERE n.age >= 30
RETURN n.name, m.name
ORDER BY n.name
LIMIT 25
```

| Clause | Status |
|--------|--------|
| `MATCH`, `OPTIONAL MATCH` | ✅ |
| `WHERE` | ✅ |
| `CREATE`, `SET`, `DELETE` | ✅ |
| `RETURN`, `WITH`, `UNWIND` | ✅ |
| `ORDER BY`, `LIMIT`, `SKIP` | ✅ |
| `MERGE`, `REMOVE`, `DETACH DELETE` | planned |

**Not supported:** `LOAD CSV`, arbitrary stored procedures (except the built-in vector procedures), full subqueries, and DDL beyond CRDs.

## Query execution

1. The parser builds an AST from the Cypher text.
2. The planner produces a logical plan, then physical fragments for distributed execution.
3. Results stream back, ending with a `QueryMeta` that declares **completeness** and any **partial-graph** warnings — a traversal that crossed an under-replicated range will say so rather than silently returning fewer rows.

## Vector procedures

Vector search is surfaced as built-in Cypher procedures, so graph and vector queries compose:

```cypher
CALL vector.search('embeddings', $queryVector, 10)
YIELD id, score
MATCH (n:Doc {id: id})
RETURN n.title, score
ORDER BY score DESC
```

See [Vector Search](doc:vector-search) for the indexing details.

## Reading & writing KV from Cypher

The key-value store is **not** a separate system you reach only over the gRPC [KV API](doc:kv-api) — it is the *same* store, addressed by the same `(namespace, key)` pairs and carrying the same eventual-consistency contract. Cypher exposes it through three built-ins, so a query can join graph structure against KV state, or mutate KV as part of a query.

| Built-in | Form | Yields / returns |
|----------|------|------------------|
| `kv.get(namespace, key)` | scalar function — use inline, in `RETURN` or `WHERE` | the value, or `null` if absent / tombstoned / expired |
| `CALL kv.put(namespace, key, value [, {ttlMs: N}])` | procedure | `version` (the committed HLC version) |
| `CALL kv.delete(namespace, key)` | procedure | `version` (the tombstone version) |

**`kv.get` — read inline.** It runs through the same read path as the gRPC `Get`: local-first, with a closest-holder fetch on miss. A UTF-8 value comes back as a string; a non-UTF-8 value comes back as `bytes` (lossless round-trip). If a holder is unreachable the read returns `null` *and* flags the query — the trailer's `partial_graph_possible` becomes true — so a `null` is never silently mistaken for "absent".

```cypher
-- join a graph match against per-node KV state
MATCH (u:User {id: 'u1'})
RETURN u.name, kv.get('profile', u.id) AS profile

-- filter graph rows on a KV flag
MATCH (u:User)
WHERE kv.get('flags', u.id) = 'banned'
RETURN u.name
```

**`kv.put` / `kv.delete` — write.** Both route through the same Coordinator as the gRPC KV API: origin+1 durable, then replication fanout. Each write is **independent** — it commits on its own and does *not* join a surrounding graph mutation's transaction. `kv.put` takes an optional 4th argument `{ttlMs: N}` for a per-key TTL (lazy, best-effort expiry). Argument arity/types are validated strictly — a malformed call is a hard error, never a silent `null`.

```cypher
-- write and consume the returned version
CALL kv.put('profile', 'u1', '{"v":2}') YIELD version
RETURN version

-- write with a 1-hour TTL
CALL kv.put('session', 'tok-abc', 'active', {ttlMs: 3600000}) YIELD version
RETURN version

-- tombstone a key
CALL kv.delete('profile', 'u1') YIELD version
RETURN version
```

Because the namespace is the same one the KV API uses, a record written with `kv.put('profile', 'u1', …)` is immediately the record you read back with `Get(namespace='profile', key='u1')`, and vice-versa.

> Try it: the [Cypher Console](doc:overview) tab runs queries against this node and renders the streamed rows, and the [Node Explorer](doc:overview) gives a force-directed view of the graph.
