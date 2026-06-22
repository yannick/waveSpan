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

> Try it: the [Cypher Console](doc:overview) tab runs queries against this node and renders the streamed rows, and the [Node Explorer](doc:overview) gives a force-directed view of the graph.
