# 16. Testing strategy

## Test philosophy

This is an eventually consistent distributed system. Tests must verify convergence, durability thresholds, and metadata honesty rather than pretending reads are linearizable.

**Model-aware, not linearizability-aware.** We deliberately do **not** run a Knossos/Elle
serializability or linearizability checker against the data path. WaveSpan permits stale
reads, concurrent versions, and partition-time divergence by contract (doc 00, doc 20), so a
linearizability checker would report *expected* "violations". The chaos layer and the
property tests below verify the **declared** model (eventual convergence, origin+1 durability,
HLC-LWW/keep-siblings, lazy TTL, optional session read-your-writes). The canonical
implementation of that model-aware verification is the correctness harness in
`25_correctness_harness.md`.

## Unit tests

### Versioning

- HLC ordering;
- tie-breakers;
- vector-clock encode/decode;
- mutation ID idempotency;
- tombstone ordering.

### Conflict resolvers

- LWW value/value;
- LWW delete/value;
- keep-siblings;
- CRDT counter if implemented;
- deterministic resolution under shuffled input.

### TTL

- bucket assignment;
- best-effort hide expired;
- tombstone emission;
- compaction eligibility.

### Placement

- distinct-node filter;
- compliance boundary hard failure;
- prefer-local-geo spillover;
- latency graph scoring;
- disk/load pressure penalties.

## Integration tests

### Docker cluster tests

- 3-node cluster forms gossip membership;
- 5-node cluster populates latency graph;
- write ACK requires one nearby durable replica;
- target-N fanout fills asynchronously;
- read miss creates dynamic cache replica;
- update propagates to dynamic cache;
- cache source failure triggers resync;
- scan cache-fast returns metadata `BEST_EFFORT`;
- range cache with certificate returns `COMPLETE`.

### Global replication tests

- two Docker clusters active-active;
- writes in both clusters replicate;
- conflicts converge by LWW;
- keep-siblings returns both versions;
- anti-entropy repairs missed log entries;
- VPN-like outage queues logs and resumes.

### Graph tests

- create nodes and edges;
- match by label;
- match by property;
- expand outgoing and incoming edges;
- delete produces tombstones;
- index rebuild restores query results.

### Vector tests

- exact search returns mathematically correct top-k;
- ANN search returns approximate candidates;
- delta index makes new vector visible;
- tombstoned vector filtered from results;
- global replicated vector appears in remote exact search after apply;
- ANN index catches up after background merge.

## Chaos tests

The canonical chaos / fault-injection layer is the **correctness harness**
(`25_correctness_harness.md`, built by M14, consumed by M12 TS-102). It composes the workloads
and nemeses below and asserts the model-aware invariants — do not build a second bespoke
chaos suite. The faults run continuously in CI nightly:

- kill random container every 10-60 seconds;
- delete one data directory and restart;
- pause a node for 2 minutes;
- partition cluster into halves;
- inject 100ms latency between groups;
- drop 10% packets;
- restart all gateways;
- simulate clock skew;
- fill disk to pressure threshold.

## Property tests

Required properties (each is implemented by a named checker in the correctness harness,
`25_correctness_harness.md`):

1. A successful write has at least two durable copies on distinct nodes at the acknowledgement instant, unless the second node dies immediately after ACK. — harness `durability` checker.
2. Repeated anti-entropy with no new writes eventually converges all live nodes to the same winning version/siblings. — harness `convergence` checker.
3. LWW resolver is deterministic under all message orders. — harness `lww-determinism` checker.
4. Dynamic cache never reports `COMPLETE` range coverage without a valid coverage certificate. — harness `completeness-honesty` checker.
5. Idempotent retry with same request ID creates one logical mutation. — harness `idempotency` checker.

The harness adds further model-aware checkers (`no-lost-update-per-policy`,
`session-monotonicity`, `ttl-bound`) that reinforce properties 2 and the doc-00/doc-03
session and TTL guarantees. None of these is a linearizability check.

## Load tests

Benchmarks:

- point write throughput;
- origin+1 write latency under spot churn;
- read hit latency from dynamic cache;
- read miss latency with closest-holder fetch;
- target-N repair throughput;
- cache subscription fanout cost;
- exact vector search latency by vector count/dimension;
- ANN recall/latency curves;
- Cypher traversal latency by graph degree/depth.

## Test fixtures

Create deterministic fixtures:

```text
fixtures/kv_basic.json
fixtures/conflicts_lww.json
fixtures/conflicts_siblings.json
fixtures/graph_social.json
fixtures/vector_1536_small.json
fixtures/vector_128_recall.json
```

## CI gates

Do not merge if:

- origin+1 invariant fails;
- conflict resolver nondeterministic;
- dynamic cache mislabels partial scan as complete;
- global anti-entropy fails convergence test;
- vector exact search returns wrong top-k;
- graph index rebuild loses nodes/edges;
- operator generates invalid StatefulSet.

