# 18. Implementation roadmap

## Milestone 0: repository and build system

Deliverables:

- repository layout;
- proto generation;
- CI skeleton;
- Docker image skeleton;
- local config parser;
- structured logging;
- metrics endpoint.

Acceptance:

- `Wavespan-node --config dev.yaml` starts;
- `/healthz` and `/metrics` respond;
- CI runs unit tests.

## Milestone 1: local WavesDB wrapper

Deliverables:

- storage abstraction;
- WavesDB wrapper;
- versioned record encoding;
- local batch write;
- local scan;
- storage UUID persistence.

Acceptance:

- put/get/scan works on one node;
- restart preserves data;
- storage UUID persists.

## Milestone 2: membership and latency graph

Deliverables:

- Docker static discovery;
- gossip membership;
- liveness states;
- latency probe EWMA/p95;
- admin membership endpoint;
- holder summary type.

Acceptance:

- 3-node Docker cluster forms;
- killing one node marks it suspect/unreachable;
- latency graph edges are visible.

## Milestone 3: KV origin+1 writes

Deliverables:

- public KV gRPC service;
- local Put/Get/Delete;
- StoreReplica internal API;
- placement candidate selection;
- ACK after origin+1;
- idempotency key support.

Acceptance:

- write fails if no nearby replica exists;
- write succeeds when one nearby replica stores durably;
- killing origin after success leaves value on replica.

## Milestone 4: target-N fanout and repair

Deliverables:

- asynchronous fanout;
- holder directory;
- repair worker;
- under-replication metrics;
- spot churn repair test.

Acceptance:

- target-N is reached after ACK;
- killing one holder triggers repair;
- repair converges without manual action.

## Milestone 5: dynamic cache subscriptions

Deliverables:

- FetchReplica;
- dynamic cache local store;
- SubscribeKey stream;
- update propagation;
- cache eviction;
- subscription lag metrics.

Acceptance:

- read miss fetches from closest holder;
- second read hits local dynamic cache;
- update on origin propagates to cache;
- broken subscription resyncs or downgrades.

## Milestone 6: range scans and TTL

Deliverables:

- cache-fast scan;
- routed-eventual scan;
- range coverage certificate;
- lazy TTL buckets;
- TTL sweeper;
- scan metadata.
- Prefix Scans

Acceptance:

- cache scan returns completeness metadata;
- complete range cache only reports complete with certificate;
- TTL eventually hides/deletes records;
- expired records do not break conflict convergence.

## Milestone 7: global active-active replication

Deliverables:

- ClusterPeer config;
- outbound/inbound mutation streams;
- HLC-LWW conflict resolver;
- keep-siblings resolver;
- anti-entropy summaries;
- global lag metrics.

Acceptance:

- two Docker clusters replicate both directions;
- concurrent writes converge deterministically;
- keep-siblings returns siblings;
- outage queues logs and resumes.

## Milestone 8: graph storage and Cypher subset

Deliverables:

- node/edge record encoding;
- label/property indexes;
- adjacency scans;
- Cypher parser subset;
- MATCH/WHERE/RETURN;
- CREATE/SET/DELETE;
- query guardrails.

Acceptance:

- social graph fixture queries pass;
- graph indexes rebuild from records;
- query metadata declares cache/freshness.

## Milestone 9: vector exact search

Deliverables:

- raw vector storage;
- exact distance functions;
- vector index CRD parsing;
- Cypher `vector.searchExact`;
- exact distributed top-k merge.

Acceptance:

- exact top-k matches test oracle;
- graph + exact vector query works;
- replicated vector is searchable after apply.

## Milestone 10: vector ANN and delta index

Deliverables:

- HNSW or ANN abstraction;
- mutable delta index;
- background segment merge;
- Cypher `vector.searchApprox`;
- exact reranking;
- vector index rebuild.

Acceptance:

- new vector visible through delta index;
- ANN recall/latency benchmark produced;
- tombstoned vectors filtered.

## Milestone 11: Kubernetes operator

Deliverables:

- CRDs;
- StatefulSet reconcile;
- Services;
- PVCs;
- topology spread;
- PDB;
- status conditions;
- drain protocol.

Acceptance:

- cluster deploys in Kubernetes;
- scale-up works;
- rolling restart keeps cluster available under expected eventual semantics;
- CRD validation rejects invalid policies.

## Milestone 12: hardening

Deliverables:

- mTLS;
- auth roles;
- backup/restore prototype;
- chaos tests;
- load tests;
- dashboards and alerts;
- documentation.

Acceptance:

- nightly chaos passes convergence properties;
- metrics dashboards cover required signals;
- backup/restore validates data and rebuilds vector indexes.

