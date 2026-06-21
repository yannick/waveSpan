# 19. Agent work items

This file converts the design into implementation tickets.

Each ticket has:

- ID;
- dependencies;
- implementation target;
- acceptance criteria.

## A. Foundation

### TS-001: Create repository skeleton

Depends on: none

Implement:

- source tree from `17_source_tree.md`;
- build system;
- proto generation;
- Dockerfile;
- CI unit-test workflow.

Acceptance:

- `make build` succeeds;
- `make test` succeeds;
- empty `Wavespan-node` starts and serves `/healthz`.

### TS-002: Implement configuration loader

Depends on: TS-001

Implement:

- YAML config;
- env overrides;
- Kubernetes/Docker runtime mode selection;
- validation errors.

Acceptance:

- invalid config fails fast;
- Docker sample config starts;
- Kubernetes env variables are parsed.

### TS-003: Implement common version types

Depends on: TS-001

Implement:

- HLC clock;
- writer sequence;
- version compare;
- mutation ID;
- protobuf encode/decode.

Acceptance:

- deterministic ordering tests pass;
- same request ID is idempotent.

## B. Local storage

### TS-010: Storage abstraction

Depends on: TS-001

Implement:

- `LocalStore` trait/interface;
- put/get/delete/batch/scan/snapshot;
- in-memory test implementation.

Acceptance:

- KV tests pass against in-memory store.

### TS-011: WavesDB wrapper

Depends on: TS-010

Implement:

- WavesDB open/close;
- logical column families or key prefixes;
- batch writes;
- range scans;
- compaction hook;
- error mapping.

Acceptance:

- put/get/scan persists across restart;
- storage UUID persists.

### TS-012: Stored record envelope

Depends on: TS-003, TS-010

Implement:

- `StoredRecord`;
- latest pointer;
- mutation log entry;
- tombstone encoding.

Acceptance:

- latest pointer rebuilds from records/log;
- tombstone hides older winner under LWW if version wins.

## C. Membership and placement

### TS-020: Docker discovery

Depends on: TS-002

Implement static seed discovery.

Acceptance:

- 3 Docker nodes discover each other.

### TS-021: Gossip liveness

Depends on: TS-020

Implement SWIM-style gossip and liveness states.

Acceptance:

- killed node becomes suspect/unreachable.

### TS-022: Latency graph

Depends on: TS-021

Implement RTT probes, EWMA, p95, edge expiry.

Acceptance:

- admin endpoint shows graph;
- injected latency changes placement score.

### TS-023: Placement engine

Depends on: TS-022

Implement filters and scoring.

Acceptance:

- distinct-node enforced;
- require-local-geo fails when no local peer;
- prefer-local-geo spills only when needed and allowed.

## D. KV and local replication

### TS-030: Public KV Get/Put/Delete local-only

Depends on: TS-012

Implement local single-node KV operations.

Acceptance:

- one-node put/get/delete works;
- response metadata present.

### TS-031: StoreReplica API

Depends on: TS-023, TS-030

Implement internal replica write API.

Acceptance:

- remote node stores durable replica and returns version.

### TS-032: Origin+1 write acknowledgement

Depends on: TS-031

Implement write coordinator ACK rule.

Acceptance:

- write fails with zero nearby candidates;
- write succeeds with one nearby durable ACK;
- response reports `ackedNearbyReplicas=1`.

### TS-033: Target-N fanout

Depends on: TS-032

Implement async fanout to target nearby replica count.

Acceptance:

- target count reached after ACK;
- failures are queued for repair.

### TS-034: Holder directory

Depends on: TS-033

Implement local holder records and gossiped holder summaries.

Acceptance:

- get miss can find a holder without broadcast.

### TS-035: Repair worker

Depends on: TS-034

Implement under-replication repair.

Acceptance:

- killing holder triggers replacement replica creation.

## E. Dynamic cache

### TS-040: FetchReplica API

Depends on: TS-034

Implement fetch from closest holder.

Acceptance:

- read miss fetches value from known holder.

### TS-041: Dynamic cache storage

Depends on: TS-040

Implement cache replica class and local latest pointer integration.

Acceptance:

- fetched key is served locally on second read;
- metadata says `LOCAL_DYNAMIC_CACHE`.

### TS-042: Key subscription stream

Depends on: TS-041

Implement SubscribeKey and update propagation.

Acceptance:

- update reaches subscribed dynamic cache.

### TS-043: Subscription resync and eviction

Depends on: TS-042

Implement gap detection, resnapshot, idle expiry, cache pressure eviction.

Acceptance:

- lost update triggers resync;
- idle cache is evicted.

## F. Scans and TTL

### TS-050: Cache-fast range scan

Depends on: TS-041

Implement local cache scan with best-effort metadata.

Acceptance:

- partial result never marked complete.

### TS-051: Routed-eventual range scan

Depends on: TS-034

Implement holder-based scan fanout and merge.

Acceptance:

- scan contacts known holders and merges sorted keys.

### TS-052: Range cache certificate

Depends on: TS-051

Implement range subscription and coverage certificate.

Acceptance:

- complete cache scan works only with valid certificate.

### TS-053: Lazy TTL

Depends on: TS-012, TS-030

Implement TTL buckets, sweeper, tombstones.

Acceptance:

- TTL keys eventually disappear;
- physical cleanup happens after grace period.

## G. Global active-active

### TS-060: Global peer config

Depends on: TS-002

Implement peer cluster config and connection management.

Acceptance:

- two Docker clusters connect.

### TS-061: Outbound/inbound mutation stream

Depends on: TS-033, TS-060

Implement global mutation log shipping.

Acceptance:

- write in cluster A appears in cluster B.

### TS-062: Conflict resolvers

Depends on: TS-061

Implement HLC-LWW and keep-siblings.

Acceptance:

- concurrent writes converge deterministically;
- sibling policy returns both versions.

### TS-063: Anti-entropy

Depends on: TS-062

Implement range hash summaries and repair exchange.

Acceptance:

- missed mutation during outage is repaired.

## H. Graph/Cypher

### TS-070: Graph record encoding

Depends on: TS-012

Implement node/edge records and adjacency keys.

Acceptance:

- create/read node and edge by ID works.

### TS-071: Graph indexes

Depends on: TS-070

Implement label and property indexes.

Acceptance:

- label scan and property seek return expected nodes.

### TS-072: Cypher parser subset

Depends on: TS-071

Implement parser/AST for production subset.

Acceptance:

- fixture queries parse.

### TS-073: Cypher planner/executor

Depends on: TS-072

Implement MATCH/WHERE/RETURN and CREATE/SET/DELETE.

Acceptance:

- graph fixture queries pass.

## I. Vector

### TS-080: Raw vector storage

Depends on: TS-012

Implement vector records and graph attachment.

Acceptance:

- vector put/get persists across restart.

### TS-081: Exact search

Depends on: TS-080

Implement exact distance search and top-k merge.

Acceptance:

- exact top-k matches oracle.

### TS-082: Vector Cypher procedures

Depends on: TS-073, TS-081

Implement `vector.searchExact`.

Acceptance:

- Cypher exact vector query works.

### TS-083: ANN index and delta

Depends on: TS-082

Implement ANN abstraction, HNSW, delta index.

Acceptance:

- new vector visible through delta;
- ANN benchmark produced.

### TS-084: Vector global replication integration

Depends on: TS-061, TS-083

Replicate raw vectors globally and update remote indexes.

Acceptance:

- vector written in cluster A becomes searchable in cluster B after apply.

## J. Kubernetes operator

### TS-090: CRD definitions

Depends on: TS-001

Implement CRDs in `12_crds.md`.

Acceptance:

- CRDs install into test cluster.

### TS-091: StatefulSet reconcile

Depends on: TS-090

Implement WaveSpanCluster controller.

Acceptance:

- data pods and PVCs are created.

### TS-092: Services and topology spread

Depends on: TS-091

Implement headless service, gateway service, topology spread, PDB.

Acceptance:

- pods schedule and discover peers through headless service.

### TS-093: Drain and rolling upgrade

Depends on: TS-092

Implement drain annotation and rolling update behavior.

Acceptance:

- one-pod rolling restart completes without permanent under-replication.

## K. Hardening

### TS-100: mTLS and auth

Depends on: TS-031, TS-060

Implement transport security and roles.

Acceptance:

- unauthenticated internal call rejected;
- peer replication requires valid cert.

### TS-101: Observability dashboards

Depends on: TS-032, TS-042, TS-061

Implement metrics and dashboards.

Acceptance:

- required metrics exist;
- alerts are generated.

### TS-102: Chaos test suite

Depends on: TS-063, TS-093

Implement chaos scenarios.

Acceptance:

- nightly convergence tests pass.

