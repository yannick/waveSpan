# WaveSpan implementation design

WaveSpan is a Kubernetes-native, eventually consistent distributed database built on top of `wavesdb` as the local storage engine.

WaveSpan and all of its processes are written in Go. The local engine `wavesdb` is a Go library that WaveSpan imports in-process — there is no FFI boundary on the data path. The C engine `tidesdb` (the ancestor of `wavesdb`) is reference-only and is not part of the build.

It provides:

- ordered key-value storage with point reads, writes, deletes, range scans, watches, and lazy TTL;
- a special latency-aware local replication and dynamic cache-subscription mode;
- optional active-active global replication across Kubernetes clusters;
- a property-graph and vector database exposed through a production subset of Cypher rather than SQL;
- approximate and exact vector search;
- Kubernetes operator deployment for production and Docker deployment for local testing.

This document set is written for an implementation agent. Treat it as the source of truth for building the first production-grade prototype.

## External facts assumed by this design

The design assumes `wavesdb` is the embedded local storage layer. `wavesdb` is a Go LSM-tree key-value storage engine — a mature, tested Go rewrite of the C engine `tidesdb`. WaveSpan links it directly as a Go library, so all storage calls are in-process Go function calls. `tidesdb` is retained as reference material only.

Kubernetes production deployment uses StatefulSets because StatefulSet pods have stable identity and storage. Pod topology spread constraints are used to spread pods across nodes, zones, regions, and custom topology domains. The database must still maintain its own membership and replication state; Kubernetes only provides scheduling, process lifecycle, identity inputs, and persistent volumes.

Cypher support targets a production subset of openCypher. openCypher is the open specification of Cypher for property graph databases.

## File map

Read in this order:

1. `00_assumptions_and_product_contract.md`
2. `01_architecture.md`
3. `02_storage_wavesdb.md`
4. `03_kv_store.md`
5. `04_membership_latency_gossip.md`
6. `05_special_cache_replication.md`
7. `06_global_active_active_replication.md`
8. `07_graph_cypher.md`
9. `08_vector_engine.md`
10. `09_kubernetes_operator.md`
11. `10_docker_dev.md`
12. `11_api_contracts.md`
13. `12_crds.md`
14. `13_failure_model.md`
15. `14_observability.md`
16. `15_security.md`
17. `16_testing_strategy.md`
18. `17_source_tree.md`
19. `18_implementation_roadmap.md`
20. `19_agent_work_items.md`
21. `20_risks_and_non_goals.md`
22. `21_current_implementation_state.md`
23. `22_versioning_and_hlc.md`
24. `23_repair_engine.md`
25. `24_container_dev_and_testing.md`
26. `25_correctness_harness.md`
27. `26_node_ui_and_observability.md`
28. `27_transport_performance.md`
29. `28_replication_factor_and_node_sync.md`
30. `adr/0001_eventual_consistency.md`
31. `adr/0002_origin_plus_one_write_ack.md`
32. `adr/0003_cache_replicas_are_derived.md`
33. `adr/0004_global_active_active_conflicts.md`
34. `adr/0005_go_and_wavesdb_engine.md`

## Implementation stance

WaveSpan is a single-language Go system. The data node, gateway, CLI, and Kubernetes operator are all Go. The local engine `wavesdb` is imported in-process as a Go library; storage operations are ordinary Go calls, not FFI. See `adr/0005_go_and_wavesdb_engine.md` for the rationale behind the Go pivot and `21_current_implementation_state.md` for what already exists versus what is greenfield.

Default behavior is eventual consistency. Do not add a global consensus layer on the hot path unless a later requirement explicitly changes the contract.

A successful local write means:

```text
origin pod durably stored the mutation
AND
at least one nearby durable replica durably stored the mutation
```

The configured target replica count `N` is a background convergence goal, not the minimum acknowledgement count. The minimum acknowledgement count is fixed at `origin + 1 nearby durable replica` for the default profile.

Dynamic cache replicas are read-created, subscription-updated, disposable replicas. They improve latency. They are not counted as write durability unless explicitly promoted.

Range scans may read from cache and are eventually consistent. They must expose result completeness metadata.

TTL is lazy. Expired data may remain physically stored and may be returned until the local node observes expiration or receives a tombstone, depending on configured read policy.

Global replication is active-active and asynchronous by default, with configurable conflict handling.

## Recommended initial defaults

```yaml
consistency:
  default: eventual
  readYourWritesSession: optional

localReplication:
  targetNearbyReplicaCount: 3
  minAckNearbyReplicas: 1
  requireDistinctNodes: true
  geoPolicy: prefer-local-geo
  complianceBoundary: none

cache:
  dynamicSubscriptions: true
  cacheReadMode: eventual
  rangeScanMayUseCache: true
  emitCompletenessMetadata: true

ttl:
  mode: lazy
  readTimePrecision: best-effort
  physicalGcPrecision: best-effort

globalReplication:
  mode: active-active-async
  defaultConflictPolicy: hlc-last-write-wins

graph:
  language: cypher-production-subset

vector:
  exactSearch: true
  approximateSearch: true
  rawVectorStorage: wavesdb
```

## Hard implementation rules

1. `wavesdb` is the Go local storage engine and is local storage only. Distributed correctness, gossip, routing, replication, cache coherence, conflict resolution, and global replication must be implemented above it in Go. It is linked in-process; never reach for an FFI/C path on the data plane.
2. Kubernetes is not the membership database. The runtime membership layer must work in Docker without Kubernetes.
3. A cache miss must not broadcast to all nodes. Resolve holders through the range directory and compact holder summaries.
4. Dynamic caches must be bounded by memory, disk, subscriber count, update lag, and idle time.
5. Every user-visible read response must declare the consistency/completeness mode actually used.
6. Vector indexes are derived state. Raw vectors and graph entities are authoritative.
7. Active-active conflict handling is mandatory before global writes are enabled.
8. Spot-node churn is expected. The system must constantly repair under-replicated keys and ranges.

