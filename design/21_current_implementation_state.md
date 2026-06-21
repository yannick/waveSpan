# 21. Current implementation state

This document maps the design to what actually exists in the repository today, so an implementation agent can tell apart "reuse this" from "build this from scratch." It is a snapshot; update it as components land.

## What exists today

These live as sibling checkouts under `storage-engines/`:

| Component | Path | What it is | Status |
|---|---|---|---|
| `wavesdb` | `../wavesdb` | The Go LSM storage engine WaveSpan links in-process. ~43 Go files. Provides column families, MVCC transactions, five isolation levels (incl. Snapshot/Serializable), bidirectional iterators, native per-key TTL, `Checkpoint`/`Compact`/`FlushMemtable`, and an object-store replica with `PromoteToPrimary`. | Done (engine), reused as-is |
| `tidesdb` | `../tidesdb` | The C ancestor of `wavesdb`. Reference only — not vendored, not built, not on any data path. | Reference |
| `testing-waves` | `../testing-waves` | Jepsen-style bank-test correctness harness. Reusable for distributed correctness testing against the same engine. | Done (harness) |
| `bench` | `../bench` | Benchmark harness used for the `wavesdb` throughput/latency numbers. | Done (harness) |
| `wavesdb-correctness-fixes` | `../wavesdb-correctness-fixes` | Merged bugfix branch for `wavesdb`. Its fixes are in the engine. | Merged |

Everything in the **distributed layer** — membership, replication, cache, global replication, graph, Cypher, vector, and the operator — is **greenfield**. None of it exists yet; the design docs are the specification for building it.

## Design component → status → target milestone

Milestones M0–M12 are defined in `18_implementation_roadmap.md`.

| Design component | Doc | Status | Target milestone |
|---|---|---|---|
| Local storage engine (`wavesdb`) | `02_storage_wavesdb.md` | Done (reuse) | M0 |
| `LocalStore` adapter over `wavesdb` | `02_storage_wavesdb.md`, `17_source_tree.md` | Greenfield | M0 |
| C engine `tidesdb` | — | Reference (not built) | n/a |
| Correctness harness (`testing-waves`) | `16_testing_strategy.md` | Done (reuse) | M0 |
| Benchmark harness (`bench`) | `16_testing_strategy.md` | Done (reuse) | M0 |
| Config + node bootstrap | `17_source_tree.md` | Greenfield | M0 |
| Versioning / HLC / vector clocks | `22_versioning_and_hlc.md` | Greenfield | M1 |
| KV API, key encoding, range scans | `03_kv_store.md` | Greenfield | M1–M2 |
| Membership and gossip | `04_membership_latency_gossip.md` | Greenfield | M2 |
| Latency graph | `04_membership_latency_gossip.md` | Greenfield | M2 |
| Placement / candidate scoring | `01_architecture.md` | Greenfield | M3 |
| Local replication (origin+1, target-N) | — / `adr/0002` | Greenfield | M3 |
| Repair engine (under-replication, spot churn) | `23_repair_engine.md` | Greenfield | M3–M4 |
| TTL buckets + sweeper | `02_storage_wavesdb.md` | Greenfield | M4 |
| Dynamic cache subscriptions | `05_special_cache_replication.md` | Greenfield | M4–M5 |
| Conflict resolution policies | `adr/0004` | Greenfield | M5 |
| Global active-active replication | `06_global_active_active_replication.md` | Greenfield | M6 |
| Backup/restore via `Checkpoint` + object-store replica | `02_storage_wavesdb.md`, `09_kubernetes_operator.md` | Greenfield (engine primitives exist) | M6 |
| Graph storage and indexes | `07_graph_cypher.md` | Greenfield | M7 |
| Cypher parser | `07_graph_cypher.md` | Greenfield | M8 |
| Cypher planner / distributed fragments | `07_graph_cypher.md` | Greenfield | M9 |
| Vector raw store + exact search | `08_vector_engine.md` | Greenfield | M9 |
| Vector ANN index (pure-Go HNSW; no cgo) | `08_vector_engine.md`, `adr/0005` | Greenfield | M10 |
| Kubernetes operator | `09_kubernetes_operator.md`, `12_crds.md` | Greenfield | M10–M11 |
| Gateway / router | `01_architecture.md`, `11_api_contracts.md` | Greenfield | M11 |
| Observability (metrics/tracing) | `14_observability.md` | Greenfield (hooks throughout) | M0–M12 |
| Security (mTLS/auth) | `15_security.md` | Greenfield | M11–M12 |

## How to read this against the rest of the docs

- "Done (reuse)" means the design depends on an existing, tested artifact. Do not reimplement it; wrap or call it. `wavesdb` is the prime example — its API is the contract in `02_storage_wavesdb.md`.
- "Reference" means it informs design but is not in the build. `tidesdb` is reference only.
- "Greenfield" means there is no code yet and the named doc is the spec. Target milestones are guidance from `18_implementation_roadmap.md`, not commitments.
