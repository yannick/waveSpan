---
title: Overview
section: Introduction
order: 1
summary: WaveSpan is a Kubernetes-native, eventually-consistent distributed database that trades linearizability for low-latency local access, honest consistency metadata, and graceful survival of spot-node churn.
---

# WaveSpan

**WaveSpan** is a Kubernetes-native, eventually-consistent distributed database built on the `wavesdb` Go LSM storage engine. It is designed for systems that need durable, geo-distributed storage but can tolerate stale reads in exchange for low latency and resilience.

It deliberately sits in the middle of the classic distributed-storage tradeoff:

- **Not** a linearizable, consensus-coordinated store (no Raft on the read/write path).
- **Not** a pure local cache with silent read-after-write surprises.

Instead it accepts eventual consistency *explicitly* and tells you about it on every response.

## What makes it different

- **No hidden consistency.** Every response carries a `ResponseMeta` declaring where the read came from, whether the result is complete, and whether a conflict was observed. You always know what you got.
- **Origin+1 durability.** A write is acknowledged only once it is durable on the origin node *plus* at least one nearby replica — durability beyond a single pod, without waiting for a quorum.
- **Dynamic cache replicas.** A read miss fetches from the closest holder, caches the value locally, and subscribes to updates. Cache replicas are derived and disposable.
- **Spot-node friendly.** The system assumes any pod can vanish at any moment. Writes always land on at least two distinct Kubernetes nodes, and a repair engine continuously heals under-replication.
- **Per-namespace replication.** Each namespace chooses its own replication factor: latency-based default, a numeric N, `all` (every node in the cluster), or `global` (every node in every cluster).
- **Two query surfaces, no SQL.** A gRPC key-value API for point operations, and a production subset of Cypher for graph and vector queries.

## Who it's for

WaveSpan fits applications that are:

- Geo-distributed and latency-sensitive.
- Running on volatile infrastructure (spot/preemptible nodes).
- Able to tolerate eventual consistency, while still needing durability guarantees and *honest* metadata about staleness.

> If you need serializable transactions or globally-consistent scans, WaveSpan is the wrong tool — and it says so up front. See [Risks & non-goals](doc:faq).

## The shape of the system

A WaveSpan deployment is a cluster of identical **data pods**, each embedding the `wavesdb` engine, a SWIM gossip layer, and the KV / graph / vector services. Pods measure latency to one another continuously and use that graph to decide where replicas live. Optional **peer clusters** replicate asynchronously in an active-active topology across regions.

A Kubernetes **operator** manages the StatefulSets, CRDs, and rolling upgrades. Each node also embeds this **console UI** (what you're reading now) for inspection.

Continue to [Architecture](doc:architecture) for the full picture, or jump to the [KV API](doc:kv-api) to start writing data.
