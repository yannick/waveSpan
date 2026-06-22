---
title: Operations & Observability
section: Operations
order: 12
summary: Health and readiness probes, the metrics surface, the live streaming feeds powering this console, and the Kubernetes operator.
---

# Operations & Observability

WaveSpan is built to be *seen*. Operators need visibility into latency, cache behaviour, repair lag, and spot-node impact — so every subsystem exports metrics, and the node embeds this live console.

## Health & readiness

| Endpoint | Meaning |
|----------|---------|
| `GET /healthz` | Liveness — the process is running. |
| `GET /readyz` | Readiness — storage open, gossip joined, repair healthy. |
| `GET /metrics` | Prometheus exposition. |

`/readyz` gating matters during rollouts: a pod that has joined gossip but hasn't finished backfill should not yet take traffic for `all`/`global` namespaces.

## Metrics

Prometheus metrics (`internal/observability`) cover every subsystem:

- **KV** — put/get/delete/scan latencies and counts.
- **Replication** — replica-store failures, fanout depth, repair lag, under-replication estimate.
- **Cache** — hit/miss ratio, eviction rate, subscription count.
- **Gossip** — round-trip times, suspect/confirm counts, latency-graph edge count.
- **Global** — cross-cluster replication lag, conflict rate.
- **Graph / Vector** — query latencies, ANN index freshness.

The [Metrics](doc:overview) tab surfaces the headline counters; `/metrics` has the full set for Grafana.

## The live console

This console (design doc 26) is a Vite + React SPA embedded into the node binary via `go:embed` and served from the admin port. It talks to the node over **ConnectRPC server-streaming** — not a separate WebSocket layer — so live feeds (gossip, data) share the same transport as ordinary RPCs.

| Tab | What it shows |
|-----|---------------|
| Cypher Console | Run graph/vector queries, see streamed rows + completeness. |
| Node Explorer | Force-directed view of the property graph. |
| Gossip Inspector | Live SWIM traffic with kind filters. |
| Data Browser | Inspect KV records at node / cluster / global scope. |
| KV Writer | Write a test record through a chosen coordinator. |
| Cluster Topology | Members, liveness, and the latency graph. |
| Metrics | Headline cluster counters. |

## The Kubernetes operator

The operator (`operator/`, a separate Kubebuilder module) manages clusters declaratively:

- **StatefulSets** with topology spread and PodDisruptionBudgets.
- **CRDs** for the cluster definition, replication policies, cluster peers, and backup jobs.
- A **drain protocol** so a terminating pod hands off before leaving.
- **Rolling upgrades** that respect readiness gates.
- TLS secret provisioning for mTLS.

## Troubleshooting starting points

| Symptom | Look at |
|---------|---------|
| Membership not forming | Gossip Inspector; seeds/DNS; `WAVESPAN_SEEDS`. |
| High read latency | Cluster Topology latency edges; cache hit ratio. |
| Under-replication climbing | repair-lag metrics; dead members in topology. |
| Stale reads | `ResponseMeta.source` on the read; subscription/cache metrics. |
| Peer cluster diverging | global replication lag; `all` vs `global` misconfiguration. |
