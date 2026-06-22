# WaveSpan documentation

WaveSpan is a Kubernetes-native, eventually-consistent distributed key-value store built on the
`wavesdb` Go LSM storage engine. It favours low-latency local access, durability beyond a single
pod, read-created cache replicas, graceful degradation under spot-node churn, and honest
consistency metadata on every response.

This is the **user and operator documentation**. For the full design specification (the source of
truth for the build), see [`../design/`](../design/); for the implementation roadmap and per-
milestone plans, see [`../IMPLEMENTATION_STRATEGY.md`](../IMPLEMENTATION_STRATEGY.md) and
[`../plans/`](../plans/).

## Start here

- **[Getting started on a Mac](getting-started-mac.md)** — build, run a single node, then a local
  3-node cluster, and read/write with `wavespanctl`. Start here for development.

## Guides

| Doc | What it covers |
|---|---|
| [Architecture](architecture.md) | Components, the data path, the keyspace, and how a write/read flows |
| [KV API](kv-api.md) | Put / Get / Delete / Scan, consistency metadata, TTL, idempotency |
| [Consistency & replication](consistency-and-replication.md) | The eventual-consistency contract, origin+1, repair, and the dynamic cache |
| [Running clusters](running-clusters.md) | Docker Compose and Apple `container`, ports, fault injection |
| [Configuration](configuration.md) | The config file and the `WAVESPAN_*` environment reference |
| [Development](development.md) | Build system, code layout, testing, and the integration suite |

## Current capabilities

WaveSpan today (milestones M0–M6) provides:

- ordered byte key-value storage: point `Put` / `Get` / `Delete`, range `Scan`, lazy TTL;
- **origin+1 writes** — a write is acknowledged only once the origin and at least one nearby
  durable replica have stored it;
- **target-N repair** — a background engine converges every key to the target replica count and
  restores replicas when holders die under spot churn;
- **dynamic cache replicas** — a read miss fetches from the closest holder, caches the value, and
  subscribes to live updates; cache replicas are derived and disposable;
- **SWIM gossip membership** with a measured latency graph driving replica placement;
- **honest reads and scans** — every response declares its read source and completeness.

Not yet built (milestones M7–M14): global active-active replication, the property-graph + Cypher
layer, vector search, the Kubernetes operator, and production hardening (mTLS, auth, backup).
