---
title: Membership, Gossip & Latency
section: Reference
order: 7
summary: SWIM membership, the piggybacked latency graph that drives placement, holder summaries, and how the system survives spot-node churn.
---

# Membership, Gossip & Latency

WaveSpan has no central membership authority. Pods discover and monitor each other with a SWIM-style gossip protocol (`internal/membership`), and the same gossip traffic carries the metadata that drives replica placement.

## Member identity

A member is identified by three things:

- **cluster ID** — which cluster it belongs to.
- **member ID** — its logical name (often the StatefulSet ordinal).
- **storage UUID** — a *persistent* identity tied to its volume.

The storage UUID matters: a pod rescheduled onto a new node with a fresh volume is a **new member**, not the old one. This is how the system distinguishes "same pod, moved" from "replacement that needs backfill."

## Discovery

| Runtime | Mechanism |
|---------|-----------|
| Kubernetes | StatefulSet ordinals, headless-service DNS, pod labels, the node API. |
| Docker / local | A static `seeds` list in config. |

## SWIM gossip

Each gossip tick:

1. Selects a random peer (plus a latency-interest peer).
2. Sends a **ping**; expects an **ack**.
3. On no ack, asks others to probe **indirectly**, then marks the peer **suspect**.
4. Piggybacks metadata: membership deltas, latency edges, holder summaries.

Liveness flows through five states:

```text
ALIVE ──▶ SUSPECT ──▶ UNREACHABLE ──▶ DEAD ──▶ FORGOTTEN
```

You can watch this traffic live in the [Gossip Inspector](doc:overview) tab — pings, acks, suspicions, and the piggybacked edges and summaries all stream there.

## The latency graph

Every ack measures round-trip time. Those samples build a directed, time-decayed **latency graph** (`internal/latencygraph`) with, per edge:

- EWMA RTT and p95 RTT
- packet loss
- connection health
- topology metadata (zone / region / geo)

### Placement scoring

When the system needs to place a replica, it scores candidate nodes:

| Signal | Weight |
|--------|-------:|
| EWMA RTT | 0.55 |
| p95 RTT | 0.15 |
| packet loss | 0.10 |
| load pressure | 0.10 |
| disk pressure | 0.05 |
| topology penalty | 0.05 |

Hard filters run first: a candidate must be alive, on a **distinct Kubernetes node**, satisfy the geo boundary, accept writes, and have disk/cache budget. "Nearby" is therefore *measured*, not assumed from topology labels.

## Holder summaries & directory

Each member gossips a compact **holder summary** — a Bloom filter plus a watermark — describing what ranges/keys it holds per namespace. Aggregated, these form the **holder directory** used for read routing and repair-source selection. A summary is considered **stale** after roughly 3× the gossip interval, at which point it is no longer trusted for routing.

## Surviving spot nodes

The whole design assumes pods vanish without warning:

- Writes always land on **two distinct nodes** before ack.
- A **drain protocol** lets a gracefully-terminating pod hand off before it leaves.
- Repair rebuilds durable replicas for any pod that disappears.
- Backfill brings replacements up to date for `all`/`global` namespaces.
