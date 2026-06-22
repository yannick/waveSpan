---
title: Replication Factor — all vs global
section: Reference
order: 6
summary: Per-namespace replication — the default latency-based placement, a numeric N, all (every node in the cluster), and global (every node in every cluster), plus the backfill path for joining nodes.
---

# Replication Factor — `all` vs `global`

Replication is configured **per namespace**. This is the most recently designed subsystem (design doc 28) and the axis most teams need to reason about.

## The four modes

A namespace's `replicationFactor` is a string with four forms:

| Value | Meaning | Crosses cluster boundary? |
|-------|---------|---------------------------|
| `""` (empty) | **Default.** Latency-based nearby placement, converging to target-N. | No |
| `"N"` (e.g. `"5"`) | **Numeric override.** Hold the key on exactly N nearby nodes. | No |
| `"all"` | **Everywhere-local.** Every node in *this* cluster holds the key. | No |
| `"global"` | **Everywhere-global.** Every node in *every* cluster holds the key. | Yes |

```yaml
namespaces:
  - name: events                       # "" => default latency-based
  - name: hot
    replicationFactor: "5"             # numeric override
  - name: ref
    replicationFactor: "all"           # every node of this cluster
  - name: config
    replicationFactor: "global"        # every node, every cluster
```

## `all` vs `global` — the key distinction

The two "everywhere" modes differ only in their **scope boundary**:

- **`all`** means *every node in the current cluster*. It never ships data across a cluster boundary. Use it for cluster-local reference data that every pod should serve with zero hops — small lookup tables, feature flags scoped to one region.
- **`global`** means *every node in every cluster*. The mutation is shipped across peer clusters via the global replication log. Use it for truly universal data — cross-region config, shared dictionaries.

> Picking `all` when you meant `global` is a common mistake: the data will be everywhere *in one cluster* but absent from peers. The [Data Browser](doc:overview) lets you check a key's holders at `node`, `cluster`, and `global` scope to confirm.

## Write path for everywhere namespaces

The origin+1 ACK rule is **unchanged** — a write still acks after origin + 1 durable replica. What changes is the *fanout target*:

- For `""` / `"N"`: fan out to the latency-selected nearby candidates.
- For `"all"` / `"global"`: fan out to **all alive members** (of the cluster, or of every cluster for `global`).

This keeps write latency low (the ack never waits for the full fan-out) while still converging to full coverage.

## Backfill — bringing a new node up to date

When a node joins (or a spot node is replaced with an empty volume), it must acquire every `all`/`global` record. It does this with the **Backfill** RPC:

```protobuf
service ReplicationService {
  rpc Backfill(BackfillRequest) returns (stream BackfillResponse);
}
```

1. The joining node picks a healthy peer from the latency graph.
2. It pages through the peer's records for each `all`/`global` namespace.
3. Records are applied **idempotently** with last-write-wins by HLC, so a backfill that overlaps live writes converges correctly.

The bootstrapper resumes on reconnect, so a backfill interrupted by churn does not restart from zero.

## Correctness properties

- **Idempotent apply.** Backfill and fanout both apply by mutation ID + HLC, so duplicates and reorderings are safe.
- **Convergence.** Given no new writes, every node in scope eventually holds every record (anti-entropy guarantees this).
- **No ack regression.** Switching a namespace to `all`/`global` does not slow the write ack — only the background fan-out widens.
