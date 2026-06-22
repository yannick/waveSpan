---
title: Configuration
section: Operations
order: 10
summary: The YAML config file, the WAVESPAN_* environment overrides, ports, and per-namespace settings.
---

# Configuration

A node is configured by a YAML file plus optional `WAVESPAN_*` environment overrides (`internal/config`). Two ready-made files ship in `config/`: `dev.yaml` (a 3-node, origin+1 cluster) and `dev-single.yaml` (a single, local-only node).

## The config file

```yaml
clusterId: dev                      # required
memberId: node1                     # required
nodeName: ""                        # physical node — used by the distinct-node filter
advertiseHost: ""                   # advertised address (default: memberId)

topology:
  zone: ""
  region: ""
  geo: ""

storage:
  path: /var/lib/wavespan           # wavesdb data directory
  engine: wavesdb                   # only engine in v1

membership:
  runtime: docker                   # docker | kubernetes
  seeds: [node1:7700, node2:7700]   # required for docker runtime

ports:
  gossip: 7700
  data: 7800

admin:
  listen: ":7900"                   # health, metrics, /ui, Cypher

replication:
  policyRef: local-cache-default
  targetNearbyReplicas: 3           # background target (origin + N)
  minAckNearbyReplicas: 1           # write-ack threshold
  globalDurabilityRequired: false

security:
  insecureDevMode: true             # dev only — mTLS in production
  certFile: ""
  keyFile: ""
  caFile: ""

namespaces:
  - name: ref
    replicationFactor: "all"        # see Replication Factor
  - name: hot
    replicationFactor: "5"
  - name: events                    # "" => default latency-based
```

## Environment overrides

Every important field has a `WAVESPAN_*` override, useful for container deployments:

| Variable | Field |
|----------|-------|
| `WAVESPAN_CLUSTER_ID` | `clusterId` |
| `WAVESPAN_MEMBER_ID` | `memberId` |
| `WAVESPAN_NODE_NAME` | `nodeName` |
| `WAVESPAN_ADVERTISE_HOST` | `advertiseHost` |
| `WAVESPAN_ZONE` / `_REGION` / `_GEO` | `topology.*` |
| `WAVESPAN_RUNTIME` | `membership.runtime` |
| `WAVESPAN_SEEDS` | `membership.seeds` (comma-separated) |
| `WAVESPAN_STORAGE_PATH` | `storage.path` |
| `WAVESPAN_ADMIN_LISTEN` | `admin.listen` |
| `WAVESPAN_TARGET_NEARBY_REPLICAS` | `replication.targetNearbyReplicas` |
| `WAVESPAN_MIN_ACK_NEARBY_REPLICAS` | `replication.minAckNearbyReplicas` |
| `WAVESPAN_INSECURE_DEV_MODE` | `security.insecureDevMode` |
| `WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES` | sets matching namespaces to `"all"` |
| `WAVESPAN_REPLICATE_GLOBAL_NAMESPACES` | sets matching namespaces to `"global"` |

## Ports

| Port | Purpose |
|------|---------|
| 7700 | Gossip (SWIM + metadata) |
| 7800 | Data (KV + internal RPC) |
| 7900 | Admin — `/healthz`, `/readyz`, `/metrics`, `/ui`, Cypher |

## TTL is lazy

`ttl_ms` on a `Put` sets a *best-effort* expiry. Two mechanisms cooperate:

1. **Native wavesdb TTL** — expired records are dropped during compaction.
2. **TTL buckets** — a coarse time-bucketed index; a sweeper writes tombstones so deletion propagates to replicas.

A record may be physically gone on one node but still served by a lagging replica until the tombstone arrives. Do not rely on TTL for correctness-critical expiry.
