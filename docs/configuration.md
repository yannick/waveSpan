# Configuration

A node is configured by a YAML file passed with `--config`, with `WAVESPAN_`-prefixed environment
variables overriding individual fields. Config is validated eagerly: an invalid config fails fast
with an actionable message.

```bash
wavespan-node --config config/dev.yaml
WAVESPAN_MEMBER_ID=node2 wavespan-node --config config/dev.yaml   # env override
```

## Config file

```yaml
clusterId: dev              # required
memberId: node1             # required; also the DNS-resolvable advertise host in docker
nodeName: ""                # physical node (for the distinct-node placement filter)
advertiseHost: ""           # override the advertised host (default: memberId)
topology:                   # static topology hints (the latency graph is authoritative)
  zone: ""
  region: ""
  geo: ""
storage:
  path: /var/lib/wavespan   # wavesdb data dir (must be writable)
  engine: wavesdb           # the only engine in v1; linked in at build time
membership:
  runtime: docker           # docker | kubernetes
  seeds:                    # static seed list (required in docker runtime)
    - "node1:7700"
    - "node2:7700"
    - "node3:7700"
ports:
  gossip: 7700
  data: 7800
admin:
  listen: ":7900"
replication:
  policyRef: local-cache-default
  targetNearbyReplicas: 3   # background convergence goal (origin + N)
  minAckNearbyReplicas: 1   # write-ACK threshold; 1 = origin+1; 0 = local-only (dev)
security:
  insecureDevMode: true     # dev only; mTLS lands in a later milestone
```

The shipped configs:

- **`config/dev.yaml`** — origin+1 (`minAck` defaults to 1). Use for clusters.
- **`config/dev-single.yaml`** — `minAck: 0`, `target: 0` for a lone development node.

### Replica counts and the single-node case

`targetNearbyReplicas` and `minAckNearbyReplicas` are optional. If unset they default to **3** and
**1** (origin+1). They are stored as pointers so an explicit `0` is honoured (and not reset to the
default) — that is how single-node development works:

```yaml
replication:
  minAckNearbyReplicas: 0   # the lone node's origin copy is durable; no peer needed
```

A 3-node cluster cannot reach a target of 4 holders; the effective target is capped at the live
cluster size, so repair converges rather than churning.

## Environment overrides

| Variable | Field |
|---|---|
| `WAVESPAN_CLUSTER_ID` | `clusterId` |
| `WAVESPAN_MEMBER_ID` | `memberId` |
| `WAVESPAN_NODE_NAME` | `nodeName` |
| `WAVESPAN_ADVERTISE_HOST` | `advertiseHost` |
| `WAVESPAN_ZONE` / `WAVESPAN_REGION` / `WAVESPAN_GEO` | `topology.*` |
| `WAVESPAN_RUNTIME` | `membership.runtime` |
| `WAVESPAN_SEEDS` | `membership.seeds` (comma-separated) |
| `WAVESPAN_STORAGE_PATH` | `storage.path` |
| `WAVESPAN_ADMIN_LISTEN` | `admin.listen` |
| `WAVESPAN_TARGET_NEARBY_REPLICAS` | `replication.targetNearbyReplicas` |
| `WAVESPAN_MIN_ACK_NEARBY_REPLICAS` | `replication.minAckNearbyReplicas` |
| `WAVESPAN_INSECURE_DEV_MODE` | `security.insecureDevMode` |

The Docker Compose and Apple `container` scripts set these per node — see
[Running clusters](running-clusters.md).

## Validation rules (fail-fast)

- `clusterId` and `memberId` are required.
- `membership.runtime` must be `docker` or `kubernetes`.
- in `docker` runtime, `membership.seeds` must be non-empty.
- `storage.engine` must be `wavesdb`.

## Ports

| Port | Purpose | Notes |
|---|---|---|
| 7700 | gossip | SWIM membership + holder summaries |
| 7800 | data | `KvService` + internal `ReplicationService` (Connect) |
| 7900 | admin | `/healthz`, `/readyz`, `/metrics`, `/admin/membership`, `/admin/latency` |
