# Running clusters

Two ways to run a local multi-node cluster on one host. Both build the **same** OCI image
(`CGO_ENABLED=0`, `FROM scratch`, multi-arch) — only the orchestration differs.

## Docker Compose (portable / CI)

```bash
make docker-up     # builds the image once, starts node1/node2/node3
make docker-kill   # tear down (removes volumes)
```

`docker/docker-compose.yaml` defines three nodes that discover each other by service name via the
static seed list. Ports are mapped to the host:

| Node | admin | data |
|---|---|---|
| node1 | `:7901` | `:7811` |
| node2 | `:7902` | `:7812` |
| node3 | `:7903` | `:7813` |

```bash
curl -fsS localhost:7901/admin/membership
wavespanctl --addr localhost:7811 kv put default k v
wavespanctl --addr localhost:7813 kv get default k    # node3 fetches + caches
```

> The build context is the **parent** directory so the sibling `wavesdb` module is in scope.

## Apple `container` (native, Apple Silicon)

Apple's `container` runs each node in its own lightweight VM ("container machine"), booting in
well under a second.

```bash
container system start      # one-time
./container/build.sh        # build the image
./container/up.sh 3         # start a 3-node cluster (N is configurable: up.sh 6)
./container/down.sh 3       # tear down
```

`up.sh` gives each node a private data volume, spreads `WAVESPAN_ZONE`/`REGION` across nodes so the
latency graph and placement filters have something to score, and points every node at the same
static seed list.

## Watching the cluster

```bash
curl -fsS localhost:7901/admin/membership   # roster + liveness (ALIVE/SUSPECT/UNREACHABLE/DEAD)
curl -fsS localhost:7901/admin/latency      # directed latency-graph edges (EWMA/p95)
curl -fsS localhost:7901/metrics            # Prometheus metrics
```

Useful metrics:

| Metric | Meaning |
|---|---|
| `kv_under_replicated_keys_estimate` | keys below the target durable-holder count (spot-churn signal) |
| `kv_repair_queue_depth` | pending repair items |
| `kv_ttl_tombstones_written_total` | tombstones emitted by the TTL sweeper |
| `gossip_*` (via `/admin/membership`) | liveness states |

## Fault injection

These are the scenarios the integration and chaos suites exercise; you can run them by hand:

| Fault | How |
|---|---|
| kill a node | `docker kill wavespan-dev-node2-1` (or `container rm -f node2`) |
| pause / resume | `docker pause` / `docker unpause` |
| empty-volume restart | `docker rm -f` then recreate with a fresh volume (new storage UUID) |
| network partition | a docker network disconnect, or pause one side |
| latency / packet loss / clock skew / disk pressure | in-process dev toggles (scratch images have no shell/tc) |

Watch the effect: kill a holder of a key and the coordinator's repair engine re-replicates it onto
a surviving node with no manual action; `kv_under_replicated_keys_estimate` drains back to zero.

## Demonstrating the headline behaviours

```bash
# origin+1: a write needs a nearby durable replica
wavespanctl --addr localhost:7811 kv put default k v   # acked_nearby_replicas=1

# read locality: a non-holder fetches then caches
wavespanctl --addr localhost:7813 kv get default k     # source=FETCHED_CLOSEST_HOLDER
wavespanctl --addr localhost:7813 kv get default k     # source=LOCAL_DYNAMIC_CACHE

# repair: kill a replica, the value is restored on another node
docker kill wavespan-dev-node2-1
# ... watch kv_under_replicated_keys_estimate drain on a survivor's /metrics ...
```

## Integration tests

The same scenarios run automatically:

```bash
make test-integration   # builds the image, runs the docker integration suite
```

This covers cluster form-up + kill detection, origin+1 + origin-kill survival, repair convergence,
cache miss-fetch + dynamic-cache hit + update propagation, and scan completeness + lazy TTL.
