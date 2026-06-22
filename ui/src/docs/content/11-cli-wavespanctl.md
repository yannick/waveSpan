---
title: CLI — wavespanctl
section: Operations
order: 11
summary: The admin/client CLI for reads, writes, scans, and inspecting membership, plus the other binaries in the project.
---

# CLI — `wavespanctl`

`wavespanctl` (`cmd/wavespanctl`) is the command-line client for interacting with a cluster — quick reads and writes, range scans, and membership inspection.

## KV commands

```bash
# write a record
wavespanctl kv put default user/42 '{"name":"Ada"}'

# read it back (prints the value and the ResponseMeta)
wavespanctl kv get default user/42

# delete (writes a tombstone)
wavespanctl kv delete default user/42

# range scan
wavespanctl kv scan default --start user/ --end user/~
```

Every read prints the `ResponseMeta` — the read source, completeness, and conflict state — so you can see *how* the value was served, not just its bytes.

## Cluster commands

```bash
# list members and their liveness
wavespanctl members

# print version
wavespanctl version
```

## Future commands

```bash
wavespanctl backup    # initiate a backup (planned)
wavespanctl restore   # restore from a backup (planned)
```

## Other binaries

The project ships several binaries (`cmd/`):

| Binary | Purpose |
|--------|---------|
| `wavespan-node` | The data pod process. Embeds wavesdb, gossip, KV/graph/vector, and this UI. |
| `wavespan-gateway` | Optional stateless router — auth and Cypher planning in front of the data pods. |
| `wavespanctl` | The admin/client CLI (this page). |
| `wavespan-bench` | Throughput / latency benchmarks (KV puts, vector search). |
| `wavespan-profile` | Cross-node pprof profiling driver. |

## Getting a local cluster

The fastest path on a laptop:

```bash
make build          # static binaries, CGO_ENABLED=0
make docker-up      # 3-node cluster via docker compose
# point wavespanctl / the UI at a node's admin port (7900)
make docker-kill    # tear it down
```

The console UI you're reading is served from each node's admin port at `/ui` — open it against any node to inspect that node's view of the cluster.
