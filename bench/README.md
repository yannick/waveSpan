# WaveSpan benchmarks

`wavespan-bench` is a load + benchmark client. It bulk-loads test data and replays Cypher queries
and KV ops under concurrency, reporting throughput and p50/p95/p99 latency.

```bash
make build              # builds bin/wavespan-bench
make docker-up          # a local cluster (node1 data port :7811)
```

## Bulk-load test data

```bash
bin/wavespan-bench load --addr localhost:7811 --users 2000 --follows 6000 --kv 5000
```

Creates a social graph (`:User` nodes with `name`/`age`/`city`, `FOLLOWS` edges) and KV keys.

## Replay the Cypher query suite

```bash
bin/wavespan-bench query --addr localhost:7811 --queries bench/queries --concurrency 16 --duration 15s
```

Runs every `*.cypher` file in [`queries/`](queries) repeatedly and reports per-query throughput +
latency. The suite covers label scans, property filters, one/two-hop traversals, filtered expands,
and approximate vector search. Add your own `.cypher` files to the folder to benchmark them.

## KV load test

```bash
bin/wavespan-bench kv --addr localhost:7811 --concurrency 32 --duration 20s --read-ratio 0.5
```

Mixed put/get load with separate latency reports.

## Example output

```
# cypher benchmark: 6 queries, concurrency=16, duration=15s each
01_label_scan                ops=42103  errs=0      2806/s  p50=5.1ms   p95=9.8ms   p99=14ms
03_one_hop                   ops=61200  errs=0      4080/s  p50=3.5ms   p95=6.9ms   p99=11ms
06_vector_search             ops=18004  errs=0      1200/s  p50=12ms    p95=22ms    p99=31ms
```

## Collections (sets / hashes / sorted sets + bulk-remove)

The benchmark web UI (`wavespan-benchui`) also drives the `CollectionService`:

- **Closed-loop workloads** `set`, `hash`, `zset` — op-mixes over a pool of collections, with live
  throughput + p50/p95/p99 charts like the KV workload. `hash` exercises the atomic counter
  (`HIncrBy`) via its `counterRatio` param.
- **Bulk-remove panel** — the headline "remove one element from a *huge* number of sets":
  1. **Seed** N sets, each containing a target member (e.g. `doomed`).
  2. **Remove from all sets** — one `BulkRemove(namespace, collections=[], [member])`, which lists
     every collection in the namespace and **proposes a removal per collection, sequentially**
     (`internal/collections/bulk.go`). The headline metric is **sets/sec** (whole-fan-out
     wall-clock; there is no per-collection latency, since it is a single aggregate call).
  3. **Scaling sweep** — repeats the seed→remove over N = 1k…1M and charts sets/sec vs N (log-x),
     visualizing the **O(N) fan-out cost**. (Measured on a 3-node dev cluster: ~35–40 sets/sec, i.e.
     wall-clock grows linearly with the set count — removing from 1M sets is hours, not seconds.)

> Collection writes commit through Raft, so the benchmark sets a context deadline on every write (the
> dev/CLI `wavespan-bench` KV path needs none). Full-namespace `BulkRemove` is **destructive across the
> entire namespace** — the UI seeds into a dedicated namespace per run.

The same workloads run headless from the CLI engine; the UI is just a driver over `internal/benchengine`.

## Correctness vs. benchmarks

These are **performance** benchmarks. For **correctness** under faults (the Jepsen-style harness),
see [`../tests/harness`](../tests/harness) and [`../tests/harness/JEPSEN.md`](../tests/harness/JEPSEN.md).
