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

## Correctness vs. benchmarks

These are **performance** benchmarks. For **correctness** under faults (the Jepsen-style harness),
see [`../tests/harness`](../tests/harness) and [`../tests/harness/JEPSEN.md`](../tests/harness/JEPSEN.md).
