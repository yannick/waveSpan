# Performance profiling (`wavespan-profile`)

`wavespan-profile` captures Go runtime profiles (CPU, heap, block, mutex, goroutine) from **every
node** while a benchmark workload runs, then prints and writes a cross-node breakdown of where CPU,
allocations, and — most importantly — **latency (off-CPU blocking)** go.

## Why all four profiles

| Profile | Answers |
|---|---|
| **CPU** | what burns CPU (throughput ceiling) |
| **Block** | where request goroutines *wait* off-CPU — fsync, network, channels. **This is where latency hides; a CPU profile never shows it.** |
| **Mutex** | which contended lock serializes the hot path |
| **Heap (alloc)** | allocation volume → GC pressure → tail latency |

The block/mutex sections are **request-path focused**: idle background loops (repair, anti-entropy,
evictor tickers) sleep on timers and otherwise dominate a block profile while doing no work, so only
samples that pass through a request handler are counted. Tables lead with **application / storage /
RPC frames** (Go runtime + net/http plumbing is reported only as an overhead %), and each section
calls out the **hottest leaf** (highest *flat* value) — the function where the cost actually happens,
not just the callers above it.

## 1. Start a profiling-enabled cluster

Profiling is off in production. The profiling compose turns it on
(`WAVESPAN_PROFILING_ENABLED`, plus block/mutex sampling rates):

```bash
# build the node binary (worktree-friendly: baked into a small image, no parent build context)
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/wavespan-node.linux ./cmd/wavespan-node
docker build -f docker/Dockerfile.profile -t wavespan-node:profile .
docker compose -f docker/docker-compose.profile.yaml up -d     # admin/pprof 7921-7923, data 7831-7833
```

To enable profiling on any node manually, set:

```
WAVESPAN_PROFILING_ENABLED=true        # serves /debug/pprof on the admin port
WAVESPAN_BLOCK_PROFILE_RATE=10000      # sample ~1 blocking event per 10µs of delay (0 = off)
WAVESPAN_MUTEX_PROFILE_FRACTION=100    # sample 1-in-100 contended mutex events (0 = off)
```

## 2. Profile a workload

```bash
# preload data, then profile a 20s KV run while capturing all three nodes
go run ./cmd/wavespan-bench load --addr localhost:7831 --kv 3000 --users 0 --follows 0
go run ./cmd/wavespan-profile \
  --nodes node1=localhost:7921,node2=localhost:7922,node3=localhost:7923 \
  --data localhost:7831 --workload kv --seconds 20 --concurrency 32 --out perf-report
```

`--workload query` replays the Cypher suite instead. The report is printed to the console and written
to `perf-report/breakdown.md`; the raw profiles are saved as `perf-report/<node>.<kind>.pb.gz` so you
can drill in with the standard tool:

```bash
go tool pprof -http=: perf-report/node1.alloc.pb.gz
```

## Reading the output

Read **Latency (block)** and **Lock contention** first for a latency hunt; **CPU** and
**Allocations** for throughput ceilings and GC tail latency. The "Hottest leaf" callout is usually
the real fix target.
