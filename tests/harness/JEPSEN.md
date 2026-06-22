# Jepsen → WaveSpan harness mapping

This is the analysis of [Jepsen](https://github.com/jepsen-io/jepsen) (and
[Elle](https://github.com/jepsen-io/elle)) and how its tests are ported into WaveSpan's pure-Go
correctness harness (`tests/harness/`). The harness keeps Jepsen's proven shape — **generate →
apply → record history → inject faults → check → dump → minimal repro** — but every assertion is
made against WaveSpan's **declared eventual-consistency model** (design/00, design/13, design/25),
not linearizability.

## Jepsen architecture (what we cloned)

| Jepsen namespace | Role | WaveSpan Go equivalent |
|---|---|---|
| `jepsen.client` | per-node client invoking ops | `tests/harness/client` (WaveSpan KV/Cypher client, records ack into history) |
| `jepsen.db` | install/start/stop the DB on nodes | the existing docker images + `tests/harness/runner/cluster.go` |
| `jepsen.generator` | lazy op stream (mix, stagger, nemesis interleave) | `tests/harness/workloads/*` Go generators driven by a seeded RNG |
| `jepsen.nemesis` | fault injection (partition/kill/pause/clock) | `tests/harness/nemesis/*` over docker (`os/exec`, no CGO) |
| `jepsen.checker` | history → validity | `tests/harness/checker/*` (model-aware; the 5 doc-16 properties) |
| `jepsen.store` / `checker.timeline` | history persistence + forensics | `runner/history.go` (+ `Dump`, `Shrink`, `EmitRepro`) |
| `elle.list-append` / `elle.rw-register` | cycle detection for serializability | **out of scope** — WaveSpan is eventual; no serializability checker (design/00) |

## Workloads (Jepsen `jepsen.tests.*`) → WaveSpan

| Jepsen test | What Jepsen asserts (typically linearizable) | WaveSpan adaptation (eventual) |
|---|---|---|
| **bank** (`tests/bank.clj`) | every read's balances sum to the constant total; no negative/nil/unexpected | transfers via keep-siblings merge or CAS-retry; the sum is asserted **post-heal** on every live replica/cluster (mid-partition divergence allowed) — `checker.convergence` over a conserved bank invariant |
| **set / g-set** (`checker/set-full`) | every acked added element is eventually present (`:lost` = added-then-absent) | grow-only set over keep-siblings; every acked add present post-heal — `checker.convergence` + `no-lost-update-per-policy` |
| **linearizable-register** (`tests/linearizable_register.clj`) | reads/writes are linearizable (knossos) | NOT linearizable; instead `lww-determinism` (HLC-LWW winner) or keep-siblings, asserted post-heal |
| **counter** (`checker/counter`) | each read ∈ [Σ ok-incs, Σ attempted-incs] | a counter workload with the same bound checker (`checker.counter`) — increments are CAS/keep-siblings merges |
| **long-fork / monotonic / causal** | snapshot isolation / causal | session monotonicity only (`session-monotonicity`); cross-key cycles NOT flagged (design/00) |

**Why post-heal, acked-only:** Jepsen usually tests strong models, so it can assert on *every*
read. WaveSpan is eventually consistent (design/13), so convergence/no-loss are asserted only after
all faults heal and writes stop, and only on ops that returned success. Stale reads and
partition-time divergence are legal, never violations.

## Nemeses (Jepsen `jepsen.nemesis`) → WaveSpan docker faults

| Jepsen nemesis | Mechanism | WaveSpan port (`tests/harness/nemesis`) |
|---|---|---|
| `partition-halves` / `partition-random-node` | iptables grudge (`net/`) | `docker network disconnect`/`connect` cutting the compose network into halves |
| `node-start-stopper` (kill) | `c/exec` start/stop | `docker kill` + `docker start` (or compose stop/start) |
| `hammer-time` (pause) | `SIGSTOP`/`SIGCONT` | `docker pause` / `docker unpause` |
| `clock-scrambler` / `bump-time` | `bump-time.c` via faketime | bounded record-only on the scratch image (no `tc`/faketime in `FROM scratch`); skew rejection is unit-tested at `internal/version` |
| `kill-origin-after-ack` | (custom) | the durability hook: `docker kill` the origin within a bounded window after a write ACK |

The scratch images intentionally have no shell/`tc`/`iptables` inside the container, so faults are
injected from the **host** (`docker` CLI on the compose network), which is exactly Jepsen's
out-of-band control plane.

## What is deliberately NOT ported

- **Knossos / Elle serializability + linearizability checkers** — WaveSpan does not promise either
  (design/00). Porting them would assert a model WaveSpan does not implement; the negative-control
  discipline (`checker.TestNegativeControlCaught`) covers "the harness is not vacuously green"
  instead.
- **`jepsen.os` / `jepsen.control` SSH** — replaced by docker/Apple-container (design/24); no SSH.
- **gnuplot latency plots** — replaced by the Prometheus metrics + Grafana dashboards (design/14).

## Running

```bash
# unit (cluster-free): history/seed/checkers/shrinker/nemesis orchestration
go test ./tests/harness/...

# live PR gate (docker): workloads × nemeses × checkers, post-heal
docker compose -f docker/docker-compose.yaml up -d
go test -tags harness -run PRGate ./tests/harness
```
