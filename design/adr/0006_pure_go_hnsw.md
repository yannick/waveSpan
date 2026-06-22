# ADR 0006 — Pure-Go HNSW for ANN (cgo prohibited)

## Status

Accepted (M10).

## Context

WaveSpan needs approximate nearest-neighbor (ANN) vector search. The mature ANN libraries
(hnswlib, FAISS, ScaNN) are C/C++ and would be reached through cgo bindings.

`CGO_ENABLED=0` is a project-wide **hard rule** (`design/24_container_dev_and_testing.md`): every
binary is statically linked and shipped from a `FROM scratch`, multi-arch image. A cgo-backed ANN
would break:

- static linking (the scratch images have no libc / shared objects),
- multi-arch cross-compilation (`linux/arm64` + `linux/amd64` from one toolchain),
- whole-program race-detector coverage of the node,
- the single-binary build and the Apple `container` dev path.

So a cgo binding is **not an available option**, not merely discouraged.

## Decision

**Implement HNSW in pure Go.** The implementation sits behind a narrow `ANNIndex` interface
(`internal/vector/ann`), so the algorithm is the only thing that knows it is HNSW; the rest of the
vector engine (delta, segments, rerank, procedures) depends only on the interface.

The HNSW reuses the M9 exact-distance kernels (cosine/dot/L2, with SIMD dispatch) for its distance
computations, so there is one distance implementation across exact and approximate paths.

## Consequences

- We own the ANN code: hierarchical layers, `efConstruction`/`efSearch`/`M` parameters, greedy
  candidate search, and neighbor selection. More code to maintain, but no FFI boundary.
- Recall/latency is validated by a benchmark (`tests/bench/vector_ann_bench_test.go`) against the
  exact oracle. The target is recall@10 ≥ 0.95 at a reasonable `efSearch`.
- **Escape hatch:** if pure-Go HNSW cannot meet the recall/latency target, the fallback is an
  **out-of-process ANN service** behind the same `ANNIndex` interface — its own container, reached
  over Connect/gRPC. Never an in-process cgo binding. Any such escalation is recorded here.
- Globally, only **raw vectors** replicate (ADR 0004 / design/06); HNSW internals are derived and
  never cross the wire. Each cluster rebuilds/updates its own index from applied raw records.
