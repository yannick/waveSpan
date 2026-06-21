# ADR 0005: Go data node with the wavesdb engine in-process

## Status

Accepted.

## Context

The original design assumed a Rust data node and gateway wrapping the C engine `tidesdb` through an FFI binding, with the operator in Go. That split implied two languages on the hot path, a C build dependency, and an FFI boundary on every storage call.

Since then the repository already contains the pieces needed to avoid all of that:

- `wavesdb` — a mature, tested Go rewrite of `tidesdb`. It provides column families, MVCC transactions, five isolation levels (including Snapshot and Serializable), bidirectional iterators, native per-key TTL, `Checkpoint`/`Compact`/`FlushMemtable`, and an object-store replica with `PromoteToPrimary`.
- `testing-waves` — a Jepsen-style bank-test correctness harness.
- `bench` — a benchmark harness, with the merged `wavesdb-correctness-fixes` branch.

`wavesdb` can be imported as an ordinary Go library, removing the need for an FFI boundary or a C toolchain on the data path.

## Decision

Build the entire distributed layer in Go: the data node, the gateway, the CLI (`wavespanctl`), and the Kubernetes operator. Import `wavesdb` in-process as a Go library — every storage operation is a direct Go call, not FFI.

`tidesdb` is retained as reference material only and is not part of the build.

## Consequences

Positive:

- reuse a tested engine (`wavesdb`) instead of building or wrapping one;
- no FFI boundary and no C toolchain on the data path; simpler build and debugging;
- a single language across data node, gateway, CLI, and operator, so types, tooling, and skills are shared;
- reuse the existing correctness harness (`testing-waves`) and benchmark harness (`bench`) directly against the same engine.

Negative:

- accept Go's garbage-collected runtime. `wavesdb` benchmarks at roughly 3x BadgerDB and about 10% behind C `tidesdb` on Put; GC pause behavior must be watched on scan-heavy hot paths and addressed with allocation discipline (buffer reuse, bounded scan batches) rather than a language change;
- the engine is Go-specific; the `LocalStore` wrapper is a coupling-reduction boundary, not a portability promise, and v1 has exactly one engine;
- the vector ANN index (the one component that might otherwise reach for a C library such as hnswlib/faiss) is also pure Go. `CGO_ENABLED=0` is a project-wide hard rule (`24_container_dev_and_testing.md`): all binaries are statically linked and shipped `FROM scratch`, so cgo is prohibited, not merely discouraged. If pure-Go HNSW underperforms, the escape hatch is an out-of-process ANN service behind the `ANNIndex` interface (decided at M10), never an in-process cgo binding.
