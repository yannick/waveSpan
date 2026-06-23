# ADR 0008: dragonboat as the multi-Raft engine

## Status

Accepted.

## Context

The replicated-collections consensus tier (design/30, ADR 0007) is **range-based multi-Raft**: one
Raft group per range, many groups per node (design/30 §2, §5.7). We need a Go Raft implementation for
it, under two **decided** constraints (design/30 §12):

- the Raft log is **unified in `wavesdb`** (one engine for log + state);
- the build stays **CGO-free** (README rule #1).

The candidates are dragonboat, etcd/raft, and hashicorp/raft (design/30 §12.1):

- **hashicorp/raft** is single-group oriented (no heartbeat coalescing / quiescing across thousands of
  groups) — not viable for range-based multi-Raft.
- **etcd/raft** is a single-group *kernel*: it ships **no** multi-group manager. Multi-Raft is achieved
  by running one `RawNode` per range and **building the manager ourselves** (tick/heartbeat
  coalescing, transport multiplexing, quiescing, snapshot orchestration) — the CockroachDB model. It
  fits our constraints natively (log in `wavesdb`, our transport, CGO-free) but the multi-group
  orchestration is substantial, correctness-critical code we would own.
- **dragonboat** ships the multi-Raft manager (`NodeHost` runs thousands of groups with coalescing,
  quiescing, snapshot streaming) and an on-disk state-machine model that fits `wavesdb`. To honor our
  constraints it needs custom adapters (transport, discovery, and a `wavesdb`-backed LogDB).

## Decision

Use **dragonboat** as the multi-Raft engine, behind a `raftshard` interface (design/30 §12.5,
Appendix B), to ship the consensus tier sooner by reusing its manager rather than building one on a
single-group kernel. We accept the adapter work:

- an on-disk state machine wrapping the collection state machine → applied state in `wavesdb`;
- a transport over the existing cheap-mTLS HTTP/2 client (no second transport);
- a node registry resolving addresses from SWIM gossip, with dragonboat's own gossip **disabled** (one
  membership plane);
- a `wavesdb`-backed `ILogDB` to unify the log — **phased**: the initial milestones run on dragonboat's
  bundled **Pebble** LogDB (pure-Go, CGO-free, no storage adapter), and the `wavesdb` `ILogDB` lands as
  a tracked follow-up. dragonboat **v4 is CGO-free by default** (Pebble; RocksDB was removed in v4).

dragonboat is reached only through `raftshard`, so the engine stays swappable.

## Consequences

Positive:

- the multi-Raft manager (NodeHost, quiescing, coalesced heartbeats, snapshot streaming) is provided —
  the fastest path to a working tier;
- the on-disk state-machine interface fits `wavesdb`; non-voting replicas (observers) match the
  learner / demand-fill model (design/30 §9);
- CGO-free by default (Pebble; RocksDB was removed in dragonboat v4).

Negative:

- **single-maintainer governance/longevity risk** for a foundational dependency — mitigated by the
  `raftshard` boundary keeping etcd/raft as a swappable fallback; revisitable later;
- we still write and maintain custom transport, discovery, and (Phase 2) LogDB adapters that must meet
  dragonboat's interface contracts;
- until the `wavesdb` LogDB lands, the Raft log is in Pebble and applied state is in `wavesdb` — **two
  embedded engines transiently**, contrary to the unified-storage target;
- we adopt dragonboat's `NodeHost` concurrency/execution model.

## Alternatives considered

- **etcd/raft + a hand-built multi-Raft manager** (CockroachDB-style). Best native fit for unified
  storage + CGO-free + single control plane, but defers a working tier behind building the
  orchestration ourselves. **Rejected for now to ship faster; retained as the swappable fallback** if
  dragonboat's governance becomes a concern.
- **hashicorp/raft.** Single-group oriented; not built for thousands of groups. Rejected.

## Relationship to ADR 0007

ADR 0007 establishes the intra-cluster consensus tier; ADR 0008 selects its engine. The `raftshard`
interface (design/30 §12.4–12.5) keeps the choice swappable, so this ADR can be revisited without
disturbing the tier's design.
