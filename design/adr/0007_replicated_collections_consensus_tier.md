# ADR 0007: Intra-cluster consensus tier for replicated collections

## Status

Accepted.

## Context

WaveSpan carries two workloads with opposite needs. The **cache tier** (design/03) is a fast,
eventually-consistent local node cache — best-effort is correct for it. The **complex datatypes**
(sets, hash tables, sorted sets) are application data **written from a central point and read across
the fleet**, and they need properties the AP substrate cannot give:

- concurrent additions from different writers must not be lost — HLC last-write-wins clobbers them
  (design/22, ADR 0001);
- atomic multi-element updates and exact aggregates (`SCARD`/`HLEN`) — there is no cross-key
  atomicity, and a single counter key is lossy under LWW;
- complete, local enumeration — a key-per-element layout enumerates via `routed-eventual` scans, which is
  `PARTIAL` and scatters across nodes (design/03 scan modes).

Two facts make a consensus tier viable here without contradicting the existing design:

- the fleet is **stable core + spot edge**, and the stable nodes are annotated in Kubernetes, so a
  voter quorum can be pinned to nodes that do not churn;
- **synchronous cross-geo writes are not required**, so consensus can stay **intra-cluster /
  in-region**.

ADR 0001 rejected *global* linearizability because it adds WAN latency and conflicts with
active-active. That reasoning is about the **global** layer; it does not speak to an **in-region**
consensus tier, which incurs only a local majority round-trip.

## Decision

Introduce an **opt-in, intra-cluster consensus tier** for replicated collections, alongside the
unchanged AP default. Details in design/30.

- Distribution is selected **per namespace**: `distribution: cache` (today's AP path, unchanged) or
  `distribution: replicated` (this tier). A cache request never enters the consensus path.
- The replicated tier is **range-based multi-Raft**: the ordered keyspace
  `r/<ns>/lenPrefix(collectionID)||<element>` is split into ranges by size/load, and **each range is
  one Raft group**. A collection is a key prefix that may span one or many groups.
- **Voters run only on voter-eligible (stable-core) nodes**; **learners** (non-voting read replicas)
  run anywhere, including spot edge, and are added **on demand** where reads land.
- Writes are linearizable through the range leader; **atomic multi-element batches are supported
  within a single range** (cross-range atomicity is deferred to v2 — design/30 §8 / §13.9, §19).
- Reads default to **bounded-stale local** reads off any replica, with **linearizable reads
  opt-in** (leader read-index). Enumeration is `COMPLETE` when the node holds all spanned ranges,
  else `PARTIAL` — honesty metadata is mandatory, as in ADR 0001.
- **Cross-cluster propagation stays asynchronous/eventual** via the global layer (design/06). This
  tier does not provide synchronous cross-geo writes.
- The client surface is a typed **`CollectionService`** (sets, hashes, sorted sets) plus parity
  **Cypher procedures**, carrying a per-request consistency flag, idempotency keys, cursor-based
  enumeration, single-range atomic batches with preconditions (CAS), a read-your-writes watermark, and
  a `Watch` change stream. A RESP/Redis-wire adapter is explicitly **optional and non-canonical**
  (design/30 §13.1). The node console gains a **Collections** inspector.

## Consequences

Positive:

- linearizable writes, atomic single-range updates, and exact aggregates for shared datatypes;
- local, bounded-stale reads everywhere; read throughput scales with on-demand replicas;
- the cache tier's hot path is untouched — no consensus tax on simple put/get;
- in-region scope avoids WAN-Raft latency; the global eventual contract is preserved;
- quorums sit on the stable core, so spot-edge churn loses only learners (no availability impact).

Negative:

- a consensus control plane must be built and operated (meta group, range directory, placement
  driver, split/merge, learner lifecycle, failover);
- a data group that loses its voter majority is **write-unavailable** for that range until recovered
  (bounded-stale reads still serve); CP replaces graceful AP degradation for this tier;
- the range leader is a per-shard **write hot-spot** and a failover point — reads scale, writes do
  not;
- wildly-varying collection sizes force dynamic **range split/merge** (a collection cannot be a single
  group);
- a new **node-role** concept (`voterEligible`) and graceful core drain are required;
- every replica/learner pays the full footprint of the ranges it holds (eviction needed);
- WaveSpan now has **two consistency models**; operators and clients must understand which a namespace
  uses.
- a **new public API surface** (`CollectionService` + Cypher procedures + client library) must be
  designed, versioned, and maintained alongside the existing KV/graph/vector APIs.

## Alternatives considered

- **No consensus — soft, hash-derived leaseholder** (design/30 predecessor): single-writer under
  stable membership with HLC-LWW as a churn backstop. Rejected for this tier: a split-brain window
  during churn, lossy concurrent writes, and no path to exact aggregates or atomic ops.
- **Consensus for coordination only — a lease/lock service.** A small Raft group hands out fencing
  tokens; data stays on the AP path. Lighter to build, but yields single-writer fencing **without**
  linearizable data reads or atomic state — short of the requirement. Retained as a fallback if the
  full tier proves too costly.
- **Full CP pivot of the entire KV** (CockroachDB-shaped). Rejected: it would tax the cache hot path
  and break spot-churn tolerance, for no benefit to the caching workload.
- **One Raft group per collection.** Rejected: collection cardinality/size vary widely — millions of
  tiny groups cause a heartbeat storm, and a huge collection overflows a single group. Range-based
  grouping handles both.

## Relationship to ADR 0001

This is a **scoped addition**, not a reversal. ADR 0001 remains the default for the cache, graph,
vector, and global-replication layers. ADR 0007 adds an in-region CP tier for namespaces that opt in,
and leaves the cross-cluster contract eventual.
