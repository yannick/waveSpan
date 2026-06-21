# 22. Versioning and hybrid logical clocks

## Goal

Every mutation in WaveSpan carries a `Version`. The version must give a deterministic,
cross-cluster total order for conflict resolution (doc 03, doc 06) and a stable, idempotent
mutation identity for replication (doc 06). This document specifies the hybrid logical clock
(HLC) and the `Version` envelope concretely.

## Version envelope

```protobuf
message Version {
  uint64 hlc_physical_ms = 1;   // 48-bit physical wall-clock milliseconds
  uint32 hlc_logical = 2;       // 16-bit logical counter
  string writer_cluster_id = 3;
  string writer_member_id = 4;
  uint64 writer_sequence = 5;   // per-member monotonic counter
}
```

The HLC timestamp is the pair `(hlc_physical_ms, hlc_logical)`:

- `hlc_physical_ms` is a 48-bit physical timestamp in milliseconds since the Unix epoch.
  48 bits of milliseconds covers years well beyond any practical retention horizon;
- `hlc_logical` is a 16-bit logical counter that disambiguates events sharing the same
  physical millisecond. It allows up to 65 536 distinct HLC events per physical
  millisecond per clock before the physical component must advance.

The two pack into a single 64-bit HLC stamp (`hlc_physical_ms << 16 | hlc_logical`) when a
compact wire/storage form is convenient; the protobuf keeps them as separate fields for
clarity.

## HLC algorithm (Lamport HLC)

Each clock keeps the last stamp it issued: `(lastPhys, lastLogical)`. `wallClockMs()` reads
the local physical clock in milliseconds.

### Local / send event

Issued when this member originates a mutation (or sends a message stamped with an HLC):

```text
phys = max(lastPhys, wallClockMs())
if phys == lastPhys:
    logical = lastLogical + 1
else:
    logical = 0
lastPhys, lastLogical = phys, logical
emit (phys, logical)
```

Advancing the physical component resets the logical counter to 0; staying within the same
physical millisecond increments it.

### Receive event

Applied when this member receives a remote mutation carrying `(msgPhys, msgLogical)`:

```text
phys = max(lastPhys, msgPhys, wallClockMs())

if phys == lastPhys and phys == msgPhys:
    logical = max(lastLogical, msgLogical) + 1
elif phys == lastPhys:
    logical = lastLogical + 1
elif phys == msgPhys:
    logical = msgLogical + 1
else:
    logical = 0

lastPhys, lastLogical = phys, logical
emit (phys, logical)
```

This is the standard Lamport HLC merge: the new physical component is the max of the local
last, the message, and the wall clock; the logical component is reconciled against whichever
source(s) the new physical component came from.

### Clock-skew protection

A remote HLC whose physical component runs too far ahead of local wall-clock time would drag
this member's clock forward and let a misconfigured peer monopolize ordering. Reject or clamp
such stamps:

```text
maxClockSkewMs: 500   # default

if msgPhys > wallClockMs() + maxClockSkewMs:
    emit clock-skew metric
    reject the mutation (or clamp msgPhys to wallClockMs() + maxClockSkewMs per policy)
```

Default policy is to reject the apply and surface it; a namespace may opt into clamping
instead. Either way emit:

```text
hlc_clock_skew_rejections_total   // remote HLC beyond maxClockSkewMs
hlc_observed_skew_ms              // observed (msgPhys - wallClockMs) distribution
```

## Writer sequence and mutation identity

`writer_sequence` is a per-member monotonic counter, incremented once per originated
mutation. It is **persisted in column family `sys`** and reloaded on startup, so it never
regresses across restarts (a restarted member resumes from the highest issued value, not
from zero). This is what makes mutation identity stable across crashes.

The replication mutation identity (doc 06) is:

```text
mutation_id = writer_cluster_id + writer_member_id + writer_sequence
```

Because `writer_sequence` is monotonic and persisted, `mutation_id` is stable and idempotent:
re-sending the same originated mutation (after a retry, reconnect, or anti-entropy pass)
produces the same `mutation_id`, and a receiver that has already applied it ignores the
duplicate.

`writer_sequence` is **not** the ordering key — it identifies and deduplicates. Ordering is
by HLC (below). Two mutations from different members with unrelated `writer_sequence` values
are ordered by their HLC stamps, not by sequence.

## Version compare and tie-break order

Conflict resolution (doc 03 `hlc-last-write-wins`) orders versions by, in order:

```text
1. hlc_physical_ms      (higher wins)
2. hlc_logical          (higher wins)
3. writer_cluster_id    (lexicographic)
4. writer_member_id     (lexicographic)
5. writer_sequence      (higher wins)
```

The HLC pair decides causality and concurrency; the writer fields are a deterministic
tie-break so that two genuinely concurrent writes (identical HLC) still get one stable
winner on every member. This matches the ordering already stated in doc 03 and is the
single source of truth for it.

### Go compare sketch

```go
// Compare returns -1 if a < b, 0 if equal, +1 if a > b under the
// hlc-last-write-wins total order. Higher means "wins".
func (a Version) Compare(b Version) int {
    if a.HLCPhysicalMs != b.HLCPhysicalMs {
        return cmpUint64(a.HLCPhysicalMs, b.HLCPhysicalMs)
    }
    if a.HLCLogical != b.HLCLogical {
        return cmpUint32(a.HLCLogical, b.HLCLogical)
    }
    if c := strings.Compare(a.WriterClusterID, b.WriterClusterID); c != 0 {
        return c
    }
    if c := strings.Compare(a.WriterMemberID, b.WriterMemberID); c != 0 {
        return c
    }
    return cmpUint64(a.WriterSequence, b.WriterSequence)
}

// Equal reports whether two versions are the same mutation identity-wise.
func (a Version) Equal(b Version) bool {
    return a.HLCPhysicalMs == b.HLCPhysicalMs &&
        a.HLCLogical == b.HLCLogical &&
        a.WriterClusterID == b.WriterClusterID &&
        a.WriterMemberID == b.WriterMemberID &&
        a.WriterSequence == b.WriterSequence
}
```

`cmpUint64`/`cmpUint32` return -1/0/+1 in the obvious way.

## Idempotency notes

- A retried apply of the same `mutation_id` is a no-op: the receiver checks applied
  mutation IDs before merging (doc 06) and the persisted `writer_sequence` guarantees the
  origin reproduces the same id.
- `Compare` is a total order: it never returns 0 for two distinct mutation identities,
  because `writer_sequence` (per member) plus `writer_member_id`/`writer_cluster_id`
  uniquely identifies an originated mutation. Two `Version`s compare equal only when they
  are the same mutation.
- Conflict resolution is therefore deterministic and convergent: every member, given the
  same set of versions, selects the same winner regardless of arrival order.

## Unit-test expectations

The HLC and version code must have tests asserting:

- **Deterministic ordering.** For a fixed set of versions, `Compare` yields the same total
  order on every run and on every member; the LWW winner is independent of the order in
  which versions are presented.
- **Tie-break correctness.** Versions with identical HLC pairs are ordered strictly by
  `writer_cluster_id`, then `writer_member_id`, then `writer_sequence`; no two distinct
  identities compare equal.
- **HLC monotonicity.** Successive local/send events never produce a non-increasing
  `(phys, logical)`; within one physical millisecond `logical` strictly increases.
- **Receive merge.** Each branch of the receive algorithm
  (`phys==lastPhys==msgPhys`, `phys==lastPhys`, `phys==msgPhys`, else) produces the
  specified logical value, and the resulting stamp dominates both inputs.
- **Skew rejection.** A remote stamp with `msgPhys > wallClock + maxClockSkewMs` is
  rejected (or clamped under clamp policy) and increments the skew metric; the local clock
  is not dragged past the skew bound.
- **Idempotent retry.** Re-applying the same `mutation_id` is a no-op; restarting a member
  and originating a new mutation produces a `writer_sequence` strictly greater than any
  previously issued (no regression after restart).
```