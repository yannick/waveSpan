# ADR 0001: Eventual consistency by default

## Status

Accepted.

## Context

The product target favors low latency, local writes, dynamic caches, active-active geo replication, and resilience under spot-node churn.

Linearizable writes would require stable quorum ownership and often wider coordination. Global linearizability would add WAN latency and conflict with the active-active requirement.

## Decision

Use eventual consistency as the default for KV, graph, vector, and global replication.

Every read and scan must expose freshness and completeness metadata.

## Consequences

Positive:

- low write latency;
- works during partitions;
- active-active is possible;
- simpler local write path;
- good fit for spot-heavy clusters.

Negative:

- stale reads are possible;
- conflicting writes are possible;
- range scans may be incomplete;
- clients must understand response metadata;
- conflict policies are mandatory.

