# ADR 0004: Global active-active replication requires conflict policy

## Status

Accepted.

## Context

Multiple clusters can accept writes to the same key or graph/vector entity. Without cross-cluster consensus, concurrent writes can conflict.

## Decision

Global replication is asynchronous active-active. Every replicated namespace/index/graph must define a conflict policy.

Required v1 policies:

- HLC last-write-wins;
- keep-siblings.

Optional later policies:

- CRDT counters/sets;
- application resolver;
- WASM resolver.

## Consequences

Positive:

- clusters continue accepting writes during partitions;
- global write latency stays local;
- deterministic convergence is possible.

Negative:

- LWW may lose concurrent updates;
- keep-siblings requires client/app resolution;
- conflict observability is mandatory;
- global scans are eventually consistent.

