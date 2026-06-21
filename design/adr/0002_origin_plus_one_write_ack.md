# ADR 0002: Origin plus one nearby durable replica for write acknowledgement

## Status

Accepted.

## Context

The requirement says a write needs at least one replication before acknowledgement.

The system also needs low latency and should avoid cross-geo writes unless policy allows.

## Decision

A successful default write requires:

```text
origin pod local durable write
AND
one nearby durable replica on a different node
```

The target nearby replica count `N` is filled asynchronously after acknowledgement.

## Consequences

Positive:

- acknowledged writes survive loss of one pod/node if the other durable holder remains;
- lower latency than waiting for all `N` replicas;
- simple implementation target;
- works with eventual consistency.

Negative:

- data can still be lost if origin and first replica are both lost before repair/fanout;
- under-replication windows exist;
- repair is critical;
- target-N is not an acknowledgement guarantee.

