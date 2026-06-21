# ADR 0003: Dynamic cache replicas are derived state

## Status

Accepted.

## Context

Read-created dynamic replicas improve locality. They subscribe to updates and can be stored on disk, but they are not part of the write acknowledgement rule.

Counting them as durable replicas would make correctness depend on cache eviction and subscription health.

## Decision

Dynamic cache replicas are derived state and do not count toward write durability unless explicitly promoted by the repair/promotion subsystem.

## Consequences

Positive:

- cache eviction is safe;
- subscription failure does not violate durability claims;
- implementation is simpler;
- read locality improves without complex ownership transfer.

Negative:

- durable replica count may be lower than total physical copies;
- repair must promote hot cache copies if they should become durable;
- operators must understand cache copies are not durability copies.

