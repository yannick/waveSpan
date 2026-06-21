# 20. Risks and non-goals

## Major risks

### 1. Dynamic subscriptions can become write amplification

A hot key with 10,000 dynamic subscribers turns one update into 10,000 push attempts.

Mitigations:

- subscriber caps per key;
- promote hot keys to durable replicas instead of unlimited subscriptions;
- batching;
- fanout trees;
- backpressure;
- stale downgrade.

### 2. Eventual scans can be misunderstood

Range scans from cache may be incomplete.

Mitigations:

- response completeness metadata;
- separate `cache-fast`, `cache-complete`, and `routed-eventual` modes;
- documentation;
- client warnings.

### 3. LWW can lose data

HLC last-write-wins is deterministic but can discard concurrent updates.

Mitigations:

- keep-siblings policy;
- CRDT policies for known types;
- namespace-level policy selection;
- conflict metrics.

### 4. Spot churn can outpace repair

If nodes disappear faster than repair completes, durability degrades.

Mitigations:

- prioritize keys with only one durable holder;
- increase target-N;
- use larger persistent volumes and stable node pools for critical data;
- expose under-replication alerts.

### 5. Vector indexes can lag raw data

ANN indexes are derived and updated asynchronously.

Mitigations:

- delta index;
- exact fallback;
- index lag metadata;
- background rebuild.

### 6. Graph queries can explode

Unbounded traversals can overload the cluster.

Mitigations:

- traversal depth limits;
- row limits;
- memory limits;
- timeout;
- remote fragment limits.

### 7. Holder directory staleness

Closest-holder resolution may point to a dead/stale node.

Mitigations:

- alternate holder lists;
- retry with backoff;
- range summary fallback;
- anti-entropy;
- read repair.

## Non-goals for v1

- linearizable global transactions;
- serializable Cypher transactions;
- full openCypher compatibility;
- SQL support;
- object storage offload;
- global quorum writes;
- automatic semantic conflict resolution for arbitrary values;
- exact TTL deadline guarantees;
- cache-complete scans without range coverage certificates;
- multi-tenant hard isolation.

## Things not to fake

Do not claim:

- a cache scan is complete unless it has a certificate;
- active-active LWW preserves all concurrent writes;
- dynamic cache replicas count toward write durability;
- Kubernetes labels alone define closeness;
- vector ANN results are exact;
- TTL deletion is precise;
- global replication has no lag.

