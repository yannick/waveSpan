# 13. Failure model and recovery

## Expected failures

The system is designed for:

- frequent spot-node removal;
- pod crashes;
- node crashes;
- rescheduling onto new nodes;
- empty replacement volumes;
- stale pods returning after partition;
- short and medium intra-cluster network outages;
- VPN restarts;
- cross-cluster replication delays;
- whole cluster disconnection from peer clusters.

## Not guaranteed in v1

- no data loss after both origin and the first acknowledged nearby replica are lost before target-N repair completes;
- with `storage.engine.syncMode` lowered from the default `full`: no data loss on correlated kernel-panic/power-loss of origin and the acknowledged replica within the fsync window (bounded by `syncInterval` for `interval`, unbounded for `none`) — see design/02 "Required local invariants";
- no stale reads;
- no conflict-free active-active writes;
- no globally complete scan during partition;
- no exact TTL deadline.

## Pod crash after successful write

Scenario:

```text
Pod A accepts write.
Pod A stores locally.
Pod B stores nearby replica.
Pod A returns success.
Pod A crashes before target-N fanout.
```

Expected:

- value remains on Pod B;
- holder summaries eventually advertise Pod B;
- repair notices target-N deficit;
- repair creates additional nearby replicas;
- reads may briefly miss if holder directory is stale, then find via range summaries/anti-entropy.

## Pod crash before nearby ACK

Scenario:

```text
Pod A stores locally.
Pod A crashes before any nearby replica ACK.
Client did not receive success.
```

Expected:

- write may exist locally if Pod A returns;
- client should retry with idempotency key;
- if duplicate appears, conflict resolver handles same request ID idempotently.

## Spot node disappearance

Expected:

1. gossip marks member suspect/unreachable;
2. placement stops selecting it;
3. repair estimates under-replicated keys;
4. new nearby replicas are created from surviving holders;
5. dynamic subscriptions from lost node expire;
6. scan completeness may degrade until repair catches up.

## Node returns after partition

A returning node may hold stale writes and missed deletes.

Startup/rejoin behavior:

1. gossip announces storage UUID and last seen epoch;
2. node does not assume its cache subscriptions are valid;
3. node advertises holder summaries;
4. anti-entropy compares versions;
5. conflict resolver applies incoming/outgoing deltas;
6. stale dynamic cache data is either resubscribed or evicted.

## Intra-cluster network partition

Because default consistency is eventual:

- both sides may accept writes;
- origin+1 may succeed independently on each side if enough nodes exist;
- same key may receive concurrent versions;
- partition heal triggers conflict resolution.

This is expected. Do not add hidden write rejection unless a policy says so.

## VPN restart / temporary global outage

Global replication behavior:

- local writes continue;
- outbound logs queue;
- lag metrics increase;
- after reconnect, stream resumes;
- anti-entropy repairs missing entries;
- conflicts resolve by policy.

## Durable replica loss

If durable holder count drops below `minAckNearbyReplicas + 1`, new writes still require origin+1. Existing keys are under-replicated until repair.

Repair priority:

1. keys with only one known durable holder;
2. keys with recent writes;
3. keys with active subscribers;
4. hot keys;
5. range coverage repair;
6. cold keys.

## Cache source failure

If a dynamic cache subscription source fails:

1. subscriber marks subscription `LAGGING`;
2. subscriber tries alternate holders;
3. if found, resubscribes from last version;
4. if not found, cache becomes stale-only or evicted;
5. reads using it must declare stale/best-effort metadata.

## Conflict examples

### LWW conflict

```text
Cluster A writes key=x value=1 at HLC 100
Cluster B writes key=x value=2 at HLC 101
After replication, value=2 wins everywhere.
```

### Sibling conflict

```text
Cluster A writes key=x value=1
Cluster B writes key=x value=2
Policy keep-siblings stores both.
Get returns siblings_present.
Client resolves and writes value=3.
```

### Delete conflict

```text
Cluster A deletes key=x at HLC 100
Cluster B writes key=x value=2 at HLC 101
LWW result: value=2 wins.
```

## Recovery from empty replacement volume

If a pod starts with no storage UUID:

- initialize new storage UUID;
- join as new member;
- do not reuse previous member's holder claims;
- receive replicas through repair;
- old member identity becomes dead/forgotten after retention.

## Acceptance tests

- [ ] Kill origin immediately after successful origin+1 write; value remains readable from replica.
- [ ] Kill origin before origin+1 ACK; client retry is safe with idempotency key.
- [ ] Partition 6-node cluster into 3/3; both sides accept writes; conflicts converge after heal.
- [ ] Delete conflict converges deterministically.
- [ ] Dynamic cache source failure triggers resubscribe or stale downgrade.
- [ ] Empty volume replacement does not claim old storage identity.
- [ ] Repair restores target-N after spot churn.

