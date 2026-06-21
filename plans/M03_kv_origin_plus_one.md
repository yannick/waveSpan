# M03 - KV origin+1 writes: public KV, StoreReplica, placement, ACK rule

**Milestone:** M3 (`design/18_implementation_roadmap.md` "Milestone 3")
**Tickets:** TS-030, TS-031, TS-032 (and TS-023 placement, folded in here)
**Depends on:** M1 (storage + envelopes), M2 (membership + latency graph)
**Enables:** M4 (target-N + repair), M5/M6 (cache/scan track), M7 (global), M11 (operator
reconcile target)

## Context

M3 is the first end-to-end write path. Any data pod can accept a write and becomes the
**write coordinator** for that mutation (`design/01_architecture.md` "Consistency design";
`design/05_special_cache_replication.md` "Write coordinator"). The coordinator writes locally,
selects nearby candidates from the latency graph (placement, TS-023), replicates via the
internal `StoreReplica` API, and **acknowledges only after origin + one nearby durable
replica** stored the mutation (`design/README.md` "Implementation stance"; ADR
`design/adr/0002_origin_plus_one_write_ack.md`). The target replica count `N` is a background
goal delivered in M4, not part of the M3 ACK.

This milestone enables the **origin+1 invariant** CI gate
(`IMPLEMENTATION_STRATEGY.md` section 3) and the first bank-invariant run.

## Files to create

```
proto/wavespan/v1/kv.proto            Put/Get/Delete RPCs, PutResult/GetResult/DeleteResult, ScanHeader (stub)
proto/wavespan/v1/replication.proto   StoreReplicaRequest/Response, ReplicaClass
internal/kv/service.go                public KV gRPC service: Get/Put/Delete
internal/kv/encode.go                 key encoder (lexicographic-preserving) for /kv/{ns}/data and latest
internal/kv/coordinator.go            write coordinator: version assign, local persist, fanout-min-ack, ACK
internal/kv/read.go                   local-first Get against latest pointer + versioned record
internal/placement/placement.go       candidate filter + scoring over the latency graph (TS-023)
internal/placement/placement_test.go  distinct-node, compliance, prefer-local-geo spillover, scoring
internal/replication/local/store_replica.go   StoreReplica server (receiver) + client
internal/replication/local/idempotency.go     mutation-id dedupe cache
internal/kv/service_test.go           local-only put/get/delete; metadata present
internal/replication/local/replica_test.go     remote replica stores durable + returns version
tests/integration/kv_origin_plus_one_test.go   3-node ACK rule + origin-kill survival
```

## Steps

1. **Protos.** `kv.proto`: `Put(namespace,key,value,ttl?,idempotencyKey?)`,
   `Get(namespace,key,options)`, `Delete(namespace,key)` with `PutResult` (version,
   `acked_nearby_replicas`, `ResponseMeta`), `GetResult` (value, version, `ResponseMeta`),
   `DeleteResult`, all carrying `ResponseMeta` from M0 (`design/03_kv_store.md` "Response
   metadata"). Include a `ScanHeader` placeholder used in M6. `replication.proto`:
   `StoreReplicaRequest` (namespace, key, `StoredRecord`, `replica_class=NEARBY_DURABLE`,
   coordinator_member_id, mutation_id) and `StoreReplicaResponse` (durable, member_id,
   applied_version, conflict_state) exactly per `design/05` "StoreReplica protocol".

2. **Key encoder, `internal/kv/encode.go`.** Encode user keys into the internal keyspace
   (`design/01_architecture.md`): `/kv/{ns}/data/{user_key}/{version}` and the latest pointer
   `/kv_meta/latest/{ns}/{user_key}`. The encoding **must preserve lexicographic ordering** of
   user keys (implementation checklist, `design/03_kv_store.md`) so scans in M6 are correct.

3. **Local Put/Get/Delete (TS-030), `internal/kv/service.go` + `read.go` + the local half of
   `coordinator.go`.** Implement the single-node path first:
   - Assign a version with the M0 HLC + writer sequence (`design/03` "Put path" 1-2).
   - In one `storage.Batch` (atomic, M1): write the versioned `StoredRecord`, update the
     `LatestPointer`, and append the `MutationEnvelope` to `repl_log`
     (`design/05` write algorithm; `design/02` "Required local invariants" 2).
   - `Delete` is `Put(tombstone=true)` (`design/03` "Delete path").
   - `Get` reads the latest pointer, returns value + `ResponseMeta` (`source=LOCAL`,
     observed version, conflict state). TS-030 acceptance: one-node put/get/delete works and
     response metadata is present.

4. **Placement (TS-023), `internal/placement/placement.go`.** Implement candidate selection
   over the M2 latency graph (`design/05` "Candidate selection"; `design/04` "Latency graph"
   hard filters + score):
   - Hard filters first: `peer != self`, alive (or recently-alive if policy allows),
     `node_name != self.node_name` when `requireDistinctNodes`, compliance boundary,
     accepting-writes, disk/queue budget.
   - Then score by `latency_score + topology_penalty + load_penalty + repair_balance_penalty`.
   - `prefer-local-geo`: same geo first; spill to nearest allowed geo only if no candidate can
     satisfy `minAckNearbyReplicas` and `allowSpilloverForDurability=true`; tag the response
     `geoSpillover=true`. `require-local-geo`: only same compliance boundary; fail otherwise.

5. **StoreReplica (TS-031), `internal/replication/local/store_replica.go`.** Server side
   (`design/05` "StoreReplica protocol" receiver rules): validate policy/namespace, compare
   incoming version with local versions, apply conflict policy (LWW winner via M0
   `version.Compare`; siblings deferred to M7's resolver but the hook exists), persist the
   versioned record + latest pointer in one batch, append a mutation-log entry tagged
   `source=local-replica`, and **acknowledge only after durable local store**. Client side
   issues the RPC to a candidate. TS-031 acceptance: a remote node stores a durable replica
   and returns its version.

6. **Origin+1 coordinator (TS-032), `internal/kv/coordinator.go`.** Wire the full write path
   from `design/05` "Write algorithm" / "Write state machine":
   `RECEIVED -> LOCAL_DURABLE -> REPLICATING_MIN_ACK -> ACKABLE`. After the local batch,
   select candidates (step 4), fan out `StoreReplica` in parallel with the policy write
   timeout, count durable ACKs, and **return success once
   `acked >= minAckNearbyReplicas` (=1 by default)**; otherwise fail with
   `InsufficientNearbyReplicas`. The response reports `acked_nearby_replicas` (TS-032
   acceptance: zero candidates -> fail; one durable ACK -> succeed with
   `ackedNearbyReplicas=1`). Enqueue target-N fanout / subscriber-notify / global as
   no-op hooks now; M4/M5/M7 fill them.

7. **Idempotency, `internal/replication/local/idempotency.go`.** A bounded dedupe cache keyed
   by `mutation_id` (M0 `MutationID`, derived from the client idempotency key or
   writer-sequence). A retried Put with the same ID produces **one logical mutation**
   (property 5, `IMPLEMENTATION_STRATEGY.md` section 3) on both the coordinator and the replica
   receiver.

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 3 and the TS tickets:

- A write **fails** when no nearby replica candidate exists. (M3; TS-032)
- A write **succeeds** when one nearby replica stores it durably; response reports
  `ackedNearbyReplicas=1`. (M3; TS-032)
- Killing the origin after a successful write leaves the value on the replica. (M3)
- One-node put/get/delete works with response metadata present. (TS-030)
- A remote node stores a durable replica and returns a version. (TS-031)
- Placement: distinct-node enforced; require-local-geo fails with no local peer;
  prefer-local-geo spills only when needed and allowed. (TS-023)

## Verification

1. **Unit:** placement tests cover distinct-node, compliance hard-fail, prefer-local-geo
   spillover, and scoring (TS-023, `design/16_testing_strategy.md` "Placement"); KV
   service tests cover local put/get/delete + metadata (TS-030); StoreReplica receiver test
   asserts durable store + returned version (TS-031); idempotency test asserts a duplicate
   `mutation_id` collapses to one record (property 5).
2. **Docker integration (`tests/integration/kv_origin_plus_one_test.go`)** on the 3-node
   compose cluster:
   - `wavespanctl --addr node1 kv put default foo bar` succeeds and reports
     `ackedNearbyReplicas=1` (origin+1 ACK; `design/16` "write ACK requires one nearby durable
     replica").
   - Configure a single isolated node (no candidates) and assert the put fails with
     `InsufficientNearbyReplicas` (M3 "fails if no nearby replica").
   - After a successful put on node1, `make docker-kill NODE=node1`, then
     `wavespanctl --addr node2 kv get default foo` returns `bar` — the value survives on the
     replica (M3 "killing origin after success leaves value on replica";
     `design/10_docker_dev.md` acceptance).
3. **Bank invariant (layer 3):** stand up the adapted `testing-waves` bank workload against
   the 3-node cluster with concurrent transfers, then quiesce and assert total balance is
   conserved and every key has a single deterministic LWW winner. This is the first
   bank-invariant run and exercises the **origin+1 invariant CI gate**
   (`IMPLEMENTATION_STRATEGY.md` section 3).
