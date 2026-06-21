# M02 - Membership and latency graph: discovery, SWIM gossip, RTT probes

**Milestone:** M2 (`design/18_implementation_roadmap.md` "Milestone 2")
**Tickets:** TS-020, TS-021, TS-022 (`design/19_agent_work_items.md`)
**Depends on:** M0 (config, observability), M1 (storage UUID for member identity)
**Enables:** M3 (placement needs the latency graph and liveness)

## Context

WaveSpan maintains its **own** runtime membership layer that works under Docker without
Kubernetes (`design/README.md` hard rule 2; `design/04_membership_latency_gossip.md`). M2
delivers: static seed discovery for Docker, SWIM-style gossip with liveness states, a
time-decayed directed latency graph (EWMA + p95 + edge expiry), the compact `HolderSummary`
type that later milestones gossip, and an admin endpoint exposing membership and the latency
graph.

This is the first milestone that runs as a real **3-node docker-compose cluster**
(`design/10_docker_dev.md`), so the docker integration test layer
(`IMPLEMENTATION_STRATEGY.md` section 3, layer 2) switches on here.

Member identity (`design/04` "Member identity"): `memberId` is runtime identity,
`storageUuid` (from M1, CF `sys`) is durable storage identity, plus topology labels
(`zone`/`region`/`geo`) from config/env and advertised gossip/data/admin addresses.

## Files to create

```
internal/membership/identity.go      Member struct (clusterId, memberId, storageUuid, topo, advertiseAddrs)
internal/membership/discovery.go     discovery interface; docker static-seed impl (parses WAVESPAN_SEEDS)
internal/membership/gossip.go        SWIM-style tick: probe, ack, indirect probe, delta exchange
internal/membership/liveness.go      ALIVE->SUSPECT->UNREACHABLE->DEAD->FORGOTTEN state machine
internal/membership/roster.go        member roster + piggybacked metadata + holder summaries
internal/membership/holder.go        HolderSummary type (proto-backed) + range-directory stub
internal/membership/membership.go    public facade: Members(), Live(), Self(), Subscribe()
internal/latencygraph/probe.go       RTT probe driver (rides gossip ticks)
internal/latencygraph/graph.go       LatencyEdge store, EWMA + p95, edge expiry, score()
internal/latencygraph/graph_test.go  EWMA/p95 math, expiry, scoring determinism
internal/membership/*_test.go        liveness transitions, discovery parsing, delta merge
internal/observability/admin.go      EXTEND: /admin/membership and /admin/latency handlers
proto/wavespan/v1/admin.proto        Member, LatencyEdge, HolderSummary, MembershipResponse messages
docker/docker-compose.yaml           FINALIZE the 3-node cluster from design/10_docker_dev.md
tests/integration/membership_test.go 3-node form-up; kill-node -> suspect; latency edges visible
```

## Steps

1. **Member identity, `internal/membership/identity.go`.** Build a `Member` from M0 config +
   M1 `StorageUUID()`: `clusterId`, `memberId`, `storageUuid`, `podName`, `nodeName`,
   `zone`, `region`, `geo`, and advertise addresses for `gossip`/`data`/`admin`
   (`design/04` "Member identity"). Topology labels come from `WAVESPAN_ZONE/REGION/GEO`.

2. **Discovery, `internal/membership/discovery.go`.** A `Discovery` interface returning seed
   addresses. Implement the **Docker** static-seed provider: parse `WAVESPAN_SEEDS`
   (comma-separated `host:port`) from M0 config (`design/04` "Docker discovery";
   `design/10_docker_dev.md` "Static discovery"). Leave a Kubernetes provider as a typed stub
   (headless-Service DNS) wired but unused until M11 — no Kubernetes API dependency in the
   data node (`design/README.md` hard rule 2).

3. **SWIM gossip, `internal/membership/gossip.go`.** Per `design/04` "Gossip protocol", each
   tick: select a peer (random + latency-interest), direct-ping, on timeout request indirect
   pings via k random peers, exchange membership deltas, exchange holder summaries and repair-
   pressure summaries (the latter a stub field until M4), and feed the measured RTT to the
   latency graph. Piggyback metadata on pings to avoid extra round-trips.

4. **Liveness, `internal/membership/liveness.go`.** Implement the state machine
   `ALIVE -> SUSPECT -> UNREACHABLE -> DEAD -> FORGOTTEN` with the triggers in `design/04`
   "Liveness states": missed pings / phi suspicion -> SUSPECT; successful direct/indirect ping
   -> ALIVE; suspicion timeout -> UNREACHABLE; longer timeout (or, later, k8s deletion) ->
   DEAD; retention elapsed + repair complete -> FORGOTTEN. **Do not forget dead members
   immediately** — their holder records are needed for repair (M4).

5. **Latency graph, `internal/latencygraph/graph.go`.** Store directed `LatencyEdge`
   (`from`, `to`, `ewma_rtt_ms`, `p95_rtt_ms`, `packet_loss`, `last_success`, `last_failure`,
   `sample_count`) per `design/04` "Latency graph". On each probe sample update EWMA and a
   streaming p95 (e.g. a bounded reservoir or P-square estimator); expire edges with no recent
   success. Implement `score(peer)` with the weights in `design/04` "Latency graph" plus the
   topology penalty table ("Topology penalty"). Hard filters (alive, distinct-node, compliance,
   accepting-writes, disk/cache budget) are defined here but consumed by placement in M3.

6. **Holder summary + range-directory stub, `internal/membership/holder.go`.** Define the
   `HolderSummary` proto (`design/04` "Holder summaries": namespace, range_id, member_id,
   bloom_filter, approximate_key_count, low/high watermark, generated_at). M2 only gossips
   empty/heartbeat summaries and stands up the range-directory data structure
   (`design/04` "Range directory"); it is populated by M4. This exists now so gossip carries
   the right envelope shape from the start.

7. **Admin endpoints, `internal/observability/admin.go`.** Add `/admin/membership` (roster +
   liveness states) and `/admin/latency` (latency-graph edges) to the M0 admin server,
   backed by `admin.proto` messages. These satisfy the M2 acceptance "latency graph edges are
   visible" and TS-022 "admin endpoint shows graph".

8. **Finalize docker-compose.** Complete `docker/docker-compose.yaml` with the three nodes
   from `design/10_docker_dev.md` (env: `WAVESPAN_RUNTIME=docker`, cluster/member IDs, topo
   labels, `WAVESPAN_SEEDS`, per-node volume, mapped admin port). `make docker-up` brings up
   a forming cluster.

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 2 and the TS tickets:

- A 3-node Docker cluster forms gossip membership. (M2; TS-020 "3 Docker nodes discover each
  other")
- Killing one node marks it suspect then unreachable. (M2; TS-021 "killed node becomes
  suspect/unreachable")
- Latency-graph edges are visible via the admin endpoint; injected latency changes the
  placement score. (M2; TS-022)

## Verification

1. **Unit:** liveness transition tests (each edge of the state machine fires on its trigger,
   and dead members are retained until the retention window); discovery parses `WAVESPAN_SEEDS`
   correctly; latency-graph tests assert EWMA/p95 math, edge expiry, and that `score` is a
   deterministic ordering and that injecting +100ms RTT raises a peer's score (TS-022).
2. **Docker integration (`tests/integration/membership_test.go`):**
   - `make docker-up`; poll `/admin/membership` on all three nodes until each lists the other
     two as `ALIVE` (3-node form-up, M2/TS-020; matches `design/16_testing_strategy.md`
     "3-node cluster forms gossip membership").
   - `make docker-kill NODE=node3`; poll the survivors' `/admin/membership` until `node3`
     becomes `SUSPECT` then `UNREACHABLE` within the configured timeouts (TS-021).
   - Hit `/admin/latency` and assert non-empty edges between live members (M2 "edges visible").
   - For the latency-graph population check, the same harness can run the 5-node compose
     overlay (`design/16` "5-node cluster populates latency graph").
3. **Gate enabled:** this milestone turns on the docker-compose integration layer; no bank
   invariant yet (no writes until M3).
