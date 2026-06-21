# 04. Membership, latency graph, and gossip

## Goal

Build a runtime membership layer that works in Kubernetes and Docker. It must maintain a live latency graph for replica placement and closest-holder reads.

## Member identity

Each running process has:

```yaml
clusterId: prod-use1
memberId: m-8f231a
storageUuid: s-01HQ...
podName: Wavespan-data-3
nodeName: ip-10-0-8-12
zone: us-east1-b
region: us-east1
geo: us-east
advertiseAddrs:
  gossip: 10.0.8.44:7700
  data: 10.0.8.44:7800
  admin: 10.0.8.44:7900
runtime:
  kubernetes: true
  docker: false
```

`memberId` is runtime identity. `storageUuid` is persistent storage identity. A pod name alone is not enough.

## Discovery modes

### Kubernetes discovery

Inputs:

- StatefulSet ordinal;
- headless Service DNS;
- pod labels;
- node name via Downward API;
- node labels through Kubernetes API watch;
- CRD cluster config.

### Docker discovery

Inputs:

- static seed list;
- environment variables for node/zone/region/geo;
- local storage directory;
- explicit advertised addresses.

Example:

```yaml
WaveSPAN_CLUSTER_ID: dev
WaveSPAN_MEMBER_ID: node-1
WaveSPAN_NODE_NAME: docker-node-1
WaveSPAN_ZONE: dev-zone-a
WaveSPAN_REGION: dev-region
WaveSPAN_GEO: dev
WaveSPAN_SEEDS: node-1:7700,node-2:7700,node-3:7700
```

## Gossip protocol

Use SWIM-style membership with piggybacked metadata.

Each gossip tick:

1. select peer by random plus latency-interest schedule;
2. ping peer;
3. measure RTT;
4. exchange membership deltas;
5. exchange holder summaries;
6. exchange repair pressure summaries;
7. update latency graph.

## Liveness states

```text
ALIVE -> SUSPECT -> UNREACHABLE -> DEAD -> FORGOTTEN
```

State transitions:

| From | To | Trigger |
|---|---|---|
| `ALIVE` | `SUSPECT` | missed pings / high phi suspicion |
| `SUSPECT` | `ALIVE` | successful direct or indirect ping |
| `SUSPECT` | `UNREACHABLE` | suspicion timeout |
| `UNREACHABLE` | `DEAD` | longer timeout or Kubernetes deletion event |
| `DEAD` | `FORGOTTEN` | retention period elapsed and repair complete |

Do not immediately forget dead members. Their holder records are needed for repair decisions.

## Latency graph

The latency graph is directed and time-decayed.

```protobuf
message LatencyEdge {
  string from_member_id = 1;
  string to_member_id = 2;
  double ewma_rtt_ms = 3;
  double p95_rtt_ms = 4;
  double packet_loss = 5;
  int64 last_success_unix_ms = 6;
  int64 last_failure_unix_ms = 7;
  uint32 sample_count = 8;
}
```

Compute score:

```text
score(peer) =
    0.55 * normalized_ewma_rtt
  + 0.15 * normalized_p95_rtt
  + 0.10 * packet_loss
  + 0.10 * load_pressure
  + 0.05 * disk_pressure
  + 0.05 * topology_penalty
```

Hard filters are applied before scoring:

- peer must be alive or recently alive;
- peer must not be on same Kubernetes node when `requireDistinctNodes=true`;
- peer must satisfy compliance boundary when required;
- peer must accept writes;
- peer must have enough disk and cache budget.

## Topology penalty

Static topology is a hint.

```text
same node: forbidden for durable replicas
same zone: +0
same region: +0.2
same geo: +0.4
outside geo: +1.0 unless policy allows only by spillover
unknown topology: +0.5
```

Measured RTT can override static topology only when policy allows.

## Holder summaries

Do not gossip every key. Gossip compact summaries per namespace/range.

```protobuf
message HolderSummary {
  string namespace = 1;
  string range_id = 2;
  string member_id = 3;
  bytes bloom_filter = 4;
  uint64 approximate_key_count = 5;
  Version low_watermark = 6;
  Version high_watermark = 7;
  int64 generated_at_unix_ms = 8;
}
```

The range directory uses holder summaries to avoid broadcast.

### Holder summary staleness

Each `HolderSummary` carries `generated_at_unix_ms`. Consumers must treat a summary as
**stale** once it is older than `staleHolderSummaryTtl`:

```yaml
staleHolderSummaryTtl: 3x gossipInterval   # default
```

The default of three gossip intervals tolerates a couple of missed gossip rounds before a
summary is distrusted. A summary is stale when:

```text
now_unix_ms - generated_at_unix_ms > staleHolderSummaryTtl
```

When the summary a consumer would use is stale, it must not place blind trust in that
holder. Fallback order:

```text
1. select an alternate holder with a fresh summary for the same range;
2. if no fresh holder summary exists, fall back to anti-entropy / range-summary exchange
   to rediscover current holders before serving the request.
```

A stale summary is still a useful hint for repair and discovery, but it must not be used
to claim cache completeness or to skip holder re-resolution.

Expose how often this happens:

```text
holder_summary_staleness_seconds   # age of summaries used, observed at consume time
```

## Range directory

Each node keeps an eventually consistent range directory:

```yaml
rangeId: kv-default-00042
startKey: /kv/default/data/a
endKey: /kv/default/data/f
knownHolders:
  - memberId: m1
    holderType: durable
    highWatermark: ...
  - memberId: m2
    holderType: dynamic-cache
    highWatermark: ...
  - memberId: m3
    holderType: summary-only
```

The directory does not need perfect accuracy. If a selected holder misses, fallback to another holder or repair source.

## Spot node handling

Spot churn handling:

- mark node `SUSPECT` quickly after missed heartbeats;
- stop selecting suspect nodes for new durable replicas;
- do not delete holder entries immediately;
- schedule anti-entropy repair for keys whose durable replica count fell below target;
- treat Kubernetes termination notice as a drain signal when available;
- assume no notice in the common case.

## Drain protocol

On graceful shutdown:

```text
1. mark self DRAINING in gossip;
2. reject new coordinator writes unless no alternative;
3. transfer active dynamic subscriptions where possible;
4. flush mutation logs;
5. send holder summaries to peers;
6. exit before termination grace expires.
```

Do not block shutdown indefinitely.

## Implementation checklist

- [ ] Member identity persisted with storage UUID.
- [ ] Kubernetes discovery implemented.
- [ ] Docker static discovery implemented.
- [ ] SWIM-style gossip implemented.
- [ ] Latency probes with EWMA/p95 implemented.
- [ ] Holder summary exchange implemented.
- [ ] Range directory implemented.
- [ ] Distinct-node filter enforced for durable replicas.
- [ ] Compliance boundary filter implemented.
- [ ] Spot drain and ungraceful failure paths implemented.

