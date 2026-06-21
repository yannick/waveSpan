# 12. Kubernetes CRDs

## WaveSpanCluster

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: WaveSpanCluster
metadata:
  name: prod
spec:
  clusterId: prod-use1
  replicas: 9
  image: Wavespan/server:dev
  storage:
    engine: wavesdb
    volumeSize: 1Ti
    storageClassName: fast-ssd
  topology:
    geoLabel: topology.Wavespan.io/geo
    regionLabel: topology.kubernetes.io/region
    zoneLabel: topology.kubernetes.io/zone
    nodeLabel: kubernetes.io/hostname
  runtime:
    gossipPort: 7700
    dataPort: 7800
    replicationPort: 7801
    adminPort: 7900
  gateway:
    enabled: true
    replicas: 3
  disruption:
    maxUnavailable: 1
  observability:
    prometheus: true
```

## ReplicationPolicy

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: ReplicationPolicy
metadata:
  name: local-cache-default
spec:
  mode: local-cache
  consistency:
    default: eventual
  local:
    targetNearbyReplicaCount: 3
    minAckNearbyReplicas: 1
    requireDistinctNodes: true
    geoPolicy: prefer-local-geo
    maxReplicaRttMs: 15
    allowSpilloverForDurability: true
  cache:
    dynamicSubscriptions: true
    cacheReadMode: eventual
    rangeScanMayUseCache: true
    requireCoverageForCompleteScan: true
    maxSubscribersPerKey: 1024
    idleSubscriptionTtlSeconds: 600
  ttl:
    mode: lazy
    bucketSeconds: 60
    hideExpiredOnRead: false
  conflict:
    defaultPolicy: hlc-last-write-wins
```

## Compliance replication policy

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: ReplicationPolicy
metadata:
  name: eu-compliance-local-cache
spec:
  mode: local-cache
  local:
    targetNearbyReplicaCount: 3
    minAckNearbyReplicas: 1
    requireDistinctNodes: true
    geoPolicy: require-local-geo
    complianceBoundary:
      label: topology.Wavespan.io/geo
      value: eu
    allowSpilloverForDurability: false
  global:
    enabled: false
```

## Global active-active policy

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: ReplicationPolicy
metadata:
  name: active-active-global
spec:
  mode: local-cache-global-active-active
  local:
    targetNearbyReplicaCount: 3
    minAckNearbyReplicas: 1
    requireDistinctNodes: true
    geoPolicy: prefer-local-geo
  global:
    enabled: true
    mode: active-active-async
    peers:
      - clusterId: prod-euw1
      - clusterId: prod-usw2
    readPolicy: local-first
    conflict:
      defaultPolicy: hlc-last-write-wins
      allowKeepSiblings: true
    antiEntropy:
      enabled: true
      intervalSeconds: 300
```

## KVNamespace

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: KVNamespace
metadata:
  name: sessions
spec:
  clusterRef: prod
  replicationPolicyRef: local-cache-default
  ttl:
    defaultTtlSeconds: 1800
    maxTtlSeconds: 86400
  scan:
    defaultMode: cache-fast
    emitCompletenessMetadata: true
  value:
    maxInlineBytes: 10485760
```

## Graph

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: Graph
metadata:
  name: knowledge
spec:
  clusterRef: prod
  replicationPolicyRef: active-active-global
  cypher:
    subset: production-v1
    maxTraversalDepth: 8
    maxRowsReturned: 10000
    timeoutMs: 30000
  partitioning:
    strategy: hash-node-id
```

## VectorIndex

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: VectorIndex
metadata:
  name: doc-embedding
spec:
  clusterRef: prod
  graphRef: knowledge
  label: DocumentChunk
  property: embedding
  dimensions: 1536
  dtype: float32
  metric: cosine
  storage:
    raw: wavesdb
  exact:
    enabled: true
  approximate:
    enabled: true
    type: hnsw
    m: 32
    efConstruction: 200
    efSearchDefault: 80
  visibility: write-visible-with-delta
  replicationPolicyRef: active-active-global
```

## ClusterPeer

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: ClusterPeer
metadata:
  name: prod-euw1
spec:
  localClusterRef: prod
  peerClusterId: prod-euw1
  geo: eu-west
  endpoints:
    gossip: Wavespan-gossip.euw1.example.com:7700
    replication: Wavespan-repl.euw1.example.com:7801
  tls:
    secretName: Wavespan-peer-prod-euw1
  status:
    expected: connected
```

## Backup

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: WaveSpanBackup
metadata:
  name: backup-2026-06-21
spec:
  clusterRef: prod
  destination:
    type: s3
    bucket: Wavespan-backups
    prefix: prod/2026-06-21
  includeVectorIndexes: false
```

## Restore

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: WaveSpanRestore
metadata:
  name: restore-test
spec:
  clusterRef: prod-restore
  source:
    type: s3
    bucket: Wavespan-backups
    prefix: prod/2026-06-21
  rebuildVectorIndexes: true
```

## CRD status conventions

Every CRD status should include:

```yaml
status:
  observedGeneration: 3
  phase: Ready
  conditions:
    - type: Ready
      status: "True"
      lastTransitionTime: "2026-06-21T10:00:00Z"
```

## Operator validation rules

Reject CRDs when:

- `minAckNearbyReplicas < 1` for local-cache mode;
- `require-local-geo` has no compliance boundary label/value;
- vector dimensions are missing or zero;
- Cypher graph references a missing cluster;
- global peers are enabled without TLS config;
- `targetNearbyReplicaCount < minAckNearbyReplicas`;
- `requireDistinctNodes=true` but replicas exceed known schedulable nodes and strict scheduling is requested;
- a namespace selects a conflict policy that is not implemented in v1 — only
  `hlc-last-write-wins` and `keep-siblings` are accepted; `crdt-counter`, `crdt-set`,
  `lww-register`, `append-log`, and `app-resolver` are deferred (doc 06) and rejected.

## Compliance boundary existence check

The rule above rejects a `require-local-geo` policy that has **no** compliance boundary
label/value configured. That is a static, syntactic check. It does not, by itself, prove
the boundary actually exists in the cluster — a policy could name
`topology.Wavespan.io/geo = eu` while no member carries that label, in which case every
compliant write would fail at runtime with `InsufficientLocalReplicas` and the gap would
only surface under load.

The operator therefore also performs an **admission-time existence check** for
compliance (`require-local-geo`) policies:

```text
on admit/reconcile of a require-local-geo ReplicationPolicy:
    boundary = spec.local.complianceBoundary   // {label, value}
    members  = members of the referenced cluster (from gossip/membership)
    if no member has labels[boundary.label] == boundary.value:
        set status condition CompliantBoundaryPresent = False
        set phase = Degraded
        record an event explaining no member satisfies the boundary
    else:
        set status condition CompliantBoundaryPresent = True
```

This closes the "boundary never validated to exist" gap: instead of silently failing every
write, the CR is marked `Degraded` with an explicit condition the moment no member can
satisfy the boundary. Because membership is dynamic (spot churn), this is a status
condition re-evaluated on reconcile rather than a hard admission webhook rejection — a
boundary can become satisfiable again when a labeled member joins, at which point the
condition flips back to `True` and the phase clears.

```yaml
status:
  phase: Degraded
  conditions:
    - type: CompliantBoundaryPresent
      status: "False"
      reason: NoMemberMatchesComplianceBoundary
      message: "no member has label topology.Wavespan.io/geo=eu"
      lastTransitionTime: "2026-06-21T10:00:00Z"
```

