# 09. Kubernetes operator

## Goal

Production deployment is Kubernetes-native and operator-managed.

The operator creates and reconciles:

- StatefulSets;
- Services;
- PVCs;
- ConfigMaps;
- Secrets;
- pod disruption budgets;
- topology spread constraints;
- replication policies;
- cluster peer configuration;
- backup/restore jobs.

## StatefulSet layout

Data pods run as a StatefulSet with persistent volumes.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: Wavespan-data
spec:
  serviceName: Wavespan-data
  replicas: 9
  selector:
    matchLabels:
      app.kubernetes.io/name: Wavespan
      app.kubernetes.io/component: data
  template:
    metadata:
      labels:
        app.kubernetes.io/name: Wavespan
        app.kubernetes.io/component: data
    spec:
      serviceAccountName: Wavespan-data
      terminationGracePeriodSeconds: 60
      containers:
        - name: Wavespan
          image: Wavespan/server:dev
          ports:
            - name: gossip
              containerPort: 7700
            - name: data
              containerPort: 7800
            - name: repl
              containerPort: 7801
            - name: admin
              containerPort: 7900
          env:
            - name: WaveSPAN_RUNTIME
              value: kubernetes
            - name: WaveSPAN_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: WaveSPAN_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          volumeMounts:
            - name: data
              mountPath: /var/lib/Wavespan
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Ti
```

## Topology spread

The operator should add spread constraints by node and zone when labels exist.

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: Wavespan
        app.kubernetes.io/component: data
  - maxSkew: 2
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: Wavespan
        app.kubernetes.io/component: data
```

Use `ScheduleAnyway` rather than `DoNotSchedule` for spot-heavy clusters unless the user requests hard placement. The data layer has its own distinct-node durable-replica rule.

## Pod disruption budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: Wavespan-data
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: Wavespan
      app.kubernetes.io/component: data
```

PDBs do not protect against spot termination. They help during voluntary disruptions.

## Node labels used

Standard labels when available:

```text
kubernetes.io/hostname
topology.kubernetes.io/zone
topology.kubernetes.io/region
```

Custom labels:

```text
topology.Wavespan.io/geo
topology.Wavespan.io/rack
topology.Wavespan.io/cluster-id
```

The runtime latency graph is still authoritative for closeness.

## Operator reconcile loop

For `WaveSpanCluster`:

1. validate spec;
2. create/update ConfigMaps;
3. create/update Secrets;
4. create/update headless Service;
5. create/update data StatefulSet;
6. create/update gateway Deployment if enabled;
7. create/update PDB;
8. create/update ServiceMonitor if enabled;
9. write status conditions.

Status conditions:

```yaml
status:
  phase: Ready | Degraded | Scaling | Upgrading | Error
  readyMembers: 8
  desiredMembers: 9
  underReplicatedEstimate: 12033
  globalReplicationLagSeconds: 4.2
  conditions:
    - type: DataPodsReady
      status: "True"
    - type: RepairHealthy
      status: "False"
      reason: SpotChurn
```

## Rolling upgrades

Upgrade one pod at a time by default.

Before deleting a pod:

1. operator sets pod annotation `Wavespan.io/drain-requested=true`;
2. pod marks itself `DRAINING` in gossip;
3. pod flushes local mutation log;
4. pod rejects new coordinator writes when alternatives exist;
5. operator waits for readiness gate or timeout;
6. pod is terminated;
7. replacement starts and rejoins.

## Scale down

Scale down must be explicit and conservative.

1. choose member to remove;
2. mark draining;
3. repair/move durable replicas away from it;
4. wait until member has no unique durable keys above threshold;
5. remove from StatefulSet;
6. retain PVC until admin confirms deletion.

Do not automatically delete PVCs on scale down in v1.

## Spot node handling

The operator cannot prevent spot loss. It should:

- tolerate frequent pod loss;
- avoid blocking on perfect spread;
- expose repair status;
- support fast replacement pod scheduling;
- avoid strict anti-affinity that makes replacement impossible.

Runtime repair is more important than Kubernetes scheduling perfection.

## Implementation checklist

- [ ] CRDs defined.
- [ ] Kubebuilder or equivalent operator scaffold created.
- [ ] StatefulSet reconciliation implemented.
- [ ] PVC handling implemented.
- [ ] Headless Service implemented.
- [ ] Topology spread implemented.
- [ ] PDB implemented.
- [ ] Drain annotation protocol implemented.
- [ ] Status conditions implemented.
- [ ] Scale-up safe path implemented.
- [ ] Scale-down guarded path implemented.

