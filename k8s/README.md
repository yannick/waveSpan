# WaveSpan on Kubernetes (Flux + Kustomize, single cluster)

A deployment example that runs **one WaveSpan node per machine** across a **dedicated node group**,
managed by **FluxCD** with a **hash-suffixed config** ConfigMap.

## Layout

```
k8s/
├── kustomization.yaml   # Kustomize entry — configMapGenerator (hash suffix) + image pin
├── config.yaml          # FULL node config (every option written out); content-hashed into the ConfigMap
├── namespace.yaml
├── rbac.yaml            # ServiceAccount + read access (for future K8s-API discovery)
├── services.yaml        # headless peer Service (gossip/data) + admin Service (metrics/health/UI)
├── certificate.yaml     # cert-manager Certificate -> wavespan-tls Secret (mTLS)
├── daemonset.yaml       # 1 pod/node on the node group; node-local storage; per-pod identity
└── flux/
    ├── source.yaml          # GitRepository (flux-system)
    └── kustomization.yaml    # Flux Kustomization -> applies ./k8s, prune + health-gated
```

## Why a DaemonSet

"One instance per node on all nodes within a node group" is exactly a **DaemonSet** scheduled with a
`nodeSelector` for the group (and `tolerations` for its taint). Each pod gets a **unique identity**
from the Downward API — `WAVESPAN_MEMBER_ID` = node name, `WAVESPAN_ADVERTISE_HOST` = pod IP — and
stores its data on **node-local disk** (`hostPath`), one DB per node. A StatefulSet would instead fix
a replica count and would not automatically track the node group.

## Adapt before applying

1. **Node group** (`daemonset.yaml`): set the `nodeSelector` to your group's label and the
   `tolerations` to its taint. Cloud equivalents are noted inline (EKS/GKE/AKS).
2. **Image** (`kustomization.yaml` `images:`): point `newTag` at your published
   `ghcr.io/yannick/wavespan-node` tag.
3. **TLS**: `config.yaml` runs mTLS (`insecureDevMode: false`) with certs from `wavespan-tls`
   (issued by cert-manager via `certificate.yaml`, needs a ClusterIssuer `wavespan-ca`). For a quick
   insecure trial, set `insecureDevMode: true`, drop `certificate.yaml` from the kustomization, and
   remove the `tls` volume/mount.
4. **Storage**: `hostPath: /var/lib/wavespan`. Swap for a local-PV StorageClass if you prefer managed
   local volumes (DaemonSets can't use `volumeClaimTemplates`, so hostPath or a pre-provisioned local
   PV is the path).
5. **Replication / namespaces** (`config.yaml`): `targetNearbyReplicas`, and the per-namespace
   `replicationFactor` (`""` | `N` | `all` | `global` — see `design/28`). This example ships a
   `ref` namespace at `all` (every node of this cluster).
6. **Flux** (`flux/`): set the `GitRepository` `url`/`ref` to your repo. Apply `flux/` once into
   `flux-system`; Flux then reconciles `./k8s` and rolls the DaemonSet whenever `config.yaml`'s hash
   changes.

## Membership bootstrap

Native Kubernetes API discovery is a stub today (`design/09`), so membership uses **static-seed**
discovery (`membership.runtime: docker`) seeded from the **headless Service** DNS
(`wavespan-peer.wavespan.svc.cluster.local:7700`, which resolves to every node). `publishNotReadyAddresses`
makes pods discoverable while the cluster forms.

## Single cluster

`globalReplication.mode: "off"` and no `peers` — this is one cluster. To federate later, set the mode
to `active-active-async`, add `peers`, and use `replicationFactor: global` for namespaces that should
span clusters.

## Apply

```bash
# render locally to inspect (note the wavespan-config-<hash> ConfigMap + rewritten DaemonSet ref):
kubectl kustomize k8s/

# GitOps: commit, then bootstrap Flux once
kubectl apply -f k8s/flux/
```
