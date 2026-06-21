# 10. Docker development mode

> **Primary local path is Apple `container`, not docker-compose.** On Apple Silicon,
> `design/24_container_dev_and_testing.md` is the canonical dev/test loop: it boots each node in a
> fast per-node lightweight VM and is the recommended inner loop. This doc describes the
> docker-compose path, which remains the **portable / CI** way to run the cluster (Linux runners,
> non-Apple-Silicon machines). Both paths run the *same* `scratch`, CGO-free OCI image — see doc
> 24 for the build, and this doc for the seed-discovery / env / acceptance contract they share.

## Goal

Run the same data-node binary without Kubernetes for local testing.

Docker mode must support:

- multiple local data nodes;
- static seed discovery;
- fake topology labels;
- persistent local directories;
- local gateway;
- optional artificial latency and partitions.

## Example docker-compose

```yaml
services:
  node1:
    image: Wavespan/server:dev
    command: ["Wavespan-node"]
    environment:
      WaveSPAN_RUNTIME: docker
      WaveSPAN_CLUSTER_ID: dev
      WaveSPAN_MEMBER_ID: node1
      WaveSPAN_NODE_NAME: docker-node-1
      WaveSPAN_ZONE: zone-a
      WaveSPAN_REGION: dev-region
      WaveSPAN_GEO: dev
      WaveSPAN_SEEDS: node1:7700,node2:7700,node3:7700
    volumes:
      - ./data/node1:/var/lib/Wavespan
    ports:
      - "7801:7800"

  node2:
    image: Wavespan/server:dev
    environment:
      WaveSPAN_RUNTIME: docker
      WaveSPAN_CLUSTER_ID: dev
      WaveSPAN_MEMBER_ID: node2
      WaveSPAN_NODE_NAME: docker-node-2
      WaveSPAN_ZONE: zone-a
      WaveSPAN_REGION: dev-region
      WaveSPAN_GEO: dev
      WaveSPAN_SEEDS: node1:7700,node2:7700,node3:7700
    volumes:
      - ./data/node2:/var/lib/Wavespan
    ports:
      - "7802:7800"

  node3:
    image: Wavespan/server:dev
    environment:
      WaveSPAN_RUNTIME: docker
      WaveSPAN_CLUSTER_ID: dev
      WaveSPAN_MEMBER_ID: node3
      WaveSPAN_NODE_NAME: docker-node-3
      WaveSPAN_ZONE: zone-b
      WaveSPAN_REGION: dev-region
      WaveSPAN_GEO: dev
      WaveSPAN_SEEDS: node1:7700,node2:7700,node3:7700
    volumes:
      - ./data/node3:/var/lib/Wavespan
    ports:
      - "7803:7800"
```

## Static discovery

Docker discovery parses `WaveSPAN_SEEDS` and gossips from there.

No Kubernetes API should be required.

## Artificial failures

Add test tooling for:

- killing containers;
- pausing containers;
- deleting local data directories;
- network partitions;
- latency injection;
- packet loss injection;
- clock skew simulation.

The full fault-injection interface — which faults are container-native vs in-process toggles, and
how the harness drives them identically under Apple `container` and Docker — is specified once in
`design/24_container_dev_and_testing.md` "Fault injection". Both orchestrators expose the same
interface, so this docker-compose path inherits it without restating it.

## Local test commands

```bash
# start cluster
make docker-up

# write key through node1
bin/Wavespanctl --addr localhost:7801 kv put default foo bar

# read through node3; should fetch/cache if missing
bin/Wavespanctl --addr localhost:7803 kv get default foo

# kill origin node
make docker-kill NODE=node1

# read from node2 or node3; should converge from replica
bin/Wavespanctl --addr localhost:7802 kv get default foo
```

## Docker acceptance tests

- [ ] Three nodes gossip and form membership.
- [ ] Latency graph is populated.
- [ ] Write to node1 acknowledges only after node2 or node3 stores replica.
- [ ] Read on node3 fetches from closest holder and creates dynamic cache.
- [ ] Update propagates to dynamic cache subscriber.
- [ ] Killing origin after successful write does not lose the value if the replica remains.
- [ ] Partition heals and conflicts converge by configured policy.

