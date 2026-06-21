#!/usr/bin/env bash
# Bring up an N-node local cluster with Apple `container` (design/24 "Bringing up a cluster").
# Usage: container/up.sh [N]   (default 3). Each node gets its own container machine, a private
# wavesdb data volume, and the shared static WAVESPAN_SEEDS list (design/04 "Docker discovery").
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-3}"
IMAGE="${IMAGE:-wavespan/node:dev}"
NET="${NET:-wavespan-dev}"

container network create "$NET" 2>/dev/null || true

seeds=""
for i in $(seq 1 "$N"); do seeds="${seeds:+$seeds,}node$i:7700"; done

half=$(((N + 1) / 2))
for i in $(seq 1 "$N"); do
  if [ "$i" -le "$half" ]; then zone="a"; else zone="b"; fi
  mkdir -p "data/node$i"
  container run -d \
    --name "node$i" \
    --network "$NET" \
    --volume "$PWD/data/node$i:/var/lib/wavespan" \
    --env WAVESPAN_RUNTIME=docker \
    --env WAVESPAN_CLUSTER_ID=dev \
    --env WAVESPAN_MEMBER_ID="node$i" \
    --env WAVESPAN_NODE_NAME="container-node-$i" \
    --env WAVESPAN_ZONE="zone-$zone" \
    --env WAVESPAN_REGION=dev-region \
    --env WAVESPAN_GEO=dev \
    --env WAVESPAN_SEEDS="$seeds" \
    "$IMAGE"
done
echo "started $N node(s) on network $NET (seeds: $seeds)"
