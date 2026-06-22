#!/usr/bin/env bash
# Seed prefix-scannable KV test records onto each node of the local dev cluster.
#
# Writes 10 records via EACH node's data port, so that node is the write origin/holder
# (origin+1 also replicates each to one nearby node). Keys are namespaced per node so they
# are prefix-scannable in the UI Data Browser:
#
#   keyOnNode1-AAA, keyOnNode1-BBB, ... keyOnNode1-JJJ   (prefix "keyOnNode1-")
#   keyOnNode2-AAA, ...                                   (prefix "keyOnNode2-")
#   keyOnNode3-AAA, ...                                   (prefix "keyOnNode3-")
#
# Usage:  ./scripts/seed-test-records.sh
# Env:    CTL (wavespanctl path), NS (namespace), ADDRS (space-separated data-port addrs)
set -euo pipefail

CTL="${CTL:-./bin/wavespanctl}"
NS="${NS:-default}"
# Compose maps node1/2/3 data ports to 7811/7812/7813 (docker/docker-compose.yaml).
read -r -a ADDRS <<<"${ADDRS:-localhost:7811 localhost:7812 localhost:7813}"
SUFFIXES=(AAA BBB CCC DDD EEE FFF GGG HHH III JJJ)

command -v "$CTL" >/dev/null 2>&1 || [ -x "$CTL" ] || { echo "wavespanctl not found at '$CTL' (run: make build)"; exit 1; }

total=0
for i in "${!ADDRS[@]}"; do
  n=$((i + 1))
  addr="${ADDRS[$i]}"
  echo "node${n} -> ${addr}"
  for s in "${SUFFIXES[@]}"; do
    key="keyOnNode${n}-${s}"
    val="record ${s} stored on node${n}: hello from ${key}"
    "$CTL" kv put --addr "$addr" "$NS" "$key" "$val" >/dev/null
    echo "  put ${NS}/${key}"
    total=$((total + 1))
  done
done
echo "done: ${total} records across ${#ADDRS[@]} nodes (namespace=${NS})"
