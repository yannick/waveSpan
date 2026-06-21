#!/usr/bin/env bash
# Tear down a local Apple `container` cluster started by up.sh. Usage: container/down.sh [N].
set -euo pipefail

N="${1:-3}"
NET="${NET:-wavespan-dev}"

for i in $(seq 1 "$N"); do
  container rm -f "node$i" 2>/dev/null || true
done
container network delete "$NET" 2>/dev/null || true
echo "removed $N node(s) and network $NET"
