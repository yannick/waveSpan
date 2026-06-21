#!/usr/bin/env bash
# Build the wavespan-node image with Apple `container` (design/24_container_dev_and_testing.md).
# Build context is the PARENT dir so the sibling `wavesdb` module (replace => ../wavesdb) is
# visible — this refines doc 24's simplified `.` example. Same Dockerfile as CI.
set -euo pipefail
cd "$(dirname "$0")/.."

PLATFORM="${PLATFORM:-linux/arm64}"
IMAGE="${IMAGE:-wavespan/node:dev}"

container build --platform "$PLATFORM" -t "$IMAGE" -f docker/Dockerfile ..
echo "built $IMAGE ($PLATFORM)"
