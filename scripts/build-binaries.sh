#!/usr/bin/env bash
# Cross-compile every wavespan command binary (CGO-free, static) for the standard targets into
# ./dist, packaging each target as a .tar.gz (.zip for Windows). cmd/wavespan-node embeds the SPA,
# so the UI (internal/ui/dist) must be built first: CI runs `npm run build`; locally run `make ui`.
#
# VERSION env var stamps the artifact names (defaults to "dev").
set -euo pipefail
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

version="${VERSION:-dev}"
targets=("linux/amd64" "linux/arm64" "darwin/amd64" "darwin/arm64" "windows/amd64")
cmds=(wavespan-node wavespan-gateway wavespanctl wavespan-bench wavespan-profile wavespan-benchui)

rm -rf dist && mkdir -p dist
for t in "${targets[@]}"; do
  os="${t%/*}"; arch="${t#*/}"
  name="wavespan_${version}_${os}_${arch}"
  stage="dist/${name}"
  mkdir -p "$stage"
  ext=""; [ "$os" = "windows" ] && ext=".exe"
  for c in "${cmds[@]}"; do
    echo "  building ${c} -> ${os}/${arch}"
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags="-s -w" \
      -o "${stage}/${c}${ext}" "./cmd/${c}"
  done
  if [ "$os" = "windows" ]; then
    (cd dist && zip -qr "${name}.zip" "${name}")
  else
    tar -C dist -czf "dist/${name}.tar.gz" "${name}"
  fi
  rm -rf "$stage"
done
echo "--- artifacts in ./dist ---"
ls -la dist
