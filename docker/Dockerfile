# Multi-stage, CGO-free, FROM scratch build of wavespan-node (design/24_container_dev_and_testing.md).
# Build context is the PARENT of waveSpan/ so the sibling `wavesdb` module (referenced via
# `replace wavesdb => ../wavesdb`) is present. The same Dockerfile is used by `make image`
# (Apple container / local) and by CI `docker buildx` (Linux, multi-arch).

# syntax=docker/dockerfile:1

# ① Build the admin console SPA so //go:embed all:dist (internal/ui) embeds real assets, not just the
# .gitkeep placeholder. `npm run build` emits to internal/ui/dist (vite outDir). Node WORKDIR is
# /waveSpan/ui so that relative outDir resolves to /waveSpan/internal/ui/dist (matching the COPY below).
FROM --platform=$BUILDPLATFORM node:20 AS ui
WORKDIR /waveSpan/ui
COPY waveSpan/ui/package.json waveSpan/ui/package-lock.json ./
RUN npm ci
COPY waveSpan/ui/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
# sibling Go modules
COPY wavesdb/ ./wavesdb/
COPY waveSpan/ ./waveSpan/
# Overlay the built console SPA BEFORE go build so //go:embed all:dist sees real assets.
COPY --from=ui /waveSpan/internal/ui/dist/ ./waveSpan/internal/ui/dist/
WORKDIR /src/waveSpan
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/wavespan-node ./cmd/wavespan-node
# Seed an empty data dir owned by the non-root runtime user; a volume mounted at this path
# inherits the ownership, so wavesdb can create its column families as uid 65532.
RUN mkdir -p /seed-data

FROM scratch
# CA certs for outbound TLS (peer clusters, object store); the only non-binary content.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/wavespan-node /wavespan-node
COPY --from=build /src/waveSpan/config/dev.yaml /etc/wavespan/dev.yaml
# Data dir owned by the runtime user so a mounted volume is writable as non-root.
COPY --from=build --chown=65532:65532 /seed-data /var/lib/wavespan
# Non-root. GC tuning is set as runtime env, not an image layer (IMPLEMENTATION_STRATEGY.md s4).
USER 65532:65532
ENV GOGC=100 GOMEMLIMIT=0
# gossip / data / admin (design/04, design/09)
EXPOSE 7700 7800 7900
ENTRYPOINT ["/wavespan-node"]
CMD ["--config", "/etc/wavespan/dev.yaml"]
