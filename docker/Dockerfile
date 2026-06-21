# Multi-stage, CGO-free, FROM scratch build of wavespan-node (design/24_container_dev_and_testing.md).
# Build context is the PARENT of waveSpan/ so the sibling `wavesdb` module (referenced via
# `replace wavesdb => ../wavesdb`) is present. The same Dockerfile is used by `make image`
# (Apple container / local) and by CI `docker buildx` (Linux, multi-arch).

# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
# sibling Go modules
COPY wavesdb/ ./wavesdb/
COPY waveSpan/ ./waveSpan/
WORKDIR /src/waveSpan
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/wavespan-node ./cmd/wavespan-node

FROM scratch
# CA certs for outbound TLS (peer clusters, object store); the only non-binary content.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/wavespan-node /wavespan-node
COPY --from=build /src/waveSpan/config/dev.yaml /etc/wavespan/dev.yaml
# Non-root. GC tuning is set as runtime env, not an image layer (IMPLEMENTATION_STRATEGY.md s4).
USER 65532:65532
ENV GOGC=100 GOMEMLIMIT=0
# gossip / data / admin (design/04, design/09)
EXPOSE 7700 7800 7900
ENTRYPOINT ["/wavespan-node"]
CMD ["--config", "/etc/wavespan/dev.yaml"]
