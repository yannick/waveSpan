# Profiling image: bakes a HOST-built static wavespan-node binary into a small base. Used only by
# wavespan-profile / docker-compose.profile.yaml. Unlike the production Dockerfile (which compiles
# inside docker from the parent context), this COPYs a prebuilt binary so it works from a git
# worktree without the parent-context (sibling wavesdb) dependency. Build context = the waveSpan
# module root. Build the binary first:
#   GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/wavespan-node.linux ./cmd/wavespan-node
FROM alpine:3.20
RUN mkdir -p /var/lib/wavespan /etc/wavespan
COPY bin/wavespan-node.linux /usr/local/bin/wavespan-node
COPY config/dev.yaml /etc/wavespan/dev.yaml
EXPOSE 7700 7800 7900
ENTRYPOINT ["/usr/local/bin/wavespan-node", "--config", "/etc/wavespan/dev.yaml"]
