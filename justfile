# WaveSpan task runner — mirrors the Makefile (run either; identical actions).
# Install just: https://github.com/casey/just   ·   list tasks:  just   (or  just --list)
#
# ──────────────────────────────────────────────────────────────────────────────
# Parameters
# ──────────────────────────────────────────────────────────────────────────────
# Override any variable below on the command line or via the environment:
#
#   just go=go1.23 test                 # use a specific Go toolchain
#   just image=ghcr.io/acme/node:ci image
#   IMAGE=foo just image                # env vars work too (same names as the Makefile)
#   NO_COLOR=1 just                     # disable the colored banners
#
# Cluster size is a recipe PARAMETER (not a variable) — pass it positionally:
#
#   just container-up         # 3-node Apple-container cluster (the default)
#   just container-up 5       # 5-node cluster
#   just container-down 5     # tear down — pass the same count you brought up

set shell := ["bash", "-c"]

# ── overridable variables (mirror the Makefile's ?= vars) ──────────────────────
go        := env_var_or_default("GO", "go")                                  # Go toolchain binary
bin_dir   := env_var_or_default("BIN_DIR", justfile_directory() / "bin")     # where binaries land
image     := env_var_or_default("IMAGE", "wavespan-node:dev")                # scratch image tag (just image)
platforms := env_var_or_default("PLATFORMS", "linux/" + `go env GOARCH`)     # buildx platform list
cgo       := env_var_or_default("CGO_ENABLED", "0")                          # CGO for `build` (0 = static scratch)
cmds      := "wavespan-node wavespan-gateway wavespanctl wavespan-bench wavespan-profile"

# ── colors for the recipe banners (disabled when NO_COLOR is set) ──────────────
nocolor := env_var_or_default("NO_COLOR", "")
esc     := `printf '\033'`
bold    := if nocolor == "" { esc + "[1m"  } else { "" }
dim     := if nocolor == "" { esc + "[2m"  } else { "" }
green   := if nocolor == "" { esc + "[32m" } else { "" }
yellow  := if nocolor == "" { esc + "[33m" } else { "" }
blue    := if nocolor == "" { esc + "[34m" } else { "" }
reset   := if nocolor == "" { esc + "[0m"  } else { "" }

# ══════════════════════════════════════════════════════════════════════════════
# General
# ══════════════════════════════════════════════════════════════════════════════

# Show this grouped task list (default)
[group('general')]
default:
    @just --list --unsorted

# Build everything (alias for build)
[group('general')]
all: build

# ══════════════════════════════════════════════════════════════════════════════
# Build & UI
# ══════════════════════════════════════════════════════════════════════════════

# Compile all cmd/ binaries into ./bin (static, no cgo)
[group('build & ui')]
build:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Building binaries into {{bin_dir}}'
    @mkdir -p {{bin_dir}}
    @for c in {{cmds}}; do \
        printf '  {{dim}}compiling{{reset}} %s\n' "$c"; \
        CGO_ENABLED={{cgo}} {{go}} build -trimpath -o {{bin_dir}}/$c ./cmd/$c || exit 1; \
    done
    @printf '{{green}}✓{{reset}} %s\n' 'built binaries in {{bin_dir}}'

# Build the embedded SPA into internal/ui/dist
[group('build & ui')]
ui:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Building the embedded SPA'
    @cd ui && npm ci && npm run build
    @printf '{{green}}✓{{reset}} %s\n' 'SPA built into internal/ui/dist'

# Run the Vite dev server (pair with WAVESPAN_UI_DEV=1 on the node)
[group('build & ui')]
ui-dev:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Starting the Vite dev server'
    @cd ui && npm run dev

# ══════════════════════════════════════════════════════════════════════════════
# Test & lint
# ══════════════════════════════════════════════════════════════════════════════

# Run the unit test suite
[group('test & lint')]
test:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Running unit tests'
    @{{go}} test ./... && printf '{{green}}✓{{reset}} %s\n' 'unit tests passed'

# Race-enabled run (cgo required, local only)
[group('test & lint')]
test-race:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Running unit tests with the race detector'
    @CGO_ENABLED=1 {{go}} test -race ./... && printf '{{green}}✓{{reset}} %s\n' 'race tests passed'

# Docker-based integration tests (requires Docker + ../wavesdb)
[group('test & lint')]
test-integration:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Building image + running docker integration tests'
    @docker compose -f docker/docker-compose.yaml build
    @{{go}} test -tags integration -timeout 600s ./tests/integration/...

# Run golangci-lint
[group('test & lint')]
lint:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Linting'
    @golangci-lint run && printf '{{green}}✓{{reset}} %s\n' 'lint clean'

# ══════════════════════════════════════════════════════════════════════════════
# Code generation
# ══════════════════════════════════════════════════════════════════════════════

# Regenerate Go messages + Connect stubs via buf
[group('code generation')]
proto:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Generating protobuf + Connect stubs'
    @buf generate && printf '{{green}}✓{{reset}} %s\n' 'proto generated'

# Regenerate and fail if the working tree drifted (CI gate)
[group('code generation')]
proto-check: proto
    @git diff --exit-code proto/ && printf '{{green}}✓{{reset}} %s\n' 'no proto drift'

# Install code-generation plugins into $(go env GOPATH)/bin
[group('code generation')]
tools:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Installing codegen plugins'
    @{{go}} install google.golang.org/protobuf/cmd/protoc-gen-go
    @{{go}} install connectrpc.com/connect/cmd/protoc-gen-connect-go
    @printf '{{green}}✓{{reset}} %s\n' 'plugins installed'

# ══════════════════════════════════════════════════════════════════════════════
# Go SDK (sdk/go)
# ══════════════════════════════════════════════════════════════════════════════
# The Go SDK is a self-contained module that vendors its own stubs; it builds OUTSIDE the server's
# module graph, so these recipes pin GOWORK=off and run from sdk/go.

# Regenerate the SDK's vendored stubs from proto/ (single source of truth)
[group('go sdk')]
sdk-proto:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Regenerating SDK stubs into sdk/go/internal/gen'
    @buf generate --template sdk/go/buf.gen.yaml && printf '{{green}}✓{{reset}} %s\n' 'SDK stubs regenerated'

# Build, vet and test the Go SDK module
[group('go sdk')]
sdk-test:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Testing the Go SDK'
    @cd sdk/go && GOWORK=off {{go}} vet ./... && GOWORK=off {{go}} test ./... && printf '{{green}}✓{{reset}} %s\n' 'SDK tests passed'

# Run the SDK quickstart against a node  (addr: host:port, default localhost:7800)
[group('go sdk')]
sdk-example addr="localhost:7800":
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Running the SDK quickstart against {{addr}}'
    @cd sdk/go && GOWORK=off {{go}} run ./examples/quickstart --addr {{addr}}

# ══════════════════════════════════════════════════════════════════════════════
# Cluster & images
# ══════════════════════════════════════════════════════════════════════════════

# Start the local 3-node docker compose cluster
[group('cluster & images')]
docker-up:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Starting 3-node docker cluster'
    @docker compose -f docker/docker-compose.yaml up -d --build
    @printf '{{green}}✓{{reset}} %s\n' 'cluster up — UI on http://localhost:7901'

# Tear down the docker compose cluster (and its volumes)
[group('cluster & images')]
docker-kill:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Tearing down the docker cluster'
    @docker compose -f docker/docker-compose.yaml down -v && printf '{{green}}✓{{reset}} %s\n' 'cluster removed'

# Build the scratch wavespan-node image (Apple container + CI buildx)
[group('cluster & images')]
image:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Building scratch image {{image}} for {{platforms}}'
    @docker buildx build --platform {{platforms}} -f docker/Dockerfile -t {{image}} --load ..

# ══════════════════════════════════════════════════════════════════════════════
# Apple `container` clusters
# ══════════════════════════════════════════════════════════════════════════════

# Build the node image with Apple `container` (prereq for container-up/single)
[group('apple container clusters')]
container-image:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Building the Apple container image'
    @./container/build.sh && printf '{{green}}✓{{reset}} %s\n' "image built — run 'just container-single' or 'just container-up'"

# Run a SINGLE-node cluster via Apple `container`
[group('apple container clusters')]
container-single:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Starting a single Apple-container node'
    @./container/up.sh 1
    @printf '{{green}}✓{{reset}} %s\n' '1 node up on network wavespan-dev'

# Run an N-node cluster via Apple `container`  (nodes: count, default 3)
[group('apple container clusters')]
container-up nodes="3":
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Starting a {{nodes}}-node Apple-container cluster'
    @./container/up.sh {{nodes}}
    @printf '{{green}}✓{{reset}} %s\n' '{{nodes}} node(s) up on network wavespan-dev'

# Tear down the Apple-container cluster  (nodes: pass the same count you brought up, default 3)
[group('apple container clusters')]
container-down nodes="3":
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Tearing down the Apple-container cluster ({{nodes}} node[s])'
    @./container/down.sh {{nodes}} && printf '{{green}}✓{{reset}} %s\n' 'cluster removed'

# ══════════════════════════════════════════════════════════════════════════════
# Housekeeping
# ══════════════════════════════════════════════════════════════════════════════

# Remove build artifacts
[group('housekeeping')]
clean:
    @printf '{{bold}}{{blue}}▸{{reset}} {{bold}}%s{{reset}}\n' 'Cleaning build artifacts'
    @rm -rf {{bin_dir}} && printf '{{green}}✓{{reset}} %s\n' 'removed {{bin_dir}}'
