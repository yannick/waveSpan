# WaveSpan build system. Targets shell out to standard tools; keep them thin.
# See design/24_container_dev_and_testing.md for the container/image contract.

GO            ?= go
BIN_DIR       ?= bin
CMDS          := wavespan-node wavespan-gateway wavespanctl
PLATFORMS     ?= linux/$(shell go env GOARCH)
IMAGE         ?= wavespan-node:dev
CGO_ENABLED   ?= 0

.PHONY: all build test lint proto image clean tools docker-up docker-kill

all: build

## build: compile all cmd/ binaries into bin/ (static, no cgo)
build:
	@mkdir -p $(BIN_DIR)
	@for c in $(CMDS); do \
		echo "build $$c"; \
		CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(BIN_DIR)/$$c ./cmd/$$c || exit 1; \
	done

## test: run the full unit test suite with the race detector
test:
	$(GO) test ./...

## test-race: race-enabled run (cgo required, local only — not the scratch build)
test-race:
	CGO_ENABLED=1 $(GO) test -race ./...

## lint: golangci-lint
lint:
	golangci-lint run

## proto: regenerate Go messages + Connect stubs via buf
proto:
	buf generate

## proto-check: regenerate and fail if the working tree drifted (CI proto gate)
proto-check: proto
	git diff --exit-code proto/

# The build context is the parent directory because go.mod has `replace wavesdb => ../wavesdb`;
# both sibling modules must be visible to the build stage.
## image: build the scratch wavespan-node image (single target for Apple container + CI buildx)
image:
	docker buildx build --platform $(PLATFORMS) -f docker/Dockerfile -t $(IMAGE) --load ..

## tools: install code-generation plugins pinned via tools.go
tools:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go

## docker-up / docker-kill: local compose cluster (used from M2 onward)
docker-up:
	docker compose -f docker/docker-compose.yaml up -d --build

docker-kill:
	docker compose -f docker/docker-compose.yaml down -v

clean:
	rm -rf $(BIN_DIR)
