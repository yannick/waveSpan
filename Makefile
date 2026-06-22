# WaveSpan build system. Targets shell out to standard tools; keep them thin.
# See design/24_container_dev_and_testing.md for the container/image contract.
# Run `make` or `make help` for a colorized list of targets.

GO            ?= go
BIN_DIR       ?= $(CURDIR)/bin
CMDS          := wavespan-node wavespan-gateway wavespanctl
PLATFORMS     ?= linux/$(shell go env GOARCH)
IMAGE         ?= wavespan-node:dev
CGO_ENABLED   ?= 0

# --- colors (disabled when not a TTY or NO_COLOR is set) ---
ifeq ($(shell test -t 1 && test -z "$$NO_COLOR" && echo y),y)
  C_RESET := \033[0m
  C_BOLD  := \033[1m
  C_BLUE  := \033[34m
  C_CYAN  := \033[36m
  C_GREEN := \033[32m
  C_YELLOW:= \033[33m
  C_DIM   := \033[2m
endif

# print a step banner: $(call step,message)
define step
	printf "$(C_BOLD)$(C_BLUE)▸$(C_RESET) $(C_BOLD)%s$(C_RESET)\n" "$(1)"
endef
define ok
	printf "$(C_GREEN)✓$(C_RESET) %s\n" "$(1)"
endef

.DEFAULT_GOAL := help
.PHONY: help all build test test-race test-integration lint proto proto-check image tools docker-up docker-kill clean

## help: show this help
help:
	@printf "$(C_BOLD)WaveSpan$(C_RESET) $(C_DIM)— make targets$(C_RESET)\n\n"
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/## //' | \
		awk -F': ' '{ printf "  $(C_CYAN)%-16s$(C_RESET) %s\n", $$1, $$2 }'
	@printf "\n$(C_DIM)binaries build into $(C_RESET)$(C_YELLOW)$(BIN_DIR)$(C_RESET)\n"

## all: build everything
all: build

## build: compile all cmd/ binaries into ./bin (static, no cgo)
build:
	@$(call step,Building binaries into $(BIN_DIR))
	@mkdir -p $(BIN_DIR)
	@for c in $(CMDS); do \
		printf "  $(C_DIM)compiling$(C_RESET) %s\n" "$$c"; \
		CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(BIN_DIR)/$$c ./cmd/$$c || exit 1; \
	done
	@$(call ok,built $(words $(CMDS)) binaries in $(BIN_DIR))

## test: run the unit test suite
test:
	@$(call step,Running unit tests)
	@$(GO) test ./... && $(call ok,unit tests passed)

## test-race: race-enabled run (cgo required, local only — not the scratch build)
test-race:
	@$(call step,Running unit tests with the race detector)
	@CGO_ENABLED=1 $(GO) test -race ./... && $(call ok,race tests passed)

## test-integration: docker-based integration tests (requires Docker + ../wavesdb)
test-integration:
	@$(call step,Building image + running docker integration tests)
	@docker compose -f docker/docker-compose.yaml build
	@$(GO) test -tags integration -timeout 600s ./tests/integration/...

## lint: golangci-lint
lint:
	@$(call step,Linting)
	@golangci-lint run && $(call ok,lint clean)

## proto: regenerate Go messages + Connect stubs via buf
proto:
	@$(call step,Generating protobuf + Connect stubs)
	@buf generate && $(call ok,proto generated)

## proto-check: regenerate and fail if the working tree drifted (CI proto gate)
proto-check: proto
	@git diff --exit-code proto/ && $(call ok,no proto drift)

## image: build the scratch wavespan-node image (Apple container + CI buildx use this)
image:
	@$(call step,Building scratch image $(IMAGE) for $(PLATFORMS))
	@docker buildx build --platform $(PLATFORMS) -f docker/Dockerfile -t $(IMAGE) --load ..

## tools: install code-generation plugins
tools:
	@$(call step,Installing codegen plugins into $(shell go env GOPATH)/bin)
	@$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go
	@$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go
	@$(call ok,plugins installed)

## docker-up: start the local 3-node compose cluster
docker-up:
	@$(call step,Starting 3-node docker cluster)
	@docker compose -f docker/docker-compose.yaml up -d --build

## docker-kill: tear down the local compose cluster
docker-kill:
	@docker compose -f docker/docker-compose.yaml down -v

## clean: remove build artifacts
clean:
	@rm -rf $(BIN_DIR) && $(call ok,removed $(BIN_DIR))
