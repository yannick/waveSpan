# WaveSpan build system. Targets shell out to standard tools; keep them thin.
# See design/24_container_dev_and_testing.md for the container/image contract.
# Run `make` or `make help` for a colorized, grouped list of targets.

GO            ?= go
BIN_DIR       ?= $(CURDIR)/bin
CMDS          := wavespan-node wavespan-gateway wavespanctl wavespan-bench wavespan-profile wavespan-benchui
PLATFORMS     ?= linux/$(shell go env GOARCH)
IMAGE         ?= wavespan-node:dev
CGO_ENABLED   ?= 0
N             ?= 3   # node count for multi-node Apple `container` clusters (override: make container-up N=5)

# --- colors ---
# On: NO_COLOR unset AND (MAKE_TERMOUT says stdout is a terminal [GNU make >= 4.1], or — on macOS's
# make 3.81, which lacks MAKE_TERMOUT — TERM is not "dumb"). `test -t 1` can't be used here: inside
# $(shell ...) fd 1 is a pipe to make, never the terminal, so it always reported "no color".
COLOR := $(if $(NO_COLOR),,$(if $(MAKE_TERMOUT),1,$(if $(filter dumb,$(TERM)),,1)))
ifeq ($(COLOR),1)
  C_RESET  := \033[0m
  C_BOLD   := \033[1m
  C_DIM    := \033[2m
  C_RED    := \033[31m
  C_GREEN  := \033[32m
  C_YELLOW := \033[33m
  C_BLUE   := \033[34m
  C_MAGENTA:= \033[35m
  C_CYAN   := \033[36m
endif

# step/ok/warn — colored recipe banners: $(call step,message)
define step
	printf "$(C_BOLD)$(C_BLUE)▸$(C_RESET) $(C_BOLD)%s$(C_RESET)\n" "$(1)"
endef
define ok
	printf "$(C_GREEN)✓$(C_RESET) %s\n" "$(1)"
endef
define warn
	printf "$(C_YELLOW)!$(C_RESET) %s\n" "$(1)"
endef

.DEFAULT_GOAL := help
.PHONY: help all build ui ui-dev docs-site docs-site-serve docs-deploy test test-race test-integration lint \
        proto proto-check tools sdk-proto sdk-test sdk-example \
        docker-up docker-kill image \
        container-image container-single container-up container-down clean

# The Go SDK is a self-contained module (sdk/go) that vendors its own stubs; it builds OUTSIDE the
# server's module graph, so SDK targets pin GOWORK=off and run from the module directory.
SDK_DIR := sdk/go

# ======================================================================
##@ General
# ======================================================================

help: ## Show this grouped, colorized target list
	@printf "$(C_BOLD)WaveSpan$(C_RESET) $(C_DIM)— make targets$(C_RESET)\n"
	@awk 'BEGIN { \
			FS = ":.*?## "; \
			n = split("$(C_BLUE)|$(C_MAGENTA)|$(C_GREEN)|$(C_YELLOW)|$(C_CYAN)", pal, "|"); \
		} \
		/^##@/ { \
			printf "\n$(C_BOLD)%s%s$(C_RESET)\n", pal[(h % n) + 1], substr($$0, 5); h++; next \
		} \
		/^[a-zA-Z0-9_-]+:.*?## / { \
			printf "  $(C_CYAN)%-18s$(C_RESET) %s\n", $$1, $$2 \
		}' $(MAKEFILE_LIST)
	@printf "\n$(C_DIM)binaries build into$(C_RESET) $(C_YELLOW)$(BIN_DIR)$(C_RESET)$(C_DIM); set$(C_RESET) $(C_YELLOW)NO_COLOR=1$(C_RESET) $(C_DIM)to disable color$(C_RESET)\n"

all: build ## Build everything (alias for build)

# ======================================================================
##@ Build & UI
# ======================================================================

build: ## Compile all cmd/ binaries into ./bin (static, no cgo)
	@$(call step,Building binaries into $(BIN_DIR))
	@mkdir -p $(BIN_DIR)
	@for c in $(CMDS); do \
		printf "  $(C_DIM)compiling$(C_RESET) $(C_CYAN)%s$(C_RESET)\n" "$$c"; \
		CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(BIN_DIR)/$$c ./cmd/$$c || exit 1; \
	done
	@$(call ok,built $(words $(CMDS)) binaries in $(BIN_DIR))

ui: ## Build the embedded SPA into internal/ui/dist
	@$(call step,Building the embedded SPA)
	@cd ui && npm ci && npm run build
	@$(call ok,SPA built into internal/ui/dist)

ui-dev: ## Run the Vite dev server (pair with WAVESPAN_UI_DEV=1 on the node)
	@$(call step,Starting the Vite dev server)
	@cd ui && npm run dev

docs-site: ## Export the docs as a standalone static website into ./docs-site (deploy anywhere)
	@$(call step,Building the static documentation site)
	@cd ui && [ -d node_modules ] || npm ci
	@cd ui && npm run build:docs
	@$(call ok,static site in ./docs-site — deploy the folder, or 'make docs-site-serve' to preview)

docs-site-serve: docs-site ## Build then locally preview the static docs site (http://localhost:4173)
	@$(call step,Serving ./docs-site at http://localhost:4173)
	@cd ui && npm run preview:docs

docs-deploy: docs-site ## Build + publish ./docs-site to the gh-pages branch (GitHub Pages)
	@$(call step,Publishing the docs site to the gh-pages branch)
	@./scripts/deploy-docs-site.sh

# ======================================================================
##@ Test & lint
# ======================================================================

test: ## Run the unit test suite
	@$(call step,Running unit tests)
	@$(GO) test ./... && $(call ok,unit tests passed)

test-race: ## Race-enabled run (cgo required, local only)
	@$(call step,Running unit tests with the race detector)
	@CGO_ENABLED=1 $(GO) test -race ./... && $(call ok,race tests passed)

test-integration: ## Docker-based integration tests (requires Docker + ../wavesdb)
	@$(call step,Building image + running docker integration tests)
	@docker compose -f docker/docker-compose.yaml build
	@$(GO) test -tags integration -timeout 600s ./tests/integration/...

lint: ## Run golangci-lint
	@$(call step,Linting)
	@golangci-lint run && $(call ok,lint clean)

# ======================================================================
##@ Code generation
# ======================================================================

proto: ## Regenerate Go messages + Connect stubs via buf
	@$(call step,Generating protobuf + Connect stubs)
	@buf generate && $(call ok,proto generated)

proto-check: proto ## Regenerate and fail if the working tree drifted (CI gate)
	@git diff --exit-code proto/ && $(call ok,no proto drift)

tools: ## Install code-generation plugins into $(GOPATH)/bin
	@$(call step,Installing codegen plugins into $(shell go env GOPATH)/bin)
	@$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go
	@$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go
	@$(call ok,plugins installed)

# ======================================================================
##@ Go SDK (sdk/go)
# ======================================================================

sdk-proto: ## Regenerate the SDK's vendored stubs from proto/ (single source of truth)
	@$(call step,Regenerating SDK stubs into $(SDK_DIR)/internal/gen)
	@buf generate --template $(SDK_DIR)/buf.gen.yaml && $(call ok,SDK stubs regenerated)

sdk-test: ## Build, vet and test the Go SDK module
	@$(call step,Testing the Go SDK)
	@cd $(SDK_DIR) && GOWORK=off $(GO) vet ./... && GOWORK=off $(GO) test ./... && $(call ok,SDK tests passed)

sdk-example: ## Run the SDK quickstart against a node (ADDR=host:port, default localhost:7800)
	@$(call step,Running the SDK quickstart against $(or $(ADDR),localhost:7800))
	@cd $(SDK_DIR) && GOWORK=off $(GO) run ./examples/quickstart --addr $(or $(ADDR),localhost:7800)

# ======================================================================
##@ Cluster & images
# ======================================================================

docker-up: ## Start the local 3-node compose cluster
	@$(call step,Starting 3-node docker cluster)
	@docker compose -f docker/docker-compose.yaml up -d --build
	@$(call ok,cluster up — UI on http://localhost:7901)

docker-kill: ## Tear down the local compose cluster (and its volumes)
	@$(call step,Tearing down the docker cluster)
	@docker compose -f docker/docker-compose.yaml down -v && $(call ok,cluster removed)

image: ## Build the scratch wavespan-node image (Apple container + CI buildx)
	@$(call step,Building scratch image $(IMAGE) for $(PLATFORMS))
	@docker buildx build --platform $(PLATFORMS) -f docker/Dockerfile -t $(IMAGE) --load ..

# ======================================================================
##@ Apple `container` clusters
# ======================================================================

container-image: ## Build the node image with Apple `container` (prereq for container-up/single)
	@$(call step,Building the Apple container image)
	@./container/build.sh && $(call ok,image built — run 'make container-single' or 'make container-up')

container-single: ## Run a SINGLE-node cluster via Apple `container`
	@$(call step,Starting a single Apple-container node)
	@./container/up.sh 1
	@$(call ok,1 node up on network wavespan-dev)

container-up: ## Run an N-node cluster via Apple `container` (default 3; override: make container-up N=5)
	@$(call step,Starting a $(N)-node Apple-container cluster)
	@./container/up.sh $(N)
	@$(call ok,$(N) nodes up on network wavespan-dev)

container-down: ## Tear down the Apple-container cluster (match the count: make container-down N=5)
	@$(call step,Tearing down the Apple-container cluster ($(N) node[s]))
	@./container/down.sh $(N) && $(call ok,cluster removed)

# ======================================================================
##@ Housekeeping
# ======================================================================

clean: ## Remove build artifacts
	@$(call step,Cleaning build artifacts)
	@rm -rf $(BIN_DIR) && $(call ok,removed $(BIN_DIR))
