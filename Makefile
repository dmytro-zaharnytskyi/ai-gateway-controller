# ai-gateway-controller Makefile
# Installs the praxis-extproc dataplane manifests. See DESIGN.md.

##@ General

.PHONY: help
help: ##	Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""
	@echo "\033[1mOptional build arguments\033[0m (e.g. \033[36mmake build GO_STRICTFIPS=false\033[0m):"
	@echo "  \033[36mGO_STRICTFIPS=false\033[0m  disable the strict FIPS runtime (on by default, matching maas-controller) for \033[36mbuild\033[0m / \033[36mrun\033[0m / \033[36mbuild-image\033[0m"

PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))

include tools.mk

BINARY_NAME := manager
BUILD_DIR := $(PROJECT_DIR)/bin

GOOS   ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
# On by default (matches the Dockerfile's own GOEXPERIMENT ARG default and
# maas-controller's convention); pass GO_STRICTFIPS=false to opt out for a
# faster local build/run loop.
GO_STRICTFIPS ?= true
VERSION ?= dev

CGO_ENABLED ?= 1

ifeq ($(GO_STRICTFIPS),true)
  GOEXPERIMENT ?= strictfipsruntime
endif

GO_ENV := GOOS=$(GOOS) GOARCH=$(GOARCH)
ifdef GOEXPERIMENT
  GO_ENV += GOEXPERIMENT=$(GOEXPERIMENT)
endif
ifdef CGO_ENABLED
  GO_ENV += CGO_ENABLED=$(CGO_ENABLED)
endif

##@ Development

.PHONY: build
build: tidy lint test binary ##	run full build: tidy, lint, test, binary

.PHONY: binary
binary: $(BUILD_DIR) ##	build manager binary to bin/manager (skip checks)
	$(GO_ENV) go build -ldflags="-X main.buildVersion=$(VERSION)" -o "$(BUILD_DIR)/$(BINARY_NAME)" ./cmd/manager

$(BUILD_DIR):
	mkdir -p "$(BUILD_DIR)"

.PHONY: run
run: binary ##	build and run manager locally (see cmd/manager/main.go for flags)
	"$(BUILD_DIR)/$(BINARY_NAME)"

TEST_FLAGS ?= -race -coverprofile=coverage.out
.PHONY: test
test: test-unit test-envtest ##	run unit and control-plane tests

.PHONY: test-unit
test-unit: tidy ##	run unit tests with coverage
	go test $(TEST_FLAGS) ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Test coverage report generated: $(abspath coverage.html)"

ENVTEST_K8S_VERSION ?= 1.35.0
ENVTEST_FLAGS ?= -race -count=1 -timeout=10m
.PHONY: test-envtest
test-envtest: $(SETUP_ENVTEST) ##	run controller scenarios against a local API server
	@set -eu; \
	assets="$${KUBEBUILDER_ASSETS:-}"; \
	if [ -z "$$assets" ]; then \
		assets="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN)/envtest -p path)"; \
	fi; \
	KUBEBUILDER_ASSETS="$$assets" go test -tags envtest $(ENVTEST_FLAGS) ./test/envtest/...

.PHONY: tidy
tidy: ##	go mod tidy
	go mod tidy

.PHONY: manifests
manifests: $(CONTROLLER_GEN) ##	generate CRDs and deepcopy implementations
	$(CONTROLLER_GEN) object paths=./api/...
	# Routed by API group: this controller's own group ships in the self bundle
	# it installs; the shared inference group is installed by maas-controller.
	$(CONTROLLER_GEN) crd paths=./api/aigateway/... \
		output:crd:artifacts:config=config/self/crd/bases
	$(CONTROLLER_GEN) crd paths=./api/inference/... \
		output:crd:artifacts:config=config/crd/bases

.PHONY: vet
vet: ##	go vet ./...
	go vet ./...

LINT_FIX ?= false
.PHONY: lint
lint: $(GOLANGCI_LINT) vet ##	run golangci-lint (use LINT_FIX=true to fix lint issues)
ifeq ($(LINT_FIX),true)
	"$(GOLANGCI_LINT)" fmt
	"$(GOLANGCI_LINT)" run --fix
else
	"$(GOLANGCI_LINT)" fmt --diff
	"$(GOLANGCI_LINT)" run
endif

.PHONY: get-manifests
get-manifests: ##	vendor the praxis-extproc kustomize overlay (see hack/scripts/get-manifests.sh)
	./hack/scripts/get-manifests.sh

# Report-only Go 1.26+ modernizer suggestions (go tool fix's mapsloop, rangeint,
# omitzero, etc.) — deliberately not part of "build"/"lint"/CI; golangci-lint's
# "modernize" linter is disabled in .golangci.yml for the same reason.
MODERNIZE_FIX ?= false
.PHONY: modernize
modernize: ##	report (or, with MODERNIZE_FIX=true, apply) go fix modernizer suggestions
ifeq ($(MODERNIZE_FIX),true)
	go fix ./...
else
	go fix -diff ./...
endif

##@ Deployment

# Standalone install for local testing until ai-gateway-operator vendors and
# deploys this repo's config/self/default as a sibling of maas-controller (see
# DESIGN.md, "Operator changes" — that integration is not done yet).
.PHONY: install
install: ##	apply config/self/default (SA, RBAC, Deployment) — namespace opendatahub
	kubectl apply -k config/self/default

.PHONY: uninstall
uninstall: ##	delete config/self/default resources
	kubectl delete -k config/self/default --ignore-not-found

##@ Container image

CONTAINER_ENGINE ?= podman
REPO ?= quay.io/opendatahub/odh-ai-gateway-controller
TAG ?= latest
FULL_IMAGE ?= $(REPO):$(TAG)

DOCKER_BUILD_ARGS := --build-arg CGO_ENABLED=$(CGO_ENABLED)
ifdef GOEXPERIMENT
  DOCKER_BUILD_ARGS += --build-arg GOEXPERIMENT=$(GOEXPERIMENT)
endif

.PHONY: build-image
build-image: get-manifests ##	build container image (use REPO= and TAG= to specify image; vendors manifests first)
	@echo "Building container image $(FULL_IMAGE)..."
	$(CONTAINER_ENGINE) build $(DOCKER_BUILD_ARGS) -f Dockerfile -t "$(FULL_IMAGE)" .
	@echo "Container image $(FULL_IMAGE) built successfully"

.PHONY: push-image
push-image: ##	push container image (use REPO= and TAG= to specify image)
	@$(CONTAINER_ENGINE) push "$(FULL_IMAGE)"

.PHONY: build-push-image
build-push-image: build-image push-image ##	build and push container image
