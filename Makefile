# Heavily inspired by Lighthouse: https://github.com/sigp/lighthouse/blob/stable/Makefile
# and Reth: https://github.com/paradigmxyz/reth/blob/main/Makefile
.DEFAULT_GOAL := help

VERSION := $(shell git describe --tags --always --dirty="-dev")

##@ Help

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: v
v: ## Show the version
	@echo "Version: ${VERSION}"

##@ Build

.PHONY: clean
clean: ## Clean the build directory
	rm -rf build/

.PHONY: build
build: ## Build the HTTP server
	@mkdir -p ./build
	go build -trimpath -ldflags "-X github.com/flashbots/tdx-orderflow-proxy/common.Version=${VERSION}" -v -o ./build/sender-proxy cmd/sender-proxy/main.go
	go build -trimpath -ldflags "-X github.com/flashbots/tdx-orderflow-proxy/common.Version=${VERSION}" -v -o ./build/receiver-proxy cmd/receiver-proxy/main.go
	go build -trimpath -ldflags "-X github.com/flashbots/tdx-orderflow-proxy/common.Version=${VERSION}" -v -o ./build/test-orderflow-sender cmd/test-tx-sender/main.go
	go build -trimpath -ldflags "-X github.com/flashbots/tdx-orderflow-proxy/common.Version=${VERSION}" -v -o ./build/test-e2e-latency cmd/test-e2e-latency/main.go

.PHONY: build-receiver-proxy
build-receiver-proxy: ## Build only the receiver-proxy
	@mkdir -p ./build
	CGO_ENABLED=0 GOOS=linux go build \
		-trimpath \
		-ldflags "-s -w -buildid= -X github.com/flashbots/tdx-orderflow-proxy/common.Version=${VERSION}" \
		-v -o ./build/receiver-proxy \
		cmd/receiver-proxy/main.go

##@ Test & Development

.PHONY: test
test: ## Run tests
	go test ./...

.PHONY: test-race
test-race: ## Run tests with race detector
	go test -race ./...

.PHONY: lint
lint: ## Run linters
	gofmt -d -s .
	gofumpt -d -extra .
	go vet ./...
	staticcheck ./...
	golangci-lint run
	# nilaway ./...

.PHONY: fmt
fmt: ## Format the code
	gofmt -s -w .
	gci write .
	gofumpt -w -extra .
	go mod tidy

.PHONY: gofumpt
gofumpt: ## Run gofumpt
	gofumpt -l -w -extra .

.PHONY: lt
lt: lint test ## Run linters and tests

.PHONY: cover
cover: ## Run tests with coverage
	go test -coverprofile=/tmp/go-sim-lb.cover.tmp ./...
	go tool cover -func /tmp/go-sim-lb.cover.tmp
	unlink /tmp/go-sim-lb.cover.tmp

.PHONY: cover-html
cover-html: ## Run tests with coverage and open the HTML report
	go test -coverprofile=/tmp/go-sim-lb.cover.tmp ./...
	go tool cover -html=/tmp/go-sim-lb.cover.tmp
	unlink /tmp/go-sim-lb.cover.tmp

.PHONY: docker
docker:
	DOCKER_BUILDKIT=1 docker build \
		--platform linux/amd64 \
		--build-arg VERSION=${VERSION} \
		--file Dockerfile \
		--tag tdx-orderflow-proxy-sender-proxy \
	.

	DOCKER_BUILDKIT=1 docker build \
		--platform linux/amd64 \
		--build-arg VERSION=${VERSION} \
		--file receiver.dockerfile \
		--tag tdx-orderflow-proxy-receiver-proxy \
	.
