# reitbrazil-sync — data pipeline for the reitbrazil MCP server

BIN_DIR      ?= ./bin
OUT_DIR      ?= ./out
CTL_BIN      := $(BIN_DIR)/reitbrazilctl
DB_PATH      ?= $(OUT_DIR)/reitbrazil.db

GO           ?= go
GOFLAGS      ?=
LDFLAGS      ?= -s -w
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

DOCKER_IMAGE ?= southamerica-east1-docker.pkg.dev/reitbrazil/reitbrazilctl/reitbrazilctl
DOCKER_TAG   ?= $(VERSION)

.PHONY: all build run-daily run-monthly test test-short test-integration \
        lint vet fmt tidy clean doctor export docker-build docker-push \
        tf-plan tf-apply help

all: build

## build: compile reitbrazilctl
build:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS) -X main.version=$(VERSION)" \
		-o $(CTL_BIN) ./cmd/reitbrazilctl

## run-daily: run the daily pipeline locally
run-daily: build
	$(CTL_BIN) run daily

## run-monthly: run the monthly pipeline locally
run-monthly: build
	$(CTL_BIN) run monthly

## doctor: run preflight checks
doctor: build
	$(CTL_BIN) doctor

## export: regenerate the SQLite export from canon tables
export: build
	@mkdir -p $(OUT_DIR)
	$(CTL_BIN) export sqlite --output $(DB_PATH)

## test: run all tests with race detector (unit + contract)
test:
	$(GO) test -race ./...

## test-short: unit tests only (skip heavy integration)
test-short:
	$(GO) test -short ./...

## test-integration: integration tests (require live GCP creds)
test-integration:
	$(GO) test -race -tags=integration ./...

## vet: go vet
vet:
	$(GO) vet ./...

## fmt: format code
fmt:
	$(GO) fmt ./...

## tidy: tidy go.mod
tidy:
	$(GO) mod tidy

## lint: golangci-lint if installed, else go vet
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, running go vet instead"; \
		$(GO) vet ./...; \
	fi

## clean: wipe build artifacts and generated SQLite
clean:
	rm -rf $(BIN_DIR) $(OUT_DIR)

## docker-build: build the Cloud Run Job image
docker-build:
	docker build -f deploy/Dockerfile -t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):latest .

## docker-push: push image to Artifact Registry
docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

## tf-plan: terraform plan
tf-plan:
	cd deploy/terraform && terraform plan -var="project_id=reitbrazil"

## tf-apply: terraform apply
tf-apply:
	cd deploy/terraform && terraform apply -var="project_id=reitbrazil"

## help: list available targets
help:
	@awk 'BEGIN {FS = ":.*?## "} /^##/ {sub(/^## /, "", $$0); print}' $(MAKEFILE_LIST)
