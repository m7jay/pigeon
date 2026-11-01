BINARY     := bin/pigeon
PORTAL     := bin/consumer-portal
PRODUCER_PORTAL := bin/producer-portal
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)

TEST_DB_NAME ?= pigeon_test
PIGEON_TEST_DATABASE_URL ?=
PIGEON_TEST_REDIS_URL ?=
TEST_DATABASE_URL := $(if $(strip $(PIGEON_TEST_DATABASE_URL)),$(PIGEON_TEST_DATABASE_URL),postgres://pigeon_test:pigeon_test@127.0.0.1:55432/$(TEST_DB_NAME)?sslmode=disable)
TEST_REDIS_URL := $(if $(strip $(PIGEON_TEST_REDIS_URL)),$(PIGEON_TEST_REDIS_URL),redis://127.0.0.1:56379/0)
TEST_SERVICES := $(strip $(if $(strip $(PIGEON_TEST_DATABASE_URL)),,postgres-test) $(if $(strip $(PIGEON_TEST_REDIS_URL)),,redis-test))

.PHONY: build test unit-test test-integration loadgen-integration-test test-up test-down load-test-up load-test \
        vet lint run run-portal run-producer-portal compose-up compose-down \
        migrate-check clean

build: unit-test
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/pigeon
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(PRODUCER_PORTAL) ./cmd/producer-portal
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(PORTAL) ./cmd/consumer-portal

# The routine suite is self-contained. Database-backed acceptance tests skip
# unless `test-integration` supplies their explicit disposable backend.
test: unit-test

unit-test:
	go test -count=1 -p 1 ./...

# Run database-backed integration tests explicitly; this is not part of `test`.
test-integration: test-up
	@echo "==> go test -count=1 -p 1 ./... against $(TEST_DATABASE_URL)" | sed -E 's#(//[^:/@]+):[^@]*@#\1:***@#'
	PIGEON_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' PIGEON_TEST_REDIS_URL='$(TEST_REDIS_URL)' PIGEON_LOADGEN_DB='$(TEST_DATABASE_URL)' go test -count=1 -p 1 ./...

# Opt-in runtime test for the loadgen loop against a fake server. It sends a
# short synthetic request stream, so it is separate from ordinary unit tests.
loadgen-integration-test:
	go test -tags loadgen_integration -count=1 ./cmd/loadgen

# Start any missing disposable test services and wait for healthchecks. Developers
# who supplied both test DSNs can use their existing backend without Docker.
test-up:
	@if [ -n "$(TEST_SERVICES)" ]; then \
	command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { \
	  echo "make test-integration needs a test backend, and this box has no usable 'docker compose':"; \
	  echo "  with docker and the compose plugin, this target starts test.compose.yaml"; \
	  echo "  (postgres-test + redis-test) and waits for it; without them, point the suite at"; \
	  echo "  servers you already have:"; \
	  echo "      PIGEON_TEST_DATABASE_URL=postgres://user@host:5432/pigeon_test?sslmode=disable"; \
	  echo "      PIGEON_TEST_REDIS_URL=redis://host:6379/0"; \
	  exit 1; \
	}; \
	docker compose -f test.compose.yaml up -d --wait $(TEST_SERVICES); \
	fi

# Stop the isolated test and load-test containers. This file never controls the
# product stack in compose.yaml.
test-down:
	docker compose -f test.compose.yaml down

# Manual saturation ramp against the isolated test core. Starting rate and
# concurrency use resources visible to the host load generator.
LOAD_EVENT_TYPE ?= load.created
LOAD_TEST_BASE_URL ?= http://127.0.0.1:18080
LOAD_TEST_DATABASE_URL ?= postgres://pigeon_test:pigeon_test@127.0.0.1:55432/pigeon_test?sslmode=disable
LOAD_TEST_SINK_URL ?= http://sink-test:9001/hook
LOAD_START_RATE ?= 0
LOAD_MAX_RATE ?= 100000
LOAD_RATE_STEP ?= 1.5
LOAD_MAX_STAGES ?= 20
LOAD_REFINE_STAGES ?= 4
LOAD_SUBS ?= 10
LOAD_CONCURRENCY ?= 0
LOAD_WARMUP ?= 15s
LOAD_MEASURE ?= 45s
LOAD_EVALUATE ?= 15s
LOAD_SETTLE ?= 10s
load-test-up:
	docker compose -f test.compose.yaml up -d --build --wait core-test sink-test

load-test: load-test-up
	@set -eu; \
	seed_output=$$(go run ./cmd/loadgen seed -db '$(LOAD_TEST_DATABASE_URL)' \
	  -producer loadtest -event-type '$(LOAD_EVENT_TYPE)' -routing consumer \
	  -endpoints '$(LOAD_SUBS)' -url '$(LOAD_TEST_SINK_URL)'); \
	printf '%s\n' "$$seed_output" | sed '/^PIGEON_LOADGEN_KEY=/d'; \
	loadgen_key=$$(printf '%s\n' "$$seed_output" | sed -n 's/^PIGEON_LOADGEN_KEY=//p'); \
	test -n "$$loadgen_key" || { echo "loadgen seed did not return an API key"; exit 2; }; \
	PIGEON_LOADGEN_KEY="$$loadgen_key" PIGEON_LOADGEN_DB='$(LOAD_TEST_DATABASE_URL)' \
	  go run ./cmd/loadgen max -base '$(LOAD_TEST_BASE_URL)' \
	  -event-type '$(LOAD_EVENT_TYPE)' -consumer-ref load-consumer -start-rate '$(LOAD_START_RATE)' \
	  -max-rate '$(LOAD_MAX_RATE)' -step '$(LOAD_RATE_STEP)' \
	  -max-stages '$(LOAD_MAX_STAGES)' -refine-stages '$(LOAD_REFINE_STAGES)' -subs '$(LOAD_SUBS)' \
	  -concurrency '$(LOAD_CONCURRENCY)' -warmup '$(LOAD_WARMUP)' \
	  -measure '$(LOAD_MEASURE)' -evaluate '$(LOAD_EVALUATE)' -settle '$(LOAD_SETTLE)' \
	  -db '$(LOAD_TEST_DATABASE_URL)' -out load-test.json

vet:
	go vet ./...

lint: vet
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; ran vet only"

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

# Migrations are embedded and checksummed; a changed file that already ran is a
# hard error at boot, and this catches it before a deploy does.
migrate-check: test-integration

clean:
	rm -rf bin
