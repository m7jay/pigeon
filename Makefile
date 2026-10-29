BINARY     := bin/pigeon
PORTAL     := bin/consumer-portal
PRODUCER_PORTAL := bin/producer-portal
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)

.PHONY: build test test-integration vet lint run run-portal run-producer-portal compose-up compose-down migrate-check clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/pigeon
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(PRODUCER_PORTAL) ./cmd/producer-portal
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(PORTAL) ./cmd/consumer-portal

test:
	go test ./...

# AC-20's harness: needs a disposable Postgres. The tests skip without it, so
# `make test` stays green on a laptop and CI fails loudly if the DB is missing.
#
# -p 1 because the integration packages share one database.
test-integration:
	@test -n "$$PIGEON_TEST_DATABASE_URL" || (echo "set PIGEON_TEST_DATABASE_URL" && exit 1)
	go test -count=1 -p 1 ./...

vet:
	go vet ./...

lint: vet
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; ran vet only"

run:
	CGO_ENABLED=0 go run ./cmd/pigeon

# Run the consumer portal locally. It needs a reachable core and a session secret;
# sensible local defaults are used when the env is unset.
run-portal:
	CGO_ENABLED=0 PIGEON_CORE_API_URL=$${PIGEON_CORE_API_URL:-http://localhost:8080} PORTAL_SESSION_SECRET=$${PORTAL_SESSION_SECRET:-dev-insecure-secret} go run ./cmd/consumer-portal

# Run the producer portal locally. Same shape as run-portal: it needs a reachable
# core and a session secret; sensible local defaults are used when the env is unset.
run-producer-portal:
	CGO_ENABLED=0 PIGEON_CORE_API_URL=$${PIGEON_CORE_API_URL:-http://localhost:8080} PIGEON_PRODUCER_PORTAL_SESSION_SECRET=$${PIGEON_PRODUCER_PORTAL_SESSION_SECRET:-dev-insecure-secret} go run ./cmd/producer-portal

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

# Migrations are embedded and checksummed; a changed file that already ran is a
# hard error at boot, and this catches it before a deploy does.
migrate-check: test

clean:
	rm -rf bin
