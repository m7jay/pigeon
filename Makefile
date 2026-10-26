BINARY  := bin/pigeon
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test test-integration vet lint run compose-up compose-down migrate-check clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/pigeon

test:
	go test ./...

# AC-20's harness: needs a disposable Postgres. The tests skip without it, so
# `make test` stays green on a laptop and CI fails loudly if the DB is missing.
test-integration:
	@test -n "$$PIGEON_TEST_DATABASE_URL" || (echo "set PIGEON_TEST_DATABASE_URL" && exit 1)
	go test -count=1 ./...

vet:
	go vet ./...

lint: vet
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; ran vet only"

run:
	go run ./cmd/pigeon

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

# Migrations are embedded and checksummed; a changed file that already ran is a
# hard error at boot, and this catches it before a deploy does.
migrate-check: test

clean:
	rm -rf bin
