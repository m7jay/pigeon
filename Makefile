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

# The local gate: the whole suite, as CI runs it, minus -race and -v. It is
# DB-backed, so it NEEDS a disposable Postgres - the packages do not all skip
# without one any more:
#
# - PIGEON_TEST_DATABASE_URL is required, not optional. internal/httpapi's
#   harness refuses to skip ("refusing to skip, the AC-15/AC-20 suite must run")
#   by design since S0, so an unset DSN reds its 60 acceptance tests at 0.00 s
#   and reads like a mass regression. The guard below says so in one line
#   instead, before anything is built.
# - -p 1 because every DB-backed package in this tree shares that one database:
#   in parallel, one package's fixtures truncate the tables under another - store
#   and relay run together red 25 store tests on `tuple concurrently updated`
#   (SQLSTATE XX000) - and internal/delivery's test binary has been seen to sit
#   past Go's 10 m default until it is killed. ci.yml's test step runs -p 1 for
#   the same reason.
# - -count=1 because the test cache cannot see the database: a PASS cached
#   before a migration or a fixture change is not evidence.
#
# The database's name must contain "test": internal/httpapi and internal/relay
# refuse any other name, and the other DB-backed harnesses - store, delivery,
# identity, db, faults - have no such check: they migrate and truncate whatever
# database the DSN names. Point the DSN at a disposable database you do not care
# about. PIGEON_TEST_REDIS_URL is optional here (unlike CI, which sets it): the
# Redis-backed packages fall back to an in-process miniredis when it is unset.
test:
	@test -n "$$PIGEON_TEST_DATABASE_URL" || { \
	  echo "PIGEON_TEST_DATABASE_URL is not set, so make test cannot pass:"; \
	  echo "the AC-15/AC-20 harness (internal/httpapi) refuses to skip without a"; \
	  echo "disposable Postgres, so an unset DSN reds its acceptance suite at 0.00s."; \
	  echo "Point it at one whose name contains \"test\", e.g."; \
	  echo "  PIGEON_TEST_DATABASE_URL=postgres://user@host:5432/pigeon_test?sslmode=disable"; \
	  exit 1; \
	}
	go test -count=1 -p 1 ./...

# The same DB-backed suite under its older name, so notes that name it keep
# working. It is exactly `test`: PIGEON_TEST_DATABASE_URL is just as required.
test-integration: test

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
# hard error at boot, and this catches it before a deploy does. It is `test`, so
# it needs the same disposable Postgres that target requires.
migrate-check: test

clean:
	rm -rf bin
