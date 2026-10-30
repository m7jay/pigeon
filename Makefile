BINARY     := bin/pigeon
PORTAL     := bin/consumer-portal
PRODUCER_PORTAL := bin/producer-portal
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)

# ---- The test backend -------------------------------------------------------
#
# `make test` runs the whole DB-backed suite against a disposable Postgres and
# Redis, and needs nothing exported: it brings up compose.yaml's `test` profile
# (postgres-test + redis-test, published on the ports below), waits for both
# healthchecks, creates the database the DSN names if it is not there yet, and
# runs the suite. `make test-down` stops the two containers again; neither target
# touches the product stack.
#
# The ports are the product stack's own (5432, 6379) with a 5 in front, so the
# test pair runs beside a `docker compose up` of the product instead of competing
# with it for a host port.
#
# A developer who already has a Postgres and a Redis - service containers,
# another compose project, a server in the next room - sets *both* DSNs and
# nothing here needs docker:
#
#   PIGEON_TEST_DATABASE_URL=postgres://user@host:5432/pigeon_test?sslmode=disable \
#   PIGEON_TEST_REDIS_URL=redis://host:6379/0 make test
#
# Setting one of the two is not half a backend: `test-env` starts the profile for
# the half that is unset, and only that half, and uses the DSN you gave for the
# other. So a developer whose own Postgres already listens on the port the test
# profile publishes is fine - set that DSN and compose starts only redis-test.
TEST_DB_NAME    ?= pigeon_test
TEST_PG_PORT    ?= 55432
TEST_REDIS_PORT ?= 56379

# The DSNs are the developer's where they set them and the test profile's where
# they did not. make reads them from the environment itself, so nothing has to be
# exported for the suite to see them.
PIGEON_TEST_DATABASE_URL ?=
PIGEON_TEST_REDIS_URL    ?=

COMPOSE_TEST_DATABASE_URL ?= postgres://pigeon_test:pigeon_test@127.0.0.1:$(TEST_PG_PORT)/$(TEST_DB_NAME)?sslmode=disable
COMPOSE_TEST_REDIS_URL    ?= redis://127.0.0.1:$(TEST_REDIS_PORT)/0

TEST_DATABASE_URL := $(if $(strip $(PIGEON_TEST_DATABASE_URL)),$(PIGEON_TEST_DATABASE_URL),$(COMPOSE_TEST_DATABASE_URL))
TEST_REDIS_URL    := $(if $(strip $(PIGEON_TEST_REDIS_URL)),$(PIGEON_TEST_REDIS_URL),$(COMPOSE_TEST_REDIS_URL))

# The services the target has to start, which is exactly the half of the backend
# the developer did not provide. Empty (both DSNs set) means it starts nothing and
# needs no docker at all.
TEST_SERVICES := $(strip $(if $(strip $(PIGEON_TEST_DATABASE_URL)),,postgres-test) $(if $(strip $(PIGEON_TEST_REDIS_URL)),,redis-test))

.PHONY: build test test-integration test-env test-up test-down create-test-db \
        vet lint run run-portal run-producer-portal compose-up compose-down \
        migrate-check clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/pigeon
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(PRODUCER_PORTAL) ./cmd/producer-portal
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(PORTAL) ./cmd/consumer-portal

# The local gate: the whole suite, as CI runs it, minus -race and -v. It is
# DB-backed, so it NEEDS a disposable Postgres; `test-env` is what gives it one:
#
# - the database comes from PIGEON_TEST_DATABASE_URL. Left unset, `test-env`
#   starts compose.yaml's `test` profile and the DSN is the address that profile
#   publishes, so a clean checkout needs no exported variable and no second
#   terminal. It is not optional in the sense that matters: internal/httpapi's
#   harness refuses to skip ("refusing to skip, the AC-15/AC-20 suite must run")
#   by design since S0, so a database that is not reachable reds its 60
#   acceptance tests at 0.00 s and reads like a mass regression.
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
# Redis-backed packages fall back to an in-process miniredis when it is unset, so
# `test-env` sets it rather than leaving a run that proves nothing about Redis.
test: test-env
	@echo "==> go test -count=1 -p 1 ./... against $(TEST_DATABASE_URL)" | sed -E 's#(//[^:/@]+):[^@]*@#\1:***@#'
	@export PIGEON_TEST_DATABASE_URL='$(TEST_DATABASE_URL)'; \
	 export PIGEON_TEST_REDIS_URL='$(TEST_REDIS_URL)'; \
	 go test -count=1 -p 1 ./...

# The test backend without the suite: start what is missing, wait for it, create
# the database if it is not there. A target of its own so a developer iterating on
# one package pays for the compose round trip once, and so `make test` is setup
# plus the suite rather than one recipe with the setup hidden inside it.
test-env: $(if $(TEST_SERVICES),test-up) create-test-db

# Start compose.yaml's `test` profile - or the half of it the developer did not
# provide - and block until those containers report healthy. `--wait` is the
# readiness check: a container that never becomes healthy fails here, with its own
# healthcheck output, instead of in whichever test reaches it first. `up -d` on a
# container that is already running is a no-op, so this is safe to re-run.
test-up:
	@command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { \
	  echo "make test needs a test backend, and this box has no usable 'docker compose':"; \
	  echo "  with docker and the compose plugin, this target starts compose.yaml's 'test' profile"; \
	  echo "  (postgres-test + redis-test) and waits for it; without them, point the suite at"; \
	  echo "  servers you already have:"; \
	  echo "      PIGEON_TEST_DATABASE_URL=postgres://user@host:5432/pigeon_test?sslmode=disable"; \
	  echo "      PIGEON_TEST_REDIS_URL=redis://host:6379/0"; \
	  exit 1; \
	}
	docker compose --profile test up -d --wait $(TEST_SERVICES)

# Stop the test containers. Both are named on purpose: `docker compose down` takes
# the product stack with it, and so does a `rm` with no service argument.
test-down:
	docker compose --profile test rm -s -f postgres-test redis-test

# Create the database the DSN names, if it is not there. Idempotent: the second
# run reports that it exists and changes nothing, so `make test` is safe to
# re-run. It assumes no client and no admin connection beyond the DSN the
# developer already gave - cmd/dbcreate opens the DSN's own server as the DSN's
# own user, on the `postgres` maintenance database, and touches no other database
# - so psql does not have to be installed and a DSN for a disposable database is
# all it takes. The DSN arrives over stdin rather than argv, so a password in it
# stays out of the process list.
create-test-db:
	@printf '%s' '$(TEST_DATABASE_URL)' | go run ./cmd/dbcreate

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
# core and a session secret; sensible local defaults are used when the env is
# unset.
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
