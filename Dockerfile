# OPS-2: scratch-based image, version-pinned with the compose file.
# The builder is only ever a build-time dependency; nothing from it ships except
# the static binary and the CA bundle the delivery worker needs for HTTPS.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependency layer first so a source edit does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
# Image builds gate deployable binaries on the self-contained unit suite. The
# database-backed acceptance tests are only enabled when their explicit DSNs are
# supplied by CI or `make test-integration`.
RUN go test -count=1 -p 1 ./...
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/pigeon ./cmd/pigeon
# The consumer portal is a second deployable shipped in the same image (§14,
# AC-40): a stateless proxy with no database, started by overriding the
# entrypoint.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/consumer-portal ./cmd/consumer-portal
# The producer portal is the third deployable in the same image (§14, AC-40):
# like the consumer portal, a stateless proxy with no database, started by
# overriding the entrypoint.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/producer-portal ./cmd/producer-portal
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/loadgen ./cmd/loadgen

FROM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch AS loadgen-sink
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/loadgen /loadgen
USER 65532:65532
EXPOSE 9001
ENTRYPOINT ["/loadgen"]

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/pigeon /pigeon
COPY --from=build /out/consumer-portal /consumer-portal
COPY --from=build /out/producer-portal /producer-portal

# Non-root, no shell, no package manager: every binary is an outbound-only static
# Go process with no reason to own anything in the image.
USER 65532:65532
EXPOSE 8080 8082 8083
# The default entrypoint is the core; the portals are reached by overriding it
# (`entrypoint: ["/consumer-portal"]`, `["/producer-portal"]` in compose.yaml).
# Not `command:` - that appends arguments to this entrypoint instead of replacing
# it, so the container would run `/pigeon /consumer-portal`.
ENTRYPOINT ["/pigeon"]
