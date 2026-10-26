# OPS-2: scratch-based image, version-pinned with the compose file.
# The builder is only ever a build-time dependency; nothing from it ships except
# the static binary and the CA bundle the delivery worker needs for HTTPS.

FROM golang:1.23-alpine AS build
WORKDIR /src

# Dependency layer first so a source edit does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/pigeon ./cmd/pigeon

FROM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/pigeon /pigeon

# Non-root, no shell, no package manager: the delivery worker is an outbound-only
# process and has no reason to own anything in the image.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/pigeon"]
