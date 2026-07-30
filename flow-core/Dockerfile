# syntax=docker/dockerfile:1.6
FROM golang:1.23-bookworm AS builder

ENV GONOSUMCHECK=*
ENV GONOSUMDB=*
ENV GOFLAGS=-mod=mod

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download || true
COPY *.go ./
COPY cmd ./cmd
COPY pb ./pb
COPY internal ./internal

# Module-aware build: `go build -o flow-core .` discovers all packages
# (root + internal/) via go.mod, instead of treating *.go as a literal
# file list which silently misses subpackages.
RUN go mod tidy
RUN CGO_ENABLED=1 GOOS=linux go build -o flow-core .
RUN CGO_ENABLED=1 GOOS=linux go build -o alarm-materializer ./cmd/alarm-materializer

FROM debian:bookworm-slim

# Production hardening:
#   - ca-certificates: TLS verification for outbound HTTPS.
#   - curl: small footprint dependency for the HEALTHCHECK below.
#   - tini: PID 1 reaper so SIGTERM from k8s/docker reaches our process
#     before the 30s grace period expires.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl tini \
 && rm -rf /var/lib/apt/lists/*

# Run as non-root. uid 10001 is conventional for "service" accounts and
# steers clear of any system uid range. Image is read-only-friendly:
# the binary lives in /app, runtime data should mount at /data.
RUN groupadd --system --gid 10001 flow \
 && useradd  --system --uid 10001 --gid flow --home-dir /app --shell /usr/sbin/nologin flow

WORKDIR /app
COPY --from=builder --chown=flow:flow /app/flow-core ./flow-core
COPY --from=builder --chown=flow:flow /app/alarm-materializer ./alarm-materializer
# TB system catalogue: widget bundles + widget types + system dashboards +
# images. Loaded into postgres on first start by bootstrap.LoadSystem().
COPY --chown=flow:flow tb-resources /etc/flow/tb-resources/

USER flow

ENV QUESTDB_HOST="questdb:9009"
ENV ALLOWED_ORIGIN="*"

EXPOSE 8080

# Liveness probe — orchestrators that don't read k8s probe specs (plain
# docker, swarm) get a working healthcheck out of the box. /ready
# checks dependencies; we use /health for the docker-level probe to
# avoid flapping the container during postgres restart.
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/health || exit 1

# tini routes SIGTERM cleanly to the Go process so graceful shutdown
# (signal.Notify in main.go) actually fires.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["./flow-core"]
