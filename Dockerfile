# syntax=docker/dockerfile:1.7
#
# pinkdb's public image: pinkdb-server and the pinkdb CLI in one image, built
# from source. Two runtime candidates are defined so the choice can be measured;
# RUNTIME picks the published one (musl by default: the smallest image that
# passes scripts/docker/smoke.sh).
#
#   docker build -t pinkdb:default .
#   docker build --build-arg VARIANT=lake -t pinkdb:lake .
#   docker build --build-arg VARIANT=quacklab -t pinkdb:quacklab .
#   docker build --build-arg RUNTIME=glibc -t pinkdb:glibc .   # measurement
#
# VARIANT selects what the image is for. `default` and `lake` differ only in the
# CLI's lake features; the server is `tls,async-server`. `quacklab` is the
# QuackLab SQL service (wave 138d): pinkdb-server with the HTTP adapter
# (`quacklab`), the file readers (`parquet`, `httpfs`), the S3 lake transport
# (`lake-s3`) and the Postgres lake catalog (`catalog-postgres`), under a
# different entrypoint (docker/entrypoint-quacklab.sh) that serves $PORT and
# keeps the wire protocol on loopback. Every variant ships the CLI with client +
# tls so remote mode (`pinkdb --host`) and encrypted remote sessions work out of
# the box.
#
# The musl candidate is built natively on Alpine and both variants are fully
# static.

ARG RUST_VERSION=1.97
ARG RUNTIME=musl
ARG VARIANT=default

# ---------------------------------------------------------------- manifests
# Every Cargo.toml in the build context as is, and every .rs file as an empty
# stub at its path (cargo finds bins, benches and examples from the files that
# exist). A hand-written COPY list fell behind the workspace once (wave 107c's
# embeddings extension broke `cargo fetch`), so nothing here names a crate.
FROM rust:${RUST_VERSION}-slim-bookworm AS manifests

WORKDIR /src
COPY . .
RUN set -eux; \
    mkdir /manifests; \
    cp Cargo.lock /manifests/; \
    find . -name target -prune -o -name Cargo.toml -print | while read -r f; do \
        mkdir -p "/manifests/${f%/*}"; \
        cp "$f" "/manifests/$f"; \
    done; \
    find . -name target -prune -o -name '*.rs' -print | while read -r f; do \
        mkdir -p "/manifests/${f%/*}"; \
        : >"/manifests/$f"; \
    done

# ---------------------------------------------------------------- fetch
# Manifests and stubs only: the copy below is cached by content, so `cargo
# fetch` reruns only when a manifest changes or a Rust file is added or
# removed; editing a .rs file only invalidates the builders. The registry is
# arch independent, so both builders reuse the same fetched crates.
FROM rust:${RUST_VERSION}-slim-bookworm AS fetch

ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always

WORKDIR /fetch
COPY --from=manifests /manifests/ ./
RUN set -eux; \
    mkdir -p /usr/local/cargo/git; \
    cargo fetch --locked

# ---------------------------------------------------------------- builders
# glibc: Debian bookworm, native gcc.
FROM rust:${RUST_VERSION}-slim-bookworm AS builder-glibc

ARG TARGETARCH
ARG VARIANT

ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always

# build-essential: ring's C/assembly. pkg-config: optional native probes.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        pkg-config; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=fetch /usr/local/cargo/registry /usr/local/cargo/registry
COPY --from=fetch /usr/local/cargo/git /usr/local/cargo/git

# Source last: editing a .rs file only invalidates this layer and the build.
COPY . .

RUN --mount=type=cache,id=cargo-target-glibc-${VARIANT}-${TARGETARCH},target=/app/target \
    set -eux; \
    case "$VARIANT" in \
        default) \
            server_features="tls,async-server"; \
            cli_features="client,tls,pinklake,s3"; \
            entrypoint="docker/entrypoint.sh" ;; \
        lake) \
            server_features="tls,async-server"; \
            cli_features="client,tls,pinklake,s3,parquetlake"; \
            entrypoint="docker/entrypoint.sh" ;; \
        quacklab) \
            server_features="quacklab,parquet,httpfs,lake-s3,catalog-postgres"; \
            cli_features="client,tls,pinklake,s3"; \
            entrypoint="docker/entrypoint-quacklab.sh" ;; \
        *) echo "unknown VARIANT=$VARIANT (expected default, lake or quacklab)" >&2; exit 2 ;; \
    esac; \
    cargo build --release --locked -p pinkdb-server --features "$server_features"; \
    cargo build --release --locked -p pinkdb-cli --features "$cli_features"; \
    mkdir -p /out; \
    cp target/release/pinkdb-server target/release/pinkdb /out/; \
    cp "$entrypoint" /out/docker-entrypoint.sh; \
    cp /etc/ssl/certs/ca-certificates.crt /out/ca-certificates.crt

# musl: Alpine, where gcc/g++ target musl natively (no cross toolchain).
# Both variants are fully static.
FROM rust:${RUST_VERSION}-alpine AS builder-musl

ARG TARGETARCH
ARG VARIANT

ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always

# build-base: gcc/g++/make for ring and other C dependencies.
RUN set -eux; \
    apk add --no-cache \
        build-base \
        ca-certificates \
        pkgconfig

WORKDIR /app
COPY --from=fetch /usr/local/cargo/registry /usr/local/cargo/registry
COPY --from=fetch /usr/local/cargo/git /usr/local/cargo/git

COPY . .

RUN --mount=type=cache,id=cargo-target-musl-${VARIANT}-${TARGETARCH},target=/app/target \
    set -eux; \
    case "$VARIANT" in \
        default) \
            server_features="tls,async-server"; \
            cli_features="client,tls,pinklake,s3"; \
            entrypoint="docker/entrypoint.sh" ;; \
        lake) \
            server_features="tls,async-server"; \
            cli_features="client,tls,pinklake,s3,parquetlake"; \
            entrypoint="docker/entrypoint.sh" ;; \
        quacklab) \
            server_features="quacklab,parquet,httpfs,lake-s3,catalog-postgres"; \
            cli_features="client,tls,pinklake,s3"; \
            entrypoint="docker/entrypoint-quacklab.sh" ;; \
        *) echo "unknown VARIANT=$VARIANT (expected default, lake or quacklab)" >&2; exit 2 ;; \
    esac; \
    cargo build --release --locked -p pinkdb-server --features "$server_features"; \
    cargo build --release --locked -p pinkdb-cli --features "$cli_features"; \
    mkdir -p /out; \
    cp target/release/pinkdb-server target/release/pinkdb /out/; \
    cp "$entrypoint" /out/docker-entrypoint.sh; \
    cp /etc/ssl/certs/ca-certificates.crt /out/ca-certificates.crt

FROM builder-${RUNTIME} AS builder

# ---------------------------------------------------------------- runtimes
# The musl candidate: Alpine gives a POSIX shell for the entrypoint, a
# writable /data for the non-root user and ca-certificates for s3:// / https://
# reads — a few MB more than scratch, still far below the glibc candidate.
FROM alpine:3.22 AS runtime-musl

RUN set -eux; \
    apk add --no-cache ca-certificates; \
    addgroup -g 10001 -S pinkdb; \
    adduser -u 10001 -S -D -G pinkdb -h /data -s /sbin/nologin pinkdb; \
    mkdir -p /data /var/lib/pinkdb; \
    chown pinkdb:pinkdb /data /var/lib/pinkdb

COPY --from=builder /out/pinkdb /out/pinkdb-server /usr/local/bin/
COPY --from=builder /out/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

# The glibc candidate: Debian slim (matches the builder's bookworm).
FROM debian:bookworm-slim AS runtime-glibc

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 pinkdb \
 && useradd --uid 10001 --gid pinkdb --home-dir /data --no-create-home \
        --shell /usr/sbin/nologin pinkdb \
 && mkdir -p /data /var/lib/pinkdb \
 && chown pinkdb:pinkdb /data /var/lib/pinkdb

COPY --from=builder /out/pinkdb /out/pinkdb-server /usr/local/bin/
COPY --from=builder /out/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

# ---------------------------------------------------------------- final image
FROM runtime-${RUNTIME} AS final

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.title="pinkdb" \
      org.opencontainers.image.description="pinkdb: an embeddable analytical SQL engine, CLI and TCP server" \
      org.opencontainers.image.source="https://github.com/openlearnia/pinkdb" \
      org.opencontainers.image.url="https://github.com/openlearnia/pinkdb" \
      org.opencontainers.image.documentation="https://github.com/openlearnia/pinkdb/blob/main/README.md" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

# Container-friendly defaults; the entrypoint turns these into server flags,
# and explicit arguments still win (`docker run … --port 6000`). The data
# directory is per variant, so each entrypoint defaults it (/data here,
# /var/lib/pinkdb for quacklab) and PINKDB_DATA_DIR overrides either.
ENV PINKDB_HOST=0.0.0.0 \
    PINKDB_PORT=5433

WORKDIR /data
VOLUME ["/data"]
EXPOSE 5433

# Fixed non-root identity (the image has no writable home and never runs as
# root). /data is the only writable path by convention.
USER 10001:10001

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
    CMD ["/usr/local/bin/pinkdb-server", "--health-check"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
