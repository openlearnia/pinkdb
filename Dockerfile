# syntax=docker/dockerfile:1.7
#
# pinkdb's public image: pinkdb-server (tls + async-server) and the pinkdb CLI
# in one image, built from source. Two runtime candidates are defined so the
# choice can be measured; RUNTIME picks the published one (musl by default:
# the smallest image that passes scripts/docker/smoke.sh).
#
#   docker build -t pinkdb:default .
#   docker build --build-arg VARIANT=lake -t pinkdb:lake .
#   docker build --build-arg RUNTIME=glibc -t pinkdb:glibc .   # measurement
#
# VARIANT only selects the CLI's lake features. The server is the same in both
# variants (tls + async-server); the CLI also gets client + tls so remote mode
# (`pinkdb --host`) and encrypted remote sessions work out of the box.
#
# The musl candidate is built natively on Alpine and both variants are fully
# static.

ARG RUST_VERSION=1.97
ARG RUNTIME=musl
ARG VARIANT=default

# ---------------------------------------------------------------- fetch
# Manifests only (plus stub sources): `cargo fetch` is reused as long as
# Cargo.toml/Cargo.lock stay unchanged, so editing a .rs file only invalidates
# the source layer and the build below. The registry is arch independent, so
# both builders reuse the same fetched crates.
FROM rust:${RUST_VERSION}-slim-bookworm AS fetch

ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always

WORKDIR /fetch

# Cache-friendly layer order: manifests + a throwaway source tree first, then
# `cargo fetch`. The fetched registry is copied into both builders below, so
# editing a .rs file does not invalidate this layer or re-download crates.
COPY Cargo.toml Cargo.lock ./
COPY crates/pinkdb/Cargo.toml crates/pinkdb/Cargo.toml
COPY crates/pinkdb-cli/Cargo.toml crates/pinkdb-cli/Cargo.toml
COPY crates/pinkdb-server/Cargo.toml crates/pinkdb-server/Cargo.toml
COPY crates/pinkdb-server-ui/Cargo.toml crates/pinkdb-server-ui/Cargo.toml
COPY crates/pinkdb-ffi/Cargo.toml crates/pinkdb-ffi/Cargo.toml
COPY crates/pinkdb-ext-loader/Cargo.toml crates/pinkdb-ext-loader/Cargo.toml
COPY crates/pinkdb-js/Cargo.toml crates/pinkdb-js/Cargo.toml
COPY crates/pinkdb-httpfs/Cargo.toml crates/pinkdb-httpfs/Cargo.toml
COPY crates/pinkdb-wasm/Cargo.toml crates/pinkdb-wasm/Cargo.toml
COPY crates/pink-lake/Cargo.toml crates/pink-lake/Cargo.toml
COPY crates/pink-lake-parquet/Cargo.toml crates/pink-lake-parquet/Cargo.toml
COPY ext-sdk/rust/Cargo.toml ext-sdk/rust/Cargo.toml
COPY ext-sdk/rust/example/string_ext/Cargo.toml ext-sdk/rust/example/string_ext/Cargo.toml
COPY extensions/json/Cargo.toml extensions/json/Cargo.toml
COPY extensions/string/Cargo.toml extensions/string/Cargo.toml
COPY extensions/datetime/Cargo.toml extensions/datetime/Cargo.toml
COPY extensions/math/Cargo.toml extensions/math/Cargo.toml
COPY extensions/arrow/Cargo.toml extensions/arrow/Cargo.toml
COPY extensions/parquet/Cargo.toml extensions/parquet/Cargo.toml
RUN set -eux; \
    for manifest in $(find . -mindepth 2 -name Cargo.toml); do \
        dir=${manifest%/Cargo.toml}; \
        mkdir -p "$dir/src"; \
        [ -f "$dir/src/lib.rs" ] || : >"$dir/src/lib.rs"; \
        [ -f "$dir/src/main.rs" ] || : >"$dir/src/main.rs"; \
    done; \
    mkdir -p crates/pinkdb-server/src/bin; \
    : >crates/pinkdb-server/src/bin/pinkdb-server.rs; \
    mkdir -p crates/pinkdb/benches; \
    : >crates/pinkdb/benches/durable.rs; \
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
        default) cli_features="client,tls,pinklake,s3" ;; \
        lake)    cli_features="client,tls,pinklake,s3,parquetlake" ;; \
        *) echo "unknown VARIANT=$VARIANT (expected default or lake)" >&2; exit 2 ;; \
    esac; \
    cargo build --release --locked -p pinkdb-server --features tls,async-server; \
    cargo build --release --locked -p pinkdb-cli --features "$cli_features"; \
    mkdir -p /out; \
    cp target/release/pinkdb-server target/release/pinkdb /out/; \
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
        default) cli_features="client,tls,pinklake,s3" ;; \
        lake)    cli_features="client,tls,pinklake,s3,parquetlake" ;; \
        *) echo "unknown VARIANT=$VARIANT (expected default or lake)" >&2; exit 2 ;; \
    esac; \
    cargo build --release --locked -p pinkdb-server --features tls,async-server; \
    cargo build --release --locked -p pinkdb-cli --features "$cli_features"; \
    mkdir -p /out; \
    cp target/release/pinkdb-server target/release/pinkdb /out/; \
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
    mkdir -p /data; \
    chown pinkdb:pinkdb /data

COPY --from=builder /out/pinkdb /out/pinkdb-server /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

# The glibc candidate: Debian slim (matches the builder's bookworm).
FROM debian:bookworm-slim AS runtime-glibc

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 pinkdb \
 && useradd --uid 10001 --gid pinkdb --home-dir /data --no-create-home \
        --shell /usr/sbin/nologin pinkdb \
 && mkdir -p /data \
 && chown pinkdb:pinkdb /data

COPY --from=builder /out/pinkdb /out/pinkdb-server /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
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
# and explicit arguments still win (`docker run … --port 6000`).
ENV PINKDB_HOST=0.0.0.0 \
    PINKDB_PORT=5433 \
    PINKDB_FILE=/data/pinkdb.pink

WORKDIR /data
VOLUME ["/data"]
EXPOSE 5433

# Fixed non-root identity (the image has no writable home and never runs as
# root). /data is the only writable path by convention.
USER 10001:10001

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
    CMD ["/usr/local/bin/pinkdb-server", "--health-check"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
