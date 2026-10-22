# Wave 91 — public Docker image on GHCR (plan)

Started 2026-10-22; **done** the same day. ROADMAP row 91: a containerized `pinkdb-server` (with the
CLI in the same image), multi-arch, non-root, a `/data` volume and a
healthcheck, published publicly to `ghcr.io/openlearnia/pinkdb` on release
tags with provenance and an SBOM; a `docker run` quickstart on the website.

## Decisions

1. **Build from source in a multi-stage `Dockerfile`** at the repository
   root (not by copying release tarballs): a pinned Rust builder stage
   (`rust:1.97-*`, matching the workspace MSRV) builds `pinkdb-server` and
   `pinkdb` (CLI) in release mode with a cache-friendly layer order
   (manifests + `cargo fetch` first); a minimal runtime stage. **Pick the
   runtime by measurement**: static musl on `scratch`/`alpine` vs
   `debian:*-slim` glibc — report both image sizes and pick the smaller one
   that passes the smoke test (TLS via rustls needs no system OpenSSL; CA
   certificates are needed for `s3://`/`https://` reads — include them).
2. **Variants (tags):** `ghcr.io/openlearnia/pinkdb:<version>` and `:latest`
   — server with `tls` + `async-server`, CLI with the default features plus
   `pinklake,s3`; `:<version>-lake` and `:lake` — the same server, CLI with
   `vortexlake,s3,parquetlake` (bigger; Vortex). A `docker build --build-arg
   VARIANT=default|lake` selects the CLI features.
3. **Runtime contract:** non-root user `pinkdb` (fixed UID/GID 10001),
   `WORKDIR /data`, `VOLUME /data`, `EXPOSE 5433`, entrypoint
   `pinkdb-server` with defaults suitable for a container
   (`--host 0.0.0.0 --port 5433 --file /data/pinkdb.pink`), every
   `pinkdb-server` flag still accepted (`docker run … pinkdb --idle-timeout
   60`), environment variables for the common ones (`PINKDB_HOST`,
   `PINKDB_PORT`, `PINKDB_FILE`, `PINKDB_TRANSPORT`, TLS cert/key paths,
   RBAC) mapped in a tiny POSIX entrypoint script **or** read by the server
   itself — pick one and document it. The CLI is reachable as
   `docker run --rm -it ghcr.io/openlearnia/pinkdb pinkdb …` (entrypoint
   passes through when the first argument is `pinkdb`).
4. **Healthcheck:** a `HEALTHCHECK` that speaks the protocol's `Ping`: add a
   small `pinkdb-server --health-check [host:port]` mode (connect, Ping,
   exit 0/1, short timeout) so the image needs no extra tools.
5. **CI/publish:** `.github/workflows/docker.yml` — on pull requests and
   pushes to `main`: build (no push) for `linux/amd64,linux/arm64` with
   buildx + QEMU and run the smoke test on amd64; on `v*` tags (and manual
   dispatch with a version): build, smoke, then push both variants to GHCR
   with `GITHUB_TOKEN` (`permissions: packages: write`), `provenance: true`,
   `sbom: true`, OCI labels (source, version, revision, licenses). Version
   checked against `crates/pinkdb-server/Cargo.toml` like `release.yml`.
   Actions pinned to commit SHAs (the repo's policy).
6. **Smoke test** (`scripts/docker/smoke.sh`, used locally and in CI): run
   the image detached with a temp volume, wait for healthy, connect with
   the image's own CLI in remote mode (`pinkdb --host …`), create a table,
   insert, select, restart the container, data persisted; `--health-check`
   exit codes; the `lake` variant runs `.lake save`/`.lake open` on a local
   directory.
7. **Docs:** README "Docker" section and a website quickstart (`docker run
   -p 5433:5433 -v pinkdb-data:/data ghcr.io/openlearnia/pinkdb`), plus
   `docs/releasing.md` (the docker job is part of the release train).

## Gates

`docker build` of both variants locally (linux/arm64 native) + the smoke
script passing for both; `actionlint` on the workflow; `cargo test -p
pinkdb-server` (health-check mode tests); `cargo clippy --workspace
--all-targets -- -D warnings`; fmt; `python3 website/build.py` +
`scripts/check-website.sh`.

## Rules for delegated agents

Do not commit, push, tag, or log in to any registry; never `docker push`.
Touch only the files the plan names (`Dockerfile`, `.dockerignore`,
`docker/` or `scripts/docker/`, `.github/workflows/docker.yml`,
`crates/pinkdb-server/**` for the health-check mode, README.md,
`docs/releasing.md`, website pages). Use `CARGO_INCREMENTAL=0`. Clean up
images and containers you create (`docker image rm`, `docker rm -f`) and
report the final image sizes.

## How it was built

Delegated to OpenCode `opencode/deepseek-v4.1-flash-free` (completed without
the paid fallback); reviewed and verified here.

Landed: a root `Dockerfile` (manifest-only fetch stage + `cargo fetch
--locked`, builder per runtime, `VARIANT=default|lake`), `.dockerignore`,
`docker/entrypoint.sh` (CLI pass-through for `pinkdb …`, any other command
run as given, `PINKDB_*` environment → server flags with defaults <
environment < arguments), `pinkdb-server --health-check [HOST:PORT]`
(connect with a timeout, protocol `Ping`, exit 0/1; wildcard binds probe
loopback) with `Client::connect_timeout`, `scripts/docker/smoke.sh`,
`.github/workflows/docker.yml`, a website Docker page, README and
`docs/releasing.md` sections.

**Runtime by measurement:** static musl on Alpine 36.6 MB vs slim glibc
170 MB → musl. The `lake` variant is dynamically linked against musl
(101 MB): a Vortex-stack C++ dependency runs `bindgen`, which needs
`libclang` at build time, so a fully static build is not possible there.

**Review fixes:**

- **CI cost.** Pushes and pull requests built both variants for arm64 under
  QEMU (an emulated Rust release build takes hours; the job timeout was
  240 min) on every commit. Now pushes/PRs build and smoke natively on amd64
  only, and only when the image's inputs change (Dockerfile, `docker/`,
  `scripts/docker/`, the workflow, `Cargo.toml`/`Cargo.lock`); the emulated
  arm64 build runs only in the tag-time publish job.
- **Health check with TLS.** The probe always spoke the plaintext protocol;
  a container serving TLS (`PINKDB_TLS_CERT`) refuses it, so it would have
  been marked unhealthy. With TLS configured the probe now checks TCP accept
  on the port (a localhost probe cannot generally verify a self-signed
  certificate); `tests/health_check.rs` covers it.

**Verified here (linux/arm64):** `default` 36.6 MB and `lake` 101 MB built;
`scripts/docker/smoke.sh` passed for both (HEALTHCHECK healthy, UID 10001,
health-check exit codes, remote CLI create/insert/select, data survives a
restart, `lake`: `.lake save`/`.lake open` on `/data`); `pinkdb-server`
108 tests, clippy on both toolchains, fmt, actionlint, website check. The
images publish on the next `v*` tag (`ghcr.io/openlearnia/pinkdb:<version>`,
`:latest`, `:<version>-lake`, `:lake`).
