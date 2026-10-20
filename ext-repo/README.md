# pinkdb extension repository

A hosted, immutable catalog of built pinkdb extension artifacts (shared
libraries exporting the pinkdb extension C ABI v1, loaded with
`LOAD EXTENSION`). The repository is an R2 bucket (`pinkdb-extensions`) fronted
by a small Cloudflare Worker that serves the catalog and the artifact bytes.

**Live: <https://pinkdb-ext-repo.defaultcms.workers.dev>** — a short info page
linking the raw catalog (`/index.json`) and the browsable catalog UI in the
docs: **<https://pinkdb-docs.defaultcms.workers.dev/extensions>** (search,
per-extension details, install commands, downloads).

The format is specified in [`FORMAT.md`](FORMAT.md) (format v1): URL layout,
`index.json` schema, content types, caching, platform tags, and the
versioning/immutability rules. Short version:

- `GET /index.json` — the catalog:
  `{ format_version, updated, extensions: [{ name, version, abi, platform, file, sha256, size?, description }] }`
  (`size` in bytes is optional but recommended; `publish.py` always writes it,
  the live `math_ext` 0.1.0 entry predates it). Carries
  `Access-Control-Allow-Origin: *` so the docs page can fetch it cross-origin;
  `OPTIONS /index.json` answers the preflight.
- `GET /index.json.sig` — detached Ed25519 signature over the exact
  `index.json` bytes (`PINKDB-SIG1 <keyid> <base64>`), used by `.install` and
  `verify.sh` before trusting any artifact hash.
- `GET /` — a short info page: repository title, format version, and links to
  the docs catalog UI and `/index.json`. It is styled in the docs site's
  "soft data-zine" paper palette with system fonts only (no webfont requests,
  no scripts). The catalog UI itself (search over
  name, description and platform; per-extension details with
  `.install <name>` / `.install <name>@<version>`, the download link, SHA-256,
  the all-versions table and the raw entry) lives at
  <https://pinkdb-docs.defaultcms.workers.dev/extensions>; it fetches
  `/index.json` client-side.
- `GET /artifacts/<name>/<version>/<platform>/<file>` — artifact bytes
  (`application/octet-stream`, correct `Content-Length`/`ETag`, immutable cache).
- An existing `name`+`version`+`platform` artifact is **immutable**;
  re-publishing the same coordinates is refused (bump the version instead).
- Platform tags published by the tag matrix: `darwin-arm64`, `darwin-x64`,
  `linux-x64`, `linux-arm64`; `windows-x64` is reserved (no Windows build yet —
  see `../docs/releasing.md`).

## This directory

```
FORMAT.md    repository format v1 (index schema, URLs, immutability rules)
README.md    this file
publish.py   publish one built extension (Python stdlib only; drives wrangler)
publish.sh   wrapper:  ./publish.sh <dylib> --name … --version … --platform … --description …
verify.sh    verify the catalog signature + every artifact hash (local or remote)
tests/       stdlib unittest for publish.py signing and verify.sh
keys/        extension-signing.pub.pem (the pinned catalog public key)
worker/      the Cloudflare Worker: src/index.js (info page + machine
             endpoints) and verify-ui.mjs (checks the info page, CORS,
             catalog and artifacts against a live repository; also runs the
             pure helpers from the generated docs Extensions page against the
             live catalog); wrangler.toml; no build step
index.json   local mirror of the catalog (gitignored; canonical copy lives in R2)
artifacts/   local mirror of published artifacts (gitignored)
```

## Publishing

Prerequisites: `wrangler` installed and authenticated, and the R2 bucket
created (see *Deploying* below). Then, from the repo root:

```sh
make -C ext-sdk/c    # build the C SDK examples (if not built yet)
./ext-repo/publish.sh ext-sdk/c/build/math_ext.dylib \
    --name math_ext --version 0.1.0 --platform darwin-arm64 \
    --description "Clamp and degree/radian helpers (ext_clamp, ext_deg2rad)"
```

`publish.py` then:

1. computes the artifact's SHA-256 and size in bytes;
2. checks the current `index.json` and **refuses** (exit 1) if
   `name`+`version`+`platform` is already published;
3. copies the artifact to `artifacts/<name>/<version>/<platform>/<file>`
   (local mirror) and appends the entry to the index (sorted, fresh `updated`);
4. signs the exact index bytes with Ed25519 when `PINKDB_EXT_SIGNING_KEY` is
   set (PEM path, PEM content, or base64 of the PEM) and writes
   `index.json.sig`;
5. uploads the artifact, the signature, and the catalog to R2 with
   `wrangler r2 object put --remote` (artifact first, catalog last, so a failed
   catalog upload cannot leave the catalog pointing at a missing artifact);
6. prints the resulting URLs.

```text
uploaded pinkdb-extensions/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
uploaded pinkdb-extensions/index.json.sig
uploaded pinkdb-extensions/index.json
published math_ext 0.1.0 (darwin-arm64)
  sha256:   38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2
  size:     17088 bytes
  artifact: https://pinkdb-ext-repo.defaultcms.workers.dev/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
  catalog:  https://pinkdb-ext-repo.defaultcms.workers.dev/index.json
  signature: https://pinkdb-ext-repo.defaultcms.workers.dev/index.json.sig (key c84e54bfea6b2d09)
  staged:   /…/ext-repo/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
```

Without `PINKDB_EXT_SIGNING_KEY` the publisher keeps the old unsigned behavior
and prints a loud warning; pinned clients refuse that catalog. See
[`../docs/extension-signing.md`](../docs/extension-signing.md) for the key,
envelope, rotation, and threat model.

Re-running is safe for **new** versions (the index is re-read and appended to);
re-publishing existing coordinates exits 1 by design. The catalog URL is
cached for 60 s, so a fresh publish can take up to a minute to appear.

## Verifying

Local mirror (signature first, then every artifact hash):

```sh
./ext-repo/verify.sh
# OK   catalog signature /…/ext-repo/index.json (key c84e54bfea6b2d09)
# OK   math_ext 0.1.0 (darwin-arm64): sha256 38abc… (17088 bytes)
# checked 1 artifact(s), 0 failure(s)
```

`--skip-artifacts` verifies only the catalog signature; `--index`, `--sig`, and
`--pubkey` override the paths. Remote mode fetches the catalog + signature,
verifies both, then checks one artifact URL against the now-signed catalog:

```sh
./ext-repo/verify.sh https://pinkdb-ext-repo.defaultcms.workers.dev/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
# OK   catalog signature …
# OK   artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib: sha256 38abc… (17088 bytes)
```

Any missing/malformed/unknown-key/invalid signature, missing artifact, hash
mismatch, or failed download exits non-zero.

## Consuming from a client

The CLI consumes this repository through the opt-in `repo` feature
(`cargo build -p pinkdb-cli --features repo`):

```text
pinkdb> .install math_ext
Installed math_ext 0.1.0 (darwin-arm64)
  repository: https://pinkdb-ext-repo.defaultcms.workers.dev
  path:       ~/.pinkdb/extensions/math_ext/0.1.0/darwin-arm64/math_ext.dylib
  sha256:     38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2
  signature:  verified (key c84e54bfea6b2d09)
Extension math_ext loaded.
```

What that client does (implemented in `crates/pinkdb-cli/src/repo/`):

1. fetch `<repo>/index.json` **and** `<repo>/index.json.sig` — repositories
   come from `$PINKDB_HOME/repositories.json` (default `~/.pinkdb`), which
   defaults to the live repository above when absent; the client is a cargo
   feature, so the default binary links no HTTP/TLS code;
2. verify the Ed25519 signature over the exact catalog bytes against the key
   pinned in the binary **before any artifact request** (a missing, malformed,
   unknown-key, or invalid signature is fatal; see
   `../docs/extension-signing.md` for the dev-only overrides);
3. select the entry with `format_version == 1`, `abi == 1`, the client's
   platform tag (derived at runtime, e.g. `darwin-arm64`), and the wanted
   `name` (+ version; highest semver-ish version if unspecified) —
   repositories are tried in config order and the first match wins;
4. download `<repo>/<file>` (timeout and size caps) to a temp file, verify its
   `sha256`, and only then rename it into
   `$PINKDB_HOME/extensions/<name>/<version>/<platform>/<file>`; a mismatch is
   refused and deletes the temp file (nothing left behind);
5. stage the verified file in a private `0700` temp directory with
   `create_new`, re-hash the staged copy, and `LOAD EXTENSION` that path
   (extensions stay process-level; the stable cache path is what WAL replay
   re-loads).

`.repositories list` / `add` / `remove` manages the repository list and
`.installed` lists the cache. The worker needs no changes for that: the
catalog is plain JSON and artifacts are plain bytes.

## Deploying the worker

```sh
wrangler r2 bucket create pinkdb-extensions   # once (skip if it exists)
cd ext-repo/worker && wrangler deploy          # prints the workers.dev URL
```

The worker is a single ES module (`worker/src/index.js`, no build step, no
dependencies) bound to the bucket as `EXT_BUCKET`. It only ever reads; there is
no write API. Local checks: `node --check src/index.js` and
`wrangler deploy --dry-run`. After deploying, `node worker/verify-ui.mjs
[repo-url] [docs-url]` checks the info page, the CORS header on `/index.json`,
the catalog headers/ETag and every artifact download against a live
repository, plus the docs Extensions page (run `python3 website/build.py`
first) — defaults: the live repository and the live docs. The headless-browser
pass lives in `website/verify-browser.mjs` (`node website/verify-browser.mjs`).
