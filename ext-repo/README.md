# pinkdb extension repository

A hosted, immutable catalog of built pinkdb extension artifacts (shared
libraries exporting the pinkdb extension C ABI v1, loaded with
`LOAD EXTENSION`). The repository is an R2 bucket (`pinkdb-extensions`) fronted
by a small Cloudflare Worker that serves the catalog and the artifact bytes.

**Live: <https://pinkdb-ext-repo.defaultcms.workers.dev>** —
catalog at <https://pinkdb-ext-repo.defaultcms.workers.dev/index.json>

The format is specified in [`FORMAT.md`](FORMAT.md) (format v1): URL layout,
`index.json` schema, content types, caching, platform tags, and the
versioning/immutability rules. Short version:

- `GET /index.json` — the catalog:
  `{ format_version, updated, extensions: [{ name, version, abi, platform, file, sha256, description }] }`.
- `GET /artifacts/<name>/<version>/<platform>/<file>` — artifact bytes
  (`application/octet-stream`, correct `Content-Length`/`ETag`, immutable cache).
- An existing `name`+`version`+`platform` artifact is **immutable**;
  re-publishing the same coordinates is refused (bump the version instead).
- Platform tags: `darwin-arm64` today; `darwin-x64`, `linux-x64`,
  `linux-arm64`, `windows-x64` reserved.

## This directory

```
FORMAT.md    repository format v1 (index schema, URLs, immutability rules)
README.md    this file
publish.py   publish one built extension (Python stdlib only; drives wrangler)
publish.sh   wrapper:  ./publish.sh <dylib> --name … --version … --platform … --description …
verify.sh    download an artifact URL and check its SHA-256 against the index
worker/      the Cloudflare Worker (wrangler.toml + src/index.js; no build step)
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

1. computes the artifact's SHA-256;
2. checks the current `index.json` and **refuses** (exit 1) if
   `name`+`version`+`platform` is already published;
3. copies the artifact to `artifacts/<name>/<version>/<platform>/<file>`
   (local mirror) and appends the entry to the index (sorted, fresh `updated`);
4. uploads the artifact and then the catalog to R2 with
   `wrangler r2 object put --remote` (artifact first, so a failed catalog
   upload cannot leave the catalog pointing at a missing artifact);
5. prints the resulting URLs.

```text
uploaded pinkdb-extensions/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
uploaded pinkdb-extensions/index.json
published math_ext 0.1.0 (darwin-arm64)
  sha256:   38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2
  artifact: https://pinkdb-ext-repo.defaultcms.workers.dev/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
  catalog:  https://pinkdb-ext-repo.defaultcms.workers.dev/index.json
  staged:   /…/ext-repo/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
```

Re-running is safe for **new** versions (the index is re-read and appended to);
re-publishing existing coordinates exits 1 by design. The catalog URL is
cached for 60 s, so a fresh publish can take up to a minute to appear.

## Verifying

```sh
./ext-repo/verify.sh https://pinkdb-ext-repo.defaultcms.workers.dev/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
# OK   https://…/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
#      sha256 38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2 (matches the index entry; 17088 bytes)
```

It downloads the artifact with `curl`, fetches the catalog (default
`<origin>/index.json`, override with a second argument), finds the entry whose
`file` matches the URL path, and compares the SHA-256. Any mismatch, missing
entry, or failed download exits non-zero.

## Consuming from a client

`INSTALL` is **wave 33** — not implemented yet; nothing in pinkdb consumes this
repository today. The intended flow for the future `INSTALL <name>` /
repository client:

1. fetch `<repo>/index.json` (the repository URL becomes configurable in wave 33);
2. select the entry with `format_version == 1`, `abi == 1`, the client's
   `platform`, and the wanted `name` (+ version, latest if unspecified);
3. download `<repo>/<file>` and verify its `sha256` before touching it;
4. write it to a local cache path and `LOAD EXTENSION` it (extensions stay
   process-level; the loader re-resolves the path at WAL replay, so prefer a
   stable cache location).

The worker needs no changes for that: the catalog is plain JSON and artifacts
are plain bytes.

## Deploying the worker

```sh
wrangler r2 bucket create pinkdb-extensions   # once (skip if it exists)
cd ext-repo/worker && wrangler deploy          # prints the workers.dev URL
```

The worker is a single ES module (`worker/src/index.js`, no build step, no
dependencies) bound to the bucket as `EXT_BUCKET`. It only ever reads; there is
no write API. Local checks: `node --check src/index.js` and
`wrangler deploy --dry-run`.
