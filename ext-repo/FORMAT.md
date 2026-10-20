# pinkdb extension repository — format v1

The extension repository is a hosted, immutable catalog of built pinkdb
extension artifacts (shared libraries exporting the pinkdb extension C ABI v1,
loaded with `LOAD EXTENSION`). It is a plain static file tree served by a small
Cloudflare Worker in front of an R2 bucket:

```
R2 bucket  pinkdb-extensions
  index.json                                          ← the catalog
  artifacts/<name>/<version>/<platform>/<file>        ← the artifact bytes

Worker (binding EXT_BUCKET)
  GET /index.json                                     ← catalog, served from R2
  GET /artifacts/<name>/<version>/<platform>/<file>   ← artifact bytes
```

There are no directory listings, no query APIs, and no mutable state beyond
"add a new immutable artifact + append it to the index". Format version 1 is
the only version; `format_version` in `index.json` exists so a future format
can be detected instead of mis-parsed. The `GET /` page is a short info page
(version + links); the browsable catalog UI lives in the docs site at
<https://pinkdb-docs.defaultcms.workers.dev/extensions> and renders the same
files client-side (it fetches `index.json` cross-origin and links the
artifact URLs). Neither adds server-side API surface.

## `index.json`

UTF-8 JSON. The top level is the catalog:

```json
{
  "format_version": 1,
  "updated": "2026-10-14T09:30:00Z",
  "extensions": [
    {
      "name": "math_ext",
      "version": "0.1.0",
      "abi": 1,
      "platform": "darwin-arm64",
      "file": "artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib",
      "sha256": "38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2",
      "size": 17088,
      "description": "Clamp and degree/radian helpers (ext_clamp, ext_deg2rad)"
    }
  ]
}
```

The example above includes `size`; the currently published `math_ext` 0.1.0
entry predates the field and omits it (consumers must tolerate that).

Field semantics:

| Field | Type | Meaning |
|---|---|---|
| `format_version` | integer | Always `1` for this format. Clients MUST refuse other values. |
| `updated` | string | ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`) of the last catalog change. Informational. |
| `extensions` | array | All published artifacts. Sorted by `name`, then version (numeric part-wise), then `platform`. |

Entry fields:

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Extension name, exactly as `pinkdb_extension_name()` reports it (e.g. `math_ext`). |
| `version` | string | Artifact version, semver-like (`0.1.0`, `1.2.0-rc1`). Chosen by the publisher. |
| `abi` | integer | pinkdb extension C ABI version the artifact was built against. Format v1 accepts only `1`. |
| `platform` | string | Platform tag (see below). |
| `file` | string | Repository-relative path of the artifact; the URL path is `/<file>`. |
| `sha256` | string | Lowercase hex SHA-256 of the artifact bytes (the exact file downloaded from `file`). |
| `size` | integer | Artifact size in bytes. **Optional but recommended**: `publish.py` always writes it, but entries published before the field existed omit it. Consumers MUST tolerate its absence and MUST NOT use it for correctness (the `sha256` is authoritative); it is meant for display and pre-download checks. |
| `description` | string | One-line human-readable description. |

## URL layout

| URL | Content-Type | Cache | Notes |
|---|---|---|---|
| `/` | `text/html; charset=utf-8` | `no-store` | Short info page: repository title, format version, links to the docs catalog UI and `/index.json`. The browsable UI lives in the docs site and fetches `/index.json` client-side. |
| `/index.json` | `application/json; charset=utf-8` | `public, max-age=60` | Catalog; carries `ETag`, supports `If-None-Match` → `304`, and `Access-Control-Allow-Origin: *`. `OPTIONS /index.json` answers `204` with `Access-Control-Allow-Methods: GET, HEAD, OPTIONS`. |
| `/index.json.sig` | `text/plain; charset=utf-8` | `public, max-age=60` | Detached Ed25519 signature over the exact `/index.json` bytes: one line `PINKDB-SIG1 <keyid> <base64 signature>`. Carries `ETag`, supports `If-None-Match` → `304` and CORS `*`. |
| `/artifacts/<name>/<version>/<platform>/<file>` | `application/octet-stream` | `public, max-age=31536000, immutable` | Artifact bytes with `Content-Length` and `ETag`; supports `If-None-Match` → `304`. |
| anything else | `text/plain` | `no-store` | `404`. Missing catalog / missing artifact are `404` with a one-line body. |

The `file` field of an entry is exactly the URL path minus the leading `/`:
entry `"file": "artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib"` is
served at `GET /artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib`.
R2 keys are the same strings (`artifacts/...`, `index.json`).

The index URL is the repository base URL: a client that knows only
`https://<host>` fetches `https://<host>/index.json` first.

## Versioning and immutability

- An existing `name` + `version` + `platform` artifact is **immutable**. The
  bytes at its `file` path never change; its `sha256` never changes.
- Re-publishing the same coordinates is **refused**: `publish.py` checks the
  current index and exits non-zero (naming the published artifact) instead of
  overwriting. To change an artifact, publish a new `version` (or, for
  breaking content changes, a new `name`); to target another platform, publish
  under a different `platform` tag.
- The catalog only grows by appending entries for new coordinates. `updated`
  is refreshed on every successful publish. Removal/tombstones are not part of
  v1 (reserved for a later format version).
- Artifacts are cached aggressively because they are immutable; the catalog is
  cached briefly (`max-age=60`) so a publish is visible within a minute.

## Platform tags

Tags are lowercase `<os>-<arch>` strings. Format v1 currently defines:

| Tag | Status |
|---|---|
| `darwin-arm64` | published (the only tag produced today) |
| `darwin-x64` | reserved |
| `linux-x64` | reserved |
| `linux-arm64` | reserved |
| `windows-x64` | reserved |

Reserved tags are allocated for future builds; publishing under them is
accepted (the tag list is closed, unknown tags are rejected). A client MUST
match its own platform exactly.

## Integrity

`sha256` is the SHA-256 of the raw artifact file as served. Clients MUST verify
the download against the index entry before loading the library.

The catalog itself is authenticated: `/index.json.sig` carries an Ed25519
signature over the exact `/index.json` bytes, and clients MUST verify it against
the public key pinned in the binary **before downloading any artifact** (the
signature transitively authenticates every `sha256` in the catalog). A missing,
malformed, unknown-key, or invalid signature is fatal; there is no unsigned
fallback except an explicitly warned development flag. The format is specified
in [`../docs/extension-signing.md`](../docs/extension-signing.md); the public
key is [`keys/extension-signing.pub.pem`](keys/extension-signing.pub.pem).

`verify.sh` performs both checks locally (signature + every artifact hash) and
against a live repository.

## Publishing

Use `publish.py` (see `README.md` for the walkthrough). It computes the hash
and size, enforces the immutability rule above against the current index,
stages the artifact at `artifacts/<name>/<version>/<platform>/<file>`, rewrites
`index.json` (sorted, fresh `updated`), signs the exact bytes (when
`PINKDB_EXT_SIGNING_KEY` is set) into `index.json.sig`, and uploads the
artifact, signature, and catalog to the R2 bucket with
`wrangler r2 object put --remote`.
