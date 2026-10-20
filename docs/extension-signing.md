# Extension signing

Status: **implemented 2026-10-20** (security review finding M-10). The publish
pipeline signs the catalog and the CLI verifies it before any artifact request;
this document is the contract both implement.

## Threat model

`.install` verifies an artifact against a SHA-256 taken from `index.json`, but the
catalog and the artifact come from the **same origin**. Anyone who can serve
`index.json` (Worker, R2 bucket, Cloudflare account, or the TLS path) can serve a
catalog whose hash matches their own payload, and the CLI then `dlopen`s it.
SHA-256 protects against corruption, not against a hostile origin. Signing the
catalog with a key pinned in the binary closes that gap.

## Keys

- Algorithm: **Ed25519** (RFC 8032).
- **Private key** (never commit, never log):
  `~/.pinkdb/extension-signing-key.pem`, mode `0600`.
  CI uses the repository secret **`PINKDB_EXT_SIGNING_KEY`** (base64 of the PEM)
  for the publish workflow only.
- **Public key** (in-tree): `ext-repo/keys/extension-signing.pub.pem` (SPKI PEM).
  The CLI embeds that file verbatim at compile time (`include_str!`) and parses
  the raw 32-byte Ed25519 key from the SPKI DER at startup, so the pin travels
  with the binary and no file needs to exist at runtime.
- **Key id**: first 16 lowercase hex characters of `SHA-256(SPKI DER)`. The
  signature envelope carries it so multiple keys can coexist during rotation.
  The current key id is **`c84e54bfea6b2d09`**.
- **Rotation**: publish a catalog signed by the new key; ship a binary that pins
  both keys; drop the old key in a later release. Never reuse a key id. The
  publisher warns when the signing key id differs from the in-tree public key,
  which is the expected state during rotation.

Current public key (SPKI PEM):

```
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAX/LYg4NzJg98ba7jxdRiEK6BEBoE7V4A2x96GsGrL0w=
-----END PUBLIC KEY-----
```

## What is signed

The **exact bytes of `index.json`** — no canonicalization; `publish.py` emits it
deterministically. Signing the catalog transitively authenticates every artifact
hash listed in it.

Detached signature file `index.json.sig`, ASCII, one line:

```
PINKDB-SIG1 <keyid> <base64 Ed25519 signature of the index.json bytes>
```

Producing it (OpenSSL 3.x):

```sh
openssl pkeyutl -sign -rawin \
  -inkey ~/.pinkdb/extension-signing-key.pem \
  -in index.json -out index.json.sig.bin
printf 'PINKDB-SIG1 %s %s\n' "$KEYID" "$(base64 < index.json.sig.bin | tr -d '\n')" \
  > index.json.sig
```

Verifying a local copy:

```sh
base64 -d <<<"$(awk '{print $3}' index.json.sig)" > /tmp/sig.bin
openssl pkeyutl -verify -rawin -pubin \
  -inkey ext-repo/keys/extension-signing.pub.pem \
  -in index.json -sigfile /tmp/sig.bin
```

## Verification order in `.install`

1. Fetch `index.json` **and** `index.json.sig` from the configured repository.
   A definite 404 on the signature means "unsigned"; any other signature
   transport failure is a hard error (a flaky endpoint cannot silently disable
   signing).
2. Parse the envelope; select a pinned public key by key id. Verify over the raw
   catalog bytes. **Fail closed before any artifact request** on a missing,
   malformed, unknown-key or invalid signature. The catalog JSON is parsed only
   after the signature verifies.
3. Look up the requested artifact in the now-authenticated catalog (platform,
   ABI, sha256); download the bytes; verify the SHA-256.
4. Write the artifact to a `0700` temp directory with `create_new` (file mode
   `0600` on Unix), re-hash the on-disk file, and only then `dlopen` it. Never
   `dlopen` on a failed verification. The staging directory is removed after
   the load attempt (best effort; the mapped library remains). (Loading from an
   already-open descriptor is a recorded follow-up to shrink the TOCTOU window
   further.)
5. No fallback to unsigned mode: an unsigned catalog is refused unless the user
   passes the explicit dev flag below.

Two environment variables exist for development only:

| Variable | Effect |
|---|---|
| `PINKDB_EXT_REPO_PUBKEY` | Replaces the pinned public key with a PEM value or a path to a PEM file. Prints a loud warning on every install. **Insecure**: a hostile shell environment can trust its own key. |
| `PINKDB_EXT_ALLOW_UNSIGNED=1` | Skips catalog signature verification entirely (including a present-but-invalid signature) and prints a loud warning. **Insecure**: only for offline/local development. |

Neither variable is read by the default (feature-less) binary; both live behind
the `repo` feature alongside the verifier.

## Publishing

`ext-repo/publish.py` assembles `index.json` as today, then signs it when
`PINKDB_EXT_SIGNING_KEY` is set. The variable may be:

- a path to the PEM private key (`~/.pinkdb/extension-signing-key.pem`);
- the PEM content itself (for example a GitHub Actions secret);
- base64 of the PEM (the form the `PINKDB_EXT_SIGNING_KEY` repository secret
  uses).

Inline content is written to a `0600` temporary file for the duration of the
`openssl` call and unlinked immediately; the key is never printed or uploaded.
The publisher writes `index.json.sig` next to the catalog and uploads it before
the catalog (artifact → signature → catalog). A stale signature is never
uploaded for new bytes: if the variable is absent, the publish stays unsigned,
a loud warning is printed, and any stale local signature is removed. Pinned
clients will refuse an unsigned catalog (fail closed).

`ext-repo/verify.sh` verifies a local `index.json` + `index.json.sig` against
the pinned public key and checks every entry's artifact under the index directory:

```sh
./ext-repo/verify.sh                    # ./index.json, ./index.json.sig, ./keys/...
./ext-repo/verify.sh --skip-artifacts   # signature only
./ext-repo/verify.sh https://…/artifacts/…  # remote catalog + one artifact
```

The publish workflow (`workflow_dispatch`) passes the secret **only to the
publish step**, requires it to be non-empty, and adds a post-sign gate that
verifies the local and live catalog signatures plus every artifact published by
the run.

Deployment order matters: the Worker (`ext-repo/worker/src/index.js`) must serve
`/index.json.sig`, and a signed catalog must be published, **before** a
repo-enabled CLI that pins the key is released. Until then a pinned client fails
closed with "catalog signature is missing".

## Out of scope here

Signing **locally loaded** extensions (`LOAD EXTENSION '<path>'`) is a separate
increment (review finding H-7 follow-up): an optional sidecar `<library>.sig` or a
configured `name → sha256` allowlist, verified before `dlopen`. The repository
signature above does not cover that path.
