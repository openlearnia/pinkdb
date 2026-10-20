# Extension signing

Status: **keys generated 2026-10-20; verification implementation pending** (security
review finding M-10). This document is the contract the publish pipeline and the
CLI must implement.

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
- **Public key** (in-tree): `ext-repo/keys/extension-signing.pub.pem` (SPKI PEM),
  also pinned as a raw 32-byte value in the CLI for offline verification.
- **Key id**: first 16 lowercase hex characters of `SHA-256(SPKI DER)`. The
  signature envelope carries it so multiple keys can coexist during rotation.
- **Rotation**: publish a catalog signed by the new key; ship a binary that pins
  both keys; drop the old key in a later release. Never reuse a key id.

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
2. Parse the envelope; select a pinned public key by key id
   (`PINKDB_EXT_REPO_PUBKEY` may override the pin for tests/dev only — it must be
   documented as insecure). Verify over the raw catalog bytes.
   **Fail closed before any artifact request** on a missing, malformed, unknown-key
   or invalid signature.
3. Look up the requested artifact in the now-authenticated catalog (platform, ABI,
   sha256); download the bytes; verify the SHA-256.
4. Write the artifact to a `0700` temp directory with `create_new`, re-hash the
   file, and only then `dlopen` it. Never `dlopen` on a failed verification.
   (Loading from an already-open descriptor is a recorded follow-up to shrink the
   TOCTOU window further.)
5. No fallback to unsigned mode: an unsigned catalog is refused unless the user
   passes an explicit, loudly warned dev flag.

## Publishing

`ext-repo/publish.py` assembles `index.json` as today, then (when
`PINKDB_EXT_SIGNING_KEY` is available) writes `index.json.sig` and uploads both.
`ext-repo/verify.sh` verifies a local index copy with the pinned public key.
The publish workflow (`workflow_dispatch`) passes the secret to the publish step
only.

## Out of scope here

Signing **locally loaded** extensions (`LOAD EXTENSION '<path>'`) is a separate
increment (review finding H-7 follow-up): an optional sidecar `<library>.sig` or a
configured `name → sha256` allowlist, verified before `dlopen`. The repository
signature above does not cover that path.
