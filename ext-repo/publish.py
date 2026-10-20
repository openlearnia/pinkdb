#!/usr/bin/env python3
"""Publish one built extension into the pinkdb extension repository (format v1).

Stdlib only; the only external tool it calls is `wrangler` (for the R2 uploads).

Usage:
    publish.py <dylib> --name NAME --version VERSION --platform PLATFORM \
               --description DESC [--repo-dir DIR]

What it does:
  1. computes the SHA-256 and size (bytes) of <dylib>;
  2. refuses the publish if name+version+platform already exists in the current
     index (published coordinates are immutable — bump the version instead);
  3. copies the artifact to artifacts/<name>/<version>/<platform>/<file>;
  4. appends the entry to index.json (sorted, fresh `updated` timestamp;
     the entry records `sha256` and the optional-but-recommended `size`);
  5. uploads the artifact, the detached signature (when signing), and the
     catalog to the R2 bucket with `wrangler r2 object put --remote`
     (artifact first, catalog last);
  6. prints the resulting URLs.

Signing (see ../docs/extension-signing.md): when `PINKDB_EXT_SIGNING_KEY` is
set — to a PEM file path, PEM content, or base64 of the PEM — `index.json` is
signed with Ed25519 (`openssl pkeyutl -sign -rawin`) and `index.json.sig` is
uploaded next to it. Without the variable the publish stays unsigned (the
historical behavior) and prints a loud warning: a client that pins the key
will refuse that catalog.

Safe to re-run: a partial failure (e.g. the catalog upload) leaves the index
unchanged locally, so re-running completes the publish. Re-publishing existing
coordinates is refused by design.

See FORMAT.md for the repository format.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

FORMAT_VERSION = 1
BUCKET = os.environ.get("PINKDB_EXT_BUCKET", "pinkdb-extensions")
BASE_URL = os.environ.get(
    "PINKDB_EXT_REPO_URL", "https://pinkdb-ext-repo.defaultcms.workers.dev"
).rstrip("/")

# Catalog signing (docs/extension-signing.md).
SIGNING_KEY_ENV = "PINKDB_EXT_SIGNING_KEY"
SIGNATURE_FILE = "index.json.sig"
SIGNATURE_PREFIX = "PINKDB-SIG1"

# Platform tags defined by FORMAT.md v1: darwin-arm64 is the tag produced
# today; the others are reserved for future builds.
KNOWN_PLATFORMS = (
    "darwin-arm64",
    "darwin-x64",
    "linux-x64",
    "linux-arm64",
    "windows-x64",
)

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")


def die(message: str) -> "NoReturn":  # noqa: F821 - typing shortcut
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def version_key(version: str) -> list:
    """Numeric-aware sort key so 0.9.0 sorts before 0.10.0."""
    parts = []
    for part in version.split("."):
        if part.isdigit():
            parts.append((0, int(part), ""))
        else:
            parts.append((1, 0, part))
    return parts


def load_index(path: str) -> dict:
    if not os.path.exists(path):
        return {"format_version": FORMAT_VERSION, "updated": None, "extensions": []}
    with open(path, "r", encoding="utf-8") as f:
        try:
            index = json.load(f)
        except json.JSONDecodeError as exc:
            die(f"{path} is not valid JSON: {exc}")
    if index.get("format_version") != FORMAT_VERSION:
        die(
            f"{path}: unsupported format_version {index.get('format_version')!r} "
            f"(expected {FORMAT_VERSION})"
        )
    if not isinstance(index.get("extensions"), list):
        die(f"{path}: 'extensions' must be a list")
    return index


def r2_put(key: str, file_path: str, content_type: str, cwd: str) -> None:
    cmd = [
        "wrangler",
        "r2",
        "object",
        "put",
        f"{BUCKET}/{key}",
        "--file",
        file_path,
        "--remote",
        "--content-type",
        content_type,
    ]
    proc = subprocess.run(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout or "")
        die(f"wrangler failed to upload {BUCKET}/{key} (exit {proc.returncode})")
    print(f"uploaded {BUCKET}/{key}")


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def _openssl(args: list, what: str) -> bytes:
    """Run openssl with no secret on the command line; return stdout."""
    try:
        proc = subprocess.run(
            ["openssl", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    except FileNotFoundError:
        die(
            "openssl is required to sign the catalog (OpenSSL 3.x: "
            "`openssl pkeyutl -sign -rawin`) but was not found on PATH"
        )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr.decode("utf-8", "replace"))
        die(f"openssl failed to {what} (exit {proc.returncode})")
    return proc.stdout


def _temp_pem(pem: str) -> str:
    """Write PEM content to a 0600 temp file and return its path."""
    fd, path = tempfile.mkstemp(prefix="pinkdb-signing-key-", suffix=".pem")
    data = (pem if pem.endswith("\n") else pem + "\n").encode("ascii")
    try:
        os.chmod(path, 0o600)
        os.write(fd, data)
    except BaseException:
        os.close(fd)
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    os.close(fd)
    return path


def resolve_signing_key(value: str) -> tuple:
    """Resolve `PINKDB_EXT_SIGNING_KEY` to (key path, temp path or None).

    Accepts a PEM file path, PEM content, or base64 of the PEM. The private
    key is never printed or logged.
    """
    text = value.strip()
    if not text:
        die(f"{SIGNING_KEY_ENV} is set but empty")
    if "PRIVATE KEY-----" in text:
        return _temp_pem(text), True
    if os.path.isfile(text):
        return os.path.abspath(text), False
    try:
        decoded = base64.b64decode("".join(text.split()), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        die(
            f"{SIGNING_KEY_ENV} is not a PEM file path, PEM content, or base64 "
            "of a PEM private key"
        )
    if "PRIVATE KEY-----" not in decoded:
        die(f"{SIGNING_KEY_ENV} decoded to a value that is not a PEM private key")
    return _temp_pem(decoded), True


def _spki_key_id(der: bytes) -> str:
    return hashlib.sha256(der).hexdigest()[:16]


def _pinned_key_id(repo_dir: str):
    path = os.path.join(repo_dir, "keys", "extension-signing.pub.pem")
    if not os.path.isfile(path):
        return None
    try:
        proc = subprocess.run(
            ["openssl", "pkey", "-pubin", "-in", path, "-outform", "DER"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    return _spki_key_id(proc.stdout)


def sign_catalog(index_path: str, repo_dir: str, signature_path: str):
    """Sign the exact `index_path` bytes and write the detached envelope.

    Returns `(key_id, signature_path)` or `None` when the key variable is
    absent (the historical unsigned behavior), with a loud warning.
    """
    value = os.environ.get(SIGNING_KEY_ENV)
    if value is None or not value.strip():
        warn(
            f"{SIGNING_KEY_ENV} is not set: publishing an UNSIGNED catalog. "
            "Clients that pin the signing key WILL REFUSE it (fail closed). "
            "Set the variable to sign."
        )
        return None

    key_path, cleanup = resolve_signing_key(value)
    try:
        mode = os.stat(key_path).st_mode & 0o777
        if mode & 0o077:
            warn(
                f"signing key {key_path} is accessible by group/other "
                f"(mode {mode:o}); restrict it to 0600"
            )
        # Key id = first 16 hex of SHA-256(SPKI DER).
        public_der = _openssl(
            ["pkey", "-in", key_path, "-pubout", "-outform", "DER"],
            "derive the signing public key",
        )
        key_id = _spki_key_id(public_der)
        pinned = _pinned_key_id(repo_dir)
        if pinned is not None and pinned != key_id:
            warn(
                f"signing key id {key_id} differs from the in-tree public key "
                f"{pinned}: clients that pin {pinned} will refuse this catalog "
                "until a binary ships the new key (rotation)"
            )
        signature = _openssl(
            [
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                key_path,
                "-in",
                index_path,
            ],
            "sign index.json",
        )
        if len(signature) != 64:
            die(
                f"openssl produced a {len(signature)}-byte signature; "
                "expected a 64-byte Ed25519 signature"
            )
        envelope = (
            f"{SIGNATURE_PREFIX} {key_id} "
            f"{base64.b64encode(signature).decode('ascii')}\n"
        )
        with open(signature_path, "w", encoding="ascii") as f:
            f.write(envelope)
        return key_id, signature_path
    finally:
        if cleanup:
            try:
                os.unlink(key_path)
            except OSError:
                pass


def coordinates(entries: list) -> set:
    return {(e.get("name"), e.get("version"), e.get("platform")) for e in entries}


def check_against_live(entries: list) -> None:
    """Refuse to upload a catalog that would drop entries the live one has.

    Only a definite HTTP 404 means "no live catalog yet"; any other failure
    stops the publish (an index built from a stale or empty local copy would
    otherwise wipe live entries).
    """
    url = f"{BASE_URL}/index.json"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            live = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return
        die(f"cannot fetch the live catalog {url} (HTTP {e.code}); refusing to publish")
    except (urllib.error.URLError, OSError, ValueError) as e:
        die(f"cannot fetch the live catalog {url} ({e}); refusing to publish")
    live_entries = live.get("extensions") if isinstance(live, dict) else None
    if not isinstance(live_entries, list):
        die(f"live catalog {url} is malformed; refusing to publish")
    missing = coordinates(live_entries) - coordinates(entries)
    if missing:
        shown = ", ".join(f"{n} {v} ({p})" for n, v, p in sorted(missing, key=str)[:5])
        die(
            f"the local index.json lacks {len(missing)} live entr"
            f"{'y' if len(missing) == 1 else 'ies'} ({shown}); fetch the live "
            "catalog first — publishing would remove them"
        )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="publish.py",
        description="Publish one built extension into the pinkdb extension repository.",
    )
    parser.add_argument("dylib", help="path to the built extension (shared library)")
    parser.add_argument("--name", required=True, help="extension name, e.g. math_ext")
    parser.add_argument("--version", required=True, help="artifact version, e.g. 0.1.0")
    parser.add_argument(
        "--platform",
        required=True,
        help=f"platform tag (one of: {', '.join(KNOWN_PLATFORMS)})",
    )
    parser.add_argument(
        "--description", required=True, help="one-line description of the extension"
    )
    parser.add_argument(
        "--skip-live-check",
        action="store_true",
        help="do not compare against the live catalog (offline/testing only)",
    )
    parser.add_argument(
        "--repo-dir",
        default=None,
        help="repository working directory (default: this script's directory)",
    )
    args = parser.parse_args(argv)

    repo_dir = os.path.abspath(
        args.repo_dir or os.path.dirname(os.path.abspath(__file__))
    )
    index_path = os.path.join(repo_dir, "index.json")

    if not NAME_RE.match(args.name):
        die(f"invalid --name {args.name!r} (allowed: letters, digits, '_', '.', '-')")
    if not VERSION_RE.match(args.version):
        die(
            f"invalid --version {args.version!r} (allowed: letters, digits, '.', '_', '+', '-')"
        )
    if args.platform not in KNOWN_PLATFORMS:
        die(
            f"unknown --platform {args.platform!r}; known tags: "
            f"{', '.join(KNOWN_PLATFORMS)}"
        )
    if "\n" in args.description or "\r" in args.description:
        die("--description must be a single line")
    description = args.description.strip()
    if not description:
        die("--description must not be empty")
    if not os.path.isfile(args.dylib):
        die(f"artifact not found: {args.dylib}")
    file_name = os.path.basename(args.dylib)
    if not NAME_RE.match(file_name):
        die(f"invalid artifact file name {file_name!r}")

    # Immutability: an existing name+version+platform is never re-published.
    index = load_index(index_path)
    entries = index["extensions"]
    for entry in entries:
        if (
            entry.get("name"),
            entry.get("version"),
            entry.get("platform"),
        ) == (args.name, args.version, args.platform):
            die(
                f"refusing to re-publish {args.name} {args.version} "
                f"({args.platform}): already published as {entry.get('file')} "
                f"(sha256 {entry.get('sha256')}); published coordinates are "
                "immutable — bump the version instead"
            )

    sha = sha256_file(args.dylib)
    size = os.path.getsize(args.dylib)
    file_field = f"artifacts/{args.name}/{args.version}/{args.platform}/{file_name}"

    # Stage the artifact locally (a mirror of what goes to R2).
    staged = os.path.join(repo_dir, *file_field.split("/"))
    os.makedirs(os.path.dirname(staged), exist_ok=True)
    shutil.copyfile(args.dylib, staged)
    if sha256_file(staged) != sha or os.path.getsize(staged) != size:
        die(f"staged copy {staged} does not match the source (copy failed)")

    entry = {
        "name": args.name,
        "version": args.version,
        "abi": 1,
        "platform": args.platform,
        "file": file_field,
        "sha256": sha,
        "size": size,  # optional-but-recommended in format v1
        "description": description,
    }

    # New catalog in memory: append, sort, timestamp.
    entries.append(entry)
    entries.sort(key=lambda e: (e["name"], version_key(e["version"]), e["platform"]))
    index["format_version"] = FORMAT_VERSION
    index["updated"] = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    if not args.skip_live_check:
        check_against_live(entries)

    index_tmp = index_path + ".tmp"
    with open(index_tmp, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # Sign the exact bytes that are about to be uploaded. `index_tmp` is only
    # renamed into place after both uploads succeed, so the local mirror and
    # its signature always describe the same bytes.
    signature_tmp = index_tmp + ".sig"
    try:
        signed = sign_catalog(index_tmp, repo_dir, signature_tmp)
    except BaseException:
        for path in (index_tmp, signature_tmp):
            if os.path.exists(path):
                os.remove(path)
        raise

    # Upload artifact first, signature second, catalog last: if the catalog
    # upload fails, an unreferenced object remains (harmless and re-runnable)
    # instead of a catalog entry pointing at a missing artifact. A signature
    # uploaded before the catalog can only describe the new bytes, and a
    # stale signature never verifies (clients fail closed).
    try:
        r2_put(file_field, staged, "application/octet-stream", repo_dir)
        if signed is not None:
            r2_put(
                SIGNATURE_FILE,
                signature_tmp,
                "text/plain; charset=utf-8",
                repo_dir,
            )
        r2_put("index.json", index_tmp, "application/json", repo_dir)
    except SystemExit:
        for path in (index_tmp, signature_tmp):
            if os.path.exists(path):
                os.remove(path)
        raise
    os.replace(index_tmp, index_path)
    if signed is not None:
        os.replace(signature_tmp, index_path + ".sig")
    else:
        # Never leave a stale local signature next to an unsigned catalog.
        stale = index_path + ".sig"
        if os.path.exists(stale):
            os.remove(stale)
            warn(f"removed the stale local {os.path.basename(stale)}")

    print(f"published {args.name} {args.version} ({args.platform})")
    print(f"  sha256:   {sha}")
    print(f"  size:     {size} bytes")
    print(f"  artifact: {BASE_URL}/{file_field}")
    print(f"  catalog:  {BASE_URL}/index.json")
    if signed is not None:
        key_id, _ = signed
        print(f"  signature: {BASE_URL}/{SIGNATURE_FILE} (key {key_id})")
    else:
        print("  signature: NONE (unsigned catalog)")
    print(f"  staged:   {staged}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
