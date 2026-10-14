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
  5. uploads the artifact and the catalog to the R2 bucket with
     `wrangler r2 object put --remote` (artifact first, catalog second);
  6. prints the resulting URLs.

Safe to re-run: a partial failure (e.g. the catalog upload) leaves the index
unchanged locally, so re-running completes the publish. Re-publishing existing
coordinates is refused by design.

See FORMAT.md for the repository format.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request

FORMAT_VERSION = 1
BUCKET = os.environ.get("PINKDB_EXT_BUCKET", "pinkdb-extensions")
BASE_URL = os.environ.get(
    "PINKDB_EXT_REPO_URL", "https://pinkdb-ext-repo.defaultcms.workers.dev"
).rstrip("/")

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

    # Upload artifact first, catalog second: if the catalog upload fails, an
    # unreferenced object remains (harmless and re-runnable) instead of a
    # catalog entry pointing at a missing artifact.
    try:
        r2_put(file_field, staged, "application/octet-stream", repo_dir)
        r2_put("index.json", index_tmp, "application/json", repo_dir)
    except SystemExit:
        if os.path.exists(index_tmp):
            os.remove(index_tmp)
        raise
    os.replace(index_tmp, index_path)

    print(f"published {args.name} {args.version} ({args.platform})")
    print(f"  sha256:   {sha}")
    print(f"  size:     {size} bytes")
    print(f"  artifact: {BASE_URL}/{file_field}")
    print(f"  catalog:  {BASE_URL}/index.json")
    print(f"  staged:   {staged}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
