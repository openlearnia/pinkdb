#!/usr/bin/env python3
"""Discover and stage built extension libraries for one platform.

The extension publish workflow builds the SDK examples and every
``extensions/*/`` Makefile in a per-platform matrix, then fans in to one
publish job. This script is the bridge: it applies exactly the catalog's
discovery rules (the same rules the old single-job workflow implemented) and
writes a small manifest plus a flat directory of artifacts that can be
uploaded as one GitHub Actions artifact.

    python3 scripts/release/extensions.py discover \
        --platform darwin-arm64 --version 0.1.0 \
        --root . --out ext-dist/darwin-arm64

Manifest columns (tab-separated, one line per artifact):

    name  version  platform  file  description  sha256

Only ``ext-sdk/c`` examples and ``extensions/<name>/build|dist`` libraries
are catalog candidates. Cargo-built core extensions (json, math, datetime,
arrow, parquet) are linked into the CLI rather than shipped as loadable
catalog artifacts, and the Go/Rust SDK examples are built and smoked but are
not catalog entries today: see docs/releasing.md for that documented gap.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

KNOWN_PLATFORMS = (
    "darwin-arm64",
    "darwin-x64",
    "linux-x64",
    "linux-arm64",
    "windows-x64",
)

# Descriptions for the in-repo C/C++ SDK examples.
EXAMPLE_DESCRIPTIONS = {
    "math_ext": "Clamp and degree/radian helpers (ext_clamp, ext_deg2rad)",
    "string_ext": "String helpers (ext_repeat, ext_title)",
    "cpp_ext": "Checked integer addition (ext_add_checked)",
}


def lib_ext(platform: str) -> str:
    if platform.startswith("darwin"):
        return ".dylib"
    if platform.startswith("linux"):
        return ".so"
    if platform.startswith("windows"):
        return ".dll"
    raise SystemExit(f"error: unsupported platform {platform!r}")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def candidates(root: Path, platform: str) -> list[tuple[str, Path, str]]:
    ext = lib_ext(platform)
    found: list[tuple[str, Path, str]] = []

    # 1) The C/C++ SDK worked examples: examples/*.c(pp) -> build/<stem><ext>.
    examples = sorted((root / "ext-sdk/c/examples").glob("*.c")) + sorted(
        (root / "ext-sdk/c/examples").glob("*.cpp")
    )
    for src in examples:
        stem = src.stem
        found.append(
            (
                stem,
                root / "ext-sdk/c/build" / f"{stem}{ext}",
                EXAMPLE_DESCRIPTIONS.get(stem, f"pinkdb C SDK example {stem}"),
            )
        )

    # 2) Repo extensions: extensions/<name>/ with exactly one built library
    #    under build/ or dist/. Ambiguity is an error, never a guess.
    for directory in sorted((root / "extensions").glob("*/")):
        if not directory.is_dir():
            continue
        name = directory.name
        libs: list[Path] = []
        for sub in ("build", "dist"):
            libs.extend(sorted((directory / sub).glob(f"*{ext}")))
        if not libs:
            continue
        if len(libs) > 1:
            raise SystemExit(
                f"error: extensions/{name}/ has {len(libs)} {ext} files under "
                "build//dist/ — expected exactly one, so a publish is never ambiguous"
            )
        description_file = directory / "description.txt"
        if description_file.is_file():
            description = description_file.read_text(encoding="utf-8").strip()
            if not description:
                raise SystemExit(f"error: extensions/{name}/description.txt is empty")
        else:
            description = f"Extension built from extensions/{name}"
        found.append((name, libs[0], description))

    return found


def cmd_discover(args: argparse.Namespace) -> int:
    platform = args.platform
    if platform not in KNOWN_PLATFORMS:
        raise SystemExit(
            f"error: unknown platform {platform!r}; known tags: {', '.join(KNOWN_PLATFORMS)}"
        )
    root = Path(args.root).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)

    rows = candidates(root, platform)
    if args.name:
        rows = [row for row in rows if row[0] == args.name]
        if not rows:
            raise SystemExit(f"error: no discovered extension named {args.name!r}")

    seen: dict[str, str] = {}
    manifest = out / "manifest.tsv"
    lines: list[str] = []
    for name, artifact, description in rows:
        if not artifact.is_file():
            raise SystemExit(f"error: {name}: expected artifact {artifact} does not exist")
        filename = artifact.name
        if filename in seen:
            raise SystemExit(
                f"error: {name} and {seen[filename]} both produce {filename}; "
                "catalog filenames must be unique"
            )
        seen[filename] = name
        target = out / filename
        shutil.copy2(artifact, target)
        digest = sha256(target)
        lines.append(
            "\t".join((name, args.version, platform, filename, description, digest))
        )
        print(f"  {name} {args.version} ({platform}) -> {filename} sha256 {digest}")

    manifest.write_text("".join(line + "\n" for line in lines), encoding="utf-8")
    print(f"staged {len(lines)} artifact(s) for {platform} in {out}")
    return 0


def expected_arch_tokens(platform: str) -> tuple[str, ...]:
    if platform in ("darwin-arm64", "linux-arm64"):
        return ("arm64", "aarch64")
    if platform in ("darwin-x64", "linux-x64", "windows-x64"):
        return ("x86_64", "x86-64")
    raise SystemExit(f"error: unknown platform {platform!r}")


def cmd_verify_arch(args: argparse.Namespace) -> int:
    tokens = expected_arch_tokens(args.platform)
    failed = 0
    for raw in args.files:
        path = Path(raw)
        if not path.is_file():
            print(f"FAIL {path}: missing", file=sys.stderr)
            failed += 1
            continue
        try:
            description = subprocess.check_output(
                ["file", "-b", str(path)], text=True, stderr=subprocess.STDOUT
            )
        except FileNotFoundError:
            raise SystemExit("error: `file` is required to verify artifact architecture")
        except subprocess.CalledProcessError as e:
            print(f"FAIL {path}: file failed: {e.output}", file=sys.stderr)
            failed += 1
            continue
        if not any(token in description for token in tokens):
            print(
                f"FAIL {path}: expected {args.platform} ({'|'.join(tokens)}), "
                f"file says: {description.strip()}",
                file=sys.stderr,
            )
            failed += 1
        else:
            print(f"ok   {path}: {description.strip()}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="stage artifacts + manifest for one platform")
    discover.add_argument("--platform", required=True, choices=KNOWN_PLATFORMS)
    discover.add_argument("--version", required=True)
    discover.add_argument("--root", default=".")
    discover.add_argument("--out", required=True)
    discover.add_argument("--name", default="", help="optional single extension name")
    discover.set_defaults(func=cmd_discover)

    verify = sub.add_parser("verify-arch", help="check Mach-O/ELF architecture with `file`")
    verify.add_argument("--platform", required=True, choices=KNOWN_PLATFORMS)
    verify.add_argument("files", nargs="+")
    verify.set_defaults(func=cmd_verify_arch)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
