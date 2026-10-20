#!/usr/bin/env python3
"""Print the release build matrix as compact JSON (stdlib only).

The GitHub workflow uses this so the runner/target/variant list has one
definition that can also be checked locally:

    python3 scripts/release/matrix.py --kind cli
    python3 scripts/release/matrix.py --kind server
    python3 scripts/release/matrix.py --kind all

Each row is an object suitable for GitHub Actions' ``matrix.include``:
kind, variant, platform, os, arch, runner, target.

Runner policy (see docs/releasing.md):
  linux-x86_64  ubuntu-latest
  linux-aarch64 ubuntu-24.04-arm      (fallback: qemu/cross-build, documented)
  darwin-aarch64 macos-latest
  darwin-x86_64 macos-latest + rustup target add x86_64-apple-darwin
                (the arm runner cross-builds; smoke runs under Rosetta when
                available, otherwise the job fails rather than publishing an
                unexercised artifact)

Windows is deliberately absent: the workspace does not build there yet, so
there is no windows asset and no install.ps1. See docs/releasing.md.
"""

from __future__ import annotations

import argparse
import json
import sys

CLI_VARIANTS = (
    "default",
    "lean",
    "pinklake",
    "pinklake-s3",
    "vortexlake",
    "vortexlake-s3",
)
SERVER_VARIANTS = ("default", "async-server", "tls")

# Extension catalog platform tags (FORMAT.md v1) and the runner that builds
# each one. windows-x64 is representable in the catalog but is not built: the
# workspace does not build on Windows yet (docs/releasing.md).
EXT_PLATFORMS = (
    {
        "platform": "linux-x64",
        "os": "linux",
        "arch": "x86_64",
        "runner": "ubuntu-latest",
        "target": "",
        "cross": "false",
    },
    {
        "platform": "linux-arm64",
        "os": "linux",
        "arch": "aarch64",
        "runner": "ubuntu-24.04-arm",
        "target": "",
        "cross": "false",
    },
    {
        "platform": "darwin-arm64",
        "os": "darwin",
        "arch": "aarch64",
        "runner": "macos-latest",
        "target": "",
        "cross": "false",
    },
    {
        "platform": "darwin-x64",
        "os": "darwin",
        "arch": "x86_64",
        "runner": "macos-latest",
        "target": "x86_64-apple-darwin",
        "cross": "true",
    },
)

PLATFORMS = (
    {
        "platform": "linux-x86_64",
        "os": "linux",
        "arch": "x86_64",
        "runner": "ubuntu-latest",
        "target": "",
    },
    {
        "platform": "linux-aarch64",
        "os": "linux",
        "arch": "aarch64",
        "runner": "ubuntu-24.04-arm",
        "target": "",
    },
    {
        "platform": "darwin-aarch64",
        "os": "darwin",
        "arch": "aarch64",
        "runner": "macos-latest",
        "target": "",
    },
    {
        "platform": "darwin-x86_64",
        "os": "darwin",
        "arch": "x86_64",
        "runner": "macos-latest",
        "target": "x86_64-apple-darwin",
    },
)


def rows(kind: str, variants: tuple[str, ...]) -> list[dict[str, str]]:
    out = []
    for platform in PLATFORMS:
        for variant in variants:
            out.append({"kind": kind, "variant": variant, **platform})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kind", choices=("cli", "server", "all", "extensions"), default="all"
    )
    parser.add_argument(
        "--platform",
        default="",
        help="extensions only: emit just this catalog platform tag",
    )
    args = parser.parse_args()

    if args.kind == "cli":
        data = rows("cli", CLI_VARIANTS)
    elif args.kind == "server":
        data = rows("server", SERVER_VARIANTS)
    elif args.kind == "extensions":
        data = [dict(row) for row in EXT_PLATFORMS]
        if args.platform:
            data = [row for row in data if row["platform"] == args.platform]
            if not data:
                raise SystemExit(f"error: unknown extension platform {args.platform!r}")
    else:
        data = rows("cli", CLI_VARIANTS) + rows("server", SERVER_VARIANTS)

    json.dump(data, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
