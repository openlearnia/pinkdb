#!/usr/bin/env python3
"""The source hash of the console's browser app (wave 121).

FNV-1a 64 over every source file of `crates/pinkdb-server-ui-web` and
`crates/pinkdb-server-ui-api`, in sorted path order (path, NUL, content,
NUL). `crates/pinkdb-server-ui/web-dist/SOURCE_HASH` records it at build
time; the console's `web_dist_is_fresh` test recomputes it the same way and
fails when the committed build is older than the sources.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRATES = ["crates/pinkdb-server-ui-web", "crates/pinkdb-server-ui-api"]
TOP_FILES = ["Cargo.toml", "Cargo.lock", "index.html"]


def files():
    out = []
    for crate in CRATES:
        for name in TOP_FILES:
            path = os.path.join(crate, name)
            if os.path.isfile(os.path.join(ROOT, path)):
                out.append(path)
        for dirpath, _, names in os.walk(os.path.join(ROOT, crate, "src")):
            for name in names:
                if name.endswith(".rs"):
                    out.append(os.path.relpath(os.path.join(dirpath, name), ROOT))
    return sorted(p.replace(os.sep, "/") for p in out)


def fnv(data, h):
    for b in data:
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def source_hash():
    h = 0xCBF29CE484222325
    for path in files():
        h = fnv(path.encode() + b"\0", h)
        with open(os.path.join(ROOT, path), "rb") as f:
            h = fnv(f.read() + b"\0", h)
    return "%016x" % h


if __name__ == "__main__":
    sys.stdout.write(source_hash() + "\n")
