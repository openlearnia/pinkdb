#!/usr/bin/env python3
"""Generate the pinkdb documentation site — Python 3 stdlib only.

Reads HTML fragments from ``src/pages/`` and the shared template
``src/layout.html``, fills the ``{{...}}`` placeholders, and writes the
static site to ``public/``. Assets under ``src/assets/`` are copied
verbatim to ``public/assets/``.

Output mapping
--------------
- ``src/pages/index.html``          -> ``public/index.html``
- ``src/pages/404.html``            -> ``public/404.html``
- ``src/pages/extensions.html``     -> ``public/extensions.html`` (top level, so
  the page is served at ``/extensions``)
- any other fragment                -> ``public/docs/<same name>``

The first three have explicit ``out`` entries in ``PAGES``; every other
fragment uses the ``docs/`` default.

Template placeholders
---------------------
- ``{{title}}`` / ``{{description}}`` — page metadata (HTML-escaped)
- ``{{content}}`` — the page fragment
- ``{{root}}`` — relative path to the site root (``""`` or ``"../"``)
- ``{{nav-<key>}}`` — expanded to the active-link attributes on the page
  whose ``nav`` key matches, empty otherwise
- ``{{python-binding}}`` — the Python binding section: a description when
  ``bindings/python`` exists in the repo, a "queued" note otherwise
"""

from __future__ import annotations

import html
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
PAGES_DIR = SRC / "pages"
OUT = ROOT / "public"
LAYOUT = SRC / "layout.html"

PLACEHOLDER = re.compile(r"\{\{[a-z0-9-]+\}\}")

# Per-page metadata. Fragments without an entry still generate (under
# docs/) with a title derived from the file name and no nav highlight.
PAGES = {
    "index.html": {
        "out": "index.html",
        "nav": "home",
        "title": "pinkdb — a mini DuckDB in Rust",
        "description": (
            "An embedded analytical SQL database in Rust: columnar vectors, "
            "vectorized execution, CSV import, durable .pink files with a WAL, "
            "a network server, and a C ABI."
        ),
    },
    "getting-started.html": {
        "out": "docs/getting-started.html",
        "nav": "getting-started",
        "title": "Getting started — pinkdb",
        "description": (
            "Build the pinkdb CLI, import a CSV, run your first queries, and "
            "learn the REPL basics."
        ),
    },
    "sql.html": {
        "out": "docs/sql.html",
        "nav": "sql",
        "title": "SQL reference — pinkdb",
        "description": (
            "The pinkdb v1 SQL surface: DDL/DML, joins, CTEs, subqueries, "
            "window functions, types, dates, three-valued logic, and "
            "multi-database sessions."
        ),
    },
    "cli.html": {
        "out": "docs/cli.html",
        "nav": "cli",
        "title": "CLI & REPL — pinkdb",
        "description": (
            "pinkdb-cli dot-commands, box-drawn output and timing, "
            "PINKDB_PARSER, PINKDB_KEY, and remote mode."
        ),
    },
    "server.html": {
        "out": "docs/server.html",
        "nav": "server",
        "title": "Server & protocol — pinkdb",
        "description": (
            "Run pinkdb-server over TCP, the framed binary protocol, remote "
            "CLI mode, robustness limits, and the security posture."
        ),
    },
    "persistence.html": {
        "out": "docs/persistence.html",
        "nav": "persistence",
        "title": "Persistence & encryption — pinkdb",
        "description": (
            "The .pink columnar format, write-ahead log crash recovery, and "
            "optional at-rest encryption (PINKENC1, AES-256-GCM + Argon2id)."
        ),
    },
    "bindings.html": {
        "out": "docs/bindings.html",
        "nav": "bindings",
        "title": "Bindings — pinkdb",
        "description": (
            "The pinkdb C ABI (pinkdb-ffi): artifacts, ownership and threading "
            "contract, and the C example."
        ),
    },
    "architecture.html": {
        "out": "docs/architecture.html",
        "nav": "architecture",
        "title": "Architecture — pinkdb",
        "description": (
            "The pinkdb query pipeline, module map, vectorized execution "
            "model, and design rules (zero async, zero unsafe in the core)."
        ),
    },
    "roadmap.html": {
        "out": "docs/roadmap.html",
        "nav": "roadmap",
        "title": "Roadmap — pinkdb",
        "description": (
            "Waves 0-26 shipped, 27-47 queued: bindings, extensions, lake "
            "storage, TLS, RBAC, and more."
        ),
    },
    "extensions.html": {
        "out": "extensions.html",
        "nav": "extensions",
        "title": "Extensions — pinkdb",
        "description": (
            "The pinkdb extension repository: browse published extension "
            "artifacts (name, version, platform, ABI, SHA-256), install with "
            ".install <name>, and download the immutable bytes."
        ),
    },
    "404.html": {
        "out": "404.html",
        "nav": "",
        "title": "Page not found — pinkdb",
        "description": "That page does not exist. Head back to the pinkdb docs.",
    },
}


def python_binding_section() -> str:
    """Build-time check for the Python binding (wave 27)."""
    binding_dir = ROOT.parent / "bindings" / "python"
    if binding_dir.is_dir():
        return (
            '<p>The Python binding lives in <code>bindings/python</code> — '
            "<code>ctypes</code> over the C ABI, exposing "
            "<code>connect()</code>, <code>execute()</code>, and a "
            "<code>Result</code> object. See the README in that directory for "
            "build steps and the current API (wave 27 in the roadmap).</p>"
        )
    return (
        '<div class="callout">\n'
        '  <span class="callout-title">Not in the repo yet</span>\n'
        "  <p>A Python binding (wave 27: <code>ctypes</code> over the C ABI) is "
        "queued but was not present at build time, so this page documents the "
        "C ABI only. In the meantime, <code>ctypes</code> or <code>cffi</code> "
        "can load <code>libpinkdb_ffi</code> directly using the header "
        "contract above.</p>\n"
        "</div>"
    )


def load_nav_keys(layout: str) -> list[str]:
    keys = []
    for match in re.finditer(r"\{\{nav-([a-z0-9-]+)\}\}", layout):
        if match.group(1) not in keys:
            keys.append(match.group(1))
    if not keys:
        sys.exit("error: layout.html contains no {{nav-*}} hooks")
    return keys


def page_meta(fragment_name: str) -> dict[str, str]:
    if fragment_name in PAGES:
        return dict(PAGES[fragment_name])
    stem = fragment_name.removesuffix(".html")
    return {
        "out": f"docs/{fragment_name}",
        "nav": "",
        "title": f"{stem.replace('-', ' ').replace('_', ' ').title()} — pinkdb",
        "description": "pinkdb documentation.",
    }


def expand(text: str, replacements: dict[str, str]) -> str:
    for token, value in replacements.items():
        text = text.replace(token, value)
    return text


_HREF_RE = re.compile(r'href="((?:\.\./|\./)*)([A-Za-z0-9_\-/]+)\.html"')


def extensionless_links(page: str) -> str:
    """Emit canonical extensionless internal links.

    Cloudflare Workers static assets serve ``docs/sql.html`` at
    ``docs/sql``, so linking the extensionless path avoids a redirect on
    every click. ``index.html`` becomes the directory root; external URLs,
    fragments, and asset paths are untouched.
    """

    def replace(match: re.Match[str]) -> str:
        prefix, path = match.group(1), match.group(2)
        if path == "index":
            return f'href="{prefix or "./"}"'
        if path.endswith("/index"):
            return f'href="{prefix}{path[: -len("index")]}"'
        return f'href="{prefix}{path}"'

    return _HREF_RE.sub(replace, page)


def render(layout: str, nav_keys: list[str], fragment_name: str,
           meta: dict[str, str]) -> str:
    out_rel = meta["out"]
    root = "../" * out_rel.count("/")

    # Expand placeholders inside the fragment first (they may use {{root}},
    # {{python-binding}}), then splice the result into the layout.
    fragment = (PAGES_DIR / fragment_name).read_text(encoding="utf-8")
    fragment = expand(fragment, {
        "{{root}}": root,
        "{{python-binding}}": python_binding_section(),
    })

    replacements = {
        "{{title}}": html.escape(meta["title"]),
        "{{description}}": html.escape(meta["description"]),
        "{{root}}": root,
        "{{python-binding}}": python_binding_section(),
        "{{content}}": fragment,
    }
    for key in nav_keys:
        active = ' class="active" aria-current="page"' if key == meta["nav"] else ""
        replacements[f"{{{{nav-{key}}}}}"] = active

    page = expand(layout, replacements)

    leftover = sorted(set(PLACEHOLDER.findall(page)))
    if leftover:
        raise SystemExit(
            f"error: {fragment_name} left unexpanded placeholders: {leftover}"
        )
    if meta["nav"] and meta["nav"] not in nav_keys:
        raise SystemExit(
            f"error: {fragment_name} sets nav={meta['nav']!r} but layout.html "
            f"has no nav-{meta['nav']} hook"
        )
    return extensionless_links(page)


def main() -> int:
    if not LAYOUT.is_file():
        sys.exit(f"error: missing template {LAYOUT}")
    if not PAGES_DIR.is_dir():
        sys.exit(f"error: missing fragments directory {PAGES_DIR}")

    layout = LAYOUT.read_text(encoding="utf-8")
    nav_keys = load_nav_keys(layout)

    fragments = sorted(p.name for p in PAGES_DIR.iterdir() if p.suffix == ".html")
    if not fragments:
        sys.exit(f"error: no .html fragments in {PAGES_DIR}")

    if OUT.exists():
        shutil.rmtree(OUT)
    shutil.copytree(SRC / "assets", OUT / "assets")

    written: list[Path] = []
    for fragment_name in fragments:
        meta = page_meta(fragment_name)
        page = render(layout, nav_keys, fragment_name, meta)
        target = OUT / meta["out"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page, encoding="utf-8")
        written.append(target)

    files = sorted(p for p in OUT.rglob("*") if p.is_file())
    total = 0
    print("Generated site:")
    for path in files:
        size = path.stat().st_size
        total += size
        print(f"  {path.relative_to(OUT)}  ({size:,} bytes)")
    print(f"{len(written)} pages + "
          f"{len(files) - len(written)} assets, "
          f"total {total:,} bytes ({total / 1024:.1f} KiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
