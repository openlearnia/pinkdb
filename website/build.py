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
  the page is served at ``/extensions`` — the extension catalog)
- ``src/pages/extensions-guide.html`` -> ``public/docs/extensions.html``
- every other fragment              -> ``public/docs/<same name>``

The first three have explicit ``out`` entries in ``PAGES``; every other
fragment uses the ``docs/`` default.

Layouts
-------
- ``home``  (index.html): hero page, full-width container, no sidebar.
- ``docs``  (default): a left sidebar with the grouped documentation nav,
  active-page highlighting, and (on wide screens) an optional "on this page"
  table of contents generated from the fragment's ``<h2>`` headings.
- ``narrow`` (404.html): a centered single column, no sidebar.

Template placeholders
---------------------
- ``{{title}}`` / ``{{description}}`` — page metadata (HTML-escaped)
- ``{{url}}`` — the page's canonical URL on the live docs origin
- ``{{main}}`` — the assembled main column (sidebar + fragment + TOC)
- ``{{root}}`` — relative path to the site root (``""`` or ``"../"``)
- ``{{nav-<key>}}`` — expanded to the active-link attributes in the top bar
  on the page whose ``nav`` key matches, empty otherwise
"""

from __future__ import annotations

import html
import posixpath
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

# Live origin of the docs site; used for canonical + OpenGraph URLs.
SITE_URL = "https://pinkdb-docs.defaultcms.workers.dev"

# The sidebar navigation: groups of (key, label, output path) items. Every
# docs page sets its ``nav`` to the key of its entry here; the catalog lives
# at the top level and is also reachable from the top bar.
NAV = [
    {
        "title": "Start here",
        "items": [
            {"key": "getting-started", "label": "Getting started",
             "out": "docs/getting-started.html"},
        ],
    },
    {
        "title": "Guides",
        "items": [
            {"key": "guide-csv-to-lake", "label": "CSV to a lake",
             "out": "docs/guide-csv-to-lake.html"},
            {"key": "guide-s3-lake", "label": "S3 / R2 lakes",
             "out": "docs/guide-s3-lake.html"},
            {"key": "guide-encryption", "label": "Encrypted databases",
             "out": "docs/guide-encryption.html"},
            {"key": "guide-durable-multiprocess", "label": "Multi-process writers",
             "out": "docs/guide-durable-multiprocess.html"},
            {"key": "guide-embedding", "label": "Embedding",
             "out": "docs/guide-embedding.html"},
        ],
    },
    {
        "title": "Reference",
        "items": [
            {"key": "cli", "label": "CLI & REPL", "out": "docs/cli.html"},
            {"key": "sql", "label": "SQL", "out": "docs/sql.html"},
            {"key": "persistence", "label": "Persistence", "out": "docs/persistence.html"},
            {"key": "lake", "label": "Lake", "out": "docs/lake.html"},
            {"key": "server", "label": "Server", "out": "docs/server.html"},
        ],
    },
    {
        "title": "APIs",
        "items": [
            {"key": "bindings", "label": "Bindings overview", "out": "docs/bindings.html"},
            {"key": "api-rust", "label": "Rust", "out": "docs/api-rust.html"},
            {"key": "api-c", "label": "C", "out": "docs/api-c.html"},
            {"key": "api-python", "label": "Python", "out": "docs/api-python.html"},
            {"key": "api-go", "label": "Go", "out": "docs/api-go.html"},
        ],
    },
    {
        "title": "Extensions & roadmap",
        "items": [
            {"key": "extensions-doc", "label": "Using extensions",
             "out": "docs/extensions.html"},
            {"key": "extensions", "label": "Extension catalog",
             "out": "extensions.html"},
            {"key": "roadmap", "label": "Roadmap", "out": "docs/roadmap.html"},
        ],
    },
]

# Per-page metadata. Fragments without an entry still generate (under
# docs/) with a title derived from the file name and no sidebar.
PAGES = {
    "index.html": {
        "out": "index.html",
        "nav": "",
        "layout": "home",
        "title": "pinkdb — a mini DuckDB in Rust",
        "description": (
            "An embeddable analytical SQL engine in Rust: columnar vectors, "
            "vectorized execution, CSV import, durable .pink files with a WAL, "
            "a lake layer over local disk or S3/R2, C/Python/Go bindings, and "
            "a ~2.4 MB default binary."
        ),
    },
    "getting-started.html": {
        "out": "docs/getting-started.html",
        "nav": "getting-started",
        "toc": True,
        "title": "Getting started — pinkdb",
        "description": (
            "Build the pinkdb CLI — with or without the lake, S3, encryption "
            "and repository features — import a CSV, run your first queries, "
            "and save the database to a .pink file."
        ),
    },
    "cli.html": {
        "out": "docs/cli.html",
        "nav": "cli",
        "toc": True,
        "title": "CLI & REPL — pinkdb",
        "description": (
            "The pinkdb REPL: dot-commands (.import, .save, .open, .lake, "
            ".install), scripting and exit codes, environment variables, and "
            "remote mode."
        ),
    },
    "sql.html": {
        "out": "docs/sql.html",
        "nav": "sql",
        "toc": True,
        "title": "SQL reference — pinkdb",
        "description": (
            "The pinkdb SQL surface: DDL/DML, joins, CTEs, subqueries, window "
            "functions, aggregates, types, dates, transactions, and what is "
            "not supported yet."
        ),
    },
    "persistence.html": {
        "out": "docs/persistence.html",
        "nav": "persistence",
        "toc": True,
        "title": "Persistence & encryption — pinkdb",
        "description": (
            "The .pink v4 columnar format, durable databases with a WAL, "
            "crash recovery, checkpoints, multi-process writers, and optional "
            "at-rest encryption (AES-256-GCM + Argon2id)."
        ),
    },
    "lake.html": {
        "out": "docs/lake.html",
        "nav": "lake",
        "toc": True,
        "title": "Lake — pinkdb",
        "description": (
            "The pinkdb lake layer: catalog + immutable snapshots, Vortex or "
            "lean .pink data files, S3/R2 object storage, read-only attach "
            "with COUNT/MIN/MAX fast paths, and writer conflict semantics."
        ),
    },
    "server.html": {
        "out": "docs/server.html",
        "nav": "server",
        "toc": True,
        "title": "Server — pinkdb",
        "description": (
            "Run pinkdb-server over TCP: flags, durable mode, the framed "
            "binary protocol, remote CLI mode, and the security posture."
        ),
    },
    "bindings.html": {
        "out": "docs/bindings.html",
        "nav": "bindings",
        "title": "Bindings — pinkdb",
        "description": (
            "Embed pinkdb from Rust, C, Python or Go through the pinkdb-ffi "
            "C ABI, with minimal examples and links to the full API pages."
        ),
    },
    "api-rust.html": {
        "out": "docs/api-rust.html",
        "nav": "api-rust",
        "toc": True,
        "title": "Rust API — pinkdb",
        "description": (
            "The pinkdb Rust API: PinkDb, Database/Connection, durability "
            "options, encryption keys, the CSV importer, and the pink-lake "
            "crate."
        ),
    },
    "api-c.html": {
        "out": "docs/api-c.html",
        "nav": "api-c",
        "toc": True,
        "title": "C API — pinkdb",
        "description": (
            "The pinkdb C ABI v1: pinkdb_open, pinkdb_execute, typed result "
            "accessors, statement-kind and type tags, and the ownership and "
            "threading contract."
        ),
    },
    "api-python.html": {
        "out": "docs/api-python.html",
        "nav": "api-python",
        "toc": True,
        "title": "Python API — pinkdb",
        "description": (
            "The pinkdb Python binding: connect(), connect_memory(), "
            "Database.execute(), Result fields, value mapping, and error "
            "handling."
        ),
    },
    "api-go.html": {
        "out": "docs/api-go.html",
        "nav": "api-go",
        "toc": True,
        "title": "Go API — pinkdb",
        "description": (
            "The pinkdb Go binding: Open, OpenMemory, Execute, Result fields, "
            "value mapping, error handling, and thread safety."
        ),
    },
    "extensions-guide.html": {
        "out": "docs/extensions.html",
        "nav": "extensions-doc",
        "title": "Using extensions — pinkdb",
        "description": (
            "Load native SQL functions into pinkdb with LOAD EXTENSION, "
            "install from the extension repository, and write your own "
            "extension with the C or Go SDK."
        ),
    },
    "guide-csv-to-lake.html": {
        "out": "docs/guide-csv-to-lake.html",
        "nav": "guide-csv-to-lake",
        "title": "Guide: CSV to a lake — pinkdb",
        "description": (
            "Walk through importing a CSV, shaping it with SQL, saving a lake "
            "snapshot, reopening it, and attaching it read-only."
        ),
    },
    "guide-s3-lake.html": {
        "out": "docs/guide-s3-lake.html",
        "nav": "guide-s3-lake",
        "title": "Guide: S3 / R2 lakes — pinkdb",
        "description": (
            "Configure an S3-compatible endpoint (AWS S3, Cloudflare R2, "
            "MinIO), build the CLI with lake + S3, and save, open and attach "
            "a lake on object storage."
        ),
    },
    "guide-encryption.html": {
        "out": "docs/guide-encryption.html",
        "nav": "guide-encryption",
        "title": "Guide: encrypted databases — pinkdb",
        "description": (
            "Build with the encryption feature, use PINKDB_KEY, save and "
            "reopen an encrypted .pink database, and understand the "
            "plaintext/encrypted mixing rules."
        ),
    },
    "guide-durable-multiprocess.html": {
        "out": "docs/guide-durable-multiprocess.html",
        "nav": "guide-durable-multiprocess",
        "title": "Guide: multi-process writers — pinkdb",
        "description": (
            "Open one durable .pink database from several processes: writers "
            "serialize on a write lease, readers refresh from the WAL, and a "
            "busy database waits up to the busy timeout."
        ),
    },
    "guide-embedding.html": {
        "out": "docs/guide-embedding.html",
        "nav": "guide-embedding",
        "title": "Guide: embedding pinkdb — pinkdb",
        "description": (
            "Embed pinkdb in an application from Rust, C, Python or Go: build "
            "the library, open a database, execute SQL, and read typed "
            "results."
        ),
    },
    "roadmap.html": {
        "out": "docs/roadmap.html",
        "nav": "roadmap",
        "toc": True,
        "title": "Roadmap — pinkdb",
        "description": (
            "What pinkdb ships today, what is in progress, and what is "
            "planned: TLS, RBAC, async object storage, wasm extensions, the "
            "Rust extension SDK, and core json/string/math/datetime "
            "extensions."
        ),
    },
    "extensions.html": {
        "out": "extensions.html",
        "nav": "extensions",
        "title": "Extensions — pinkdb",
        "description": (
            "The pinkdb extension catalog: browse published artifacts (name, "
            "version, platform, ABI, SHA-256), install with .install <name>, "
            "and download the immutable bytes."
        ),
    },
    "404.html": {
        "out": "404.html",
        "nav": "",
        "layout": "narrow",
        "title": "Page not found — pinkdb",
        "description": "That page does not exist. Head back to the pinkdb docs.",
    },
}


def nav_items() -> list[dict[str, str]]:
    return [item for group in NAV for item in group["items"]]


def rel_href(from_out: str, to_out: str) -> str:
    """A relative link from one output file to another (pre-extensionless)."""
    from_dir = posixpath.dirname(from_out) or "."
    return posixpath.relpath(to_out, from_dir)


def render_sidebar(from_out: str, active_key: str) -> str:
    parts = [
        '<input class="sidebar-toggle-cb" type="checkbox" id="sidebar-toggle">',
        '<label class="sidebar-toggle" for="sidebar-toggle">Documentation menu',
        '<span class="sidebar-toggle-arrow" aria-hidden="true">&#9662;</span></label>',
        '<nav class="sidebar-nav" aria-label="Documentation">',
    ]
    for group in NAV:
        parts.append('<div class="side-group">')
        parts.append(f'<p class="side-group-title">{html.escape(group["title"])}</p>')
        parts.append("<ul>")
        for item in group["items"]:
            href = html.escape(rel_href(from_out, item["out"]), quote=True)
            active = ' class="active" aria-current="page"' if item["key"] == active_key else ""
            parts.append(f'<li><a href="{href}"{active}>{html.escape(item["label"])}</a></li>')
        parts.append("</ul>")
        parts.append("</div>")
    parts.append("</nav>")
    return "\n".join(parts)


H2_RE = re.compile(r"<h2(\s[^>]*)?>(.*?)</h2>", re.S)
ID_RE = re.compile(r'id="([^"]+)"')
TAG_RE = re.compile(r"<[^>]+>")


def text_content(fragment: str) -> str:
    text = TAG_RE.sub("", fragment)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "section"


def with_toc(fragment: str) -> tuple[str, list[tuple[str, str]]]:
    """Add ids to plain ``<h2>`` headings; return the fragment and its TOC."""
    used = set(ID_RE.findall(fragment))
    toc: list[tuple[str, str]] = []

    def replace(match: re.Match[str]) -> str:
        attrs, body = match.group(1) or "", match.group(2)
        if "id=" in attrs:
            return match.group(0)
        label = text_content(body)
        slug = base = slugify(label)
        n = 2
        while slug in used:
            slug, n = f"{base}-{n}", n + 1
        used.add(slug)
        toc.append((slug, label))
        return f'<h2 id="{slug}"{attrs}>{body}</h2>'

    return H2_RE.sub(replace, fragment), toc


def render_toc(entries: list[tuple[str, str]]) -> str:
    if len(entries) < 3:
        return ""
    items = "\n".join(
        f'  <li><a href="#{slug}">{html.escape(label)}</a></li>'
        for slug, label in entries
    )
    return (
        '<nav class="toc" aria-label="On this page">\n'
        '<p class="toc-title">On this page</p>\n'
        f"<ul>\n{items}\n</ul>\n"
        "</nav>"
    )


def load_nav_keys(layout: str) -> list[str]:
    keys = []
    for match in re.finditer(r"\{\{nav-([a-z0-9-]+)\}\}", layout):
        if match.group(1) not in keys:
            keys.append(match.group(1))
    if not keys:
        sys.exit("error: layout.html contains no {{nav-*}} hooks")
    return keys


def page_meta(fragment_name: str) -> dict:
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


def canonical_url(out_rel: str) -> str:
    """The page's canonical URL: ``index.html`` collapses to the site root."""
    path = out_rel
    if path.endswith("index.html"):
        path = path[: -len("index.html")]
    return f"{SITE_URL}/{path}"


def render(layout: str, nav_keys: list[str], fragment_name: str,
           meta: dict) -> str:
    out_rel = meta["out"]
    root = "../" * out_rel.count("/")

    fragment = (PAGES_DIR / fragment_name).read_text(encoding="utf-8")
    fragment = expand(fragment, {"{{root}}": root})

    toc_html = ""
    if meta.get("toc"):
        fragment, toc = with_toc(fragment)
        toc_html = render_toc(toc)

    layout_kind = meta.get("layout")
    if layout_kind == "home":
        main = f'<div class="container home">\n{fragment}\n</div>'
    elif layout_kind == "narrow":
        main = f'<div class="container narrow">\n{fragment}\n</div>'
    else:
        sidebar = render_sidebar(out_rel, meta.get("nav", ""))
        shell_cls = "container docs-shell has-toc" if toc_html else "container docs-shell"
        main = (
            f'<div class="{shell_cls}">\n'
            f'<aside class="sidebar">\n{sidebar}\n</aside>\n'
            f'<article class="doc-main">\n{fragment}\n</article>\n'
            f"{toc_html}\n"
            "</div>"
        )

    replacements = {
        "{{title}}": html.escape(meta["title"]),
        "{{description}}": html.escape(meta["description"]),
        "{{url}}": html.escape(canonical_url(out_rel)),
        "{{root}}": root,
        "{{main}}": main,
    }
    for key in nav_keys:
        active = ' class="active" aria-current="page"' if key == meta.get("nav") else ""
        replacements[f"{{{{nav-{key}}}}}"] = active

    page = expand(layout, replacements)

    leftover = sorted(set(PLACEHOLDER.findall(page)))
    if leftover:
        raise SystemExit(
            f"error: {fragment_name} left unexpanded placeholders: {leftover}"
        )
    if meta.get("nav") and meta["nav"] not in {item["key"] for item in nav_items()}:
        raise SystemExit(
            f"error: {fragment_name} sets nav={meta['nav']!r} but no NAV entry "
            "uses that key"
        )
    return extensionless_links(page)


def validate_nav() -> None:
    fragment_outs = {page_meta(name)["out"] for name in
                     (p.name for p in PAGES_DIR.iterdir() if p.suffix == ".html")}
    for item in nav_items():
        if item["out"] not in fragment_outs:
            sys.exit(
                f"error: NAV item {item['key']!r} points at {item['out']!r}, "
                "which no fragment generates"
            )


def main() -> int:
    if not LAYOUT.is_file():
        sys.exit(f"error: missing template {LAYOUT}")
    if not PAGES_DIR.is_dir():
        sys.exit(f"error: missing fragments directory {PAGES_DIR}")

    layout = LAYOUT.read_text(encoding="utf-8")
    nav_keys = load_nav_keys(layout)
    validate_nav()

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
