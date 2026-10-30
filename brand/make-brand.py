#!/usr/bin/env python3
"""Generate the pinkdb logo set (Python 3 stdlib only).

Every logo file in brand/logo/ and brand/icon/ is written from the geometry
below, so the mark, wordmark and lockups never drift apart. The favicon is
also copied to the docs site. PNG renders (brand/png/, the
site's favicon.png, apple-touch-icon.png and og.png) come from headless
Chrome (pass --png); skip it and only the SVGs are rewritten.

    python3 brand/make-brand.py          # SVGs
    python3 brand/make-brand.py --png    # SVGs + PNGs (needs Google Chrome)

Construction is documented in brand/README.md.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent

INK = "#241F1C"
PAPER = "#F7F1E8"
PINK = "#D6336C"

# --- mark ------------------------------------------------------------------
# 64-unit grid. A lowercase p: the stem is a capsule column cut into three
# row groups (cuts 3 units), the bowl is the pink dot. Content box is
# x 9..55, y 8..56 (46 x 48), centred on the grid.
MARK_STEM = "M9 14a6 6 0 0 1 12 0v6H9zM9 23h12v15H9zM9 41h12v9a6 6 0 0 1-12 0z"
MARK_DOT = (39.5, 23.5, 15.5)  # cx, cy, r: top and bottom match cut 2
MARK_BOX = (9, 8, 46, 48)

# Small-size mark (favicons, <= 24 px): the cuts close up, so the stem is solid.
MARK_STEM_SOLID = "M9 14a6 6 0 0 1 12 0v36a6 6 0 0 1-12 0z"

# --- wordmark --------------------------------------------------------------
# Monoline geometric lowercase, stroke 10, round caps. x-height 50 (y 40..90,
# baseline 90), ascender to y 20, descender to y 112. Bowls are r 20 circles
# on the stroke centreline. The tittle of the i is the pink dot.
WORD_STROKES = (
    "M5 45V107"                              # p stem
    "M66 45V85"                              # i
    "M89 45V85M89 65a20 20 0 0 1 40 0V85"    # n
    "M152 25V85M184 45 154 70M166 60 186 85"  # k
    "M245 25V85"                             # d stem
    "M268 25V85"                             # b stem
)
WORD_BOWLS = ((25, 65), (225, 65), (288, 65))  # p, d, b
WORD_DOT = (66, 25, 7)
WORD_BOX = (0, 18, 313, 94)


def mark_body(stem_fill: str, dot_fill: str, solid: bool = False) -> str:
    cx, cy, r = MARK_DOT
    stem = MARK_STEM_SOLID if solid else MARK_STEM
    return f'<path fill="{stem_fill}" d="{stem}"/><circle cx="{cx}" cy="{cy}" r="{r}" fill="{dot_fill}"/>'


def word_body(ink: str, dot: str) -> str:
    bowls = "".join(f'<circle cx="{x}" cy="{y}" r="20"/>' for x, y in WORD_BOWLS)
    cx, cy, r = WORD_DOT
    return (
        f'<g fill="none" stroke="{ink}" stroke-width="10" stroke-linecap="round" '
        f'stroke-linejoin="round"><path d="{WORD_STROKES}"/>{bowls}</g>'
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{dot}"/>'
    )


def svg(view_box: str, body: str, comment: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}" role="img" aria-label="pinkdb">\n'
        f"  <!-- {comment} -->\n"
        f"  {body}\n"
        f"</svg>\n"
    )


def fmt(v: float) -> str:
    return f"{v:.4f}".rstrip("0").rstrip(".")


def lockup_h(ink: str, dot: str) -> tuple[str, str]:
    """Mark beside the wordmark; mark height = ascender-to-descender height."""
    wx, wy, ww, wh = WORD_BOX
    mx, my, mw, mh = MARK_BOX
    s = 92 / mh  # wordmark ascender (20) to descender (112)
    gap = 34  # just under the x-height 50 minus one stroke
    tx = -mx * s
    ty = 20 - my * s
    word_x = mw * s + gap
    body = (
        f'<g transform="translate({fmt(tx)} {fmt(ty)}) scale({fmt(s)})">{mark_body(ink, dot)}</g>'
        f'<g transform="translate({fmt(word_x)} 0)">{word_body(ink, dot)}</g>'
    )
    vb = f"0 18 {fmt(word_x + ww)} {wh}"
    return vb, body


def lockup_v(ink: str, dot: str) -> tuple[str, str]:
    """Mark centred over the wordmark."""
    wx, wy, ww, wh = WORD_BOX
    mx, my, mw, mh = MARK_BOX
    s = 3.0
    gap = 44
    mark_w, mark_h = mw * s, mh * s
    tx = (ww - mark_w) / 2 - mx * s
    ty = -my * s
    word_y = mark_h + gap - wy
    body = (
        f'<g transform="translate({fmt(tx)} {fmt(ty)}) scale({fmt(s)})">{mark_body(ink, dot)}</g>'
        f'<g transform="translate(0 {fmt(word_y)})">{word_body(ink, dot)}</g>'
    )
    vb = f"0 0 {ww} {fmt(mark_h + gap + wh)}"
    return vb, body


def app_icon(bg: str, stem: str, dot: str, radius: float = 14) -> str:
    """64-unit rounded square with the mark at 75 %."""
    mx, my, mw, mh = MARK_BOX
    s = 0.75
    tx = (64 - mw * s) / 2 - mx * s
    ty = (64 - mh * s) / 2 - my * s
    return (
        f'<rect width="64" height="64" rx="{radius}" fill="{bg}"/>'
        f'<g transform="translate({fmt(tx)} {fmt(ty)}) scale({s})">{mark_body(stem, dot)}</g>'
    )


def favicon(bg: str, stem: str, dot: str) -> str:
    """32-unit favicon: solid stem, mark at 72 % of the tile, no cuts to blur."""
    mx, my, mw, mh = MARK_BOX
    s = 0.5
    tx = (32 - mw * s) / 2 - mx * s
    ty = (32 - mh * s) / 2 - my * s
    return (
        f'<rect width="32" height="32" rx="7" fill="{bg}"/>'
        f'<g transform="translate({fmt(tx)} {fmt(ty)}) scale({s})">{mark_body(stem, dot, solid=True)}</g>'
    )


def write(rel: str, text: str) -> None:
    path = ROOT / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print(f"wrote {rel}")


def build_svgs() -> None:
    m = "0 0 64 64"
    write("logo/pinkdb-mark.svg", svg(m, mark_body(INK, PINK), "pinkdb mark, for light backgrounds"))
    write("logo/pinkdb-mark-reverse.svg", svg(m, mark_body(PAPER, PINK), "pinkdb mark, for dark backgrounds"))
    write("logo/pinkdb-mark-mono.svg", svg(m, mark_body("currentColor", "currentColor"), "pinkdb mark, one colour (inherits currentColor)"))

    wb = " ".join(fmt(v) for v in WORD_BOX)
    write("logo/pinkdb-wordmark.svg", svg(wb, word_body(INK, PINK), "pinkdb wordmark, for light backgrounds"))
    write("logo/pinkdb-wordmark-reverse.svg", svg(wb, word_body(PAPER, PINK), "pinkdb wordmark, for dark backgrounds"))
    write("logo/pinkdb-wordmark-mono.svg", svg(wb, word_body("currentColor", "currentColor"), "pinkdb wordmark, one colour (inherits currentColor)"))

    for suffix, ink, dot, note in (
        ("", INK, PINK, "for light backgrounds"),
        ("-reverse", PAPER, PINK, "for dark backgrounds"),
        ("-mono", "currentColor", "currentColor", "one colour (inherits currentColor)"),
    ):
        vb, body = lockup_h(ink, dot)
        write(f"logo/pinkdb-lockup{suffix}.svg", svg(vb, body, f"pinkdb horizontal lockup, {note}"))
        vb, body = lockup_v(ink, dot)
        write(f"logo/pinkdb-lockup-stacked{suffix}.svg", svg(vb, body, f"pinkdb stacked lockup, {note}"))

    write("icon/app-icon.svg", svg(m, app_icon(INK, PAPER, PINK), "pinkdb app icon (primary: ink tile)"))
    write("icon/app-icon-light.svg", svg(m, app_icon(PAPER, INK, PINK), "pinkdb app icon (paper tile)"))
    write("icon/app-icon-pink.svg", svg(m, app_icon(PINK, PAPER, INK), "pinkdb app icon (pink tile, ink dot; for promo use)"))
    write("icon/favicon.svg", svg("0 0 32 32", favicon(INK, PAPER, PINK), "pinkdb favicon: solid stem for 16-32 px"))


CHROME_CANDIDATES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome",
    "chromium",
)

REPO = ROOT.parent
SITE_ASSETS = REPO / "website" / "src" / "assets"

PNGS = (
    # source (relative to brand/), out (relative to the repo), width, height
    # (None: keep the source's aspect ratio)
    ("icon/favicon.svg", "brand/png/favicon-16.png", 16, 16),
    ("icon/favicon.svg", "brand/png/favicon-32.png", 32, 32),
    ("icon/favicon.svg", "brand/png/favicon-48.png", 48, 48),
    ("icon/favicon.svg", "brand/png/favicon-64.png", 64, 64),
    ("icon/app-icon.svg", "brand/png/apple-touch-icon.png", 180, 180),
    ("icon/app-icon.svg", "brand/png/app-icon-192.png", 192, 192),
    ("icon/app-icon.svg", "brand/png/app-icon-512.png", 512, 512),
    ("icon/app-icon.svg", "brand/png/app-icon-1024.png", 1024, 1024),
    ("logo/pinkdb-mark.svg", "brand/png/pinkdb-mark-512.png", 512, 512),
    ("logo/pinkdb-lockup.svg", "brand/png/pinkdb-lockup-1200.png", 1200, None),
    ("logo/pinkdb-lockup-reverse.svg", "brand/png/pinkdb-lockup-reverse-1200.png", 1200, None),
    ("logo/pinkdb-lockup-stacked.svg", "brand/png/pinkdb-lockup-stacked-800.png", 800, None),
    # The docs site (website/src/assets/, copied to public/ by build.py).
    ("icon/favicon.svg", "website/src/assets/favicon.png", 64, 64),
    ("icon/app-icon.svg", "website/src/assets/apple-touch-icon.png", 180, 180),
)


def build_site_svgs() -> None:
    """Copy the favicon to the docs site. (The console inlines it as a data
    URI: `FAVICON` in crates/pinkdb-server-ui/src/html.rs.)"""
    dest = SITE_ASSETS / "favicon.svg"
    dest.write_text((ROOT / "icon" / "favicon.svg").read_text())
    print(f"wrote {dest.relative_to(REPO)}")


def og_page() -> str:
    """The docs site's 1200 x 630 social card: the logo, one line, a query."""
    fonts = (SITE_ASSETS / "fonts").as_uri()
    lockup = (ROOT / "logo" / "pinkdb-lockup.svg").as_uri()
    return f"""<!doctype html><html><head><style>
@font-face {{ font-family: Bricolage; src: url("{fonts}/bricolage-grotesque.woff2"); font-weight: 400 800; }}
@font-face {{ font-family: Mono; src: url("{fonts}/jetbrains-mono.woff2"); font-weight: 400 700; }}
body {{ margin: 0; width: 1200px; height: 630px; background: {PAPER}; color: {INK};
  position: relative; overflow: hidden; font-family: Bricolage, sans-serif; }}
.logo {{ position: absolute; left: 84px; top: 112px; width: 470px; }}
h1 {{ position: absolute; left: 84px; top: 268px; margin: 0; font-size: 50px; font-weight: 700;
  letter-spacing: -0.02em; line-height: 1.12; }}
.links {{ position: absolute; left: 84px; bottom: 92px; margin: 0; font: 500 24px Mono, monospace; color: #52483F; }}
.card {{ position: absolute; left: 688px; top: 128px; width: 428px; padding: 26px 28px;
  background: #FCF8F1; border: 3px solid {INK}; border-radius: 16px; box-shadow: 8px 8px 0 {INK};
  font: 400 19px/1.55 Mono, monospace; white-space: pre; }}
.card .k {{ color: #A61E4D; font-weight: 700; }}
.card hr {{ border: 0; border-top: 2px dashed #B9A88F; margin: 16px 0; }}
.card .m {{ color: #6E6156; }}
.rule {{ position: absolute; left: 0; right: 0; bottom: 0; height: 18px; background: {INK}; }}
</style></head><body>
<img class="logo" src="{lockup}" alt="">
<h1>An embedded analytical<br>SQL database, in Rust.</h1>
<p class="links">docs · extensions · C ABI</p>
<div class="card"><span class="k">SELECT</span> region, avg(total)
<span class="k">FROM</span> sales
<span class="k">GROUP BY</span> region
<span class="k">ORDER BY</span> 2 <span class="k">DESC</span>;<hr>region   avg(total)
west          175.0
east           50.5
<span class="m">2 rows</span></div>
<div class="rule"></div>
</body></html>"""


def find_chrome() -> str:
    for c in CHROME_CANDIDATES:
        if os.path.isabs(c) and os.path.exists(c):
            return c
        if found := shutil.which(c):
            return found
    sys.exit("make-brand: --png needs Google Chrome or Chromium on PATH")


def view_box(path: Path) -> tuple[float, float]:
    head = path.read_text().split('viewBox="', 1)[1].split('"', 1)[0]
    _, _, w, h = (float(v) for v in head.split())
    return w, h


def render(chrome: str, html: str, w: int, h: int, out: str, tmp: str) -> None:
    page = Path(tmp) / "page.html"
    page.write_text(html)
    target = REPO / out
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars",
            "--allow-file-access-from-files", "--force-device-scale-factor=1",
            "--default-background-color=00000000",
            f"--window-size={w},{h}", f"--screenshot={target}", page.as_uri(),
        ],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    print(f"wrote {out} ({w}x{h})")


def build_pngs() -> None:
    chrome = find_chrome()
    with tempfile.TemporaryDirectory() as tmp:
        for src, out, w, h in PNGS:
            if h is None:
                vw, vh = view_box(ROOT / src)
                h = round(w * vh / vw)
            html = (
                "<!doctype html><html><body style='margin:0;background:transparent'>"
                f"<img src='{(ROOT / src).as_uri()}' width='{w}' height='{h}' style='display:block'>"
                "</body></html>"
            )
            render(chrome, html, w, h, out, tmp)
        render(chrome, og_page(), 1200, 630, "website/src/assets/og.png", tmp)


if __name__ == "__main__":
    build_svgs()
    build_site_svgs()
    if "--png" in sys.argv[1:]:
        build_pngs()
