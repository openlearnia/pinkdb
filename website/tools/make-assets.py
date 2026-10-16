#!/usr/bin/env python3
"""Generate the branded raster assets for the docs site (Python 3 stdlib only).

Outputs (committed, then copied verbatim into ``public/assets/`` by
``build.py``):

- ``src/assets/favicon.png``     64x64   brand mark: pink rounded square, three
                                          bars (raster fallback, transparent corners)
- ``src/assets/og.png``          1200x630 social card in the docs' light theme

Everything is drawn with a tiny supersampled software rasteriser, so the
build stays dependency-free. Run from anywhere:

    python3 website/tools/make-assets.py
"""

from __future__ import annotations

import math
import struct
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "assets"

# Light-theme tokens from src/assets/style.css.
BG = (0xFF, 0xFF, 0xFF)
BG_SUBTLE = (0xFA, 0xFA, 0xFB)
SURFACE = (0xF6, 0xF6, 0xF8)
BORDER = (0xE6, 0xE4, 0xEA)
TEXT = (0x17, 0x15, 0x1B)
TEXT_2 = (0x47, 0x43, 0x4F)
TEXT_3 = (0x6B, 0x65, 0x75)
ACCENT = (0xC2, 0x25, 0x5C)
ACCENT_SOFT = (0xFD, 0xF0, 0xF5)
ACCENT_BORDER = (0xF5, 0xC6, 0xD8)
WHITE = (0xFF, 0xFF, 0xFF)


class Canvas:
    """RGBA canvas (premultiplied) with supersampled, bounding-box-limited
    drawing. ``background=None`` starts fully transparent."""

    def __init__(self, width: int, height: int, background=BG, ss: int = 4):
        self.width = width
        self.height = height
        self.ss = ss
        self.w = width * ss
        self.h = height * ss
        self.opaque = background is not None
        fill = (*background, 1.0) if background is not None else (0.0, 0.0, 0.0, 0.0)
        self.buf = [fill] * (self.w * self.h)

    def _fill(self, test, bbox, color, alpha: float = 1.0) -> None:
        """Fill pixels whose centre passes ``test``; ``bbox`` in final coords."""
        if alpha <= 0.0:
            return
        ss = self.ss
        x0 = max(0, int(math.floor(bbox[0] * ss)))
        y0 = max(0, int(math.floor(bbox[1] * ss)))
        x1 = min(self.w, int(math.ceil(bbox[2] * ss)) + 1)
        y1 = min(self.h, int(math.ceil(bbox[3] * ss)) + 1)
        cr, cg, cb = color
        buf = self.buf
        for py in range(y0, y1):
            y = (py + 0.5) / ss
            row = py * self.w
            for px in range(x0, x1):
                if test((px + 0.5) / ss, y):
                    index = row + px
                    r, g, b, a = buf[index]
                    buf[index] = (
                        cr * alpha + r * (1 - alpha),
                        cg * alpha + g * (1 - alpha),
                        cb * alpha + b * (1 - alpha),
                        alpha + a * (1 - alpha),
                    )

    # -- shapes (final-resolution coordinates) ----------------------------

    def rect(self, x, y, w, h, color, alpha: float = 1.0) -> None:
        self._fill(lambda px, py: x <= px <= x + w and y <= py <= y + h,
                   (x, y, x + w, y + h), color, alpha)

    def rounded_rect(self, x, y, w, h, r, color, alpha: float = 1.0) -> None:
        def test(px, py):
            if px < x or px > x + w or py < y or py > y + h:
                return False
            near_left = px < x + r
            near_right = px > x + w - r
            near_top = py < y + r
            near_bottom = py > y + h - r
            if (near_left or near_right) and (near_top or near_bottom):
                cx = x + r if near_left else x + w - r
                cy = y + r if near_top else y + h - r
                return math.hypot(px - cx, py - cy) <= r
            return True

        self._fill(test, (x, y, x + w, y + h), color, alpha)

    def circle(self, cx, cy, r, color, alpha: float = 1.0) -> None:
        self._fill(lambda px, py: math.hypot(px - cx, py - cy) <= r,
                   (cx - r, cy - r, cx + r, cy + r), color, alpha)

    def line(self, x1, y1, x2, y2, width, color, alpha: float = 1.0) -> None:
        half = width / 2
        dx, dy = x2 - x1, y2 - y1
        length_sq = dx * dx + dy * dy or 1.0

        def test(px, py):
            t = ((px - x1) * dx + (py - y1) * dy) / length_sq
            t = max(0.0, min(1.0, t))
            return math.hypot(px - (x1 + t * dx), py - (y1 + t * dy)) <= half

        self._fill(test, (min(x1, x2) - half, min(y1, y2) - half,
                          max(x1, x2) + half, max(y1, y2) + half), color, alpha)

    # -- output -----------------------------------------------------------

    def save(self, path: Path) -> None:
        ss = self.ss
        area = ss * ss
        raw = bytearray()
        for y in range(self.height):
            raw.append(0)
            rows = [self.buf[(y * ss + sy) * self.w:(y * ss + sy) * self.w + self.w]
                    for sy in range(ss)]
            for x in range(self.width):
                r = g = b = a = 0.0
                for row in rows:
                    for sx in range(ss):
                        pr, pg, pb, pa = row[x * ss + sx]
                        r += pr
                        g += pg
                        b += pb
                        a += pa
                a /= area
                if self.opaque:
                    raw += bytes((round(r / area), round(g / area), round(b / area)))
                elif a <= 0.0:
                    raw += bytes(4)
                else:
                    # Un-premultiply for straight-alpha PNG.
                    raw += bytes((min(255, round(r / area / a)), min(255, round(g / area / a)),
                                  min(255, round(b / area / a)), round(a * 255)))
        path.write_bytes(_png(self.width, self.height, bytes(raw), alpha=not self.opaque))


def _png(width: int, height: int, raw: bytes, alpha: bool = False) -> bytes:
    def chunk(tag: bytes, data: bytes) -> bytes:
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))

    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6 if alpha else 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9))
            + chunk(b"IEND", b""))


# 5x7 bitmap glyphs (rows top to bottom; '#' = filled).
FONT: dict[str, tuple[str, ...]] = {
    " ": tuple("....." for _ in range(7)),
    ".": (".....", ".....", ".....", ".....", ".....", ".##..", ".##.."),
    "-": (".....", ".....", ".....", ".###.", ".....", ".....", "....."),
    "/": ("....#", "...#.", "..#..", "..#..", ".#...", "#....", "....."),
    "\u00b7": (".....", ".....", "..##.", "..##.", ".....", ".....", "....."),
    "a": (".....", ".###.", "....#", ".####", "#...#", ".####", "....."),
    "b": ("#....", "#....", "####.", "#...#", "#...#", "####.", "....."),
    "c": (".....", ".###.", "#....", "#....", "#....", ".###.", "....."),
    "d": ("....#", "....#", ".####", "#...#", "#...#", ".####", "....."),
    "e": (".....", ".###.", "#...#", "#####", "#....", ".###.", "....."),
    "f": ("..##.", ".#...", "####.", ".#...", ".#...", ".#...", "....."),
    "g": (".....", ".####", "#...#", "#...#", ".####", "....#", ".###."),
    "h": ("#....", "#....", "####.", "#...#", "#...#", "#...#", "....."),
    "i": (".#...", ".....", "##...", ".#...", ".#...", ".###.", "....."),
    "k": ("#....", "#....", "#..#.", "#.#..", "##...", "#.#..", "....."),
    "l": ("##...", ".#...", ".#...", ".#...", ".#...", ".###.", "....."),
    "m": (".....", "####.", "#.#.#", "#.#.#", "#...#", "#...#", "....."),
    "n": (".....", "####.", "#...#", "#...#", "#...#", "#...#", "....."),
    "o": (".....", ".###.", "#...#", "#...#", "#...#", ".###.", "....."),
    "p": (".....", "####.", "#...#", "#...#", "####.", "#....", "#...."),
    "r": (".....", "#.##.", "##..#", "#....", "#....", "#....", "....."),
    "s": (".....", ".####", "#....", ".###.", "....#", "####.", "....."),
    "t": (".#...", ".#...", "####.", ".#...", ".#..#", "..##.", "....."),
    "u": (".....", "#...#", "#...#", "#...#", "#...#", ".###.", "....."),
    "v": (".....", "#...#", "#...#", "#...#", ".#.#.", "..#..", "....."),
    "w": (".....", "#...#", "#...#", "#.#.#", "#.#.#", ".#.#.", "....."),
    "x": (".....", "#...#", ".#.#.", "..#..", ".#.#.", "#...#", "....."),
    "y": (".....", "#...#", "#...#", ".####", "....#", ".###.", "....."),
    "z": (".....", "#####", "...#.", "..#..", ".#...", "#####", "....."),
    "B": ("####.", "#...#", "####.", "#...#", "#...#", "####.", "....."),
    "D": ("####.", "#...#", "#...#", "#...#", "#...#", "####.", "....."),
    "R": ("####.", "#...#", "####.", "#.#..", "#..#.", "#...#", "....."),
    "2": (".###.", "#...#", "...#.", "..#..", ".#...", "#####", "....."),
    "6": ("..##.", ".#...", "#....", "####.", "#...#", ".###.", "....."),
}


def draw_text(canvas: Canvas, text: str, x: float, y: float, scale: float, color, alpha=1.0) -> float:
    """Draw ``text`` with the bitmap font; returns the right edge."""
    advance = (5 + 1) * scale
    for index, char in enumerate(text):
        glyph = FONT.get(char)
        if glyph is None:
            raise SystemExit(f"make-assets: no glyph for {char!r}")
        ox = x + index * advance
        for row, line in enumerate(glyph):
            for col, cell in enumerate(line):
                if cell == "#":
                    # Slightly overlapping cells keep strokes solid.
                    canvas.rect(ox + col * scale - 0.25, y + row * scale - 0.25,
                                scale + 0.5, scale + 0.5, color, alpha)
    return x + len(text) * advance - scale


def text_width(text: str, scale: float) -> float:
    return len(text) * 6 * scale - scale


def brand_mark(canvas: Canvas, x: float, y: float, size: float,
               bg=ACCENT, bar=WHITE, alpha: float = 1.0) -> None:
    """The docs brand mark (layout.html's 24-unit SVG), scaled to ``size``."""
    u = size / 24
    canvas.rounded_rect(x + 1 * u, y + 1 * u, 22 * u, 22 * u, 6.5 * u, bg, alpha)
    for bx, by, bh in ((6, 12, 6), (10.5, 8, 10), (15, 5.5, 12.5)):
        canvas.rounded_rect(x + bx * u, y + by * u, 3 * u, bh * u, 1.2 * u, bar, alpha)


def make_favicon() -> None:
    # Fill the whole tile (the SVG's 1-unit margin is too small to matter at
    # 64 px and a full tile reads better as a touch icon).
    canvas = Canvas(64, 64, None, ss=4)
    canvas.rounded_rect(0, 0, 64, 64, 64 * 6.5 / 22, ACCENT)
    u = 64 / 22
    for bx, by, bh in ((6, 12, 6), (10.5, 8, 10), (15, 5.5, 12.5)):
        canvas.rounded_rect((bx - 1) * u, (by - 1) * u, 3 * u, bh * u, 1.2 * u, WHITE)
    canvas.save(OUT / "favicon.png")


def make_og() -> None:
    W, H = 1200, 630
    canvas = Canvas(W, H, BG, ss=2)

    # Right-hand motif: the brand mark's three ascending bars, large, in a
    # soft pink panel bleeding off the edge.
    canvas.rounded_rect(860, 70, 420, 490, 36, ACCENT_SOFT)
    for bx, top, alpha in ((915, 330, 0.35), (1020, 230, 0.6), (1125, 150, 1.0)):
        canvas.rounded_rect(bx, top, 72, 500 - top, 16, ACCENT, alpha)
    canvas.rect(885, 500, 400, 3, ACCENT_BORDER)

    # Brand row: mark + wordmark + "docs" pill.
    brand_mark(canvas, 80, 78, 64)
    right = draw_text(canvas, "pinkdb", 164, 89, 6, TEXT)
    pill_x = right + 22
    canvas.rounded_rect(pill_x, 88, 118, 44, 22, BORDER)
    canvas.rounded_rect(pill_x + 2, 90, 114, 40, 20, BG)
    draw_text(canvas, "docs", pill_x + 22, 99, 3.2, TEXT_3)

    # Headline, two lines, "duckdb" in the accent.
    scale = 9
    line_2 = 228 + 7 * scale + 26
    draw_text(canvas, "a mini ", 80, 228, scale, TEXT)
    draw_text(canvas, "duckdb", 80 + text_width("a mini ", scale) + scale, 228, scale, ACCENT)
    draw_text(canvas, "in rust", 80, line_2, scale, TEXT)

    # Sub-line and footer.
    draw_text(canvas, "columnar \u00b7 vectorized \u00b7 embedded", 82, 420, 3.6, TEXT_2)
    canvas.rect(80, 498, 700, 2, BORDER)
    draw_text(canvas, "docs \u00b7 extensions \u00b7 lake \u00b7 bindings", 82, 526, 3.2, TEXT_3)

    # Accent rule along the top edge.
    canvas.rect(0, 0, W, 6, ACCENT)
    canvas.save(OUT / "og.png")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    make_favicon()
    make_og()
    for name in ("favicon.png", "og.png"):
        path = OUT / name
        print(f"wrote {path.relative_to(ROOT.parent)} ({path.stat().st_size:,} bytes)")
