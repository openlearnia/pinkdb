#!/usr/bin/env python3
"""Generate the branded raster assets for the docs site (Python 3 stdlib only).

Outputs (committed, then copied verbatim into ``public/assets/`` by
``build.py``):

- ``src/assets/favicon.png``     64x64   paper chip + pink dot (raster fallback)
- ``src/assets/og.png``          1200x630 social card, "soft data-zine" styling

Everything is drawn with a tiny supersampled software rasteriser, so the
build stays dependency-free. Run from anywhere:

    python3 website/tools/make-assets.py
"""

from __future__ import annotations

import math
import random
import struct
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "assets"

PAPER = (0xF7, 0xF1, 0xE8)
PAPER_2 = (0xFB, 0xF7, 0xF0)
PAPER_3 = (0xEF, 0xE6, 0xD8)
TAPE = (0xE7, 0xDB, 0xC9)
INK = (0x24, 0x1F, 0x1C)
PINK = (0xC9, 0x55, 0x7A)
PINK_DEEP = (0xA6, 0x3A, 0x5E)
BLUE = (0x2C, 0x4A, 0x66)


class Canvas:
    """Opaque RGB canvas with supersampled, bounding-box-limited drawing."""

    def __init__(self, width: int, height: int, background=PAPER, ss: int = 4):
        self.width = width
        self.height = height
        self.ss = ss
        self.w = width * ss
        self.h = height * ss
        self.buf = [background] * (self.w * self.h)

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
                    r, g, b = buf[index]
                    buf[index] = (
                        cr * alpha + r * (1 - alpha),
                        cg * alpha + g * (1 - alpha),
                        cb * alpha + b * (1 - alpha),
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

    def ring(self, cx, cy, r, width, color, alpha: float = 1.0) -> None:
        self._fill(lambda px, py: abs(math.hypot(px - cx, py - cy) - r) <= width / 2,
                   (cx - r - width, cy - r - width, cx + r + width, cy + r + width),
                   color, alpha)

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

    def dashed_line(self, x1, y1, x2, y2, width, dash, gap, color, alpha: float = 1.0) -> None:
        total = math.hypot(x2 - x1, y2 - y1)
        travelled = 0.0
        while travelled < total:
            end = min(travelled + dash, total)
            t0, t1 = travelled / total, end / total
            self.line(x1 + (x2 - x1) * t0, y1 + (y2 - y1) * t0,
                      x1 + (x2 - x1) * t1, y1 + (y2 - y1) * t1, width, color, alpha)
            travelled += dash + gap

    def rot_rect(self, cx, cy, w, h, angle_deg, color, alpha: float = 1.0) -> None:
        theta = math.radians(-angle_deg)
        cos_t, sin_t = math.cos(theta), math.sin(theta)
        half_w, half_h = w / 2, h / 2
        reach = math.hypot(half_w, half_h)
        diag = math.degrees(math.atan2(half_h, half_w))
        corners = [
            _rotate(cx, cy, reach, angle_deg - diag),
            _rotate(cx, cy, reach, angle_deg + diag),
        ]
        xs = [p[0] for p in corners]
        ys = [p[1] for p in corners]
        box = (min(xs), min(ys), max(xs), max(ys))

        def test(px, py):
            dx, dy = px - cx, py - cy
            rx = dx * cos_t + dy * sin_t
            ry = -dx * sin_t + dy * cos_t
            return abs(rx) <= half_w and abs(ry) <= half_h

        self._fill(test, box, color, alpha)

    def grain(self, coverage: float = 0.05, seed: int = 42, radius: float = 1.1) -> None:
        rng = random.Random(seed)
        count = int(self.width * self.height * coverage / (math.pi * radius * radius))
        for _ in range(count):
            x = rng.uniform(0, self.width)
            y = rng.uniform(0, self.height)
            tone = INK if rng.random() < 0.7 else PINK_DEEP
            self.circle(x, y, rng.uniform(0.4, radius), tone, rng.uniform(0.05, 0.14))

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
                r = g = b = 0.0
                for row in rows:
                    for sx in range(ss):
                        pr, pg, pb = row[x * ss + sx]
                        r += pr
                        g += pg
                        b += pb
                raw += bytes((round(r / area), round(g / area), round(b / area)))
        path.write_bytes(_png(self.width, self.height, bytes(raw)))


def _rotate(cx, cy, r, angle_deg):
    theta = math.radians(angle_deg)
    return cx + r * math.cos(theta), cy + r * math.sin(theta)


def _png(width: int, height: int, raw: bytes) -> bytes:
    def chunk(tag: bytes, data: bytes) -> bytes:
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))

    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9))
            + chunk(b"IEND", b""))


# 5x7 bitmap glyphs (rows top to bottom; '#' = ink).
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
    "B": ("####.", "#...#", "####.", "#...#", "#...#", "####.", "....."),
    "D": ("####.", "#...#", "#...#", "#...#", "#...#", "####.", "....."),
    "R": ("####.", "#...#", "####.", "#.#..", "#..#.", "#...#", "....."),
    "2": (".###.", "#...#", "...#.", "..#..", ".#...", "#####", "....."),
    "6": ("..##.", ".#...", "#....", "####.", "#...#", ".###.", "....."),
}


def draw_text(canvas: Canvas, text: str, x: float, y: float, scale: float, color, alpha=1.0) -> float:
    advance = (5 + 1) * scale
    for index, char in enumerate(text):
        glyph = FONT.get(char)
        if glyph is None:
            raise SystemExit(f"make-assets: no glyph for {char!r}")
        ox = x + index * advance
        for row, line in enumerate(glyph):
            for col, cell in enumerate(line):
                if cell == "#":
                    canvas.rect(ox + col * scale, y + row * scale, scale, scale, color, alpha)
    return x + len(text) * advance - scale


def make_favicon() -> None:
    canvas = Canvas(64, 64, PAPER, ss=4)
    canvas.rounded_rect(2, 2, 60, 60, 15, INK)
    canvas.rounded_rect(4.5, 4.5, 55, 55, 13, PAPER)
    canvas.line(14, 21, 36, 21, 5.5, INK, 0.85)
    canvas.line(14, 31, 29, 31, 5.5, INK, 0.85)
    canvas.circle(42.5, 42.5, 11.6, INK)
    canvas.circle(42.5, 42.5, 9.4, PINK)
    canvas.save(OUT / "favicon.png")


def make_og() -> None:
    canvas = Canvas(1200, 630, PAPER, ss=2)

    # Ambient paper shapes: a big pink sun, tape strips, a torn slab.
    canvas.circle(1040, 150, 150, PINK)
    canvas.ring(1040, 150, 150, 9, INK, 0.9)
    canvas.rot_rect(150, 92, 250, 52, -3.5, TAPE, 0.95)
    canvas.rot_rect(1080, 470, 300, 46, 2.5, PAPER_3, 1.0)
    canvas.dashed_line(70, 452, 1130, 452, 5, 16, 12, INK, 0.75)

    # Index card holding the URL line.
    canvas.rounded_rect(70, 486, 950, 78, 12, PAPER_2)
    canvas.line(73, 489, 1017, 489, 3, INK, 0.5)
    canvas.line(73, 561, 1017, 561, 3, INK, 0.5)
    canvas.line(73, 489, 73, 561, 3, INK, 0.5)
    canvas.line(1017, 489, 1017, 561, 3, INK, 0.5)

    # Wordmark + a pink version sticker.
    draw_text(canvas, "pinkdb", 78, 150, 18, INK)
    canvas.rounded_rect(950, 352, 190, 54, 7, PINK)
    canvas.line(952, 354, 1138, 354, 4, INK, 0.9)
    canvas.line(952, 404, 1138, 404, 4, INK, 0.9)
    canvas.line(952, 354, 952, 404, 4, INK, 0.9)
    canvas.line(1138, 354, 1138, 404, 4, INK, 0.9)
    draw_text(canvas, "wave 26", 957, 370, 4.4, PAPER_2)

    draw_text(canvas, "a mini duckdb in rust", 78, 352, 6.4, INK, 0.92)
    draw_text(canvas, "docs \u00b7 extensions \u00b7 c abi", 98, 503, 5.6, BLUE)

    # Cut-line ticks + pink spine at the bottom edge.
    canvas.rect(0, 606, 1200, 24, PINK)
    canvas.rect(0, 606, 1200, 5, INK, 0.85)

    canvas.grain(coverage=0.045)
    canvas.save(OUT / "og.png")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    make_favicon()
    make_og()
    for name in ("favicon.png", "og.png"):
        path = OUT / name
        print(f"wrote {path.relative_to(ROOT.parent)} ({path.stat().st_size:,} bytes)")
