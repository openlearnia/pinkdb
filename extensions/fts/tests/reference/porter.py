"""The Porter stemmer as Snowball's `porter`, for the fts fixtures.

DuckDB's `stemmer := 'porter'` is libstemmer's `porter`, i.e. Snowball's
`porter.sbl`: the 1980 suffix tables expressed over the `R1`/`R2` regions
rather than the paper's `m` measurement. That is why this file differs from a
paper implementation in a handful of places (`abli` rather than `bli`, no
`logi`, step 3's `ative` deletes) — the two disagree on 65 of the 2,994 words
in `tests/fixtures/porter_stems.tsv`.

This is the independent side of the check: `src/porter.rs` is a byte-slice
state machine, this is string slicing over suffix tables, so a mistake in one
is unlikely to be repeated in the other. Standard library only. It generates
the fixture (`python3 tests/reference/porter.py --write`) and can re-check a
system word list (`--wordlist`).

Usage:
    python3 tests/reference/porter.py --write
    python3 tests/reference/porter.py --wordlist /usr/share/dict/words
"""

import sys
from pathlib import Path

VOWELS = set("aeiouy")
# A doubled consonant that step 1b undoes: "hopping" -> "hopp" -> "hop".
UNDOUBLE = {"bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt"}
STEP1A = (("sses", "ss"), ("ies", "i"), ("ss", "ss"), ("s", ""))
STEP2 = (
    ("ational", "ate"),
    ("tional", "tion"),
    ("enci", "ence"),
    ("anci", "ance"),
    ("abli", "able"),
    ("eli", "e"),
    ("alli", "al"),
    ("ousli", "ous"),
    ("entli", "ent"),
    ("aliti", "al"),
    ("biliti", "ble"),
    ("iviti", "ive"),
    ("alism", "al"),
    ("ation", "ate"),
    ("ization", "ize"),
    ("izer", "ize"),
    ("ator", "ate"),
    ("iveness", "ive"),
    ("fulness", "ful"),
    ("ousness", "ous"),
)
STEP3 = (
    ("icate", "ic"),
    ("ative", ""),
    ("alize", "al"),
    ("iciti", "ic"),
    ("ical", "ic"),
    ("ful", ""),
    ("ness", ""),
)
# Step 4 deletes these inside R2; "-ion" only after "s" or "t".
STEP4 = (
    "ement",
    "ance",
    "ence",
    "able",
    "ible",
    "ment",
    "ant",
    "ent",
    "ism",
    "ate",
    "iti",
    "ous",
    "ive",
    "ize",
    "ion",
    "al",
    "er",
    "ic",
    "ou",
)


def _longest(word, table):
    """The longest suffix of `word` in `table`, as (start, replacement)."""
    best = None
    for suffix, replacement in table:
        if word.endswith(suffix) and (best is None or len(suffix) > best[0]):
            best = (len(suffix), replacement)
    if best is None:
        return None
    return len(word) - best[0], best[1]


def _vowel_at(word, i):
    return 0 <= i < len(word) and word[i] in VOWELS


def _short(word):
    """Snowball's "short syllable" at the end of `word`."""
    n = len(word)
    return (
        n >= 3
        and word[n - 1] not in set("aeiouwxy")
        and _vowel_at(word, n - 2)
        and not _vowel_at(word, n - 3)
    )


def _regions(word):
    """R1/R2: after the first vowel-then-non-vowel pair, and again inside R1."""

    def after_pair(start):
        for i in range(start, len(word) - 1):
            if _vowel_at(word, i) and not _vowel_at(word, i + 1):
                return i + 2
        return len(word)

    r1 = after_pair(0)
    return r1, after_pair(r1)


def _step1a(word):
    hit = _longest(word, STEP1A)
    if hit:
        return word[: hit[0]] + hit[1]
    return word


def _step1b(word, r1):
    hit = _longest(word, (("eed", "eed"), ("ed", ""), ("ing", "")))
    if hit is None:
        return word
    start, replacement = hit
    if replacement == "eed":
        return word[:start] + "ee" if start >= r1 else word
    if not any(c in VOWELS for c in word[:start]):
        # "sing" keeps its "ing": the stem must still contain a vowel.
        return word
    word = word[:start]
    hit = _longest(word, (("at", "ate"), ("bl", "ble"), ("iz", "ize")))
    if hit:
        return word[: hit[0]] + hit[1]
    if word[-2:] in UNDOUBLE:
        return word[:-1]
    if len(word) == r1 and _short(word):
        return word + "e"
    return word


def _step1c(word):
    if len(word) >= 2 and word[-1] in "yY" and any(c in VOWELS for c in word[:-1]):
        return word[:-1] + "i"
    return word


def _replace_in_region(word, table, region):
    hit = _longest(word, table)
    if hit and hit[0] >= region:
        return word[: hit[0]] + hit[1]
    return word


def _step4(word, r2):
    hit = _longest(word, tuple((s, "") for s in STEP4))
    if hit is None or hit[0] < r2:
        return word
    start = hit[0]
    if word[start:] == "ion" and (start == 0 or word[start - 1] not in "st"):
        return word
    return word[:start]


def _step5(word, r1, r2):
    if word.endswith("e"):
        start = len(word) - 1
        if start >= r2 or (start >= r1 and not _short(word[:-1])):
            word = word[:-1]
    if word.endswith("ll") and len(word) > r2:
        word = word[:-1]
    return word


def stem(word):
    if len(word) <= 2 or not word.isascii() or not word.islower():
        return word
    # Snowball marks a consonantal `y` as `Y` so the vowel tests see it as a
    # vowel; only the `Y`s that step 1c converts become `i`.
    chars = list(word)
    if chars[0] == "y":
        chars[0] = "Y"
    for i in range(1, len(chars)):
        if chars[i] == "y" and chars[i - 1].lower() in VOWELS:
            chars[i] = "Y"
    word = "".join(chars)
    r1, r2 = _regions(word)
    word = _step1a(word)
    word = _step1b(word, r1)
    word = _step1c(word)
    word = _replace_in_region(word, STEP2, r1)
    word = _replace_in_region(word, STEP3, r1)
    word = _step4(word, r2)
    word = _step5(word, r1, r2)
    return word.replace("Y", "y")


def _fixture():
    return Path(__file__).resolve().parent.parent / "fixtures" / "porter_stems.tsv"


def main(argv):
    if "--write" in argv:
        words = [
            w.strip()
            for w in Path("/usr/share/dict/words").read_text().splitlines()
            if w.strip().isascii() and w.strip().islower() and len(w.strip()) > 2
        ]
        words = sorted(set(words))[:3000]
        lines = [
            "# Porter (Snowball `porter`) stems used by tests/stemmer.rs.",
            "# word<TAB>stem, generated once from /usr/share/dict/words by",
            "# tests/reference/porter.py (standard library only). Do not edit by hand.",
        ]
        lines += [f"{w}\t{stem(w)}" for w in words]
        _fixture().write_text("\n".join(lines) + "\n")
        print(f"wrote {len(words)} words to {_fixture()}")
        return 0
    words = argv[argv.index("--wordlist") + 1] if "--wordlist" in argv else None
    if words is None:
        print(__doc__)
        return 2
    rows = [
        line.split("\t")
        for line in Path(words).read_text().splitlines()
        if line.strip() and "\t" in line
    ]
    bad = [(w, want, stem(w)) for w, want in rows if stem(w) != want]
    for w, want, got in bad[:20]:
        print(f"{w}: expected {want}, got {got}")
    print(f"{len(rows) - len(bad)}/{len(rows)} match")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
