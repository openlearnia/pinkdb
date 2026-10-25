"""An independent reference implementation of DuckDB's BM25 full-text score.

This file is a test aid: `extensions/fts/tests/engine.rs` rebuilds the same
corpus in pinkdb, runs the same queries through `match_bm25`, and compares the
scores with the fixtures this script emits. It is deliberately separate code
(Python, standard library only) with its own Porter stemmer, so a bug in the
Rust implementation does not silently reproduce itself here.

DuckDB's documented model, which both sides implement:

    idf(t)    = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))     N = #documents
    score(d)  = sum_t idf(t) * sum_f tf(t,d,f) * (k+1)
                            / (tf(t,d,f) + k * (1 - b + b * len(d,f) / avg(f)))

with `k = 1.2`, `b = 0.75`, `df` counted per document (not per field), query
terms de-duplicated, and `conjunctive := 1` keeping only documents that match
every query term. A real DuckDB run is still outstanding (the fts extension is
not installed locally); a reviewer with fts available should re-run the same
corpus.

Usage: python3 bm25.py        (rewrites the two fixtures next to this file)
"""

import math
import re
import unicodedata
import pathlib

# DuckDB's `stopwords := 'english'`.
# DuckDB's `stopwords := 'english'`: the SMART system's 571 English stop words.
STOPWORDS = set(
    """    a a's able about above according accordingly across
        actually after afterwards again against ain't all allow
        allows almost alone along already also although always
        am among amongst an and another any anybody
        anyhow anyone anything anyway anyways anywhere apart appear
        appreciate appropriate are aren't around as aside ask
        asking associated at available away awfully b be
        became because become becomes becoming been before beforehand
        behind being believe below beside besides best better
        between beyond both brief but by c c'mon
        c's came can can't cannot cant cause causes
        certain certainly changes clearly co com come comes
        concerning consequently consider considering contain containing contains corresponding
        could couldn't course currently d definitely described despite
        did didn't different do does doesn't doing don't
        done down downwards during e each edu eg
        eight either else elsewhere enough entirely especially et
        etc even ever every everybody everyone everything everywhere
        ex exactly example except f far few fifth
        first five followed following follows for former formerly
        forth four from further furthermore g get gets
        getting given gives go goes going gone got
        gotten greetings h had hadn't happens hardly has
        hasn't have haven't having he he's hello help
        hence her here here's hereafter hereby herein hereupon
        hers herself hi him himself his hither hopefully
        how howbeit however i i'd i'll i'm i've
        ie if ignored immediate in inasmuch inc indeed
        indicate indicated indicates inner insofar instead into inward
        is isn't it it'd it'll it's its itself
        j just k keep keeps kept know known
        knows l last lately later latter latterly least
        less lest let let's like liked likely little
        look looking looks ltd m mainly many may
        maybe me mean meanwhile merely might more moreover
        most mostly much must my myself n name
        namely nd near nearly necessary need needs neither
        never nevertheless new next nine no nobody non
        none noone nor normally not nothing novel now
        nowhere o obviously of off often oh ok
        okay old on once one ones only onto
        or other others otherwise ought our ours ourselves
        out outside over overall own p particular particularly
        per perhaps placed please plus possible presumably probably
        provides q que quite qv r rather rd
        re really reasonably regarding regardless regards relatively respectively
        right s said same saw say saying says
        second secondly see seeing seem seemed seeming seems
        seen self selves sensible sent serious seriously seven
        several shall she should shouldn't since six so
        some somebody somehow someone something sometime sometimes somewhat
        somewhere soon sorry specified specify specifying still sub
        such sup sure t t's take taken tell
        tends th than thank thanks thanx that that's
        thats the their theirs them themselves then thence
        there there's thereafter thereby therefore therein thereof theres
        thereupon these they they'd they'll they're they've think
        third this thither thorough thoroughly those though three
        through throughout thru thus to together too took
        toward towards tried tries truly try trying twice
        two u un under unfortunately unless unlikely until
        unto up upon us use used useful uses
        using usually v value various very via viz
        vs w want wants was wasn't way we
        we'd we'll we're we've welcome well went were
        weren't what what's whatever when whence whenever where
        where's whereafter whereas whereby wherein whereupon wherever whether
        which while whither who who's whoever whole whom
        whose why will willing wish with within without
        won't wonder would wouldn't x y yes yet
        you you'd you'll you're you've your yours yourself
        yourselves z zero""".split()
)

# DuckDB's default `ignore := '(\\.|[^a-z])+'`: DuckDB strings do not
# escape, so it is an escaped backslash plus any character, or a non-letter.
IGNORE = re.compile(r"(\\.|[^a-z])+")

CORPUS = [
    (0, "Escapes", "A literal escape such as \\nline or C:\\tables joins nothing"),
    (1, "Relational Databases", "A relational database stores rows in tables"),
    (2, "Search Engines and BM25", "Full text search ranks matching documents with BM25 over an inverted index"),
    (3, "Cooking", "Bread needs flour, water and time"),
    (4, "Café Culture", "Crème brûlée and café au lait in a Parisian café"),
    (5, "Text Search", "The search index ranks the matching rows of the search table"),
    (6, "Databases", "Relational rows, relational tables, relational keys"),
    (7, "Empty Fields", ""),
    (8, "Numbers", "1984 was a year; 1984 is a number and text search ignores it"),
]

QUERIES = [
    ("relational database", None, 1.2, 0.75, 0),
    ("search", None, 1.2, 0.75, 0),
    ("search index", None, 1.2, 0.75, 0),
    ("search index", None, 1.2, 0.75, 1),
    ("relational database", None, 1.2, 0.75, 1),
    ("the matching", None, 1.2, 0.75, 0),
    ("cafe", None, 1.2, 0.75, 0),
    ("relational", "title", 1.2, 0.75, 0),
    ("relational", "body", 1.2, 0.75, 0),
    ("rows tables", None, 1.2, 0.75, 0),
    ("rows tables", None, 0.3, 0.0, 0),
    ("rows tables", None, 2.5, 1.0, 0),
    ("search", "title,body", 1.5, 0.6, 0),
    ("1984", None, 1.2, 0.75, 0),
    ("relational relational", None, 1.2, 0.75, 0),
]


def strip_accents(text):
    """Keep the base letter of a character that decomposes into letter+marks."""
    out = []
    for ch in text:
        decomposed = unicodedata.normalize("NFD", ch)
        if len(decomposed) > 1 and all(
            unicodedata.combining(part) for part in decomposed[1:]
        ):
            out.append(decomposed[0])
        else:
            out.append(ch)
    return "".join(out)


def tokens(text):
    text = strip_accents(text.lower())
    return [t for t in IGNORE.sub(" ", text).split() if t not in STOPWORDS]


def is_vowel(c):
    return c in "aeiouy"


def regions(word):
    def after_pair(start):
        i = start
        while i + 1 < len(word):
            if is_vowel(word[i]) and not is_vowel(word[i + 1]):
                return i + 2
            i += 1
        return len(word)

    r1 = after_pair(0)
    return r1, after_pair(r1)


def short_syllable(stem_text):
    n = len(stem_text)
    return (
        n >= 3
        and stem_text[n - 1] not in "aeiouwxY"
        and is_vowel(stem_text[n - 2])
        and not is_vowel(stem_text[n - 3])
    )


STEP2 = [
    ("ational", "ate"), ("tional", "tion"), ("enci", "ence"), ("anci", "ance"),
    ("abli", "able"), ("eli", "e"), ("alli", "al"), ("ousli", "ous"),
    ("entli", "ent"), ("aliti", "al"), ("biliti", "ble"), ("iviti", "ive"),
    ("alism", "al"), ("ation", "ate"), ("ization", "ize"), ("izer", "ize"),
    ("ator", "ate"), ("iveness", "ive"), ("fulness", "ful"), ("ousness", "ous"),
]
STEP3 = [
    ("icate", "ic"), ("ative", ""), ("alize", "al"), ("iciti", "ic"),
    ("ical", "ic"), ("ful", ""), ("ness", ""),
]
STEP4 = [
    "ement", "ance", "ence", "able", "ible", "ment", "ant", "ent", "ism", "ate",
    "iti", "ous", "ive", "ize", "ion", "al", "er", "ic", "ou",
]


def longest(word, table):
    best = None
    for suffix, replacement in table:
        if word.endswith(suffix) and (best is None or len(suffix) > len(best[0])):
            best = (suffix, replacement)
    return best


def stem(word):
    """Snowball's `porter`, the stemmer DuckDB's `stemmer := 'porter'` uses."""
    if len(word) <= 2 or not all("a" <= c <= "z" for c in word):
        return word
    chars = list(word)
    if chars[0] == "y":
        chars[0] = "Y"
    for i in range(1, len(chars)):
        if chars[i] == "y" and is_vowel(chars[i - 1]):
            chars[i] = "Y"
    w = "".join(chars)
    r1, r2 = regions(w)

    for suffix, replacement in [("sses", "ss"), ("ies", "i"), ("ss", "ss"), ("s", "")]:
        if w.endswith(suffix):
            w = w[: len(w) - len(suffix)] + replacement
            break

    for suffix in ("eed", "ed", "ing"):
        if w.endswith(suffix) and (suffix != "ed" or not w.endswith("eed")):
            start = len(w) - len(suffix)
            if suffix == "eed":
                if start >= r1:
                    w = w[:start] + "ee"
            elif any(is_vowel(c) for c in w[:start]):
                w = w[:start]
                for follow, replacement in [("at", "ate"), ("bl", "ble"), ("iz", "ize")]:
                    if w.endswith(follow):
                        w = w[: len(w) - len(follow)] + replacement
                        break
                else:
                    if w[-2:] in ("bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt"):
                        w = w[:-1]
                    elif len(w) == r1 and short_syllable(w):
                        w += "e"
            break

    if w.endswith(("y", "Y")) and any(is_vowel(c) for c in w[:-1]):
        w = w[:-1] + "i"

    for table, region in ((STEP2, r1), (STEP3, r1)):
        found = longest(w, table)
        if found:
            suffix, replacement = found
            if len(w) - len(suffix) >= region:
                w = w[: len(w) - len(suffix)] + replacement
            break

    for suffix in sorted(STEP4, key=len, reverse=True):
        if w.endswith(suffix) and len(w) - len(suffix) >= r2:
            if suffix == "ion" and (len(w) < 4 or w[-4] not in "st"):
                break
            w = w[: len(w) - len(suffix)]
            break

    if w.endswith("e"):
        start = len(w) - 1
        if start >= r2 or (start >= r1 and not short_syllable(w[:start])):
            w = w[:-1]
    if w.endswith("ll") and len(w) - 1 >= r2:
        w = w[:-1]

    return w.replace("Y", "y")


class Index:
    def __init__(self, corpus, num_fields):
        self.num_docs = len(corpus)
        self.lens = {}  # doc -> [len per field]
        postings = {}  # term -> list of (doc, field, tf)
        for doc_id, *texts in corpus:
            self.lens[doc_id] = [len(tokens(t)) for t in texts]
            for field, text in enumerate(texts):
                counts = {}
                for token in tokens(text):
                    stemmed = stem(token)
                    counts[stemmed] = counts.get(stemmed, 0) + 1
                for term, tf in counts.items():
                    postings.setdefault(term, []).append((doc_id, field, tf))
        df = {t: len({doc for doc, _, _ in p}) for t, p in postings.items()}
        n = float(self.num_docs)
        self.idf = {t: math.log(1.0 + (n - d + 0.5) / (d + 0.5)) for t, d in df.items()}
        self.avg = [
            (sum(self.lens[d][f] for d in self.lens) / self.num_docs) or 0.0
            for f in range(num_fields)
        ]
        self.postings = postings

    def score(self, query, fields, k, b, conjunctive):
        terms = sorted({stem(t) for t in tokens(query)})
        field_ids = None
        if fields is not None:
            names = [f.strip() for f in fields.split(",")]
            field_ids = {f for f, name in enumerate(FIELD_NAMES) if name in names}
        scores, hits = {}, {}
        for term in terms:
            idf = self.idf.get(term)
            if idf is None:
                continue
            for doc, field, tf in self.postings[term]:
                if field_ids is not None and field not in field_ids:
                    continue
                avg = self.avg[field]
                norm = 1.0 - b + b * self.lens[doc][field] / avg if avg > 0 else 1.0
                scores[doc] = scores.get(doc, 0.0) + idf * tf * (k + 1.0) / (tf + k * norm)
                hits.setdefault(doc, set()).add(term)
        if conjunctive:
            scores = {d: s for d, s in scores.items() if len(hits[d]) == len(terms)}
        return scores


FIELD_NAMES = ["title", "body"]


def bm25(corpus=None, queries=None):
    """Every `(query, scores)` pair for the fixture corpus."""
    index = Index(corpus or CORPUS, len(FIELD_NAMES))
    return index, [
        ((q, fields, k, b, c), index.score(q, fields, k, b, c))
        for q, fields, k, b, c in (queries or QUERIES)
    ]


def main():
    here = pathlib.Path(__file__).parent
    index, results = bm25()
    with open(here / "bm25_corpus.tsv", "w") as f:
        f.write("# doc_id\ttitle\tbody\n")
        for doc_id, title, body in CORPUS:
            f.write(f"{doc_id}\t{title}\t{body}\n")
    with open(here / "bm25_queries.tsv", "w") as f:
        f.write("# query\tfields\tk\tb\tconjunctive\tscores(id:score;...)\n")
        for query, scores in results:
            q, fields, k, b, c = query
            packed = ";".join(f"{d}:{s:.17g}" for d, s in sorted(scores.items()))
            f.write(f"{q}\t{fields or ''}\t{k}\t{b}\t{c}\t{packed}\n")
    print(f"{len(results)} queries over {index.num_docs} documents")


if __name__ == "__main__":
    main()
