//! The Porter stemmer, as Snowball's `porter` — which is the algorithm
//! DuckDB's `stemmer := 'porter'` ships through libstemmer.
//!
//! It is the 1980 algorithm expressed over Snowball's `R1`/`R2` regions
//! rather than the paper's `m` count, so the suffix tables differ from the
//! paper in a few places (`abli` rather than `bli`, no `logi`, step 3's
//! `ative` deletes). The reference word list the tests check is
//! `tests/fixtures/porter_stems.tsv`.
//!
//! Input is the tokenizer's output: a lowercase word. Non-ASCII words are
//! returned unchanged, because the rules are defined over `a`–`z`.

/// The Porter stem of `word`.
pub fn stem(word: &str) -> String {
    if word.len() <= 2 || !word.chars().all(|c| c.is_ascii_lowercase()) {
        return word.to_string();
    }
    // Snowball marks `y` as a vowel where it starts a word or follows a vowel
    // (`Y`), stems, then turns `Y` back into `y`. Only the marks convert to
    // `i`, which is why "happy" -> "happi" but "cry" keeps its `y`.
    let mut w: Vec<u8> = word.bytes().collect();
    if w[0] == b'y' {
        w[0] = b'Y';
    }
    for i in 1..w.len() {
        if w[i] == b'y' && vowel(w[i - 1]) {
            w[i] = b'Y';
        }
    }
    let (r1, r2) = regions(&w);
    step1a(&mut w);
    step1b(&mut w, r1);
    step1c(&mut w);
    step2(&mut w, r1);
    step3(&mut w, r1);
    step4(&mut w, r2);
    step5(&mut w, r1, r2);
    w.into_iter()
        .map(|c| if c == b'Y' { 'y' } else { c as char })
        .collect()
}

/// `G_v` in Snowball's porter: `a e i o u` and `y`.
fn vowel(c: u8) -> bool {
    matches!(c, b'a' | b'e' | b'i' | b'o' | b'u' | b'y')
}

/// A "short syllable" at the end of `s`: a vowel followed by a non-vowel
/// other than `w`, `x` or `Y`, itself preceded by a non-vowel.
fn shortv(s: &[u8]) -> bool {
    let n = s.len();
    n >= 3
        && !matches!(
            s[n - 1],
            b'a' | b'e' | b'i' | b'o' | b'u' | b'w' | b'x' | b'Y'
        )
        && vowel(s[n - 2])
        && !vowel(s[n - 3])
}

/// Snowball's `R1` and `R2`: the positions after the first vowel/non-vowel
/// pair, and after the first such pair inside `R1`.
fn regions(w: &[u8]) -> (usize, usize) {
    fn after_first_pair(w: &[u8], from: usize) -> usize {
        let mut i = from;
        while i + 1 < w.len() {
            if vowel(w[i]) && !vowel(w[i + 1]) {
                return i + 2;
            }
            i += 1;
        }
        w.len()
    }
    let r1 = after_first_pair(w, 0);
    let r2 = after_first_pair(w, r1);
    (r1, r2)
}

/// The longest suffix of `w` out of `table`, as `(start, replacement)`.
fn longest<'a>(w: &[u8], table: &[(&'a str, &'a str)]) -> Option<(usize, &'a str)> {
    let mut best: Option<(usize, &str)> = None;
    for (suffix, with) in table {
        let suffix = suffix.as_bytes();
        if w.len() >= suffix.len() && &w[w.len() - suffix.len()..] == suffix {
            let start = w.len() - suffix.len();
            if best.is_none_or(|(s, _)| start < s) {
                best = Some((start, with));
            }
        }
    }
    best
}

fn splice(w: &mut Vec<u8>, start: usize, with: &str) {
    w.truncate(start);
    w.extend_from_slice(with.as_bytes());
}

/// Step 1a: plurals. The word must end in `s`, and `ss` is left alone.
fn step1a(w: &mut Vec<u8>) {
    let table = [("sses", "ss"), ("ies", "i"), ("ss", "ss"), ("s", "")];
    if let Some((start, with)) = longest(w, &table) {
        splice(w, start, with);
    }
}

/// Step 1b: `-eed`, `-ed` and `-ing`, plus the `e` that a short stem needs
/// back ("hop" -> "hopp" -> "hop" stays, "fil" -> "file").
fn step1b(w: &mut Vec<u8>, r1: usize) {
    let table = [("eed", "eed"), ("ed", ""), ("ing", "")];
    let Some((start, with)) = longest(w, &table) else {
        return;
    };
    if with == "eed" {
        if start >= r1 {
            splice(w, start, "ee");
        }
        return;
    }
    // A vowel must be left in the stem ("sing" keeps its "ing").
    if !w[..start].iter().any(|c| vowel(*c)) {
        return;
    }
    splice(w, start, "");
    if let Some((at, with)) = longest(w, &[("at", "ate"), ("bl", "ble"), ("iz", "ize")]) {
        splice(w, at, with);
        return;
    }
    const DOUBLES: [&[u8]; 9] = [
        b"bb", b"dd", b"ff", b"gg", b"mm", b"nn", b"pp", b"rr", b"tt",
    ];
    if DOUBLES.iter().any(|pair| w.ends_with(pair)) {
        // "hopping" -> "hopp" -> "hop"; `ss`, `ll` and `zz` stay doubled.
        w.pop();
        return;
    }
    if w.len() == r1 && shortv(w) {
        w.push(b'e');
    }
}

/// Step 1c: a terminal `y` becomes `i` when a vowel precedes it.
fn step1c(w: &mut [u8]) {
    let n = w.len();
    if n >= 2 && (w[n - 1] == b'y' || w[n - 1] == b'Y') && w[..n - 1].iter().any(|c| vowel(*c)) {
        w[n - 1] = b'i';
    }
}

/// Step 2: derivational suffixes inside `R1`.
fn step2(w: &mut Vec<u8>, r1: usize) {
    let table = [
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
    ];
    if let Some((start, with)) = longest(w, &table)
        && start >= r1
    {
        splice(w, start, with);
    }
}

/// Step 3: more derivational suffixes inside `R1`.
fn step3(w: &mut Vec<u8>, r1: usize) {
    let table = [
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];
    if let Some((start, with)) = longest(w, &table)
        && start >= r1
    {
        splice(w, start, with);
    }
}

/// Step 4: suffixes removed inside `R2`; `-ion` only after `s` or `t`.
fn step4(w: &mut Vec<u8>, r2: usize) {
    const TABLE: &[&str] = &[
        "ement", "ance", "ence", "able", "ible", "ment", "ant", "ent", "ism", "ate", "iti", "ous",
        "ive", "ize", "ion", "al", "er", "ic", "ou",
    ];
    let table: Vec<(&str, &str)> = TABLE.iter().map(|s| (*s, "")).collect();
    let Some((start, _)) = longest(w, &table) else {
        return;
    };
    if start < r2 {
        return;
    }
    if &w[start..] == b"ion" && !matches!(start.checked_sub(1).map(|i| w[i]), Some(b's' | b't')) {
        return;
    }
    splice(w, start, "");
}

/// Step 5: a final `e`, then a doubled `l`.
fn step5(w: &mut Vec<u8>, r1: usize, r2: usize) {
    let n = w.len();
    if n > 1 && w[n - 1] == b'e' {
        let start = n - 1;
        if start >= r2 || (start >= r1 && !shortv(&w[..start])) {
            w.pop();
        }
    }
    let n = w.len();
    if n >= 2 && w[n - 1] == b'l' && w[n - 2] == b'l' && n > r2 {
        w.pop();
    }
}
