//! `string` — pinkdb's core string extension (Rust extension SDK, ABI v1).
//!
//! One [`pinkdb_extension!`] declaration defines ten deterministic functions,
//! DuckDB-pinned (verified live against DuckDB `v2.0.0-alpha46057`):
//!
//! | function | DuckDB signature | here |
//! |---|---|---|
//! | [`split_part`] | `(VARCHAR, VARCHAR, BIGINT) -> VARCHAR` | same |
//! | [`reverse`] | `(VARCHAR) -> VARCHAR` | same |
//! | [`left`] / [`right`] | `(VARCHAR, BIGINT) -> VARCHAR` | same |
//! | [`strpos`] | `(VARCHAR, VARCHAR) -> BIGINT` | same |
//! | [`starts_with`] / [`ends_with`] | `(VARCHAR, VARCHAR) -> BOOLEAN` | same |
//! | [`regexp_extract`] | `(VARCHAR, VARCHAR, INTEGER, VARCHAR) -> VARCHAR` | same 4-arg form |
//! | [`regexp_matches`] | `(VARCHAR, VARCHAR, VARCHAR) -> BOOLEAN` | same 3-arg form |
//! | [`regexp_match`] | *not in DuckDB* | pinned: first match text |
//!
//! `trim`, `ltrim`, `rtrim` and `replace` are **already engine built-ins**
//! with DuckDB-pinned semantics and differential coverage (the `trim-family`
//! and `replace-fn` probes). Extension functions cannot shadow a built-in, so
//! re-registering them here would make `LOAD EXTENSION` fail; the extension
//! deliberately does not. See the crate README for the full semantics matrix
//! and the documented divergences.
//!
//! # Loading
//!
//! - native: `make -C extensions/string` (or
//!   `cargo build --release -p pinkdb-ext-string --features abi-exports`),
//!   then `LOAD EXTENSION '<repo>/target/release/libstring_ext.dylib';`
//! - linked (tests, wasm): `string_ext::pinkdb_register_linked()?`, then
//!   `LOAD EXTENSION 'string';` — same declaration, same functions.
//!
//! The `abi-exports` feature is what emits the three `#[no_mangle]` ABI
//! symbols. It is off by default because this crate also builds an rlib, and
//! a linked-only extension must not carry them: two extension rlibs in one
//! binary collide on Linux (`duplicate symbol: pinkdb_extension_init`).
//!
//! # ABI v1 strict typing
//!
//! The engine inserts **no implicit casts** for extension arguments, so a
//! bare integer literal does not bind to a `BIGINT` parameter: write
//! `left(name, CAST(2 AS BIGINT))`. DuckDB widens `2` automatically; this is
//! an ABI v1 property shared by every extension, not a choice of this crate.

use pinkdb_ext_sdk::pinkdb_extension;
use regex::Regex;

/// Parse a DuckDB `regexp_*` options string into `regex` crate switches.
///
/// The letters and their left-to-right precedence match DuckDB (live-pinned):
/// `i`/`c` case-insensitive/case-sensitive, `l` literal, `m`/`n`/`p`
/// newline-sensitive (`.` does not match `\n`; the default), `s`
/// non-newline-sensitive (`.` matches `\n`) — later letters win, so `"sm"` is
/// newline-sensitive and `"ms"` is not. `g` (global replace) is refused for
/// every function here, exactly like DuckDB refuses it outside
/// `regexp_replace`; any other letter is an error (DuckDB errors too).
#[derive(Debug)]
struct RegexOptions {
    case_insensitive: bool,
    dot_matches_new_line: bool,
    literal: bool,
}

fn parse_regex_options(options: &str) -> Result<RegexOptions, String> {
    let mut out = RegexOptions {
        case_insensitive: false,
        dot_matches_new_line: false,
        literal: false,
    };
    for ch in options.chars() {
        match ch {
            'c' => out.case_insensitive = false,
            'i' => out.case_insensitive = true,
            'l' => out.literal = true,
            'm' | 'n' | 'p' => out.dot_matches_new_line = false,
            's' => out.dot_matches_new_line = true,
            // DuckDB: "Option 'g' (global replace) is only valid for regexp_replace".
            'g' => {
                return Err(
                    "option 'g' (global replace) is only valid for regexp_replace".to_string(),
                );
            }
            other => return Err(format!("unrecognized regex option {other}")),
        }
    }
    Ok(out)
}

/// Compile `pattern` under `options`; the `l` option escapes the pattern so it
/// matches literally (DuckDB's `'l'`), everything else is the RE2-style
/// syntax the `regex` crate shares with RE2/DuckDB.
fn compile_regex(pattern: &str, options: &str) -> Result<Regex, String> {
    let opts = parse_regex_options(options)?;
    let pattern = if opts.literal {
        regex::escape(pattern)
    } else {
        pattern.to_string()
    };
    regex::RegexBuilder::new(&pattern)
        .case_insensitive(opts.case_insensitive)
        .dot_matches_new_line(opts.dot_matches_new_line)
        .build()
        .map_err(|e| format!("invalid regular expression: {e}"))
}

/// The parts `split_part` indexes: `split(delimiter)`, or one part per
/// character when the delimiter is empty (DuckDB: `split_part('abc', '', 2)`
/// is `'b'`).
fn split_parts<'a>(string: &'a str, delimiter: &str) -> Vec<&'a str> {
    if !delimiter.is_empty() {
        return string.split(delimiter).collect();
    }
    let mut parts = Vec::new();
    for (start, ch) in string.char_indices() {
        parts.push(&string[start..start + ch.len_utf8()]);
    }
    parts
}

pinkdb_extension! {
    name = "string",
    functions = [
        /// The `index`-th part of `string` after splitting on `delimiter`.
        ///
        /// DuckDB-pinned: 1-based; `index = 0`, an out-of-range index and a
        /// part beyond either end return `''`; a negative index counts from
        /// the end (`-1` is the last part); an empty delimiter splits into
        /// characters; any NULL argument returns NULL.
        fn split_part(
            string: Option<&str>,
            delimiter: Option<&str>,
            index: Option<i64>,
        ) -> Option<String> {
            let (string, delimiter, index) = (string?, delimiter?, index?);
            let parts = split_parts(string, delimiter);
            let part = if index > 0 {
                // `try_from` so a huge index cannot truncate on 32-bit
                // targets (wasm32): out of range stays out of range.
                usize::try_from(index - 1).ok().and_then(|i| parts.get(i))
            } else if index < 0 {
                // `i128` so `index == i64::MIN` cannot overflow; a position
                // before the first part is out of range.
                let position = parts.len() as i128 + index as i128;
                if position < 0 {
                    None
                } else {
                    parts.get(position as usize)
                }
            } else {
                None
            };
            Some(part.copied().unwrap_or("").to_string())
        },

        /// `string` reversed by Unicode scalar values (not bytes), matching
        /// DuckDB on multibyte input: `reverse('héllo')` is `'olléh'`.
        fn reverse(string: Option<&str>) -> Option<String> {
            Some(string?.chars().rev().collect())
        },

        /// The first `count` characters of `string` (Unicode scalars).
        ///
        /// DuckDB-pinned: a negative `count` drops that many characters from
        /// the *right* (`left('hello', -2)` is `'hel'`), a count past the
        /// length is the whole string, and `0` is `''`.
        fn left(string: Option<&str>, count: Option<i64>) -> Option<String> {
            let (string, count) = (string?, count?);
            let chars: Vec<char> = string.chars().collect();
            // `usize::MAX` for a magnitude no `usize` can hold keeps huge
            // counts clamped to the whole string on 32-bit targets (wasm32).
            let magnitude = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
            let take = if count >= 0 {
                magnitude.min(chars.len())
            } else {
                chars.len().saturating_sub(magnitude.min(chars.len()))
            };
            Some(chars[..take].iter().collect())
        },

        /// The last `count` characters of `string` (Unicode scalars).
        ///
        /// DuckDB-pinned: a negative `count` drops that many characters from
        /// the *left* (`right('hello', -2)` is `'llo'`), a count past the
        /// length is the whole string, and `0` is `''`.
        fn right(string: Option<&str>, count: Option<i64>) -> Option<String> {
            let (string, count) = (string?, count?);
            let chars: Vec<char> = string.chars().collect();
            let magnitude = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
            let skip = if count >= 0 {
                chars.len().saturating_sub(magnitude.min(chars.len()))
            } else {
                magnitude.min(chars.len())
            };
            Some(chars[skip..].iter().collect())
        },

        /// The 1-based **character** position of the first occurrence of
        /// `search` in `string`, `0` when it does not occur. DuckDB-pinned:
        /// the empty search is found at position 1, and the position counts
        /// characters, not bytes (`strpos('aé b', 'b')` is `4`).
        fn strpos(string: Option<&str>, search: Option<&str>) -> Option<i64> {
            let (string, search) = (string?, search?);
            Some(match string.find(search) {
                Some(byte_offset) => string[..byte_offset].chars().count() as i64 + 1,
                None => 0,
            })
        },

        /// Whether `string` starts with `search` (an empty search always
        /// matches).
        fn starts_with(string: Option<&str>, search: Option<&str>) -> Option<bool> {
            let (string, search) = (string?, search?);
            Some(string.starts_with(search))
        },

        /// Whether `string` ends with `search` (an empty search always
        /// matches).
        fn ends_with(string: Option<&str>, search: Option<&str>) -> Option<bool> {
            let (string, search) = (string?, search?);
            Some(string.ends_with(search))
        },

        /// The first match of `pattern` in `string`, as `pattern`'s capturing
        /// group `group` (0, the default, is the whole match); `''` when the
        /// pattern does not match or the group did not participate.
        ///
        /// DuckDB-pinned: `group` must be 0..=9 (`NULL` yields `''`);
        /// `options` is required by this fixed-arity form and must not be
        /// `NULL` (DuckDB: "Regex options field must not be NULL"); an
        /// invalid pattern or option is a statement error. Divergence: DuckDB
        /// also accepts 2- and 3-argument calls and requires `group` to be a
        /// constant — ABI v1 has no overloads/optional arguments, so pass
        /// `NULL`/`''` explicitly.
        fn regexp_extract(
            string: Option<&str>,
            pattern: Option<&str>,
            group: Option<i32>,
            options: Option<&str>,
        ) -> Result<Option<String>, String> {
            // Validation order mirrors DuckDB's (live-pinned): options, then
            // group range, then the pattern, then NULL inputs.
            let Some(options) = options else {
                return Err("regex options must not be NULL".to_string());
            };
            if let Some(group) = group
                && !(0..=9).contains(&group)
            {
                return Err("group index must be between 0 and 9".to_string());
            }
            let Some(pattern) = pattern else {
                return Ok(None);
            };
            let regex = compile_regex(pattern, options)?;
            let Some(string) = string else {
                return Ok(None);
            };
            let Some(group) = group else {
                // DuckDB: a NULL group returns '' without matching.
                return Ok(Some(String::new()));
            };
            Ok(Some(
                regex
                    .captures(string)
                    .and_then(|caps| caps.get(group as usize))
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
            ))
        },

        /// Whether `pattern` matches anywhere in `string` (partial match, not
        /// a full match — use `^`/`$` to anchor), with DuckDB's `regexp_*`
        /// options: `i`/`c`, `l` (literal), `m`/`n`/`p` (newline-sensitive,
        /// the default), `s` (dot matches newline), applied left to right.
        ///
        /// DuckDB-pinned: `NULL` input or pattern is NULL, `NULL` options is
        /// an error, `g` is refused. Divergence: this fixed-arity form needs
        /// an explicit `options` argument (`''` for none); DuckDB also
        /// accepts the 2-argument call.
        fn regexp_matches(
            string: Option<&str>,
            pattern: Option<&str>,
            options: Option<&str>,
        ) -> Result<Option<bool>, String> {
            let Some(options) = options else {
                return Err("regex options must not be NULL".to_string());
            };
            let Some(pattern) = pattern else {
                return Ok(None);
            };
            let regex = compile_regex(pattern, options)?;
            let Some(string) = string else {
                return Ok(None);
            };
            Ok(Some(regex.is_match(string)))
        },

        /// **pinkdb-pinned shape**: the text of the first match of `pattern`
        /// in `string`, `''` when there is no match.
        ///
        /// Divergence, stated loudly: DuckDB has no `regexp_match` at all
        /// (`Catalog Error`); PostgreSQL's `regexp_match` returns
        /// `TEXT[]` — one element per capture group of the first match — and
        /// ABI v1 has no list type, so that shape cannot be represented here.
        /// This function is the pragmatic VARCHAR pin: the whole first match
        /// (exactly `regexp_extract(string, pattern, 0, '')`); use
        /// `regexp_extract` for a capture group. Inline flags (`(?i)`,
        /// `(?s)`, ...) still work; the fixed 2-argument form has no
        /// `options` parameter.
        fn regexp_match(string: Option<&str>, pattern: Option<&str>) -> Result<Option<String>, String> {
            let Some(pattern) = pattern else {
                return Ok(None);
            };
            let regex = compile_regex(pattern, "")?;
            let Some(string) = string else {
                return Ok(None);
            };
            Ok(Some(
                regex
                    .find(string)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
            ))
        },
    ],
}

// The cdylib half of this crate opts in to the ABI v1 `#[no_mangle]` exports
// (`LOAD EXTENSION '<path>'` looks them up). They stay out of the default
// build because this crate also builds the rlib the tests (and any linked
// host) use: two linked extension rlibs carrying the same globals fail to
// link on Linux (`duplicate symbol: pinkdb_extension_init`). `make -C
// extensions/string` builds the shared library with `--features abi-exports`.
#[cfg(feature = "abi-exports")]
pinkdb_ext_sdk::pinkdb_abi_exports!();

#[cfg(test)]
mod tests {
    use super::*;

    fn s(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    // ---------- declaration ----------

    #[test]
    fn the_declared_functions_are_exactly_the_string_surface_and_all_deterministic() {
        let specs = pinkdb_scalar_specs();
        let names: Vec<&str> = specs.iter().map(|s| s.name).collect();
        assert_eq!(
            names,
            vec![
                "split_part",
                "reverse",
                "left",
                "right",
                "strpos",
                "starts_with",
                "ends_with",
                "regexp_extract",
                "regexp_matches",
                "regexp_match",
            ]
        );
        // Deterministic functions only: no volatile list in the declaration.
        assert!(specs.iter().all(|s| !s.volatile));
        let linked = pinkdb_linked_extension().unwrap();
        assert_eq!(linked.name(), "string");
        assert_eq!(linked.functions().len(), specs.len());
        // The DuckDB-pinned declared types.
        let by_name = |name: &str| specs.iter().find(|s| s.name == name).unwrap();
        use pinkdb_ext_sdk::ExtType;
        assert_eq!(
            by_name("split_part").args,
            &[ExtType::Varchar, ExtType::Varchar, ExtType::Bigint]
        );
        assert_eq!(by_name("strpos").ret, ExtType::Bigint);
        assert_eq!(by_name("starts_with").ret, ExtType::Boolean);
        assert_eq!(
            by_name("regexp_extract").args,
            &[
                ExtType::Varchar,
                ExtType::Varchar,
                ExtType::Integer,
                ExtType::Varchar
            ]
        );
    }

    // ---------- split_part ----------

    #[test]
    fn split_part_matches_duckdb_including_zero_negative_and_out_of_range() {
        let split = |s: &str, d: &str, i: i64| split_part(Some(s), Some(d), Some(i));
        assert_eq!(split("a,b,c", ",", 1), s("a"));
        assert_eq!(split("a,b,c", ",", 3), s("c"));
        assert_eq!(split("a,b,c", ",", 4), s(""));
        assert_eq!(split("a,b,c", ",", 0), s(""));
        // Negative indexes count from the end.
        assert_eq!(split("a,b,c", ",", -1), s("c"));
        assert_eq!(split("a,b,c", ",", -2), s("b"));
        assert_eq!(split("a,b,c", ",", -3), s("a"));
        assert_eq!(split("a,b,c", ",", -4), s(""));
        assert_eq!(split("a,b,c", ",", i64::MIN), s(""));
        assert_eq!(split("a,b,c", ",", i64::MAX), s(""));
        // An empty delimiter splits into characters (DuckDB).
        assert_eq!(split("abc", "", 2), s("b"));
        assert_eq!(split("abc", "", -1), s("c"));
        assert_eq!(split("abc", "", 4), s(""));
        assert_eq!(split("", "", 1), s(""));
        // Edges and unicode.
        assert_eq!(split("a", ",", 1), s("a"));
        assert_eq!(split("a", ",", 2), s(""));
        assert_eq!(split("", ",", 1), s(""));
        assert_eq!(split("héllo,world", ",", 1), s("héllo"));
        assert_eq!(split("aXbXc", "X", -2), s("b"));
        assert_eq!(split(",a,,b,", ",", 5), s(""));
        assert_eq!(split("aXXb", "XX", 2), s("b"));
    }

    #[test]
    fn split_part_propagates_every_null() {
        assert_eq!(split_part(None, Some(","), Some(1)), None);
        assert_eq!(split_part(Some("a,b"), None, Some(1)), None);
        assert_eq!(split_part(Some("a,b"), Some(","), None), None);
        assert_eq!(split_part(None, None, None), None);
    }

    // ---------- reverse / left / right ----------

    #[test]
    fn reverse_is_unicode_aware() {
        assert_eq!(reverse(Some("hello")), s("olleh"));
        assert_eq!(reverse(Some("héllo")), s("olléh"));
        assert_eq!(reverse(Some("日本")), s("本日"));
        assert_eq!(reverse(Some("")), s(""));
        assert_eq!(reverse(None), None);
    }

    #[test]
    fn left_and_right_match_duckdb_negative_counts_included() {
        let left_of = |v: &str, n: i64| left(Some(v), Some(n));
        let right_of = |v: &str, n: i64| right(Some(v), Some(n));
        assert_eq!(left_of("hello", 2), s("he"));
        assert_eq!(left_of("hello", 0), s(""));
        assert_eq!(left_of("hello", 99), s("hello"));
        assert_eq!(left_of("hello", -2), s("hel"));
        assert_eq!(left_of("hello", -99), s(""));
        assert_eq!(left_of("hello", i64::MIN), s(""));
        assert_eq!(left_of("héllo", 4), s("héll"));
        assert_eq!(left_of("hello", 3), s("hel"));
        assert_eq!(left_of("", 1), s(""));

        assert_eq!(right_of("hello", 2), s("lo"));
        assert_eq!(right_of("hello", 0), s(""));
        assert_eq!(right_of("hello", 99), s("hello"));
        assert_eq!(right_of("hello", -2), s("llo"));
        assert_eq!(right_of("hello", -99), s(""));
        assert_eq!(right_of("hello", i64::MAX), s("hello"));
        assert_eq!(right_of("héllo", 4), s("éllo"));
        assert_eq!(right_of("", 1), s(""));

        assert_eq!(left(None, Some(1)), None);
        assert_eq!(left(Some("x"), None), None);
        assert_eq!(right(None, Some(1)), None);
        assert_eq!(right(Some("x"), None), None);
    }

    // ---------- strpos / starts_with / ends_with ----------

    #[test]
    fn strpos_is_one_based_character_positioned_and_zero_when_missing() {
        assert_eq!(strpos(Some("hello"), Some("ll")), Some(3));
        assert_eq!(strpos(Some("hello"), Some("z")), Some(0));
        assert_eq!(strpos(Some("hello"), Some("")), Some(1));
        assert_eq!(strpos(Some(""), Some("")), Some(1));
        assert_eq!(strpos(Some(""), Some("x")), Some(0));
        // Characters, not bytes: 'é' is 2 bytes and still position 2.
        assert_eq!(strpos(Some("héllo"), Some("é")), Some(2));
        assert_eq!(strpos(Some("héllo"), Some("llo")), Some(3));
        assert_eq!(strpos(Some("aé b"), Some("b")), Some(4));
        assert_eq!(strpos(None, Some("x")), None);
        assert_eq!(strpos(Some("x"), None), None);
    }

    #[test]
    fn starts_with_and_ends_with_are_strict_booleans() {
        assert_eq!(starts_with(Some("hello"), Some("he")), Some(true));
        assert_eq!(starts_with(Some("hello"), Some("el")), Some(false));
        assert_eq!(starts_with(Some("hello"), Some("")), Some(true));
        assert_eq!(starts_with(Some("héllo"), Some("hé")), Some(true));
        assert_eq!(ends_with(Some("hello"), Some("lo")), Some(true));
        assert_eq!(ends_with(Some("hello"), Some("el")), Some(false));
        assert_eq!(ends_with(Some("hello"), Some("")), Some(true));
        assert_eq!(ends_with(Some("héllo"), Some("llo")), Some(true));
        assert_eq!(starts_with(None, Some("a")), None);
        assert_eq!(starts_with(Some("a"), None), None);
        assert_eq!(ends_with(None, Some("a")), None);
        assert_eq!(ends_with(Some("a"), None), None);
    }

    // ---------- regexp options ----------

    #[test]
    fn option_parsing_matches_duckdb_letters_and_precedence() {
        let err = |options: &str| parse_regex_options(options).unwrap_err();
        assert!(err("q").contains("unrecognized regex option q"));
        assert!(err("g").contains("regexp_replace"));
        assert!(err("I").contains("unrecognized"));
        let parsed = parse_regex_options("sm").unwrap();
        assert!(
            !parsed.dot_matches_new_line,
            "'sm' -> m wins (newline-sensitive)"
        );
        let parsed = parse_regex_options("ms").unwrap();
        assert!(parsed.dot_matches_new_line, "'ms' -> s wins (dotall)");
        let parsed = parse_regex_options("ci").unwrap();
        assert!(parsed.case_insensitive);
        let parsed = parse_regex_options("ic").unwrap();
        assert!(!parsed.case_insensitive);
        let parsed = parse_regex_options("li").unwrap();
        assert!(parsed.literal && parsed.case_insensitive);
        // Empty options: the defaults.
        let parsed = parse_regex_options("").unwrap();
        assert!(!parsed.case_insensitive && !parsed.dot_matches_new_line && !parsed.literal);
    }

    // ---------- regexp_extract ----------

    #[test]
    fn regexp_extract_matches_duckdb_groups_and_no_match() {
        let extract = |v: &str, p: &str, g: Option<i32>, o: &str| {
            regexp_extract(Some(v), Some(p), g, Some(o))
        };
        assert_eq!(extract("hello", "l+", Some(0), ""), Ok(s("ll")));
        assert_eq!(extract("hello", "(l+)", Some(1), ""), Ok(s("ll")));
        assert_eq!(
            extract("abc-123", "([a-z]+)-([0-9]+)", Some(2), ""),
            Ok(s("123"))
        );
        // No match is '' (not NULL), and a non-participating group is ''.
        assert_eq!(extract("hello", "z", Some(0), ""), Ok(s("")));
        assert_eq!(extract("ac", "a(b)?c", Some(1), ""), Ok(s("")));
        // A group index with no such group is '' (DuckDB), 0..=9 only.
        assert_eq!(extract("abc", "a(b)c", Some(3), ""), Ok(s("")));
        assert_eq!(
            extract("abc", "a(b)c", Some(10), ""),
            Err("group index must be between 0 and 9".to_string())
        );
        assert_eq!(
            extract("abc", "a(b)c", Some(-1), ""),
            Err("group index must be between 0 and 9".to_string())
        );
        // NULL group is '' (DuckDB), NULL input/pattern is NULL.
        assert_eq!(extract("abc", "b", None, ""), Ok(s("")));
        assert_eq!(extract("abc", "b", None, "i"), Ok(s("")));
        assert_eq!(regexp_extract(None, Some("a"), Some(0), Some("")), Ok(None));
        assert_eq!(regexp_extract(Some("a"), None, Some(0), Some("")), Ok(None));
        // Options NULL is an error (DuckDB), and so is an invalid pattern.
        assert_eq!(
            regexp_extract(Some("a"), Some("a"), Some(0), None),
            Err("regex options must not be NULL".to_string())
        );
        assert!(
            regexp_extract(Some("a"), Some("("), Some(0), Some(""))
                .unwrap_err()
                .contains("invalid regular expression")
        );
        // An invalid pattern beats a NULL group (DuckDB validates the pattern).
        assert!(regexp_extract(Some("a"), Some("("), None, Some("")).is_err());
        // Options: i / c / l / s / m.
        assert_eq!(
            extract("HELLO world", "hello", Some(0), "i"),
            Ok(s("HELLO"))
        );
        assert_eq!(extract("HELLO world", "hello", Some(0), ""), Ok(s("")));
        assert_eq!(extract("HELLO", "hello", Some(0), "ic"), Ok(s("")));
        assert_eq!(extract("HELLO", "hello", Some(0), "ci"), Ok(s("HELLO")));
        assert_eq!(extract("a.b", "a.b", Some(0), "l"), Ok(s("a.b")));
        assert_eq!(extract("axb", "a.b", Some(0), "l"), Ok(s("")));
        assert_eq!(extract("a\nb", "a.b", Some(0), "s"), Ok(s("a\nb")));
        assert_eq!(extract("a\nb", "a.b", Some(0), ""), Ok(s("")));
        assert_eq!(extract("a\nb", "a.b", Some(0), "sm"), Ok(s("")));
        assert_eq!(extract("a\nb", "a.b", Some(0), "ms"), Ok(s("a\nb")));
        // Inline flags work like DuckDB/RE2.
        assert_eq!(extract("a\nb", "(?s)a.b", Some(0), ""), Ok(s("a\nb")));
        assert_eq!(extract("abc", "(?i)ABC", Some(0), ""), Ok(s("abc")));
        // Unicode.
        assert_eq!(extract("héllo", "l+", Some(0), ""), Ok(s("ll")));
        assert_eq!(extract("héllo", "(é)", Some(1), ""), Ok(s("é")));
        // The first match wins, not the longest.
        assert_eq!(extract("aa", "a", Some(0), ""), Ok(s("a")));
    }

    // ---------- regexp_matches ----------

    #[test]
    fn regexp_matches_is_a_partial_match_with_duckdb_null_and_error_rules() {
        let matches = |v: &str, p: &str, o: &str| regexp_matches(Some(v), Some(p), Some(o));
        assert_eq!(matches("hello", "l", ""), Ok(Some(true)));
        assert_eq!(matches("hello", "^h", ""), Ok(Some(true)));
        assert_eq!(matches("hello", "^e", ""), Ok(Some(false)));
        assert_eq!(matches("hello", "o$", ""), Ok(Some(true)));
        assert_eq!(matches("abc", "^abc$", ""), Ok(Some(true)));
        assert_eq!(matches("abc", "b", ""), Ok(Some(true)));
        assert_eq!(matches("abc", "z", ""), Ok(Some(false)));
        assert_eq!(matches("abc", "", ""), Ok(Some(true)));
        // Case handling.
        assert_eq!(matches("HELLO", "hello", "i"), Ok(Some(true)));
        assert_eq!(matches("HELLO", "hello", "c"), Ok(Some(false)));
        // Dot/newline handling.
        assert_eq!(matches("a\nb", "a.b", ""), Ok(Some(false)));
        assert_eq!(matches("a\nb", "a.b", "s"), Ok(Some(true)));
        assert_eq!(matches("a\nb", "a.b", "sm"), Ok(Some(false)));
        // Literal option.
        assert_eq!(matches("a.b", "a.b", "l"), Ok(Some(true)));
        assert_eq!(matches("axb", "a.b", "l"), Ok(Some(false)));
        // Inline flags.
        assert_eq!(matches("a\nb", "(?s)a.b", ""), Ok(Some(true)));
        // NULL policy: input/pattern NULL -> NULL; options NULL -> error.
        assert_eq!(regexp_matches(None, Some("a"), Some("")), Ok(None));
        assert_eq!(regexp_matches(Some("a"), None, Some("")), Ok(None));
        assert_eq!(
            regexp_matches(Some("a"), Some("a"), None),
            Err("regex options must not be NULL".to_string())
        );
        // Options beat NULL patterns (DuckDB checks options first).
        assert_eq!(
            regexp_matches(Some("a"), None, None),
            Err("regex options must not be NULL".to_string())
        );
        // 'g' is refused outside regexp_replace.
        assert!(
            regexp_matches(Some("a"), Some("a"), Some("g"))
                .unwrap_err()
                .contains("regexp_replace")
        );
    }

    // ---------- regexp_match (pinned) ----------

    #[test]
    fn regexp_match_returns_the_first_match_text() {
        let m = |v: &str, p: &str| regexp_match(Some(v), Some(p));
        assert_eq!(m("hello", "l+"), Ok(s("ll")));
        assert_eq!(m("aa", "a"), Ok(s("a")));
        assert_eq!(m("héllo", "(é)"), Ok(s("é")));
        assert_eq!(m("abc", "z"), Ok(s("")));
        assert_eq!(m("abc", ""), Ok(s("")));
        assert_eq!(m("HELLO", "(?i)hello"), Ok(s("HELLO")));
        assert_eq!(regexp_match(None, Some("a")), Ok(None));
        assert_eq!(regexp_match(Some("a"), None), Ok(None));
        assert!(regexp_match(Some("a"), Some("(")).is_err());
    }
}
