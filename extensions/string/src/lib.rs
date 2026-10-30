//! `string` — pinkdb's core string extension (Rust extension SDK, ABI v1).
//!
//! One [`pinkdb_extension!`] declaration defines fourteen deterministic
//! functions, DuckDB-pinned (verified live against DuckDB
//! `v2.0.0-alpha46057`):
//!
//! | function | DuckDB signature | here |
//! |---|---|---|
//! | [`split_part`] | `(VARCHAR, VARCHAR, BIGINT) -> VARCHAR` | same |
//! | [`reverse`] | `(VARCHAR) -> VARCHAR` | same |
//! | [`left`] / [`right`] | `(VARCHAR, BIGINT) -> VARCHAR` | same |
//! | [`strpos`] | `(VARCHAR, VARCHAR) -> BIGINT` | same |
//! | [`starts_with`] / [`ends_with`] | `(VARCHAR, VARCHAR) -> BOOLEAN` | same |
//! | [`lpad`] / [`rpad`] | `(VARCHAR, INTEGER, VARCHAR) -> VARCHAR` | same |
//! | [`concat_ws`] | `(VARCHAR, VARCHAR, *VARCHAR) -> VARCHAR` | same, VARCHAR arguments only |
//! | [`regexp_extract`] | `(VARCHAR, VARCHAR [, INTEGER [, VARCHAR]]) -> VARCHAR` | same, optional arity |
//! | [`regexp_matches`] | `(VARCHAR, VARCHAR [, VARCHAR]) -> BOOLEAN` | same, optional arity |
//! | [`regexp_replace`] | `(VARCHAR, VARCHAR, VARCHAR [, VARCHAR]) -> VARCHAR` | same, optional arity |
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
//! # ABI v1 implicit casts
//!
//! The engine widens an INTEGER argument into a `BIGINT` parameter and a DATE
//! into a `TIMESTAMP` one, the two implicit casts DuckDB applies
//! (`left(name, 2)` works). Every other declared type is exact: a VARCHAR
//! count stays a bind error instead of being parsed.

use pinkdb_ext_sdk::{Optional, pinkdb_extension};
use regex::Regex;

/// Parse a DuckDB `regexp_*` options string into `regex` crate switches.
///
/// The letters and their left-to-right precedence match DuckDB (live-pinned):
/// `i`/`c` case-insensitive/case-sensitive, `l` literal, `m`/`n`/`p`
/// newline-sensitive (`.` does not match `\n`; the default), `s`
/// non-newline-sensitive (`.` matches `\n`) — later letters win, so `"sm"` is
/// newline-sensitive and `"ms"` is not. `g` (global replace) is parsed into
/// [`RegexOptions::global`]; only `regexp_replace` accepts it, and the other
/// callers refuse it like DuckDB does. Any other letter is an error (DuckDB
/// errors too).
#[derive(Debug)]
struct RegexOptions {
    case_insensitive: bool,
    dot_matches_new_line: bool,
    literal: bool,
    global: bool,
}

fn parse_regex_options(options: &str) -> Result<RegexOptions, String> {
    let mut out = RegexOptions {
        case_insensitive: false,
        dot_matches_new_line: false,
        literal: false,
        global: false,
    };
    for ch in options.chars() {
        match ch {
            'c' => out.case_insensitive = false,
            'i' => out.case_insensitive = true,
            'g' => out.global = true,
            'l' => out.literal = true,
            'm' | 'n' | 'p' => out.dot_matches_new_line = false,
            's' => out.dot_matches_new_line = true,
            other => return Err(format!("unrecognized regex option {other}")),
        }
    }
    Ok(out)
}

/// Compile `pattern` under `options`; the `l` option escapes the pattern so it
/// matches literally (DuckDB's `'l'`), everything else is the RE2-style
/// syntax the `regex` crate shares with RE2/DuckDB. Returns the compiled
/// pattern and whether `g` asked for a global replace; `allow_global` is false
/// for every caller but `regexp_replace`.
/// Validate an options string without a pattern: DuckDB checks the options
/// (and the `g` rule) before it looks at NULL inputs, so a bad option is an
/// error even when the pattern or the input is NULL.
fn check_regex_options(options: &str, allow_global: bool) -> Result<(), String> {
    if parse_regex_options(options)?.global && !allow_global {
        return Err("option 'g' (global replace) is only valid for regexp_replace".to_string());
    }
    Ok(())
}

fn compile_regex(
    pattern: &str,
    options: &str,
    allow_global: bool,
) -> Result<(Regex, bool), String> {
    let opts = parse_regex_options(options)?;
    if opts.global && !allow_global {
        // DuckDB: "Option 'g' (global replace) is only valid for regexp_replace".
        return Err("option 'g' (global replace) is only valid for regexp_replace".to_string());
    }
    let pattern = if opts.literal {
        regex::escape(pattern)
    } else {
        pattern.to_string()
    };
    let regex = regex::RegexBuilder::new(&pattern)
        .case_insensitive(opts.case_insensitive)
        .dot_matches_new_line(opts.dot_matches_new_line)
        .build()
        .map_err(|e| format!("invalid regular expression: {e}"))?;
    Ok((regex, opts.global))
}

/// One piece of a `regexp_replace` replacement string: RE2 rewrites `\0`..`\9`
/// to a capture group and `\\` to a backslash, everything else is literal.
/// Parsing and validating happen before the search, like DuckDB (which rejects
/// a bad rewrite schema even when the pattern does not match).
enum ReplacementPart {
    Literal(String),
    Group(usize),
}

fn parse_replacement(replacement: &str, groups: usize) -> Result<Vec<ReplacementPart>, String> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut chars = replacement.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            literal.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => literal.push('\\'),
            Some(digit @ '0'..='9') => {
                let group = digit.to_digit(10).expect("a decimal digit") as usize;
                if group > groups {
                    return Err(format!(
                        "replacement references group {group}, but the pattern has {groups} group(s)"
                    ));
                }
                if !literal.is_empty() {
                    parts.push(ReplacementPart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(ReplacementPart::Group(group));
            }
            Some(other) => {
                return Err(format!(
                    "replacement string: '\\' must be followed by a digit or '\\', found '{other}'"
                ));
            }
            None => return Err("replacement string: '\\' not allowed at end".to_string()),
        }
    }
    if !literal.is_empty() {
        parts.push(ReplacementPart::Literal(literal));
    }
    Ok(parts)
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

/// DuckDB's `lpad`/`rpad` body: character-based, and a `count` no longer than
/// the string truncates it. The pad string is only consulted when padding is
/// needed, which is why an empty one is an error only then (`lpad('abc', 3,
/// '')` is `'abc'`, `lpad('a', 2, '')` raises).
fn pad(
    string: Option<&str>,
    count: Option<i32>,
    characters: Option<&str>,
    on_left: bool,
) -> Result<Option<String>, String> {
    let (Some(string), Some(count), Some(characters)) = (string, count, characters) else {
        return Ok(None);
    };
    let count = count.max(0) as usize;
    let mut chars: Vec<char> = string.chars().collect();
    if chars.len() >= count {
        chars.truncate(count);
        return Ok(Some(chars.into_iter().collect()));
    }
    let pad: Vec<char> = characters.chars().collect();
    if pad.is_empty() {
        let which = if on_left { "LPAD" } else { "RPAD" };
        return Err(format!("Insufficient padding in {which}."));
    }
    let fill = count - chars.len();
    let padding = (0..fill).map(|i| pad[i % pad.len()]);
    let padded = if on_left {
        padding.chain(chars).collect()
    } else {
        chars.into_iter().chain(padding).collect()
    };
    Ok(Some(padded))
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

        /// Pads `string` to `count` characters by repeating `characters` on
        /// the left (DuckDB's `lpad('a', 5, 'abc')` is `'abcaa'`).
        ///
        /// DuckDB-pinned: characters, not bytes; a `count` no longer than the
        /// string **truncates** it from the right (`lpad('abcdef', 3, 'x')`
        /// is `'abc'`); a negative `count` is `''`; padding that needs an
        /// empty pad string is an error; NULL string/count/pad is NULL.
        fn lpad(
            string: Option<&str>,
            count: Option<i32>,
            characters: Option<&str>,
        ) -> Result<Option<String>, String> {
            pad(string, count, characters, true)
        },

        /// `lpad` on the right: pads by repeating `characters` after `string`
        /// (`rpad('a', 5, 'abc')` is `'aabca'`), truncates the same way, and
        /// has the same NULL and empty-pad rules.
        fn rpad(
            string: Option<&str>,
            count: Option<i32>,
            characters: Option<&str>,
        ) -> Result<Option<String>, String> {
            pad(string, count, characters, false)
        },

        /// Joins the arguments after the separator, skipping NULLs
        /// (DuckDB: `concat_ws(',', 'a', NULL, 'b')` is `'a,b'`); a NULL
        /// separator makes the whole result NULL. The separator and at least
        /// one value are required, as in DuckDB.
        ///
        /// Divergence: DuckDB casts every argument to VARCHAR, ABI v1
        /// declares VARCHAR only, so `concat_ws(',', 1, 2.5)` needs explicit
        /// casts.
        variadic(1) fn concat_ws(values: Vec<Option<&str>>) -> Result<Option<String>, String> {
            // The variadic tail is one list, so the two-argument minimum is a
            // body check rather than a declared `min_args`.
            if values.len() < 2 {
                return Err("concat_ws takes at least 2 argument(s)".to_string());
            }
            let Some(separator) = values[0] else {
                return Ok(None);
            };
            let mut out = String::new();
            let mut first = true;
            for value in values[1..].iter().flatten() {
                if !first {
                    out.push_str(separator);
                }
                out.push_str(value);
                first = false;
            }
            Ok(Some(out))
        },

        /// The first match of `pattern` in `string`, as `pattern`'s capturing
        /// group `group` (0 when omitted, the whole match); `''` when the
        /// pattern does not match or the group did not participate.
        ///
        /// DuckDB-pinned: the arguments are optional left to right
        /// (`regexp_extract(s, p [, group [, options]])`); `group` is an
        /// INTEGER in 0..=9 (DuckDB takes INTEGER, so a BIGINT needs a CAST;
        /// `NULL` yields `''`); `options` defaults to `''` and an
        /// explicit `NULL` is an error (DuckDB: "Regex options field must not
        /// be NULL"); an invalid pattern or option is a statement error.
        /// Divergence: DuckDB requires `group` to be a constant, ABI v1
        /// evaluates every argument, so a column works here.
        optional(2)
        fn regexp_extract(
            string: Option<&str>,
            pattern: Option<&str>,
            group: Optional<i32>,
            options: Optional<&str>,
        ) -> Result<Option<String>, String> {
            // Validation order mirrors DuckDB's (live-pinned): options, then
            // group range, then the pattern, then NULL inputs.
            let options = match options {
                Optional::Absent => "",
                Optional::Null => return Err("regex options must not be NULL".to_string()),
                Optional::Value(options) => options,
            };
            let group = match group {
                Optional::Absent => Some(0),
                Optional::Null => None,
                Optional::Value(group) => Some(group),
            };
            if let Some(group) = group
                && !(0..=9).contains(&group)
            {
                return Err("group index must be between 0 and 9".to_string());
            }
            check_regex_options(options, false)?;
            let Some(pattern) = pattern else {
                return Ok(None);
            };
            let (regex, _) = compile_regex(pattern, options, false)?;
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
        /// DuckDB-pinned: `options` is optional
        /// (`regexp_matches(s, p [, options])`), defaults to `''`, and an
        /// explicit `NULL` is an error; `NULL` input or pattern is NULL; `g`
        /// is refused.
        optional(2)
        fn regexp_matches(
            string: Option<&str>,
            pattern: Option<&str>,
            options: Optional<&str>,
        ) -> Result<Option<bool>, String> {
            let options = match options {
                Optional::Absent => "",
                Optional::Null => return Err("regex options must not be NULL".to_string()),
                Optional::Value(options) => options,
            };
            check_regex_options(options, false)?;
            let Some(pattern) = pattern else {
                return Ok(None);
            };
            let (regex, _) = compile_regex(pattern, options, false)?;
            let Some(string) = string else {
                return Ok(None);
            };
            Ok(Some(regex.is_match(string)))
        },

        /// Replaces the first match of `pattern` in `string` with
        /// `replacement` — every match with the `g` option — rewriting
        /// `\0`..`\9` in `replacement` to a capture group and `\\` to a
        /// backslash, DuckDB's RE2 rewrite syntax.
        ///
        /// DuckDB-pinned: `regexp_replace(s, p, r [, options])`; a NULL
        /// input, pattern or replacement is NULL before any pattern is
        /// compiled; a NULL options field is an error; and a rewrite that
        /// references a group the pattern does not have (or ends in a
        /// backslash) is an error even when the pattern does not match.
        optional(3)
        fn regexp_replace(
            string: Option<&str>,
            pattern: Option<&str>,
            replacement: Option<&str>,
            options: Optional<&str>,
        ) -> Result<Option<String>, String> {
            let options = match options {
                Optional::Absent => "",
                Optional::Null => return Err("regex options must not be NULL".to_string()),
                Optional::Value(options) => options,
            };
            check_regex_options(options, true)?;
            let (Some(string), Some(pattern), Some(replacement)) = (string, pattern, replacement)
            else {
                return Ok(None);
            };
            let (regex, global) = compile_regex(pattern, options, true)?;
            let parts = parse_replacement(replacement, regex.captures_len() - 1)?;
            let expand = |caps: &regex::Captures<'_>| -> String {
                parts
                    .iter()
                    .map(|part| match part {
                        ReplacementPart::Literal(text) => text.as_str(),
                        ReplacementPart::Group(group) => {
                            caps.get(*group).map_or("", |m| m.as_str())
                        }
                    })
                    .collect()
            };
            let replaced = if global {
                regex.replace_all(string, expand)
            } else {
                regex.replacen(string, 1, expand)
            };
            Ok(Some(replaced.into_owned()))
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
            let regex = compile_regex(pattern, "", false)?.0;
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

/// The registry name: `LOAD EXTENSION 'string'` once registered.
pub const EXTENSION_NAME: &str = "string";

/// Publish this extension in the process-global linked registry (the CLI and
/// the server do this at startup, so the functions work without a `LOAD
/// EXTENSION`). A second call for the same extension is a no-op, so every
/// host and test binary can call it unconditionally.
pub fn ensure_registered() -> Result<(), pinkdb_ext_sdk::ExtError> {
    match pinkdb_register_linked() {
        Ok(()) => Ok(()),
        Err(error) => match pinkdb_ext_sdk::linked::get(EXTENSION_NAME) {
            Some(existing) if same_shape(&existing) => Ok(()),
            _ => Err(error),
        },
    }
}

/// Whether `existing` declares exactly our function names (a second
/// bootstrap of *this* extension, not a foreign owner of the name).
fn same_shape(existing: &pinkdb_ext_sdk::linked::LinkedExtension) -> bool {
    let Ok(ours) = pinkdb_linked_extension() else {
        return false;
    };
    let mut a: Vec<&str> = ours.functions().iter().map(|f| f.name()).collect();
    let mut b: Vec<&str> = existing.functions().iter().map(|f| f.name()).collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
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
                "lpad",
                "rpad",
                "concat_ws",
                "regexp_extract",
                "regexp_matches",
                "regexp_replace",
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
        // The optional/variadic ranges DuckDB declares (and the fixed ones).
        assert_eq!(by_name("regexp_extract").min_args, 2);
        assert_eq!(by_name("regexp_matches").min_args, 2);
        assert_eq!(by_name("regexp_replace").min_args, 3);
        assert_eq!(by_name("regexp_replace").args.len(), 4);
        assert_eq!(
            by_name("lpad").args,
            &[ExtType::Varchar, ExtType::Integer, ExtType::Varchar]
        );
        assert_eq!(
            by_name("rpad").args,
            &[ExtType::Varchar, ExtType::Integer, ExtType::Varchar]
        );
        assert_eq!(by_name("concat_ws").args, &[ExtType::Varchar]);
        assert!(by_name("concat_ws").variadic);
        assert_eq!(by_name("concat_ws").min_args, 1);
        assert_eq!(by_name("concat_ws").ret, ExtType::Varchar);
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
        assert!(err("I").contains("unrecognized"));
        // `g` parses, but only regexp_replace may compile with it.
        assert!(parse_regex_options("g").unwrap().global);
        assert!(
            compile_regex("a", "g", false)
                .unwrap_err()
                .contains("regexp_replace")
        );
        assert!(compile_regex("a", "g", true).unwrap().1);
        assert!(!compile_regex("a", "", true).unwrap().1);
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
            regexp_extract(
                Some(v),
                Some(p),
                g.map_or(Optional::Null, Optional::Value),
                Optional::Value(o),
            )
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
        // The DuckDB optional forms: group defaults to 0, options to ''.
        assert_eq!(
            regexp_extract(Some("abc"), Some("b"), Optional::Absent, Optional::Absent),
            Ok(s("b"))
        );
        assert_eq!(
            regexp_extract(
                Some("abc-123"),
                Some("([a-z]+)-([0-9]+)"),
                Optional::Value(2),
                Optional::Absent
            ),
            Ok(s("123"))
        );
        assert_eq!(
            regexp_extract(Some("abc"), Some("b"), Optional::Value(1), Optional::Absent),
            Ok(s(""))
        );
        assert_eq!(
            regexp_extract(None, Some("a"), Optional::Absent, Optional::Absent),
            Ok(None)
        );
        assert_eq!(
            regexp_extract(Some("a"), None, Optional::Value(0), Optional::Value("")),
            Ok(None)
        );
        // Options NULL is an error (DuckDB), and so is an invalid pattern.
        assert_eq!(
            regexp_extract(Some("a"), Some("a"), Optional::Value(0), Optional::Null),
            Err("regex options must not be NULL".to_string())
        );
        assert!(
            regexp_extract(Some("a"), Some("("), Optional::Value(0), Optional::Absent)
                .unwrap_err()
                .contains("invalid regular expression")
        );
        // An invalid pattern beats a NULL group (DuckDB validates the pattern).
        assert!(regexp_extract(Some("a"), Some("("), Optional::Null, Optional::Absent).is_err());
        // Options are validated before a NULL pattern (DuckDB errors first).
        assert_eq!(
            regexp_extract(Some("a"), None, Optional::Value(0), Optional::Value("q")),
            Err("unrecognized regex option q".to_string())
        );
        assert_eq!(
            regexp_extract(Some("a"), None, Optional::Value(0), Optional::Value("g")),
            Err("option 'g' (global replace) is only valid for regexp_replace".to_string())
        );
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
        let matches =
            |v: &str, p: &str, o: &str| regexp_matches(Some(v), Some(p), Optional::Value(o));
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
        // The DuckDB 2-argument form: options default to ''.
        assert_eq!(
            regexp_matches(Some("abc"), Some("b"), Optional::Absent),
            Ok(Some(true))
        );
        assert_eq!(
            regexp_matches(Some("ABC"), Some("b"), Optional::Absent),
            Ok(Some(false))
        );
        // NULL policy: input/pattern NULL -> NULL; options NULL -> error.
        assert_eq!(
            regexp_matches(None, Some("a"), Optional::Value("")),
            Ok(None)
        );
        assert_eq!(
            regexp_matches(Some("a"), None, Optional::Value("")),
            Ok(None)
        );
        assert_eq!(
            regexp_matches(Some("a"), Some("a"), Optional::Null),
            Err("regex options must not be NULL".to_string())
        );
        // Options beat NULL patterns (DuckDB checks options first).
        assert_eq!(
            regexp_matches(Some("a"), None, Optional::Null),
            Err("regex options must not be NULL".to_string())
        );
        // A bad option beats a NULL pattern or input too (DuckDB validates the
        // options first).
        assert!(
            regexp_matches(Some("a"), None, Optional::Value("q"))
                .unwrap_err()
                .contains("unrecognized regex option q")
        );
        assert!(
            regexp_matches(None, Some("a"), Optional::Value("g"))
                .unwrap_err()
                .contains("regexp_replace")
        );
        // 'g' is refused outside regexp_replace.
        assert!(
            regexp_matches(Some("a"), Some("a"), Optional::Value("g"))
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

    // ---------- regexp_replace ----------

    #[test]
    fn regexp_replace_matches_duckdb_rewrites_and_global_option() {
        let replace = |v: &str, p: &str, r: &str, o: &str| {
            regexp_replace(Some(v), Some(p), Some(r), Optional::Value(o))
        };
        // The first match only, unless `g` asks for every match.
        assert_eq!(replace("abcabc", "b", "x", ""), Ok(s("axcabc")));
        assert_eq!(replace("abcabc", "b", "x", "g"), Ok(s("axcaxc")));
        assert_eq!(replace("aaa", "a", "x", "g"), Ok(s("xxx")));
        // RE2 rewrites: \1 a group, \0 the whole match, \\ a backslash.
        assert_eq!(replace("abcabc", "(b)", "[\\1]", ""), Ok(s("a[b]cabc")));
        assert_eq!(replace("abcabc", "(b)", "[\\1]", "g"), Ok(s("a[b]ca[b]c")));
        assert_eq!(replace("abc", "(b)", "\\1\\1", ""), Ok(s("abbc")));
        assert_eq!(replace("abc", "b", "<\\0>", ""), Ok(s("a<b>c")));
        assert_eq!(replace("abc", "b", "\\\\", ""), Ok(s("a\\c")));
        // `$` is not a reference in DuckDB's replacement syntax.
        assert_eq!(replace("abc", "b", "$1", ""), Ok(s("a$1c")));
        // Options and the no-match case.
        assert_eq!(replace("ABC", "b", "x", "i"), Ok(s("AxC")));
        assert_eq!(replace("a.b", "a.b", "x", "l"), Ok(s("x")));
        assert_eq!(replace("abc", "z", "x", ""), Ok(s("abc")));
        // An empty pattern matches at every position.
        assert_eq!(replace("abc", "", "-", ""), Ok(s("-abc")));
        assert_eq!(replace("abc", "", "-", "g"), Ok(s("-a-b-c-")));
        // NULL input/pattern/replacement is NULL before any validation; a
        // NULL options field is the error DuckDB raises.
        assert_eq!(
            regexp_replace(None, Some("b"), Some("\\q"), Optional::Absent),
            Ok(None)
        );
        assert_eq!(
            regexp_replace(Some("abc"), None, Some("\\q"), Optional::Absent),
            Ok(None)
        );
        assert_eq!(
            regexp_replace(Some("abc"), Some("b"), None, Optional::Absent),
            Ok(None)
        );
        assert_eq!(
            regexp_replace(Some("abc"), Some("b"), Some("x"), Optional::Null),
            Err("regex options must not be NULL".to_string())
        );
        // The rewrite schema is validated even when the pattern never matches.
        assert!(
            regexp_replace(Some("xyz"), Some("b"), Some("\\q"), Optional::Absent)
                .unwrap_err()
                .contains("must be followed by a digit")
        );
        assert!(
            regexp_replace(Some("xyz"), Some("(b)"), Some("\\2"), Optional::Absent)
                .unwrap_err()
                .contains("group 2")
        );
        assert!(
            regexp_replace(Some("abc"), Some("b"), Some("\\"), Optional::Absent)
                .unwrap_err()
                .contains("not allowed at end")
        );
        // Options and patterns are validated like the other regexp_* calls.
        assert!(
            replace("abc", "b", "x", "G")
                .unwrap_err()
                .contains("unrecognized regex option G")
        );
        assert!(
            regexp_replace(Some("a"), Some("("), Some("x"), Optional::Absent)
                .unwrap_err()
                .contains("invalid regular expression")
        );
        // The DuckDB 3-argument form: first match only, no options.
        assert_eq!(
            regexp_replace(Some("abcabc"), Some("b"), Some("x"), Optional::Absent),
            Ok(s("axcabc"))
        );
    }

    // ---------- lpad / rpad ----------

    #[test]
    fn lpad_and_rpad_match_duckdb_padding_truncation_and_errors() {
        let left_pad = |v: &str, n: i32, c: &str| lpad(Some(v), Some(n), Some(c));
        let right_pad = |v: &str, n: i32, c: &str| rpad(Some(v), Some(n), Some(c));
        // Padding repeats the pad string, character by character.
        assert_eq!(left_pad("a", 3, "x"), Ok(s("xxa")));
        assert_eq!(left_pad("a", 3, "xyz"), Ok(s("xya")));
        assert_eq!(left_pad("a", 5, "ab"), Ok(s("ababa")));
        assert_eq!(left_pad("a", 5, "abc"), Ok(s("abcaa")));
        assert_eq!(right_pad("a", 5, "abc"), Ok(s("aabca")));
        assert_eq!(right_pad("", 2, "ab"), Ok(s("ab")));
        assert_eq!(left_pad("", 3, "x"), Ok(s("xxx")));
        // A count no longer than the string truncates it.
        assert_eq!(left_pad("abcdef", 3, "x"), Ok(s("abc")));
        assert_eq!(right_pad("abcdef", 3, "x"), Ok(s("abc")));
        assert_eq!(left_pad("abc", 3, ""), Ok(s("abc")));
        assert_eq!(left_pad("abc", 2, ""), Ok(s("ab")));
        assert_eq!(left_pad("a", 1, ""), Ok(s("a")));
        // Negative and zero counts are the empty string.
        assert_eq!(left_pad("abc", -1, "x"), Ok(s("")));
        assert_eq!(left_pad("abc", -1, ""), Ok(s("")));
        assert_eq!(left_pad("a", 0, "x"), Ok(s("")));
        // Characters, not bytes.
        assert_eq!(left_pad("héllo", 6, "x"), Ok(s("xhéllo")));
        assert_eq!(left_pad("a", 3, "éé"), Ok(s("ééa")));
        // An empty pad string is an error only when padding is needed.
        assert!(
            left_pad("a", 3, "")
                .unwrap_err()
                .contains("Insufficient padding in LPAD.")
        );
        assert!(left_pad("a", 2, "").unwrap_err().contains("LPAD"));
        assert!(right_pad("a", 3, "").unwrap_err().contains("RPAD"));
        // NULL string/count/pad propagate.
        assert_eq!(lpad(None, Some(3), Some("x")), Ok(None));
        assert_eq!(lpad(Some("a"), None, Some("x")), Ok(None));
        assert_eq!(lpad(Some("a"), Some(3), None), Ok(None));
        assert_eq!(rpad(None, Some(3), Some("x")), Ok(None));
        assert_eq!(rpad(Some("a"), Some(3), None), Ok(None));
    }

    // ---------- concat_ws ----------

    #[test]
    fn concat_ws_skips_nulls_and_a_null_separator_poisons_the_row() {
        let join = |values: &[Option<&str>]| concat_ws(values.to_vec());
        assert_eq!(join(&[Some(","), Some("a"), None, Some("b")]), Ok(s("a,b")));
        assert_eq!(join(&[Some(","), Some("a")]), Ok(s("a")));
        assert_eq!(join(&[Some(","), None, None]), Ok(s("")));
        assert_eq!(join(&[Some(""), Some("a"), Some("b")]), Ok(s("ab")));
        assert_eq!(
            join(&[Some("|"), Some("a"), Some(""), Some("b")]),
            Ok(s("a||b"))
        );
        // A NULL separator makes the whole result NULL, before any value.
        assert_eq!(join(&[None, Some("a")]), Ok(None));
        assert_eq!(join(&[None, None]), Ok(None));
        // DuckDB requires the separator and at least one value.
        assert!(
            join(&[Some(",")])
                .unwrap_err()
                .contains("at least 2 argument(s)")
        );
    }
}
