//! JSON path parsing and lookup: the DuckDB v1 path subset.
//!
//! Grammar (probed against DuckDB α46057):
//!
//! ```text
//! path   := '$' step*
//! step   := '.' name            unquoted key: runs to the next '.' or '['
//!         | '.' '"' escaped '"' quoted key: \" and \\ are the escapes
//!         | '[' int ']'         array index; negative counts from the end
//! ```
//!
//! Everything else is a path **error** (`JSON path error near '<rest>'`), like
//! DuckDB: an empty key (`$.""`), a trailing dot, a bracket that is not an
//! integer index, a stray `$`, and — deliberately — the wildcard/recursive
//! constructs DuckDB does support (`[*]`, `[0,1]`, `$..a`), which v1 refuses
//! with a clear message instead of guessing.
//!
//! Lookup rules: a key that is absent, or a step applied to the wrong kind of
//! value (`.a` on an array, `[0]` on an object/scalar), yields "no match" —
//! the SQL functions turn that into NULL, exactly like DuckDB. A duplicate
//! key resolves to its **first** occurrence (DuckDB: `{"a":1,"a":2}` →
//! `$.a` is `1`).

use crate::value::Json;

/// One path step.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// `.name` / `."name"`.
    Key(String),
    /// `[index]`; negative indices count from the end at lookup time.
    Index(i64),
}

/// A parsed path: the `$` root plus its steps.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Path {
    steps: Vec<Step>,
}

impl Path {
    /// The steps, in order.
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }
}

/// Parse `input`; the message mirrors DuckDB's `JSON path error near '…'`.
pub fn parse_path(input: &str) -> Result<Path, String> {
    let mut rest = input
        .strip_prefix('$')
        .ok_or_else(|| format!("JSON path error near '{}'", near(input)))?;
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('.') {
            if let Some(quoted) = after.strip_prefix('"') {
                let (key, used) = parse_quoted_key(quoted)?;
                if key.is_empty() {
                    // DuckDB refuses an empty quoted key (`$.""`).
                    return Err(format!("JSON path error near '{}'", near(rest)));
                }
                steps.push(Step::Key(key));
                rest = &after[used..];
            } else {
                let end = after.find(['.', '[']).unwrap_or(after.len());
                if end == 0 {
                    return Err(format!("JSON path error near '{}'", near(rest)));
                }
                steps.push(Step::Key(after[..end].to_string()));
                rest = &after[end..];
            }
        } else if let Some(after) = rest.strip_prefix('[') {
            if after.starts_with('*') {
                return Err(
                    "JSON path wildcards ([*], [0,1], $..a) are not supported in v1".to_string(),
                );
            }
            let close = after
                .find(']')
                .ok_or_else(|| format!("JSON path error near '{}'", near(rest)))?;
            let digits = &after[..close];
            // DuckDB refuses `$[+1]` and `$[ 1 ]`; Rust's `parse` would
            // accept a leading `+`, so allow only an optional `-`.
            let index: i64 = match digits.strip_prefix('+') {
                Some(_) => return Err(format!("JSON path error near '{}'", near(rest))),
                None => digits
                    .parse()
                    .map_err(|_| format!("JSON path error near '{}'", near(rest)))?,
            };
            steps.push(Step::Index(index));
            rest = &after[close + 1..];
        } else if rest.starts_with("..") {
            return Err(
                "JSON path wildcards ([*], [0,1], $..a) are not supported in v1".to_string(),
            );
        } else {
            return Err(format!("JSON path error near '{}'", near(rest)));
        }
    }
    Ok(Path { steps })
}

/// The offending text of a path error: the first few characters of `rest`.
fn near(rest: &str) -> String {
    let mut out = String::new();
    for c in rest.chars().take(4) {
        out.push(c);
    }
    out
}

/// A quoted key: its value and the number of bytes consumed (including both
/// quotes). `esc` is the text after the opening quote.
fn parse_quoted_key(esc: &str) -> Result<(String, usize), String> {
    let mut out = String::new();
    let mut used = 1; // the opening quote
    let mut chars = esc.chars();
    loop {
        match chars.next() {
            None => return Err("JSON path error near '\"'".to_string()),
            Some('"') => {
                used += 1;
                return Ok((out, used));
            }
            Some('\\') => {
                used += 1;
                match chars.next() {
                    Some('"') => {
                        out.push('"');
                        used += 1;
                    }
                    Some('\\') => {
                        out.push('\\');
                        used += 1;
                    }
                    _ => return Err("JSON path error near '\\'".to_string()),
                }
            }
            Some(c) => {
                out.push(c);
                used += c.len_utf8();
            }
        }
    }
}

/// Resolve `path` in `doc`. `None` means "no match" (missing key, index out
/// of range, or a step applied to the wrong value kind) — never an error.
pub fn lookup<'a>(doc: &'a Json, path: &Path) -> Option<&'a Json> {
    let mut current = doc;
    for step in path.steps() {
        current = match (step, current) {
            (Step::Key(key), Json::Object(entries)) => {
                // First match wins on duplicates, like DuckDB.
                entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)?
            }
            (Step::Index(index), Json::Array(items)) => {
                let resolved = if *index < 0 {
                    items.len() as i64 + *index
                } else {
                    *index
                };
                if resolved < 0 {
                    return None;
                }
                items.get(resolved as usize)?
            }
            _ => return None,
        };
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse;
    use crate::value::to_string;

    fn extract(doc: &str, path: &str) -> Option<String> {
        let doc = parse(doc).unwrap();
        let path = parse_path(path).unwrap();
        lookup(&doc, &path).map(to_string)
    }

    /// Pinned against DuckDB α46057.
    #[test]
    fn lookup_matches_duckdb() {
        let cases: &[(&str, &str, Option<&str>)] = &[
            (r#"{"a":1}"#, "$.a", Some("1")),
            (r#"{"a":1,"a":2}"#, "$.a", Some("1")),
            (r#"{"a b":3}"#, r#"$."a b""#, Some("3")),
            (r#"{"a.b":1}"#, r#"$."a.b""#, Some("1")),
            (r#"{"a.b":1}"#, "$.a.b", None),
            (r#"{"a[b":1}"#, r#"$."a[b""#, Some("1")),
            (r#"{"a]b":1}"#, "$.a]b", Some("1")),
            (r#"{"a b":1}"#, "$.a b", Some("1")),
            (r#"{"a":1}"#, "$.A", None),
            (r#"{"A":1}"#, r#"$."A""#, Some("1")),
            ("[10,20,30]", "$[1]", Some("20")),
            ("[1,2,3]", "$[-1]", Some("3")),
            ("[1,2,3]", "$[-4]", None),
            ("[1,2,3]", "$[3]", None),
            (r#"{"a":[{"b":true}]}"#, "$.a[0].b", Some("true")),
            (r#"{"a":[{"b":[1,2]}]}"#, "$.a[-1].b[-2]", Some("1")),
            (r#"{"a":1}"#, "$.a.b", None),
            (r#"{"a":{"b":1}}"#, "$.a.b.c", None),
            (r#"{"a":1}"#, "$[0]", None),
            ("[1,2]", "$.a", None),
            (r#"{"a":1}"#, "$.missing", None),
            (r#"{"a":1}"#, "$", Some(r#"{"a":1}"#)),
            ("[1,2]", "$", Some("[1,2]")),
            ("1", "$", Some("1")),
            ("true", "$", Some("true")),
            ("null", "$", Some("null")),
            (r#""s""#, "$", Some(r#""s""#)),
        ];
        for (doc, path, expected) in cases {
            assert_eq!(
                extract(doc, path).as_deref(),
                *expected,
                "for {path} on {doc}"
            );
        }
    }

    #[test]
    fn path_errors_match_duckdb_shape() {
        // Errors (probed against DuckDB): a path must start with `$`, a
        // trailing dot needs a key, brackets need an integer index, an empty
        // or unterminated quoted key is refused, and `['a']` is not a thing.
        for path in [
            "a", "", "$.a.", "$[0", "$[1.5]", "$[+1]", "$[ 1 ]", "$$", "$.\"\"", "$.\"", "$['a']",
            "$[\"a\"]",
        ] {
            let err = parse_path(path).unwrap_err();
            assert!(err.contains("JSON path error"), "{path:?} -> {err}");
        }
        // Single quotes and unquoted oddities are just key characters, and a
        // quoted key may contain brackets (DuckDB accepts all of these).
        for path in ["$.'a'", "$.a'b", "$.a\"b", r#"$."a[b""#, "$.a]b", "$.a b"] {
            assert!(parse_path(path).is_ok(), "{path:?} must parse");
        }
    }

    #[test]
    fn wildcards_are_refused_with_a_clear_message() {
        for path in ["$.a[*]", "$.a[0,1]", "$..a", "$..*"] {
            let err = parse_path(path).unwrap_err();
            assert!(
                err.contains("not supported in v1") || err.contains("JSON path error"),
                "{path:?} -> {err}"
            );
        }
    }

    #[test]
    fn quoted_keys_handle_escapes_and_unicode() {
        let path = parse_path(r#"$."a\"b""#).unwrap();
        assert_eq!(path.steps(), &[Step::Key("a\"b".to_string())]);
        let path = parse_path(r#"$."a\\b""#).unwrap();
        assert_eq!(path.steps(), &[Step::Key("a\\b".to_string())]);
        let path = parse_path(r#"$."héllo😀""#).unwrap();
        assert_eq!(path.steps(), &[Step::Key("héllo😀".to_string())]);
        let doc = parse(r#"{"héllo😀":7}"#).unwrap();
        assert_eq!(lookup(&doc, &path), Some(&Json::Int(7)));
    }
}
