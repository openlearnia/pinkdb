//! The JSON parser: DuckDB/yyjson-compatible, including its leniencies.
//!
//! Strict RFC 8259 syntax, plus the extensions the reference parser
//! (yyjson, as configured by DuckDB α46057) accepts — probed case by case:
//!
//! * **trailing commas** in arrays and objects (`[1,]`, `{"a":1,}`);
//! * **`NaN` / `Infinity` / `inf`** literals, case-insensitively and with a
//!   leading `-` (`nan`, `NAN`, `-Infinity`, …), kept as raw text;
//! * numbers are *not* grammar-relaxed: `01`, `.5`, `1.`, `+1` are invalid,
//!   like DuckDB;
//! * raw control characters inside strings are invalid
//!   (`"a<TAB>b"`), like DuckDB; invalid escapes are invalid.
//!
//! * **lone surrogates** (`"\uD800"`, `"a\uDC00"`) are invalid, exactly like
//!   DuckDB: a high surrogate must be followed by a low one and a lone low
//!   surrogate is never valid, so `json_valid` is false and the extracting
//!   functions error — the two never disagree about a document.

use crate::value::Json;

/// Nesting limit: deeper input is rejected instead of risking stack growth in
/// the recursive parser (DuckDB's parser is iterative and effectively
/// unbounded; documents past this depth are far outside the v1 envelope).
pub const MAX_DEPTH: usize = 1000;

/// Parse one JSON document (trailing whitespace allowed, trailing garbage is
/// not). The error message names the byte offset.
pub fn parse(input: &str) -> Result<Json, String> {
    let mut p = Parser { input, pos: 0 };
    p.skip_ws();
    if p.peek().is_none() {
        return Err("malformed JSON at byte 0: input is empty".to_string());
    }
    let value = p.value(0)?;
    p.skip_ws();
    if p.peek().is_some() {
        return p.err("unexpected character after the document");
    }
    Ok(value)
}

/// Whether `input` is a valid document — exactly `json_valid`'s question.
pub fn is_valid(input: &str) -> bool {
    parse(input).is_ok()
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn peek_byte(&self, offset: usize) -> Option<u8> {
        self.input.as_bytes().get(self.pos + offset).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_ws(&mut self) {
        while let Some(c @ (' ' | '\t' | '\n' | '\r')) = self.peek() {
            self.pos += c.len_utf8();
        }
    }

    fn err<T>(&self, what: impl std::fmt::Display) -> Result<T, String> {
        Err(format!("malformed JSON at byte {}: {what}", self.pos))
    }

    fn expect(&mut self, c: char) -> Result<(), String> {
        if self.peek() == Some(c) {
            self.bump();
            Ok(())
        } else {
            self.err(format!("expected '{c}'"))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, String> {
        if depth >= MAX_DEPTH {
            return self.err(format!("nesting exceeds {MAX_DEPTH} levels"));
        }
        match self.peek() {
            Some('{') => self.object(depth),
            Some('[') => self.array(depth),
            Some('"') => Ok(Json::Str(self.string()?)),
            Some('-') | Some('0'..='9') => self.number(),
            Some(c) if c.is_ascii_alphabetic() => self.literal(),
            _ => self.err("unexpected character"),
        }
    }

    /// `true` / `false` / `null`, plus the lenient `NaN`/`Infinity`/`inf`
    /// number literals the reference parser accepts.
    fn literal(&mut self) -> Result<Json, String> {
        let start = self.pos;
        while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
            self.bump();
        }
        let word = &self.input[start..self.pos];
        match word {
            "true" => Ok(Json::Bool(true)),
            "false" => Ok(Json::Bool(false)),
            "null" => Ok(Json::Null),
            _ if is_nan_inf(word) => Ok(Json::Raw(word.to_string())),
            _ => self.err(format!("invalid literal \"{word}\"")),
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, String> {
        self.expect('[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.bump();
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value(depth + 1)?);
            self.skip_ws();
            match self.peek() {
                // A trailing comma is accepted (`[1,]`), a leading or doubled
                // one is not (`[,]`, `[1,,]`).
                Some(',') => {
                    self.bump();
                    self.skip_ws();
                    if self.peek() == Some(']') {
                        self.bump();
                        return Ok(Json::Array(items));
                    }
                }
                Some(']') => {
                    self.bump();
                    return Ok(Json::Array(items));
                }
                _ => return self.err("expected ',' or ']'"),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, String> {
        self.expect('{')?;
        let mut entries = Vec::new();
        self.skip_ws();
        if self.peek() == Some('}') {
            self.bump();
            return Ok(Json::Object(entries));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some('"') {
                return self.err("expected a string key");
            }
            let key = self.string()?;
            self.skip_ws();
            self.expect(':')?;
            self.skip_ws();
            let value = self.value(depth + 1)?;
            // Source order and duplicates are kept: `{"a":1,"a":2}` is stored
            // twice and a lookup returns the first match, like DuckDB.
            entries.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.bump();
                    self.skip_ws();
                    if self.peek() == Some('}') {
                        self.bump();
                        return Ok(Json::Object(entries));
                    }
                }
                Some('}') => {
                    self.bump();
                    return Ok(Json::Object(entries));
                }
                _ => return self.err("expected ',' or '}'"),
            }
        }
    }

    /// A quoted string, decoded (escapes, surrogate pairs).
    fn string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut out = String::new();
        let mut raw_start = self.pos;
        loop {
            match self.peek() {
                None => return self.err("unterminated string"),
                Some('"') => {
                    out.push_str(&self.input[raw_start..self.pos]);
                    self.bump();
                    return Ok(out);
                }
                Some('\\') => {
                    out.push_str(&self.input[raw_start..self.pos]);
                    self.bump();
                    let Some(escape) = self.bump() else {
                        return self.err("unterminated escape");
                    };
                    match escape {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => self.unicode_escape(&mut out)?,
                        other => return self.err(format!("invalid escape '\\{other}'")),
                    }
                    raw_start = self.pos;
                }
                Some(c) if (c as u32) < 0x20 => {
                    return self.err("unexpected control character in string");
                }
                Some(_) => {
                    self.bump();
                }
            }
        }
    }

    /// The `XXXX` of a `\uXXXX` escape, already past the `u`; a high+low
    /// surrogate pair is combined, a lone surrogate is an error (like
    /// DuckDB's parser).
    fn unicode_escape(&mut self, out: &mut String) -> Result<(), String> {
        let code = self.hex4()?;
        match code {
            0xD800..=0xDBFF => {
                // A high surrogate must be followed by a low one.
                if self.peek() == Some('\\') && self.peek_byte(1) == Some(b'u') {
                    self.bump();
                    self.bump();
                    let low = self.hex4()?;
                    if (0xDC00..=0xDFFF).contains(&low) {
                        let combined = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                        out.push(char::from_u32(combined).expect("a decoded surrogate pair"));
                        return Ok(());
                    }
                }
                self.err("no low surrogate in string")
            }
            0xDC00..=0xDFFF => self.err("unexpected low surrogate in string"),
            _ => {
                out.push(char::from_u32(code).expect("a non-surrogate scalar"));
                Ok(())
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let mut code = 0u32;
        for _ in 0..4 {
            let Some(c) = self.peek() else {
                return self.err("truncated \\u escape");
            };
            let Some(digit) = c.to_digit(16) else {
                return self.err("invalid \\u escape");
            };
            self.bump();
            code = code * 16 + digit;
        }
        Ok(code)
    }

    /// A number. Integers that fit `i64` (then `u64`) are canonicalized;
    /// dot/exponent forms are parsed as `f64`; anything wider — and the
    /// `NaN`/`Infinity` words after a `-` — is kept verbatim.
    fn number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.bump();
            if let Some(c) = self.peek()
                && c.is_ascii_alphabetic()
            {
                let word_start = self.pos;
                while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
                    self.bump();
                }
                let word = &self.input[word_start..self.pos];
                if is_nan_inf(word) {
                    return Ok(Json::Raw(format!("-{word}")));
                }
                return self.err(format!("invalid literal \"-{word}\""));
            }
        }
        // Integer part: `0`, or a nonzero digit followed by digits.
        match self.peek() {
            Some('0') => {
                self.bump();
            }
            Some(c) if c.is_ascii_digit() => {
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    self.bump();
                }
            }
            _ => return self.err("invalid number"),
        }
        let mut is_double = false;
        if self.peek() == Some('.') {
            is_double = true;
            self.bump();
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return self.err("invalid number");
            }
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.bump();
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            is_double = true;
            self.bump();
            if matches!(self.peek(), Some('+' | '-')) {
                self.bump();
            }
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return self.err("invalid number");
            }
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.bump();
            }
        }
        let text = &self.input[start..self.pos];
        if is_double {
            let value: f64 = text
                .parse()
                .map_err(|_| format!("malformed JSON at byte {start}: invalid number"))?;
            // An exponent that overflows f64 (`1e400`) stays verbatim, like
            // DuckDB; an underflow (`1e-400`) is a real 0.0.
            return Ok(if value.is_finite() {
                Json::Float(value)
            } else {
                Json::Raw(text.to_string())
            });
        }
        if let Ok(i) = text.parse::<i64>() {
            Ok(Json::Int(i))
        } else if let Ok(u) = text.parse::<u64>() {
            Ok(Json::UInt(u))
        } else {
            Ok(Json::Raw(text.to_string()))
        }
    }
}

/// Case-insensitive `nan` / `inf` / `infinity`.
fn is_nan_inf(word: &str) -> bool {
    word.eq_ignore_ascii_case("nan")
        || word.eq_ignore_ascii_case("inf")
        || word.eq_ignore_ascii_case("infinity")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid(input: &str) -> bool {
        is_valid(input)
    }

    /// Pinned against DuckDB α46057 (`SELECT json_valid('<input>')`).
    #[test]
    fn validity_matches_duckdb() {
        let valid_cases = [
            r#"{"a":1}"#,
            "  {\"a\":1}  ",
            "{\"a\":1,}",
            "[1,2]",
            "1e400",
            "1e-400",
            "[nan]",
            "nan",
            "NaN",
            "nAn",
            "Infinity",
            "-Infinity",
            "inf",
            "INF",
            "-nan",
            "0",
            "-0",
            "0.5",
            "1e5",
            "[true,null]",
            r#""a\u0041b""#,
            r#""a\\uD800""#,
            "{}",
            "[]",
            "{\"a\":1}\n",
        ];
        for input in valid_cases {
            assert!(valid(input), "{input:?} must be valid");
        }
        let invalid_cases = [
            "{",
            "",
            " ",
            "{\"a\":1,",
            "{\"a\":1,,",
            "[,1]",
            "[1,,2]",
            "01",
            ".5",
            "1.",
            "+1",
            "0x10",
            "[]extra",
            "[1 2]",
            "tru",
            "truE",
            "nul",
            "truex",
            "infi",
            "nanx",
            "[infi]",
            "[+inf]",
            "[nanx]",
            "{\"a\" 1}",
            "\"abc",
            "\"a\\x41b\"",
            "\"a\\u12\"",
            "\"a\\uZZZZ\"",
            "\"a\tb\"",
            "{\"a\":}",
            "{- 1}",
            "- 1",
            "{\"a\":1} {\"b\":2}",
        ];
        for input in invalid_cases {
            assert!(!valid(input), "{input:?} must be invalid");
        }
    }

    #[test]
    fn numbers_are_canonicalized_like_duckdb() {
        let cases: &[(&str, &str)] = &[
            ("1.50", "1.5"),
            ("1e2", "100.0"),
            ("1E+3", "1000.0"),
            ("-0", "0"),
            ("0.0", "0.0"),
            ("-0.0", "-0.0"),
            ("1e400", "1e400"),
            ("1e309", "1e309"),
            ("-1e400", "-1e400"),
            ("1e-400", "0.0"),
            ("-1e-400", "-0.0"),
            ("18446744073709551615", "18446744073709551615"),
            ("18446744073709551616", "18446744073709551616"),
            (
                "-123456789012345678901234567890",
                "-123456789012345678901234567890",
            ),
            ("9223372036854775807", "9223372036854775807"),
            ("-9223372036854775808", "-9223372036854775808"),
            ("12345678901234567890", "12345678901234567890"),
            ("123456789012345678901234567890.5", "1.2345678901234568e29"),
            ("12345678901234567890e5", "1.2345678901234568e24"),
        ];
        for (input, expected) in cases {
            let parsed = parse(input).expect(input);
            assert_eq!(&crate::value::to_string(&parsed), expected, "for {input}");
        }
        // `12345678901234567890e5` is 1.2345678901234568e24; assert through a
        // round-trip rather than a hand-typed literal.
        let big = parse("12345678901234567890e5").unwrap();
        assert_eq!(big, Json::Float(1.2345678901234568e24));
        // The lenient words stay verbatim, case included.
        for word in [
            "nan",
            "NaN",
            "nAn",
            "inf",
            "INF",
            "Infinity",
            "-inf",
            "-Infinity",
        ] {
            assert_eq!(crate::value::to_string(&parse(word).unwrap()), word);
        }
    }

    #[test]
    fn objects_keep_order_duplicates_and_escapes() {
        let doc = parse(r#"{"a":1,"b":{"c":"x"},"a":2}"#).unwrap();
        assert_eq!(
            doc,
            Json::Object(vec![
                ("a".to_string(), Json::Int(1)),
                (
                    "b".to_string(),
                    Json::Object(vec![("c".to_string(), Json::Str("x".to_string()))])
                ),
                ("a".to_string(), Json::Int(2)),
            ])
        );
        // Escapes and surrogate pairs decode; output re-escapes them.
        let s = parse(r#""he said \"hi\" \u0041 \uD83D\uDE00 \n""#).unwrap();
        assert_eq!(
            crate::value::to_string(&s),
            "\"he said \\\"hi\\\" A \u{1F600} \\n\""
        );
    }

    #[test]
    fn whitespace_and_trailing_commas() {
        assert!(parse(" \t\n\r[ 1 , 2 , 3 , ] \t").is_ok());
        assert!(parse(" { \"a\" : 1 , } ").is_ok());
        assert!(parse("[,]").is_err());
        assert!(parse("[1,,2]").is_err());
        assert!(parse("{\"a\":1,,\"b\":2}").is_err());
        assert!(parse("{\"a\":1 \"b\":2}").is_err());
    }

    #[test]
    fn lone_surrogates_are_refused_like_duckdb() {
        for input in [
            r#""\uD800""#,
            r#""a\uD800""#,
            r#""a\uD800b""#,
            r#""\uDC00""#,
            r#""a\uDC00""#,
            r#""\uD800x""#,
        ] {
            let err = parse(input).unwrap_err();
            assert!(err.contains("surrogate"), "{input}: {err}");
            assert!(!is_valid(input), "{input} must be invalid");
        }
        // A proper pair decodes.
        let pair = parse(r#""\uD83D\uDE00""#).unwrap();
        assert_eq!(crate::value::to_string(&pair), "\"\u{1F600}\"");
    }

    #[test]
    fn depth_is_bounded() {
        let deep = format!("{}{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(parse(&deep).is_ok());
        let deeper = format!("{}{}", "[".repeat(MAX_DEPTH + 1), "]".repeat(MAX_DEPTH + 1));
        let err = parse(&deeper).unwrap_err();
        assert!(err.contains("nesting exceeds"), "{err}");
    }

    #[test]
    fn errors_name_the_byte_offset() {
        let err = parse("{\"a\": 1,").unwrap_err();
        assert!(err.starts_with("malformed JSON at byte "), "{err}");
        let err = parse("[1, x]").unwrap_err();
        assert!(err.contains("invalid literal"), "{err}");
        let err = parse("").unwrap_err();
        assert!(err.contains("input is empty"), "{err}");
    }
}
