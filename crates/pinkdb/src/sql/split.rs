//! Byte-level scans over raw SQL text.
//!
//! The PEG grammar parses `&str` in one pass, so there is no token stream.
//! Two scans remain because their consumers need the raw source:
//! [`statement_texts`] splits a multi-statement input into per-statement
//! source slices for the WAL, and [`nul_in_quoted_identifier`] locates a NUL
//! byte inside a quoted identifier, which is rejected at parse time because
//! the C bindings hand names over as NUL-terminated strings. Both skip
//! string literals (`''` escapes), quoted identifiers (`""` escapes), `$$ … $$`
//! dollar-quoted strings, `--` line comments and `/* */` block comments
//! exactly like the grammar does.

/// Split `sql` into one source text per statement, mirroring the parser's
/// top-level `;` splitting. Returns the raw source of every non-empty
/// statement, in order, with surrounding trivia trimmed.
///
/// Call only after the input parsed successfully: malformed quotes and
/// comments (which the grammar rejects) then cannot occur, so this scan is
/// infallible.
pub(crate) fn statement_texts(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            _ if bytes[i].is_ascii_whitespace() => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'\'' | b'"' => i = skip_quoted(bytes, i, bytes[i]),
            b'$' => i = skip_dollar_quoted(bytes, i),
            b';' => {
                if let Some(s) = start.take() {
                    out.push(sql[s..i].trim_end().to_string());
                }
                i += 1;
            }
            _ => {
                if start.is_none() {
                    start = Some(i);
                }
                i += 1;
            }
        }
    }
    if let Some(s) = start {
        out.push(sql[s..].trim_end().to_string());
    }
    out
}

/// Byte offsets of every `?` placeholder in `sql`, in textual order.
///
/// Mirrors the grammar's scan rules exactly: string literals (`''` escapes),
/// quoted identifiers (`""` escapes), `--` line comments and `/* */` block
/// comments are skipped, so a `?` inside any of them is not a placeholder.
/// The parser is the authority for what a placeholder is (it alone knows a
/// `?` was parsed as a parameter); [`crate::PreparedStatement::parse`]
/// cross-checks the two, so a drift between this scan and the grammar is
/// caught at prepare time instead of splicing substitution into wrong bytes.
pub(crate) fn parameter_offsets(sql: &str) -> Vec<usize> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            _ if bytes[i].is_ascii_whitespace() => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'\'' | b'"' => i = skip_quoted(bytes, i, bytes[i]),
            b'$' => i = skip_dollar_quoted(bytes, i),
            b'?' => {
                out.push(i);
                i += 1;
            }
            _ => i += 1,
        }
    }
    out
}

/// Offset of the opening quote of the first double-quoted identifier whose
/// text contains a NUL byte, if any.
///
/// Mirrors the grammar's scan order (string literals and comments are
/// skipped, `""` is an escaped quote); `None` means no such identifier — an
/// unterminated one included, since the grammar reports that shape itself.
pub(crate) fn nul_in_quoted_identifier(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            _ if bytes[i].is_ascii_whitespace() => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'\'' => i = skip_quoted(bytes, i, b'\''),
            b'$' => i = skip_dollar_quoted(bytes, i),
            b'"' => {
                let start = i;
                i += 1;
                loop {
                    if i >= bytes.len() {
                        // Unterminated identifier: the grammar reports it.
                        return None;
                    }
                    if bytes[i] == b'"' {
                        if bytes.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    if bytes[i] == 0 {
                        return Some(start);
                    }
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// Offset just past a dollar-quoted string starting at `start` (a `$` byte):
/// `$$ text $$` (see the grammar's `dollar_string`). Only the untagged form is
/// a string; a `$` that does not open one is an ordinary byte. An unterminated
/// run simply ends at the end of input (the grammar reports that shape).
fn skip_dollar_quoted(bytes: &[u8], start: usize) -> usize {
    if bytes.get(start + 1) != Some(&b'$') {
        return start + 1;
    }
    let mut i = start + 2;
    while i < bytes.len() {
        if bytes[i] == b'$' && bytes.get(i + 1) == Some(&b'$') {
            return i + 2;
        }
        i += 1;
    }
    i
}

/// Offset just past a quoted run that started at `start` (an opening `quote`
/// already consumed); `''` / `""` is an escaped quote. An unterminated run
/// simply ends at the end of input.
fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    i
}
