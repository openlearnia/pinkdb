//! SQL lexer: raw text → token stream.
//!
//! Single quotes = string literals (`''` = escaped quote).
//! Double quotes = quoted identifiers (`""` = escaped quote); a NUL byte
//! inside a quoted identifier is a parse error (see `lex_quoted`), while
//! string literals remain free to carry NUL bytes.
//! `--` line comments and `/* */` block comments are skipped.

use crate::types::{PinkDbError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select,
    Distinct,
    From,
    Where,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Limit,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Full,
    Cross,
    On,
    As,
    And,
    Or,
    Not,
    Null,
    Is,
    Between,
    Create,
    Table,
    Insert,
    Into,
    Values,
    True,
    False,
    With,
    In,
    Exists,
    Date,
    Timestamp,
    Over,
    Partition,
    Rows,
    Row,
    Range,
    Unbounded,
    Preceding,
    Following,
    Current,
    Update,
    Delete,
    Set,
    Begin,
    Commit,
    Rollback,
    Transaction,
    Union,
    View,
    Drop,
    Cast,
    Like,
    Case,
    When,
    Then,
    Else,
    End,
    Use,
    Database,
    Load,
    Extension,
}

impl Keyword {
    pub fn lookup(s: &str) -> Option<Keyword> {
        Some(match s.to_ascii_uppercase().as_str() {
            "SELECT" => Keyword::Select,
            "DISTINCT" => Keyword::Distinct,
            "FROM" => Keyword::From,
            "WHERE" => Keyword::Where,
            "GROUP" => Keyword::Group,
            "BY" => Keyword::By,
            "HAVING" => Keyword::Having,
            "ORDER" => Keyword::Order,
            "ASC" => Keyword::Asc,
            "DESC" => Keyword::Desc,
            "LIMIT" => Keyword::Limit,
            "JOIN" => Keyword::Join,
            "INNER" => Keyword::Inner,
            "LEFT" => Keyword::Left,
            "RIGHT" => Keyword::Right,
            "OUTER" => Keyword::Outer,
            "FULL" => Keyword::Full,
            "CROSS" => Keyword::Cross,
            "ON" => Keyword::On,
            "AS" => Keyword::As,
            "AND" => Keyword::And,
            "OR" => Keyword::Or,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "IS" => Keyword::Is,
            "BETWEEN" => Keyword::Between,
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "WITH" => Keyword::With,
            "IN" => Keyword::In,
            "EXISTS" => Keyword::Exists,
            "DATE" => Keyword::Date,
            "TIMESTAMP" => Keyword::Timestamp,
            "OVER" => Keyword::Over,
            "PARTITION" => Keyword::Partition,
            "ROWS" => Keyword::Rows,
            "ROW" => Keyword::Row,
            "RANGE" => Keyword::Range,
            "UNBOUNDED" => Keyword::Unbounded,
            "PRECEDING" => Keyword::Preceding,
            "FOLLOWING" => Keyword::Following,
            "CURRENT" => Keyword::Current,
            "UPDATE" => Keyword::Update,
            "DELETE" => Keyword::Delete,
            "SET" => Keyword::Set,
            "BEGIN" => Keyword::Begin,
            "COMMIT" => Keyword::Commit,
            "ROLLBACK" => Keyword::Rollback,
            "TRANSACTION" => Keyword::Transaction,
            "UNION" => Keyword::Union,
            "VIEW" => Keyword::View,
            "DROP" => Keyword::Drop,
            "CAST" => Keyword::Cast,
            "LIKE" => Keyword::Like,
            "CASE" => Keyword::Case,
            "WHEN" => Keyword::When,
            "THEN" => Keyword::Then,
            "ELSE" => Keyword::Else,
            "END" => Keyword::End,
            "USE" => Keyword::Use,
            "DATABASE" => Keyword::Database,
            "LOAD" => Keyword::Load,
            "EXTENSION" => Keyword::Extension,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Eof,
    Semicolon,
    Comma,
    Dot,
    Star,
    LParen,
    RParen,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    String(String),
    Integer(i64),
    Float(f64),
    Ident(String),
    Keyword(Keyword),
}

/// Lex `input` into `(token, byte_offset)` pairs, ending with one `Eof`.
pub fn lex(input: &str) -> Result<Vec<(Token, usize)>> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;

    let err = |pos: usize, msg: &str| {
        PinkDbError::Parse(format!("lex error at offset {pos}: {msg}"))
    };

    while i < bytes.len() {
        let c = bytes[i];
        // Whitespace.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Line comment `-- ...`.
        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment `/* ... */`.
        if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            let start = i;
            i += 2;
            let mut closed = false;
            while i + 1 < bytes.len() {
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    closed = true;
                    break;
                }
                i += 1;
            }
            if !closed {
                return Err(err(start, "unterminated block comment"));
            }
            continue;
        }
        let pos = i;
        match c {
            b';' => {
                out.push((Token::Semicolon, pos));
                i += 1;
            }
            b',' => {
                out.push((Token::Comma, pos));
                i += 1;
            }
            b'.' => {
                // Leading-dot float like `.5`? Only if followed by a digit.
                if bytes.get(i + 1).map(|d| d.is_ascii_digit()).unwrap_or(false) {
                    i = lex_number(bytes, i, &mut out)?;
                } else {
                    out.push((Token::Dot, pos));
                    i += 1;
                }
            }
            b'*' => {
                out.push((Token::Star, pos));
                i += 1;
            }
            b'(' => {
                out.push((Token::LParen, pos));
                i += 1;
            }
            b')' => {
                out.push((Token::RParen, pos));
                i += 1;
            }
            b'+' => {
                out.push((Token::Plus, pos));
                i += 1;
            }
            b'-' => {
                out.push((Token::Minus, pos));
                i += 1;
            }
            b'/' => {
                out.push((Token::Slash, pos));
                i += 1;
            }
            b'%' => {
                out.push((Token::Percent, pos));
                i += 1;
            }
            b'=' => {
                out.push((Token::Eq, pos));
                i += 1;
            }
            b'<' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    out.push((Token::LtEq, pos));
                    i += 2;
                } else if bytes.get(i + 1) == Some(&b'>') {
                    out.push((Token::NotEq, pos));
                    i += 2;
                } else {
                    out.push((Token::Lt, pos));
                    i += 1;
                }
            }
            b'>' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    out.push((Token::GtEq, pos));
                    i += 2;
                } else {
                    out.push((Token::Gt, pos));
                    i += 1;
                }
            }
            b'!' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    out.push((Token::NotEq, pos));
                    i += 2;
                } else {
                    return Err(err(pos, "unexpected `!` (did you mean `!=`?)"));
                }
            }
            b'\'' => {
                let (s, next) = lex_quoted(input, i, b'\'')?;
                out.push((Token::String(s), pos));
                i = next;
            }
            b'"' => {
                let (s, next) = lex_quoted(input, i, b'"')?;
                out.push((Token::Ident(s), pos));
                i = next;
            }
            b'0'..=b'9' => {
                i = lex_number(bytes, i, &mut out)?;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                let mut j = i + 1;
                while j < bytes.len()
                    && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_')
                {
                    j += 1;
                }
                let word = &input[i..j];
                match Keyword::lookup(word) {
                    Some(kw) => out.push((Token::Keyword(kw), pos)),
                    None => out.push((Token::Ident(word.to_string()), pos)),
                }
                i = j;
            }
            _ => return Err(err(pos, &format!("unexpected character `{}`", c as char))),
        }
    }
    out.push((Token::Eof, input.len()));
    Ok(out)
}

/// Lex a number starting at `i` (digit or leading `.`); pushes the token.
/// Returns the next offset.
fn lex_number(
    bytes: &[u8],
    start: usize,
    out: &mut Vec<(Token, usize)>,
) -> Result<usize> {
    let mut j = start;
    while j < bytes.len() && bytes[j].is_ascii_digit() {
        j += 1;
    }
    let mut is_float = false;
    // Fractional part: `.` must be followed by a digit (`t.2` would lex `t`, `.`, `2`
    // — but `1.` followed by non-digit stays an integer then a dot; acceptable v1).
    if bytes.get(j) == Some(&b'.')
        && bytes.get(j + 1).map(|d| d.is_ascii_digit()).unwrap_or(false)
    {
        is_float = true;
        j += 1;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
    }
    // Exponent.
    if bytes.get(j).map(|c| *c == b'e' || *c == b'E').unwrap_or(false) {
        let mut k = j + 1;
        if bytes.get(k).map(|c| *c == b'+' || *c == b'-').unwrap_or(false) {
            k += 1;
        }
        if bytes.get(k).map(|d| d.is_ascii_digit()).unwrap_or(false) {
            is_float = true;
            k += 1;
            while k < bytes.len() && bytes[k].is_ascii_digit() {
                k += 1;
            }
            j = k;
        }
    }
    let text = std::str::from_utf8(&bytes[start..j]).map_err(|_| {
        PinkDbError::Parse(format!("lex error at offset {start}: bad number"))
    })?;
    if is_float {
        text.parse::<f64>().map(|x| out.push((Token::Float(x), start))).map_err(|_| {
            PinkDbError::Parse(format!("lex error at offset {start}: bad float `{text}`"))
        })?;
    } else {
        text.parse::<i64>().map(|n| out.push((Token::Integer(n), start))).map_err(|_| {
            PinkDbError::Parse(format!(
                "lex error at offset {start}: integer literal out of range `{text}`"
            ))
        })?;
    }
    Ok(j)
}

/// Lex a quoted string/identifier starting at the opening quote `quote`.
/// `''` inside a string (or `""` inside an identifier) is an escaped quote.
fn lex_quoted(input: &str, start: usize, quote: u8) -> Result<(String, usize)> {
    let bytes = input.as_bytes();
    let mut s = String::new();
    let mut i = start + 1;
    loop {
        if i >= bytes.len() {
            return Err(PinkDbError::Parse(format!(
                "lex error at offset {start}: unterminated quoted literal"
            )));
        }
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                s.push(quote as char);
                i += 2;
                continue;
            }
            return Ok((s, i + 1));
        }
        // Copy one UTF-8 char (strings may hold any text).
        let ch = input[i..].chars().next().expect("char boundary");
        // Quoted identifiers cross into C bindings as NUL-terminated strings
        // in several places; a NUL inside one would be silently truncated
        // there, so it is rejected at parse time. String literals may hold
        // NULs (they are byte data, not names).
        if ch == '\0' && quote == b'"' {
            return Err(PinkDbError::Parse(format!(
                "lex error at offset {start}: identifier contains a NUL byte"
            )));
        }
        s.push(ch);
        i += ch.len_utf8();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<Token> {
        lex(input).unwrap().into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn keywords_are_case_insensitive() {
        assert_eq!(
            kinds("select DISTINCT from t WHERE"),
            vec![
                Token::Keyword(Keyword::Select),
                Token::Keyword(Keyword::Distinct),
                Token::Keyword(Keyword::From),
                Token::Ident("t".to_string()),
                Token::Keyword(Keyword::Where),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn literals_and_operators() {
        assert_eq!(
            kinds("a <> 1 != 2.5 'it''s' 3e2"),
            vec![
                Token::Ident("a".to_string()),
                Token::NotEq,
                Token::Integer(1),
                Token::NotEq,
                Token::Float(2.5),
                Token::String("it's".to_string()),
                Token::Float(300.0),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn comments_and_quoted_idents() {
        assert_eq!(
            kinds("-- hi\n\"my col\" /* x */ 1"),
            vec![
                Token::Ident("my col".to_string()),
                Token::Integer(1),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn bad_chars_error() {
        assert!(lex("select `x`").is_err());
        assert!(lex("select 'abc").is_err());
    }

    #[test]
    fn nul_in_quoted_identifier_is_rejected_but_allowed_in_strings() {
        let e = lex("SELECT \"a\0b\"").unwrap_err().to_string();
        assert!(e.contains("identifier contains a NUL byte"), "{e}");
        // A NUL inside a *string literal* is value bytes, not a name: allowed.
        assert_eq!(
            kinds("SELECT 'a\0b'"),
            vec![
                Token::Keyword(Keyword::Select),
                Token::String("a\0b".to_string()),
                Token::Eof,
            ]
        );
    }
}
