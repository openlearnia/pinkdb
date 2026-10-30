//! The editor's SQL helpers: a tokenizer (highlighting), a statement
//! splitter (run the statement at the cursor), the word being typed and the
//! table references of a statement (completion). Best-effort by design: they
//! never fail, and an unterminated string or comment runs to the end.
//!
//! Positions are **byte** offsets into the text.

/// What a token is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenKind {
    Whitespace,
    Comment,
    /// `'…'`
    String,
    /// `"…"`
    QuotedIdent,
    Number,
    Keyword,
    Type,
    /// An identifier followed by `(`.
    Function,
    Ident,
    Operator,
    Punct,
}

impl TokenKind {
    /// The CSS class the editor gives it.
    pub fn class(self) -> &'static str {
        match self {
            TokenKind::Whitespace => "",
            TokenKind::Comment => "t-comment",
            TokenKind::String => "t-string",
            TokenKind::QuotedIdent => "t-qident",
            TokenKind::Number => "t-number",
            TokenKind::Keyword => "t-keyword",
            TokenKind::Type => "t-type",
            TokenKind::Function => "t-func",
            TokenKind::Ident => "t-ident",
            TokenKind::Operator => "t-op",
            TokenKind::Punct => "t-punct",
        }
    }
}

/// One token: its kind and byte span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
}

/// SQL keywords (lowercase).
pub const KEYWORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "group",
    "by",
    "order",
    "having",
    "limit",
    "offset",
    "as",
    "and",
    "or",
    "not",
    "in",
    "is",
    "null",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "outer",
    "cross",
    "on",
    "using",
    "natural",
    "semi",
    "anti",
    "asof",
    "lateral",
    "union",
    "all",
    "intersect",
    "except",
    "distinct",
    "with",
    "recursive",
    "case",
    "when",
    "then",
    "else",
    "end",
    "insert",
    "into",
    "values",
    "update",
    "set",
    "delete",
    "create",
    "table",
    "view",
    "materialized",
    "index",
    "drop",
    "alter",
    "add",
    "column",
    "rename",
    "to",
    "if",
    "exists",
    "replace",
    "database",
    "use",
    "show",
    "tables",
    "databases",
    "describe",
    "explain",
    "analyze",
    "begin",
    "commit",
    "rollback",
    "transaction",
    "primary",
    "key",
    "references",
    "unique",
    "check",
    "default",
    "constraint",
    "between",
    "like",
    "ilike",
    "similar",
    "escape",
    "asc",
    "desc",
    "nulls",
    "first",
    "last",
    "true",
    "false",
    "cast",
    "try_cast",
    "interval",
    "over",
    "partition",
    "window",
    "rows",
    "range",
    "unbounded",
    "preceding",
    "following",
    "current",
    "row",
    "filter",
    "qualify",
    "pivot",
    "unpivot",
    "macro",
    "function",
    "procedure",
    "call",
    "trigger",
    "before",
    "after",
    "for",
    "each",
    "returns",
    "return",
    "language",
    "do",
    "grant",
    "revoke",
    "role",
    "user",
    "password",
    "copy",
    "at",
    "version",
    "timestamp",
    "date",
    "time",
    "refresh",
    "checkpoint",
    "load",
    "extension",
    "install",
    "temp",
    "temporary",
    "exclude",
    "collate",
    "any",
    "some",
    "array",
    "struct",
    "map",
];

/// Type names (lowercase).
pub const TYPES: &[&str] = &[
    "integer",
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "hugeint",
    "double",
    "float",
    "real",
    "decimal",
    "numeric",
    "varchar",
    "text",
    "string",
    "boolean",
    "bool",
    "blob",
    "json",
    "variant",
    "uuid",
    "timestamptz",
];

/// Function names offered by completion (lowercase).
pub const FUNCTIONS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "any_value",
    "list",
    "string_agg",
    "median",
    "mode",
    "quantile_cont",
    "quantile_disc",
    "percentile_cont",
    "percentile_disc",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "corr",
    "approx_count_distinct",
    "bool_and",
    "bool_or",
    "arg_min",
    "arg_max",
    "abs",
    "ceil",
    "floor",
    "round",
    "trunc",
    "sqrt",
    "power",
    "exp",
    "ln",
    "log",
    "log10",
    "log2",
    "sign",
    "greatest",
    "least",
    "coalesce",
    "nullif",
    "ifnull",
    "length",
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "substr",
    "substring",
    "concat",
    "concat_ws",
    "contains",
    "starts_with",
    "ends_with",
    "strpos",
    "position",
    "lpad",
    "rpad",
    "repeat",
    "reverse",
    "split_part",
    "regexp_matches",
    "regexp_replace",
    "regexp_extract",
    "md5",
    "hash",
    "now",
    "current_date",
    "current_timestamp",
    "current_user",
    "current_roles",
    "date_trunc",
    "date_part",
    "extract",
    "date_diff",
    "date_add",
    "strftime",
    "strptime",
    "epoch",
    "make_date",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
    "json_extract",
    "json_extract_string",
    "json_object",
    "json_array",
    "to_json",
    "array_length",
    "unnest",
    "generate_series",
    "read_csv",
    "read_parquet",
    "read_json",
    "pinkdb_catalog",
    "pinkdb_column_stats",
    "lake_changes",
    "embed",
];

fn is_keyword(word: &str) -> bool {
    KEYWORDS.contains(&word)
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || !c.is_ascii()
}

fn is_ident(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == '$'
}

/// Split `text` into tokens that cover it exactly.
pub fn tokenize(text: &str) -> Vec<Token> {
    let bytes = text.as_bytes();
    let n = text.len();
    let char_at = |i: usize| text[i..].chars().next().unwrap_or('\0');
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        let start = i;
        let c = char_at(i);
        let kind = if c.is_whitespace() {
            while i < n && char_at(i).is_whitespace() {
                i += char_at(i).len_utf8();
            }
            TokenKind::Whitespace
        } else if text[i..].starts_with("--") {
            i = text[i..].find('\n').map_or(n, |off| i + off);
            TokenKind::Comment
        } else if text[i..].starts_with("/*") {
            i = text[i + 2..].find("*/").map_or(n, |off| i + 2 + off + 2);
            TokenKind::Comment
        } else if c == '\'' || c == '"' {
            let quote = bytes[i];
            i += 1;
            while i < n {
                if bytes[i] == quote {
                    if i + 1 < n && bytes[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            // A quote is ASCII, so `i` is on a char boundary or at a
            // multi-byte char's start only after this loop when n is reached.
            while i < n && !text.is_char_boundary(i) {
                i += 1;
            }
            if quote == b'\'' {
                TokenKind::String
            } else {
                TokenKind::QuotedIdent
            }
        } else if c.is_ascii_digit()
            || (c == '.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            while i < n && (bytes[i].is_ascii_digit() || bytes[i] == b'.' || bytes[i] == b'_') {
                i += 1;
            }
            if i < n
                && (bytes[i] == b'e' || bytes[i] == b'E')
                && (bytes.get(i + 1).is_some_and(u8::is_ascii_digit)
                    || (matches!(bytes.get(i + 1), Some(b'+' | b'-'))
                        && bytes.get(i + 2).is_some_and(u8::is_ascii_digit)))
            {
                i += 2;
                while i < n && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            TokenKind::Number
        } else if is_ident_start(c) {
            while i < n && is_ident(char_at(i)) {
                i += char_at(i).len_utf8();
            }
            let word = text[start..i].to_ascii_lowercase();
            let mut j = i;
            while j < n && (bytes[j] == b' ' || bytes[j] == b'\t') {
                j += 1;
            }
            if is_keyword(&word) {
                TokenKind::Keyword
            } else if TYPES.contains(&word.as_str()) {
                TokenKind::Type
            } else if bytes.get(j) == Some(&b'(') {
                TokenKind::Function
            } else {
                TokenKind::Ident
            }
        } else if "(),;.[]{}".contains(c) {
            i += 1;
            TokenKind::Punct
        } else {
            i += c.len_utf8();
            if "<>=!|:".contains(c) {
                while i < n && b"<>=|:".contains(&bytes[i]) {
                    i += 1;
                }
            }
            TokenKind::Operator
        };
        out.push(Token {
            kind,
            start,
            end: i,
        });
    }
    out
}

/// One statement of a text: its span (through its `;`) and trimmed text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Statement {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// The statements of `text`, split at top-level `;` (never inside a string,
/// quoted identifier or comment). Statements of only whitespace and
/// comments are left out.
pub fn statements(text: &str) -> Vec<Statement> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut meaningful = false;
    let flush = |start: usize, end: usize, meaningful: &mut bool, out: &mut Vec<Statement>| {
        if *meaningful {
            out.push(Statement {
                start,
                end,
                text: text[start..end].trim().to_string(),
            });
        }
        *meaningful = false;
    };
    for token in tokenize(text) {
        if token.kind == TokenKind::Punct && &text[token.start..token.end] == ";" {
            flush(start, token.end, &mut meaningful, &mut out);
            start = token.end;
        } else if !matches!(token.kind, TokenKind::Whitespace | TokenKind::Comment) {
            meaningful = true;
        }
    }
    flush(start, text.len(), &mut meaningful, &mut out);
    out
}

/// The statement the cursor (byte offset `pos`) is in: the last one that
/// begins at or before it, so a cursor right after a `;` runs the statement
/// that `;` ends. `None` when the text has no statement.
pub fn statement_at(text: &str, pos: usize) -> Option<Statement> {
    let list = statements(text);
    let mut best = list.first()?.clone();
    for statement in &list {
        let lead = text[statement.start..]
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(0);
        if pos >= statement.start + lead {
            best = statement.clone();
        }
    }
    Some(best)
}

/// The identifier being typed at byte offset `pos`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Word {
    /// What has been typed of it.
    pub prefix: String,
    /// Where it starts.
    pub start: usize,
    /// The name before a `.` (`t.co|` → `t`).
    pub qualifier: Option<String>,
}

/// The word being typed at `pos`.
pub fn word_at(text: &str, pos: usize) -> Word {
    let pos = pos.min(text.len());
    let before = &text[..pos];
    let start = before
        .char_indices()
        .rev()
        .take_while(|(_, c)| is_ident(*c))
        .last()
        .map_or(pos, |(i, _)| i);
    let prefix = text[start..pos].to_string();
    let mut qualifier = None;
    if text[..start].ends_with('.') {
        let q = start - 1;
        let head = &text[..q];
        if let Some(inner) = head.strip_suffix('"') {
            if let Some(open) = inner.rfind('"') {
                qualifier = Some(inner[open + 1..].replace("\"\"", "\""));
            }
        } else {
            let qs = head
                .char_indices()
                .rev()
                .take_while(|(_, c)| is_ident(*c))
                .last()
                .map_or(q, |(i, _)| i);
            if qs < q {
                qualifier = Some(text[qs..q].to_string());
            }
        }
    }
    Word {
        prefix,
        start,
        qualifier,
    }
}

/// A table named after `FROM` / `JOIN`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef {
    pub db: Option<String>,
    pub name: String,
    pub alias: Option<String>,
}

/// Remove an identifier's double quotes.
pub fn unquote(name: &str) -> String {
    if name.len() >= 2 && name.starts_with('"') && name.ends_with('"') {
        name[1..name.len() - 1].replace("\"\"", "\"")
    } else {
        name.to_string()
    }
}

/// The tables a statement names after `FROM` / `JOIN` (comma lists
/// included), with their aliases: what completion offers columns of.
pub fn table_refs(text: &str) -> Vec<TableRef> {
    let tokens: Vec<(TokenKind, &str)> = tokenize(text)
        .into_iter()
        .filter(|t| !matches!(t.kind, TokenKind::Whitespace | TokenKind::Comment))
        .map(|t| (t.kind, &text[t.start..t.end]))
        .collect();
    let nameish = |i: usize| {
        tokens.get(i).is_some_and(|(k, _)| {
            matches!(
                k,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Type
            )
        })
    };
    let mut refs = Vec::new();
    for (k, (kind, word)) in tokens.iter().enumerate() {
        let word = word.to_ascii_lowercase();
        if *kind != TokenKind::Keyword || (word != "from" && word != "join") {
            continue;
        }
        let mut m = k + 1;
        loop {
            let mut parts = Vec::new();
            while nameish(m) {
                parts.push(unquote(tokens[m].1));
                if tokens.get(m + 1).is_some_and(|t| t.1 == ".") && nameish(m + 2) {
                    m += 2;
                } else {
                    m += 1;
                    break;
                }
            }
            let Some(name) = parts.last().cloned() else {
                break;
            };
            let db = (parts.len() > 1).then(|| parts[0].clone());
            if tokens
                .get(m)
                .is_some_and(|(k, w)| *k == TokenKind::Keyword && w.eq_ignore_ascii_case("as"))
            {
                m += 1;
            }
            let alias = match tokens.get(m) {
                Some((TokenKind::Ident | TokenKind::QuotedIdent, w)) => {
                    m += 1;
                    Some(unquote(w))
                }
                _ => None,
            };
            refs.push(TableRef { db, name, alias });
            if tokens.get(m).is_some_and(|t| t.1 == ",") {
                m += 1;
                continue;
            }
            break;
        }
    }
    refs
}

/// Quote an identifier when it needs it (not lowercase-simple, or a
/// keyword).
pub fn quote_ident(name: &str) -> String {
    let simple = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if simple && !is_keyword(name) {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Quote a string literal.
pub fn quote_string(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(text: &str) -> Vec<TokenKind> {
        tokenize(text)
            .into_iter()
            .map(|t| t.kind)
            .filter(|k| *k != TokenKind::Whitespace)
            .collect()
    }

    #[test]
    fn tokens_cover_the_text_exactly() {
        let text = "SELECT a, 'it''s' AS s, \"we\"\"ird\" -- c\nFROM t WHERE x >= 1.5e3; /* end";
        let joined: String = tokenize(text)
            .iter()
            .map(|t| &text[t.start..t.end])
            .collect();
        assert_eq!(joined, text);
        use TokenKind as K;
        assert_eq!(
            kinds(text),
            vec![
                K::Keyword,
                K::Ident,
                K::Punct,
                K::String,
                K::Keyword,
                K::Ident,
                K::Punct,
                K::QuotedIdent,
                K::Comment,
                K::Keyword,
                K::Ident,
                K::Keyword,
                K::Ident,
                K::Operator,
                K::Number,
                K::Punct,
                K::Comment
            ]
        );
    }

    #[test]
    fn multibyte_text_is_safe() {
        let text = "SELECT 'é ✓', naïve FROM \"ünïcode\" -- ✓";
        let joined: String = tokenize(text)
            .iter()
            .map(|t| &text[t.start..t.end])
            .collect();
        assert_eq!(joined, text);
        assert_eq!(word_at("SELECT naï", "SELECT naï".len()).prefix, "naï");
    }

    #[test]
    fn functions_and_types_are_told_apart() {
        use TokenKind::*;
        assert_eq!(
            kinds("count (x)::bigint"),
            vec![Function, Punct, Ident, Punct, Operator, Type]
        );
    }

    #[test]
    fn statements_split_only_at_top_level_semicolons() {
        let text = "SELECT ';'; -- a; b\nSELECT \";\" FROM t;\n\n  ;  SELECT 3";
        let list: Vec<String> = statements(text).into_iter().map(|s| s.text).collect();
        assert_eq!(
            list,
            vec!["SELECT ';';", "-- a; b\nSELECT \";\" FROM t;", "SELECT 3"]
        );
    }

    #[test]
    fn the_statement_at_the_cursor() {
        let text = "SELECT 1;\nSELECT 2;\n";
        let at = |pos| statement_at(text, pos).map(|s| s.text);
        assert_eq!(at(3).as_deref(), Some("SELECT 1;"));
        assert_eq!(at(9).as_deref(), Some("SELECT 1;"));
        assert_eq!(at(12).as_deref(), Some("SELECT 2;"));
        assert_eq!(at(text.len()).as_deref(), Some("SELECT 2;"));
        assert_eq!(statement_at("  -- only\n", 2), None);
    }

    #[test]
    fn the_word_being_typed_and_its_qualifier() {
        assert_eq!(
            word_at("SELECT o.cu", 11),
            Word {
                prefix: "cu".into(),
                start: 9,
                qualifier: Some("o".into())
            }
        );
        assert_eq!(
            word_at("SELECT \"my t\".x", 15).qualifier.as_deref(),
            Some("my t")
        );
        assert_eq!(
            word_at("SELECT ", 7),
            Word {
                prefix: String::new(),
                start: 7,
                qualifier: None
            }
        );
    }

    #[test]
    fn table_references_with_aliases() {
        let refs = table_refs("SELECT * FROM main.orders o JOIN customer AS c ON o.k = c.k");
        assert_eq!(
            refs,
            vec![
                TableRef {
                    db: Some("main".into()),
                    name: "orders".into(),
                    alias: Some("o".into())
                },
                TableRef {
                    db: None,
                    name: "customer".into(),
                    alias: Some("c".into())
                },
            ]
        );
        let refs = table_refs("FROM a, b x");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[1].alias.as_deref(), Some("x"));
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("orders"), "orders");
        assert_eq!(quote_ident("Order"), "\"Order\"");
        assert_eq!(quote_ident("select"), "\"select\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_string("it's"), "'it''s'");
    }
}
