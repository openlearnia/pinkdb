//! Stored procedures (wave 44): the SQL procedural language and the `CALL`
//! executor.
//!
//! ```sql
//! CREATE [OR REPLACE] PROCEDURE p(a INTEGER, b VARCHAR) AS $$
//! BEGIN
//!   DECLARE n INTEGER DEFAULT 0;
//!   IF a > 0 THEN
//!     SET n = a * 2;
//!   ELSIF a = 0 THEN
//!     SET n = 1;
//!   ELSE
//!     SET n = -a;
//!   END IF;
//!   WHILE n > 0 DO
//!     INSERT INTO t VALUES (n, b);
//!     SET n = n - 1;
//!   END WHILE;
//!   RETURN n;
//! END
//! $$;
//! CALL p(3, 'x');
//! ```
//!
//! Procedures live in the per-database catalog next to tables and views, are
//! persisted in `.pink` (flags bit 4, `FLAG_PROCEDURES`) and replay through
//! the ordinary WAL path (`CREATE`/`DROP PROCEDURE` are logged statements).
//!
//! # The language
//!
//! A body is one `BEGIN ... END` block; its statements are separated by `;`.
//! The body itself is written as a string literal — preferably dollar-quoted
//! (`$$ ... $$`, Postgres-style; the tagged `$tag$ ... $tag$` form is not part
//! of this SQL surface), so the inner quotes and semicolons need no escaping.
//! A single-quoted body works too (double every inner quote).
//!
//! * `DECLARE name TYPE [DEFAULT expr]` — a typed local variable, visible from
//!   its declaration onward in its block and in nested blocks. Its value starts
//!   NULL without `DEFAULT`. A declaration inside an `IF`/`WHILE` branch or a
//!   nested `BEGIN` block belongs to that branch/block.
//! * `SET name = expr` (also `SET name := expr`, or a bare `name := expr`) —
//!   assign; the value is coerced to the declared type with the engine's
//!   INSERT coercion, an impossible conversion is a clear error.
//! * `IF ... THEN ... [ELSIF ... THEN ...]* [ELSE ...] END IF` — the condition
//!   must be BOOLEAN; NULL counts as false.
//! * `WHILE ... DO ... END WHILE` — likewise.
//! * `BEGIN ... END` — a nested block with its own variable scope.
//! * `RETURN [expr]` — end the call; `RETURN expr` makes the expression the
//!   call's result (a one-row `Selected` named `return`), `RETURN` alone keeps
//!   the last statement's result.
//! * `RAISE expr` — abort the call with the (VARCHAR, non-NULL) message.
//! * any ordinary SQL statement (DML, DDL on tables, `SELECT`, `SHOW`,
//!   `DESCRIBE`, `EXPLAIN`, and nested `CALL`s).
//!
//! Store/process-level statements and transaction control are **refused in a
//! body at `CREATE` time**: `USE`, `BEGIN`/`COMMIT`/`ROLLBACK` (in a body
//! `BEGIN` starts a block), `CHECKPOINT`, `LOAD EXTENSION`, `CREATE`/`DROP
//! DATABASE`, `CREATE`/`DROP`/`REFRESH MATERIALIZED VIEW`, and
//! `CREATE`/`DROP PROCEDURE`. Nested `CALL`s are allowed and bounded
//! ([`MAX_CALL_DEPTH`]).
//!
//! # Names and values
//!
//! Variables (parameters included) **shadow table columns**: inside an
//! expression a bare name that matches a variable in scope is that variable;
//! qualify a column (`t.x`) to reach the column. Variables are interpolated
//! as **bound values, never as SQL text**: at each statement's bind the
//! variable's current value is handed to the binder as a literal, so no input
//! can ever change the shape of the statement (injection-safe by
//! construction).
//!
//! Each body statement **rebinds** against the current catalog every time the
//! call runs, so a procedure may create a table and then insert into it, and
//! a call always sees the current schema.
//!
//! # Transactions and atomicity
//!
//! A top-level `CALL` is **statement-atomic**: the store is snapshotted
//! before the body runs and restored if the body fails, so a failing call
//! leaves no trace (data or catalog) behind — not even a table it created.
//! That holds inside an open transaction too: a failing call is undone *by
//! itself*, while the surrounding transaction stays open with its earlier
//! statements, its buffered WAL text and its optimistic base intact (a
//! statement-level rule, like a failed `INSERT` — not a savepoint). `CALL` is
//! never a transaction boundary itself: it joins the caller's transaction and
//! its statements are the transaction's statements.
//!
//! # Determinism and the WAL
//!
//! A durable store logs the committed `CALL <name>(<args>)` **statement text**
//! (one record, exactly like any other statement); replay re-executes the
//! call against the same state. That is sound because this engine has no
//! nondeterministic built-in function (no clock, no randomness), and the
//! volatile constructs that could exist are refused while the call runs:
//!
//! * a statement whose expressions reference a **loadable-extension function**
//!   is refused (a callback is opaque: it may be impure, non-deterministic or
//!   fail);
//! * a statement that **reads an attached lake table** is refused (attached
//!   tables are session-local and never replay);
//! * a statement that reads a **session-local view** is refused on a durable
//!   store (views are never persisted, so the logged call could not replay);
//! * the call's own arguments are bound as constant expressions, and a
//!   dynamic scalar there is refused by the same rule.
//!
//! The call's **touched-object set** (every table its statements actually
//! read or wrote, plus the tables its own arguments read) is reported to the
//! WAL frame and to the transaction's conflict set — the set cannot be
//! approximated before execution (loop conditions are data-dependent), so the
//! executor records what really ran.

use crate::QueryResult;
use crate::plan::LogicalPlan;
use crate::sql::ast::{Expr, Projection, Statement};
use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value};

/// Maximum `CALL` nesting depth. A procedure may call itself (directly or
/// through another procedure) up to this depth; the next call is a clean
/// error. The bound keeps a runaway recursion a reported failure instead of a
/// stack overflow.
pub(crate) const MAX_CALL_DEPTH: usize = 32;

/// Maximum `BEGIN`/`IF`/`WHILE` nesting inside one body. The cap protects the
/// recursive body parser and executor (like the AST depth guard protects the
/// statement parser) and sits far below the depth a 2 MiB test thread can
/// handle.
pub(crate) const MAX_BODY_NESTING: usize = 16;

/// A procedure's implementation language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcLanguage {
    /// The SQL procedural language documented in this module.
    Sql,
    /// JavaScript, evaluated in the deterministic sandbox of the optional
    /// `pinkdb-js` crate (`js` cargo feature).
    JavaScript,
}

/// Refuse a definition whose language this build cannot run, **before**
/// anything is stored: `LANGUAGE JAVASCRIPT` is a clear build-level error,
/// never a definition that could only fail later.
pub(crate) fn check_language_available(language: ProcLanguage) -> Result<()> {
    match language {
        ProcLanguage::Sql => Ok(()),
        ProcLanguage::JavaScript => {
            #[cfg(feature = "js")]
            if crate::js::engine_available().is_ok() {
                return Ok(());
            }
            Err(PinkDbError::Execution(format!(
                "JavaScript procedures are not available in this build: {}",
                js_unavailable_reason()
            )))
        }
    }
}

/// Why a JavaScript body cannot run here, in one sentence: the missing cargo
/// feature, or the missing engine binding inside `pinkdb-js` (the recorded
/// follow-up). Shared by the `CREATE`-time refusal and the `CALL`-time one, so
/// a definition that arrived in a `.pink` file reports the same reason.
fn js_unavailable_reason() -> String {
    #[cfg(not(feature = "js"))]
    {
        "the pinkdb-js sandbox is the optional `js` cargo feature — rebuild with \
         `--features js` (the SQL procedural language needs no feature)"
            .to_string()
    }
    #[cfg(feature = "js")]
    {
        crate::js::engine_available()
            .err()
            .unwrap_or_else(|| "the JavaScript engine is unavailable".to_string())
    }
}

impl ProcLanguage {
    /// The language tag persisted in `.pink` (stable across versions).
    pub(crate) fn tag(self) -> u8 {
        match self {
            ProcLanguage::Sql => 0,
            ProcLanguage::JavaScript => 1,
        }
    }

    /// Decode a persisted language tag.
    pub(crate) fn from_tag(tag: u8) -> Result<ProcLanguage> {
        match tag {
            0 => Ok(ProcLanguage::Sql),
            1 => Ok(ProcLanguage::JavaScript),
            other => Err(PinkDbError::Execution(format!(
                "corrupt file: unknown procedure language tag {other}"
            ))),
        }
    }

    /// The `LANGUAGE <name>` spelling.
    pub fn name(self) -> &'static str {
        match self {
            ProcLanguage::Sql => "SQL",
            ProcLanguage::JavaScript => "JAVASCRIPT",
        }
    }
}

/// One stored procedure: its signature, language, body text and (for SQL) the
/// parsed body.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcedureDef {
    /// Display name as created (catalog keys are lowercased).
    pub name: String,
    /// Typed parameters, in written order: `(name, type)`.
    pub params: Vec<(String, DataType)>,
    pub language: ProcLanguage,
    /// The body text exactly as written (persisted in `.pink`; the SQL body is
    /// re-parsed from it on load, like a materialized view's defining SQL).
    pub source: String,
    /// The parsed body. `None` for a JavaScript definition (opaque text) and
    /// for a SQL definition whose load-time re-parse failed — the latter is a
    /// corrupt-file error at load, so a stored definition always has one.
    pub body: Option<ProcBody>,
}

impl ProcedureDef {
    /// The signature as written, for `CALL`/error messages:
    /// `name(a INTEGER, b VARCHAR)`.
    pub(crate) fn signature(&self) -> String {
        let params: Vec<String> = self
            .params
            .iter()
            .map(|(n, t)| format!("{n} {t}"))
            .collect();
        format!("{}({})", self.name, params.join(", "))
    }
}

/// A parsed procedure body: the statements of its outer `BEGIN ... END`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProcBody {
    pub stmts: Vec<ProcStmt>,
}

/// One body statement, with the byte offset of its source text inside the
/// body (for error messages).
#[derive(Debug, Clone, PartialEq)]
pub struct ProcStmt {
    pub off: usize,
    pub kind: ProcStmtKind,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ProcStmtKind {
    Declare {
        name: String,
        dtype: DataType,
        default: Option<Expr>,
    },
    Assign {
        name: String,
        expr: Expr,
    },
    If {
        /// `(condition, body)` per branch, in written order (`IF`, `ELSIF`s).
        branches: Vec<(Expr, Vec<ProcStmt>)>,
        /// The `ELSE` body (empty without one).
        else_: Vec<ProcStmt>,
    },
    While {
        cond: Expr,
        body: Vec<ProcStmt>,
    },
    /// A nested `BEGIN ... END` block (its own variable scope).
    Block(Vec<ProcStmt>),
    /// `RETURN [expr]`.
    Return(Option<Expr>),
    /// `RAISE expr`.
    Raise(Expr),
    /// An ordinary SQL statement (carrying its source text for messages).
    Sql {
        stmt: Statement,
        text: String,
    },
}

// ---------------------------------------------------------------------------
// The body parser
// ---------------------------------------------------------------------------

/// Parse a procedure body (`BEGIN ... END`) into [`ProcBody`].
///
/// Every syntax error is a [`PinkDbError::Parse`] naming the body and the byte
/// offset. The scan skips string literals, quoted identifiers, comments and
/// dollar-quoted strings, so a `;` inside any of those never splits a
/// statement. `?` placeholders are refused (a stored body has no bind call to
/// take values from).
pub(crate) fn parse_body(name: &str, params: &[String], body: &str) -> Result<ProcBody> {
    if let Some(off) = crate::sql::split::parameter_offsets(body)
        .into_iter()
        .next()
    {
        return Err(PinkDbError::Parse(format!(
            "procedure body: `?` parameters are not supported inside a procedure body \
             (found at byte {off} of \"{name}\"'s body); DECLARE a variable instead"
        )));
    }
    let mut parser = BodyParser {
        name,
        body,
        segs: scan_segments(name, body)?,
        pos: 0,
        pending: None,
        nesting: 0,
        // Parameters are the outermost scope's declarations: `SET` may target
        // them, and a `DECLARE` may not redeclare one.
        decls: vec![params.to_vec()],
    };
    parser.parse_top()
}

/// One `;`-separated (top-level) segment of a body.
#[derive(Debug, Clone, PartialEq)]
struct Seg {
    text: String,
    off: usize,
}

/// Split a body into segments at `;`, skipping strings, quoted identifiers,
/// comments and dollar-quoted strings. Unterminated quotes/comments are
/// errors (the SQL parser would see them too, but the segmenter reports them
/// with body-relative offsets first).
fn scan_segments(name: &str, body: &str) -> Result<Vec<Seg>> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            _ if bytes[i].is_ascii_whitespace() => i += 1,
            b'$' => {
                if let Some(end) = dollar_quoted_end(bytes, i) {
                    i = end;
                } else {
                    i += 1;
                }
            }
            b'\'' | b'"' => {
                i = skip_quoted(name, body, i, bytes[i])?;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return Err(PinkDbError::Parse(format!(
                        "procedure body: unterminated /* comment (procedure \"{name}\")"
                    )));
                }
            }
            b';' => {
                if let Some(s) = start.take() {
                    push_seg(&mut out, body, s, i);
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
        push_seg(&mut out, body, s, bytes.len());
    }
    Ok(out)
}

fn push_seg(out: &mut Vec<Seg>, body: &str, start: usize, end: usize) {
    let text = body[start..end].trim_end().to_string();
    if !text.trim().is_empty() {
        out.push(Seg { text, off: start });
    }
}

/// Just past a dollar-quoted run starting at `start` (a `$` byte), or `None`
/// when the bytes at `start` do not open one (`$1`, a lone `$`). Both the
/// untagged `$$ … $$` form (the grammar's literal) and the tagged
/// `$tag$ … $tag$` form are recognized: the scanner only decides where a body
/// statement ends, so recognizing more than the grammar accepts can never
/// mis-split a body — the statement parser reports the real error.
fn dollar_quoted_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return None;
    }
    let tag = &bytes[start..=j];
    let mut k = j + 1;
    while k + tag.len() <= bytes.len() {
        if &bytes[k..k + tag.len()] == tag {
            return Some(k + tag.len());
        }
        k += 1;
    }
    // Unterminated: treat the opener as ordinary text (the SQL parser reports
    // the real error when this text reaches it).
    None
}

/// Just past a quoted run starting at `start` (the opening quote is
/// `bytes[start]`); `''`/`""` escapes a quote. Unterminated is an error.
fn skip_quoted(name: &str, body: &str, start: usize, quote: u8) -> Result<usize> {
    let bytes = body.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
            } else {
                return Ok(i + 1);
            }
        } else {
            i += 1;
        }
    }
    Err(PinkDbError::Parse(format!(
        "procedure body: unterminated {} (procedure \"{name}\")",
        if quote == b'\'' {
            "string literal"
        } else {
            "quoted identifier"
        }
    )))
}

/// The leading token of a segment (leading trivia skipped).
#[derive(Debug, Clone, PartialEq)]
enum Head {
    /// A raw (unquoted) word and the byte length it spans.
    Word(String, usize),
    /// A double-quoted identifier (unescaped) and the bytes it spans.
    Quoted(String, usize),
    /// Not an identifier (an expression, `(`, a number, …).
    Other,
}

fn head(text: &str) -> Head {
    let bytes = text.as_bytes();
    match bytes.first() {
        Some(b'"') => {
            let mut name = String::new();
            let mut i = 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if bytes.get(i + 1) == Some(&b'"') {
                        name.push('"');
                        i += 2;
                    } else {
                        return Head::Quoted(name, i + 1);
                    }
                } else {
                    name.push(bytes[i] as char);
                    i += 1;
                }
            }
            Head::Other
        }
        Some(c) if c.is_ascii_alphabetic() || *c == b'_' => {
            let mut i = 0;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            Head::Word(text[..i].to_string(), i)
        }
        _ => Head::Other,
    }
}

/// `text` with leading whitespace and comments skipped (the parser classifies
/// a segment by its first real token, so a leading `-- note` never hides it).
fn trim_sql_start(text: &str) -> &str {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
        } else if bytes[i] == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i < bytes.len() {
                if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    i += 2;
                    break;
                }
                i += 1;
            }
        } else {
            break;
        }
    }
    &text[i..]
}

/// True when `text` holds nothing but trivia.
fn is_trivia_only(text: &str) -> bool {
    trim_sql_start(text).is_empty()
}

/// The byte offset of the first whole-word `kw` in `text` at parenthesis depth
/// 0 and `CASE` depth 0, scanning from `from` with strings, quoted
/// identifiers, comments and dollar-quoted strings skipped. This is how a
/// control-flow header finds its own terminator (`THEN`, `DO`) without being
/// fooled by a `THEN` of a `CASE` expression, a nested call's parentheses or a
/// literal.
fn find_top_level_keyword(text: &str, from: usize, kw: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = from;
    let mut parens = 0usize;
    let mut cases = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'$' if bytes.get(i + 1) == Some(&b'$') => {
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'$' && bytes.get(i + 1) == Some(&b'$') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
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
            b'(' => {
                parens += 1;
                i += 1;
            }
            b')' => {
                parens = parens.saturating_sub(1);
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &text[start..i];
                if word.eq_ignore_ascii_case("case") {
                    cases += 1;
                } else if word.eq_ignore_ascii_case("end") {
                    cases = cases.saturating_sub(1);
                } else if parens == 0 && cases == 0 && word.eq_ignore_ascii_case(kw) {
                    return Some(start);
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// What one segment is: the statement kind plus the trailing remainder of a
/// split control-flow header (`BEGIN <stmt>`, `IF cond THEN <stmt>`,
/// `WHILE cond DO <stmt>`, `ELSE <stmt>`), which becomes the first segment of
/// the construct's body.
#[derive(Debug)]
enum Kind {
    Declare {
        name: String,
        dtype: DataType,
        default: Option<Expr>,
    },
    Assign {
        name: String,
        expr: Expr,
    },
    If {
        cond: Expr,
    },
    While {
        cond: Expr,
    },
    Begin,
    Return(Option<Expr>),
    Raise(Expr),
    Sql(Box<Statement>),
    Stop(Stop),
}

/// A block terminator (`parse_stmts` returns without consuming it). `ELSIF`
/// and `ELSE` carry the trailing remainder of their own segment.
#[derive(Debug, Clone, PartialEq)]
enum Stop {
    End,
    EndIf,
    EndWhile,
    Elsif { cond: Expr, rest: Option<Seg> },
    Else { rest: Option<Seg> },
}

impl Stop {
    fn describe(&self) -> &'static str {
        match self {
            Stop::End => "END",
            Stop::EndIf => "END IF",
            Stop::EndWhile => "END WHILE",
            Stop::Elsif { .. } => "ELSIF",
            Stop::Else { .. } => "ELSE",
        }
    }
}

/// Which block a `parse_stmts` call belongs to (its valid terminator).
#[derive(Debug, Clone, Copy, PartialEq)]
enum Block {
    Top,
    If,
    While,
}

impl Block {
    fn expected(&self) -> &'static str {
        match self {
            Block::Top => "END",
            Block::If => "END IF",
            Block::While => "END WHILE",
        }
    }
}

struct BodyParser<'a> {
    name: &'a str,
    body: &'a str,
    segs: Vec<Seg>,
    pos: usize,
    /// The trailing remainder of a split control-flow header, read as if it
    /// were the next segment. At most one is ever pending: a split happens
    /// exactly when a header is consumed, and the block it opens reads it
    /// first.
    pending: Option<Seg>,
    nesting: usize,
    /// Declared variable names per open block (outermost first), used to
    /// validate `SET` targets and duplicate declarations at `CREATE` time.
    decls: Vec<Vec<String>>,
}

impl<'a> BodyParser<'a> {
    fn err(&self, off: usize, msg: impl std::fmt::Display) -> PinkDbError {
        PinkDbError::Parse(format!(
            "procedure body of \"{}\" at byte {off}: {msg}",
            self.name
        ))
    }

    /// The next segment (the pending remainder first).
    fn peek(&self) -> Option<&Seg> {
        self.pending.as_ref().or_else(|| self.segs.get(self.pos))
    }

    /// Consume the next segment.
    fn take(&mut self) -> Option<Seg> {
        if let Some(p) = self.pending.take() {
            return Some(p);
        }
        let seg = self.segs.get(self.pos).cloned();
        if seg.is_some() {
            self.pos += 1;
        }
        seg
    }

    fn parse_top(&mut self) -> Result<ProcBody> {
        let Some(seg) = self.peek().cloned() else {
            return Err(self.err(
                0,
                "the body is empty; write `BEGIN <statements> END` (declare the body as a \
                 dollar-quoted string: AS $$ BEGIN ... END $$)",
            ));
        };
        let text = trim_sql_start(&seg.text);
        let off = seg.off + (seg.text.len() - text.len());
        match head(text) {
            Head::Word(w, _) if w.eq_ignore_ascii_case("begin") => {}
            _ => return Err(self.err(off, "the body must be one `BEGIN ... END` block")),
        }
        let rest = &text[5..];
        self.take();
        if let Some(rest) = rest_seg(rest, off + 5) {
            self.pending = Some(rest);
        }
        self.enter(off)?;
        let (stmts, stop) = self.parse_stmts(Block::Top)?;
        if stop != Stop::End {
            let at = self.peek().map(|s| s.off).unwrap_or(self.body.len());
            return Err(self.err(
                at,
                format!("block is closed by {} (expected END)", stop.describe()),
            ));
        }
        self.take(); // consume END
        self.leave();
        if let Some(extra) = self.peek() {
            return Err(self.err(
                extra.off,
                "statements are not allowed after the body's closing END",
            ));
        }
        Ok(ProcBody { stmts })
    }

    fn enter(&mut self, off: usize) -> Result<()> {
        if self.nesting >= MAX_BODY_NESTING {
            return Err(self.err(
                off,
                format!(
                    "blocks nested too deep (limit {MAX_BODY_NESTING}); split the procedure \
                     into smaller ones"
                ),
            ));
        }
        self.nesting += 1;
        self.decls.push(Vec::new());
        Ok(())
    }

    fn leave(&mut self) {
        self.nesting = self.nesting.saturating_sub(1);
        self.decls.pop();
    }

    /// Declare `name` in the innermost block; a duplicate in the same block is
    /// an error (shadowing an outer block's variable is allowed).
    fn declare(&mut self, off: usize, name: &str) -> Result<()> {
        let scope = self.decls.last_mut().expect("a scope is always open");
        if scope.iter().any(|n| n.eq_ignore_ascii_case(name)) {
            return Err(self.err(
                off,
                format!("variable \"{name}\" is declared twice in the same block"),
            ));
        }
        scope.push(name.to_string());
        Ok(())
    }

    fn is_declared(&self, name: &str) -> bool {
        self.decls
            .iter()
            .any(|s| s.iter().any(|n| n.eq_ignore_ascii_case(name)))
    }

    /// Parse statements until the block's terminator (which is not consumed).
    fn parse_stmts(&mut self, block: Block) -> Result<(Vec<ProcStmt>, Stop)> {
        let mut out = Vec::new();
        loop {
            let Some(seg) = self.peek().cloned() else {
                return Err(self.err(
                    self.body.len(),
                    format!("block is not closed: expected {}", block.expected()),
                ));
            };
            if is_trivia_only(&seg.text) {
                self.take();
                continue;
            }
            let (kind, rest) = self.classify(&seg)?;
            if let Kind::Stop(stop) = kind {
                return Ok((out, stop));
            }
            let off = seg.off;
            self.take();
            // A split header's remainder opens the construct's body.
            self.pending = rest;
            match kind {
                Kind::Stop(_) => unreachable!("returned above"),
                Kind::Begin => {
                    self.enter(off)?;
                    let (stmts, inner) = self.parse_stmts(Block::Top)?;
                    if inner != Stop::End {
                        let at = self.peek().map(|s| s.off).unwrap_or(self.body.len());
                        return Err(self.err(
                            at,
                            format!("block is closed by {} (expected END)", inner.describe()),
                        ));
                    }
                    self.take();
                    self.leave();
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::Block(stmts),
                    });
                }
                Kind::If { cond } => {
                    self.enter(off)?;
                    let (body, mut stop) = self.parse_stmts(Block::If)?;
                    self.leave();
                    let mut branches = vec![(cond, body)];
                    let mut else_ = Vec::new();
                    loop {
                        match stop {
                            Stop::Elsif {
                                cond,
                                rest: elsif_rest,
                            } => {
                                let eoff = self.peek().map(|s| s.off).unwrap_or(self.body.len());
                                self.take();
                                self.pending = elsif_rest;
                                self.enter(eoff)?;
                                let (body, next) = self.parse_stmts(Block::If)?;
                                self.leave();
                                branches.push((cond, body));
                                stop = next;
                            }
                            Stop::Else { rest: else_rest } => {
                                let eoff = self.peek().map(|s| s.off).unwrap_or(self.body.len());
                                self.take();
                                self.pending = else_rest;
                                self.enter(eoff)?;
                                let (body, next) = self.parse_stmts(Block::If)?;
                                self.leave();
                                else_ = body;
                                stop = next;
                            }
                            Stop::EndIf => {
                                self.take();
                                break;
                            }
                            other => {
                                let at = self.peek().map(|s| s.off).unwrap_or(self.body.len());
                                return Err(self.err(
                                    at,
                                    format!(
                                        "IF is closed by {} (expected END IF)",
                                        other.describe()
                                    ),
                                ));
                            }
                        }
                    }
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::If { branches, else_ },
                    });
                }
                Kind::While { cond } => {
                    self.enter(off)?;
                    let (body, stop) = self.parse_stmts(Block::While)?;
                    self.leave();
                    if stop != Stop::EndWhile {
                        let at = self.peek().map(|s| s.off).unwrap_or(self.body.len());
                        return Err(self.err(
                            at,
                            format!(
                                "WHILE is closed by {} (expected END WHILE)",
                                stop.describe()
                            ),
                        ));
                    }
                    self.take();
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::While { cond, body },
                    });
                }
                Kind::Declare {
                    name,
                    dtype,
                    default,
                } => {
                    self.declare(off, &name)?;
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::Declare {
                            name,
                            dtype,
                            default,
                        },
                    });
                }
                Kind::Assign { name, expr } => {
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::Assign { name, expr },
                    });
                }
                Kind::Return(expr) => {
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::Return(expr),
                    });
                }
                Kind::Raise(expr) => {
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::Raise(expr),
                    });
                }
                Kind::Sql(stmt) => {
                    let text = snippet(&seg.text);
                    out.push(ProcStmt {
                        off,
                        kind: ProcStmtKind::Sql { stmt: *stmt, text },
                    });
                }
            }
        }
    }

    /// Classify one segment: the statement kind plus the trailing remainder of
    /// a split header (see [`Kind`]).
    fn classify(&self, seg: &Seg) -> Result<(Kind, Option<Seg>)> {
        let text = trim_sql_start(&seg.text);
        let base = seg.off + (seg.text.len() - text.len());
        let (word, rest_from) = match head(text) {
            Head::Word(w, len) => (Some(w.to_ascii_lowercase()), len),
            Head::Quoted(_, len) => (None, len),
            Head::Other => (None, 0),
        };
        // The remainder (leading trivia stripped; `rest_off` is its offset in
        // the body, so error messages point at the right byte).
        let rest_raw = &text[rest_from..];
        let rest = trim_sql_start(rest_raw);
        let rest_off = base + rest_from + (rest_raw.len() - rest.len());
        if let Some(w) = &word {
            match w.as_str() {
                "declare" => {
                    let (name, dtype, default) = parse_declare(self.name, base, rest)?;
                    return Ok((
                        Kind::Declare {
                            name,
                            dtype,
                            default,
                        },
                        None,
                    ));
                }
                "set" => {
                    let (name, expr) = parse_set(self.name, base, rest)?;
                    if !self.is_declared(&name) {
                        return Err(self.err(
                            base,
                            format!(
                                "SET target \"{name}\" is not declared in this block; declare \
                                 it with DECLARE {name} <type> first"
                            ),
                        ));
                    }
                    return Ok((Kind::Assign { name, expr }, None));
                }
                "if" => {
                    let (cond, rest) =
                        split_header(self.name, base, text, rest_from, "THEN", "IF")?;
                    return Ok((Kind::If { cond }, rest));
                }
                "while" => {
                    let (cond, rest) =
                        split_header(self.name, base, text, rest_from, "DO", "WHILE")?;
                    return Ok((Kind::While { cond }, rest));
                }
                "begin" => {
                    return Ok((Kind::Begin, rest_seg(rest, rest_off)));
                }
                "return" => {
                    let expr = if rest.is_empty() {
                        None
                    } else {
                        Some(parse_expr_text_at(self.name, rest_off, rest, "RETURN")?)
                    };
                    return Ok((Kind::Return(expr), None));
                }
                "raise" => {
                    let rest = strip_leading_word(rest, "exception").unwrap_or(rest);
                    if is_trivia_only(rest) {
                        return Err(
                            self.err(base, "RAISE needs a message expression (RAISE 'text')")
                        );
                    }
                    return Ok((
                        Kind::Raise(parse_expr_text_at(self.name, base, rest, "RAISE")?),
                        None,
                    ));
                }
                "end" => {
                    return Ok((
                        Kind::Stop(match (is_trivia_only(rest), head(rest)) {
                            (true, _) => Stop::End,
                            (false, Head::Word(_, _)) if rest.len() >= 2 => {
                                let word_len = match head(rest) {
                                    Head::Word(_, len) => len,
                                    _ => 0,
                                };
                                let after = trim_sql_start(&rest[word_len..]);
                                if !is_trivia_only(after) {
                                    return Err(self
                                        .err(base, "expected END, END IF or END WHILE after END"));
                                }
                                let upper = rest[..word_len].to_ascii_uppercase();
                                match upper.as_str() {
                                    "IF" => Stop::EndIf,
                                    "WHILE" => Stop::EndWhile,
                                    other => {
                                        return Err(self.err(
                                            base,
                                            format!(
                                                "expected END, END IF or END WHILE after END, \
                                                 found `{other}`"
                                            ),
                                        ));
                                    }
                                }
                            }
                            _ => {
                                return Err(
                                    self.err(base, "expected END, END IF or END WHILE after END")
                                );
                            }
                        }),
                        None,
                    ));
                }
                "elsif" => {
                    let (cond, rest) =
                        split_header(self.name, base, text, rest_from, "THEN", "ELSIF")?;
                    return Ok((Kind::Stop(Stop::Elsif { cond, rest }), None));
                }
                "else" => {
                    return Ok((
                        Kind::Stop(Stop::Else {
                            rest: rest_seg(rest, rest_off),
                        }),
                        None,
                    ));
                }
                _ => {}
            }
        }
        // A bare assignment (`x := expr` / `"x" := expr`).
        if let Some((name, expr)) = try_parse_bare_assign(self.name, base, text)? {
            if !self.is_declared(&name) {
                return Err(self.err(
                    base,
                    format!(
                        "assignment target \"{name}\" is not declared in this block; declare \
                         it with DECLARE {name} <type> first"
                    ),
                ));
            }
            return Ok((Kind::Assign { name, expr }, None));
        }
        Ok((
            Kind::Sql(Box::new(parse_sql_segment(self.name, seg)?)),
            None,
        ))
    }
}

/// A trailing segment for a split control-flow header, or `None` when the
/// remainder is empty trivia. `off` is the remainder's byte offset in the body;
/// leading trivia inside it is skipped here, so the segment reads as its first
/// real token (a `-- note` before the first statement is trivia, not a
/// statement).
fn rest_seg(text: &str, off: usize) -> Option<Seg> {
    if is_trivia_only(text) {
        return None;
    }
    let trimmed = trim_sql_start(text);
    let off = off + (text.len() - trimmed.len());
    Some(Seg {
        text: trimmed.to_string(),
        off,
    })
}

/// Split a control-flow header into its expression and trailing remainder:
/// `IF <expr> THEN [<rest>]`, `WHILE <expr> DO [<rest>]`,
/// `ELSIF <expr> THEN [<rest>]`.
fn split_header(
    name: &str,
    base: usize,
    text: &str,
    from: usize,
    end_kw: &str,
    what: &str,
) -> Result<(Expr, Option<Seg>)> {
    let Some(then_at) = find_top_level_keyword(text, from, end_kw) else {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {base}: {what} needs `{end_kw}` after the \
             condition"
        )));
    };
    let cond_text = &text[from..then_at];
    if is_trivia_only(cond_text) {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {base}: {what} needs a condition"
        )));
    }
    let cond_off = base + from;
    let rest_from = then_at + end_kw.len();
    let rest = rest_seg(&text[rest_from..], base + rest_from);
    let cond = parse_expr_text_at(name, cond_off, cond_text, what)?;
    Ok((cond, rest))
}

/// Strip a leading keyword (`EXCEPTION`) from a segment remainder.
fn strip_leading_word<'t>(rest: &'t str, word: &str) -> Option<&'t str> {
    let trimmed = trim_sql_start(rest);
    let bytes = trimmed.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    if i > 0 && trimmed[..i].eq_ignore_ascii_case(word) {
        Some(trimmed[i..].trim_start())
    } else {
        None
    }
}

/// The identifier starting `text` (raw or double-quoted), and the *untrimmed*
/// rest (callers trim it after [`ident_end`] checked the boundary).
fn parse_ident_at<'t>(name: &str, off: usize, text: &'t str) -> Result<(String, &'t str)> {
    let bytes = text.as_bytes();
    match bytes.first() {
        Some(b'"') => {
            let mut out = String::new();
            let mut i = 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if bytes.get(i + 1) == Some(&b'"') {
                        out.push('"');
                        i += 2;
                    } else {
                        return Ok((out, text[i + 1..].trim_start()));
                    }
                } else {
                    out.push(bytes[i] as char);
                    i += 1;
                }
            }
            Err(PinkDbError::Parse(format!(
                "procedure body of \"{name}\" at byte {off}: unterminated quoted identifier"
            )))
        }
        Some(c) if c.is_ascii_alphabetic() || *c == b'_' => {
            let mut i = 0;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            Ok((text[..i].to_string(), &text[i..]))
        }
        _ => Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: expected an identifier"
        ))),
    }
}

/// The rest of a segment after a variable name: the name must be followed by
/// a non-identifier character (so `SET xy = 1` never reads as `SET x`).
fn ident_end<'t>(name: &str, off: usize, var: &str, rest: &'t str) -> Result<&'t str> {
    if rest
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '"')
    {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: \"{var}\" is not a whole identifier"
        )));
    }
    Ok(rest)
}

/// `DECLARE <name> <type> [DEFAULT <expr>]`.
fn parse_declare(name: &str, off: usize, rest: &str) -> Result<(String, DataType, Option<Expr>)> {
    let (var, after) = parse_ident_at(name, off, rest)?;
    let after = ident_end(name, off, &var, after)?.trim_start();
    // Split a trailing top-level `DEFAULT <expr>` from the type text.
    let (type_text, default_text) = match split_keyword(after, "default") {
        Some((t, d)) => (t, Some(d)),
        None => (after, None),
    };
    if type_text.trim().is_empty() {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: DECLARE {var} needs a type \
             (DECLARE {var} INTEGER ...)"
        )));
    }
    if let Some(d) = default_text
        && is_trivia_only(d)
    {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: DEFAULT needs an expression"
        )));
    }
    let dtype = parse_type(name, off, type_text.trim())?;
    let default = match default_text {
        Some(d) => Some(parse_expr_text_at(name, off, d, "DEFAULT")?),
        None => None,
    };
    Ok((var, dtype, default))
}

/// `SET <name> (= | :=) <expr>` (the `SET` keyword already consumed).
fn parse_set(name: &str, off: usize, rest: &str) -> Result<(String, Expr)> {
    let (var, after) = parse_ident_at(name, off, rest)?;
    let after = ident_end(name, off, &var, after)?.trim_start();
    let Some((_, expr_text)) = split_assign_op(after) else {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: SET needs `name = expr`"
        )));
    };
    if is_trivia_only(expr_text) {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: SET needs an expression"
        )));
    }
    Ok((var, parse_expr_text_at(name, off, expr_text, "SET")?))
}

/// A bare `name := expr` segment, if it is one. Only `:=` marks a bare
/// assignment: an ordinary SQL statement (`EXPLAIN SELECT … WHERE a = 1`)
/// can hold a top-level `=` in its text, a `:=` never.
fn try_parse_bare_assign(name: &str, off: usize, text: &str) -> Result<Option<(String, Expr)>> {
    let Ok((var, after)) = parse_ident_at(name, off, text) else {
        return Ok(None);
    };
    let after = after.trim_start();
    let Some(expr_text) = after.strip_prefix(":=") else {
        return Ok(None);
    };
    if is_trivia_only(expr_text) {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: `:=` needs an expression"
        )));
    }
    Ok(Some((
        var,
        parse_expr_text_at(name, off, expr_text, "assignment")?,
    )))
}

/// Split `text` at the first top-level occurrence of whole-word `kw`
/// (outside strings, quoted identifiers, comments and parentheses). Returns
/// `(before, after)`.
fn split_keyword<'t>(text: &'t str, kw: &str) -> Option<(&'t str, &'t str)> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
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
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            c if depth == 0 && (c.is_ascii_alphabetic() || c == b'_') => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if text[start..i].eq_ignore_ascii_case(kw) {
                    return Some((&text[..start], &text[i..]));
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// Split at the first top-level `=` or `:=` (not part of `>=`, `<=`, `!=`,
/// `<>`). Returns `(before, after)`.
fn split_assign_op(text: &str) -> Option<(&str, &str)> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
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
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b':' if depth == 0 && bytes.get(i + 1) == Some(&b'=') => {
                return Some((&text[..i], &text[i + 2..]));
            }
            b'=' if depth == 0 => {
                let prev = i.checked_sub(1).map(|j| bytes[j]);
                let next = bytes.get(i + 1).copied();
                let part_of_op = matches!(prev, Some(b'<') | Some(b'>') | Some(b'!') | Some(b'='));
                if !part_of_op && next != Some(b'=') {
                    return Some((&text[..i], &text[i + 1..]));
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

/// Parse a type name by parsing `SELECT CAST(NULL AS <text>)` and reading the
/// cast's resolved type — the engine's own type grammar, no duplication.
fn parse_type(name: &str, off: usize, text: &str) -> Result<DataType> {
    let sql = format!("SELECT CAST(NULL AS {text});");
    let err = |e: PinkDbError| {
        PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: `{text}` is not a type: {e}"
        ))
    };
    let stmts = crate::sql::parse_statements(&sql).map_err(err)?;
    let Some(Statement::Select(sel)) = stmts.into_iter().next() else {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: `{text}` is not a type"
        )));
    };
    match sel.projections.into_iter().next() {
        Some(Projection::Expr {
            expr: Expr::Cast { dtype, .. },
            alias: None,
        }) => Ok(dtype),
        _ => Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: `{text}` is not a type"
        ))),
    }
}

/// Parse one expression by wrapping it in `SELECT (<text>)` — the engine's own
/// expression grammar. The parentheses prevent a trailing token from being
/// absorbed as a projection alias.
fn parse_expr_text_at(name: &str, off: usize, text: &str, what: &str) -> Result<Expr> {
    let sql = format!("SELECT ({text});");
    let stmts = crate::sql::parse_statements(&sql).map_err(|e| {
        PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: {what}: not an expression: {e}"
        ))
    })?;
    let Some(Statement::Select(sel)) = stmts.into_iter().next() else {
        return Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: {what}: not an expression"
        )));
    };
    match sel.projections.into_iter().next() {
        Some(Projection::Expr { expr, alias: None }) => Ok(expr),
        _ => Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {off}: {what}: not an expression"
        ))),
    }
}

/// Statements that may not appear in a procedure body (store/process-level
/// operations, transaction control, and procedure/materialized-view DDL).
/// Returns the SQL statement name for the error. `CALL` is allowed (bounded by
/// [`MAX_CALL_DEPTH`]); a `?` never reaches here — [`parse_body`] refuses
/// placeholders in the body text up front.
pub(crate) fn refused_in_body(stmt: &Statement) -> Option<&'static str> {
    Some(match stmt {
        Statement::Begin => "BEGIN (use BEGIN ... END for a block)",
        Statement::Commit => "COMMIT",
        Statement::Rollback => "ROLLBACK",
        Statement::Use { .. } => "USE",
        Statement::Checkpoint => "CHECKPOINT",
        Statement::LoadExtension { .. } => "LOAD EXTENSION",
        Statement::CreateDatabase { .. } => "CREATE DATABASE",
        Statement::DropDatabase { .. } => "DROP DATABASE",
        Statement::CreateProcedure { .. } => "CREATE PROCEDURE",
        Statement::DropProcedure { .. } => "DROP PROCEDURE",
        // Wave 77: a macro definition is catalog metadata the call's snapshot
        // undo would not carry, exactly like a procedure definition. Macro
        // *use* inside a body is ordinary SQL and is allowed.
        Statement::CreateMacro { .. } => "CREATE MACRO",
        Statement::DropMacro { .. } => "DROP MACRO",
        Statement::CreateMaterializedView { .. } => "CREATE MATERIALIZED VIEW",
        Statement::DropMaterializedView { .. } => "DROP MATERIALIZED VIEW",
        Statement::RefreshMaterializedView { .. } => "REFRESH MATERIALIZED VIEW",
        Statement::RefreshMaterializedViews => "REFRESH MATERIALIZED VIEWS",
        _ => return None,
    })
}

/// Parse one ordinary SQL statement segment (exactly one statement).
fn parse_sql_segment(name: &str, seg: &Seg) -> Result<Statement> {
    let stmts = crate::sql::parse_statements(&seg.text).map_err(|e| {
        PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {} (near `{}`): {e}",
            seg.off,
            snippet(&seg.text)
        ))
    })?;
    match stmts.len() {
        1 => {
            let stmt = stmts.into_iter().next().expect("checked one");
            if let Some(what) = refused_in_body(&stmt) {
                return Err(PinkDbError::Parse(format!(
                    "procedure body of \"{name}\" at byte {}: {what} is not allowed inside a \
                     procedure body",
                    seg.off
                )));
            }
            Ok(stmt)
        }
        0 => Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {}: empty statement",
            seg.off
        ))),
        n => Err(PinkDbError::Parse(format!(
            "procedure body of \"{name}\" at byte {}: a body statement must be exactly one \
             statement (found {n})",
            seg.off
        ))),
    }
}

/// A short one-line excerpt of a statement's source for error messages.
fn snippet(text: &str) -> String {
    let one_line: String = text
        .chars()
        .map(|c| if c == '\n' { ' ' } else { c })
        .collect();
    let mut out: String = one_line.chars().take(60).collect();
    if one_line.chars().count() > 60 {
        out.push('…');
    }
    out
}

// ---------------------------------------------------------------------------
// CALL execution
// ---------------------------------------------------------------------------

/// The touched-object set of the `CALL` currently executing (wave 44).
///
/// A call's statements rebind and run one by one, and the tables they touch
/// depend on the data (loop conditions, `IF` branches), so the set cannot be
/// computed from the plan before execution. The executor records every
/// statement's reads and writes here as they run; the statement-boundary
/// logging paths read it right after the call returns — a WAL frame carries
/// the call's actual set, and a transaction's conflict set includes it.
///
/// Statement-scoped: the engine clears it at every statement boundary
/// (`PinkDb::proc_frame_reset`), so nothing leaks from an earlier statement.
#[derive(Debug, Default, Clone)]
pub(crate) struct ProcFrame {
    /// Every `(database, table)` any executed statement read or wrote.
    touched: crate::wal::FrameSets,
    /// The `(database, table)` pairs among those that were *written* (the ON
    /// COMMIT materialized-view trigger's dirty set).
    writes: std::collections::HashSet<(String, String)>,
}

impl ProcFrame {
    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn add_sets(&mut self, sets: &crate::wal::FrameSets) {
        self.touched.extend(sets);
    }

    pub(crate) fn add_writes(&mut self, writes: &std::collections::HashSet<(String, String)>) {
        self.writes.extend(writes.iter().cloned());
    }

    /// The canonical touched set (sorted, deduped, lowercased).
    pub(crate) fn touched(&self) -> crate::wal::FrameSets {
        let mut out = self.touched.clone();
        out.canonicalize();
        out
    }

    /// The set of tables the call wrote.
    pub(crate) fn writes(&self) -> std::collections::HashSet<(String, String)> {
        self.writes.clone()
    }

    /// The set of tables the call `read` (touched minus written).
    pub(crate) fn reads(&self) -> std::collections::HashSet<(String, String)> {
        let mut out: std::collections::HashSet<(String, String)> =
            self.touched.table_pairs().iter().cloned().collect();
        for w in &self.writes {
            out.remove(w);
        }
        out
    }
}

impl PinkDb {
    /// Wave 44: fold a `CALL` that just ran into the open transaction's
    /// bookkeeping. The call's actual touched set is only known after
    /// execution (loop conditions are data-dependent) and the pre-execution
    /// bookkeeping saw an empty plan set, so it is recorded here — both for
    /// `COMMIT`'s read/write conflict validation and for the ON COMMIT
    /// materialized-view trigger. A no-op outside a transaction and for any
    /// other statement.
    pub(crate) fn note_proc_frame_in_txn(&mut self, plan: &LogicalPlan) {
        if !self.in_transaction() || !matches!(plan, LogicalPlan::Call { .. }) {
            return;
        }
        self.txn_touched.extend(&self.proc_frame.touched());
        self.txn_writes.extend(self.proc_frame.writes());
    }
}

/// One variable (a parameter or a `DECLARE`).
#[derive(Debug, Clone)]
struct Var {
    name: String,
    dtype: DataType,
    value: Value,
}

/// Per-call execution state.
struct Frame<'a> {
    name: &'a str,
    db_name: String,
    scopes: Vec<Vec<Var>>,
    /// Body statements executed so far (reported by `QueryResult::Called`).
    statements: usize,
    /// The last statement's result.
    last: Option<QueryResult>,
}

/// What a block did.
enum Flow {
    /// Ran to the end.
    Normal,
    /// `RETURN [expr]`: the optional value is the call's result.
    Return(Option<QueryResult>),
}

impl<'a> Frame<'a> {
    /// Every variable in scope, outermost first — later entries shadow
    /// earlier ones for the binder's lookup.
    fn variables(&self) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        for scope in &self.scopes {
            for v in scope {
                out.push((v.name.clone(), v.value.clone()));
            }
        }
        out
    }

    fn lookup(&self, name: &str) -> Option<&Var> {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope
                .iter()
                .rev()
                .find(|v| v.name.eq_ignore_ascii_case(name))
            {
                return Some(v);
            }
        }
        None
    }

    fn lookup_mut(&mut self, name: &str) -> Option<&mut Var> {
        for scope in self.scopes.iter_mut().rev() {
            if let Some(i) = scope
                .iter()
                .rposition(|v| v.name.eq_ignore_ascii_case(name))
            {
                return scope.get_mut(i);
            }
        }
        None
    }

    fn err(&self, off: usize, msg: impl std::fmt::Display) -> PinkDbError {
        PinkDbError::Execution(format!("procedure \"{}\" at byte {off}: {msg}", self.name))
    }
}

/// Run one `CALL`: coerce the arguments, execute the body, and return the
/// call's result. See the module docs for the transaction/atomicity rule.
pub(crate) fn execute_call(
    db: &mut PinkDb,
    db_name: &str,
    def: &ProcedureDef,
    args: Vec<Value>,
) -> Result<QueryResult> {
    if db.proc_depth >= MAX_CALL_DEPTH {
        return Err(PinkDbError::Execution(format!(
            "procedure call depth exceeded (limit {MAX_CALL_DEPTH}) calling \"{}\"; \
             a procedure may call itself up to {MAX_CALL_DEPTH} levels deep",
            def.name
        )));
    }
    db.proc_depth += 1;
    let out = execute_call_inner(db, db_name, def, args);
    db.proc_depth = db.proc_depth.saturating_sub(1);
    out
}

fn execute_call_inner(
    db: &mut PinkDb,
    db_name: &str,
    def: &ProcedureDef,
    args: Vec<Value>,
) -> Result<QueryResult> {
    if let Some(e) = unsupported_language(def) {
        return Err(e);
    }
    if def.language == ProcLanguage::Sql && def.body.is_none() {
        return Err(PinkDbError::Execution(format!(
            "procedure \"{}\" has no body in this build",
            def.name
        )));
    }
    if args.len() != def.params.len() {
        return Err(PinkDbError::Execution(format!(
            "procedure \"{}\" takes {} argument(s), but {} were given",
            def.signature(),
            def.params.len(),
            args.len()
        )));
    }
    let mut scope = Vec::with_capacity(args.len());
    for ((name, dtype), value) in def.params.iter().zip(args) {
        let value = crate::storage::DataTable::coerce(value, *dtype).map_err(|e| {
            PinkDbError::Type(format!(
                "procedure \"{}\": argument \"{name}\": {e}",
                def.name
            ))
        })?;
        scope.push(Var {
            name: name.clone(),
            dtype: *dtype,
            value,
        });
    }
    // Statement atomicity: the outermost call is all-or-nothing. It takes its
    // own snapshot (the store clone the transaction machinery uses anyway) and
    // restores it on failure — *including* inside a caller's transaction,
    // where the restore keeps that transaction open and its earlier
    // statements, buffered WAL text and validation base untouched. On success
    // the snapshot is simply dropped: the call's effect stays in memory, and
    // the runner logs the `CALL` statement like any other statement's. The
    // rule is the same for both languages.
    let snapshot = if db.proc_depth == 1 {
        Some(Box::new(db.clone()))
    } else {
        None
    };
    let out = match def.language {
        ProcLanguage::Sql => run_sql_body(db, db_name, def, scope),
        ProcLanguage::JavaScript => run_js_body(db, db_name, def, scope),
    };
    if out.is_err()
        && let Some(snap) = snapshot
    {
        db.proc_snapshot_restore(snap);
    }
    out
}

/// Run a SQL body with its parameter scope (the caller owns atomicity).
fn run_sql_body(
    db: &mut PinkDb,
    db_name: &str,
    def: &ProcedureDef,
    scope: Vec<Var>,
) -> Result<QueryResult> {
    let Some(body) = &def.body else {
        return Err(PinkDbError::Execution(format!(
            "procedure \"{}\" has no body in this build",
            def.name
        )));
    };
    let mut frame = Frame {
        name: &def.name,
        db_name: db_name.to_string(),
        scopes: vec![scope],
        statements: 0,
        last: None,
    };
    let flow = exec_block(db, &mut frame, &body.stmts)?;
    let result = match flow {
        Flow::Return(Some(result)) => Some(result),
        Flow::Return(None) | Flow::Normal => frame.last,
    };
    Ok(QueryResult::Called {
        name: def.name.clone(),
        statements: frame.statements,
        result: result.map(Box::new),
        output: Vec::new(),
    })
}

/// Run a JavaScript body in the `pinkdb-js` sandbox (the caller owns
/// atomicity; `unsupported_language` already refused builds without it).
#[cfg(feature = "js")]
fn run_js_body(
    db: &mut PinkDb,
    db_name: &str,
    def: &ProcedureDef,
    scope: Vec<Var>,
) -> Result<QueryResult> {
    let args: Vec<Value> = scope.into_iter().map(|v| v.value).collect();
    crate::js::call(db, db_name, def, &args)
}

#[cfg(not(feature = "js"))]
fn run_js_body(
    _db: &mut PinkDb,
    _db_name: &str,
    def: &ProcedureDef,
    _scope: Vec<Var>,
) -> Result<QueryResult> {
    Err(unsupported_language_error(def))
}

/// A clean error for a definition this build cannot run (a build without
/// the `js` feature, or a `pinkdb-js` without its engine).
fn unsupported_language(def: &ProcedureDef) -> Option<PinkDbError> {
    match def.language {
        ProcLanguage::Sql => None,
        ProcLanguage::JavaScript => {
            #[cfg(feature = "js")]
            if crate::js::engine_available().is_ok() {
                return None;
            }
            Some(unsupported_language_error(def))
        }
    }
}

fn unsupported_language_error(def: &ProcedureDef) -> PinkDbError {
    PinkDbError::Execution(format!(
        "procedure \"{}\" is written in JavaScript, and this build cannot run it: {}",
        def.name,
        js_unavailable_reason()
    ))
}

/// Execute a block: a fresh variable scope for its statements.
fn exec_block(db: &mut PinkDb, frame: &mut Frame, stmts: &[ProcStmt]) -> Result<Flow> {
    frame.scopes.push(Vec::new());
    let out = exec_stmts(db, frame, stmts);
    frame.scopes.pop();
    out
}

fn exec_stmts(db: &mut PinkDb, frame: &mut Frame, stmts: &[ProcStmt]) -> Result<Flow> {
    for stmt in stmts {
        match &stmt.kind {
            ProcStmtKind::Declare {
                name,
                dtype,
                default,
            } => {
                let value = match default {
                    None => Value::Null,
                    Some(expr) => {
                        let v = eval_expr_value(db, frame, stmt.off, expr, "DECLARE")?;
                        crate::storage::DataTable::coerce(v, *dtype).map_err(|e| {
                            frame.err(
                                stmt.off,
                                format!("DECLARE \"{name}\": cannot initialize as {dtype}: {e}"),
                            )
                        })?
                    }
                };
                frame.scopes.last_mut().expect("a scope is open").push(Var {
                    name: name.clone(),
                    dtype: *dtype,
                    value,
                });
            }
            ProcStmtKind::Assign { name, expr } => {
                let Some(var) = frame.lookup(name) else {
                    return Err(frame.err(stmt.off, format!("variable \"{name}\" is not in scope")));
                };
                let dtype = var.dtype;
                let v = eval_expr_value(db, frame, stmt.off, expr, "SET")?;
                let v = crate::storage::DataTable::coerce(v, dtype).map_err(|e| {
                    frame.err(
                        stmt.off,
                        format!("SET {name}: cannot store value as {dtype}: {e}"),
                    )
                })?;
                if let Some(var) = frame.lookup_mut(name) {
                    var.value = v;
                }
            }
            ProcStmtKind::If { branches, else_ } => {
                let mut taken = None;
                for (cond, body) in branches {
                    if eval_cond(db, frame, stmt.off, cond)? {
                        taken = Some(body);
                        break;
                    }
                }
                let body = match taken {
                    Some(b) => b,
                    None => else_,
                };
                if let Flow::Return(v) = exec_block(db, frame, body)? {
                    return Ok(Flow::Return(v));
                }
            }
            ProcStmtKind::While { cond, body } => {
                while eval_cond(db, frame, stmt.off, cond)? {
                    if let Flow::Return(v) = exec_block(db, frame, body)? {
                        return Ok(Flow::Return(v));
                    }
                }
            }
            ProcStmtKind::Block(inner) => {
                if let Flow::Return(v) = exec_block(db, frame, inner)? {
                    return Ok(Flow::Return(v));
                }
            }
            ProcStmtKind::Return(expr) => {
                let value = match expr {
                    None => None,
                    Some(e) => {
                        let bound = bind_constant(db, frame, stmt.off, e, "RETURN")?;
                        let dtype = bound.dtype();
                        let v = eval_bound(db, frame, stmt.off, &bound)?;
                        Some(QueryResult::Selected {
                            names: vec!["return".to_string()],
                            types: vec![dtype],
                            rows: vec![vec![v]],
                        })
                    }
                };
                return Ok(Flow::Return(value));
            }
            ProcStmtKind::Raise(expr) => {
                let v = eval_expr_value(db, frame, stmt.off, expr, "RAISE")?;
                return Err(match v {
                    Value::Varchar(msg) => PinkDbError::Execution(format!(
                        "procedure \"{}\" raised: {msg}",
                        frame.name
                    )),
                    Value::Null => Err(frame.err(stmt.off, "RAISE needs a non-NULL message"))?,
                    other => Err(frame.err(
                        stmt.off,
                        format!(
                            "RAISE needs a VARCHAR message, got {other} ({})",
                            type_name(&other)
                        ),
                    ))?,
                });
            }
            ProcStmtKind::Sql { stmt: sql, text } => {
                frame.statements += 1;
                let result = exec_sql(db, frame, stmt.off, sql).map_err(|e| {
                    PinkDbError::Execution(format!(
                        "procedure \"{}\": statement {} (`{text}`): {e}",
                        frame.name, frame.statements
                    ))
                })?;
                frame.last = Some(result);
            }
        }
    }
    Ok(Flow::Normal)
}

fn type_name(v: &Value) -> String {
    match v.data_type() {
        Some(t) => t.to_string(),
        None => "NULL".to_string(),
    }
}

/// Evaluate a condition expression (bound fresh in an empty scope with the
/// frame's variables visible): NULL counts as false.
fn eval_cond(db: &mut PinkDb, frame: &Frame, off: usize, expr: &Expr) -> Result<bool> {
    let bound = bind_constant(db, frame, off, expr, "condition")?;
    if bound.dtype() != DataType::Boolean {
        return Err(frame.err(
            off,
            format!(
                "condition must be BOOLEAN, got {} — write an explicit comparison",
                bound.dtype()
            ),
        ));
    }
    match eval_bound(db, frame, off, &bound)? {
        Value::Boolean(b) => Ok(b),
        // The binder types the expression BOOLEAN, so this is unreachable; a
        // total match keeps the executor honest.
        other => Err(frame.err(
            off,
            format!("condition did not evaluate to BOOLEAN (got {other})"),
        )),
    }
}

/// Bind one constant expression (no table scope) with the frame's variables
/// substituted as bound values.
///
/// A `CALL` is WAL-logged as a single record and replay re-evaluates this
/// expression, so a **volatile** (nondeterministic) extension call here could
/// take a different branch (or produce a different value) on replay: it is
/// refused, exactly like the statement-level guard refuses it in a logged
/// statement. Deterministic calls keep working here as before — they replay
/// identically.
fn bind_constant(
    db: &PinkDb,
    frame: &Frame,
    off: usize,
    expr: &Expr,
    what: &str,
) -> Result<crate::sql::binder::BoundExpr> {
    let slots = crate::sql::binder::ParamSlots::new(Vec::new()).with_variables(frame.variables());
    let (bound, dynamic) =
        crate::sql::binder::bind_constant_expr_tracked(db, &frame.db_name, expr, Some(slots))
            .map_err(|e| frame.err(off, format!("{what}: {e}")))?;
    if let Some(function) = db.first_volatile_dynamic_function(&dynamic) {
        return Err(frame.err(
            off,
            format!(
                "{what}: extension function {function}() is volatile (nondeterministic) and cannot \
                 be evaluated inside a procedure body: a CALL is WAL-logged and replayed, so its \
                 result could differ. Call it in a read-only statement outside the procedure"
            ),
        ));
    }
    Ok(bound)
}

/// Evaluate a bound constant expression against an empty single-row chunk.
fn eval_bound(
    db: &PinkDb,
    frame: &Frame,
    off: usize,
    bound: &crate::sql::binder::BoundExpr,
) -> Result<Value> {
    let single = crate::vector::DataChunk {
        columns: Vec::new(),
        num_rows: 1,
    };
    let cx = crate::exec::Cx::new(db);
    let v = crate::exec::expr::eval_expr(bound, &single, &cx)?;
    if v.len() != 1 {
        return Err(frame.err(off, "the expression did not evaluate to exactly one value"));
    }
    Ok(v.get(0))
}

/// Bind+evaluate an expression in one step.
fn eval_expr_value(
    db: &mut PinkDb,
    frame: &Frame,
    off: usize,
    expr: &Expr,
    what: &str,
) -> Result<Value> {
    let bound = bind_constant(db, frame, off, expr, what)?;
    eval_bound(db, frame, off, &bound)
}

/// Execute one ordinary SQL statement of a body: rebind it against the current
/// catalog with the frame's variables as values, refuse the constructs whose
/// replay would not be deterministic, record its touched set, and run it.
fn exec_sql(
    db: &mut PinkDb,
    frame: &mut Frame,
    off: usize,
    sql: &Statement,
) -> Result<QueryResult> {
    let slots = crate::sql::binder::ParamSlots::new(Vec::new()).with_variables(frame.variables());
    let db_name = frame.db_name.clone();
    run_call_statement(db, &db_name, slots, sql, |msg| frame.err(off, msg))
}

/// Bind, check and run one statement of a procedure call — the shared path
/// of SQL bodies (variables as bound literals) and JavaScript host calls
/// (`?` placeholders bound to the script's values). `err` builds the
/// language-specific "this statement cannot be part of a logged call" error.
pub(crate) fn run_call_statement(
    db: &mut PinkDb,
    db_name: &str,
    slots: crate::sql::binder::ParamSlots,
    sql: &Statement,
    err: impl Fn(&str) -> PinkDbError,
) -> Result<QueryResult> {
    let slots = slots.with_logged_procedure(db.is_durable());
    let (plan, _dynamic) = crate::sql::binder::bind_tracked_with(db, sql, db_name, Some(slots))?;
    // Lazy index lifecycle: mutation times only set a flag (see
    // `PinkDb::rebuild_stale_indexes`).
    db.rebuild_stale_indexes();
    let plan = crate::optimizer::optimize(db, plan);
    if crate::plan::contains_dynamic_scalar(&plan) {
        return Err(err(
            "a statement of this procedure calls a loadable-extension function; extension \
             callbacks are opaque (impure or nondeterministic), so a durable store cannot \
             replay this call — run the statement outside the procedure instead",
        ));
    }
    // The whole call is logged on a durable store: a statement that reads a
    // session-local lake could not replay.
    db.refuse_logged_attached_reads(&plan)?;
    // The call's actual touched set (the WAL frame's payload and the
    // transaction's conflict set): collected here because loop conditions are
    // data-dependent and cannot be approximated before execution.
    let sets = db.plan_sets_store(&plan, db_name);
    let writes = db.plan_writes_of(&plan, db_name);
    db.proc_frame.add_sets(&sets);
    db.proc_frame.add_writes(&writes);
    crate::exec::run(db, &plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryResult;
    use crate::types::Value;
    use crate::wal::SyncPolicy;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Per-test scratch directory (removed on drop, panics included), so no
    /// `.pink`/`.wal`/lock artifact ever lands in the tree.
    struct Scratch {
        dir: PathBuf,
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "pinkdb_proc_{tag}_{}_{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self { dir }
        }

        fn path(&self) -> String {
            self.dir.join("db.pink").to_string_lossy().to_string()
        }

        fn other(&self, name: &str) -> String {
            self.dir.join(name).to_string_lossy().to_string()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn durable(s: &Scratch) -> PinkDb {
        PinkDb::open_durable_with(&s.path(), SyncPolicy::Batch)
            .unwrap()
            .0
    }

    fn last(db: &mut PinkDb, sql: &str) -> QueryResult {
        db.execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .pop()
            .expect("one result")
    }

    fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        match last(db, sql) {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows from {sql}, got {other:?}"),
        }
    }

    fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
        rows(db, sql)
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i as i64,
                Value::Bigint(i) => *i,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect()
    }

    fn err(db: &mut PinkDb, sql: &str) -> String {
        db.execute(sql)
            .expect_err(&format!("{sql} was expected to fail"))
            .to_string()
    }

    /// The `Called` payload of one `CALL`.
    fn called(db: &mut PinkDb, sql: &str) -> (String, usize, Option<QueryResult>) {
        match last(db, sql) {
            QueryResult::Called {
                name,
                statements,
                result,
                ..
            } => (name, statements, result.map(|r| *r)),
            other => panic!("expected a CALL result from {sql}, got {other:?}"),
        }
    }

    fn seeded() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y');")
            .unwrap();
        db
    }

    // ---------- the happy path ----------

    /// CREATE / CALL / DROP round-trip: an INSERT procedure appends rows, the
    /// call reports its statement count and last result, DROP removes it.
    #[test]
    fn create_call_drop_roundtrip() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE add_row(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'proc'); END $$;",
        )
        .unwrap();
        assert!(db.procedure_in("main", "add_row").is_ok());
        let (name, statements, result) = called(&mut db, "CALL add_row(7);");
        assert_eq!(name, "add_row");
        assert_eq!(statements, 1);
        assert_eq!(result, Some(QueryResult::Inserted { count: 1 }));
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 7]);
        // A second call appends again (the body rebinds every time).
        db.execute("CALL add_row(8);").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 7, 8]
        );
        assert_eq!(
            last(&mut db, "CALL add_row(9);"),
            QueryResult::Called {
                name: "add_row".to_string(),
                statements: 1,
                result: Some(Box::new(QueryResult::Inserted { count: 1 })),
                output: Vec::new(),
            }
        );
        // DROP; IF EXISTS is a no-op afterwards.
        db.execute("DROP PROCEDURE add_row;").unwrap();
        assert!(db.procedure_in("main", "add_row").is_err());
        let msg = err(&mut db, "CALL add_row(1);");
        assert!(msg.contains("not found"), "{msg}");
        db.execute("DROP PROCEDURE IF EXISTS add_row;").unwrap();
        let msg = err(&mut db, "DROP PROCEDURE add_row;");
        assert!(msg.contains("not found"), "{msg}");
        // CREATE OR REPLACE swaps the definition; a plain CREATE refuses.
        db.execute(
            "CREATE PROCEDURE p(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'one'); END $$;",
        )
        .unwrap();
        let msg = err(
            &mut db,
            "CREATE PROCEDURE p(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'two'); END $$;",
        );
        assert!(msg.contains("already exists"), "{msg}");
        db.execute(
            "CREATE OR REPLACE PROCEDURE p(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'two'); END $$;",
        )
        .unwrap();
        db.execute("CALL p(100);").unwrap();
        assert_eq!(
            rows(&mut db, "SELECT b FROM t WHERE a = 100;"),
            vec![vec![Value::Varchar("two".to_string())]]
        );
    }

    /// The body rebinds per call, so a procedure may create a table and then
    /// use it (and the same call sees the fresh schema).
    #[test]
    fn a_body_sees_the_catalog_it_builds() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE PROCEDURE mk(x INTEGER) AS $$ BEGIN \
               CREATE TABLE fresh (v INTEGER); \
               INSERT INTO fresh VALUES (x); \
               INSERT INTO fresh VALUES (x + 1); \
             END $$;",
        )
        .unwrap();
        let (_, statements, _) = called(&mut db, "CALL mk(41);");
        assert_eq!(statements, 3);
        assert_eq!(
            ints(&mut db, "SELECT v FROM fresh ORDER BY v;"),
            vec![41, 42]
        );
        // A second call fails cleanly at the CREATE (the table exists) and —
        // autocommit atomicity — leaves the existing data alone.
        let msg = err(&mut db, "CALL mk(1);");
        assert!(
            msg.contains("fresh") && msg.contains("already exists"),
            "{msg}"
        );
        assert_eq!(
            ints(&mut db, "SELECT v FROM fresh ORDER BY v;"),
            vec![41, 42]
        );
    }

    /// Parameters and `DECLARE`/`SET` variables, `IF`/`ELSIF`/`ELSE`,
    /// `WHILE`, nested blocks and `RETURN`.
    #[test]
    fn variables_and_control_flow() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE facts(n INTEGER) AS $$ BEGIN \
               DECLARE i INTEGER DEFAULT 1; \
               DECLARE acc BIGINT DEFAULT 1; \
               WHILE i <= n DO \
                 SET acc = acc * i; \
                 SET i = i + 1; \
               END WHILE; \
               RETURN acc; \
             END $$;",
        )
        .unwrap();
        let (_, statements, result) = called(&mut db, "CALL facts(5);");
        assert_eq!(statements, 0, "only non-SQL statements ran");
        assert_eq!(
            result,
            Some(QueryResult::Selected {
                names: vec!["return".to_string()],
                types: vec![DataType::Bigint],
                rows: vec![vec![Value::Bigint(120)]],
            })
        );
        // IF / ELSIF / ELSE with a nested block and a nested IF.
        db.execute(
            "CREATE PROCEDURE classify(v INTEGER) AS $$ BEGIN \
               DECLARE out VARCHAR DEFAULT 'none'; \
               IF v > 0 THEN \
                 BEGIN \
                   DECLARE doubled INTEGER DEFAULT v * 2; \
                   IF doubled > 10 THEN SET out = 'big'; ELSE SET out = 'small'; END IF; \
                 END; \
               ELSIF v = 0 THEN \
                 SET out = 'zero'; \
               ELSE \
                 SET out = 'negative'; \
               END IF; \
               INSERT INTO t VALUES (v, out); \
             END $$;",
        )
        .unwrap();
        for v in [7, 3, 0, -4] {
            db.execute(&format!("CALL classify({v});")).unwrap();
        }
        assert_eq!(
            rows(
                &mut db,
                "SELECT b FROM t WHERE a IN (7, 3, 0, -4) ORDER BY a;"
            ),
            vec![
                vec![Value::Varchar("negative".to_string())], // -4
                vec![Value::Varchar("zero".to_string())],     // 0
                vec![Value::Varchar("small".to_string())],    // 3
                vec![Value::Varchar("big".to_string())],      // 7
            ]
        );
        // `:=` is accepted for SET, and as a bare assignment.
        db.execute(
            "CREATE PROCEDURE assign() AS $$ BEGIN \
               DECLARE a INTEGER DEFAULT 1; \
               DECLARE b INTEGER DEFAULT 0; \
               SET a := a + 1; \
               b := a * 10; \
               INSERT INTO t VALUES (b, 'assigned'); \
             END $$;",
        )
        .unwrap();
        db.execute("CALL assign();").unwrap();
        assert_eq!(
            rows(&mut db, "SELECT b FROM t WHERE a = 20;"),
            vec![vec![Value::Varchar("assigned".to_string())]]
        );
    }

    /// The call's result is the last statement's result; a `SELECT` in the
    /// body produces rows, `RETURN;` keeps them.
    #[test]
    fn call_returns_the_last_result() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE peek(lo INTEGER) AS $$ BEGIN \
               INSERT INTO t VALUES (99, 'peek'); \
               SELECT a, b FROM t WHERE a >= lo ORDER BY a; \
             END $$;",
        )
        .unwrap();
        let (_, statements, result) = called(&mut db, "CALL peek(2);");
        assert_eq!(statements, 2);
        match result {
            Some(QueryResult::Selected { names, types, rows }) => {
                assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
                assert_eq!(types, vec![DataType::Integer, DataType::Varchar]);
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Integer(2), Value::Varchar("y".to_string())],
                        vec![Value::Integer(99), Value::Varchar("peek".to_string())],
                    ]
                );
            }
            other => panic!("expected the SELECT's rows, got {other:?}"),
        }
        // `RETURN;` (no expression) keeps the last statement's result.
        db.execute("CREATE PROCEDURE keep() AS $$ BEGIN SELECT 41 + 1 AS n; RETURN; END $$;")
            .unwrap();
        let (_, statements, result) = called(&mut db, "CALL keep();");
        assert_eq!(statements, 1);
        assert!(matches!(result, Some(QueryResult::Selected { .. })));
    }

    /// SHOW/DESCRIBE/EXPLAIN are ordinary statements inside a body.
    #[test]
    fn utility_statements_run_in_a_body() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE info() AS $$ BEGIN \
               SHOW TABLES; \
               DESCRIBE t; \
             END $$;",
        )
        .unwrap();
        let (_, statements, result) = called(&mut db, "CALL info();");
        assert_eq!(statements, 2);
        match result {
            Some(QueryResult::Selected { names, rows, .. }) => {
                assert_eq!(names[0], "column_name");
                assert!(rows.len() >= 2, "{rows:?}");
            }
            other => panic!("expected DESCRIBE rows, got {other:?}"),
        }
    }

    // ---------- values are bound, never spliced ----------

    /// Injection-shaped parameter values stay values: the classic
    /// `'); DROP TABLE t; --` lands as a string, the table survives, and a
    /// value that looks like SQL is compared as a value.
    #[test]
    fn parameters_bind_as_values_never_sql_text() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE ins(v VARCHAR) AS $$ BEGIN INSERT INTO t VALUES (42, v); END $$;",
        )
        .unwrap();
        let payload = "'); DROP TABLE t; --";
        db.execute("CALL ins('x');").unwrap();
        // Round-trip through a prepared CALL so the payload is bound by the
        // engine's own API as well as written inline as a literal.
        let stmt = db.prepare("CALL ins(?);").unwrap();
        db.execute_prepared(&stmt, &[Value::Varchar(payload.to_string())])
            .unwrap();
        let mut got = rows(&mut db, "SELECT b FROM t WHERE a = 42;");
        got.sort_by_key(|r| r[0].to_string());
        let mut want = vec![
            vec![Value::Varchar("x".to_string())],
            vec![Value::Varchar(payload.to_string())],
        ];
        want.sort_by_key(|r| r[0].to_string());
        assert_eq!(got, want, "the payload is data, not SQL");
        assert!(db.table_in("main", "t").is_ok(), "the table survives");
        // A variable holding the payload is also just a value.
        db.execute(
            "CREATE PROCEDURE ins_var() AS $$ BEGIN \
               DECLARE v VARCHAR DEFAULT '''); DELETE FROM t; --'; \
               INSERT INTO t VALUES (43, v); \
             END $$;",
        )
        .unwrap();
        db.execute("CALL ins_var();").unwrap();
        assert_eq!(
            rows(&mut db, "SELECT b FROM t WHERE a = 43;"),
            vec![vec![Value::Varchar("'); DELETE FROM t; --".to_string())]]
        );
        assert_eq!(
            ints(&mut db, "SELECT COUNT(*) FROM t;"),
            vec![5],
            "two seeded rows plus three inserts"
        );
        // An injection-shaped comparison never changes the statement's shape.
        db.execute(
            "CREATE PROCEDURE find(v VARCHAR) AS $$ BEGIN SELECT a FROM t WHERE b = v; END $$;",
        )
        .unwrap();
        let (_, _, result) = called(&mut db, "CALL find(''' OR 1=1 --');");
        match result {
            Some(QueryResult::Selected { rows, .. }) => assert!(rows.is_empty(), "{rows:?}"),
            other => panic!("expected no rows, got {other:?}"),
        }
    }

    /// A procedure may create a table whose name comes from a value (DDL with
    /// a quoted, parameterized name is not expressible; a value cannot become
    /// an identifier), and the row values are typed by the columns.
    #[test]
    fn values_coerce_to_the_declared_types() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE typed(i INTEGER, s VARCHAR, d DECIMAL(10,2), dt DATE) AS $$ \
             BEGIN INSERT INTO u VALUES (i, s, d, dt); END $$;",
        )
        .unwrap();
        db.execute("CREATE TABLE u (a INTEGER, b VARCHAR, c DECIMAL(10,2), d DATE);")
            .unwrap();
        db.execute("CALL typed(1, 'x', 1.5, DATE '2024-01-02');")
            .unwrap();
        assert_eq!(
            rows(&mut db, "SELECT a, b, c, d FROM u;"),
            vec![vec![
                Value::Integer(1),
                Value::Varchar("x".to_string()),
                Value::Decimal(150, 2),
                Value::Date(19_724),
            ]]
        );
        // A value that cannot coerce to the parameter's type is a clean
        // error, and the call is atomic (nothing of it applied).
        let msg = err(
            &mut db,
            "CALL typed('not a number', 'x', 1.0, DATE '2024-01-02');",
        );
        assert!(msg.contains("argument") && msg.contains('a'), "{msg}");
        assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM u;"), vec![1]);
        // A variable assignment enforces the declared type too.
        db.execute(
            "CREATE PROCEDURE bad_set() AS $$ BEGIN \
               DECLARE n INTEGER DEFAULT 0; \
               SET n = 'nope'; \
               INSERT INTO t VALUES (n, 'never'); \
             END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL bad_set();");
        assert!(msg.contains("SET n") && msg.contains("INTEGER"), "{msg}");
        assert_eq!(
            ints(&mut db, "SELECT COUNT(*) FROM t WHERE b = 'never';"),
            vec![0]
        );
    }

    /// A `NULL` argument stays NULL, and NULL variables are usable in
    /// comparisons and inserts.
    #[test]
    fn nulls_flow_through_parameters_and_variables() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE nul(v INTEGER) AS $$ BEGIN \
             DECLARE x INTEGER; \
             INSERT INTO t VALUES (v, CAST(x AS VARCHAR)); \
             SELECT COUNT(*) AS n FROM t WHERE v = x; \
             END $$;",
        )
        .unwrap();
        db.execute("CALL nul(NULL);").unwrap();
        assert_eq!(
            rows(&mut db, "SELECT a, b FROM t WHERE b IS NULL;"),
            vec![vec![Value::Null, Value::Null]]
        );
        assert_eq!(
            ints(&mut db, "SELECT COUNT(*) FROM t WHERE a IS NULL;"),
            vec![1]
        );
    }

    /// Variables shadow columns: a bare name that matches one is the variable,
    /// and a qualified name (`t.a`) is always the column — in projections and
    /// in predicates alike.
    #[test]
    fn variables_shadow_columns() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE shadow(a INTEGER) AS $$ BEGIN \
               SELECT a AS bare, t.a AS qualified FROM t ORDER BY t.a; \
             END $$;",
        )
        .unwrap();
        let (_, _, result) = called(&mut db, "CALL shadow(7);");
        match result {
            Some(QueryResult::Selected { rows, .. }) => assert_eq!(
                rows,
                vec![
                    vec![Value::Integer(7), Value::Integer(1)],
                    vec![Value::Integer(7), Value::Integer(2)],
                ],
                "the bare name is the variable (constant), `t.a` the column"
            ),
            other => panic!("expected rows, got {other:?}"),
        }
        // The same rule in a predicate: `a = 1` compares the variable, only
        // `t.a = 1` reaches the column.
        db.execute(
            "CREATE PROCEDURE count_a(a INTEGER) AS $$ BEGIN \
               SELECT \
                 (SELECT COUNT(*) FROM t WHERE a = 1) AS via_variable, \
                 (SELECT COUNT(*) FROM t WHERE t.a = 1) AS via_column; \
             END $$;",
        )
        .unwrap();
        let (_, _, result) = called(&mut db, "CALL count_a(7);");
        match result {
            Some(QueryResult::Selected { rows, .. }) => assert_eq!(
                rows,
                vec![vec![Value::Bigint(0), Value::Bigint(1)]],
                "the unqualified name never silently means the column"
            ),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    // ---------- failures and atomicity ----------

    /// `RAISE` aborts with the message, and an autocommit call leaves no
    /// trace: neither the rows it inserted nor the table it created.
    #[test]
    fn raise_aborts_and_autocommit_calls_are_atomic() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE boom(v INTEGER) AS $$ BEGIN \
               CREATE TABLE side (x INTEGER); \
               INSERT INTO side VALUES (v); \
               RAISE 'v must be negative, got ' || CAST(v AS VARCHAR); \
             END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL boom(5);");
        assert!(msg.contains("raised"), "{msg}");
        assert!(msg.contains("v must be negative, got 5"), "{msg}");
        assert!(
            db.table_in("main", "side").is_err(),
            "the call's DDL was rolled back"
        );
        assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM t;"), vec![2]);
        // A failing statement mid-body rolls back everything before it too.
        db.execute(
            "CREATE PROCEDURE half() AS $$ BEGIN \
               INSERT INTO t VALUES (100, 'first'); \
               INSERT INTO t VALUES ('nope', 'second'); \
               INSERT INTO t VALUES (101, 'third'); \
             END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL half();");
        assert!(msg.contains("statement 2"), "{msg}");
        assert_eq!(
            ints(&mut db, "SELECT COUNT(*) FROM t WHERE a IN (100, 101);"),
            vec![0]
        );
        // A RAISE with a non-VARCHAR message is a clear error.
        db.execute("CREATE PROCEDURE bad_raise() AS $$ BEGIN RAISE 42; END $$;")
            .unwrap();
        let msg = err(&mut db, "CALL bad_raise();");
        assert!(msg.contains("VARCHAR"), "{msg}");
        db.execute("CREATE PROCEDURE null_raise() AS $$ BEGIN RAISE NULL; END $$;")
            .unwrap();
        let msg = err(&mut db, "CALL null_raise();");
        assert!(msg.contains("non-NULL"), "{msg}");
        // The catalog is intact after every failure.
        assert!(db.procedure_in("main", "boom").is_ok());
        assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM t;"), vec![2]);
    }

    /// Inside a transaction the call joins it: `ROLLBACK` discards its work,
    /// `COMMIT` keeps it, and a failing call does not roll the transaction
    /// back (the documented behavior — the caller decides).
    #[test]
    fn calls_join_the_surrounding_transaction() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE two() AS $$ BEGIN \
               INSERT INTO t VALUES (50, 'a'); \
               INSERT INTO t VALUES (51, 'b'); \
             END $$;",
        )
        .unwrap();
        db.execute("BEGIN;").unwrap();
        db.execute("CALL two();").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t WHERE a >= 50 ORDER BY a;"),
            vec![50, 51],
            "visible inside the transaction"
        );
        db.execute("ROLLBACK;").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT COUNT(*) FROM t WHERE a >= 50;"),
            vec![0]
        );
        db.execute("BEGIN; CALL two(); COMMIT;").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t WHERE a >= 50 ORDER BY a;"),
            vec![50, 51]
        );
        // A failing call inside a transaction leaves the earlier part; a
        // ROLLBACK discards it.
        db.execute("BEGIN;").unwrap();
        let msg = err(
            &mut db,
            "CREATE PROCEDURE fail_mid() AS $$ BEGIN \
               INSERT INTO t VALUES (60, 'before'); \
               RAISE 'nope'; \
             END $$;",
        );
        assert!(msg.contains("not allowed inside a transaction"), "{msg}");
        db.execute("ROLLBACK;").unwrap();
        // Procedure DDL is refused inside a transaction; CALL is allowed.
        db.execute("BEGIN;").unwrap();
        let msg = err(&mut db, "DROP PROCEDURE two;");
        assert!(
            msg.contains("DROP PROCEDURE") && msg.contains("not allowed"),
            "{msg}"
        );
        db.execute("CALL two();").unwrap();
        db.execute("COMMIT;").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT COUNT(*) FROM t WHERE a >= 50;"),
            vec![4]
        );
    }

    /// A failing call is undone *inside* an open transaction too: its own
    /// partial effects are discarded, while the caller's earlier statements,
    /// the open transaction and (on a durable store) the transaction's
    /// buffered record all survive. The `WORK`/`SAVEPOINT`-style rule is
    /// statement-level, not transaction-level.
    #[test]
    fn a_failing_call_inside_a_transaction_is_undone_and_keeps_it_open() {
        let s = Scratch::new("txnfail");
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (a INTEGER, tag VARCHAR);")
            .unwrap();
        db.execute(
            "CREATE PROCEDURE half(v INTEGER) AS $$ BEGIN \
               INSERT INTO t VALUES (v, 'call'); \
               RAISE 'stop'; \
             END $$;",
        )
        .unwrap();
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'keep');").unwrap();
        let msg = err(&mut db, "CALL half(2);");
        assert!(msg.contains("stop"), "{msg}");
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![1],
            "the call's own insert is undone, the caller's is not"
        );
        // The transaction is still open and can commit; the durable record
        // carries the caller's statement only.
        db.execute("COMMIT;").unwrap();
        drop(db);
        let mut db = durable(&s);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1]);
        assert_eq!(
            rows(&mut db, "SELECT tag FROM t;"),
            vec![vec![Value::Varchar("keep".to_string())]]
        );
        // The same rule through a connection's transaction (its working copy
        // is restored by the call's own snapshot).
        let database = crate::Database::new();
        let mut c = database.connect();
        c.execute("CREATE TABLE u (a INTEGER, tag VARCHAR);")
            .unwrap();
        c.execute(
            "CREATE PROCEDURE half(v INTEGER) AS $$ BEGIN \
               INSERT INTO u VALUES (v, 'call'); \
               RAISE 'stop'; \
             END $$;",
        )
        .unwrap();
        c.execute("BEGIN;").unwrap();
        c.execute("INSERT INTO u VALUES (1, 'keep');").unwrap();
        let msg = c.execute("CALL half(2);").unwrap_err().to_string();
        assert!(msg.contains("stop"), "{msg}");
        c.execute("COMMIT;").unwrap();
        let rows = match c.execute("SELECT a, tag FROM u;").unwrap().pop() {
            Some(QueryResult::Selected { rows, .. }) => rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Varchar("keep".to_string())]]
        );
    }

    /// A call's statements take part in the transaction's read/write set: a
    /// concurrent-looking write through a CALL conflicts like a hand-written
    /// one (pinned here through the connection path's version check).
    #[test]
    fn calls_in_transactions_are_still_serializable() {
        let database = crate::Database::new();
        {
            let mut c = database.connect();
            c.execute("CREATE TABLE t (a INTEGER);").unwrap();
            c.execute("INSERT INTO t VALUES (1);").unwrap();
            c.execute(
                "CREATE PROCEDURE add(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v); END $$;",
            )
            .unwrap();
        }
        let mut c1 = database.connect();
        let mut c2 = database.connect();
        c1.execute("BEGIN;").unwrap();
        c1.execute("CALL add(2);").unwrap();
        // The other connection writes the same table: the first committer wins,
        // the second conflicts (the call's write is part of the write set).
        c2.execute("BEGIN;").unwrap();
        c2.execute("INSERT INTO t VALUES (99);").unwrap();
        c2.execute("COMMIT;").unwrap();
        let msg = c1.execute("COMMIT;").expect_err("conflict").to_string();
        assert!(msg.contains("conflict"), "{msg}");
        c1.execute("ROLLBACK;").ok();
        let mut c3 = database.connect();
        assert_eq!(
            match c3.execute("SELECT a FROM t ORDER BY a;").unwrap().pop() {
                Some(QueryResult::Selected { rows, .. }) => rows,
                other => panic!("{other:?}"),
            },
            vec![vec![Value::Integer(1)], vec![Value::Integer(99)]]
        );
    }

    // ---------- names, catalogs, databases ----------

    /// Procedures are per database: `CALL db.p()` runs the body against that
    /// database and an unqualified call resolves in the session's own.
    #[test]
    fn procedures_are_per_database() {
        let mut db = seeded();
        db.execute("CREATE DATABASE other; USE other;").unwrap();
        db.execute("CREATE TABLE u (x INTEGER);").unwrap();
        db.execute("CREATE PROCEDURE p(v INTEGER) AS $$ BEGIN INSERT INTO u VALUES (v); END $$;")
            .unwrap();
        db.execute("CALL p(1);").unwrap();
        assert_eq!(ints(&mut db, "SELECT x FROM u;"), vec![1]);
        db.execute("USE main;").unwrap();
        // The session's `main` has no such procedure.
        let msg = err(&mut db, "CALL p(2);");
        assert!(msg.contains("p") && msg.contains("main"), "{msg}");
        // Qualified: runs against `other`.
        db.execute("CALL other.p(3);").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT x FROM other.u ORDER BY x;"),
            vec![1, 3]
        );
        // ... while a body statement resolves in the procedure's own database:
        // `t` exists only in `main`, so `other.fail()` cannot see it.
        db.execute("USE other;").unwrap();
        db.execute(
            "CREATE PROCEDURE see_t(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'x'); END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL see_t(1);");
        assert!(msg.contains("t"), "{msg}");
        // Same name, two databases, two definitions.
        db.execute("USE main;").unwrap();
        db.execute("CREATE PROCEDURE p() AS $$ BEGIN INSERT INTO t VALUES (77, 'main'); END $$;")
            .unwrap();
        db.execute("CALL p();").unwrap();
        assert_eq!(
            rows(&mut db, "SELECT b FROM t WHERE a = 77;"),
            vec![vec![Value::Varchar("main".to_string())]]
        );
    }

    /// Quoted/odd procedure names survive creation, `CALL`, the WAL record
    /// and a reopen.
    #[test]
    fn quoted_names_round_trip() {
        let s = Scratch::new("quoted");
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute(
            "CREATE PROCEDURE \"we\"\"ird proc\"(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v); END $$;",
        )
        .unwrap();
        db.execute("CALL \"we\"\"ird proc\"(5);").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![5]);
        drop(db);
        let mut db = durable(&s);
        db.execute("CALL \"we\"\"ird proc\"(6);").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![5, 6]);
    }

    /// A procedure shares the table/view/materialized-view name space at
    /// creation time, in one direction.
    #[test]
    fn name_conflicts_are_reported() {
        let mut db = seeded();
        let msg = err(
            &mut db,
            "CREATE PROCEDURE t() AS $$ BEGIN SELECT 1; END $$;",
        );
        assert!(msg.contains("table") && msg.contains("t"), "{msg}");
        db.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        let msg = err(
            &mut db,
            "CREATE PROCEDURE v() AS $$ BEGIN SELECT 1; END $$;",
        );
        assert!(msg.contains("view") && msg.contains("v"), "{msg}");
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT a FROM t;")
            .unwrap();
        let msg = err(
            &mut db,
            "CREATE PROCEDURE mv() AS $$ BEGIN SELECT 1; END $$;",
        );
        assert!(
            msg.contains("materialized view") && msg.contains("mv"),
            "{msg}"
        );
        // `SHOW TABLES` never lists a procedure, and a procedure name may
        // still be used as a table name.
        db.execute("CREATE PROCEDURE q() AS $$ BEGIN SELECT 1; END $$;")
            .unwrap();
        assert!(db.execute("CREATE TABLE q (x INTEGER);").is_ok());
        assert_eq!(
            rows(&mut db, "SHOW TABLES;")
                .into_iter()
                .filter(|r| r[0] == Value::Varchar("q".to_string()))
                .count(),
            1
        );
    }

    // ---------- parsing and validation ----------

    /// Every malformed definition is refused at CREATE time with a message
    /// naming the place, and nothing is stored.
    #[test]
    fn body_shape_errors_are_clear() {
        let mut db = seeded();
        let cases: &[(&str, &str)] = &[
            ("CREATE PROCEDURE p(x INTEGER) AS $$ SELECT 1; $$;", "BEGIN"),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN SELECT 1; $$;",
                "not closed",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN IF true THEN SELECT 1; END $$;",
                "END IF",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN WHILE true DO SELECT 1; END WHILE END WHILE; $$;",
                "after",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN SELECT 1; END; SELECT 2; $$;",
                "after",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN SET x = 1; END $$;",
                "not declared",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN DECLARE x BOGUS; SELECT 1; END $$;",
                "not a type",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN DECLARE x INTEGER; DECLARE x INTEGER; SELECT 1; END $$;",
                "twice",
            ),
            (
                "CREATE PROCEDURE p(x INTEGER, X INTEGER) AS $$ BEGIN SELECT 1; END $$;",
                "twice",
            ),
            (
                "CREATE PROCEDURE p(x INTEGER) AS $$ BEGIN SELECT ?; END $$;",
                "not supported",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN CREATE DATABASE d; END $$;",
                "CREATE DATABASE",
            ),
            ("CREATE PROCEDURE p() AS $$ BEGIN USE main; END $$;", "USE"),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN CHECKPOINT; END $$;",
                "CHECKPOINT",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN CREATE PROCEDURE q() AS 'BEGIN SELECT 1; END'; END $$;",
                "CREATE PROCEDURE",
            ),
            (
                "CREATE PROCEDURE p() AS $$ BEGIN INSERT INTO t VALUES (1) END $$;",
                "parse error",
            ),
        ];
        for (sql, expected) in cases {
            let msg = err(&mut db, sql);
            assert!(
                msg.contains(expected),
                "for {sql}\n  expected `{expected}` in: {msg}"
            );
            assert!(
                db.procedure_in("main", "p").is_err(),
                "nothing stored for {sql}"
            );
        }
        // A condition's type is checked when the statement binds — at CALL
        // time — because the body rebinds against the catalog every call.
        db.execute("CREATE PROCEDURE cond() AS $$ BEGIN IF 1 THEN SELECT 1; END IF; END $$;")
            .unwrap();
        let msg = err(&mut db, "CALL cond();");
        assert!(msg.contains("BOOLEAN"), "{msg}");
        db.execute(
            "CREATE PROCEDURE cond2() AS $$ BEGIN WHILE 'x' DO SELECT 1; END WHILE; END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL cond2();");
        assert!(msg.contains("BOOLEAN"), "{msg}");
        // The same checks run for a `CREATE OR REPLACE` (a bad definition must
        // not replace a good one).
        db.execute("CREATE PROCEDURE keep() AS $$ BEGIN SELECT 1; END $$;")
            .unwrap();
        let msg = err(
            &mut db,
            "CREATE OR REPLACE PROCEDURE keep() AS $$ BEGIN SELECT 1; $$;",
        );
        assert!(msg.contains("not closed"), "{msg}");
        assert!(db.procedure_in("main", "keep").is_ok());
    }

    /// A body that nests blocks deeper than the cap is refused cleanly (the
    /// recursive parser must never overflow the stack).
    #[test]
    fn deep_block_nesting_is_refused() {
        let mut db = PinkDb::new();
        let mut body = String::from("BEGIN ");
        for _ in 0..(MAX_BODY_NESTING + 4) {
            body.push_str("BEGIN ");
        }
        for _ in 0..(MAX_BODY_NESTING + 4) {
            body.push_str("END; ");
        }
        body.push_str("END");
        let msg = err(
            &mut db,
            &format!("CREATE PROCEDURE deep() AS $$ {body} $$;"),
        );
        assert!(msg.contains("too deep"), "{msg}");
    }

    /// `CALL` arity and argument-position errors surface at bind time; the
    /// arguments are constant expressions.
    #[test]
    fn call_arguments_are_checked() {
        let mut db = seeded();
        db.execute("CREATE PROCEDURE p(a INTEGER, b VARCHAR) AS $$ BEGIN SELECT 1; END $$;")
            .unwrap();
        let msg = err(&mut db, "CALL p(1);");
        assert!(
            msg.contains("takes 2") && msg.contains("1 were given"),
            "{msg}"
        );
        let msg = err(&mut db, "CALL p(1, 'x', 3);");
        assert!(msg.contains("takes 2"), "{msg}");
        let msg = err(&mut db, "CALL p(a, 'x');");
        assert!(msg.contains("no column"), "{msg}");
        // Constant expressions (arithmetic, functions, subqueries) bind.
        db.execute(
            "CREATE PROCEDURE sums(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'sum'); END $$;",
        )
        .unwrap();
        db.execute("CALL sums(1 + 2 * 3);").unwrap();
        // The scalar subquery reads the table *before* the insert, so the
        // second call inserts the current maximum.
        db.execute("CALL sums((SELECT max(a) FROM t));").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t WHERE b = 'sum' ORDER BY a;"),
            vec![7, 7]
        );
        // A prepared CALL binds its `?` like any other statement.
        let stmt = db.prepare("CALL sums(?);").unwrap();
        db.execute_prepared(&stmt, &[Value::Integer(11)]).unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t WHERE b = 'sum' ORDER BY a;"),
            vec![7, 7, 11]
        );
        // `*` is not an argument.
        let msg = err(&mut db, "CALL p(*, 'x');");
        assert!(!msg.is_empty());
    }

    /// Recursion is allowed but bounded; the error names the limit.
    #[test]
    fn recursion_is_bounded() {
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE down(n INTEGER) AS $$ BEGIN \
               IF n > 0 THEN CALL down(n - 1); END IF; \
             END $$;",
        )
        .unwrap();
        db.execute("CALL down(5);").unwrap();
        let msg = err(&mut db, "CALL down(1000);");
        assert!(msg.contains("depth exceeded"), "{msg}");
        assert!(msg.contains(&MAX_CALL_DEPTH.to_string()), "{msg}");
        // Mutual recursion is bounded the same way, and the engine stays
        // usable afterwards.
        db.execute("CREATE PROCEDURE ping(n INTEGER) AS $$ BEGIN CALL pong(n - 1); END $$;")
            .unwrap();
        db.execute("CREATE PROCEDURE pong(n INTEGER) AS $$ BEGIN CALL ping(n - 1); END $$;")
            .unwrap();
        let msg = err(&mut db, "CALL ping(200);");
        assert!(msg.contains("depth exceeded"), "{msg}");
        assert_eq!(ints(&mut db, "SELECT COUNT(*) FROM t;"), vec![2]);
    }

    /// A nested call is an ordinary statement of the outer body: its rows and
    /// its writes are the outer call's.
    #[test]
    fn nested_calls_work_and_report_their_statements() {
        let mut db = seeded();
        db.execute("CREATE PROCEDURE leaf(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'leaf'); END $$;")
            .unwrap();
        db.execute(
            "CREATE PROCEDURE outer_proc() AS $$ BEGIN \
               CALL leaf(10); \
               CALL leaf(11); \
               SELECT a FROM t WHERE b = 'leaf' ORDER BY a; \
             END $$;",
        )
        .unwrap();
        let (name, statements, result) = called(&mut db, "CALL outer_proc();");
        assert_eq!(name, "outer_proc");
        assert_eq!(statements, 3, "the two nested calls plus the SELECT");
        match result {
            Some(QueryResult::Selected { rows, .. }) => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Integer(10)], vec![Value::Integer(11)]]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// A JavaScript definition that reached the catalog (a store written by a
    /// build with the sandbox): a build without `js` keeps it inert and
    /// reports the refusal at `CALL`; a `js` build runs it (wave 81). It is
    /// never executed as SQL.
    #[test]
    fn javascript_definitions_refuse_at_call_time_too() {
        let mut db = PinkDb::new();
        db.procedures_insert_in(
            "main",
            "js_def".to_string(),
            ProcedureDef {
                name: "js_def".to_string(),
                params: vec![("a".to_string(), DataType::Bigint)],
                language: ProcLanguage::JavaScript,
                source: "return a + 1;".to_string(),
                body: None,
            },
        )
        .unwrap();
        if cfg!(feature = "js") {
            let (_, _, result) = called(&mut db, "CALL js_def(1);");
            assert_eq!(
                result,
                Some(QueryResult::Selected {
                    names: vec!["return".to_string()],
                    types: vec![DataType::Integer],
                    rows: vec![vec![Value::Integer(2)]],
                })
            );
        } else {
            let msg = err(&mut db, "CALL js_def(1);");
            assert!(
                msg.contains("`js`") && msg.contains("--features js"),
                "{msg}"
            );
        }
        // The definition survives (a later build with the engine can run it).
        assert!(db.procedure_in("main", "js_def").is_ok());
        // A SQL definition loaded without a parsed body cannot run either, and
        // says so instead of guessing.
        db.procedures_insert_in(
            "main",
            "bodyless".to_string(),
            ProcedureDef {
                name: "bodyless".to_string(),
                params: Vec::new(),
                language: ProcLanguage::Sql,
                source: "BEGIN END".to_string(),
                body: None,
            },
        )
        .unwrap();
        let msg = err(&mut db, "CALL bodyless();");
        assert!(msg.contains("no body"), "{msg}");
    }

    /// The parser's nesting/depth guards still apply *inside* a body: the
    /// dollar-quoted text is skipped by the pre-parse estimate (so its
    /// semicolons and parens are the body parser's business), and each body
    /// statement re-runs the ordinary guards on its own text.
    #[test]
    fn body_nesting_guards_still_apply() {
        let mut db = PinkDb::new();
        // A moderately nested expression is fine.
        let ok = format!(
            "CREATE PROCEDURE ok_nest() AS $$ BEGIN SELECT {}1{}; END $$;",
            "(".repeat(10),
            ")".repeat(10)
        );
        db.execute(&ok).unwrap();
        db.execute("CALL ok_nest();").unwrap();
        // Far too deep is refused (the pre-parse estimate would have rejected
        // it *before* the body parser even ran, were it not skipped).
        let deep = format!(
            "CREATE PROCEDURE bad_nest() AS $$ BEGIN SELECT {}1{}; END $$;",
            "(".repeat(crate::sql::MAX_NESTING_DEPTH * 2),
            ")".repeat(crate::sql::MAX_NESTING_DEPTH * 2)
        );
        let msg = err(&mut db, &deep);
        assert!(msg.contains("nesting too deep"), "{msg}");
        // A deeply nested condition is refused by the same guard (the body's
        // own expression parse runs the ordinary pre-parse estimate).
        let cond = format!(
            "IF {}1{} > 0 THEN SELECT 1; END IF;",
            "(".repeat(crate::sql::MAX_NESTING_DEPTH * 2),
            ")".repeat(crate::sql::MAX_NESTING_DEPTH * 2)
        );
        let msg = err(
            &mut db,
            &format!("CREATE PROCEDURE deep_cond() AS $$ BEGIN {cond} END $$;"),
        );
        assert!(msg.contains("nesting too deep"), "{msg}");
    }

    /// A JavaScript definition is refused in a build without the `js` feature
    /// (nothing is stored); a `js` build stores it (wave 81).
    #[test]
    fn javascript_definitions_are_refused_cleanly() {
        let mut db = PinkDb::new();
        if cfg!(feature = "js") {
            db.execute("CREATE PROCEDURE js1() LANGUAGE JAVASCRIPT AS $$ return 1; $$;")
                .unwrap();
            assert!(db.procedure_in("main", "js1").is_ok());
        } else {
            let msg = err(
                &mut db,
                "CREATE PROCEDURE js1() LANGUAGE JAVASCRIPT AS $$ return 1; $$;",
            );
            assert!(msg.contains("`js`"), "{msg}");
            assert!(msg.contains("--features js"), "{msg}");
            assert!(db.procedure_in("main", "js1").is_err());
        }
        // The `LANGUAGE` clause may also follow the body, and the SQL
        // language may be written explicitly.
        db.execute("CREATE PROCEDURE ok() AS $$ BEGIN SELECT 1; END $$ LANGUAGE SQL;")
            .unwrap();
        db.execute("CREATE PROCEDURE ok2() LANGUAGE SQL AS $$ BEGIN SELECT 1; END $$;")
            .unwrap();
        let msg = err(
            &mut db,
            "CREATE PROCEDURE ok3() LANGUAGE COBOL AS $$ BEGIN SELECT 1; END $$;",
        );
        assert!(msg.contains("SQL or JAVASCRIPT"), "{msg}");
    }

    // ---------- persistence and the WAL ----------

    /// A checkpoint (`.pink`) carries the definitions: save/load and durable
    /// reopen both replay them exactly.
    #[test]
    fn persistence_round_trips_definitions() {
        let s = Scratch::new("save");
        let mut db = seeded();
        db.execute(
            "CREATE PROCEDURE save_me(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'saved'); END $$;",
        )
        .unwrap();
        let path = s.other("plain.pink");
        db.save(&path).unwrap();
        let mut loaded = PinkDb::open(&path).unwrap();
        db.execute("CALL save_me(1);").unwrap();
        loaded.execute("CALL save_me(2);").unwrap();
        assert_eq!(
            ints(&mut loaded, "SELECT a FROM t WHERE b = 'saved';"),
            vec![2]
        );
        // A procedure in a non-default database rides the same section.
        db.execute("CREATE DATABASE other;").unwrap();
        db.execute("CREATE PROCEDURE other.side(v INTEGER) AS $$ BEGIN SELECT v; END $$;")
            .unwrap();
        db.save(&path).unwrap();
        let mut loaded = PinkDb::open(&path).unwrap();
        assert!(loaded.procedure_in("other", "side").is_ok());
        loaded.execute("CALL other.side(1);").unwrap();
        // The flags word carries the procedure section for a store that has
        // one and stays 0 without.
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            crate::persist::FLAG_PROCEDURES_FOR_TEST,
            "a procedure store sets flags bit 4"
        );
        let plain = Scratch::new("save_plain");
        let plain_path = plain.other("plain.pink");
        let mut empty = PinkDb::new();
        empty.execute("CREATE TABLE x (a INTEGER);").unwrap();
        empty.save(&plain_path).unwrap();
        let bytes = std::fs::read(&plain_path).unwrap();
        assert_eq!(
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            0,
            "a procedure-free store keeps flags 0"
        );
    }

    /// Durable reopen: the WAL-only path (no checkpoint) and the checkpoint +
    /// tail path both land on the live state, and the replayed definitions
    /// are live (a further call writes through them).
    #[test]
    fn durable_replay_reproduces_calls_exactly() {
        let s = Scratch::new("replay");
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (a INTEGER, tag VARCHAR);")
            .unwrap();
        db.execute(
            "CREATE PROCEDURE ins(v INTEGER) AS $$ BEGIN \
               DECLARE next INTEGER DEFAULT v; \
               INSERT INTO t VALUES (next, 'ins'); \
             END $$;",
        )
        .unwrap();
        db.execute("CALL ins(1);").unwrap();
        db.execute("CALL ins(2);").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        drop(db);
        // WAL-only replay.
        let mut db = durable(&s);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        assert!(db.procedure_in("main", "ins").is_ok());
        db.execute("CALL ins(3);").unwrap();
        // Checkpoint, then more calls: the checkpoint carries the definitions
        // (and the data) and the WAL tail replays on top of it.
        db.execute("CHECKPOINT;").unwrap();
        db.execute("CALL ins(4);").unwrap();
        db.execute("CREATE PROCEDURE ins2(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'ins2'); END $$;")
            .unwrap();
        db.execute("CALL ins2(5);").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3, 4, 5]
        );
        drop(db);
        let mut db = durable(&s);
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3, 4, 5]
        );
        assert!(db.procedure_in("main", "ins2").is_ok());
        db.execute("CALL ins2(6);").unwrap();
        assert_eq!(
            rows(&mut db, "SELECT tag FROM t WHERE a = 6;"),
            vec![vec![Value::Varchar("ins2".to_string())]]
        );
        // Dropping a procedure replays too.
        db.execute("DROP PROCEDURE ins;").unwrap();
        db.execute("CALL ins2(7);").unwrap();
        drop(db);
        let mut db = durable(&s);
        assert!(db.procedure_in("main", "ins").is_err());
        let msg = err(&mut db, "CALL ins(8);");
        assert!(msg.contains("not found"), "{msg}");
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3, 4, 5, 6, 7]
        );
    }

    /// A call in an explicit transaction replays through the transaction's
    /// one record (the `CALL` text rides the same record as its writes).
    #[test]
    fn durable_transaction_replay_includes_calls() {
        let s = Scratch::new("txn");
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("CREATE PROCEDURE two() AS $$ BEGIN INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); END $$;")
            .unwrap();
        db.execute("BEGIN; CALL two(); INSERT INTO t VALUES (3); COMMIT;")
            .unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
        drop(db);
        let mut db = durable(&s);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
        assert!(db.procedure_in("main", "two").is_ok());
    }

    /// A durable store refuses a procedure that reads a session-local view
    /// (the logged call could not replay); in memory the same call works.
    #[test]
    fn durable_calls_refuse_session_local_views() {
        let s = Scratch::new("view");
        let mut db = durable(&s);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        db.execute("CREATE PROCEDURE reads_view() AS $$ BEGIN SELECT a FROM v; END $$;")
            .unwrap();
        let msg = err(&mut db, "CALL reads_view();");
        assert!(
            msg.contains("session-local view") && msg.contains('v'),
            "{msg}"
        );
        // A statement of the body that writes through a view is refused by the
        // binder's own rule as well.
        db.execute(
            "CREATE PROCEDURE writes_view() AS $$ BEGIN CREATE TABLE u AS SELECT a FROM v; END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL writes_view();");
        assert!(msg.contains("view"), "{msg}");
        // In memory the same procedure is fine (nothing is logged).
        let mut mem = PinkDb::new();
        mem.execute("CREATE TABLE t (a INTEGER);").unwrap();
        mem.execute("INSERT INTO t VALUES (1);").unwrap();
        mem.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        mem.execute("CREATE PROCEDURE reads_view() AS $$ BEGIN SELECT a FROM v; END $$;")
            .unwrap();
        let (_, _, result) = called(&mut mem, "CALL reads_view();");
        assert!(
            matches!(result, Some(QueryResult::Selected { .. })),
            "{result:?}"
        );
    }

    /// Two handles on one durable store: one's call reaches the other through
    /// the ordinary WAL refresh protocol.
    #[test]
    fn a_second_open_replays_calls() {
        let s = Scratch::new("two");
        let mut a = durable(&s);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        a.execute("CREATE PROCEDURE ins(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v); END $$;")
            .unwrap();
        let mut b = durable(&s);
        a.execute("CALL ins(1);").unwrap();
        assert_eq!(ints(&mut b, "SELECT a FROM t;"), vec![1]);
        b.execute("CALL ins(2);").unwrap();
        assert_eq!(ints(&mut a, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        // A procedure created by one handle reaches the other.
        a.execute(
            "CREATE PROCEDURE ins3(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v * 3); END $$;",
        )
        .unwrap();
        b.execute("CALL ins3(10);").unwrap();
        assert_eq!(ints(&mut a, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 30]);
    }

    /// An empty body is a valid no-op call; `RETURN` inside a `WHILE` (and a
    /// nested block) unwinds the whole call; table DDL and `INSERT … SELECT`
    /// are ordinary body statements.
    #[test]
    fn empty_bodies_returns_and_ddl() {
        let mut db = seeded();
        db.execute("CREATE PROCEDURE nothing() AS $$ BEGIN END $$;")
            .unwrap();
        assert_eq!(
            last(&mut db, "CALL nothing();"),
            QueryResult::Called {
                name: "nothing".to_string(),
                statements: 0,
                result: None,
                output: Vec::new(),
            }
        );
        // `RETURN` inside a loop unwinds the whole call, and the returned
        // value is a one-row result named `return`.
        db.execute(
            "CREATE PROCEDURE first_above(lo INTEGER) AS $$ BEGIN \
               DECLARE n INTEGER DEFAULT 0; \
               WHILE n < 100 DO \
                 IF n > lo THEN RETURN n; END IF; \
                 SET n = n + 1; \
               END WHILE; \
               RETURN -1; \
             END $$;",
        )
        .unwrap();
        for (lo, want) in [(5, 6i64), (20, 21), (200, -1)] {
            let (_, _, result) = called(&mut db, &format!("CALL first_above({lo});"));
            match result {
                Some(QueryResult::Selected { rows, .. }) => {
                    assert_eq!(rows, vec![vec![Value::Integer(want as i32)]], "lo={lo}");
                }
                other => panic!("expected a returned value, got {other:?}"),
            }
        }
        // DDL and `INSERT … SELECT` inside a body.
        db.execute(
            "CREATE PROCEDURE archive(cut INTEGER) AS $$ BEGIN \
               CREATE TABLE kept AS SELECT a, b FROM t WHERE a <= cut; \
               DROP TABLE t; \
               CREATE TABLE t2 (n BIGINT); \
               INSERT INTO t2 SELECT COUNT(*) FROM kept; \
               SELECT n FROM t2; \
             END $$;",
        )
        .unwrap();
        let (_, statements, result) = called(&mut db, "CALL archive(1);");
        assert_eq!(statements, 5);
        assert!(
            matches!(result, Some(QueryResult::Selected { .. })),
            "{result:?}"
        );
        assert_eq!(ints(&mut db, "SELECT n FROM t2;"), vec![1]);
        assert_eq!(
            rows(&mut db, "SELECT b FROM kept;"),
            vec![vec![Value::Varchar("x".to_string())]]
        );
        // A failed call rolled the whole thing back (the tables exist from
        // the successful call; `t` is gone, `kept`/`t2` are not).
        assert!(db.table_in("main", "t").is_err());
        assert!(db.table_in("main", "kept").is_ok());
        assert!(db.table_in("main", "t2").is_ok());
    }

    /// The connection (server) path: `CALL` through a `Connection`, inside and
    /// outside transactions, `SHOW TABLES` untouched.
    #[test]
    fn connection_path_calls_work() {
        let database = crate::Database::new();
        {
            let mut c = database.connect();
            c.execute("CREATE TABLE t (a INTEGER);").unwrap();
            c.execute(
                "CREATE PROCEDURE ins(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v); END $$;",
            )
            .unwrap();
            c.execute("CALL ins(1);").unwrap();
        }
        let mut c = database.connect();
        let results = c.execute("CALL ins(2);").unwrap();
        assert!(matches!(results[0], QueryResult::Called { .. }));
        c.execute("BEGIN;").unwrap();
        c.execute("CALL ins(3);").unwrap();
        c.execute("ROLLBACK;").unwrap();
        let rows = match c.execute("SELECT a FROM t ORDER BY a;").unwrap().pop() {
            Some(QueryResult::Selected { rows, .. }) => rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }
}
