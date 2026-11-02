//! Parser for DuckDB's sqllogictest dialect.
//!
//! Design ported from DuckDB's `test/sqlite/sqllogic_parser.cpp` and the
//! directive dispatch in `sqllogic_test_runner.cpp` (MIT licensed), without
//! copying its code: a file is a sequence of blocks separated by blank lines,
//! a directive tokenizes on whitespace, then an optional SQL block and an
//! optional `----` result block. The one deliberate difference is that an
//! unknown directive is a *file skip* here, not a parser failure, so a newer
//! DuckDB test that uses a directive we do not model does not count against
//! the compatibility rate.

use std::fmt;
use std::time::Duration;

/// A comparison in `skipif i=2` / `onlyif i<>2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CondOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    /// A system name (`duckdb`) when `op` is `None`, else a loop variable.
    pub key: String,
    pub op: Option<CondOp>,
    pub value: String,
    pub skip_if: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementExpect {
    Ok,
    Error,
    Maybe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortMode {
    #[default]
    NoSort,
    RowSort,
    ValueSort,
}

impl SortMode {
    /// DuckDB accepts aliases for the two useful modes (`test_config.cpp`,
    /// `TryParseSortStyle`).
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "nosort" | "none" => Some(Self::NoSort),
            "rowsort" | "sort" => Some(Self::RowSort),
            "valuesort" => Some(Self::ValueSort),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Statement {
    pub expect: StatementExpect,
    pub connection: Option<String>,
    pub sql: String,
    /// The `----` block, joined with `\n`; empty means "any error".
    pub expected_error: String,
}

#[derive(Debug, Clone)]
pub struct Query {
    /// The `T`/`I`/`R` type string; its length is the expected column count.
    pub types: String,
    pub sort: SortMode,
    pub connection: Option<String>,
    pub label: Option<String>,
    pub sql: String,
    pub expected: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum NodeKind {
    Statement(Statement),
    Query(Query),
    Require {
        name: String,
        arg: Option<String>,
    },
    RequireEnv {
        name: String,
        value: Option<String>,
    },
    ModeSkip {
        reason: String,
    },
    ModeUnskip,
    Halt,
    Sleep(Duration),
    Load {
        path: String,
        read_only: bool,
    },
    Restart,
    SetVariable {
        name: String,
        value: String,
    },
    ResetLabel {
        name: String,
    },
    Loop {
        var: String,
        values: Vec<String>,
        concurrent: bool,
        body: Vec<Node>,
    },
}

#[derive(Debug, Clone)]
pub struct Node {
    /// 1-based line of the directive.
    pub line: usize,
    pub conditions: Vec<Condition>,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// The file uses something this runner does not model; the whole file is
    /// skipped with this text as the reason.
    Unsupported(String),
    /// The file is malformed; this is a failure, not a skip.
    Syntax { line: usize, message: String },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(name) => write!(f, "unsupported directive: {name}"),
            Self::Syntax { line, message } => write!(f, "parse error at line {line}: {message}"),
        }
    }
}

pub fn parse_script(src: &str) -> Result<Vec<Node>, ParseError> {
    let lines: Vec<&str> = src
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    let mut parser = Parser {
        lines,
        pos: 0,
        conditions: Vec::new(),
    };
    let nodes = parser.block(false)?;
    if !parser.conditions.is_empty() {
        return Err(ParseError::Syntax {
            line: parser.lines.len().max(1),
            message: "dangling skipif/onlyif".to_string(),
        });
    }
    Ok(nodes)
}

struct Parser<'a> {
    lines: Vec<&'a str>,
    pos: usize,
    conditions: Vec<Condition>,
}

impl<'a> Parser<'a> {
    fn block(&mut self, in_loop: bool) -> Result<Vec<Node>, ParseError> {
        let mut nodes = Vec::new();
        loop {
            while self.pos < self.lines.len() {
                let line = self.lines[self.pos];
                if line.is_empty() || line.starts_with('#') {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            if self.pos >= self.lines.len() {
                if in_loop {
                    return Err(self.syntax(self.lines.len().max(1), "missing endloop"));
                }
                return Ok(nodes);
            }
            let line_no = self.pos + 1;
            let line = self.lines[self.pos];
            self.pos += 1;
            let tokens: Vec<String> = line.split_whitespace().map(str::to_string).collect();
            let directive = tokens[0].as_str();
            match directive {
                "skipif" | "onlyif" => {
                    let skip_if = directive == "skipif";
                    self.conditions
                        .extend(parse_conditions(&tokens[1..], skip_if, line_no)?);
                }
                "endloop" => {
                    if in_loop {
                        return Ok(nodes);
                    }
                    return Err(self.syntax(line_no, "endloop without loop"));
                }
                "statement" => {
                    let expect = match tokens.get(1).map(String::as_str) {
                        Some("ok") => StatementExpect::Ok,
                        Some("error") => StatementExpect::Error,
                        Some("maybe") => StatementExpect::Maybe,
                        _ => {
                            return Err(
                                self.syntax(line_no, "statement requires ok, error or maybe")
                            );
                        }
                    };
                    let connection = tokens.get(2).cloned();
                    let sql = self.read_sql();
                    let (expected_error, _) = self.read_expected();
                    let expected_error = expected_error.join("\n");
                    if expect == StatementExpect::Ok && !expected_error.is_empty() {
                        return Err(self.syntax(
                            line_no,
                            "only statement error or maybe can have an expected error block",
                        ));
                    }
                    nodes.push(self.node(
                        line_no,
                        NodeKind::Statement(Statement {
                            expect,
                            connection,
                            sql,
                            expected_error,
                        }),
                    ));
                }
                "query" => {
                    let types = tokens
                        .get(1)
                        .ok_or_else(|| self.syntax(line_no, "query requires a type string"))?
                        .clone();
                    if !types.chars().all(|c| matches!(c, 'T' | 'I' | 'R')) {
                        return Err(self.syntax(line_no, "query types must be a string of T/I/R"));
                    }
                    let mut sort = SortMode::NoSort;
                    let mut connection = None;
                    let mut label = None;
                    if let Some(token) = tokens.get(2) {
                        if let Some(parsed) = SortMode::from_token(token) {
                            sort = parsed;
                        } else {
                            connection = Some(token.clone());
                        }
                        label = tokens.get(3).cloned();
                    }
                    let sql = self.read_sql();
                    let (expected, _) = self.read_expected();
                    nodes.push(self.node(
                        line_no,
                        NodeKind::Query(Query {
                            types,
                            sort,
                            connection,
                            label,
                            sql,
                            expected,
                        }),
                    ));
                }
                "require" => {
                    let name = tokens
                        .get(1)
                        .ok_or_else(|| self.syntax(line_no, "require needs a name"))?
                        .clone();
                    let arg = tokens.get(2).cloned();
                    nodes.push(self.node(line_no, NodeKind::Require { name, arg }));
                }
                "require-env" => {
                    let name = tokens
                        .get(1)
                        .ok_or_else(|| self.syntax(line_no, "require-env needs a name"))?
                        .clone();
                    let value = tokens.get(2).cloned();
                    nodes.push(self.node(line_no, NodeKind::RequireEnv { name, value }));
                }
                "mode" => match tokens.get(1).map(String::as_str) {
                    Some("skip") => {
                        let reason = tokens.get(2..).unwrap_or_default().join(" ");
                        nodes.push(self.node(
                            line_no,
                            NodeKind::ModeSkip {
                                reason: if reason.is_empty() {
                                    "mode skip".to_string()
                                } else {
                                    reason
                                },
                            },
                        ));
                    }
                    Some("unskip") => nodes.push(self.node(line_no, NodeKind::ModeUnskip)),
                    Some(other) => {
                        return Err(ParseError::Unsupported(format!("mode {other}")));
                    }
                    None => return Err(self.syntax(line_no, "mode needs an argument")),
                },
                "halt" => nodes.push(self.node(line_no, NodeKind::Halt)),
                "sleep" => {
                    if tokens.len() != 3 {
                        return Err(self.syntax(line_no, "sleep requires <n> <unit>"));
                    }
                    let count: u64 = tokens[1]
                        .parse()
                        .map_err(|_| self.syntax(line_no, "sleep count is not a number"))?;
                    let unit = match tokens[2].as_str() {
                        "second" | "seconds" | "sec" => Duration::from_secs(count),
                        "millisecond" | "milliseconds" | "milli" => Duration::from_millis(count),
                        "microsecond" | "microseconds" | "micro" => Duration::from_micros(count),
                        "nanosecond" | "nanoseconds" | "nano" => Duration::from_nanos(count),
                        other => {
                            return Err(self.syntax(line_no, format!("unknown sleep unit {other}")));
                        }
                    };
                    nodes.push(self.node(line_no, NodeKind::Sleep(unit)));
                }
                "load" => {
                    let path = tokens.get(1).cloned().unwrap_or_default();
                    let read_only = match tokens.get(2).map(|s| s.to_ascii_lowercase()) {
                        None => false,
                        Some(mode) if mode == "readonly" => true,
                        Some(mode) if mode == "readwrite" => false,
                        Some(mode) => {
                            return Err(ParseError::Unsupported(format!("load {mode}")));
                        }
                    };
                    nodes.push(self.node(line_no, NodeKind::Load { path, read_only }));
                }
                "restart" => nodes.push(self.node(line_no, NodeKind::Restart)),
                "set" => {
                    if tokens.get(1).map(String::as_str) != Some("variable") {
                        // DuckDB also has `set seed ...` and
                        // `set ignore_error_messages`; neither is modelled.
                        return Err(ParseError::Unsupported(format!(
                            "set {}",
                            tokens.get(1).map(String::as_str).unwrap_or("")
                        )));
                    }
                    let name = tokens
                        .get(2)
                        .ok_or_else(|| self.syntax(line_no, "set variable needs a name"))?
                        .clone();
                    let value = tokens.get(3..).unwrap_or_default().join(" ");
                    nodes.push(self.node(line_no, NodeKind::SetVariable { name, value }));
                }
                "reset" => {
                    if tokens.get(1).map(String::as_str) != Some("label") {
                        return Err(ParseError::Unsupported(format!(
                            "reset {}",
                            tokens.get(1).map(String::as_str).unwrap_or("")
                        )));
                    }
                    let name = tokens
                        .get(2)
                        .ok_or_else(|| self.syntax(line_no, "reset label needs a name"))?
                        .clone();
                    nodes.push(self.node(line_no, NodeKind::ResetLabel { name }));
                }
                "loop" | "concurrentloop" => {
                    if tokens.len() != 4 {
                        return Err(self.syntax(line_no, "loop requires <var> <start> <end>"));
                    }
                    let start: i64 = tokens[2]
                        .parse()
                        .map_err(|_| self.syntax(line_no, "loop start is not a number"))?;
                    let end: i64 = tokens[3]
                        .parse()
                        .map_err(|_| self.syntax(line_no, "loop end is not a number"))?;
                    let values = if start > end {
                        Vec::new()
                    } else {
                        (start..=end).map(|v| v.to_string()).collect()
                    };
                    let body = self.block(true)?;
                    nodes.push(self.node(
                        line_no,
                        NodeKind::Loop {
                            var: tokens[1].clone(),
                            values,
                            concurrent: directive == "concurrentloop",
                            body,
                        },
                    ));
                }
                "foreach" | "concurrentforeach" => {
                    if tokens.len() < 3 {
                        return Err(self.syntax(line_no, "foreach requires <var> <value>..."));
                    }
                    let values = tokens[2..].to_vec();
                    let body = self.block(true)?;
                    nodes.push(self.node(
                        line_no,
                        NodeKind::Loop {
                            var: tokens[1].clone(),
                            values,
                            concurrent: directive == "concurrentforeach",
                            body,
                        },
                    ));
                }
                other => return Err(ParseError::Unsupported(other.to_string())),
            }
        }
    }

    fn node(&mut self, line: usize, kind: NodeKind) -> Node {
        Node {
            line,
            conditions: std::mem::take(&mut self.conditions),
            kind,
        }
    }

    /// SQL runs to the next blank line, comment line or `----`, matching
    /// DuckDB's `ExtractStatement`.
    fn read_sql(&mut self) -> String {
        let mut sql = Vec::new();
        while let Some(&line) = self.lines.get(self.pos) {
            if line.is_empty() || line.starts_with('#') || line.trim_end() == "----" {
                break;
            }
            sql.push(line);
            self.pos += 1;
        }
        sql.join("\n")
    }

    /// The optional `----` block runs to the next empty line. Returns the
    /// lines and whether a `----` separator was present.
    fn read_expected(&mut self) -> (Vec<String>, bool) {
        let mut had_separator = false;
        if self
            .lines
            .get(self.pos)
            .is_some_and(|l| l.trim_end() == "----")
        {
            had_separator = true;
            self.pos += 1;
        }
        let mut result = Vec::new();
        while let Some(&line) = self.lines.get(self.pos) {
            if line.is_empty() {
                break;
            }
            result.push(line.to_string());
            self.pos += 1;
        }
        (result, had_separator)
    }

    fn syntax(&self, line: usize, message: impl Into<String>) -> ParseError {
        ParseError::Syntax {
            line,
            message: message.into(),
        }
    }
}

fn parse_conditions(
    params: &[String],
    skip_if: bool,
    line: usize,
) -> Result<Vec<Condition>, ParseError> {
    let joined = params.join(" ");
    let mut conditions = Vec::new();
    for part in joined.split("&&") {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut parsed = None;
        for (token, op) in [
            ("<>", CondOp::Ne),
            (">=", CondOp::Ge),
            ("<=", CondOp::Le),
            ("!=", CondOp::Ne),
            (">", CondOp::Gt),
            ("<", CondOp::Lt),
            ("=", CondOp::Eq),
        ] {
            if let Some((key, value)) = part.split_once(token) {
                parsed = Some(Condition {
                    key: key.trim().to_string(),
                    op: Some(op),
                    value: value.trim().to_string(),
                    skip_if,
                });
                break;
            }
        }
        conditions.push(parsed.unwrap_or_else(|| Condition {
            key: part.to_string(),
            op: None,
            value: String::new(),
            skip_if,
        }));
    }
    if conditions.is_empty() {
        return Err(ParseError::Syntax {
            line,
            message: "skipif/onlyif needs a system name or comparison".to_string(),
        });
    }
    Ok(conditions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_statement_ok_and_error() {
        let nodes = parse_script(
            "statement ok\nCREATE TABLE t(i INTEGER)\n\n\
             statement error Binder Error\nSELECT * FROM missing\n----\nCatalog Error: missing\n",
        )
        .unwrap();
        assert_eq!(nodes.len(), 2);
        match &nodes[0].kind {
            NodeKind::Statement(s) => {
                assert_eq!(s.expect, StatementExpect::Ok);
                assert_eq!(s.sql, "CREATE TABLE t(i INTEGER)");
            }
            other => panic!("unexpected {other:?}"),
        }
        match &nodes[1].kind {
            NodeKind::Statement(s) => {
                assert_eq!(s.expect, StatementExpect::Error);
                assert_eq!(s.expected_error, "Catalog Error: missing");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn parses_query_sort_modes_and_labels() {
        let script = "query III rowsort label_a\nSELECT 1, 2, 3\n----\n1\t2\t3\n\n\
                      query I valuesort\nSELECT 2 UNION ALL SELECT 1\n----\n1\n2\n\n\
                      query I conn1\nSELECT 1\n----\n1\n";
        let nodes = parse_script(script).unwrap();
        let query = |idx: usize| match &nodes[idx].kind {
            NodeKind::Query(q) => q.clone(),
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(query(0).sort, SortMode::RowSort);
        assert_eq!(query(0).label.as_deref(), Some("label_a"));
        assert_eq!(query(0).types, "III");
        assert_eq!(query(1).sort, SortMode::ValueSort);
        assert_eq!(query(2).connection.as_deref(), Some("conn1"));
    }

    #[test]
    fn parses_conditions() {
        let nodes = parse_script(
            "skipif duckdb\nstatement ok\nSELECT 1\n\n\
             onlyif sqlite\nstatement ok\nSELECT 2\n\n\
             skipif i<>2 && j=3\nstatement ok\nSELECT 3\n",
        )
        .unwrap();
        assert_eq!(nodes[0].conditions.len(), 1);
        assert!(nodes[0].conditions[0].skip_if);
        assert_eq!(nodes[0].conditions[0].op, None);
        assert_eq!(nodes[1].conditions[0].op, None);
        assert!(!nodes[1].conditions[0].skip_if);
        assert_eq!(nodes[2].conditions.len(), 2);
        assert_eq!(nodes[2].conditions[0].op, Some(CondOp::Ne));
        assert_eq!(nodes[2].conditions[1].op, Some(CondOp::Eq));
    }

    #[test]
    fn parses_loops_and_foreach() {
        let nodes = parse_script(
            "foreach t integer,bigint\nstatement ok\nCREATE TABLE x(i ${t})\n\nendloop\n\n\
             concurrentloop i 1 3\nstatement ok\nSELECT ${i}\n\nendloop\n",
        )
        .unwrap();
        match &nodes[0].kind {
            NodeKind::Loop {
                var,
                values,
                concurrent,
                body,
            } => {
                assert_eq!(var, "t");
                assert_eq!(values, &["integer,bigint"]);
                assert!(!concurrent);
                assert_eq!(body.len(), 1);
            }
            other => panic!("unexpected {other:?}"),
        }
        match &nodes[1].kind {
            NodeKind::Loop {
                values, concurrent, ..
            } => {
                assert_eq!(values, &["1", "2", "3"]);
                assert!(concurrent);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn parses_dispatcher_directives() {
        let nodes = parse_script(
            "require skip_reload\n\nrequire-env PINKDB_SLT_X 1\n\nmode skip broken\n\n\
             set variable answer 42\n\nreset label l\n\nhalt\n",
        )
        .unwrap();
        assert!(matches!(&nodes[0].kind, NodeKind::Require { name, .. } if name == "skip_reload"));
        assert!(
            matches!(&nodes[1].kind, NodeKind::RequireEnv { name, value: Some(v) } if name == "PINKDB_SLT_X" && v == "1")
        );
        assert!(matches!(&nodes[2].kind, NodeKind::ModeSkip { reason } if reason == "broken"));
        assert!(
            matches!(&nodes[3].kind, NodeKind::SetVariable { name, value } if name == "answer" && value == "42")
        );
        assert!(matches!(&nodes[4].kind, NodeKind::ResetLabel { name } if name == "l"));
        assert!(matches!(&nodes[5].kind, NodeKind::Halt));
    }

    #[test]
    fn parses_load_restart_sleep_and_mode_unskip() {
        let nodes = parse_script(
            "load {TEST_DIR}/x.db readonly\n\nrestart\n\nsleep 5 milliseconds\n\nmode unskip\n",
        )
        .unwrap();
        assert!(
            matches!(&nodes[0].kind, NodeKind::Load { path, read_only: true } if path == "{TEST_DIR}/x.db")
        );
        assert!(matches!(&nodes[1].kind, NodeKind::Restart));
        assert!(matches!(&nodes[2].kind, NodeKind::Sleep(d) if *d == Duration::from_millis(5)));
        assert!(matches!(&nodes[3].kind, NodeKind::ModeUnskip));
    }

    #[test]
    fn unknown_directive_is_a_file_skip_with_its_name() {
        let error = parse_script("waitfor a b\n").unwrap_err();
        assert_eq!(error, ParseError::Unsupported("waitfor".to_string()));
        assert_eq!(error.to_string(), "unsupported directive: waitfor");
    }

    #[test]
    fn unknown_mode_and_set_are_skips() {
        assert_eq!(
            parse_script("mode output_hash\n").unwrap_err(),
            ParseError::Unsupported("mode output_hash".to_string())
        );
        assert_eq!(
            parse_script("set seed 1\n").unwrap_err(),
            ParseError::Unsupported("set seed".to_string())
        );
    }

    #[test]
    fn malformed_files_are_syntax_errors() {
        assert!(matches!(
            parse_script("statement nope\nSELECT 1\n"),
            Err(ParseError::Syntax { .. })
        ));
        assert!(matches!(
            parse_script("loop i 1 2\nstatement ok\nSELECT 1\n"),
            Err(ParseError::Syntax { .. })
        ));
        assert!(matches!(
            parse_script("endloop\n"),
            Err(ParseError::Syntax { .. })
        ));
        assert!(matches!(
            parse_script("query X\nSELECT 1\n"),
            Err(ParseError::Syntax { .. })
        ));
    }
}
