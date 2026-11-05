//! Agent-friendly rendering (wave 99): compact Markdown result tables and
//! one-line JSON errors.
//!
//! The formatter is deliberately independent of the CLI so the server and
//! extensions (wave 105's MCP bridge) can reuse it: [`format_table`] takes
//! the same `names`/`types`/`rows` a [`crate::QueryResult::Selected`] carries
//! and returns the rendered text, and [`error_json_for`] turns a
//! [`crate::PinkDbError`] into the one-line JSON contract.
//!
//! Agent mode never changes the default (interactive) renderer; the CLI only
//! calls this module when the mode is on.

use std::fmt::Write as _;

use crate::{DataType, PinkDb, PinkDbError, Value};

/// Display caps for the Markdown renderer. `0` means "no cap".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentDisplay {
    /// Rows shown before the truncation notice.
    pub max_rows: usize,
    /// Columns shown before the truncation notice.
    pub max_columns: usize,
    /// Characters per cell (and per column header) before `…`.
    pub max_cell_chars: usize,
}

impl AgentDisplay {
    /// 100 rows, 32 columns, 500 characters per cell.
    pub const DEFAULT: Self = Self {
        max_rows: 100,
        max_columns: 32,
        max_cell_chars: 500,
    };
}

impl Default for AgentDisplay {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One statement's cheap scan estimate (see [`estimate_scan`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanEstimate {
    /// Rows in the tables the bound plan reads (each table counted once).
    pub rows: u64,
    /// An attached lake table or a table function is involved, so the scan
    /// reaches remote/external data.
    pub remote: bool,
}

/// The value text the box renderer uses, before table-specific escaping.
fn raw_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        // DuckDB prints `1.0`, not `1`.
        Value::Double(x) => {
            if x.fract() == 0.0 && x.is_finite() && x.abs() < 1e15 {
                format!("{x:.1}")
            } else {
                format!("{x}")
            }
        }
        Value::Varchar(s) => s.clone(),
        _ => v.to_string(),
    }
}

/// Render one value for a terminal: control characters escaped, text values
/// sanitized the same way the box renderer sanitizes them.
pub fn display_value(v: &Value) -> String {
    match v {
        Value::Varchar(s) => crate::sanitize_terminal(s),
        _ => raw_value(v),
    }
}

/// Truncate to at most `max` characters (0 = no cap), marking the cut with
/// `…`. The cut happens before escaping so an escape sequence is never split.
fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return s.to_string();
    }
    match s.char_indices().nth(max) {
        Some((at, _)) => {
            let mut out = s[..at].to_string();
            out.push('…');
            out
        }
        None => s.to_string(),
    }
}

/// One table cell: truncated, terminal-safe, and Markdown-safe.
fn cell_text(text: &str, max_chars: usize) -> String {
    // `|` would end the cell; the entity keeps the Markdown table parseable
    // without doubling backslashes (which would corrupt `\u{1b}` escapes).
    crate::sanitize_terminal(&truncate(text, max_chars)).replace('|', "&#124;")
}

/// Thousands separators, e.g. `1284` → `1,284`.
pub fn format_count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Render a `SELECT` result as a compact Markdown table, with an explicit
/// notice for every row or column the caps hide.
///
/// The output has no trailing newline (the caller decides), matching the box
/// renderer. A result with no rows still prints the header and separator so
/// the schema is visible.
pub fn format_table(
    names: &[String],
    types: &[DataType],
    rows: &[Vec<Value>],
    display: AgentDisplay,
) -> String {
    format_table_counted(names, types, rows, rows.len() as u64, display)
}

/// [`format_table`] for a streamed result: `rows` holds at least the rows
/// the table shows (the first `display.max_rows`), and `total` is how many
/// the result had, which the "… N more rows" notice reports.
pub fn format_table_counted(
    names: &[String],
    types: &[DataType],
    rows: &[Vec<Value>],
    total: u64,
    display: AgentDisplay,
) -> String {
    let total_columns = names.len().min(types.len());
    let shown_columns = if display.max_columns == 0 {
        total_columns
    } else {
        total_columns.min(display.max_columns)
    };
    let shown_rows = if display.max_rows == 0 {
        rows.len()
    } else {
        rows.len().min(display.max_rows)
    };
    if shown_columns == 0 {
        return String::new();
    }

    let mut out = String::new();
    let mut header = String::from("|");
    for i in 0..shown_columns {
        let label = format!("{}:{}", names[i], types[i].name());
        header.push(' ');
        header.push_str(&cell_text(&label, display.max_cell_chars));
        header.push_str(" |");
    }
    out.push_str(&header);
    out.push('\n');
    out.push('|');
    for _ in 0..shown_columns {
        out.push_str("---|");
    }
    out.push('\n');
    for row in rows.iter().take(shown_rows) {
        out.push('|');
        for i in 0..shown_columns {
            out.push(' ');
            match row.get(i) {
                Some(value) => out.push_str(&cell_text(&raw_value(value), display.max_cell_chars)),
                None => out.push_str("NULL"),
            }
            out.push_str(" |");
        }
        out.push('\n');
    }

    // Match the box renderer: no trailing newline; the notice (if any)
    // starts on its own line.
    if out.ends_with('\n') {
        out.pop();
    }

    let hidden_rows = (total as usize).saturating_sub(shown_rows);
    let hidden_columns = total_columns.saturating_sub(shown_columns);
    if hidden_rows > 0 || hidden_columns > 0 {
        out.push('\n');
        out.push('…');
        out.push(' ');
        let mut parts = Vec::new();
        if hidden_rows > 0 {
            parts.push(format!(
                "{} more row{} ({} total)",
                format_count(hidden_rows as u64),
                if hidden_rows == 1 { "" } else { "s" },
                format_count(total)
            ));
        }
        if hidden_columns > 0 {
            // A hostile wide result could hide thousands of names; the
            // notice stays one line by listing the first ten.
            let mut listed: Vec<String> = names[shown_columns..total_columns]
                .iter()
                .take(10)
                .map(|name| crate::sanitize_terminal(name))
                .collect();
            if hidden_columns > listed.len() {
                listed.push("…".to_string());
            }
            parts.push(format!(
                "{} column{} hidden: {}",
                format_count(hidden_columns as u64),
                if hidden_columns == 1 { "" } else { "s" },
                listed.join(", ")
            ));
        }
        out.push_str(&parts.join("; "));
    }
    out
}

/// The stable error kind for the JSON contract.
pub fn error_kind(err: &PinkDbError) -> &'static str {
    match err {
        PinkDbError::Catalog(_) => "catalog",
        PinkDbError::Type(_) => "type",
        PinkDbError::Parse(_) => "parse",
        PinkDbError::Parameter(_) => "parameter",
        PinkDbError::Execution(_) => "execution",
        PinkDbError::Permission(_) => "permission",
        PinkDbError::Io(_) => "io",
        PinkDbError::Csv(_) => "csv",
    }
}

/// One line of JSON: `{"error":"…","kind":"…","position":…}`.
///
/// `position` is a 0-based byte offset into the SQL text, or `None` when the
/// engine has no location for the error. Control characters are escaped so
/// the line is always valid JSON and never carries a raw terminal sequence.
pub fn error_json(message: &str, kind: &str, position: Option<usize>) -> String {
    let mut out = String::with_capacity(message.len() + 48);
    out.push_str("{\"error\":\"");
    json_escape_into(&mut out, message);
    out.push_str("\",\"kind\":\"");
    json_escape_into(&mut out, kind);
    out.push_str("\",\"position\":");
    match position {
        Some(position) => {
            let _ = write!(out, "{position}");
        }
        None => out.push_str("null"),
    }
    out.push('}');
    out
}

/// JSON string escaping without a dependency. Every control character is
/// escaped, so the result is one physical line.
pub(crate) fn json_escape_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// The byte offset of a parser's `line:column` position (both 1-based), or
/// `None` when the message has no such prefix or names a line past the end.
fn parse_position(message: &str, sql: &str) -> Option<usize> {
    let rest = message.strip_prefix("parse error: error at ")?;
    let (line, rest) = rest.split_once(':')?;
    let (column, _) = rest.split_once(':')?;
    let line: usize = line.parse().ok()?;
    let column: usize = column.parse().ok()?;
    if line == 0 || column == 0 {
        return None;
    }
    let mut offset = 0;
    for (index, text) in sql.split_inclusive('\n').enumerate() {
        if index + 1 == line {
            let at = text
                .char_indices()
                .nth(column - 1)
                .map(|(at, _)| at)
                // A position past the line end clamps to the line's end.
                .unwrap_or(text.len());
            return Some(offset + at);
        }
        offset += text.len();
    }
    None
}

/// Byte offset of a parse error, `None` for every other kind.
pub fn error_position(err: &PinkDbError, sql: &str) -> Option<usize> {
    match err {
        PinkDbError::Parse(_) => parse_position(&err.to_string(), sql),
        _ => None,
    }
}

/// The CLI's error line: `{"error","kind","position"}` for one failed
/// statement.
pub fn error_json_for(err: &PinkDbError, sql: &str) -> String {
    error_json(&err.to_string(), error_kind(err), error_position(err, sql))
}

/// A cheap pre-execution estimate from the bound plan: the sum of the input
/// tables' row counts and whether an attached lake or a table function is
/// read.
///
/// This is a lower bound, not an optimizer cardinality estimate (the
/// optimizer has none): a self-join counts its table once, and a view is not
/// expanded. `None` when the SQL does not parse or bind, so the caller never
/// prints a hint for a statement that is about to fail anyway.
pub fn estimate_scan(db: &PinkDb, sql: &str) -> Option<ScanEstimate> {
    let stmt = crate::sql::parse_statements(sql).ok()?.into_iter().next()?;
    let session = db.current_ns().clone();
    let (plan, _) = crate::sql::bind_tracked(db, &stmt, &session).ok()?;
    let plan = crate::optimizer::optimize(db, plan);
    let mut tables = std::collections::HashSet::new();
    crate::plan::collect_tables_read(&plan, &session, &mut tables);
    let mut rows = 0u64;
    let mut remote = false;
    for (ns, table) in tables {
        if let Ok(table) = db.table_in(&ns, &table) {
            rows += table.num_rows() as u64;
        } else if let Some(attached) = db
            .attached_lookup(ns.db(), ns.schema(), &table)
            .map(|(t, _)| t.clone())
        {
            rows += attached.rows;
            remote = true;
        }
    }
    if crate::plan::contains_table_function(&plan) {
        remote = true;
    }
    Some(ScanEstimate { rows, remote })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selected(rows: Vec<Vec<Value>>) -> String {
        format_table(
            &["a".to_string(), "b".to_string()],
            &[DataType::Integer, DataType::Varchar],
            &rows,
            AgentDisplay::DEFAULT,
        )
    }

    #[test]
    fn renders_a_markdown_table_with_null() {
        let text = selected(vec![
            vec![Value::Integer(1), Value::Varchar("x".to_string())],
            vec![Value::Null, Value::Null],
        ]);
        assert_eq!(
            text,
            "| a:INTEGER | b:VARCHAR |\n|---|---|\n| 1 | x |\n| NULL | NULL |"
        );
    }

    #[test]
    fn empty_result_keeps_the_header() {
        assert_eq!(selected(Vec::new()), "| a:INTEGER | b:VARCHAR |\n|---|---|");
    }

    #[test]
    fn caps_report_hidden_rows_and_columns_with_totals() {
        let rows: Vec<Vec<Value>> = (0..5)
            .map(|i| vec![Value::Integer(i), Value::Varchar(format!("r{i}"))])
            .collect();
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let types = vec![DataType::Integer; 3];
        let text = format_table(
            &names,
            &types,
            &rows,
            AgentDisplay {
                max_rows: 2,
                max_columns: 1,
                max_cell_chars: 0,
            },
        );
        assert!(text.ends_with("… 3 more rows (5 total); 2 columns hidden: b, c"));
        assert_eq!(text.lines().count(), 5);
    }

    #[test]
    fn long_cells_and_headers_are_cut_once() {
        let text = format_table(
            &["a long name".to_string()],
            &[DataType::Varchar],
            &[vec![Value::Varchar("0123456789".to_string())]],
            AgentDisplay {
                max_rows: 0,
                max_columns: 0,
                max_cell_chars: 4,
            },
        );
        assert_eq!(text, "| a lo… |\n|---|\n| 0123… |");
    }

    #[test]
    fn control_characters_and_pipes_are_escaped() {
        let text = selected(vec![vec![
            Value::Integer(1),
            Value::Varchar("a\u{1b}[31m|b".to_string()),
        ]]);
        assert!(text.contains("a\\u{1b}[31m&#124;b"));
        assert!(!text.chars().any(|c| c.is_control() && c != '\n'));
    }

    #[test]
    fn error_json_is_one_valid_line() {
        let line = error_json("table \"x\"\nnot found", "catalog", Some(12));
        assert_eq!(
            line,
            "{\"error\":\"table \\\"x\\\"\\nnot found\",\"kind\":\"catalog\",\"position\":12}"
        );
        assert!(!line.contains('\n'));
    }

    #[test]
    fn parse_position_is_a_byte_offset() {
        assert_eq!(
            parse_position("parse error: error at 1:7: expected", "selct 1"),
            Some(6)
        );
        assert_eq!(
            parse_position("parse error: error at 2:3: expected", "select\n  +"),
            Some(9)
        );
        assert_eq!(parse_position("no position", "select 1"), None);
    }

    #[test]
    fn counts_get_thousands_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_284), "1,284");
        assert_eq!(format_count(1_234_567), "1,234,567");
    }
}
