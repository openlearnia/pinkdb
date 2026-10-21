//! The console's preview formatter — a **mirror** of the repository's box
//! renderer.
//!
//! The renderer lives in the CLI *binary* crate
//! (`crates/pinkdb-cli/src/main.rs`: `format_select`, `display_value`,
//! `sanitize_cell`). Extracting it into a shared crate would move the CLI's
//! renderer out of the CLI and change the CLI's own build, so wave 78
//! mirrors the rules instead and pins them with tests. The mirrored rules:
//!
//! * a `NULL` cell renders as the text `NULL`;
//! * a `DOUBLE` renders DuckDB-style: `1.0`, not `1` (and `1.5` stays
//!   `1.5`);
//! * a `VARCHAR` cell never carries a raw control character: a newline or a
//!   carriage return becomes a space (it would break the layout) and every
//!   other control character (notably `ESC`) is shown as `\u{..}`;
//! * every other value uses the engine's own canonical display.
//!
//! The one addition the console needs: a **per-cell width cap**. The
//! terminal box sizes a column to its widest cell, which a browser table
//! cannot do (a 10 MB cell would be rendered as-is); the console truncates
//! the cell text at [`MAX_CELL_CHARS`] characters and marks the cell as
//! truncated.

use pinkdb::Value;

/// The console's per-cell character cap (the terminal box renderer has no
/// cap; a browser table needs one).
pub const MAX_CELL_CHARS: usize = 512;

/// One previewed cell: its text, whether it is NULL, and whether the text
/// was truncated at [`MAX_CELL_CHARS`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreviewCell {
    /// The cell's text, exactly as the box renderer would draw it.
    pub text: String,
    /// The cell is a SQL NULL.
    pub is_null: bool,
    /// The text was cut at [`MAX_CELL_CHARS`] characters.
    pub truncated: bool,
}

/// The box renderer's cell text (mirrors the CLI's `display_value`).
pub fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        // DuckDB prints `1.0`, not `1`.
        Value::Double(x) => {
            if x.fract() == 0.0 && x.is_finite() && x.abs() < 1e15 {
                format!("{x:.1}")
            } else {
                format!("{x}")
            }
        }
        // Stored text is untrusted: never emit raw control characters (a
        // terminal escape sequence in a row would inject output).
        Value::Varchar(s) => sanitize_cell(s),
        _ => value.to_string(),
    }
}

/// Escape control characters in a displayed VARCHAR cell (mirrors the CLI's
/// `sanitize_cell`): `\n`/`\r` become a space (a newline would break the
/// box) and every other control (notably ESC) is shown as `\u{..}`.
fn sanitize_cell(s: &str) -> String {
    if !s.chars().any(char::is_control) {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\t' => out.push('\t'),
            '\n' | '\r' => out.push(' '),
            c if c.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{{{:x}}}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// Preview one cell: the box renderer's text, capped at [`MAX_CELL_CHARS`]
/// characters.
pub fn preview_cell(value: &Value) -> PreviewCell {
    preview_cell_capped(value, MAX_CELL_CHARS)
}

/// [`preview_cell`] with an explicit cap.
pub fn preview_cell_capped(value: &Value, max_chars: usize) -> PreviewCell {
    let text = display_value(value);
    let is_null = value.is_null();
    if text.chars().count() > max_chars {
        let mut out: String = text.chars().take(max_chars.saturating_sub(1)).collect();
        out.push('…');
        PreviewCell {
            text: out,
            is_null,
            truncated: true,
        }
    } else {
        PreviewCell {
            text,
            is_null,
            truncated: false,
        }
    }
}

/// The estimated in-memory width of one value of the given DuckDB-style type
/// name.
///
/// The engine keeps **no byte-size accounting** (a `DataTable` knows its row
/// count and its chunks, not its bytes), so the console's "size" is an
/// estimate: the row count times the sum of these per-column widths. A
/// `VARCHAR` is counted as an average inline string (16 bytes) — the
/// console never scans the column to measure it.
pub fn type_width_bytes(dtype: &str) -> u64 {
    let upper = dtype.to_ascii_uppercase();
    if upper.starts_with("DECIMAL") {
        return 16;
    }
    match upper.as_str() {
        "BOOLEAN" => 1,
        "INTEGER" => 4,
        "BIGINT" => 8,
        "DOUBLE" => 8,
        "DATE" => 4,
        "TIMESTAMP" => 8,
        "INTERVAL" => 16,
        // VARCHAR: an average inline string (see the doc comment).
        _ => 16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_renders_as_null() {
        let cell = preview_cell(&Value::Null);
        assert_eq!(cell.text, "NULL");
        assert!(cell.is_null);
        assert!(!cell.truncated);
    }

    #[test]
    fn doubles_render_duckdb_style() {
        assert_eq!(display_value(&Value::Double(1.0)), "1.0");
        assert_eq!(display_value(&Value::Double(1.5)), "1.5");
        assert_eq!(display_value(&Value::Double(-0.0)), "-0.0");
    }

    #[test]
    fn control_characters_are_escaped() {
        assert_eq!(display_value(&Value::Varchar("a\nb".into())), "a b");
        assert_eq!(display_value(&Value::Varchar("a\rb".into())), "a b");
        assert_eq!(
            display_value(&Value::Varchar("a\u{1b}b".into())),
            "a\\u{1b}b"
        );
        assert_eq!(display_value(&Value::Varchar("a\tb".into())), "a\tb");
    }

    #[test]
    fn long_cells_are_truncated_and_marked() {
        let cell = preview_cell_capped(&Value::Varchar("x".repeat(100)), 10);
        assert_eq!(cell.text.chars().count(), 10);
        assert!(cell.text.ends_with('…'));
        assert!(cell.truncated);
        assert!(!cell.is_null);
    }

    #[test]
    fn widths_are_the_documented_ones() {
        assert_eq!(type_width_bytes("INTEGER"), 4);
        assert_eq!(type_width_bytes("BIGINT"), 8);
        assert_eq!(type_width_bytes("DOUBLE"), 8);
        assert_eq!(type_width_bytes("BOOLEAN"), 1);
        assert_eq!(type_width_bytes("DATE"), 4);
        assert_eq!(type_width_bytes("TIMESTAMP"), 8);
        assert_eq!(type_width_bytes("DECIMAL(18,3)"), 16);
        assert_eq!(type_width_bytes("INTERVAL"), 16);
        assert_eq!(type_width_bytes("VARCHAR"), 16);
    }
}
