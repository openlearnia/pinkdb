//! pinkdb REPL — thin consumer of the `pinkdb` library.
//!
//! Line editing via rustyline, multi-line input until `;`, dot-commands,
//! and DuckDB-style box-drawn result tables with a type row.

use std::time::Instant;

use pinkdb::{DataType, PinkDb, QueryResult, SyncPolicy, Value};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

const PROMPT: &str = "pinkdb> ";
const CONT_PROMPT: &str = "   ...> ";
/// Rows shown before truncating with a "(N more rows)" note.
const DISPLAY_CAP: usize = 10_000;

fn print_help() {
    println!("Dot-commands:");
    println!("  .help                  show this help");
    println!("  .quit | .exit          leave the shell");
    println!("  .tables                list tables");
    println!("  .schema <table>        show table schema");
    println!("  .import <file.csv> [table]  import CSV (table defaults to file stem)");
    println!("  .save <file.pink>      checkpoint the bound file; snapshot elsewhere");
    println!("  .open <file.pink>      open durable: loads the file, replays <file.pink.wal>");
    println!("SQL: end statements with `;`.");
}

fn cmd_tables(db: &PinkDb) {
    let tables = db.list_tables();
    let views = db.list_views();
    if tables.is_empty() && views.is_empty() {
        println!("(no tables)");
        return;
    }
    for t in tables {
        let n = db.table(&t).map(|x| x.num_rows()).unwrap_or(0);
        println!("{t} ({n} rows)");
    }
    for v in views {
        println!("{v} (view)");
    }
}

fn cmd_schema(db: &PinkDb, arg: &str) {
    let name = arg.trim();
    if name.is_empty() {
        println!("Usage: .schema <table>");
        return;
    }
    match db.table(name) {
        Ok(t) => {
            println!("CREATE TABLE {} (", t.name);
            let schema = t.schema();
            for (i, (col, dtype)) in schema.iter().enumerate() {
                let comma = if i + 1 == schema.len() { "" } else { "," };
                println!("  {col} {dtype}{comma}");
            }
            println!("); -- {} rows, {} chunks", t.num_rows(), t.num_chunks());
        }
        Err(e) => println!("Error: {e}"),
    }
}

fn cmd_import(db: &mut PinkDb, arg: &str) {
    let parts: Vec<&str> = arg.split_whitespace().collect();
    if parts.is_empty() {
        println!("Usage: .import <file.csv> [table]");
        return;
    }
    let t0 = Instant::now();
    match pinkdb::csv::import_csv(db, parts[0], parts.get(1).copied()) {
        Ok(n) => println!("Imported {n} rows in {:.3}s", t0.elapsed().as_secs_f64()),
        Err(e) => println!("Error: {e}"),
    }
}

/// Returns false when the REPL should exit.
fn run_dot(db: &mut PinkDb, line: &str) -> bool {
    let (cmd, rest) = match line.find(char::is_whitespace) {
        Some(i) => (&line[..i], line[i..].trim()),
        None => (line, ""),
    };
    match cmd {
        ".help" => print_help(),
        ".quit" | ".exit" => return false,
        ".tables" => cmd_tables(db),
        ".schema" => cmd_schema(db, rest),
        ".import" => cmd_import(db, rest),
        ".save" => {
            let path = rest.split_whitespace().next().unwrap_or("");
            if path.is_empty() {
                println!("Usage: .save <file.pink>");
            } else {
                let t0 = Instant::now();
                // Saving to the bound database is a checkpoint: atomic
                // publish plus a WAL generation roll. The target is compared
                // by filesystem identity, so `./x.pink`, absolute paths, and
                // case-only differences on case-insensitive filesystems all
                // checkpoint instead of silently snapshotting (which would
                // let the WAL replay committed rows twice). Any other target
                // is a snapshot, refused when it would clobber another
                // database whose WAL still holds records.
                let result = if db.is_bound_to(path) {
                    db.checkpoint()
                } else {
                    db.save_snapshot(path)
                };
                match result {
                    Ok(()) => {
                        if !db.list_views().is_empty() {
                            println!("Note: views are session-local and were not saved.");
                        }
                        println!("Saved in {:.3}s", t0.elapsed().as_secs_f64())
                    }
                    Err(e) => println!("Error: {e}"),
                }
            }
        }
        ".open" => {
            let path = rest.split_whitespace().next().unwrap_or("");
            if path.is_empty() {
                println!("Usage: .open <file.pink>");
            } else {
                match PinkDb::open_durable_with(path, SyncPolicy::Sync) {
                    Ok((new_db, info)) => {
                        if info.checkpoints_completed > 0 {
                            println!("Completed an interrupted checkpoint from its .tmp state");
                        }
                        if info.replayed_records > 0 {
                            println!(
                                "Replayed {} WAL record(s) ({} statement(s))",
                                info.replayed_records, info.replayed_statements
                            );
                        }
                        if info.truncated_bytes > 0 {
                            println!(
                                "Truncated {} byte(s) of torn WAL tail",
                                info.truncated_bytes
                            );
                        }
                        *db = new_db;
                        println!("Opened {path}");
                    }
                    Err(e) => println!("Error: {e}"),
                }
            }
        }
        _ => println!("Unknown dot-command \"{cmd}\". Try .help"),
    }
    true
}

fn display_value(v: &Value) -> String {
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
        _ => v.to_string(),
    }
}

/// Formats a `SELECT` result as the DuckDB-style box table: header names,
/// type row, up to [`DISPLAY_CAP`] data rows, and the trailing summary line.
///
/// Only the shown rows are formatted: column widths are computed from the
/// header names, the type row, and those rows, so huge results don't pay for
/// rows that are never displayed. The summary still reports the true total
/// row count.
///
/// The result has no trailing newline; `run_sql` prints it with `println!`
/// so a single call reproduces the old per-line output byte for byte.
fn format_select(names: &[String], types: &[DataType], rows: &[Vec<Value>]) -> String {
    let shown = rows.len().min(DISPLAY_CAP);
    let cells: Vec<Vec<String>> = rows
        .iter()
        .take(shown)
        .map(|r| r.iter().map(display_value).collect())
        .collect();
    let type_row: Vec<String> = types.iter().map(|t| t.name().to_string()).collect();
    let mut widths: Vec<usize> = names.iter().map(|n| n.chars().count().max(1)).collect();
    for (i, t) in type_row.iter().enumerate() {
        widths[i] = widths[i].max(t.chars().count());
    }
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }

    let mut out = String::new();
    let border = |out: &mut String, l: &str, m: &str, r: &str| {
        out.push_str(l);
        for (i, w) in widths.iter().enumerate() {
            if i > 0 {
                out.push_str(m);
            }
            out.push_str(&"─".repeat(w + 2));
        }
        out.push_str(r);
        out.push('\n');
    };
    let row_out = |out: &mut String, items: &[String]| {
        out.push('│');
        for (i, c) in items.iter().enumerate() {
            let w = widths[i];
            out.push(' ');
            out.push_str(c);
            out.push_str(&" ".repeat(w - c.chars().count() + 1));
            out.push('│');
        }
        out.push('\n');
    };

    border(&mut out, "┌", "┬", "┐");
    row_out(&mut out, names);
    row_out(&mut out, &type_row);
    border(&mut out, "├", "┼", "┤");
    for row in &cells {
        row_out(&mut out, row);
    }
    border(&mut out, "└", "┴", "┘");
    if rows.len() > DISPLAY_CAP {
        out.push_str(&format!(
            "({} more rows not shown)\n",
            rows.len() - DISPLAY_CAP
        ));
    }
    let noun = if rows.len() == 1 { "row" } else { "rows" };
    out.push_str(&format!("{} {noun}", rows.len()));
    out
}

fn run_sql(db: &mut PinkDb, sql: &str) {
    let t0 = Instant::now();
    match db.execute(sql) {
        Ok(results) => {
            for r in &results {
                match r {
                    QueryResult::Created => println!("Table created."),
                    QueryResult::Inserted { count } => println!("Inserted {count} row(s)."),
                    QueryResult::Updated { count } => println!("Updated {count} row(s)."),
                    QueryResult::Deleted { count } => println!("Deleted {count} row(s)."),
                    QueryResult::Began => println!("Transaction started."),
                    QueryResult::Committed => println!("Transaction committed."),
                    QueryResult::RolledBack => println!("Transaction rolled back."),
                    QueryResult::CreatedView { name } => println!("View {name} created."),
                    QueryResult::DroppedTable { name } => println!("Table {name} dropped."),
                    QueryResult::DroppedView { name } => println!("View {name} dropped."),
                    QueryResult::Selected { names, types, rows } => {
                        if rows.is_empty() {
                            println!("(no rows)");
                        }
                        let table = format_select(names, types, rows);
                        println!("{table}");
                    }
                }
            }
        }
        Err(e) => println!("Error: {e}"),
    }
    println!("Run Time (s): real {:.3}", t0.elapsed().as_secs_f64());
}

fn main() {
    let mut db = PinkDb::new();
    let mut rl = DefaultEditor::new().unwrap_or_else(|e| {
        eprintln!("rustyline init failed ({e}); falling back to stdin");
        DefaultEditor::new().expect("rustyline fallback")
    });
    println!("pinkdb v0.1.0 — mini DuckDB (in-memory). Type .help, end SQL with ;");

    let mut pending = String::new();
    loop {
        let prompt = if pending.is_empty() {
            PROMPT
        } else {
            CONT_PROMPT
        };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // Dot-commands only at the start of a fresh statement.
                if pending.is_empty() && trimmed.starts_with('.') {
                    rl.add_history_entry(line.as_str()).ok();
                    if !run_dot(&mut db, trimmed) {
                        break;
                    }
                    continue;
                }
                pending.push_str(&line);
                pending.push('\n');
                if trimmed.ends_with(';') {
                    rl.add_history_entry(pending.as_str()).ok();
                    let sql = pending.trim().to_string();
                    pending.clear();
                    run_sql(&mut db, &sql);
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: drop pending input, stay in shell.
                pending.clear();
                println!("(^C — input cleared)");
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_small_result_exactly() {
        let names = vec!["id".to_string(), "name".to_string()];
        let types = vec![DataType::Integer, DataType::Varchar];
        let rows = vec![
            vec![Value::Integer(1), Value::Varchar("alice".to_string())],
            vec![Value::Integer(2), Value::Varchar("bob".to_string())],
        ];
        let expected = concat!(
            "┌─────────┬─────────┐\n",
            "│ id      │ name    │\n",
            "│ INTEGER │ VARCHAR │\n",
            "├─────────┼─────────┤\n",
            "│ 1       │ alice   │\n",
            "│ 2       │ bob     │\n",
            "└─────────┴─────────┘\n",
            "2 rows",
        );
        assert_eq!(format_select(&names, &types, &rows), expected);
    }

    #[test]
    fn caps_formatting_but_reports_true_total() {
        let names = vec!["v".to_string()];
        let types = vec![DataType::Varchar];
        let total = DISPLAY_CAP + 5;
        // A uniquely wide cell lives only beyond the display cap; if it were
        // formatted, it would widen the column and leak into the output.
        let rows: Vec<Vec<Value>> = (0..total)
            .map(|i| {
                if i == total - 1 {
                    vec![Value::Varchar("w".repeat(60))]
                } else {
                    vec![Value::Varchar(format!("r{i}"))]
                }
            })
            .collect();

        let out = format_select(&names, &types, &rows);

        assert!(
            out.contains("(5 more rows not shown)"),
            "missing truncation note:\n{out}"
        );
        assert!(
            out.contains(&format!("{total} rows")),
            "summary must report the true total:\n{out}"
        );
        // Widths come from the shown rows only, so the border stays narrow
        // (VARCHAR is 7 wide) instead of 60 wide.
        assert_eq!(out.lines().next().unwrap(), "┌─────────┐");
        assert!(
            !out.contains(&"w".repeat(60)),
            "beyond-cap cell leaked into output"
        );
    }

    #[test]
    fn single_row_summary_is_singular() {
        let names = vec!["n".to_string()];
        let types = vec![DataType::Bigint];
        let rows = vec![vec![Value::Bigint(42)]];

        let out = format_select(&names, &types, &rows);

        assert!(
            out.ends_with("\n1 row"),
            "expected singular summary, got:\n{out}"
        );
    }
}
