//! pinkdb REPL — thin consumer of the `pinkdb` library.
//!
//! Line editing via rustyline, multi-line input until `;`, dot-commands,
//! and DuckDB-style box-drawn result tables with a type row.

use std::time::Instant;

use pinkdb::{DataType, PinkDb, QueryResult, Value};
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
    println!("  .save <file.pink>      persist every table to disk");
    println!("  .open <file.pink>      replace the database with a saved file");
    println!("SQL: end statements with `;`.");
}

fn cmd_tables(db: &PinkDb) {
    let tables = db.list_tables();
    if tables.is_empty() {
        println!("(no tables)");
        return;
    }
    for t in tables {
        let n = db.table(&t).map(|x| x.num_rows()).unwrap_or(0);
        println!("{t} ({n} rows)");
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
                match db.save(path) {
                    Ok(()) => println!("Saved in {:.3}s", t0.elapsed().as_secs_f64()),
                    Err(e) => println!("Error: {e}"),
                }
            }
        }
        ".open" => {
            let path = rest.split_whitespace().next().unwrap_or("");
            if path.is_empty() {
                println!("Usage: .open <file.pink>");
            } else {
                match PinkDb::open(path) {
                    Ok(new_db) => {
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

fn render_select(names: &[String], types: &[DataType], rows: &[Vec<Value>]) {
    let cells: Vec<Vec<String>> = rows.iter().map(|r| r.iter().map(display_value).collect()).collect();
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

    let border = |l: &str, m: &str, r: &str| {
        let mut s = String::from(l);
        for (i, w) in widths.iter().enumerate() {
            if i > 0 {
                s.push_str(m);
            }
            s.push_str(&"─".repeat(w + 2));
        }
        s.push_str(r);
        println!("{s}");
    };
    let row_out = |items: &[String]| {
        let mut s = String::from("│");
        for (i, c) in items.iter().enumerate() {
            let w = widths[i];
            s.push(' ');
            s.push_str(c);
            s.push_str(&" ".repeat(w - c.chars().count() + 1));
            s.push('│');
        }
        println!("{s}");
    };

    border("┌", "┬", "┐");
    row_out(&names.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    row_out(&type_row);
    border("├", "┼", "┤");
    for row in cells.iter().take(DISPLAY_CAP) {
        row_out(row);
    }
    border("└", "┴", "┘");
    if cells.len() > DISPLAY_CAP {
        println!("({} more rows not shown)", cells.len() - DISPLAY_CAP);
    }
    let noun = if rows.len() == 1 { "row" } else { "rows" };
    println!("{} {noun}", rows.len());
}

fn run_sql(db: &mut PinkDb, sql: &str) {
    let t0 = Instant::now();
    match db.execute(sql) {
        Ok(results) => {
            for r in &results {
                match r {
                    QueryResult::Created => println!("Table created."),
                    QueryResult::Inserted { count } => println!("Inserted {count} row(s)."),
                    QueryResult::Selected { names, types, rows } => {
                        if rows.is_empty() {
                            println!("(no rows)");
                        }
                        render_select(names, types, rows);
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
        let prompt = if pending.is_empty() { PROMPT } else { CONT_PROMPT };
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
