//! Multi-database CLI surface: `.databases`, `USE`, qualified names, and
//! per-database scoping of `.tables` / `.schema` / `.import`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_multidb_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Run the REPL to a clean exit and return its stdout.
fn run_in(cwd: Option<&Path>, cmds: &str) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let mut child = cmd.spawn().expect("spawn pinkdb CLI");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(cmds.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("wait for CLI");
    assert!(out.status.success(), "CLI exited with {}", out.status);
    String::from_utf8(out.stdout).expect("CLI stdout is UTF-8")
}

fn run(cmds: &str) -> String {
    run_in(None, cmds)
}

#[test]
fn databases_dot_command_marks_the_current_one() {
    let out = run(
        ".databases\n\
         CREATE DATABASE analytics;\n\
         .databases\n\
         USE analytics;\n\
         .databases\n\
         USE main;\n\
         .databases\n\
         .quit\n",
    );
    assert!(out.contains("Database analytics created."), "{out}");
    assert!(out.contains("Using database analytics."), "{out}");
    assert!(out.contains("Using database main."), "{out}");
    // The first listing has main as current; the second marks analytics.
    let first = out.find("main (current)").expect("main marked current");
    let second = out.find("analytics (current)").expect("analytics marked current");
    assert!(first < second, "{out}");
    assert!(out.contains("analytics (0 tables)"), "{out}");
}

#[test]
fn use_and_qualified_names_scope_sql_and_dot_commands() {
    let out = run(
        "CREATE DATABASE db1;\n\
         CREATE TABLE db1.t (a INTEGER);\n\
         INSERT INTO db1.t VALUES (7);\n\
         USE db1;\n\
         .tables\n\
         SELECT a FROM t;\n\
         USE main;\n\
         .tables\n\
         SELECT a FROM db1.t;\n\
         .quit\n",
    );
    assert!(!out.contains("Error"), "{out}");
    // In db1, `.tables` shows its table; on main it is empty.
    let tables_at = out.find("t (1 rows)").expect("db1.t listed");
    let empty_at = out.find("(no tables)").expect("main has no tables");
    assert!(tables_at < empty_at, "{out}");
    // Both unqualified (in db1) and qualified (from main) selects see row 7.
    assert_eq!(out.matches("│ 7       │").count(), 2, "{out}");
}

#[test]
fn schema_accepts_qualified_names() {
    let out = run(
        "CREATE DATABASE db1;\n\
         CREATE TABLE db1.t (a INTEGER, b VARCHAR);\n\
         .schema db1.t\n\
         .schema t\n\
         .quit\n",
    );
    assert!(out.contains("CREATE TABLE t ("), "{out}");
    assert!(out.contains("a INTEGER"), "{out}");
    assert!(out.contains("b VARCHAR"), "{out}");
    // Unqualified `.schema t` resolves against the session database (main).
    assert!(out.contains("Error: table \"t\" not found"), "{out}");
}

#[test]
fn import_targets_the_current_database_or_a_qualified_table() {
    let dir = scratch("import");
    let csv = dir.join("data.csv");
    std::fs::write(&csv, "a\n1\n2\n").unwrap();
    let cmds = format!(
        "CREATE DATABASE db1;\n\
         USE db1;\n\
         .import {} t\n\
         .tables\n\
         USE main;\n\
         .import {} db1.u\n\
         SELECT a FROM db1.t ORDER BY a;\n\
         SELECT a FROM db1.u ORDER BY a;\n\
         .quit\n",
        csv.display(),
        csv.display()
    );
    let out = run(&cmds);
    assert!(!out.contains("Error"), "{out}");
    assert!(out.contains("Imported 2 rows"), "{out}");
    // Two rows per select (t and u), so four displayed data rows.
    assert_eq!(out.matches("│ 1       │").count(), 2, "{out}");
    assert_eq!(out.matches("│ 2       │").count(), 2, "{out}");
    cleanup(&dir);
}

#[test]
fn databases_counts_tables_and_views_separately_and_schema_shows_views() {
    let out = run(
        "CREATE TABLE t (a INTEGER);\n\
         CREATE VIEW v AS SELECT a FROM t;\n\
         CREATE DATABASE db1;\n\
         USE db1;\n\
         CREATE VIEW dv AS SELECT 1 AS x;\n\
         USE main;\n\
         .databases\n\
         .schema v\n\
         .schema db1.dv\n\
         .schema db1.missing\n\
         .quit\n",
    );
    // Views are counted and labeled as views, not folded into the table count.
    assert!(out.contains("main (current) (1 table, 1 view)"), "{out}");
    assert!(out.contains("db1 (0 tables, 1 view)"), "{out}");
    // `.schema` on a view (qualified or not) shows its output columns.
    assert!(out.contains("CREATE VIEW v ("), "{out}");
    assert!(out.contains("a INTEGER"), "{out}");
    assert!(out.contains("CREATE VIEW dv ("), "{out}");
    assert!(out.contains("x INTEGER"), "{out}");
    // A missing name is a clear error naming both kinds.
    assert!(
        out.contains("Error: table \"missing\" not found; no such view either"),
        "{out}"
    );
}

#[test]
fn save_notes_views_in_any_database() {
    let dir = scratch("saveviews");
    let ckpt = dir.join("s.pink");
    let path = ckpt.to_str().unwrap();
    let out = run(&format!(
        "CREATE DATABASE db1;\n\
         USE db1;\n\
         CREATE VIEW dv AS SELECT 1 AS x;\n\
         USE main;\n\
         .save {path}\n\
         .quit\n"
    ));
    assert!(!out.contains("Error"), "{out}");
    // The view lives in db1, not the session database: the note must still
    // appear (views are session-local in every database).
    assert!(
        out.contains("Note: views are session-local and were not saved."),
        "{out}"
    );
    cleanup(&dir);
}

#[test]
fn dotted_and_quoted_import_targets() {
    let dir = scratch("targets");
    let csv = dir.join("f.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let dotted = dir.join("data.2024.csv");
    std::fs::write(&dotted, "a\n2\n").unwrap();
    let cmds = format!(
        "CREATE DATABASE \"My Db\";\n\
         .import {} \"My Db\".t\n\
         .schema \"My Db\".t\n\
         .import {}\n\
         .schema data.2024\n\
         .import {} other.2024\n\
         .schema other.2024\n\
         .import {} nope.u\n\
         .schema nope.u\n\
         .quit\n",
        csv.display(),
        dotted.display(),
        csv.display(),
        csv.display()
    );
    let out = run(&cmds);
    assert!(!out.contains("Error"), "{out}");
    // Quoted database name with a space.
    assert!(out.contains("CREATE TABLE t ("), "{out}");
    assert_eq!(out.matches("Imported 1 rows").count(), 4, "{out}");
    // A dotted file stem and a dotted explicit table both stay whole names.
    assert!(out.contains("CREATE TABLE data.2024 ("), "{out}");
    assert!(out.contains("CREATE TABLE other.2024 ("), "{out}");
    // A missing database name is not a qualifier: the whole string is a table.
    assert!(out.contains("CREATE TABLE nope.u ("), "{out}");
    cleanup(&dir);
}

#[test]
fn databases_persist_across_save_and_reopen() {
    let dir = scratch("persist");
    let ckpt = dir.join("multi.pink");
    let path = ckpt.to_str().unwrap();
    let cmds = format!(
        ".open {path}\n\
         CREATE DATABASE db1;\n\
         CREATE TABLE main.t (a INTEGER);\n\
         INSERT INTO main.t VALUES (1);\n\
         CREATE TABLE db1.t (a INTEGER);\n\
         INSERT INTO db1.t VALUES (2);\n\
         .save {path}\n\
         .quit\n"
    );
    let out = run(&cmds);
    assert!(!out.contains("Error"), "{out}");

    // Reopen in-process: both databases and their rows survived.
    let db = pinkdb::PinkDb::open(path).unwrap();
    assert_eq!(db.list_databases(), vec!["db1".to_string(), "main".to_string()]);
    assert_eq!(db.table("t").unwrap().num_rows(), 1);
    assert_eq!(db.table_in("db1", "t").unwrap().num_rows(), 1);
    assert_eq!(db.current_db(), "main");

    // Reopen through the REPL and query both.
    let out = run(&format!(
        ".open {path}\n.databases\nUSE db1;\nSELECT a FROM t;\nUSE main;\nSELECT a FROM t;\n.quit\n"
    ));
    assert!(out.contains("main (current)"), "{out}");
    assert_eq!(out.matches("│ 1       │").count(), 1, "{out}");
    assert_eq!(out.matches("│ 2       │").count(), 1, "{out}");
    cleanup(&dir);
}
