//! Wave 108a end to end: a CLI built with `--features fts` publishes the
//! package at startup and loads it into every store, so the fts SQL works
//! without a `LOAD EXTENSION` statement, and a durable store keeps its index.
#![cfg(feature = "fts")]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

fn run(args: &[&str], stdin: &str) -> (String, String, Option<i32>) {
    let mut child = cli()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code(),
    )
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_fts_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const SETUP: &str = "\
CREATE TABLE docs (id INTEGER, title VARCHAR, body VARCHAR);
INSERT INTO docs VALUES
  (1, 'Relational databases', 'A relational database stores rows in tables'),
  (2, 'Search engines', 'Full text search ranks documents with BM25'),
  (3, 'Cooking', 'Bread needs flour water and time');
CALL create_fts_index('docs', 'id', 'title', 'body');
SELECT stem('relational'), rrf(1);
SELECT id, match_bm25('docs', id, 'relational database') AS score
  FROM docs ORDER BY score DESC;
";

#[test]
fn fts_works_without_a_load_statement() {
    let dir = scratch("memory");
    let (out, err, code) = run(&[], SETUP);
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("relat"), "{out}");
    assert!(out.contains("0.016393"), "{out}");
    // The best match for the query is doc 1; doc 3 has no term at all.
    assert!(out.contains("3.774"), "{out}");
    assert!(out.contains("NULL"), "{out}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn durable_store_keeps_its_index() {
    let dir = scratch("durable");
    let path = dir.join("store.pinkdb");
    let path = path.to_str().unwrap();
    let (_, err, code) = run(&[], &format!(".open {path}\n{SETUP}"));
    assert_eq!(code, Some(0), "{err}");
    // A second session, opened on the saved store, still ranks.
    let (out, err, code) = run(
        &[],
        &format!(
            ".open {path}\n\
             SELECT id, match_bm25('docs', id, 'bread flour') AS score FROM docs \
             ORDER BY score DESC LIMIT 1;\n"
        ),
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("2.136"), "{out}");
    let _ = std::fs::remove_dir_all(dir);
}
