//! CLI coverage for a `.pink` lake catalog (wave 103, T1b): `.lake save
//! <pink:path> data <dir>`, `.lake open`, `.lake attach` and the writable
//! attach + `.lake commit`, plus two real CLI processes committing to one
//! catalog.

#![cfg(feature = "pinklake")]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_pink_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn cli_binary() -> String {
    std::env::var("PINKDB_CLI_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_pinkdb").to_string())
}

fn run(cmds: &str) -> String {
    let command = Command::new(cli_binary());
    let (out, status) = common::run_merged(command, cmds);
    common::assert_script_exit(status);
    out
}

/// A catalog URL and data location inside one scratch directory.
fn paths(dir: &Path) -> (String, String) {
    (
        format!("pink:{}", dir.join("catalog.pink").display()),
        dir.join("data-root").display().to_string(),
    )
}

fn catalog_exists(path: &Path) -> bool {
    path.is_file() || PathBuf::from(format!("{}.wal", path.display())).is_file()
}

#[test]
fn save_open_attach_and_commit_through_a_pink_catalog() {
    let dir = scratch("roundtrip");
    let (catalog, data) = paths(&dir);

    // Save: the catalog is created and the data location is recorded once.
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
         .lake save {catalog} data {data}\n\
         .lake versions {catalog}\n\
         .lake snapshots {catalog}\n\
         .lake tables {catalog}\n\
         .lake info {catalog}\n\
         .lake open {catalog}\n\
         SELECT a, b FROM t ORDER BY a;\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains(&format!("lake: {data}")), "{out}");
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(!out.contains("Error"), "{out}");
    assert!(out.contains("│ 1       │ x       │"), "{out}");
    assert!(out.contains("│ 2       │ NULL    │"), "{out}");

    // The metadata is in one durable pinkdb database; the data location has
    // the marker and files only.
    let catalog_path = dir.join("catalog.pink");
    assert!(catalog_exists(&catalog_path), "the catalog database exists");
    assert!(
        dir.join("data-root").join("lake-data.json").is_file(),
        "the ownership marker is at the data root"
    );
    assert!(
        !dir.join("data-root").join("catalog.json").exists(),
        "no object-store metadata at the data location"
    );

    // Read-only attach by the catalog URL, an explicit `data` open (the moved
    // location override), and detach by the same URL.
    let out = run(&format!(
        ".lake attach {catalog}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake detach {catalog}\n\
         .lake open {catalog} data {data}\n\
         SELECT COUNT(*) FROM t;\n\
         .quit\n"
    ));
    assert!(out.contains("statistics-only"), "{out}");
    assert!(out.contains("│ 2        │"), "{out}");
    assert!(out.contains(&format!("Detached {catalog}")), "{out}");

    // Writable attach + commit, all keyed by the catalog URL.
    let out = run(&format!(
        ".lake attach {catalog} write\n\
         INSERT INTO t VALUES (3, 'z');\n\
         .lake commit {catalog}\n\
         .quit\n"
    ));
    assert!(out.contains("Committed lake version 3"), "{out}");
    assert!(!out.contains("no writable attach"), "{out}");

    // The new version is visible from a fresh session.
    let out = run(&format!(
        ".lake open {catalog}\nSELECT COUNT(*) FROM t;\n.quit\n"
    ));
    assert!(out.contains("Opened lake version 3"), "{out}");
    assert!(out.contains("│ 3        │"), "{out}");

    cleanup(&dir);
}

/// A fresh `.pink` catalog defaults its data root to DuckLake's
/// `<catalog path>.files/` when `data` is omitted, and records it so later
/// opens need only the catalog URL.
#[test]
fn a_pink_save_without_data_uses_the_default_data_root() {
    let dir = scratch("default-data");
    let (catalog, _) = paths(&dir);
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {catalog}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let data = dir.join("catalog.pink.files");
    assert!(out.contains(&format!("lake: {}", data.display())), "{out}");
    assert!(
        data.join("lake-data.json").is_file(),
        "the default data root sits next to the catalog"
    );
    assert!(
        data.join("data").is_dir(),
        "the data files are under the default root"
    );

    let out = run(&format!(
        ".lake open {catalog}\nSELECT COUNT(*) FROM t;\n.quit\n"
    ));
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 1        │"), "{out}");
    cleanup(&dir);
}

/// A live CLI process for the cross-process test: SQL/commands in over stdin,
/// merged output out, marker lines synchronized by polling.
struct Cli {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    lines: std::sync::mpsc::Receiver<String>,
    out: String,
}

impl Cli {
    fn spawn() -> Self {
        let (reader, writer) = std::io::pipe().expect("create pipe");
        let mut child = Command::new(cli_binary())
            .stdin(Stdio::piped())
            .stdout(writer.try_clone().expect("clone pipe writer"))
            .stderr(writer)
            .spawn()
            .expect("spawn pinkdb CLI");
        let stdin = child.stdin.take().expect("child stdin");
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            child,
            stdin: Some(stdin),
            lines,
            out: String::new(),
        }
    }

    fn send(&mut self, text: &str) {
        let stdin = self.stdin.as_mut().expect("stdin available");
        stdin.write_all(text.as_bytes()).expect("write CLI stdin");
        stdin.flush().expect("flush CLI stdin");
    }

    fn wait_for(&mut self, marker: &str) -> String {
        loop {
            let line = match self.lines.recv_timeout(std::time::Duration::from_secs(120)) {
                Ok(line) => line,
                Err(error) => panic!(
                    "no CLI output while waiting for {marker:?} ({error}); collected:\n{}",
                    self.out
                ),
            };
            self.out.push_str(&line);
            if line.contains(marker) {
                return line;
            }
        }
    }

    /// Wait for the first line containing any of `markers`.
    fn wait_for_any(&mut self, markers: &[&str]) -> String {
        loop {
            let line = match self.lines.recv_timeout(std::time::Duration::from_secs(120)) {
                Ok(line) => line,
                Err(error) => panic!(
                    "no CLI output while waiting for {markers:?} ({error}); collected:\n{}",
                    self.out
                ),
            };
            self.out.push_str(&line);
            if markers.iter().any(|marker| line.contains(marker)) {
                return line;
            }
        }
    }

    fn quit(mut self) {
        self.send(".quit\n");
        let _ = self.child.wait();
    }
}

/// Two real CLI processes attach writable to one `pink:` catalog and both
/// insert. They serialize on pinkdb's write lease: the first commit lands,
/// the second is the retryable "the stored difference cannot be applied"
/// conflict (the writable diff's base moved), and its re-attach + retry
/// lands. The final version count is exactly the two commits on top of the
/// create.
#[test]
fn two_cli_processes_commit_to_one_pink_catalog() {
    let dir = scratch("multiprocess");
    let (catalog, data) = paths(&dir);
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (0);\n\
         .lake save {catalog} data {data}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    let mut a = Cli::spawn();
    let mut b = Cli::spawn();
    for cli in [&mut a, &mut b] {
        cli.send(&format!(".lake attach {catalog} write\n"));
        cli.wait_for("writable");
    }
    a.send("INSERT INTO t VALUES (1);\n");
    a.wait_for("Inserted 1 row");
    b.send("INSERT INTO t VALUES (2);\n");
    b.wait_for("Inserted 1 row");

    // A lands first.
    a.send(&format!(".lake commit {catalog}\n"));
    a.wait_for("Committed lake version 3");
    // B's diff is based on version 2. Either the commit loop rebases and
    // lands v4 directly, or the table-changed conflict is reported and the
    // documented recovery (re-attach, re-apply, commit) lands v4.
    b.send(&format!(".lake commit {catalog}\n"));
    let outcome = b.wait_for_any(&[
        "Committed lake version 4",
        "changed in the lake since this change was computed",
    ]);
    if outcome.contains("Error") {
        b.send(&format!(".lake detach {catalog} force\n"));
        b.wait_for("Detached");
        b.send(&format!(".lake attach {catalog} write\n"));
        b.wait_for("writable");
        b.send("INSERT INTO t VALUES (2);\n");
        b.wait_for("Inserted 1 row");
        b.send(&format!(".lake commit {catalog}\n"));
        b.wait_for("Committed lake version 4");
    }
    a.quit();
    b.quit();

    let out = run(&format!(
        ".lake versions {catalog}\n\
         .lake open {catalog}\n\
         SELECT COUNT(*) FROM t;\n\
         .quit\n"
    ));
    assert!(out.contains("Opened lake version 4"), "{out}");
    assert!(out.contains("│ 3        │"), "{out}");
    cleanup(&dir);
}

/// Lake-wide maintenance and settings through the CLI: the SQL catalog must
/// see its tables, not a table-less head. Each command's effect is checked by
/// a later read, not just by the absence of an error.
#[test]
fn maintenance_and_settings_commands_work() {
    let dir = scratch("maintenance");
    let (catalog, data) = paths(&dir);

    // Flush: three inlined rows become one data file; values survive.
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'one');\n\
         .lake save {catalog} data {data}\n\
         .lake attach {catalog} write\n\
         INSERT INTO t VALUES (2, 'two'), (3, 'three'), (4, 'four');\n\
         .lake commit {catalog}\n\
         .lake detach {catalog}\n\
         .lake flush {catalog}\n\
         .lake open {catalog}\n\
         SELECT count(*) FROM t;\n\
         SELECT a, b FROM t ORDER BY a;\n\
         .quit\n"
    ));
    assert!(out.contains("Flushed 3 inlined rows"), "{out}");
    assert!(out.contains("│ 4        │"), "{out}");
    assert!(out.contains("│ 4       │ four    │"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    // Merge / rewrite / settings on a second catalog, with inlining off so
    // every commit is a data file.
    let (maint, maint_data) = (
        format!("{}maint", catalog),
        dir.join("maintenance-data").display().to_string(),
    );
    let mut script = format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {maint} data {maint_data}\n\
         .lake inlining {maint} t 0\n\
         .lake attach {maint} write\n"
    );
    for i in 2..6 {
        script.push_str(&format!(
            "INSERT INTO t VALUES ({i});\n.lake commit {maint}\n"
        ));
    }
    script.push_str(&format!(
        ".lake detach {maint}\n\
         .lake merge {maint}\n\
         .lake attach {maint} write\n\
         DELETE FROM t WHERE a = 1;\n\
         .lake commit {maint}\n\
         .lake detach {maint}\n\
         .lake rewrite {maint} delete_threshold=0.0\n\
         .lake partition {maint} t by (a)\n\
         .lake inlining {maint} t 5\n\
         .lake open {maint}\n\
         SELECT count(*) FROM t;\n\
         .quit\n"
    ));
    let out = run(&script);
    assert!(out.contains("Merged"), "{out}");
    assert!(!out.contains("Nothing to merge"), "{out}");
    assert!(out.contains("Rewrote"), "{out}");
    assert!(!out.contains("Nothing to rewrite"), "{out}");
    assert!(out.contains("Partitioned"), "{out}");
    assert!(out.contains("5 rows are stored"), "{out}");
    assert!(out.contains("│ 4        │"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    // Register an existing data file written by another lake.
    let source_catalog = format!("{}source", catalog);
    let source_data = dir.join("source-data").display().to_string();
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (7), (8);\n\
         .lake save {source_catalog} data {source_data}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let source_dir = std::path::Path::new(&source_data).join("data/main.t");
    let file = std::fs::read_dir(&source_dir)
        .expect("source data files")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .find(|name| name.ends_with(".pink"))
        .expect("one .pink data file");
    let rel = format!("data/main.t/{file}");
    let target_catalog = format!("{}target", catalog);
    let target_data = dir.join("target-data").display().to_string();
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         .lake save {target_catalog} data {target_data}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let target_file = std::path::Path::new(&target_data)
        .join("data/main.t")
        .join(&file);
    std::fs::create_dir_all(target_file.parent().unwrap()).unwrap();
    std::fs::copy(source_dir.join(&file), &target_file).unwrap();
    let out = run(&format!(
        ".lake add_data_files {target_catalog} t {rel}\n\
         .lake open {target_catalog}\n\
         SELECT count(*) FROM t;\n\
         .quit\n"
    ));
    assert!(out.contains("Registered"), "{out}");
    assert!(out.contains("2 rows"), "{out}");
    assert!(out.contains("│ 2        │"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    cleanup(&dir);
}
