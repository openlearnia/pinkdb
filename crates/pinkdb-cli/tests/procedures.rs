//! Stored procedures through the CLI, the script runner and the server path
//! (wave 44).
//!
//! The SQL semantics live in `pinkdb::procedure`'s unit tests; this file pins
//! the *integration* surfaces: the CLI's rendering of `CALL`, `CALL` inside a
//! `.read`/script run, the remote client (server) round-trip of a call
//! result, and the refusal of a body statement that calls a loadable-extension
//! function (extension callbacks are opaque, so a durable store could not
//! replay the logged `CALL`).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

mod common;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

/// stdout, stderr, exit code — separately.
fn run_split(args: &[&str], stdin: &str) -> (String, String, Option<i32>) {
    use std::io::Write;
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

/// A scratch directory that cleans itself up.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("pinkdb_cli_proc_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn file(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn path(&self, name: &str) -> String {
        self.file(name).to_string_lossy().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The CLI renders a call's own result and its statement count.
#[test]
fn cli_renders_procedure_results() {
    let (out, err, code) = run_split(
        &[],
        "CREATE TABLE t (a INTEGER);\n\
         CREATE PROCEDURE ins(v INTEGER) AS $$ BEGIN DECLARE n INTEGER DEFAULT v * 10; \
           INSERT INTO t VALUES (n); END $$;\n\
         CALL ins(4);\n\
         SELECT a FROM t;\n\
         DROP PROCEDURE ins;\n\
         CALL ins(1);\n",
    );
    assert_eq!(code, Some(1), "the last CALL fails");
    assert!(err.contains("not found"), "{err}");
    assert!(out.contains("Procedure ins created."), "{out}");
    assert!(out.contains("Procedure ins called (1 statement)."), "{out}");
    assert!(out.contains("Procedure ins dropped."), "{out}");
    // The call's own result (the INSERT) renders like any other statement.
    assert!(out.contains("Inserted 1 row(s)."), "{out}");
    assert!(out.contains("40"), "the inserted value: {out}");
}

/// A `SELECT`-shaped call result (the body's last statement) renders as rows
/// under the call's own line.
#[test]
fn cli_renders_a_call_returning_rows() {
    let (out, err, code) = run_split(
        &[],
        "CREATE PROCEDURE answer() AS $$ BEGIN SELECT 41 + 1 AS answer; END $$;\nCALL answer();\n",
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(
        out.contains("Procedure answer called (1 statement)."),
        "{out}"
    );
    // The nested result set is drawn by the shared renderer.
    assert!(out.contains("answer"), "{out}");
    assert!(out.contains("42"), "{out}");
}

/// `CALL` in a script file (`.read`) and in a duckdb-style `-c` run.
#[test]
fn call_in_scripts_and_c() {
    let s = Scratch::new("script");
    let script = s.file("procs.sql");
    std::fs::write(
        &script,
        "CREATE TABLE log (msg VARCHAR);\n\
         CREATE PROCEDURE note(m VARCHAR) AS $$ BEGIN INSERT INTO log VALUES (m); END $$;\n\
         CALL note('from script');\n",
    )
    .unwrap();
    let (out, err, code) = run_split(
        &[],
        &format!(
            ".read {}\nCALL note('from read');\nSELECT msg FROM log;\n",
            script.display()
        ),
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(
        out.contains("from script") && out.contains("from read"),
        "{out}"
    );
    let (out, err, code) = run_split(&["-c", "SELECT 1;"], "");
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains('1'), "{out}");
}

/// A durable database keeps its procedures across a CLI restart (the `.pink`
/// checkpoint and the WAL both carry them).
#[test]
fn durable_cli_session_keeps_procedures() {
    let s = Scratch::new("durable");
    let path = s.path("db.pink");
    let (out, err, code) = run_split(
        &[],
        &format!(
            ".open {path}\n\
             CREATE TABLE t (a INTEGER);\n\
             CREATE PROCEDURE ins(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v); END $$;\n\
             CALL ins(1);\n\
             CHECKPOINT;\n\
             CALL ins(2);\n"
        ),
    );
    assert_eq!(code, Some(0), "{err}\n{out}");
    // A fresh process: the checkpoint's definition and the WAL tail replay.
    let (out, err, code) = run_split(
        &[],
        &format!(".open {path}\nCALL ins(3);\nSELECT a FROM t;\n"),
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("Procedure ins called (1 statement)."), "{out}");
    for v in [1, 2, 3] {
        assert!(out.contains(&v.to_string()), "{v} missing: {out}");
    }
}

/// Compile the fixture extension once per test process (the same `cc` pattern
/// the loader and repository tests use).
fn fixture_extension() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
            let source = manifest.join("tests/fixtures/proc_ext.c");
            let include = manifest.join("../pinkdb-ext-loader/include");
            let dir =
                std::env::temp_dir().join(format!("pinkdb_cli_proc_ext_{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create fixture extension directory");
            let library = dir.join(format!("proc_ext{}", std::env::consts::DLL_SUFFIX));
            let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
            let out = Command::new(&cc)
                .arg("-std=c99")
                .arg("-O1")
                .arg("-fPIC")
                .arg("-shared")
                .arg("-I")
                .arg(&include)
                .arg("-o")
                .arg(&library)
                .arg(&source)
                .output()
                .unwrap_or_else(|e| panic!("could not run the C compiler `{cc}`: {e} (set CC?)"));
            assert!(
                out.status.success(),
                "compiling proc_ext.c failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            library
        })
        .clone()
}

/// A body statement that calls a loadable-extension function is refused when
/// the call runs: a callback is opaque (impure, nondeterministic or failing),
/// so the logged `CALL` could not replay. The refusal names the statement.
#[test]
fn extension_calls_inside_a_body_are_refused() {
    let ext = fixture_extension();
    let (out, err, code) = run_split(
        &[],
        &format!(
            "LOAD EXTENSION '{}';\n\
             CREATE TABLE t (a INTEGER);\n\
             INSERT INTO t VALUES (1);\n\
             CREATE PROCEDURE uses_ext(v BIGINT) AS $$ BEGIN \
               INSERT INTO t VALUES (CAST(ext_proc_identity(v) AS INTEGER)); \
             END $$;\n\
             CALL uses_ext(5);\n",
            ext.display()
        ),
    );
    assert_eq!(code, Some(1), "{out}");
    assert!(err.contains("loadable-extension"), "{err}");
    // The call is atomic: the refused statement left nothing behind.
    let (out, err, code) = run_split(
        &[],
        &format!(
            "LOAD EXTENSION '{}';\n\
             CREATE TABLE t (a INTEGER);\n\
             CREATE PROCEDURE no_ext(v INTEGER) AS $$ BEGIN \
               DECLARE w INTEGER DEFAULT v * 2; \
               INSERT INTO t VALUES (w); \
             END $$;\n\
             CALL no_ext(5);\n\
             SELECT a FROM t;\n",
            ext.display()
        ),
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("Procedure no_ext called"), "{out}");
    assert!(out.contains("10"), "the body's own arithmetic ran: {out}");
}

/// The remote (server) path: a `CALL` streams its rows and the client rebuilds
/// the nested result; a plain call reports its outcome.
#[cfg(feature = "client")]
#[test]
fn remote_calls_round_trip() {
    use pinkdb::Database;
    use pinkdb_server::Server;

    let server = Server::bind("127.0.0.1:0", Database::new()).expect("bind");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn");
    let mut client = pinkdb_server::Client::connect(&addr).unwrap();
    client
        .execute(
            "CREATE TABLE t (a INTEGER); \
             CREATE PROCEDURE ins(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v); END $$;",
        )
        .unwrap();
    let out = client.execute("CALL ins(7);").unwrap();
    match &out[0] {
        pinkdb::QueryResult::Called {
            name, statements, ..
        } => {
            assert_eq!(name, "ins");
            assert_eq!(*statements, 1);
        }
        other => panic!("expected a Called result, got {other:?}"),
    }
    // A call whose last statement is a SELECT streams its rows, nested back
    // into `Called.result` by the client.
    client
        .execute("CREATE PROCEDURE peek() AS $$ BEGIN SELECT a FROM t ORDER BY a; END $$;")
        .unwrap();
    let out = client.execute("CALL peek();").unwrap();
    match &out[0] {
        pinkdb::QueryResult::Called {
            result: Some(inner),
            ..
        } => match &**inner {
            pinkdb::QueryResult::Selected { rows, .. } => {
                assert_eq!(rows, &vec![vec![pinkdb::Value::Integer(7)]]);
            }
            other => panic!("expected rows, got {other:?}"),
        },
        other => panic!("expected a nested result, got {other:?}"),
    }
    // The remote REPL renders the same outcome.
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.args(["--host", &addr]);
    let (text, status) = common::run_merged(command, "CALL peek();\n");
    common::assert_script_exit(status);
    assert!(
        text.contains("Procedure peek called (1 statement)."),
        "{text}"
    );
    assert!(text.contains("│ 7"), "{text}");
    handle.join();
}
