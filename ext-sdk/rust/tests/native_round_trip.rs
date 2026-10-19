//! End-to-end test of the **native** (cdylib) path: the SDK's worked example
//! is compiled with `rustc` straight from source, loaded through the normal
//! `LOAD EXTENSION '<path>'` statement and queried through SQL.
//!
//! Why `rustc` and not a nested `cargo build`: a nested cargo invocation from
//! a test would contend for the target-directory lock, and the example is
//! deliberately a single file whose only dependency is the SDK. `cargo` still
//! builds the same source as a normal workspace member (the example package),
//! which is what `smoke.sh` and the CI smoke job drive; this test builds it
//! hermetically from the rlib the test binary already links.
//!
//! The example declares both deterministic functions and one volatile one, so
//! this file covers the flag through the *native* registration entry point
//! (`register_scalar_flags`), the C ABI's error mapping, and the engine's
//! refusal of volatile calls in WAL-logged statements — plus a durable
//! round trip that proves deterministic extension statements replay.

use std::path::{Path, PathBuf};
use std::process::Command;

use pinkdb::{PinkDb, QueryResult, Value};

/// Compile `example/string_ext/src/lib.rs` into a cdylib, once per process.
fn fixture_path() -> PathBuf {
    static BUILT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
            let source = manifest.join("example/string_ext/src/lib.rs");
            assert!(
                source.is_file(),
                "missing example source: {}",
                source.display()
            );

            // The SDK rlib cargo built for this test binary: `<target>/<profile>/deps/`.
            let exe = std::env::current_exe().expect("current test binary path");
            let deps = exe.parent().expect("the test binary lives in deps/");
            let rlib = newest_sdk_rlib(deps).unwrap_or_else(|| {
                panic!(
                    "no libpinkdb_ext_sdk-*.rlib in {} — run this test through `cargo test`",
                    deps.display()
                )
            });

            let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
                .join(format!("rust_sdk_native_{}", std::process::id()));
            std::fs::create_dir_all(&out_dir).expect("create fixture directory");
            let library = out_dir.join(format!(
                "librust_string_ext{}",
                std::env::consts::DLL_SUFFIX
            ));

            let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
            let output = Command::new(&rustc)
                .arg("--edition")
                .arg("2024")
                .arg("--crate-name")
                .arg("rust_string_ext")
                .arg("--crate-type")
                .arg("cdylib")
                .arg("-L")
                .arg(format!("dependency={}", deps.display()))
                .arg("--extern")
                .arg(format!("pinkdb_ext_sdk={}", rlib.display()))
                .arg("-o")
                .arg(&library)
                .arg(&source)
                .output()
                .unwrap_or_else(|e| panic!("could not run `{rustc}`: {e} (set RUSTC?)"));
            assert!(
                output.status.success(),
                "compiling the example failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            library
        })
        .clone()
}

/// The newest `libpinkdb_ext_sdk-*.rlib` in `deps`.
fn newest_sdk_rlib(deps: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(deps)
        .ok()?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            let matches = name.starts_with("libpinkdb_ext_sdk-") && name.ends_with(".rlib");
            if !matches {
                return None;
            }
            let modified = path.metadata().ok()?.modified().ok()?;
            Some((modified, path))
        })
        .collect();
    candidates.sort();
    candidates.pop().map(|(_, path)| path)
}

/// A fresh in-memory store with the example loaded via `LOAD EXTENSION`.
fn fixture_db() -> PinkDb {
    let path = fixture_path();
    let mut db = PinkDb::new();
    let name = db
        .load_extension(path.to_str().expect("UTF-8 fixture path"))
        .unwrap_or_else(|e| panic!("LOAD EXTENSION {}: {e}", path.display()));
    assert_eq!(name, "rust_string_ext");
    db
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Error message of a failing `sql`.
fn error(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(results) => panic!("expected an error for {sql:?}, got {results:?}"),
        Err(e) => e.to_string(),
    }
}

/// A scratch directory for the durable test.
fn scratch(tag: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("rust_sdk_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn the_example_loads_and_answers_typed_queries() {
    let mut db = fixture_db();
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_repeat('ab', CAST(3 AS BIGINT));"),
        vec![vec![Value::Varchar("ababab".to_string())]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_repeat('héllo', CAST(2 AS BIGINT));"),
        vec![vec![Value::Varchar("héllohéllo".to_string())]]
    );
    // Bytes, not characters — and composed with a built-in.
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_len('héllo') = 6, length('héllo') = 5, upper(ext_rs_repeat('ab', CAST(2 AS BIGINT))) = 'ABAB';"
        ),
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_next_day(DATE '2024-02-28') = DATE '2024-02-29';"
        ),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));"
        ),
        vec![vec![Value::Bigint(5)]]
    );
    // The extension's functions are visible to a `Connection` too.
    let shared = pinkdb::Database::from_store(db);
    let mut conn = shared.connect();
    assert_eq!(
        match conn
            .execute("SELECT ext_rs_len('abc');")
            .unwrap()
            .pop()
            .unwrap()
        {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        },
        vec![vec![Value::Bigint(3)]]
    );
}

#[test]
fn nulls_flow_through_option_parameters() {
    let mut db = fixture_db();
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_repeat(NULL, CAST(3 AS BIGINT)) IS NULL, ext_rs_repeat('x', NULL) IS NOT NULL, \
             ext_rs_len(NULL) IS NULL, ext_rs_next_day(NULL) IS NULL;"
        ),
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
    // NULL into a plain parameter: the statement fails with the ABI's code 1
    // (the SDK reports a value the declared signature does not accept).
    let message = error(
        &mut db,
        "SELECT ext_rs_clamp(NULL, CAST(1 AS BIGINT), CAST(2 AS BIGINT));",
    );
    assert!(
        message.contains("extension function ext_rs_clamp() failed with code 1"),
        "{message}"
    );
}

#[test]
fn returned_errors_fail_the_statement() {
    let mut db = fixture_db();
    let message = error(
        &mut db,
        "SELECT ext_rs_repeat('x', CAST(100000 AS BIGINT));",
    );
    assert!(
        message.contains("extension function ext_rs_repeat() failed with code 1"),
        "{message}"
    );
    let message = error(&mut db, "SELECT ext_rs_repeat('x', CAST(-1 AS BIGINT));");
    assert!(message.contains("failed with code 1"), "{message}");
}

#[test]
fn volatile_functions_are_refused_in_wal_logged_statements_but_reads_work() {
    let mut db = fixture_db();
    // A read may call it (and each call may return a fresh value).
    assert!(matches!(
        rows(&mut db, "SELECT ext_rs_tick();")[0][0],
        Value::Bigint(_)
    ));
    db.execute("CREATE TABLE t (a BIGINT);").unwrap();
    let message = error(&mut db, "INSERT INTO t VALUES (ext_rs_tick());");
    assert!(
        message.contains("ext_rs_tick()")
            && message.contains("volatile (nondeterministic)")
            && message.contains("cannot be called in a statement the engine WAL-logs"),
        "{message}"
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(0)]],
        "the refused statement must not have applied anything"
    );
}

#[test]
fn deterministic_extension_statements_wal_log_and_replay() {
    let fixture = fixture_path();
    let dir = scratch("native_replay");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{}'; CREATE TABLE t (a BIGINT);",
            fixture.display()
        ))
        .unwrap();
        db.execute("INSERT INTO t VALUES (ext_rs_clamp(CAST(7 AS BIGINT), CAST(0 AS BIGINT), CAST(5 AS BIGINT)));")
            .unwrap();
        db.execute("INSERT INTO t VALUES (ext_rs_len('abcd'));")
            .unwrap();
        // Refused, and therefore never logged:
        let err = db
            .execute("INSERT INTO t VALUES (ext_rs_tick());")
            .unwrap_err()
            .to_string();
        assert!(err.contains("volatile"), "{err}");
        db.flush_wal().unwrap();
        let wal =
            String::from_utf8_lossy(&std::fs::read(format!("{ckpt}.wal")).unwrap()).into_owned();
        assert!(!wal.contains("ext_rs_tick"), "{wal}");
        assert!(
            wal.contains("LOAD EXTENSION"),
            "the record is prefixed: {wal}"
        );
    }
    // Reopen: replay re-loads the extension from the record and re-applies the
    // deterministic statements byte for byte.
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Bigint(4)], vec![Value::Bigint(5)]]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Wave 54: a declaration that uses a ranged arity (`optional`/`variadic`)
/// cannot be registered by the native ABI v1, which carries a fixed argument
/// count and no lower bound. The SDK must fail the load with a clear
/// diagnostic instead of silently registering the function as fixed-arity at
/// its maximum (which would refuse the shorter calls the declaration
/// promises). The same source loads fine through the linked model — see
/// `linked_round_trip.rs`.
#[test]
fn a_ranged_declaration_is_refused_by_the_native_load() {
    /// A declaration whose only function is `optional(1)`.
    const SOURCE: &str = r#"
use pinkdb_ext_sdk::{Optional, pinkdb_extension};

pinkdb_extension! {
    name = "wave54_ranged_native",
    functions = [
        /// 1 or 2 arguments (a ranged arity: linked-only by design).
        optional(1) fn ext_ranged_tail(a: f64, b: Optional<f64>) -> f64 {
            b.value().copied().unwrap_or(a)
        },
    ],
}
"#;

    let exe = std::env::current_exe().expect("current test binary path");
    let deps = exe.parent().expect("the test binary lives in deps/");
    let rlib = newest_sdk_rlib(deps)
        .unwrap_or_else(|| panic!("no libpinkdb_ext_sdk-*.rlib in {}", deps.display()));
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("rust_sdk_ranged_{}", std::process::id()));
    std::fs::create_dir_all(&out_dir).expect("create fixture directory");
    let source = out_dir.join("ranged_ext.rs");
    std::fs::write(&source, SOURCE).expect("write the ranged fixture source");
    let library = out_dir.join(format!("libranged_ext{}", std::env::consts::DLL_SUFFIX));

    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let output = Command::new(&rustc)
        .arg("--edition")
        .arg("2024")
        .arg("--crate-name")
        .arg("ranged_ext")
        .arg("--crate-type")
        .arg("cdylib")
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("--extern")
        .arg(format!("pinkdb_ext_sdk={}", rlib.display()))
        .arg("-o")
        .arg(&library)
        .arg(&source)
        .output()
        .unwrap_or_else(|e| panic!("could not run `{rustc}`: {e} (set RUSTC?)"));
    assert!(
        output.status.success(),
        "compiling the ranged fixture failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut db = PinkDb::new();
    let message = db
        .load_extension(library.to_str().expect("UTF-8 fixture path"))
        .expect_err("a ranged declaration must not load natively")
        .to_string();
    // The loader reports the init code; the SDK's stderr diagnostic names the
    // function and the linked alternative.
    assert!(
        message.contains("failed to initialize") && message.contains("returned 5"),
        "{message}"
    );
    // Nothing was registered: a failed load is atomic.
    let message = error(&mut db, "SELECT ext_ranged_tail(1.0);");
    assert!(message.contains("unknown function"), "{message}");
    let _ = std::fs::remove_dir_all(&out_dir);
}
