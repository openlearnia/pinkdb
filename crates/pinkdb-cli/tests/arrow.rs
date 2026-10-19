//! End-to-end arrow file surfaces through the CLI: `COPY … (FORMAT arrow)`,
//! `FROM 'f.arrow'`, `read_arrow`, and the durable replay rule (a logged
//! statement that reads a file replays by re-reading it).
//!
//! Only compiles with the opt-in `arrow` feature; the feature-less behavior
//! is pinned by `tests/arrow_disabled.rs`.

#![cfg(feature = "arrow")]

mod common;

use std::path::PathBuf;
use std::process::Command;

/// The CLI output with all whitespace removed: box-drawing cell padding
/// depends on column widths, so assertions match the *values*, not the
/// layout.
fn flat(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

fn scratch_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_arrow_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn copy_read_and_alias_spellings() {
    let dir = scratch_dir("surfaces");
    let file = dir.join("t.arrow");
    let script = format!(
        "\
CREATE TABLE t (a INTEGER, s VARCHAR, d DOUBLE, dt DATE);
INSERT INTO t VALUES (1, 'x', 1.5, DATE '2024-01-01'), (2, NULL, NULL, NULL);
COPY t TO '{file}' (FORMAT arrow);
SELECT COUNT(*), SUM(a) FROM '{file}';
SELECT a, s FROM '{file}' WHERE a IS NOT NULL ORDER BY a;
SELECT COUNT(*) FROM read_arrow('{file}');
SELECT COUNT(*) FROM read_ipc('{file}');
SELECT COUNT(*) FROM read_feather('{file}');
.help
.quit
",
        file = file.display()
    );
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &script);
    common::assert_script_exit(status);
    let f = flat(&text);
    assert!(f.contains("│2│3│"), "{text}");
    assert!(f.contains("│1│x│"), "{text}");
    assert!(f.contains("ARROW:`FROM'f.arrow'`"), "{text}");
    assert!(!text.contains("Error:"), "{text}");
    assert!(file.exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn extension_inference_and_format_errors() {
    let dir = scratch_dir("errors");
    let script = format!(
        "\
CREATE TABLE t (a INTEGER);
INSERT INTO t VALUES (1);
COPY t TO '{dir}/inferred.arrow';
COPY t TO '{dir}/x.dat';
COPY t TO '{dir}/y.arrow' (FORMAT parquet);
COPY t TO '{dir}/z.dat' (FORMAT arrow);
SELECT * FROM '{dir}/missing.arrow';
SELECT * FROM read_nope('{dir}/inferred.arrow');
.quit
",
        dir = dir.display()
    );
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &script);
    common::assert_script_exit(status);
    let f = flat(&text);
    assert!(f.contains("Count"), "{text}");
    assert!(
        f.contains("Error:nofileformatisregisteredfor"),
        "a .dat target without FORMAT must be refused:\n{text}"
    );
    assert!(
        f.contains("nofileformat\"parquet\"isregistered"),
        "FORMAT wins for unknown names:\n{text}"
    );
    assert!(
        (f.contains("Error:cannotread") || f.contains("Error:cannotopen"))
            && f.contains("missing.arrow"),
        "{text}"
    );
    assert!(f.contains("unknowntablefunction\"read_nope\""), "{text}");
    assert!(dir.join("inferred.arrow").exists());
    assert!(
        !dir.join("x.dat").exists(),
        "the refused COPY wrote nothing"
    );
    assert!(
        dir.join("z.dat").exists(),
        "an explicit FORMAT writes the given path, whatever the extension"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The durable rule: `INSERT … SELECT … FROM 'f.arrow'` is WAL-logged as SQL
/// text and replays by re-reading the file (the CLI's `arrow` feature
/// registers the format process-wide *before* any store is opened, which is
/// what makes that replay resolvable). A file deleted before the reopen is a
/// hard recovery error, never a silent drop.
#[test]
fn durable_replay_re_reads_the_file() {
    let dir = scratch_dir("durable");
    let src = dir.join("src.arrow");
    let store = dir.join("store.pink");
    let script = format!(
        "\
CREATE TABLE src (a INTEGER);
INSERT INTO src VALUES (1), (2), (3);
COPY src TO '{src}' (FORMAT arrow);
.save {store}
.open {store}
CREATE TABLE dst (a INTEGER);
INSERT INTO dst SELECT a FROM '{src}' WHERE a > 1;
SELECT a FROM dst;
.quit
",
        src = src.display(),
        store = store.display()
    );
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &script);
    common::assert_script_exit(status);
    let f = flat(&text);
    assert!(f.contains("│2│"), "{text}");
    assert!(f.contains("│3│"), "{text}");

    // Reopen: the logged INSERT re-reads the file and lands the same rows.
    let script = format!(
        ".open {}\nSELECT COUNT(*), SUM(a) FROM dst;\n.quit\n",
        store.display()
    );
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &script);
    common::assert_script_exit(status);
    assert!(
        text.contains("Replayed"),
        "the reopen must replay the logged INSERT:\n{text}"
    );
    assert!(flat(&text).contains("│2│5│"), "{text}");

    // The file is gone: the replay is a hard error, never a silent drop.
    std::fs::remove_file(&src).unwrap();
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &script);
    common::assert_script_exit(status);
    assert!(text.contains("Error:"), "{text}");
    assert!(
        (flat(&text).contains("cannotread") || flat(&text).contains("cannotopen"))
            && text.contains("src.arrow"),
        "the recovery error must name the missing file:\n{text}"
    );
    std::fs::remove_dir_all(&dir).ok();
}
