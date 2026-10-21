//! Shared CLI test plumbing.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

/// Run `command` with `stdin` as its input and return its stdout and stderr
/// interleaved in write order (one pipe for both, like `2>&1`) plus the exit
/// status. Errors go to stderr since the CLI follows sqlite3/duckdb
/// conventions, so tests that look for `Error:` lines see them in place.
pub fn run_merged(mut command: Command, stdin: &str) -> (String, ExitStatus) {
    let (mut reader, writer) = std::io::pipe().expect("create pipe");
    command
        .stdin(Stdio::piped())
        .stdout(writer.try_clone().expect("clone pipe writer"))
        .stderr(writer);
    let mut child = command.spawn().expect("spawn pinkdb CLI");
    // The parent's copies of the write end live in `command`; drop them so
    // the reader sees EOF when the child exits.
    drop(command);
    let mut child_stdin = child.stdin.take().expect("child stdin");
    let input = stdin.to_string();
    let feeder = std::thread::spawn(move || {
        let _ = child_stdin.write_all(input.as_bytes());
    });
    let mut out = String::new();
    reader.read_to_string(&mut out).expect("read CLI output");
    feeder.join().expect("stdin feeder");
    let status = child.wait().expect("wait for the CLI");
    (out, status)
}

/// Whether `line` is exactly a `.timer on` line with the phase breakdown:
/// `^Run Time \(s\): real [0-9]+\.[0-9]{3} \(parse [0-9.]+, bind [0-9.]+,
/// optimize [0-9.]+, execute [0-9.]+, log [0-9.]+ ms\)$`.
pub fn is_timer_line(line: &str) -> bool {
    fn digits(s: &str) -> bool {
        s.chars().all(|c| c.is_ascii_digit())
    }
    fn digits_and_dots(s: &str) -> bool {
        !s.is_empty() && s.chars().all(|c| c.is_ascii_digit() || c == '.')
    }
    let Some(rest) = line.strip_prefix("Run Time (s): real ") else {
        return false;
    };
    let Some((real, rest)) = rest.split_once(" (parse ") else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(" ms)") else {
        return false;
    };
    let Some((int, frac)) = real.split_once('.') else {
        return false;
    };
    if int.is_empty() || !digits(int) || frac.len() != 3 || !digits(frac) {
        return false;
    }
    let phases: Vec<&str> = rest.split(", ").collect();
    phases.len() == 5
        && digits_and_dots(phases[0])
        && ["bind ", "optimize ", "execute ", "log "]
            .iter()
            .zip(&phases[1..])
            .all(|(label, phase)| phase.strip_prefix(label).is_some_and(digits_and_dots))
}

/// A clean script exit: 0 (no errors) or 1 (a statement or command failed);
/// never a signal or a panic exit code.
pub fn assert_script_exit(status: ExitStatus) {
    assert!(
        matches!(status.code(), Some(0) | Some(1)),
        "CLI did not exit cleanly: {status}"
    );
}

/// A deterministic corpus for the `.lake attach` equivalence tests (shared by
/// the full `tests/lake.rs` and the lean `tests/pinklake.rs`): the SQL script
/// is returned, two CSVs are written into `dir`. Everything the design calls
/// out: all-NULL column, empty table, NaN/±0 payloads, negative ints and
/// extremes, long/outlined strings, and a >65,000-row imported table.
pub fn attach_corpus(dir: &Path) -> String {
    // `big`: 70,000 rows, strings of mixed lengths.
    let big_csv = dir.join("big.csv");
    let mut csv = String::from("a,s\n");
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    for i in 0..70_000i64 {
        let r = next();
        let word_len = (r % 200) as usize;
        csv.push_str(&format!("{i},w{}-{}\n", r % 1000, "y".repeat(word_len)));
    }
    std::fs::write(&big_csv, csv).unwrap();

    // `nan`: DOUBLE payloads that SQL literals cannot express.
    let nan_csv = dir.join("nan.csv");
    std::fs::write(&nan_csv, "d\n1.5\nNaN\n-0.0\n3.25\n\n0.0\n").unwrap();

    let mut sql = String::new();
    sql.push_str("CREATE DATABASE lake;\nUSE lake;\n");
    sql.push_str(
        "CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN, \
         dt DATE, ts TIMESTAMP, n INTEGER);\n",
    );
    // 300 rows: a NULL pattern, extremes, ±0, and long strings.
    let mut state = 0x1234_5678_9ABC_DEF0u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let mut rows = Vec::new();
    for i in 0..300i64 {
        let r = next();
        let d = if i == 7 {
            "-0.0".to_string()
        } else {
            format!("{:.3}", (r % 4000) as f64 / 8.0 - 250.0)
        };
        let s = if i % 5 == 0 {
            format!("'{}'", "x".repeat((r % 300) as usize + 1))
        } else if i % 11 == 0 {
            "NULL".to_string()
        } else {
            format!("'k{}-{}'", r % 100, i % 3)
        };
        let dt = (r % 20000) as i64 - 10000;
        rows.push(format!(
            "({}, {}, {}, {}, {}, {}, {}, NULL)",
            (r % 500) as i64 - 250,
            if i == 1 {
                "-9223372036854775808".to_string()
            } else if i == 2 {
                "9223372036854775807".to_string()
            } else {
                format!("{}", r as i64)
            },
            d,
            s,
            r % 2 == 0,
            dt,
            r as i64 - 5000,
        ));
    }
    sql.push_str(&format!("INSERT INTO t VALUES {};\n", rows.join(", ")));
    sql.push_str("CREATE TABLE empty (x INTEGER, y VARCHAR);\n");
    sql.push_str(&format!(".import {} nan\n", nan_csv.display()));
    sql.push_str(&format!(".import {} big\n", big_csv.display()));
    sql
}

/// The identical query batch every `.lake attach` arm runs (both builds).
/// Mixes stats-answerable shapes (COUNT/COUNT(col)/MIN/MAX) with shapes that
/// must fall back (SUM/AVG, filter, group by, order by, joins, expressions)
/// so the transcripts prove both paths agree.
pub fn attach_corpus_queries() -> &'static str {
    "SELECT '===BEGIN===' AS marker;\n\
     SELECT COUNT(*) FROM t;\n\
     SELECT COUNT(n), COUNT(s), COUNT(d), COUNT(b) FROM t;\n\
     SELECT MIN(i), MAX(i), MIN(g), MAX(g) FROM t;\n\
     SELECT MIN(s), MAX(s) FROM t;\n\
     SELECT MIN(b), MAX(b), MIN(dt), MAX(dt), MIN(ts), MAX(ts) FROM t;\n\
     SELECT MIN(d), MAX(d), COUNT(d) FROM t;\n\
     SELECT COUNT(*) FROM empty;\n\
     SELECT COUNT(x), MIN(x), MAX(y) FROM empty;\n\
     SELECT x, y FROM empty;\n\
     SELECT y FROM empty ORDER BY x;\n\
     SELECT * FROM empty;\n\
     SELECT COUNT(*), COUNT(d), MIN(d), MAX(d) FROM nan;\n\
     SELECT COUNT(*) FROM big;\n\
     SELECT COUNT(s), MIN(a), MAX(a) FROM big;\n\
     SELECT MIN(s), MAX(s) FROM big;\n\
     SELECT COUNT(*) FROM big WHERE a > 0;\n\
     SELECT s FROM big ORDER BY s LIMIT 2;\n\
     SELECT a, s FROM big ORDER BY a DESC LIMIT 2;\n\
     CREATE VIEW tv AS SELECT i, s FROM t;\n\
     SELECT COUNT(*), MAX(i), MAX(s) FROM tv;\n\
     SELECT s FROM tv ORDER BY i LIMIT 2;\n\
     DROP VIEW tv;\n\
     SELECT SUM(i), AVG(i), SUM(d) FROM t;\n\
     SELECT COUNT(*) FROM t WHERE i > 0;\n\
     SELECT b, COUNT(*) FROM t GROUP BY b ORDER BY b;\n\
     SELECT i, g FROM t ORDER BY i DESC LIMIT 5;\n\
     SELECT MAX(i) FROM (SELECT i FROM t) s;\n\
     SELECT MIN(ABS(i)) FROM t;\n\
     SELECT t.i, big.a FROM t JOIN big ON t.i = big.a ORDER BY t.i LIMIT 5;\n\
     SELECT * FROM t ORDER BY i LIMIT 3;\n\
     SELECT s FROM t ORDER BY i DESC LIMIT 3;\n\
     SELECT d FROM t WHERE d < 0 ORDER BY d LIMIT 4;\n\
     SELECT dt, COUNT(*) FROM t GROUP BY dt ORDER BY dt LIMIT 4;\n\
     SELECT i, ROW_NUMBER() OVER (PARTITION BY b ORDER BY i) FROM t ORDER BY i LIMIT 5;\n\
     SELECT ROW_NUMBER() OVER () FROM t LIMIT 2;\n\
     WITH w AS (SELECT i, s FROM t) SELECT MAX(i), COUNT(s) FROM w;\n\
     SELECT COUNT(*) FROM (SELECT d FROM t) x;\n\
     SELECT 1 FROM t LIMIT 1;\n\
     SELECT DISTINCT b FROM t ORDER BY b;\n\
     SELECT i FROM t UNION ALL SELECT a FROM big LIMIT 3;\n\
     SELECT (SELECT SUM(d) FROM t) AS m, (SELECT COUNT(s) FROM t) AS c FROM t LIMIT 2;\n"
}

/// The output from the shared `===BEGIN===` sentinel on (so the arm-specific
/// setup lines are excluded).
pub fn tail_after_marker(out: &str) -> &str {
    let idx = out
        .find("===BEGIN===")
        .unwrap_or_else(|| panic!("sentinel missing from output:\n{out}"));
    &out[idx..]
}
