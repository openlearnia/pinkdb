//! `.timer` (wave 84): each SQL string prints the historical
//! `Run Time (s): real X.XXX` prefix followed by the engine's summed phase
//! breakdown; turning the timer off restores the plain output byte for byte.

use std::process::Command;

mod common;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

/// The statements both arms run; `timed` wraps them in `.timer on` / a
/// trailing statement after `.timer off`.
fn statements() -> &'static str {
    "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);\n\
     SELECT a FROM t ORDER BY a;\n\
     UPDATE t SET a = a + 1;\n\
     SELECT COUNT(*) FROM t;\n"
}

#[test]
fn timer_reports_a_phase_breakdown_per_sql_string() {
    let script = format!(
        ".timer on\n{}.timer off\nSELECT 99 AS answer;\n",
        statements()
    );
    let (out, status) = common::run_merged(cli(), &script);
    assert_eq!(status.code(), Some(0), "{out}");

    let timer_lines: Vec<&str> = out
        .lines()
        .filter(|line| line.starts_with("Run Time (s):"))
        .collect();
    // Four SQL strings ran while the timer was on; the statement after
    // `.timer off` produced no line.
    assert_eq!(
        timer_lines.len(),
        4,
        "expected one timer line per SQL string:\n{out}"
    );
    for line in &timer_lines {
        assert!(
            common::is_timer_line(line),
            "timer line does not match the breakdown format: {line:?}\n{out}"
        );
    }
}

#[test]
fn timer_off_leaves_results_unchanged() {
    let plain_script = format!("{}SELECT 99 AS answer;\n", statements());
    let (plain, status) = common::run_merged(cli(), &plain_script);
    assert_eq!(status.code(), Some(0), "{plain}");

    let script = format!(
        ".timer on\n{}.timer off\nSELECT 99 AS answer;\n",
        statements()
    );
    let (timed, status) = common::run_merged(cli(), &script);
    assert_eq!(status.code(), Some(0), "{timed}");

    // Removing only the timer lines reproduces the untimed run exactly.
    let stripped: Vec<&str> = timed
        .lines()
        .filter(|line| !line.starts_with("Run Time (s):"))
        .collect();
    let plain_lines: Vec<&str> = plain.lines().collect();
    assert_eq!(stripped, plain_lines, "timer changed the rendered results");
}

#[test]
fn timer_off_produces_no_timer_line() {
    let (out, status) = common::run_merged(cli(), ".timer off\nSELECT 1 AS a;\n");
    assert_eq!(status.code(), Some(0), "{out}");
    assert!(!out.contains("Run Time"), "{out}");
}
