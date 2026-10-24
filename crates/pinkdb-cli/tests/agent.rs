//! Wave 99: agent-friendly CLI mode. Every test forces the mode with a flag
//! except the detection test, so the suite is independent of the caller's
//! environment.

mod common;

use std::process::Command;

use common::{assert_script_exit, run_merged};

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
}

fn agent_cli() -> Command {
    let mut command = cli();
    command.arg("-agent");
    command
}

#[test]
fn markdown_tables_report_hidden_rows_and_columns() {
    let script = "\
.agent rows 2
.agent cols 2
select 1 as a, 'x' as b, true as c union all select 2, 'y', false \
union all select 3, 'z', true union all select 4, 'w', false;
select NULL as n, 1.0 as d;
";
    let (out, status) = run_merged(agent_cli(), script);
    assert_script_exit(status);
    assert!(
        out.contains("agent mode on: -agent was given"),
        "one announcement line expected:\n{out}"
    );
    assert!(out.contains("| a:INTEGER | b:VARCHAR |"), "{out}");
    assert!(out.contains("| 1 | x |"), "{out}");
    assert!(
        out.contains("… 2 more rows (4 total); 1 column hidden: c"),
        "explicit truncation notice expected:\n{out}"
    );
    assert!(out.contains("| NULL | 1.0 |"), "NULL and 1.0:\n{out}");
    assert!(!out.contains('┌'), "box renderer leaked:\n{out}");
}

#[test]
fn sql_errors_are_one_json_line_with_a_byte_position() {
    let (out, status) = run_merged(
        {
            let mut command = cli();
            command.args(["-agent", "-c", "selct 1;"]);
            command
        },
        "",
    );
    assert_eq!(status.code(), Some(1), "{out}");
    let line = out
        .lines()
        .find(|line| line.starts_with('{'))
        .unwrap_or_else(|| panic!("no JSON error line:\n{out}"));
    assert!(line.ends_with("\"position\":6}"), "{line}");
    assert!(line.contains("\"kind\":\"parse\""), "{line}");
    assert!(line.contains("\"error\":\"parse error"), "{line}");
}

#[test]
fn guard_cancels_the_statement_and_keeps_the_session_usable() {
    let script = "\
create table t as select 1 as a union all select 2 union all select 3 \
union all select 4 union all select 5 union all select 6;
.agent budget 5
select * from t;
select 1 as still_usable;
";
    let (out, status) = run_merged(agent_cli(), script);
    assert_eq!(status.code(), Some(1), "{out}");
    let guarded = out
        .find("result limit exceeded: statement returned more than 5 rows")
        .unwrap_or_else(|| panic!("guard message missing:\n{out}"));
    let after = out
        .find("still_usable")
        .unwrap_or_else(|| panic!("session unusable after the guard:\n{out}"));
    assert!(guarded < after, "{out}");
}

#[test]
fn cost_hint_names_the_estimated_scan() {
    let script = "\
create table t as select 1 as a union all select 2;
.agent hint 1
select * from t;
";
    let (out, status) = run_merged(agent_cli(), script);
    assert_script_exit(status);
    assert!(
        out.contains("agent hint: estimated 2 rows scanned (local scan)"),
        "{out}"
    );
}

#[test]
fn control_characters_are_escaped_never_raw() {
    let esc = '\u{1b}';
    let script = format!(
        "create table t (\"c{esc}[31m\" varchar);\n\
         insert into t values ('a{esc}[31mb');\n\
         select * from t;\n"
    );
    let (out, status) = run_merged(agent_cli(), &script);
    assert_script_exit(status);
    assert!(
        !out.chars().any(|c| c.is_control() && c != '\n'),
        "raw control character reached the terminal:\n{out:?}"
    );
    assert!(out.contains("\\u{1b}"), "escapes should be visible:\n{out}");
}

#[test]
fn an_agent_marker_plus_a_pipe_enables_auto_detection() {
    let (out, status) = run_merged(
        {
            let mut command = cli();
            // Cargo exports this to integration tests and the guard keeps
            // inherited markers from changing their output; remove it to
            // exercise the product's auto-detection.
            command
                .env_remove("PINKDB_AGENT_MODE")
                .env("AI_AGENT", "pi");
            command
        },
        "select 1 as a;\n",
    );
    assert_script_exit(status);
    assert!(
        out.contains("pinkdb agent mode on: AI_AGENT is set and stdout is not a terminal"),
        "{out}"
    );
    assert!(out.contains("| a:INTEGER |"), "{out}");
    assert!(!out.contains('┌'), "{out}");
}

#[test]
fn no_agent_keeps_the_box_renderer() {
    let (out, status) = run_merged(
        {
            let mut command = cli();
            command
                .env_remove("PINKDB_AGENT_MODE")
                .env("AI_AGENT", "pi")
                .arg("-no-agent");
            command
        },
        "select 1 as a;\n",
    );
    assert_script_exit(status);
    assert!(!out.contains("agent mode on"), "{out}");
    assert!(out.contains('┌'), "box renderer expected:\n{out}");
}

#[test]
fn help_agent_documents_the_mode() {
    let (out, status) = run_merged(
        {
            let mut command = cli();
            command.args(["-agent", "-c", ".help agent"]);
            command
        },
        "",
    );
    assert_script_exit(status);
    for needle in [
        "Agent mode:",
        "-agent / -no-agent",
        "PINKDB_AGENT_MODE",
        "PINKDB_AGENT_SCAN_ROWS",
        "runaway guard",
        "cost hint",
    ] {
        assert!(out.contains(needle), "missing {needle:?}:\n{out}");
    }
}
