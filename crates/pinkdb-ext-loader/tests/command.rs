//! ABI minor 4: the optional `pinkdb_ext_command` entry point, its sink, and
//! the `execute_internal` host channel.
//!
//! The loader's side of the contract only: symbol detection, both statement
//! channels wired, chunks on both sink channels, and the one-call-at-a-time
//! rule shared with `serve`. What a statement may do is the engine's policy,
//! covered by `pinkdb::serve` and the CLI's agent tests.

mod common;

use pinkdb_ext_loader::{CommandHost, ExtError, LoadedExtension, command, text_allocation_stats};

/// The loader allows one running command per library *process-wide*, and every
/// test here runs the same `command_ext`, so the tests take turns: run in
/// parallel, one test's call made another's fail with `Serving`.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A host that records both channels and streams chunks into a vector.
struct Recorder {
    internal: Vec<String>,
    statements: Vec<String>,
    chunks: Vec<(i32, String)>,
    /// `Some(ext)` makes the internal channel call back into the same library.
    reenter: Option<LoadedExtension>,
    second: Option<String>,
}

impl Recorder {
    fn new() -> Self {
        Recorder {
            internal: Vec::new(),
            statements: Vec::new(),
            chunks: Vec::new(),
            reenter: None,
            second: None,
        }
    }
}

impl CommandHost for Recorder {
    fn execute_statement(&mut self, sql: &str) -> (i32, String) {
        self.statements.push(sql.to_string());
        (0, "| a:INTEGER |\n|---|\n| 2 |".to_string())
    }

    fn execute_internal(&mut self, sql: &str) -> (i32, String) {
        self.internal.push(sql.to_string());
        if let Some(ext) = &self.reenter {
            let mut inner = Recorder::new();
            self.second = Some(match command::run_command(ext, "inner", &mut inner) {
                Ok(()) => "ran twice".to_string(),
                Err(e) => e.to_string(),
            });
        }
        (0, r#"[{"columns":["ok"],"rows":[[true]]}]"#.to_string())
    }

    fn emit(&mut self, channel: i32, text: &str) {
        self.chunks.push((channel, text.to_string()));
    }
}

#[test]
fn command_entry_point_runs_and_returns() {
    let _serial = serial();
    let ext = pinkdb_ext_loader::load(&common::extension_path("command_ext")).unwrap();
    assert!(
        ext.command_fn().is_some(),
        "the loader must detect pinkdb_ext_command"
    );
    assert!(
        ext.serve_fn().is_none(),
        "a command extension need not export pinkdb_ext_serve"
    );

    let before = text_allocation_stats();
    let mut host = Recorder::new();
    command::run_command(&ext, "ask \"top 3\"", &mut host).unwrap();

    assert_eq!(
        host.internal,
        vec!["INSERT INTO pinkdb_agent.messages VALUES (1)".to_string()],
        "the internal channel carries the extension's own bookkeeping"
    );
    assert_eq!(
        host.statements,
        vec!["SELECT 2".to_string()],
        "the statement channel carries model tool calls"
    );
    assert_eq!(
        host.chunks,
        vec![
            (0, r#"[{"columns":["ok"],"rows":[[true]]}]"#.to_string()),
            (1, "| a:INTEGER |\n|---|\n| 2 |".to_string()),
            (0, "ask \"top 3\"".to_string()),
        ],
        "chunks keep their channel and their bytes"
    );
    let after = text_allocation_stats();
    assert_eq!(
        after.allocations - before.allocations,
        after.frees - before.frees,
        "the extension must free the host's text exactly once"
    );
    assert!(
        !command::is_active(&ext.canonical_path()),
        "the one-call lock is released when the command returns"
    );
}

#[test]
fn an_extension_without_the_symbol_is_refused() {
    let _serial = serial();
    // A v1 functions-only extension and a minor-2 serve extension both still
    // load; they simply have no commands.
    for name in ["test_ext", "serve_ext"] {
        let ext = pinkdb_ext_loader::load(&common::extension_path(name)).unwrap();
        assert!(ext.command_fn().is_none(), "{name} exports no command");
        let mut host = Recorder::new();
        match command::run_command(&ext, "", &mut host) {
            Err(ExtError::NoCommand { path }) => {
                assert!(path.contains(name), "unexpected path: {path}");
            }
            other => panic!("expected NoCommand, got {other:?}"),
        }
        assert!(host.statements.is_empty(), "no statement must run");
        assert!(host.internal.is_empty(), "no statement must run");
    }
}

#[test]
fn running_the_same_library_twice_is_refused() {
    let _serial = serial();
    let ext = pinkdb_ext_loader::load(&common::extension_path("command_ext")).unwrap();
    let mut host = Recorder::new();
    host.reenter = Some(ext.clone());
    command::run_command(&ext, "outer", &mut host).unwrap();
    let second = host.second.expect("the internal channel ran");
    assert!(
        second.contains("cannot be loaded or served twice"),
        "unexpected nested command result: {second}"
    );
    assert!(
        !command::is_active(&ext.canonical_path()),
        "the outer call still released its lock"
    );
}

#[test]
fn a_statement_failure_is_reported_as_a_failed_command() {
    let _serial = serial();
    // `command_ext` returns the first non-zero code it saw; the host answers
    // the statement channel with `1` when the policy refused it.
    struct Refusing(Recorder);
    impl CommandHost for Refusing {
        fn execute_statement(&mut self, sql: &str) -> (i32, String) {
            self.0.execute_statement(sql);
            (
                1,
                r#"{"error":"read-only session","kind":"permission","position":null}"#.to_string(),
            )
        }
        fn execute_internal(&mut self, sql: &str) -> (i32, String) {
            self.0.execute_internal(sql)
        }
        fn emit(&mut self, channel: i32, text: &str) {
            self.0.emit(channel, text);
        }
    }
    let ext = pinkdb_ext_loader::load(&common::extension_path("command_ext")).unwrap();
    let mut host = Refusing(Recorder::new());
    match command::run_command(&ext, "", &mut host) {
        Err(ExtError::CommandFailed { code, path }) => {
            assert_eq!(code, 1);
            assert!(path.contains("command_ext"), "unexpected path: {path}");
        }
        other => panic!("expected CommandFailed, got {other:?}"),
    }
    assert!(
        host.0
            .chunks
            .iter()
            .any(|(channel, text)| *channel == 1 && text.contains("read-only session")),
        "the refusal text reached the extension's sink: {:?}",
        host.0.chunks
    );
}
