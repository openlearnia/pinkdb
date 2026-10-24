//! ABI minor 2: the optional `pinkdb_ext_serve` entry point and the host
//! statement callback it calls.
//!
//! These are the loader's side of the contract (symbol detection, the API the
//! extension receives, the serving registry, transport grammar). The engine's
//! policy — what a statement may do — lives in `pinkdb::serve` and is covered
//! by its own tests.

mod common;

use std::path::PathBuf;
use std::sync::Mutex;

use pinkdb_ext_loader::{
    ExtError, LoadedExtension,
    serve::{self, StatementHost, Transport},
    text_allocation_stats,
};

/// The serve tests observe the process-wide text-allocation counters, so they
/// must not overlap each other (cargo runs one binary's tests in threads).
static SERIAL: Mutex<()> = Mutex::new(());

/// A statement host that records the SQL and answers with a canned reply.
struct Reply {
    code: i32,
    text: String,
    seen: Vec<String>,
}

impl Reply {
    fn ok(text: &str) -> Self {
        Reply {
            code: 0,
            text: text.to_string(),
            seen: Vec::new(),
        }
    }
}

impl StatementHost for Reply {
    fn execute_statement(&mut self, sql: &str) -> (i32, String) {
        self.seen.push(sql.to_string());
        (self.code, self.text.clone())
    }
}

/// A host that tries to serve the same library from inside a statement call.
struct Reenter {
    ext: LoadedExtension,
    second: Option<String>,
}

impl StatementHost for Reenter {
    fn execute_statement(&mut self, _sql: &str) -> (i32, String) {
        let mut inner = Reply::ok("inner");
        self.second = Some(match serve::serve(&self.ext, "stdio", "", &mut inner) {
            Ok(()) => "served twice".to_string(),
            Err(e) => e.to_string(),
        });
        (0, "outer".to_string())
    }
}

fn out_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pinkdb_serve_ext_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create serve test directory");
    dir.join(name)
}

/// Remove the file first: a stale one from an earlier run must not make the
/// assertions pass.
fn fresh_path(name: &str) -> PathBuf {
    let path = out_path(name);
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn serve_entry_point_runs_and_returns() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let ext = pinkdb_ext_loader::load(&common::extension_path("serve_ext")).unwrap();
    assert!(
        ext.serve_fn().is_some(),
        "the loader must detect pinkdb_ext_serve"
    );

    let path = fresh_path("stdio.txt");
    let before = text_allocation_stats();
    let mut host = Reply::ok("| a:INTEGER |\n|---|\n| 1 |");
    serve::serve(&ext, "stdio", path.to_str().unwrap(), &mut host).unwrap();

    assert_eq!(host.seen, vec!["SELECT 1".to_string()]);
    let written = std::fs::read_to_string(&path).expect("the extension wrote its report");
    assert!(
        written.starts_with("transport=stdio\ncode=0\n"),
        "unexpected report: {written}"
    );
    assert!(written.contains("| a:INTEGER |"), "answer text: {written}");
    let after = text_allocation_stats();
    assert_eq!(
        after.allocations - before.allocations,
        after.frees - before.frees,
        "the extension must free the host's text exactly once"
    );
}

#[test]
fn a_statement_error_reaches_the_extension_as_text() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let ext = pinkdb_ext_loader::load(&common::extension_path("serve_ext")).unwrap();
    let path = fresh_path("error.txt");
    let mut host = Reply {
        code: 1,
        text: r#"{"error":"read-only session","kind":"permission","position":null}"#.to_string(),
        seen: Vec::new(),
    };
    serve::serve(&ext, "stdio", path.to_str().unwrap(), &mut host).unwrap();
    let written = std::fs::read_to_string(&path).expect("the extension wrote its report");
    assert!(written.starts_with("transport=stdio\ncode=1\n"));
    assert!(
        written.contains("read-only session"),
        "answer text: {written}"
    );
}

#[test]
fn serving_the_same_library_twice_is_refused() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let ext = pinkdb_ext_loader::load(&common::extension_path("serve_ext")).unwrap();
    let path = fresh_path("reenter.txt");
    let mut host = Reenter {
        ext: ext.clone(),
        second: None,
    };
    serve::serve(&ext, "stdio", path.to_str().unwrap(), &mut host).unwrap();
    let second = host.second.expect("the callback ran");
    assert!(
        second.contains("cannot be loaded or served twice"),
        "unexpected nested serve result: {second}"
    );
}

#[test]
fn an_extension_without_the_symbol_is_refused() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    // A v1 functions-only extension still loads; it simply cannot serve.
    let ext = pinkdb_ext_loader::load(&common::extension_path("test_ext")).unwrap();
    assert!(ext.serve_fn().is_none());
    let mut host = Reply::ok("unused");
    match serve::serve(&ext, "stdio", "", &mut host) {
        Err(ExtError::NoServe { .. }) => {}
        other => panic!("expected NoServe, got {other:?}"),
    }
    assert!(host.seen.is_empty(), "no statement must run");
}

#[test]
fn a_bad_transport_is_refused_before_serving() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let ext = pinkdb_ext_loader::load(&common::extension_path("serve_ext")).unwrap();
    let mut host = Reply::ok("unused");
    match serve::serve(&ext, "tcp:127.0.0.1:1", "unused", &mut host) {
        Err(ExtError::BadTransport { message }) => {
            assert!(message.contains("stdio"), "unexpected message: {message}");
        }
        other => panic!("expected BadTransport, got {other:?}"),
    }
    assert!(host.seen.is_empty(), "no statement must run");
}

#[test]
fn transport_grammar_is_shared() {
    assert_eq!(serve::parse_transport("stdio").unwrap(), Transport::Stdio);
    let http = serve::parse_transport("http:127.0.0.1:8931").unwrap();
    assert_eq!(
        http,
        Transport::Http {
            host: "127.0.0.1".to_string(),
            port: 8931
        }
    );
    // IPv6 keeps its colons: the port is the last one.
    let v6 = serve::parse_transport("http:::1:8931").unwrap();
    assert_eq!(
        v6,
        Transport::Http {
            host: "::1".to_string(),
            port: 8931
        }
    );
    assert_eq!(http.descriptor(), "http:127.0.0.1:8931");
    assert!(matches!(
        serve::parse_transport("http:127.0.0.1"),
        Err(ExtError::BadTransport { .. })
    ));
    assert!(matches!(
        serve::parse_transport("http:127.0.0.1:notaport"),
        Err(ExtError::BadTransport { .. })
    ));
}
