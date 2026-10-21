//! M-9 regression: untrusted identifiers and name payloads never reach the
//! terminal raw. Covers local SQL (quoted identifiers may contain ESC) and a
//! hostile server that sends control bytes as column/object names.

#![cfg(feature = "client")]

mod common;

use std::io::BufReader;
use std::net::TcpListener;
use std::process::Command;

use pinkdb::DataType;
use pinkdb_server::protocol::{self, DoneKind, PROTOCOL_VERSION, Request, Response};

fn assert_no_raw_control(text: &str) {
    assert!(
        !text.chars().any(|c| c.is_control() && c != '\n'),
        "raw control character reached the terminal:\n{text:?}"
    );
}

#[test]
fn local_quoted_identifiers_are_rendered_safely() {
    let esc = '\u{1b}';
    let bel = '\u{7}';
    let table = format!("t{esc}[31mRED{esc}[0m");
    let view = format!("v{esc}]0;pwned{bel}");
    let db = format!("d{esc}[2J");
    let user = format!("u{esc}[1m");
    let role = format!("r{bel}");
    let column = format!("c{esc}[32m");

    let script = format!(
        "CREATE TABLE \"{table}\" ({column} INTEGER);\n\
         INSERT INTO \"{table}\" VALUES (1);\n\
         SELECT * FROM \"{table}\";\n\
         CREATE VIEW \"{view}\" AS SELECT 1 AS \"{column}\";\n\
         CREATE DATABASE \"{db}\";\n\
         CREATE USER \"{user}\" PASSWORD 'x';\n\
         CREATE ROLE \"{role}\";\n\
         GRANT SELECT ON TABLE \"{table}\" TO \"{role}\";\n\
         DROP VIEW \"{view}\";\n\
         .databases\n\
         .tables\n\
         .schema \"{table}\"\n\
         .quit\n"
    );
    let command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    let (text, status) = common::run_merged(command, &script);
    common::assert_script_exit(status);
    assert_no_raw_control(&text);
    assert!(
        text.contains("\\u{1b}"),
        "escapes should be visible:\n{text}"
    );
    // Legitimate UTF-8 still renders normally (the sanitizer is not ASCII-only).
    let command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    let (text, status) = common::run_merged(
        command,
        "CREATE TABLE \"Chloé\" (\"naïve\" VARCHAR);\n\
         INSERT INTO \"Chloé\" VALUES ('日本語');\n\
         SELECT * FROM \"Chloé\";\n\
         .schema \"Chloé\"\n.quit\n",
    );
    common::assert_script_exit(status);
    assert!(text.contains("Chloé") && text.contains("naïve"), "{text}");
    assert!(text.contains("日本語"), "{text}");
}

#[test]
fn hostile_server_column_names_are_sanitized() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let evil_name = "\u{1b}]0;pwned\u{7}".to_string();
    let evil_value = "\u{1b}[31mred\u{1b}[0m".to_string();
    let evil_object = "\u{1b}[2Jobject".to_string();

    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;
        assert!(matches!(
            protocol::read_request(&mut reader).unwrap(),
            Request::Hello { .. }
        ));
        protocol::write_response(
            &mut writer,
            &Response::HelloOk {
                server_version: "hostile".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        assert!(matches!(
            protocol::read_request(&mut reader).unwrap(),
            Request::Execute { .. }
        ));
        protocol::write_response(
            &mut writer,
            &Response::ResultSet {
                names: vec![evil_name.clone(), "ok".to_string()],
                types: vec![DataType::Varchar, DataType::Integer],
            },
        )
        .unwrap();
        protocol::write_response(
            &mut writer,
            &Response::Rows {
                rows: vec![vec![
                    pinkdb::Value::Varchar(evil_value.clone()),
                    pinkdb::Value::Integer(1),
                ]],
            },
        )
        .unwrap();
        protocol::write_response(
            &mut writer,
            &Response::Done {
                kind: DoneKind::Selected,
                count: 1,
                name: String::new(),
            },
        )
        .unwrap();
        protocol::write_response(&mut writer, &Response::BatchDone { statements: 1 }).unwrap();

        // A second statement: a non-row result carrying a hostile object name.
        assert!(matches!(
            protocol::read_request(&mut reader).unwrap(),
            Request::Execute { .. }
        ));
        protocol::write_response(
            &mut writer,
            &Response::Done {
                kind: DoneKind::CreatedView,
                count: 0,
                name: evil_object,
            },
        )
        .unwrap();
        protocol::write_response(&mut writer, &Response::BatchDone { statements: 1 }).unwrap();
    });

    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.args(["--host", &addr]);
    let (text, status) =
        common::run_merged(command, "SELECT 1;\nCREATE VIEW x AS SELECT 1;\n.quit\n");
    common::assert_script_exit(status);
    server.join().unwrap();
    assert_no_raw_control(&text);
    assert!(text.contains("\\u{1b}"), "{text}");
    assert!(text.contains("ok"), "{text}");
}
