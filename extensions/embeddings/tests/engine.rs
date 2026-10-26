//! Wave 107c: `embed(text)` through the real engine and the recorded
//! provider in `support`.
//!
//! Every test points the extension at a loopback mock, so nothing here
//! needs a network, a key, or a provider; the live path is
//! `tests/live.rs`, ignored unless asked for.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pinkdb::serve::{ServePolicy, execute};
use pinkdb::{ArrayElem, DataType, DurableOpenOptions, PinkDb, QueryResult, Value};
use serde_json::json;
use support::{Env, Mock, Reply, SENTINEL, engine_env};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

fn count(db: &mut PinkDb, sql: &str) -> i64 {
    match &rows(db, sql)[0][0] {
        Value::Integer(value) => i64::from(*value),
        Value::Bigint(value) => *value,
        other => panic!("expected a count from {sql}, got {other:?}"),
    }
}

/// The `FLOAT[N]` elements of one cell, as the engine stores them.
fn floats(cell: &Value) -> Vec<f32> {
    match cell {
        Value::Array(array) => array.floats32().expect("a FLOAT array").to_vec(),
        other => panic!("expected an array, got {other:?}"),
    }
}

fn expected(text: &str) -> Vec<f32> {
    support::vector(text, 4)
        .iter()
        .map(|value| *value as f32)
        .collect()
}

fn bearer() -> String {
    format!("Bearer {SENTINEL}")
}

#[test]
fn a_call_carries_the_model_inputs_dimensions_and_bearer_key() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    db.execute("CREATE TABLE docs (body VARCHAR)").unwrap();
    db.execute("INSERT INTO docs VALUES ('first document'), ('second document')")
        .unwrap();

    let results = db
        .execute("SELECT embed(body), typeof(embed(body)) FROM docs")
        .unwrap();
    let QueryResult::Selected { types, rows, .. } = results.into_iter().next().unwrap() else {
        panic!("expected rows");
    };
    assert_eq!(
        types[0],
        DataType::Array {
            elem: ArrayElem::Float,
            len: 4
        }
    );
    assert_eq!(rows[0][1], Value::Varchar("FLOAT[4]".to_string()));
    assert_eq!(floats(&rows[0][0]), expected("first document"));
    assert_eq!(floats(&rows[1][0]), expected("second document"));

    let requests = mock.requests();
    assert_eq!(requests.len(), 1, "one chunk, one request");
    let request = &requests[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/embeddings");
    assert_eq!(request.header("content-type"), Some("application/json"));
    assert_eq!(request.header("authorization"), Some(bearer().as_str()));
    let body = request.json();
    assert_eq!(body["model"], support::MODEL);
    assert_eq!(body["dimensions"], 4);
    assert_eq!(body["input"], json!(["first document", "second document"]));
}

#[test]
fn a_thousand_rows_with_three_hundred_texts_cost_three_requests() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    db.execute("CREATE TABLE docs (body VARCHAR)").unwrap();
    let values: Vec<String> = (0..1000)
        .map(|row| format!("('text {}')", row % 300))
        .collect();
    db.execute(&format!("INSERT INTO docs VALUES {}", values.join(", ")))
        .unwrap();

    let rows = rows(&mut db, "SELECT embed(body) FROM docs");
    assert_eq!(rows.len(), 1000);
    // ceil(300 / 128) = 3, not 1000 and not 300: duplicates are collapsed
    // within the chunk before the batch split.
    assert_eq!(mock.count(), 3);
    let requests = mock.requests();
    let sent: usize = requests.iter().map(|request| request.inputs().len()).sum();
    assert_eq!(sent, 300);
    assert!(requests.iter().all(|request| request.inputs().len() <= 128));
}

#[test]
fn nulls_make_no_request_and_empty_texts_are_sent() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    db.execute("CREATE TABLE docs (body VARCHAR)").unwrap();
    db.execute("INSERT INTO docs VALUES (NULL), (''), ('x')")
        .unwrap();

    let rows = rows(&mut db, "SELECT embed(body) FROM docs");
    assert_eq!(rows[0][0], Value::Null);
    assert_eq!(floats(&rows[1][0]), expected(""));
    assert_eq!(floats(&rows[2][0]), expected("x"));
    assert_eq!(mock.count(), 1);
    assert_eq!(
        mock.requests()[0].inputs(),
        vec!["".to_string(), "x".to_string()]
    );
}

#[test]
fn explain_and_false_filters_never_call_the_provider() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    db.execute("CREATE TABLE docs (body VARCHAR)").unwrap();
    db.execute("INSERT INTO docs VALUES ('a'), ('b')").unwrap();

    let explained = db.execute("EXPLAIN SELECT embed(body) FROM docs").unwrap();
    assert!(!explained.is_empty());
    assert_eq!(mock.count(), 0, "EXPLAIN evaluated embed()");

    assert!(rows(&mut db, "SELECT embed(body) FROM docs WHERE false").is_empty());
    assert_eq!(mock.count(), 0, "a false filter evaluated embed()");

    // The filter reads `body`, not the embedding, and no row matches it.
    assert!(
        rows(
            &mut db,
            "SELECT embed(body) FROM docs WHERE body = 'missing'"
        )
        .is_empty()
    );
    assert_eq!(
        mock.count(),
        0,
        "a projection was evaluated before the filter"
    );

    // The harness counts: the same statement without the filter does call.
    assert_eq!(rows(&mut db, "SELECT embed(body) FROM docs").len(), 2);
    assert_eq!(mock.count(), 1);
}

#[test]
fn a_second_query_is_answered_from_the_cache() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();

    let first = rows(&mut db, "SELECT embed('same text')");
    assert_eq!(mock.count(), 1);
    let second = rows(&mut db, "SELECT embed('same text')");
    assert_eq!(mock.count(), 1, "the cache did not answer the repeat");
    assert_eq!(first[0][0].to_string(), second[0][0].to_string());

    rows(&mut db, "SELECT embed('other text')");
    assert_eq!(mock.count(), 2);
}

#[test]
fn a_dimension_mismatch_is_an_error_that_names_the_setting() {
    let mock =
        Mock::start(|_| Reply::Json(json!({"data": [{"index": 0, "embedding": [0.0, 0.0, 0.0]}]})));
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();

    let message = err(&mut db, "SELECT embed('x')");
    assert!(
        message.contains("PINKDB_EMBEDDINGS_DIMENSIONS"),
        "{message}"
    );
    assert!(message.contains("3 dimensions"), "{message}");
    assert!(message.contains("FLOAT[4]"), "{message}");
    assert_eq!(mock.count(), 1, "a dimension mismatch must not be retried");
}

#[test]
fn a_rate_limit_is_retried_and_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mock = {
        let attempts = attempts.clone();
        Mock::start(move |recorded| {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Reply::Status(429, "{\"error\": \"slow down\"}".to_string())
            } else {
                support::embed_reply(recorded)
            }
        })
    };
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();

    let rows = rows(&mut db, "SELECT embed('x')");
    assert_eq!(floats(&rows[0][0]), expected("x"));
    assert_eq!(mock.count(), 2);
    assert_eq!(
        mock.requests()[0].body,
        mock.requests()[1].body,
        "a retry must resend the same batch"
    );
}

#[test]
fn a_rejected_key_is_not_retried_and_never_echoed() {
    // A provider that quotes the presented key back in its error body must
    // not get it into the message: 401 is reported without the body.
    let mock = Mock::start(|_| {
        Reply::Status(
            401,
            format!("{{\"error\": \"invalid key\", \"key\": \"{SENTINEL}\"}}"),
        )
    });
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();

    let message = err(&mut db, "SELECT embed('x')");
    assert!(message.contains("HTTP 401"), "{message}");
    assert!(!message.contains(SENTINEL), "{message}");
    assert_eq!(mock.count(), 1);
}

#[test]
fn a_connection_that_never_answers_times_out() {
    let mock = Mock::start(|_| Reply::Hang(Duration::from_secs(2)));
    let _env = Env::set(vec![
        ("PINKDB_EMBEDDINGS_BASE_URL", mock.base_url()),
        ("PINKDB_EMBEDDINGS_MODEL", support::MODEL.to_string()),
        ("PINKDB_EMBEDDINGS_API_KEY", SENTINEL.to_string()),
        ("PINKDB_EMBEDDINGS_DIMENSIONS", "4".to_string()),
        ("PINKDB_EMBEDDINGS_TIMEOUT_MS", "200".to_string()),
    ]);
    let mut db = support::store();

    let started = Instant::now();
    let message = err(&mut db, "SELECT embed('x')");
    assert!(message.contains("could not reach"), "{message}");
    assert!(started.elapsed() < Duration::from_secs(2), "{message}");
    assert_eq!(mock.count(), 1);
}

#[test]
fn a_missing_key_is_an_error_even_when_other_providers_keys_are_set() {
    let mock = Mock::embeddings();
    // A key exported for the agent or z.ai is never sent to the embeddings
    // endpoint, which may be another provider entirely.
    let mut env = Env::set(vec![
        ("PINKDB_EMBEDDINGS_BASE_URL", mock.base_url()),
        ("PINKDB_EMBEDDINGS_MODEL", support::MODEL.to_string()),
        ("PINKDB_EMBEDDINGS_DIMENSIONS", "4".to_string()),
        ("PINKDB_AGENT_API_KEY", SENTINEL.to_string()),
        ("Z_API_KEY", SENTINEL.to_string()),
    ]);
    env.unset(&["PINKDB_EMBEDDINGS_API_KEY"]);
    let mut db = support::store();

    let message = err(&mut db, "SELECT embed('x')");
    assert!(message.contains("PINKDB_EMBEDDINGS_API_KEY"), "{message}");
    assert!(!message.contains(SENTINEL), "{message}");
    assert_eq!(mock.count(), 0, "no key must fail before any request");
}

#[test]
fn there_is_no_default_provider_or_model() {
    let mock = Mock::embeddings();
    for unset in ["PINKDB_EMBEDDINGS_BASE_URL", "PINKDB_EMBEDDINGS_MODEL"] {
        let mut env = engine_env(&mock.base_url(), None);
        env.unset(&[unset]);
        let mut db = support::store();

        let message = err(&mut db, "SELECT embed('x')");
        assert!(message.contains("no embeddings provider"), "{message}");
        assert!(message.contains("PINKDB_EMBEDDINGS_BASE_URL"), "{message}");
        assert!(message.contains("PINKDB_EMBEDDINGS_MODEL"), "{message}");
        drop(env);
    }
    assert_eq!(mock.count(), 0);
}

#[test]
fn the_api_key_never_reaches_the_store_or_its_wal() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    support::register();
    let directory = support::scratch("wal");
    let path = directory.join("db.pink");
    {
        let options = DurableOpenOptions::default();
        let (mut db, _) = PinkDb::open_durable_with_options(path.to_str().unwrap(), options)
            .expect("open the durable store");
        db.load_extension_package("embeddings").unwrap();
        db.execute("CREATE TABLE docs (body VARCHAR, v FLOAT[4])")
            .unwrap();
        db.execute("INSERT INTO docs VALUES ('a private note', NULL)")
            .unwrap();
        db.execute("UPDATE docs SET v = embed(body)").unwrap();
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM docs"), 1);
        assert_eq!(mock.count(), 1);
    }

    let mut files = 0;
    for entry in std::fs::read_dir(&directory).unwrap() {
        let entry = entry.unwrap();
        files += 1;
        let bytes = std::fs::read(entry.path()).unwrap();
        assert!(
            !bytes
                .windows(SENTINEL.len())
                .any(|window| window == SENTINEL.as_bytes()),
            "{} holds the API key",
            entry.path().display()
        );
    }
    assert!(files > 0, "the store wrote nothing to scan");
    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
fn the_agent_policy_refuses_embed_and_the_services_switch_permits_it() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    db.execute("CREATE TABLE docs (body VARCHAR)").unwrap();
    db.execute("INSERT INTO docs VALUES ('a')").unwrap();

    let (code, text) = execute(
        &mut db,
        &ServePolicy::default(),
        "SELECT embed(body) FROM docs",
    );
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("external services"), "{text}");
    assert!(text.contains("allow-files"), "{text}");
    assert_eq!(mock.count(), 0, "a refused statement must not call");

    let allowed = ServePolicy {
        allow_external_services: true,
        ..ServePolicy::default()
    };
    let (code, text) = execute(&mut db, &allowed, "SELECT embed(body) FROM docs");
    assert_eq!(code, 0, "{text}");
    assert_eq!(mock.count(), 1);
}

#[test]
fn documents_are_embedded_and_a_top_k_query_finds_the_match() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    db.execute("CREATE TABLE docs (id INTEGER, body VARCHAR, v FLOAT[4])")
        .unwrap();
    db.execute(
        "INSERT INTO docs VALUES \
         (1, 'red apple', NULL), (2, 'green apple', NULL), (3, 'blue ocean', NULL)",
    )
    .unwrap();

    db.execute("UPDATE docs SET v = embed(body)").unwrap();
    assert_eq!(mock.count(), 1, "three texts, one request");
    assert_eq!(
        count(&mut db, "SELECT COUNT(*) FROM docs WHERE v IS NOT NULL"),
        3
    );

    // Brute force, because distance functions (wave 107b) are not on this
    // branch: the stored vector of the query's own text is exactly zero away.
    let ranked = rows(
        &mut db,
        "WITH q AS (SELECT embed('red apple') AS v) \
         SELECT d.id, \
                (d.v[1]-q.v[1])*(d.v[1]-q.v[1]) + (d.v[2]-q.v[2])*(d.v[2]-q.v[2]) + \
                (d.v[3]-q.v[3])*(d.v[3]-q.v[3]) + (d.v[4]-q.v[4])*(d.v[4]-q.v[4]) AS dist \
         FROM docs d, q ORDER BY dist LIMIT 1",
    );
    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0][0].to_string(), "1");
    match ranked[0][1] {
        Value::Double(distance) => assert_eq!(distance, 0.0),
        ref other => panic!("expected a DOUBLE distance, got {other:?}"),
    }
    // The query's text was embedded by the UPDATE, so the cache answered it.
    assert_eq!(mock.count(), 1);
}

/// A crash (a drop without a checkpoint) replays the statement; its memo
/// answers `embed()` bit for bit, so the provider is not needed — not even
/// its key — and a provider whose answers changed cannot change the replayed
/// rows.
#[test]
fn a_replayed_statement_serves_its_vectors_from_the_wal_memo() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let mock = Mock::start(move |request| {
        let mut reply = support::embed_reply(request);
        // The original UPDATE's request answers the recorded vectors; every
        // later one answers something else, so a replay that called the
        // provider would be visible.
        if counter.fetch_add(1, Ordering::SeqCst) > 0
            && let Reply::Json(body) = &mut reply
        {
            for entry in body["data"].as_array_mut().expect("data") {
                entry["embedding"] = json!(vec![0.5; 4]);
            }
        }
        reply
    });
    let env = engine_env(&mock.base_url(), None);
    support::register();
    let directory = support::scratch("replay");
    let path = directory.join("db.pink");
    let recorded: Vec<Vec<f32>>;
    {
        let options = DurableOpenOptions::default();
        let (mut db, _) = PinkDb::open_durable_with_options(path.to_str().unwrap(), options)
            .expect("open the durable store");
        db.load_extension_package("embeddings").unwrap();
        db.execute("CREATE TABLE docs (id INTEGER, body VARCHAR, v FLOAT[4])")
            .unwrap();
        db.execute(
            "INSERT INTO docs VALUES (1, 'alpha', NULL), (2, 'beta', NULL), (3, 'alpha', NULL)",
        )
        .unwrap();
        db.execute("UPDATE docs SET v = embed(body)").unwrap();
        recorded = rows(&mut db, "SELECT v FROM docs ORDER BY id")
            .into_iter()
            .map(|row| floats(&row[0]))
            .collect();
        assert_eq!(recorded.len(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one request for two texts");
        // Dropped without a checkpoint: the next open replays this record.
    }
    // The mock stays up so a stray request would be counted, but the replay
    // points at a dead endpoint and has no key: any call fails the open.
    drop(env);
    // No key, and an endpoint that would fail the open if it were called.
    let _offline = Env::set(vec![
        (
            "PINKDB_EMBEDDINGS_BASE_URL",
            "http://127.0.0.1:9/embeddings".to_string(),
        ),
        ("PINKDB_EMBEDDINGS_DIMENSIONS", "4".to_string()),
    ]);
    let options = DurableOpenOptions::default();
    let (mut db, _) = PinkDb::open_durable_with_options(path.to_str().unwrap(), options)
        .expect("replay without the provider or a key");
    let replayed: Vec<Vec<f32>> = rows(&mut db, "SELECT v FROM docs ORDER BY id")
        .into_iter()
        .map(|row| floats(&row[0]))
        .collect();
    assert_eq!(replayed, recorded, "replay must serve the recorded vectors");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the replay must make no request"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// A transaction's record carries the memo of every statement it logged.
#[test]
fn a_transaction_replays_every_statements_memo() {
    let mock = Mock::embeddings();
    let env = engine_env(&mock.base_url(), None);
    support::register();
    let directory = support::scratch("replay_txn");
    let path = directory.join("db.pink");
    {
        let options = DurableOpenOptions::default();
        let (mut db, _) = PinkDb::open_durable_with_options(path.to_str().unwrap(), options)
            .expect("open the durable store");
        db.load_extension_package("embeddings").unwrap();
        db.execute("CREATE TABLE docs (id INTEGER, body VARCHAR, v FLOAT[4])")
            .unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO docs VALUES (1, 'one', embed('one'))")
            .unwrap();
        db.execute("INSERT INTO docs VALUES (2, 'two', embed('two'))")
            .unwrap();
        db.execute("COMMIT").unwrap();
        // Each statement is its own chunk, so each text is its own request;
        // the point here is that both statements' memos travel in the
        // transaction's record.
        assert_eq!(mock.count(), 2);
    }
    drop(env);
    let _offline = Env::set(vec![
        (
            "PINKDB_EMBEDDINGS_BASE_URL",
            "http://127.0.0.1:9/embeddings".to_string(),
        ),
        ("PINKDB_EMBEDDINGS_DIMENSIONS", "4".to_string()),
    ]);
    let options = DurableOpenOptions::default();
    let (mut db, _) = PinkDb::open_durable_with_options(path.to_str().unwrap(), options)
        .expect("replay without the provider or a key");
    let values: Vec<Vec<f32>> = rows(&mut db, "SELECT v FROM docs ORDER BY id")
        .into_iter()
        .map(|row| floats(&row[0]))
        .collect();
    assert_eq!(values, vec![expected("one"), expected("two")]);
    let _ = std::fs::remove_dir_all(&directory);
}

/// A policy that allows writes but not files must refuse `embed()` even when
/// the call is reached by execution rather than by binding (wave 107c's
/// execution-time gate).
fn write_only() -> ServePolicy {
    ServePolicy {
        allow_writes: true,
        ..ServePolicy::default()
    }
}

fn write_and_services() -> ServePolicy {
    ServePolicy {
        allow_writes: true,
        allow_external_services: true,
        ..ServePolicy::default()
    }
}

fn run(db: &mut PinkDb, policy: &ServePolicy, sql: &str) {
    let (code, text) = execute(db, policy, sql);
    assert_eq!(code, 0, "{sql}: {text}");
}

fn assert_permission(text: &str) {
    assert!(text.contains("\"kind\":\"permission\""), "{text}");
    assert!(text.contains("external services"), "{text}");
}

#[test]
fn a_batch_whose_call_cannot_bind_at_check_time_is_refused_at_execution() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let script = "CREATE TABLE t(x VARCHAR); INSERT INTO t VALUES ('a'); \
                  SELECT embed(x) FROM t";

    let mut db = support::store();
    let (code, text) = execute(&mut db, &write_only(), script);
    assert_eq!(code, 1, "{text}");
    assert_permission(&text);
    assert_eq!(mock.count(), 0, "a refused statement must not call");

    let mut db = support::store();
    let (code, text) = execute(&mut db, &write_and_services(), script);
    assert_eq!(code, 0, "{text}");
    assert_eq!(mock.count(), 1);
}

#[test]
fn a_macro_and_a_view_cannot_smuggle_embed_past_the_policy() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    run(
        &mut db,
        &write_and_services(),
        "CREATE MACRO m(x) AS embed(x)",
    );
    run(
        &mut db,
        &write_and_services(),
        "CREATE VIEW v AS SELECT embed('a') AS e",
    );
    assert_eq!(mock.count(), 0, "defining a macro or view must not call");

    for sql in ["SELECT m('a')", "SELECT e FROM v"] {
        let (code, text) = execute(&mut db, &write_only(), sql);
        assert_eq!(code, 1, "{sql}: {text}");
        assert_permission(&text);
    }
    assert_eq!(mock.count(), 0);

    let (code, text) = execute(&mut db, &write_and_services(), "SELECT e FROM v");
    assert_eq!(code, 0, "{text}");
    assert_eq!(mock.count(), 1);
}

#[test]
fn a_trigger_body_and_a_do_block_are_refused_at_execution() {
    let mock = Mock::embeddings();
    let _env = engine_env(&mock.base_url(), None);
    let mut db = support::store();
    run(&mut db, &write_only(), "CREATE TABLE t(x VARCHAR)");
    run(&mut db, &write_only(), "CREATE TABLE log(v FLOAT[4])");
    run(
        &mut db,
        &write_only(),
        "CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES (embed('a'))",
    );
    assert_eq!(mock.count(), 0, "creating the trigger must not call");

    let (code, text) = execute(&mut db, &write_only(), "INSERT INTO t VALUES ('x')");
    assert_eq!(code, 1, "{text}");
    assert_permission(&text);
    assert_eq!(mock.count(), 0);
    assert_eq!(
        count(&mut db, "SELECT COUNT(*) FROM t"),
        0,
        "the insert rolled back"
    );

    let (code, text) = execute(
        &mut db,
        &write_only(),
        "DO $$ BEGIN INSERT INTO log VALUES (embed('b')); END $$",
    );
    assert_eq!(code, 1, "{text}");
    assert_permission(&text);
    assert_eq!(mock.count(), 0);

    // The same trigger and body run once the switch permits external I/O.
    let (code, text) = execute(&mut db, &write_and_services(), "INSERT INTO t VALUES ('y')");
    assert_eq!(code, 0, "{text}");
    assert_eq!(mock.count(), 1);
    let (code, text) = execute(
        &mut db,
        &write_and_services(),
        "DO $$ BEGIN INSERT INTO log VALUES (embed('b')); END $$",
    );
    assert_eq!(code, 0, "{text}");
    assert_eq!(mock.count(), 2);
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM log"), 2);
}
