//! Wave 121: the engine hooks the warehouse console uses — a cancellation
//! flag that stops a running statement, `Connection::impersonate`, and the
//! `pinkdb_catalog()` relation with its privilege filtering.

use std::time::{Duration, Instant};

use pinkdb::{CancelFlag, Database, QueryResult, STATEMENT_CANCELLED, Value};

fn rows(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn store() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE big (x INTEGER)").unwrap();
    let values: Vec<String> = (0..20_000).map(|i| format!("({i})")).collect();
    conn.execute(&format!("INSERT INTO big VALUES {}", values.join(",")))
        .unwrap();
    db
}

#[test]
fn a_cancel_flag_stops_a_running_statement() {
    let db = store();
    let flag = CancelFlag::new();
    let mut conn = db.connect();
    conn.set_cancel_flag(Some(flag.clone()));
    let canceller = {
        let flag = flag.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            flag.cancel();
        })
    };
    let started = Instant::now();
    let err = conn
        .execute("SELECT count(*) FROM big a, big b WHERE a.x + b.x < -1")
        .expect_err("the statement must be cancelled");
    canceller.join().unwrap();
    assert!(err.to_string().contains(STATEMENT_CANCELLED), "{err}");
    assert!(started.elapsed() < Duration::from_secs(20));
    // A set flag refuses the next statement too, until it is reset.
    assert!(conn.execute("SELECT count(*) FROM big").is_err());
    flag.reset();
    let out = conn
        .execute("SELECT count(*) FROM big")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Bigint(20_000)]]);
    // A cancelled write leaves nothing behind.
    flag.cancel();
    assert!(conn.execute("INSERT INTO big SELECT x FROM big").is_err());
    flag.reset();
    let out = conn
        .execute("SELECT count(*) FROM big")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Bigint(20_000)]]);
}

#[test]
fn impersonation_adopts_a_users_grants_without_a_password() {
    let db = store();
    let mut admin = db.connect();
    admin
        .execute("CREATE USER bob PASSWORD 'pw'; CREATE TABLE open (y INTEGER); GRANT SELECT ON TABLE main.open TO bob")
        .unwrap();
    let mut conn = db.connect();
    assert!(conn.impersonate("nobody").is_err());
    conn.impersonate("BOB").unwrap();
    assert_eq!(conn.principal(), Some("bob"));
    let me = conn
        .execute("SELECT current_user()")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rows(me), vec![vec![Value::Varchar("bob".into())]]);
    assert!(conn.execute("SELECT * FROM open").is_ok());
    let err = conn.execute("SELECT * FROM big").unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("permission"),
        "{err}"
    );
}

#[test]
fn the_catalog_relation_lists_objects_a_session_may_read() {
    let db = store();
    let mut admin = db.connect();
    admin
        .execute(
            "CREATE TABLE open (y INTEGER); CREATE VIEW open_v AS SELECT y FROM open; \
             CREATE INDEX big_x ON big(x); CREATE INDEX open_y ON open(y); \
             CREATE MACRO plus1(a) AS a + 1; \
             CREATE PROCEDURE add_y(v INTEGER) AS $$ BEGIN INSERT INTO open VALUES (v); END $$; \
             CREATE USER bob PASSWORD 'pw'; GRANT SELECT ON TABLE main.open TO bob",
        )
        .unwrap();
    let list = |conn: &mut pinkdb::Connection| -> Vec<String> {
        let out = conn
            .execute(
                "SELECT \"database\", \"name\", \"kind\", \"table_name\", \"detail\" \
                 FROM pinkdb_catalog() WHERE \"kind\" <> 'DATABASE' ORDER BY 1, 3, 2",
            )
            .unwrap()
            .pop()
            .unwrap();
        rows(out)
            .into_iter()
            .map(|r| {
                r.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    };
    // Wave 124: one DATABASE row per database carries its storage.
    let databases = rows(
        admin
            .execute(
                "SELECT \"name\", storage, location, read_only, status FROM pinkdb_catalog() \
                 WHERE kind = 'DATABASE'",
            )
            .unwrap()
            .pop()
            .unwrap(),
    );
    assert_eq!(databases.len(), 1, "{databases:?}");
    assert_eq!(
        databases[0]
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("|"),
        "main|local|NULL|false|ok"
    );
    let all = list(&mut admin);
    assert_eq!(
        all,
        vec![
            "main|big_x|INDEX|big|x (hash)",
            "main|open_y|INDEX|open|y (hash)",
            "main|plus1|MACRO|NULL|a",
            "main|add_y|PROCEDURE|NULL|v INTEGER · SQL",
            "main|big|TABLE|NULL|NULL",
            "main|open|TABLE|NULL|NULL",
            "main|open_v|VIEW|NULL|NULL",
        ]
    );
    // Bob sees the table he may read and its index, not `big` or its index.
    let mut bob = db.connect();
    bob.impersonate("bob").unwrap();
    let seen = list(&mut bob);
    assert!(
        seen.contains(&"main|open|TABLE|NULL|NULL".to_string()),
        "{seen:?}"
    );
    assert!(
        seen.contains(&"main|open_y|INDEX|open|y (hash)".to_string()),
        "{seen:?}"
    );
    assert!(!seen.iter().any(|r| r.contains("big")), "{seen:?}");
    // Only an administrator may CALL a procedure, so only one sees it.
    assert!(!seen.iter().any(|r| r.contains("PROCEDURE")), "{seen:?}");
    assert!(
        db.connect()
            .execute("SELECT * FROM pinkdb_catalog('x')")
            .is_err()
    );
}

/// Wave 121b: a `VALUES` list longer than one chunk (2,048 rows) is a row
/// source like any other (it failed with "vector overflow" before).
#[test]
fn a_values_list_spans_several_chunks() {
    let db = Database::new();
    let mut conn = db.connect();
    let list: Vec<String> = (0..5_000).map(|i| format!("({i}, 'r{i}')")).collect();
    let values = list.join(", ");
    let out = conn
        .execute(&format!(
            "SELECT count(*), sum(a), max(b) FROM (VALUES {values}) AS v(a, b)"
        ))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        rows(out),
        vec![vec![
            Value::Bigint(5_000),
            Value::Bigint(12_497_500),
            Value::Varchar("r999".into())
        ]]
    );
    conn.execute(&format!(
        "CREATE TABLE v AS SELECT a, b FROM (VALUES {values}) AS v(a, b)"
    ))
    .unwrap();
    let out = conn
        .execute("SELECT count(*) FROM v WHERE a >= 2048")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Bigint(2_952)]]);
    // Exactly one chunk, and empty, still work.
    let one: Vec<String> = (0..2_048).map(|i| format!("({i})")).collect();
    let out = conn
        .execute(&format!(
            "SELECT count(*) FROM (VALUES {}) AS v(a)",
            one.join(",")
        ))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Bigint(2_048)]]);
}

/// Wave 121b: `pinkdb_users()` and `pinkdb_grants()` show an administrator
/// (or a trusted session) everything and anyone else only themselves and
/// their roles.
#[test]
fn the_access_relations_show_each_principal_what_is_theirs() {
    let db = Database::new();
    db.connect()
        .execute(
            "CREATE TABLE t (a INTEGER); CREATE USER root PASSWORD 'pw'; GRANT ADMIN TO root; \
             CREATE USER bob PASSWORD 'pw'; CREATE USER eve PASSWORD 'pw'; \
             CREATE ROLE readers; GRANT SELECT ON TABLE main.t TO readers; \
             GRANT readers TO bob; GRANT INSERT ON TABLE main.t TO eve; \
             GRANT CREATE ON DATABASE main TO eve",
        )
        .unwrap();
    let list = |conn: &mut pinkdb::Connection, sql: &str| -> Vec<String> {
        rows(conn.execute(sql).unwrap().pop().unwrap())
            .into_iter()
            .map(|r| r.iter().map(Value::to_string).collect::<Vec<_>>().join("|"))
            .collect()
    };
    let users =
        "SELECT \"name\", \"kind\", \"is_admin\", \"roles\" FROM pinkdb_users() ORDER BY 2, 1";
    let grants = "SELECT \"principal\", \"privilege\", \"object_type\", \"database\", \"name\" \
                  FROM pinkdb_grants() ORDER BY 1, 2";
    let mut trusted = db.connect();
    assert_eq!(
        list(&mut trusted, users),
        vec![
            "readers|ROLE|false|",
            "bob|USER|false|readers",
            "eve|USER|false|",
            "root|USER|true|",
        ]
    );
    assert_eq!(
        list(&mut trusted, grants),
        vec![
            "bob|MEMBER|ROLE|NULL|readers",
            "eve|CREATE|DATABASE|main|NULL",
            "eve|INSERT|TABLE|main|t",
            "readers|SELECT|TABLE|main|t",
            "root|ADMIN|GLOBAL|NULL|NULL",
        ]
    );
    let mut root = db.connect();
    root.impersonate("root").unwrap();
    assert_eq!(list(&mut root, grants).len(), 5);
    let mut bob = db.connect();
    bob.impersonate("bob").unwrap();
    assert_eq!(
        list(&mut bob, users),
        vec!["readers|ROLE|false|", "bob|USER|false|readers"]
    );
    assert_eq!(
        list(&mut bob, grants),
        vec![
            "bob|MEMBER|ROLE|NULL|readers",
            "readers|SELECT|TABLE|main|t"
        ]
    );
    let mut eve = db.connect();
    eve.impersonate("eve").unwrap();
    assert_eq!(list(&mut eve, users), vec!["eve|USER|false|"]);
    assert!(
        db.connect()
            .execute("SELECT * FROM pinkdb_users(1)")
            .is_err()
    );
}
