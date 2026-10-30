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
             CREATE USER bob PASSWORD 'pw'; GRANT SELECT ON TABLE main.open TO bob",
        )
        .unwrap();
    let list = |conn: &mut pinkdb::Connection| -> Vec<String> {
        let out = conn
            .execute(
                "SELECT \"database\", \"name\", \"kind\", \"table_name\", \"detail\" \
                 FROM pinkdb_catalog() ORDER BY 1, 3, 2",
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
    let all = list(&mut admin);
    assert_eq!(
        all,
        vec![
            "main|big_x|INDEX|big|x (hash)",
            "main|open_y|INDEX|open|y (hash)",
            "main|plus1|MACRO|NULL|a",
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
    assert!(
        db.connect()
            .execute("SELECT * FROM pinkdb_catalog('x')")
            .is_err()
    );
}
