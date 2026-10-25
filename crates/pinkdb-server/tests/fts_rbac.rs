//! Wave 108a, RBAC: `match_bm25` scores from the index database, which holds
//! a copy of the base table's text. A host scalar that reads tables declares
//! them at bind time, so the server's read pass asks for `SELECT` on the base
//! table exactly as a direct scan would. Without that, any logged-in user
//! could probe a table they cannot read by searching its index.
#![cfg(feature = "fts")]

use pinkdb::{Connection, Database, QueryResult};
use pinkdb_server::{Client, Server, ServerHandle};

/// A server over `db` plus its address; the handle keeps it alive.
fn start(db: Database) -> (ServerHandle, String) {
    let server = Server::bind("127.0.0.1:0", db).expect("bind ephemeral server");
    let addr = server.local_addr().to_string();
    (server.spawn().expect("spawn server"), addr)
}

fn err_of(r: Result<Vec<QueryResult>, pinkdb_server::ClientError>) -> String {
    r.expect_err("statement should have failed").to_string()
}

/// The corpus and the index, built by the trusted embedded session.
/// Package registration is process-global; two tests share it.
fn ensure_fts() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| pinkdb_fts::ensure_registered().unwrap());
}

fn seeded() -> (Database, Connection) {
    ensure_fts();
    let db = Database::new();
    db.set_allow_sql_load_extension(true);
    let mut conn = db.connect();
    conn.execute("LOAD EXTENSION 'fts';").unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER, title VARCHAR, body VARCHAR);")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES
           (1, 'Salary bands', 'the salary band for each level'),
           (2, 'Holidays', 'the holiday policy'),
           (3, 'Salary review', 'review of the salary policy');",
    )
    .unwrap();
    conn.execute("CALL create_fts_index('docs', 'id', 'title', 'body');")
        .unwrap();
    conn.execute(
        "CREATE USER root PASSWORD 'root-secret';
         GRANT ADMIN TO root;
         CREATE USER alice PASSWORD 'alice-secret';",
    )
    .unwrap();
    (db, conn)
}

#[test]
fn match_bm25_needs_select_on_the_base_table() {
    let (db, mut trusted) = seeded();
    // A trusted session (the embedded/CLI path) is unchanged: no principal,
    // no grant check.
    assert_eq!(
        trusted
            .execute("SELECT match_bm25('docs', 1, 'salary');")
            .unwrap()
            .len(),
        1
    );
    let (_handle, addr) = start(db);

    let mut alice = Client::connect(&addr).unwrap();
    alice.login("alice", "alice-secret").unwrap();
    // The leak this guards: the statement names no table the binder would
    // check, yet the index answers questions about `docs`.
    let e = err_of(alice.execute("SELECT match_bm25('docs', 1, 'salary');"));
    assert!(e.contains("permission denied"), "{e}");
    assert!(e.contains("docs"), "{e}");

    // Writing an index is still ADMIN-only.
    let e = err_of(
        alice.execute("CALL create_fts_index('docs', 'id', 'title', 'body', overwrite := 1);"),
    );
    assert!(e.contains("needs the ADMIN privilege"), "{e}");

    let mut root = Client::connect(&addr).unwrap();
    root.login("root", "root-secret").unwrap();
    root.execute("GRANT SELECT ON TABLE docs TO alice;")
        .unwrap();
    // An admin needs no grant.
    assert!(
        root.execute("SELECT match_bm25('docs', 1, 'salary');")
            .is_ok()
    );

    // After the grant the scalar works — including inside a query that reads
    // the table for its ids.
    let rows = alice
        .execute(
            "SELECT id, match_bm25('docs', id, 'salary') AS score
               FROM docs ORDER BY score DESC NULLS LAST;",
        )
        .unwrap();
    let QueryResult::Selected { rows, .. } = &rows[0] else {
        panic!("expected a query result, got {:?}", rows[0]);
    };
    assert_eq!(rows.len(), 3);

    // The declared read must come from a constant, so the check cannot be
    // dodged by binding the index name per row.
    let e = err_of(root.execute("SELECT match_bm25(title, 1, 'salary') FROM docs;"));
    assert!(e.contains("literal index name"), "{e}");
}

/// The index database's own tables are ordinary tables, so reading them
/// directly needs the grant they carry — a user cannot launder a read through
/// them either.
#[test]
fn index_tables_need_their_own_grant() {
    let (db, _trusted) = seeded();
    let (_handle, addr) = start(db);
    let mut alice = Client::connect(&addr).unwrap();
    alice.login("alice", "alice-secret").unwrap();
    let e = err_of(alice.execute("SELECT * FROM fts_main_docs.terms LIMIT 1;"));
    assert!(e.contains("permission denied"), "{e}");
}
