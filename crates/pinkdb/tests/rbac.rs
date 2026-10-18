//! Wave 48: RBAC end to end in the engine — `CREATE USER`/`CREATE ROLE`/
//! `GRANT`/`REVOKE` semantics (idempotence, role inheritance, cycle refusal,
//! cascades), the per-statement privilege check an authenticated
//! [`Connection`] runs under, `.pink` persistence (flags bit 5, round-trip,
//! clean corruption errors) and durable WAL replay — including the
//! no-plaintext-at-rest guarantee for both artifacts.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{Database, PinkDb, Privilege, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_rbac_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn exec(db: &mut PinkDb, sql: &str) -> QueryResult {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .unwrap()
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should have failed"))
        .to_string()
}

/// One embedded (trusted) store with a table, a user and a role.
fn store() -> PinkDb {
    let mut db = PinkDb::new();
    exec(&mut db, "CREATE TABLE t (i INTEGER, s VARCHAR);");
    exec(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b');");
    exec(&mut db, "CREATE USER alice PASSWORD 'alice-secret';");
    exec(&mut db, "CREATE ROLE analyst;");
    db
}

/// A connection logged in as `alice` over a shared `Database`.
fn alice(db: &Database) -> pinkdb::Connection {
    let mut conn = db.connect();
    conn.login("alice", "alice-secret").expect("login");
    conn
}

/// Run one statement through a trusted (never logged in) connection: the
/// administrative path an embedder or the bootstrap server uses.
fn admin_sql(db: &Database, sql: &str) {
    let mut conn = db.connect();
    conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn execute(conn: &mut pinkdb::Connection, sql: &str) -> String {
    match conn.execute(sql) {
        Ok(_) => String::new(),
        Err(e) => e.to_string(),
    }
}

// ---------- DDL semantics ----------

#[test]
fn create_user_hashes_the_password_and_authentication_distinguishes_failures() {
    let mut db = PinkDb::new();
    exec(&mut db, "CREATE USER Alice PASSWORD 's3cret';");
    assert!(db.has_users());
    // The catalog keeps the as-created spelling, the key is lowercased.
    assert_eq!(db.authenticate("alice", "s3cret").unwrap(), "Alice");
    assert_eq!(db.authenticate("ALICE", "s3cret").unwrap(), "Alice");
    let wrong = db.authenticate("alice", "wrong").unwrap_err().to_string();
    let unknown = db.authenticate("nobody", "x").unwrap_err().to_string();
    assert!(wrong.starts_with("authentication failed"), "{wrong}");
    // An unknown user and a wrong password are indistinguishable.
    assert_eq!(wrong, unknown);
    // The hash is an Argon2id PHC string, and it is not the plaintext.
    let key = "alice";
    let stored = db.user_password_hash(key).unwrap();
    assert!(
        stored.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"),
        "{stored}"
    );
    assert!(!stored.contains("s3cret"));
    assert!(pinkdb::verify_password(stored, "s3cret"));
    assert!(!pinkdb::verify_password(stored, "s3cret "));
}

#[test]
fn alter_user_replaces_the_password() {
    let mut db = store();
    exec(&mut db, "ALTER USER alice PASSWORD 'new-secret';");
    assert!(db.authenticate("alice", "new-secret").is_ok());
    assert!(db.authenticate("alice", "alice-secret").is_err());
}

#[test]
fn if_exists_and_if_not_exists_are_idempotent_no_ops() {
    let mut db = store();
    // Re-creating with IF NOT EXISTS is a no-op (the old password survives).
    exec(&mut db, "CREATE USER IF NOT EXISTS alice PASSWORD 'other';");
    assert!(db.authenticate("alice", "alice-secret").is_ok());
    assert!(db.authenticate("alice", "other").is_err());
    exec(&mut db, "CREATE ROLE IF NOT EXISTS analyst;");
    exec(&mut db, "DROP USER IF EXISTS nobody;");
    exec(&mut db, "DROP ROLE IF EXISTS nobody;");
    // Without IF EXISTS the same statements are errors.
    let e = err(&mut db, "DROP USER nobody;");
    assert!(e.contains("user \"nobody\" not found"), "{e}");
    let e = err(&mut db, "DROP ROLE nobody;");
    assert!(e.contains("role \"nobody\" not found"), "{e}");
}

#[test]
fn user_and_role_names_do_not_collide() {
    let mut db = store();
    let e = err(&mut db, "CREATE USER analyst PASSWORD 'x';");
    assert!(e.contains("a role named \"analyst\" already exists"), "{e}");
    let e = err(&mut db, "CREATE ROLE alice;");
    assert!(e.contains("a user named \"alice\" already exists"), "{e}");
    let e = err(&mut db, "DROP USER analyst;");
    assert!(e.contains("is a role, not a user"), "{e}");
    let e = err(&mut db, "DROP ROLE alice;");
    assert!(e.contains("is a user, not a role"), "{e}");
    let e = err(&mut db, "ALTER USER analyst PASSWORD 'x';");
    assert!(e.contains("is a role, not a user"), "{e}");
    let e = err(&mut db, "CREATE USER alice PASSWORD 'x';");
    assert!(e.contains("user \"alice\" already exists"), "{e}");
    let e = err(&mut db, "CREATE ROLE analyst;");
    assert!(e.contains("role \"analyst\" already exists"), "{e}");
}

#[test]
fn empty_password_and_bad_hashes_are_refused() {
    let mut db = PinkDb::new();
    let e = err(&mut db, "CREATE USER bob PASSWORD '';");
    assert!(e.contains("password must not be empty"), "{e}");
    let e = err(&mut db, "CREATE USER bob PASSWORD_HASH 'not-a-hash';");
    assert!(e.contains("not a valid PHC hash"), "{e}");
    // Valid PHC parameters, wrong algorithm.
    let argon2i = "$argon2i$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA\
$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let e = err(
        &mut db,
        &format!("CREATE USER bob PASSWORD_HASH '{argon2i}';"),
    );
    assert!(e.contains("must be an argon2id hash"), "{e}");
    // The pre-hashed form is accepted (and never derived).
    let argon2id = argon2i.replace("$argon2i$", "$argon2id$");
    exec(
        &mut db,
        &format!("CREATE USER bob PASSWORD_HASH '{argon2id}';"),
    );
    assert!(db.has_users());
}

#[test]
fn grant_and_revoke_are_idempotent_and_scope_checked() {
    let mut db = store();
    // Repeated grants are no-ops with the same reported description.
    for _ in 0..2 {
        let r = exec(&mut db, "GRANT SELECT ON TABLE t TO alice;");
        assert_eq!(
            r,
            QueryResult::Granted {
                description: "SELECT ON TABLE main.t TO alice".to_string()
            }
        );
    }
    // Revoking something that is not held is a no-op too.
    for _ in 0..2 {
        let r = exec(&mut db, "REVOKE INSERT ON TABLE t FROM alice;");
        assert_eq!(
            r,
            QueryResult::Revoked {
                description: "INSERT ON TABLE main.t TO alice".to_string()
            }
        );
    }
    // `ALL` expands per scope and the result names every privilege.
    let r = exec(&mut db, "GRANT ALL ON TABLE t TO analyst;");
    assert_eq!(
        r,
        QueryResult::Granted {
            description: "SELECT, INSERT, UPDATE, DELETE ON TABLE main.t TO analyst".to_string()
        }
    );
    let r = exec(&mut db, "GRANT ALL ON DATABASE main TO analyst;");
    assert_eq!(
        r,
        QueryResult::Granted {
            description: "CREATE ON DATABASE main TO analyst".to_string()
        }
    );
    // Scope mismatches name the statement form that accepts the privilege.
    let e = err(&mut db, "GRANT CREATE ON TABLE t TO alice;");
    assert!(e.contains("CREATE is a database privilege"), "{e}");
    let e = err(&mut db, "GRANT SELECT ON DATABASE main TO alice;");
    assert!(e.contains("SELECT is a table privilege"), "{e}");
}

#[test]
fn grant_error_paths_name_what_is_missing() {
    let mut db = store();
    let e = err(&mut db, "GRANT SELECT ON TABLE missing TO alice;");
    assert!(e.contains("table \"missing\" not found"), "{e}");
    let e = err(&mut db, "GRANT CREATE ON DATABASE missing TO alice;");
    assert!(e.contains("database \"missing\" not found"), "{e}");
    let e = err(&mut db, "GRANT SELECT ON TABLE t TO nobody;");
    assert!(e.contains("user or role \"nobody\" not found"), "{e}");
    let e = err(&mut db, "GRANT nobody TO alice;");
    assert!(e.contains("role \"nobody\" not found"), "{e}");
    let e = err(&mut db, "GRANT alice TO analyst;");
    assert!(e.contains("\"alice\" is a user, not a role"), "{e}");
}

#[test]
fn role_membership_is_transitive_and_cycles_are_refused() {
    let mut db = store();
    exec(&mut db, "CREATE ROLE reader;");
    exec(&mut db, "GRANT reader TO analyst;");
    exec(&mut db, "GRANT analyst TO alice;");
    // Re-granting an existing membership is a no-op.
    exec(&mut db, "GRANT reader TO analyst;");
    // Cycles are refused, whatever their length.
    let e = err(&mut db, "GRANT alice TO analyst;");
    assert!(e.contains("\"alice\" is a user, not a role"), "{e}");
    let e = err(&mut db, "GRANT analyst TO analyst;");
    assert!(e.contains("cannot be a member of itself"), "{e}");
    let e = err(&mut db, "GRANT analyst TO reader;");
    assert!(e.contains("would create a membership cycle"), "{e}");
    // A redundant edge is *not* a cycle: alice already reaches `reader`
    // through analyst, and adding the direct membership changes nothing.
    exec(&mut db, "GRANT reader TO alice;");
    // A three-level cycle is refused at whichever edge closes it.
    exec(&mut db, "CREATE ROLE senior;");
    exec(&mut db, "GRANT senior TO reader;");
    let e = err(&mut db, "GRANT analyst TO senior;");
    assert!(e.contains("would create a membership cycle"), "{e}");
    // REVOKE breaks the chain and then the edge can be granted again.
    exec(&mut db, "REVOKE senior FROM reader;");
    exec(&mut db, "GRANT analyst TO senior;");
}

#[test]
fn drop_role_removes_dependent_grants_and_memberships() {
    let mut db = store();
    exec(&mut db, "CREATE ROLE reader;");
    exec(&mut db, "GRANT reader TO analyst;");
    exec(&mut db, "GRANT analyst TO alice;");
    exec(&mut db, "GRANT SELECT ON TABLE t TO analyst;");
    exec(&mut db, "GRANT ALL ON TABLE t TO reader;");
    exec(&mut db, "GRANT CREATE ON DATABASE main TO analyst;");
    exec(&mut db, "GRANT ADMIN TO reader;");
    exec(&mut db, "DROP ROLE analyst;");
    // Every grant naming the role is gone: alice is not an admin through the
    // (now vanished) analyst membership.
    assert!(!db.principal_is_admin("alice"));
    assert!(
        db.effective_table_privileges("alice", "main", "t")
            .is_empty()
    );
    // Dropping a role a user was a member of leaves the user alone.
    exec(&mut db, "DROP ROLE reader;");
    assert!(!db.principal_is_admin("alice"));
    assert!(db.authenticate("alice", "alice-secret").is_ok());
}

#[test]
fn dropping_a_user_removes_its_grants() {
    let mut db = store();
    exec(&mut db, "GRANT SELECT ON TABLE t TO alice;");
    exec(&mut db, "GRANT ADMIN TO alice;");
    exec(&mut db, "DROP USER alice;");
    assert!(!db.has_users());
    assert!(!db.principal_is_admin("alice"));
    assert!(
        db.effective_table_privileges("alice", "main", "t")
            .is_empty()
    );
    assert!(db.authenticate("alice", "alice-secret").is_err());
}

#[test]
fn dropping_a_table_or_database_takes_its_grants() {
    let mut db = store();
    exec(&mut db, "GRANT SELECT ON TABLE t TO analyst;");
    exec(&mut db, "GRANT CREATE ON DATABASE main TO analyst;");
    exec(&mut db, "DROP TABLE t;");
    assert!(
        db.effective_table_privileges("analyst", "main", "t")
            .is_empty()
    );
    // Recreating the name must not inherit the old privileges.
    exec(&mut db, "CREATE TABLE t (i INTEGER);");
    assert!(
        db.effective_table_privileges("analyst", "main", "t")
            .is_empty()
    );
    // A dropped database takes its grants with it (and the name does not
    // inherit them on re-creation).
    exec(&mut db, "CREATE DATABASE other;");
    exec(&mut db, "CREATE TABLE other.o (i INTEGER);");
    exec(&mut db, "GRANT CREATE ON DATABASE other TO analyst;");
    assert_eq!(
        db.effective_database_privileges("analyst", "other"),
        vec![Privilege::Create]
    );
    exec(&mut db, "DROP DATABASE other;");
    exec(&mut db, "CREATE DATABASE other;");
    assert!(
        db.effective_database_privileges("analyst", "other")
            .is_empty()
    );
}

#[test]
fn renaming_a_table_moves_its_grants() {
    let mut db = store();
    exec(&mut db, "GRANT SELECT ON TABLE t TO analyst;");
    exec(&mut db, "ALTER TABLE t RENAME TO renamed;");
    assert_eq!(
        db.effective_table_privileges("analyst", "main", "renamed"),
        vec![Privilege::Select]
    );
    assert!(
        db.effective_table_privileges("analyst", "main", "t")
            .is_empty()
    );
}

#[test]
fn rbac_ddl_is_refused_inside_a_transaction() {
    let mut db = store();
    db.execute("BEGIN;").unwrap();
    let e = err(&mut db, "CREATE USER bob PASSWORD 'x';");
    assert!(
        e.contains("CREATE USER is not allowed inside a transaction"),
        "{e}"
    );
    let e = err(&mut db, "DROP USER alice;");
    assert!(
        e.contains("DROP USER is not allowed inside a transaction"),
        "{e}"
    );
    let e = err(&mut db, "CREATE ROLE r;");
    assert!(
        e.contains("CREATE ROLE is not allowed inside a transaction"),
        "{e}"
    );
    let e = err(&mut db, "GRANT SELECT ON TABLE t TO alice;");
    assert!(
        e.contains("GRANT is not allowed inside a transaction"),
        "{e}"
    );
    let e = err(&mut db, "REVOKE SELECT ON TABLE t FROM alice;");
    assert!(
        e.contains("REVOKE is not allowed inside a transaction"),
        "{e}"
    );
    db.execute("ROLLBACK;").unwrap();
}

// ---------- enforcement ----------

#[test]
fn an_unauthenticated_embedded_connection_stays_trusted() {
    let mut db = store();
    exec(&mut db, "CREATE DATABASE other;");
    // The embedded API carries no principal: no check runs, even with users,
    // roles and (empty) grants defined.
    for sql in [
        "SELECT * FROM t;",
        "INSERT INTO t VALUES (3, 'c');",
        "UPDATE t SET s = 'x';",
        "DELETE FROM t;",
        "CREATE TABLE other_table (a INTEGER);",
        "DROP TABLE other_table;",
        "CREATE DATABASE third;",
        "DROP DATABASE third;",
        "USE other;",
        "USE main;",
        "CHECKPOINT;",
        "SHOW DATABASES;",
        "DESCRIBE t;",
        "EXPLAIN SELECT * FROM t;",
    ] {
        let e = execute_embedded(&mut db, sql);
        assert!(e.is_empty(), "{sql}: {e}");
    }
    // And through a `Connection` that never logged in.
    let shared = Database::from_store(db);
    let mut conn = shared.connect();
    assert!(conn.principal().is_none());
    assert!(execute(&mut conn, "SELECT * FROM t;").is_empty());
}

fn execute_embedded(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(_) => String::new(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn authenticated_sessions_are_checked_per_statement() {
    let mut db = store();
    exec(&mut db, "CREATE DATABASE other;");
    exec(&mut db, "CREATE TABLE other.o (i INTEGER);");
    let shared = Database::from_store(db);

    let mut alice = alice(&shared);
    assert_eq!(alice.principal(), Some("alice"));

    // No grants at all: reads, writes and DDL are all refused, with a hint
    // naming the missing privilege.
    let e = execute(&mut alice, "SELECT * FROM t;");
    assert!(e.contains("permission denied"), "{e}");
    assert!(e.contains("SELECT privilege on table \"main.t\""), "{e}");
    assert!(e.contains("GRANT SELECT ON TABLE main.t TO alice"), "{e}");
    let e = execute(&mut alice, "INSERT INTO t VALUES (3, 'c');");
    assert!(e.contains("INSERT privilege on table \"main.t\""), "{e}");
    let e = execute(&mut alice, "UPDATE t SET s = 'x';");
    assert!(e.contains("UPDATE privilege on table \"main.t\""), "{e}");
    let e = execute(&mut alice, "DELETE FROM t WHERE i = 1;");
    assert!(e.contains("DELETE privilege on table \"main.t\""), "{e}");
    let e = execute(&mut alice, "CREATE TABLE bob_table (a INTEGER);");
    assert!(e.contains("CREATE privilege on database \"main\""), "{e}");
    let e = execute(&mut alice, "SELECT * FROM other.o;");
    assert!(e.contains("SELECT privilege on table \"other.o\""), "{e}");

    // Granting SELECT lets the read through; the writes stay refused.
    admin_sql(&shared, "GRANT SELECT ON TABLE t TO alice;");
    assert!(execute(&mut alice, "SELECT * FROM t;").is_empty());
    assert!(execute(&mut alice, "SELECT count(*) FROM t;").is_empty());
    assert!(execute(&mut alice, "EXPLAIN SELECT * FROM t;").is_empty());
    let e = execute(&mut alice, "SELECT * FROM other.o;");
    assert!(e.contains("permission denied"), "{e}");

    // A join needs SELECT on every table it reads.
    admin_sql(&shared, "CREATE TABLE other.o2 (i INTEGER);");
    let e = execute(&mut alice, "SELECT * FROM t JOIN other.o2 ON t.i = o2.i;");
    assert!(e.contains("SELECT privilege on table \"other.o2\""), "{e}");

    // UPDATE/DELETE need their own privilege *and* SELECT (they read their
    // predicate).
    admin_sql(&shared, "GRANT UPDATE ON TABLE t TO alice;");
    assert!(execute(&mut alice, "UPDATE t SET s = 'x' WHERE i = 1;").is_empty());
    admin_sql(&shared, "GRANT DELETE ON TABLE t TO alice;");
    assert!(execute(&mut alice, "DELETE FROM t WHERE i = 2;").is_empty());

    // INSERT needs INSERT; INSERT..SELECT additionally needs SELECT on the
    // source.
    admin_sql(&shared, "GRANT INSERT ON TABLE t TO alice;");
    assert!(execute(&mut alice, "INSERT INTO t VALUES (4, 'd');").is_empty());
    let e = execute(&mut alice, "INSERT INTO t SELECT i, 'x' FROM other.o;");
    assert!(e.contains("SELECT privilege on table \"other.o\""), "{e}");

    // Schema DDL is gated by CREATE on the database.
    admin_sql(&shared, "GRANT CREATE ON DATABASE main TO alice;");
    assert!(execute(&mut alice, "CREATE TABLE alice_table (a INTEGER);").is_empty());
    assert!(execute(&mut alice, "DROP TABLE alice_table;").is_empty());
    assert!(execute(&mut alice, "CREATE VIEW v AS SELECT * FROM t;").is_empty());
    assert!(execute(&mut alice, "DROP VIEW v;").is_empty());
    assert!(execute(&mut alice, "CREATE INDEX ix ON t (i);").is_empty());
    assert!(execute(&mut alice, "DROP INDEX ix;").is_empty());
    // CREATE TABLE AS reads its source, so it needs SELECT too.
    let e = execute(&mut alice, "CREATE TABLE copy AS SELECT * FROM other.o;");
    assert!(e.contains("SELECT privilege on table \"other.o\""), "{e}");
    assert!(execute(&mut alice, "CREATE TABLE copy AS SELECT * FROM t;").is_empty());
    admin_sql(&shared, "DROP TABLE copy;");
    // A stored procedure can be defined with CREATE…
    assert!(
        execute(
            &mut alice,
            "CREATE PROCEDURE p() AS $$ BEGIN SELECT 1; END $$;"
        )
        .is_empty()
    );
    // …but running one requires ADMIN (its body is not privilege-checked).
    let e = execute(&mut alice, "CALL p();");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");

    // Metadata and session control need no privilege.
    for sql in [
        "SHOW TABLES;",
        "SHOW DATABASES;",
        "SHOW MATERIALIZED VIEWS;",
        "DESCRIBE t;",
        "USE main;",
        "BEGIN;",
        "COMMIT;",
        "SELECT 1;",
    ] {
        let e = execute(&mut alice, sql);
        assert!(e.is_empty(), "{sql}: {e}");
    }

    // Store-level statements need ADMIN.
    let e = execute(&mut alice, "CREATE DATABASE nope;");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");
    let e = execute(&mut alice, "CHECKPOINT;");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");
    let e = execute(&mut alice, "LOAD EXTENSION 'nope.so';");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");
    // User/role management needs ADMIN.
    let e = execute(&mut alice, "CREATE USER bob PASSWORD 'x';");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");
    let e = execute(&mut alice, "GRANT SELECT ON TABLE t TO alice;");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");
}

#[test]
fn privileges_flow_through_nested_roles_and_admin_passes_everything() {
    let mut db = store();
    exec(&mut db, "CREATE ROLE reader;");
    exec(&mut db, "CREATE ROLE senior;");
    exec(&mut db, "GRANT reader TO senior;");
    exec(&mut db, "GRANT senior TO analyst;");
    exec(&mut db, "GRANT analyst TO alice;");
    // The privilege on the innermost role reaches the user three levels up.
    exec(&mut db, "GRANT SELECT ON TABLE t TO reader;");
    let shared = Database::from_store(db);
    let mut alice = alice(&shared);
    assert!(execute(&mut alice, "SELECT * FROM t;").is_empty());
    let e = execute(&mut alice, "INSERT INTO t VALUES (9, 'z');");
    assert!(e.contains("permission denied"), "{e}");
    // A grant to the *user* works exactly like a grant to one of its roles.
    admin_sql(&shared, "GRANT INSERT ON TABLE t TO alice;");
    assert!(execute(&mut alice, "INSERT INTO t VALUES (9, 'z');").is_empty());
    // Admin passes every check (including the privileged statements).
    admin_sql(&shared, "GRANT ADMIN TO alice;");
    for sql in [
        "DELETE FROM t;",
        "DROP TABLE t;",
        "CREATE DATABASE admin_db;",
        "DROP DATABASE admin_db;",
        "CREATE USER bob PASSWORD 'bob-secret';",
        "DROP USER bob;",
        "CHECKPOINT;",
    ] {
        let e = execute(&mut alice, sql);
        assert!(e.is_empty(), "{sql}: {e}");
    }
    // Revoking ADMIN takes the bypass away again.
    admin_sql(&shared, "REVOKE ADMIN FROM alice;");
    let e = execute(&mut alice, "CREATE DATABASE other;");
    assert!(e.contains("needs the ADMIN privilege"), "{e}");
}

#[test]
fn revoked_privileges_stop_working_immediately() {
    let mut db = store();
    exec(&mut db, "GRANT SELECT ON TABLE t TO alice;");
    let shared = Database::from_store(db);
    let mut alice = alice(&shared);
    assert!(execute(&mut alice, "SELECT * FROM t;").is_empty());
    admin_sql(&shared, "REVOKE SELECT ON TABLE t FROM alice;");
    let e = execute(&mut alice, "SELECT * FROM t;");
    assert!(e.contains("permission denied"), "{e}");
}

#[test]
fn prepared_statements_are_checked_at_execution() {
    let db = store();
    let shared = Database::from_store(db);
    let mut alice = alice(&shared);
    // Preparing needs no privilege (parsing only) …
    let stmt = alice.prepare("SELECT * FROM t WHERE i = ?;").unwrap();
    // … binding and executing it does.
    let e = alice
        .execute_prepared(&stmt, &[Value::Integer(1)])
        .unwrap_err()
        .to_string();
    assert!(e.contains("permission denied"), "{e}");
    admin_sql(&shared, "GRANT SELECT ON TABLE t TO alice;");
    let rows = alice.execute_prepared(&stmt, &[Value::Integer(1)]).unwrap();
    assert!(matches!(rows, QueryResult::Selected { .. }));
}

#[test]
fn has_users_is_false_only_for_an_empty_catalog() {
    let mut db = PinkDb::new();
    assert!(!db.has_users());
    exec(&mut db, "CREATE ROLE r;");
    // A role alone does not switch the server's authentication on: no one
    // could log in.
    assert!(!db.has_users());
    exec(&mut db, "CREATE USER bob PASSWORD 'x';");
    assert!(db.has_users());
    exec(&mut db, "DROP USER bob;");
    assert!(!db.has_users());
}

// ---------- persistence ----------

#[test]
fn pink_round_trip_preserves_users_roles_and_grants() {
    let dir = scratch("save");
    let path = dir.join("db.pink");
    let mut db = store();
    exec(&mut db, "CREATE ROLE reader;");
    exec(&mut db, "GRANT reader TO analyst;");
    exec(&mut db, "GRANT analyst TO alice;");
    exec(&mut db, "GRANT SELECT, INSERT ON TABLE t TO reader;");
    exec(&mut db, "GRANT CREATE ON DATABASE main TO analyst;");
    exec(&mut db, "GRANT ADMIN TO analyst;");
    db.save(path.to_str().unwrap()).unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let flags = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    assert_eq!(flags & 0x20, 0x20, "the RBAC flag bit is set");
    // A store without RBAC data keeps flags 0 (byte-identical to wave 47).
    let plain = {
        let mut p = PinkDb::new();
        exec(&mut p, "CREATE TABLE t (i INTEGER);");
        p.to_pink_bytes().unwrap()
    };
    assert_eq!(
        u32::from_le_bytes([plain[8], plain[9], plain[10], plain[11]]),
        0
    );

    let mut reopened = PinkDb::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        reopened.authenticate("alice", "alice-secret").unwrap(),
        "alice"
    );
    assert_eq!(
        reopened.effective_table_privileges("alice", "main", "t"),
        vec![Privilege::Select, Privilege::Insert]
    );
    assert_eq!(
        reopened.effective_database_privileges("alice", "main"),
        vec![Privilege::Create]
    );
    assert!(reopened.principal_is_admin("alice"));
    // DDL still works after the round trip (the catalog is live).
    exec(&mut reopened, "ALTER USER alice PASSWORD 'changed';");
    assert!(reopened.authenticate("alice", "changed").is_ok());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn wal_replay_reproduces_rbac_state_without_a_checkpoint() {
    let dir = scratch("wal");
    let path = dir.join("db.pink");
    {
        let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
        exec(&mut db, "CREATE TABLE t (i INTEGER);");
        exec(&mut db, "CREATE USER alice PASSWORD 'alice-secret';");
        exec(&mut db, "CREATE ROLE analyst;");
        exec(&mut db, "GRANT analyst TO alice;");
        exec(&mut db, "GRANT SELECT ON TABLE t TO analyst;");
        exec(&mut db, "GRANT ADMIN TO alice;");
        exec(&mut db, "ALTER USER alice PASSWORD 'rotated';");
    }
    // No checkpoint: the whole state comes from the WAL.
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(db.authenticate("alice", "rotated").unwrap(), "alice");
    assert!(db.authenticate("alice", "alice-secret").is_err());
    assert_eq!(
        db.effective_table_privileges("alice", "main", "t"),
        vec![Privilege::Select]
    );
    assert!(db.principal_is_admin("alice"));
    // A checkpoint + further DDL still replays (the section, not the WAL).
    exec(&mut db, "REVOKE ADMIN FROM alice;");
    db.checkpoint().unwrap();
    exec(&mut db, "CREATE USER bob PASSWORD 'bob-secret';");
    drop(db);
    let db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert!(!db.principal_is_admin("alice"));
    assert_eq!(db.authenticate("bob", "bob-secret").unwrap(), "bob");
    assert_eq!(
        db.effective_table_privileges("alice", "main", "t"),
        vec![Privilege::Select]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn no_plaintext_password_reaches_the_pink_file_or_the_wal() {
    let dir = scratch("secrets");
    let path = dir.join("db.pink");
    let plaintext = "hunter2-do-not-leak";
    {
        let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
        exec(&mut db, "CREATE TABLE t (i INTEGER);");
        exec(&mut db, "CREATE USER alice PASSWORD 'hunter2-do-not-leak';");
        exec(&mut db, "CREATE USER bob PASSWORD 'hunter2-do-not-leak';");
        exec(&mut db, "ALTER USER alice PASSWORD 'hunter2-do-not-leak';");
        // The WAL holds the replayed DDL records (before a checkpoint rolls
        // them into the catalog).
        let wal = std::fs::read(dir.join("db.pink.wal")).unwrap();
        assert!(
            contains_bytes(&wal, b"$argon2id$v=19$"),
            "the WAL should hold the rewritten PASSWORD_HASH form"
        );
        db.checkpoint().unwrap();
        let checkpoint = std::fs::read(&path).unwrap();
        for (what, bytes) in [(".pink", &checkpoint), ("WAL", &wal)] {
            assert!(
                !contains_bytes(bytes, plaintext.as_bytes()),
                "{what} contains the plaintext password"
            );
            assert!(
                contains_bytes(bytes, b"$argon2id$v=19$"),
                "{what} should hold the PHC hash"
            );
        }
        // The two users got *different* salts for the same password.
        let a = db.user_password_hash("alice").unwrap();
        let b = db.user_password_hash("bob").unwrap();
        assert_ne!(a, b, "a fresh salt per hash");
        assert!(pinkdb::verify_password(a, plaintext));
        assert!(pinkdb::verify_password(b, plaintext));
    }
    // The reopened store still authenticates with the original plaintext.
    let db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(db.authenticate("alice", plaintext).unwrap(), "alice");
    std::fs::remove_dir_all(&dir).ok();
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn an_awkward_user_name_round_trips_through_the_wal_rewrite() {
    // The WAL logs the rewritten `PASSWORD_HASH` statement, which must
    // re-parse: a name that needs quoting (a reserved word, a space, an
    // embedded double quote) exercises the rewrite's identifier quoting.
    let dir = scratch("names");
    let path = dir.join("db.pink");
    {
        let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
        // `select` is a reserved word; the space and quote exercise the
        // quoting path; the mixed case survives as the display name.
        exec(&mut db, "CREATE USER \"select\" PASSWORD 'a-secret';");
        exec(
            &mut db,
            "CREATE USER \"Weird Name\"\"x\" PASSWORD 'b-secret';",
        );
        exec(&mut db, "GRANT ADMIN TO \"select\";");
    }
    let db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(db.authenticate("select", "a-secret").unwrap(), "select");
    assert_eq!(
        db.authenticate("Weird Name\"x", "b-secret").unwrap(),
        "Weird Name\"x"
    );
    assert!(db.principal_is_admin("SELECT"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn table_grants_follow_a_transactions_drop_and_rename() {
    // A transaction's `DROP TABLE`/`RENAME` decides the table's grant entry
    // in its private snapshot; COMMIT must carry that decision into the
    // shared store (and ROLLBACK must not).
    let mut db = store();
    exec(&mut db, "GRANT SELECT ON TABLE t TO analyst;");
    exec(&mut db, "GRANT analyst TO alice;");
    let shared = Database::from_store(db);
    let mut conn = shared.connect();
    conn.execute("BEGIN;").unwrap();
    conn.execute("DROP TABLE t;").unwrap();
    conn.execute("ROLLBACK;").unwrap();
    // The rollback restored the table *and* its grants.
    let mut reader = alice(&shared);
    let e = execute(&mut reader, "SELECT * FROM t;");
    assert!(e.is_empty(), "after rollback: {e}");
    // The same drop committed: the grants go with the table and are not
    // inherited by a table of the same name created later.
    conn.execute("BEGIN;").unwrap();
    conn.execute("DROP TABLE t;").unwrap();
    conn.execute("COMMIT;").unwrap();
    conn.execute("CREATE TABLE t (i INTEGER);").unwrap();
    let e = execute(&mut reader, "SELECT * FROM t;");
    assert!(e.contains("permission denied"), "{e}");
    // A rename inside a transaction moves the grants with the table.
    let mut db2 = store();
    exec(&mut db2, "GRANT SELECT ON TABLE t TO analyst;");
    exec(&mut db2, "GRANT analyst TO alice;");
    let shared2 = Database::from_store(db2);
    let mut conn2 = shared2.connect();
    conn2.execute("BEGIN;").unwrap();
    conn2.execute("ALTER TABLE t RENAME TO t2;").unwrap();
    conn2.execute("COMMIT;").unwrap();
    let mut reader2 = alice(&shared2);
    assert!(execute(&mut reader2, "SELECT * FROM t2;").is_empty());
    let e = execute(&mut reader2, "SELECT * FROM t;");
    assert!(
        e.contains("does not exist") || e.contains("not found"),
        "{e}"
    );
}
