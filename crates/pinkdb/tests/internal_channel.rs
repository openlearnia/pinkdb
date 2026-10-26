//! Wave 109: an extension's own channel runs without the RBAC gate — that is
//! what lets it keep state in a database nobody may be granted. The gate is
//! then the policy: what the channel may read and write, and whether it may
//! leave the process. These tests pin that policy where it is enforced, for a
//! connection (the CLI's and the server's path).
//!
//! The extension side is tested in `extensions/memory/tests/memory.rs`; this
//! file deliberately plays an extension that quotes its SQL badly.

use pinkdb::package::{ExtensionPackage, HostParam, HostScalar};
use pinkdb::serve::{ServePolicy, SessionHost};
use pinkdb::{Database, ExtType, Value};
use pinkdb_ext_loader::serve::StatementHost;

const MEMORY: &str = "pinkdb_memory";
const AGENT: &str = "pinkdb_agent";

/// The internal policy the CLI hands the memory extension: read and write
/// memory, reach an embeddings provider, and nothing else.
fn memory_channel(services: bool) -> ServePolicy {
    ServePolicy {
        internal: Some(Box::new(ServePolicy {
            allow_writes: true,
            write_database: Some(MEMORY.to_string()),
            read_databases: Some(vec![MEMORY.to_string()]),
            allow_external_services: services,
            ..ServePolicy::default()
        })),
        ..ServePolicy::default()
    }
}

/// The same for the agent's transcript: its database, no files, no services.
fn agent_channel() -> ServePolicy {
    ServePolicy {
        internal: Some(Box::new(ServePolicy {
            allow_writes: true,
            write_database: Some(AGENT.to_string()),
            read_databases: Some(vec![AGENT.to_string()]),
            ..ServePolicy::default()
        })),
        ..ServePolicy::default()
    }
}

/// A store holding both extension databases plus a user table every way round
/// a read can reach it.
fn store() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    for sql in [
        "CREATE DATABASE pinkdb_memory",
        "CREATE DATABASE pinkdb_agent",
        "CREATE TABLE pinkdb_memory.atoms (id VARCHAR, text VARCHAR)",
        "CREATE TABLE pinkdb_memory.atom_vectors (atom VARCHAR, v FLOAT[3])",
        "CREATE TABLE pinkdb_agent.sessions (id VARCHAR)",
        "CREATE TABLE main.secret (x INTEGER)",
        "INSERT INTO main.secret VALUES (42)",
        // A view inside the allowed database, and one outside it.
        "CREATE VIEW pinkdb_memory.peek AS SELECT * FROM main.secret",
        "CREATE VIEW main.peek AS SELECT * FROM main.secret",
        "CREATE MACRO leak() AS TABLE SELECT * FROM main.secret",
    ] {
        conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db
}

/// The extension that declares a call which leaves the process — the shape
/// wave 107c's `embed()` has, without a provider.
fn register_extension() {
    let package = ExtensionPackage::new("wave109_internal").with_host_scalar(
        HostScalar::new(
            "ext_touch",
            vec![HostParam::new("text", ExtType::Varchar)],
            ExtType::Varchar,
            |_db, args| match args.first() {
                Some(Value::Varchar(text)) => Ok(Value::Varchar(format!("touched:{text}"))),
                _ => Ok(Value::Null),
            },
        )
        .external_io(),
    );
    // Another test in this binary may have registered it first; that is the
    // registry's answer, not a failure.
    let _ = pinkdb::register_extension_package(package);
}

fn run(db: &Database, policy: &ServePolicy, sql: &str) -> (i32, String) {
    let mut conn = db.connect();
    SessionHost::new(&mut conn, policy).execute_internal(sql)
}

fn ok(db: &Database, policy: &ServePolicy, sql: &str) -> String {
    let (code, text) = run(db, policy, sql);
    assert_eq!(code, 0, "{sql} must run on the internal channel: {text}");
    text
}

fn refused(db: &Database, policy: &ServePolicy, sql: &str) -> String {
    let (code, text) = run(db, policy, sql);
    assert_eq!(code, 1, "{sql} must be refused: {text}");
    assert!(
        text.contains("\"permission\""),
        "{sql} must be a permission error: {text}"
    );
    text
}

#[test]
fn the_channel_reads_and_writes_its_own_database() {
    let db = store();
    let policy = memory_channel(true);
    ok(
        &db,
        &policy,
        "INSERT INTO pinkdb_memory.atoms VALUES ('a1', 'the warehouse is TPC-H SF1')",
    );
    let text = ok(&db, &policy, "SELECT text FROM pinkdb_memory.atoms");
    assert!(text.contains("TPC-H"), "{text}");
    // The write rule is unchanged: one database, its own.
    let text = refused(&db, &policy, "INSERT INTO main.secret VALUES (1)");
    assert!(text.contains("writes outside it"), "{text}");
}

#[test]
fn a_user_table_is_refused_every_way_a_read_can_reach_it() {
    let db = store();
    let policy = memory_channel(true);
    for sql in [
        // Directly.
        "SELECT * FROM main.secret",
        // Through a view, from inside the allowed database and from outside.
        "SELECT * FROM pinkdb_memory.peek",
        "SELECT * FROM main.peek",
        // Through a CTE and through a macro body.
        "WITH c AS (SELECT * FROM main.secret) SELECT * FROM c",
        "SELECT * FROM leak()",
        // Through a subquery, which is a different plan node.
        "SELECT (SELECT max(x) FROM main.secret) AS m",
    ] {
        let text = refused(&db, &policy, sql);
        assert!(
            text.contains(MEMORY),
            "{sql} must name the channels's own database: {text}"
        );
    }
}

#[test]
fn a_file_source_is_refused_before_it_could_be_bound() {
    let db = store();
    let policy = memory_channel(true);
    for sql in [
        "SELECT * FROM read_csv('/tmp/pinkdb-wave109.csv')",
        "SELECT * FROM read_parquet('/tmp/pinkdb-wave109.parquet')",
        "SELECT * FROM '/tmp/pinkdb-wave109.parquet'",
    ] {
        refused(&db, &policy, sql);
    }
}

#[test]
fn an_external_service_is_its_own_switch() {
    let db = store();
    register_extension();
    db.set_allow_sql_load_extension(true);
    db.connect()
        .execute("LOAD EXTENSION 'wave109_internal'")
        .expect("the test's own package loads");

    // The embeddings-shaped call is what the memory channel is allowed to
    // make: the recall half of a hybrid search needs the provider.
    let text = ok(&db, &memory_channel(true), "SELECT ext_touch('x') AS v");
    assert!(text.contains("touched:x"), "{text}");

    // Without that switch the same call is refused — with the list of its own
    // tables still allowing everything the statement reads, because it reads
    // none.
    let text = refused(&db, &memory_channel(false), "SELECT ext_touch('x') AS v");
    assert!(text.contains("external services are disabled"), "{text}");

    // And the reverse: a channel that may call out still may not read a file.
    let policy = memory_channel(true);
    refused(
        &db,
        &policy,
        "SELECT * FROM read_csv('/tmp/pinkdb-wave109.csv')",
    );
}

#[test]
fn the_agent_channel_is_confined_the_same_way() {
    let db = store();
    let policy = agent_channel();
    ok(
        &db,
        &policy,
        "INSERT INTO pinkdb_agent.sessions VALUES ('s1')",
    );
    let text = ok(&db, &policy, "SELECT id FROM pinkdb_agent.sessions");
    assert!(text.contains("s1"), "{text}");
    for sql in [
        "SELECT * FROM main.secret",
        "SELECT * FROM pinkdb_memory.atoms",
        "WITH c AS (SELECT * FROM pinkdb_memory.atoms) SELECT * FROM c",
    ] {
        let text = refused(&db, &policy, sql);
        assert!(text.contains(AGENT), "{sql} must name its database: {text}");
    }
}
