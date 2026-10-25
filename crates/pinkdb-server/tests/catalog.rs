//! Wave 96b: a `--catalog` store — engine metadata in the lake catalog, rows
//! in the lake's data files — round-trips across a restart with no local
//! state, refuses a second server, and keeps secrets hashed.
//!
//! Every test drives a real ephemeral-port [`Server`] over TCP with a real
//! [`Client`], and each test owns a fresh temporary directory pair (catalog
//! database + data location), so the suite is parallel-safe.
#![cfg(feature = "catalog")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pinkdb::{QueryResult, Value};
use pinkdb_server::{CatalogStore, Client, Server, ServerHandle};

/// The hermetic S3 server the CLI's `lake_s3` tests use: no cloud, no
/// credentials, a local plain-HTTP listener.
#[cfg(feature = "lake-s3")]
#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

/// A fresh directory under the system temp dir.
fn tempdir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-96b-{tag}-{}-{nanos}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// A server on an ephemeral port over a catalog store, alive until dropped.
struct Running {
    store: CatalogStore,
    _handle: ServerHandle,
    addr: String,
}

impl Running {
    /// Open the catalog store and serve it. The owner string is the lease's
    /// identity, not the process id, so a test can restart on the same
    /// catalog under a new name.
    fn start(catalog: &str, data: &Path, owner: &str) -> Running {
        let store = CatalogStore::open(catalog, data.to_str().expect("utf-8 data dir"), owner)
            .unwrap_or_else(|err| panic!("open {catalog}: {err}"));
        let server = Server::bind("127.0.0.1:0", store.database()).expect("bind server");
        let addr = server.local_addr().to_string();
        let handle = server.spawn().expect("spawn server");
        Running {
            store,
            _handle: handle,
            addr,
        }
    }
}

fn values(addr: &str, sql: &str) -> Vec<Vec<Value>> {
    let mut client = open(addr);
    client
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .into_iter()
        .flat_map(|result| match result {
            QueryResult::Selected { rows, .. } => rows,
            _ => Vec::new(),
        })
        .collect()
}

#[cfg(feature = "lake-s3")]
fn ints_client(client: &mut Client, sql: &str) -> Vec<i64> {
    client
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .into_iter()
        .flat_map(|result| match result {
            QueryResult::Selected { rows, .. } => rows,
            _ => Vec::new(),
        })
        .map(|row| match &row[0] {
            Value::Integer(value) => i64::from(*value),
            Value::Bigint(value) => *value,
            other => panic!("{sql}: expected an integer, got {other:?}"),
        })
        .collect()
}

fn ints(addr: &str, sql: &str) -> Vec<i64> {
    values(addr, sql)
        .into_iter()
        .map(|row| match &row[0] {
            Value::Integer(value) => i64::from(*value),
            Value::Bigint(value) => *value,
            other => panic!("{sql}: expected an integer, got {other:?}"),
        })
        .collect()
}

fn texts(addr: &str, sql: &str) -> Vec<String> {
    values(addr, sql)
        .into_iter()
        .map(|row| match &row[0] {
            Value::Varchar(value) => value.clone(),
            other => panic!("{sql}: expected text, got {other:?}"),
        })
        .collect()
}

/// `DESCRIBE` rows with NULL spelled as the empty string.
fn describe(addr: &str, table: &str) -> Vec<Vec<String>> {
    values(addr, &format!("DESCRIBE {table};"))
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Varchar(text) => text,
                    Value::Null => String::new(),
                    other => panic!("DESCRIBE {table}: unexpected {other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Everything the store is asked to keep: tables (constraints, defaults,
/// indexes), views, a macro, a materialized view, a procedure, a trigger,
/// users, roles and grants — then the data that goes with it. Built over the
/// store's embedded (trusted) connection, like the RBAC tests: the first user
/// closes the network's trusted path, and the admin has to exist before
/// anything can be granted to it.
fn build_catalog(store: &CatalogStore) {
    let mut client = store.database().connect();
    for statement in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY);",
        "ALTER TABLE t ADD COLUMN name VARCHAR DEFAULT 'unnamed';",
        "CREATE TABLE u (a INTEGER NOT NULL UNIQUE);",
        "CREATE TABLE audit (a INTEGER);",
        "CREATE INDEX u_a ON u (a);",
        "INSERT INTO t VALUES (1, 'one'), (2, 'two');",
        // The column default, before the restart writes anything.
        "INSERT INTO t (id) VALUES (10);",
        "INSERT INTO u VALUES (1), (2);",
        "CREATE VIEW v AS SELECT id, name FROM t;",
        "CREATE MACRO plus(x) AS x + 1;",
        "CREATE MATERIALIZED VIEW mv AS SELECT count(*) AS n FROM t;",
        "CREATE PROCEDURE add_row() AS $$ BEGIN INSERT INTO t VALUES (3, 'proc'); END; $$;",
        "CREATE TRIGGER u_after AFTER INSERT ON u FOR EACH ROW \
         EXECUTE INSERT INTO audit VALUES (NEW.a);",
        // Fired by the trigger: this row must be in `audit` after a restart.
        "INSERT INTO u VALUES (7);",
        "CREATE USER root PASSWORD 'root-secret';",
        "GRANT ADMIN TO root;",
        "CREATE USER alice PASSWORD 'alice-secret';",
        "CREATE ROLE reader;",
        "GRANT SELECT ON TABLE t TO reader;",
        "GRANT reader TO alice;",
    ] {
        client
            .execute(statement)
            .unwrap_or_else(|err| panic!("{statement}: {err}"));
    }
}

/// The same observations before and after the restart: identical data,
/// identical column/constraint metadata, and every catalog object still there.
/// Read-only plus "does it already exist" probes, so it can run on both sides.
fn assert_survived(addr: &str, phase: &str) {
    assert_eq!(
        ints(addr, "SELECT id FROM t WHERE id <= 2 ORDER BY id;"),
        vec![1, 2],
        "{phase}"
    );
    assert_eq!(
        texts(addr, "SELECT name FROM t WHERE id = 10;"),
        vec!["unnamed".to_string()],
        "{phase}: the column default"
    );
    assert_eq!(
        ints(addr, "SELECT a FROM audit ORDER BY a;"),
        vec![7],
        "{phase}: the trigger's insert"
    );
    // Primary key, unique and NOT NULL are declarative metadata (like
    // DuckDB): they round-trip as DESCRIBE's `key` and `null` columns.
    let expected: Vec<Vec<String>> = vec![
        vec![
            "id".into(),
            "INTEGER".into(),
            "NO".into(),
            "PRI".into(),
            "".into(),
            "".into(),
        ],
        vec![
            "name".into(),
            "VARCHAR".into(),
            "YES".into(),
            "".into(),
            "'unnamed'".into(),
            "".into(),
        ],
    ];
    assert_eq!(describe(addr, "t"), expected, "{phase}");
    let expected = vec![vec!["a", "INTEGER", "NO", "UNI", "", ""]];
    assert_eq!(describe(addr, "u"), expected, "{phase}");
    // Existence probes: each definition fails to be created again.
    let mut client = open(addr);
    for statement in [
        "CREATE INDEX u_a ON u (a);",
        "CREATE VIEW v AS SELECT 1;",
        "CREATE MACRO plus(x) AS x + 1;",
        "CREATE PROCEDURE add_row() AS $$ BEGIN END; $$;",
        "CREATE TRIGGER u_after AFTER INSERT ON u FOR EACH ROW EXECUTE SELECT 1;",
        "CREATE USER root PASSWORD 'other';",
        "CREATE ROLE reader;",
    ] {
        assert!(
            client.execute(statement).is_err(),
            "{phase}: {statement} should find the definition already there"
        );
    }
    // Definitions that are not merely present but still work.
    assert_eq!(ints(addr, "SELECT plus(41);"), vec![42], "{phase}");
    assert_eq!(ints(addr, "SELECT n FROM mv;"), vec![3], "{phase}");
    assert_eq!(
        values(addr, "SELECT id FROM v WHERE id = 1;").len(),
        1,
        "{phase}"
    );
}

/// A client: as the admin once the store has users, anonymous before that.
fn open(addr: &str) -> Client {
    Client::connect_with_login(addr, "root", "root-secret")
        .or_else(|_| Client::connect(addr))
        .expect("connect")
}

/// The full round trip on one catalog backend: build everything, stop the
/// server, keep only the catalog and the data location, start a fresh server
/// and see the identical store.
fn round_trip(name: &str, catalog: String) {
    let scheme = name;
    let dir = tempdir(name);
    let data = dir.join("data");

    {
        let running = Running::start(&catalog, &data, "test-96b-first");
        build_catalog(&running.store);
        assert_survived(&running.addr, "before restart");
        assert!(running.store.version() > 0, "the build published versions");
    }
    // The server kept nothing durable of its own here: no local scratch, no
    // state beside the catalog database and the lake data files.
    let mut foreign: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("list scratch dir").flatten() {
        let path = entry.path();
        // The catalog database's own files (and their WAL/lock sidecars) are
        // the catalog, not server-local state; the data directory is the lake.
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with("catalog.") || path == data {
            continue;
        }
        foreign.push(path);
    }
    assert!(
        foreign.is_empty(),
        "{scheme}: unexpected local state after the server stopped: {foreign:?}"
    );

    // A fresh server on the same catalog and data: no shared memory, no
    // process state, no local file of ours.
    let running = Running::start(&catalog, &data, "test-96b-second");
    assert_survived(&running.addr, "after restart");

    // A procedure and a trigger still run, on data the restart did not see.
    let mut client = open(&running.addr);
    client.execute("CALL add_row();").unwrap();
    assert_eq!(
        ints(&running.addr, "SELECT id FROM t WHERE id = 3;"),
        vec![3],
        "{scheme}: the procedure's body"
    );
    // The materialized view still refreshes against the surviving tables.
    client.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
    assert_eq!(
        ints(&running.addr, "SELECT n FROM mv;"),
        vec![4],
        "{scheme}: the materialized view refreshes"
    );
    client.execute("INSERT INTO u VALUES (9);").unwrap();
    assert_eq!(
        ints(&running.addr, "SELECT a FROM audit ORDER BY a;"),
        vec![7, 9],
        "{scheme}: the trigger still fires"
    );

    // RBAC survived: alice logs in (the password hash was not lost) and her
    // role's grant applies, while a table she was not granted is refused.
    let mut alice = Client::connect_with_login(&running.addr, "alice", "alice-secret")
        .expect("alice logs in after the restart");
    assert!(
        alice.execute("SELECT id FROM t ORDER BY id;").is_ok(),
        "{scheme}: alice reads t through her role"
    );
    assert!(
        alice
            .execute("INSERT INTO t VALUES (500, 'nope');")
            .is_err(),
        "{scheme}: alice has no INSERT grant"
    );
    assert!(
        alice.execute("SELECT a FROM audit;").is_err(),
        "{scheme}: alice has no grant on audit"
    );
    assert!(
        Client::connect_with_login(&running.addr, "alice", "wrong-password").is_err(),
        "{scheme}: the password hash still gates the login"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn round_trip_through_a_pink_catalog() {
    let dir = tempdir("pink");
    round_trip(
        "pink",
        format!("pink:{}", dir.join("catalog.pink-catalog").display()),
    );
}

#[test]
fn round_trip_through_a_sqlite_catalog() {
    let dir = tempdir("sqlite");
    round_trip(
        "sqlite",
        format!("sqlite:{}", dir.join("catalog.sqlite").display()),
    );
}

/// The Postgres catalog, when the binary was built with `catalog-postgres`
/// and the environment points at a throwaway server (`PINKDB_TEST_POSTGRES_URL`,
/// as the pink-lake suite uses it). Each run gets its own schema so parallel
/// runs and leftovers cannot collide.
#[test]
#[cfg(feature = "catalog-postgres")]
fn round_trip_through_a_postgres_catalog() {
    let Ok(url) = std::env::var("PINKDB_TEST_POSTGRES_URL") else {
        eprintln!("skip: PINKDB_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url.trim();
    if url.is_empty() {
        eprintln!("skip: PINKDB_TEST_POSTGRES_URL is empty");
        return;
    }
    let schema = format!(
        "w96b_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let separator = if url.contains('?') { '&' } else { '?' };
    round_trip("postgres", format!("{url}{separator}schema={schema}"));
}

#[test]
fn a_second_server_is_refused_until_phase_2() {
    let dir = tempdir("lease");
    let catalog = format!("pink:{}", dir.join("catalog.pink-catalog").display());
    let data = dir.join("data");
    let first = Running::start(&catalog, &data, "test-owner-one");
    let error = match CatalogStore::open(&catalog, data.to_str().unwrap(), "test-owner-two") {
        Ok(_) => panic!("a second server must be refused"),
        Err(error) => error,
    };
    assert!(
        error.contains("already served by test-owner-one"),
        "the refusal names the holder: {error}"
    );
    // A clean shutdown releases the lease, so the next server can take it.
    drop(first);
    let second = Running::start(&catalog, &data, "test-owner-three");
    assert_eq!(ints(&second.addr, "SELECT 1;"), vec![1]);
    drop(second);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A killed server (no clean shutdown, no lease release) leaves a catalog and
/// data location that a later server loads: every acknowledged statement is
/// there, because each one was published before it was acknowledged.
#[test]
fn a_killed_server_leaves_a_complete_catalog() {
    let dir = tempdir("killed");
    let catalog = format!("sqlite:{}", dir.join("catalog.sqlite").display());
    let data = dir.join("data");
    let (mut child, mut client) = spawn_child(&catalog, data.to_str().unwrap(), &[]);
    client.execute("CREATE TABLE t (a INTEGER);").unwrap();
    for value in 1..=5 {
        client
            .execute(&format!("INSERT INTO t VALUES ({value});"))
            .unwrap();
    }
    drop(client);
    child.kill().expect("kill the server");
    let _ = child.wait();

    // The dead server held the lease until its heartbeat went stale: that is
    // the crash window, and it is exactly what a restart has to get past.
    let error = match CatalogStore::open(&catalog, data.to_str().unwrap(), "test-after-kill") {
        Ok(_) => panic!("the dead server's lease must still be fresh"),
        Err(error) => error,
    };
    assert!(error.contains("already served by"), "{error}");
    std::thread::sleep(pink_lake::LEASE_TIMEOUT + Duration::from_millis(300));

    let running = Running::start(&catalog, &data, "test-after-kill");
    assert_eq!(
        ints(&running.addr, "SELECT a FROM t ORDER BY a;"),
        vec![1, 2, 3, 4, 5]
    );
    drop(running);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A port reserved for a child server: the child cannot report the ephemeral
/// port it picked, and a fixed one would collide in CI.
fn reserved_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("reserve a port")
        .port()
}

/// Spawn `pinkdb-server --catalog … --data …` as a child process and wait for
/// it to accept a trusted connection. Children are the only way to give a
/// server its own `AWS_*` environment, so the real credentials of the machine
/// never reach the mock; every caller passes a cleared environment.
fn spawn_child(catalog: &str, data: &str, envs: &[(&str, &str)]) -> (std::process::Child, Client) {
    let port = reserved_port();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pinkdb-server"));
    for name in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_S3_FORCE_PATH_STYLE",
        "PINKDB_S3_ALLOW_DEGRADED",
        "PINKDB_S3_ALLOW_HTTP",
    ] {
        command.env_remove(name);
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--catalog",
            catalog,
            "--data",
            data,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn pinkdb-server");
    let addr = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(120);
    let client = loop {
        match Client::connect(&addr) {
            Ok(client) => break client,
            Err(err) if Instant::now() < deadline => {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!("the server exited early ({status}): {err}");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("the server never accepted a connection: {err}"),
        }
    };
    (child, client)
}

/// The mock's environment: credentials the mock accepts, its endpoint, and
/// the plain-HTTP opt-in a local test endpoint needs.
#[cfg(feature = "lake-s3")]
fn s3_env(mock: &mock_s3::MockS3) -> Vec<(&'static str, String)> {
    vec![
        ("AWS_ACCESS_KEY_ID", mock.access_key_id()),
        ("AWS_SECRET_ACCESS_KEY", mock.secret_access_key()),
        ("AWS_ENDPOINT_URL", mock.endpoint()),
        ("AWS_ENDPOINT_URL_S3", mock.endpoint()),
        ("AWS_REGION", "auto".to_string()),
        ("AWS_S3_FORCE_PATH_STYLE", "1".to_string()),
        ("PINKDB_S3_ALLOW_HTTP", "1".to_string()),
    ]
}

/// Data in mock S3: rows above the inlining limit land in the bucket, so the
/// restart reads the table data from object storage, not from the catalog.
#[test]
#[cfg(feature = "lake-s3")]
fn round_trip_on_mock_s3_data() {
    let mock = mock_s3::MockS3::start();
    let dir = tempdir("s3");
    let catalog = format!("sqlite:{}", dir.join("catalog.sqlite").display());
    let data = mock.url("wave96b", "lake");
    let env = s3_env(&mock);
    let borrowed: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();

    let (mut first, mut client) = spawn_child(&catalog, &data, &borrowed);
    client
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR);")
        .unwrap();
    let rows: Vec<String> = (1..=30).map(|i| format!("({i}, 'row-{i}')")).collect();
    client
        .execute(&format!("INSERT INTO t VALUES {};", rows.join(", ")))
        .unwrap();
    client
        .execute("CREATE VIEW big AS SELECT id FROM t WHERE id > 25;")
        .unwrap();
    client.execute("CREATE MACRO twice(x) AS x * 2;").unwrap();
    drop(client);
    first.kill().expect("kill the first server");
    let _ = first.wait();

    let keys = mock.keys();
    assert!(
        keys.iter().any(|key| key.contains("/data/")),
        "no data file reached the bucket: {keys:?}"
    );
    assert!(
        !keys.iter().any(|key| key.ends_with("catalog.json")),
        "the metadata must stay in the catalog, not in the bucket: {keys:?}"
    );

    std::thread::sleep(pink_lake::LEASE_TIMEOUT + Duration::from_millis(300));
    let (mut second, mut client) = spawn_child(&catalog, &data, &borrowed);
    assert_eq!(
        ints_client(&mut client, "SELECT id FROM t ORDER BY id;").len(),
        30
    );
    assert_eq!(
        ints_client(&mut client, "SELECT id FROM big ORDER BY id;"),
        vec![26, 27, 28, 29, 30],
        "the view survived"
    );
    assert_eq!(
        ints_client(&mut client, "SELECT twice(4);"),
        vec![8],
        "the macro survived"
    );
    second.kill().expect("kill the second server");
    let _ = second.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn secrets_are_not_stored_in_plaintext() {
    let dir = tempdir("secrets");
    let catalog = format!("sqlite:{}", dir.join("catalog.sqlite").display());
    let data = dir.join("data");
    let running = Running::start(&catalog, &data, "test-secrets");
    let mut client = running.store.database().connect();
    client
        .execute("CREATE USER carol PASSWORD 'correct-horse-battery';")
        .unwrap();
    drop(client);
    drop(running);

    // The password itself appears nowhere in the catalog (which holds the
    // engine image) nor in the data files; the argon2 hash does.
    let mut saw_hash = false;
    for path in walk(&dir) {
        let bytes = std::fs::read(&path).unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("correct-horse-battery"),
            "the plaintext password is in {}",
            path.display()
        );
        saw_hash |= text.contains("$argon2");
    }
    assert!(saw_hash, "the catalog stores the password's argon2 hash");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every file under `dir`.
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files
}
