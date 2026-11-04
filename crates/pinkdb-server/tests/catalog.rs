//! Wave 124: the server owns its data. A data directory holds the served
//! store, the state store, managed lakes and upload staging; local and lake
//! databases survive SIGTERM and a restart; `--in-memory` writes nothing;
//! lake locations are confined to the data directory and `--lake-root`s;
//! only an administrator attaches; a lake has one writer at a time; a killed
//! server's lake reopens once its lease goes stale; passwords stay hashed.
//! (Wave 96b's `--catalog` mode, which these tests used to cover, is gone:
//! a lake is a database now.)
//!
//! Every test drives the real binary on an ephemeral port in its own
//! temporary directory, so the suite is parallel-safe.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pinkdb::{QueryResult, Value};
use pinkdb_server::Client;

/// The hermetic S3 server the CLI's `lake_s3` tests use: no cloud, no
/// credentials, a local plain-HTTP listener.
#[cfg(feature = "lake-s3")]
#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

/// A fresh directory under the system temp dir (canonical: macOS's `/var`
/// is a link to `/private/var`, and lake paths are compared resolved).
fn tempdir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-124-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir.canonicalize().expect("canonical temp dir")
}

/// A running server binary: its child and address. Killed on drop.
struct Running {
    child: Child,
    addr: String,
}

impl Running {
    /// Spawn the binary in `cwd` with `args` (plus `--port 0`) and `envs`,
    /// and wait for its listening line.
    fn start(cwd: &Path, args: &[&str], envs: &[(&str, String)]) -> Running {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"));
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            "AWS_S3_FORCE_PATH_STYLE",
            "PINKDB_S3_ALLOW_HTTP",
        ] {
            command.env_remove(name);
        }
        for (key, value) in envs {
            command.env(key, value);
        }
        let child = command
            .current_dir(cwd)
            .args(["--host", "127.0.0.1", "--port", "0"])
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn pinkdb-server");
        // Owned by `Running` from here: its drop reaps the child on a panic.
        let mut running = Running {
            child,
            addr: String::new(),
        };
        let stdout = running.child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader
                .read_line(&mut line)
                .expect("read the server's stdout");
            assert!(n > 0, "pinkdb-server exited before listening");
            if let Some(rest) = line.split_once("listening on ") {
                let addr = rest.1.split_whitespace().next().unwrap().to_string();
                // Keep draining stdout so the server never blocks on it.
                std::thread::spawn(move || for _ in reader.lines() {});
                running.addr = addr;
                return running;
            }
        }
    }

    fn client(&self) -> Client {
        Client::connect(&self.addr).expect("connect")
    }

    /// SIGTERM, then wait (bounded) for the exit.
    fn terminate(mut self) -> ExitStatus {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success());
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("wait") {
                return status;
            }
            assert!(Instant::now() < deadline, "the server ignored SIGTERM");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// SIGKILL: no shutdown at all.
    fn kill(mut self) {
        self.child.kill().expect("SIGKILL");
        let _ = self.child.wait();
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn rows(client: &mut Client, sql: &str) -> Vec<Vec<Value>> {
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

fn ints(client: &mut Client, sql: &str) -> Vec<i64> {
    rows(client, sql)
        .into_iter()
        .map(|row| match &row[0] {
            Value::Integer(v) => *v as i64,
            Value::Bigint(v) => *v,
            v => panic!("{sql}: not an integer: {v:?}"),
        })
        .collect()
}

fn error(client: &mut Client, sql: &str) -> String {
    client
        .execute(sql)
        .expect_err(&format!("{sql} must fail"))
        .to_string()
}

/// `SHOW DATABASES` as `(name, storage, version, status)`.
fn databases(client: &mut Client) -> Vec<(String, String, Option<i64>, String)> {
    rows(client, "SHOW DATABASES;")
        .into_iter()
        .map(|r| {
            let text = |v: &Value| match v {
                Value::Varchar(s) => s.clone(),
                v => v.to_string(),
            };
            let version = match &r[4] {
                Value::Bigint(v) => Some(*v),
                _ => None,
            };
            (text(&r[0]), text(&r[1]), version, text(&r[5]))
        })
        .collect()
}

#[test]
fn the_default_data_dir_is_created_in_the_working_directory() {
    let cwd = tempdir("default");
    let server = Running::start(&cwd, &[], &[]);
    let mut client = server.client();
    client.execute("CREATE TABLE t (a INTEGER);").unwrap();
    let data = cwd.join("pinkdb-data");
    assert!(data.join("lakes").is_dir() && data.join("tmp").is_dir());
    assert!(
        data.join("pinkdb.pink").exists() || data.join("pinkdb.pink.wal").exists(),
        "the served store lives in the data directory"
    );
    assert!(
        data.join("state.pink").exists() || data.join("state.pink.wal").exists(),
        "the state store lives in the data directory"
    );
    drop(client);
    let status = server.terminate();
    assert!(status.success(), "a SIGTERM shutdown is clean: {status}");
    // The shutdown checkpointed the store.
    assert!(data.join("pinkdb.pink").is_file());
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn local_and_lake_databases_survive_sigterm_and_restart() {
    let dir = tempdir("restart");
    let data = dir.join("data");
    let data_arg = data.to_str().unwrap().to_string();
    let server = Running::start(&dir, &["--data-dir", &data_arg], &[]);
    let mut client = server.client();
    client
        .execute(
            "CREATE DATABASE sales; CREATE TABLE sales.orders (id INTEGER PRIMARY KEY, \
             amount DOUBLE); INSERT INTO sales.orders VALUES (1, 9.5), (2, 3.0);",
        )
        .unwrap();
    client
        .execute(
            "CREATE DATABASE events (TYPE LAKE); CREATE TABLE events.clicks (id INTEGER, \
             page VARCHAR); INSERT INTO events.clicks VALUES (1, '/'), (2, '/docs'), \
             (3, '/blog');",
        )
        .unwrap();
    // Reading the lake while writing locally is allowed (the read is pinned).
    client
        .execute("CREATE TABLE sales.pages AS SELECT DISTINCT page FROM events.clicks;")
        .unwrap();
    // A transaction cannot write both storages.
    client
        .execute("BEGIN; INSERT INTO sales.orders VALUES (3, 1.0);")
        .unwrap();
    let msg = error(&mut client, "INSERT INTO events.clicks VALUES (4, '/x');");
    assert!(msg.contains("not both"), "{msg}");
    client.execute("ROLLBACK;").unwrap();
    assert!(data.join("lakes").join("events").is_dir(), "a managed lake");
    drop(client);
    assert!(server.terminate().success());

    let server = Running::start(&dir, &["--data-dir", &data_arg], &[]);
    let mut client = server.client();
    assert_eq!(
        ints(&mut client, "SELECT count(*) FROM sales.orders;"),
        vec![2]
    );
    assert_eq!(
        ints(&mut client, "SELECT count(*) FROM events.clicks;"),
        vec![3]
    );
    assert_eq!(
        ints(&mut client, "SELECT count(*) FROM sales.pages;"),
        vec![3]
    );
    let dbs = databases(&mut client);
    let events = dbs.iter().find(|d| d.0 == "events").expect("events listed");
    assert_eq!(events.1, "lake");
    assert_eq!(events.3, "ok");
    assert!(events.2.is_some_and(|v| v >= 3), "{dbs:?}");
    let sales = dbs.iter().find(|d| d.0 == "sales").unwrap();
    assert_eq!(sales.1, "local");
    drop(client);
    assert!(server.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_in_memory_server_writes_nothing() {
    let cwd = tempdir("memory");
    let server = Running::start(&cwd, &["--in-memory"], &[]);
    let mut client = server.client();
    client
        .execute("CREATE DATABASE x; CREATE TABLE x.t (a INTEGER); INSERT INTO x.t VALUES (1);")
        .unwrap();
    let msg = error(&mut client, "CREATE DATABASE l (TYPE LAKE);");
    assert!(msg.contains("needs a CATALOG"), "{msg}");
    drop(client);
    assert!(server.terminate().success());
    let left: Vec<_> = std::fs::read_dir(&cwd).unwrap().collect();
    assert!(left.is_empty(), "an in-memory server wrote {left:?}");
    // The flags exclude each other.
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .current_dir(&cwd)
        .args(["--in-memory", "--data-dir", "d"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("mutually exclusive"));
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn lake_paths_outside_the_lake_roots_are_refused() {
    let dir = tempdir("roots");
    let root = dir.join("archive");
    std::fs::create_dir_all(&root).unwrap();
    let elsewhere = dir.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let data = dir.join("data");
    let server = Running::start(
        &dir,
        &[
            "--data-dir",
            data.to_str().unwrap(),
            "--lake-root",
            root.to_str().unwrap(),
        ],
        &[],
    );
    let mut client = server.client();
    let msg = error(
        &mut client,
        &format!(
            "CREATE DATABASE a (TYPE LAKE, CATALOG 'pink:{}/c.pink', DATA '{}/d');",
            elsewhere.display(),
            elsewhere.display()
        ),
    );
    assert!(
        msg.contains("outside the server's lake directories"),
        "{msg}"
    );
    assert!(!elsewhere.join("c.pink").exists());
    // Inside a --lake-root, and a relative path inside <data-dir>/lakes.
    client
        .execute(&format!(
            "CREATE DATABASE b (TYPE LAKE, CATALOG 'pink:{}/b.pink', DATA '{}/b-data');",
            root.display(),
            root.display()
        ))
        .unwrap();
    client
        .execute("CREATE DATABASE c (TYPE LAKE, CATALOG 'pink:c.pink', DATA 'c-data');")
        .unwrap();
    assert!(data.join("lakes").join("c-data").is_dir());
    let msg = error(
        &mut client,
        "ATTACH 'lake:pink:../escape.pink' AS e (READ_ONLY);",
    );
    assert!(msg.contains("`..`"), "{msg}");
    let msg = error(
        &mut client,
        "ATTACH 'lake:postgres://u:hunter2@localhost/x' AS p (READ_ONLY);",
    );
    assert!(
        msg.contains("carries a password") && !msg.contains("hunter2"),
        "{msg}"
    );
    drop(client);
    assert!(server.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn only_an_administrator_attaches() {
    let dir = tempdir("admin");
    let data = dir.join("data");
    // The users exist before the server starts (a trusted embedded open of
    // the data directory's store, as an operator would bootstrap it).
    {
        let dir = pinkdb_server::DataDir::prepare(&data).unwrap();
        let mut store = pinkdb::PinkDb::open_durable(&dir.store().to_string_lossy()).unwrap();
        store
            .execute(
                "CREATE USER root PASSWORD 'root-pw'; GRANT ADMIN TO root; \
                 CREATE USER bob PASSWORD 'bob-pw';",
            )
            .unwrap();
        store.checkpoint().unwrap();
    }
    let server = Running::start(&dir, &["--data-dir", data.to_str().unwrap()], &[]);
    let mut bob = Client::connect_with_login(&server.addr, "bob", "bob-pw").unwrap();
    for sql in [
        "ATTACH 'lake:pink:x.pink' AS x (READ_ONLY);",
        "CREATE DATABASE y (TYPE LAKE);",
        "DETACH IF EXISTS z;",
    ] {
        let msg = error(&mut bob, sql);
        assert!(msg.contains("ADMIN"), "{sql}: {msg}");
    }
    let mut root = Client::connect_with_login(&server.addr, "root", "root-pw").unwrap();
    root.execute("CREATE DATABASE y (TYPE LAKE);").unwrap();
    root.execute("CREATE TABLE y.t (a INTEGER); GRANT SELECT ON TABLE y.t TO bob;")
        .unwrap();
    assert_eq!(ints(&mut bob, "SELECT count(*) FROM y.t;"), vec![0]);
    drop((bob, root));
    assert!(server.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

/// One writer per lake: a second server's writable ATTACH is refused while
/// the first holds the lease; a read-only one is fine. A SIGKILLed server's
/// lake reopens on restart once its lease goes stale.
#[test]
fn one_writer_per_lake_and_a_killed_servers_lake_reopens() {
    let dir = tempdir("lease");
    let root = dir.join("lakes");
    std::fs::create_dir_all(&root).unwrap();
    let root_arg = root.to_str().unwrap().to_string();
    let a_data = dir.join("a").to_str().unwrap().to_string();
    let b_data = dir.join("b").to_str().unwrap().to_string();
    let catalog = format!("pink:{}/shared.pink", root.display());
    let a = Running::start(
        &dir,
        &["--data-dir", &a_data, "--lake-root", &root_arg],
        &[],
    );
    let mut client = a.client();
    client
        .execute(&format!(
            "CREATE DATABASE shared (TYPE LAKE, CATALOG '{catalog}', DATA '{}/shared-data');\
             CREATE TABLE shared.t (a INTEGER); INSERT INTO shared.t VALUES (1), (2);",
            root.display()
        ))
        .unwrap();
    drop(client);

    let b = Running::start(
        &dir,
        &["--data-dir", &b_data, "--lake-root", &root_arg],
        &[],
    );
    let mut other = b.client();
    let msg = error(&mut other, &format!("ATTACH 'lake:{catalog}' AS shared;"));
    assert!(msg.contains("already served by"), "{msg}");
    other
        .execute(&format!("ATTACH 'lake:{catalog}' AS ro (READ_ONLY);"))
        .unwrap();
    assert_eq!(ints(&mut other, "SELECT count(*) FROM ro.t;"), vec![2]);
    // The identical ATTACH again is a no-op (a client script reruns it); the
    // same name with other options is refused.
    other
        .execute(&format!("ATTACH 'lake:{catalog}' AS ro (READ_ONLY);"))
        .unwrap();
    let msg = error(&mut other, &format!("ATTACH 'lake:{catalog}' AS ro;"));
    assert!(msg.contains("with different options"), "{msg}");
    assert_eq!(ints(&mut other, "SELECT count(*) FROM ro.t;"), vec![2]);
    drop(other);
    assert!(b.terminate().success());

    // Kill A without a shutdown: its lease row stays, with a fresh beat.
    a.kill();
    let started = Instant::now();
    let a = Running::start(
        &dir,
        &["--data-dir", &a_data, "--lake-root", &root_arg],
        &[],
    );
    let mut client = a.client();
    assert_eq!(ints(&mut client, "SELECT count(*) FROM shared.t;"), vec![2]);
    client.execute("INSERT INTO shared.t VALUES (3);").unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "the restart waited out the stale lease, not forever"
    );
    drop(client);
    assert!(a.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn secrets_are_not_stored_in_plaintext() {
    let dir = tempdir("secrets");
    let data = dir.join("data");
    let server = Running::start(&dir, &["--data-dir", data.to_str().unwrap()], &[]);
    let mut client = server.client();
    client
        .execute(
            "CREATE DATABASE l (TYPE LAKE); CREATE TABLE l.t (a INTEGER); \
             CREATE USER carol PASSWORD 'plaintext-sentinel-124';",
        )
        .unwrap();
    drop(client);
    assert!(server.terminate().success());
    for file in walk(&data) {
        let bytes = std::fs::read(&file).unwrap();
        assert!(
            !bytes
                .windows(b"plaintext-sentinel-124".len())
                .any(|w| w == b"plaintext-sentinel-124"),
            "{} holds the plaintext password",
            file.display()
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// A transaction on a lake database publishes one lake version.
#[test]
fn an_explicit_transaction_publishes_once() {
    let dir = tempdir("txn");
    let data = dir.join("data");
    let server = Running::start(&dir, &["--data-dir", data.to_str().unwrap()], &[]);
    let mut client = server.client();
    client
        .execute("CREATE DATABASE l (TYPE LAKE); CREATE TABLE l.t (a INTEGER);")
        .unwrap();
    let version = |client: &mut Client| {
        databases(client)
            .into_iter()
            .find(|d| d.0 == "l")
            .and_then(|d| d.2)
            .unwrap()
    };
    let before = version(&mut client);
    client
        .execute("BEGIN; INSERT INTO l.t VALUES (1); INSERT INTO l.t VALUES (2); COMMIT;")
        .unwrap();
    assert_eq!(version(&mut client), before + 1);
    client
        .execute("BEGIN; INSERT INTO l.t VALUES (3); ROLLBACK;")
        .unwrap();
    assert_eq!(version(&mut client), before + 1);
    assert_eq!(ints(&mut client, "SELECT count(*) FROM l.t;"), vec![2]);
    drop(client);
    assert!(server.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

/// SQLite catalog, data in mock S3: rows above the inlining limit land in the
/// bucket, and the restart reads them back from object storage.
#[test]
#[cfg(all(feature = "lake-s3", feature = "catalog-sqlite"))]
fn a_lake_with_sqlite_catalog_and_s3_data_round_trips() {
    let mock = mock_s3::MockS3::start();
    let env: Vec<(&str, String)> = vec![
        ("AWS_ACCESS_KEY_ID", mock.access_key_id()),
        ("AWS_SECRET_ACCESS_KEY", mock.secret_access_key()),
        ("AWS_ENDPOINT_URL", mock.endpoint()),
        ("AWS_ENDPOINT_URL_S3", mock.endpoint()),
        ("AWS_REGION", "auto".to_string()),
        ("AWS_S3_FORCE_PATH_STYLE", "1".to_string()),
        ("PINKDB_S3_ALLOW_HTTP", "1".to_string()),
    ];
    let dir = tempdir("s3");
    let data = dir.join("data").to_str().unwrap().to_string();
    let bucket = mock.url("wave124", "events");
    let server = Running::start(&dir, &["--data-dir", &data], &env);
    let mut client = server.client();
    let values: Vec<String> = (0..200).map(|i| format!("({i})")).collect();
    client
        .execute(&format!(
            "CREATE DATABASE ev (TYPE LAKE, CATALOG 'sqlite:ev.sqlite', DATA '{bucket}'); \
             CREATE TABLE ev.t (a INTEGER); INSERT INTO ev.t VALUES {};",
            values.join(", ")
        ))
        .unwrap();
    drop(client);
    assert!(server.terminate().success());
    assert!(
        mock.keys().iter().any(|key| key.contains("/data/")),
        "no data file reached the bucket: {:?}",
        mock.keys()
    );
    let server = Running::start(&dir, &["--data-dir", &data], &env);
    let mut client = server.client();
    assert_eq!(
        ints(&mut client, "SELECT count(*), sum(a) FROM ev.t;"),
        vec![200]
    );
    drop(client);
    assert!(server.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A Postgres catalog (a throwaway local server, when
/// `PINKDB_TEST_POSTGRES_URL` is set): the password travels through
/// `PGPASSWORD`, never through the catalog URL.
#[test]
#[cfg(feature = "catalog-postgres")]
fn a_lake_with_a_postgres_catalog_round_trips() {
    let Some(url) = std::env::var("PINKDB_TEST_POSTGRES_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
    else {
        eprintln!("skip: PINKDB_TEST_POSTGRES_URL is not set");
        return;
    };
    let (url, password) = split_password(url.trim());
    let schema = format!("w124_{}", std::process::id());
    let separator = if url.contains('?') { '&' } else { '?' };
    let catalog = format!("{url}{separator}schema={schema}");
    let env: Vec<(&str, String)> = password
        .map(|p| vec![("PGPASSWORD", p)])
        .unwrap_or_default();
    let dir = tempdir("postgres");
    let data = dir.join("data").to_str().unwrap().to_string();
    let server = Running::start(&dir, &["--data-dir", &data], &env);
    let mut client = server.client();
    client
        .execute(&format!(
            "CREATE DATABASE pg (TYPE LAKE, CATALOG '{catalog}', DATA 'pg-data'); \
             CREATE TABLE pg.t (a INTEGER); INSERT INTO pg.t VALUES (1), (2);"
        ))
        .unwrap();
    drop(client);
    assert!(server.terminate().success());
    let server = Running::start(&dir, &["--data-dir", &data], &env);
    let mut client = server.client();
    assert_eq!(ints(&mut client, "SELECT count(*) FROM pg.t;"), vec![2]);
    drop(client);
    assert!(server.terminate().success());
    let _ = std::fs::remove_dir_all(&dir);
}

/// `postgres://user:secret@host/db` → (`postgres://user@host/db`, `secret`).
#[cfg(feature = "catalog-postgres")]
fn split_password(url: &str) -> (String, Option<String>) {
    let Some((scheme, rest)) = url.split_once("://") else {
        return (url.to_string(), None);
    };
    let end = rest.find('/').unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    match authority.rsplit_once('@') {
        Some((userinfo, host)) => match userinfo.split_once(':') {
            Some((user, password)) => (
                format!("{scheme}://{user}@{host}{tail}"),
                Some(password.to_string()),
            ),
            None => (url.to_string(), None),
        },
        None => (url.to_string(), None),
    }
}
