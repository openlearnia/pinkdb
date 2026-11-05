//! Wave 138c: the QuackLab adapter's warehouse as a pinkdb lake database.
//!
//! `GRAIN_DUCKLAKE=1` makes the database every request runs in a lake
//! (`ATTACH 'lake:<catalog>' … CREATE_IF_NOT_EXISTS`); a write lease detaches
//! it so an outside writer can take the lake's writer lease, and release
//! attaches it again at the new head. These tests drive the adapter's HTTP
//! surface in process over:
//!
//! - a local `pink:` catalog with local data (every build);
//! - a Postgres catalog with data in the mock S3 when
//!   `PINKDB_TEST_POSTGRES_URL` is set and the build has `lake-s3` and
//!   `catalog-postgres` (skipped with a note otherwise, like
//!   `pink-lake/tests/postgres_catalog.rs`).
#![cfg(feature = "quacklab")]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pinkdb::Database;
use pinkdb_server::quacklab::{
    LakeConfig, LakeWarehouse, QuacklabHandle, QuacklabOptions, QuacklabServer,
};

const TOKEN: &str = "lake-token";

fn request(addr: &str, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to the adapter");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("a read timeout");
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\
         Authorization: Bearer {TOKEN}\r\n"
    );
    if let Some(body) = body {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write the head");
    if let Some(body) = body {
        stream.write_all(body.as_bytes()).expect("write the body");
    }
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read the reply");
    let (head, body) = response.split_once("\r\n\r\n").expect("a whole response");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code");
    (status, body.to_string())
}

fn sql(addr: &str, statement: &str) -> (u16, String) {
    let body = serde_json::json!({ "sql": statement }).to_string();
    request(addr, "POST", "/sql", Some(&body))
}

fn ok_sql(addr: &str, statement: &str) -> String {
    let (status, body) = sql(addr, statement);
    assert_eq!(status, 200, "{statement}: {body}");
    body
}

fn post(addr: &str, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let (status, body) = request(addr, "POST", path, Some(&body.to_string()));
    (
        status,
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("JSON from {path}: {body}")),
    )
}

/// A throwaway directory under the system temp dir, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_quacklab_lake_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn env(pairs: &[(&str, String)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    move |name: &str| map.get(name).cloned()
}

/// One adapter over its own in-memory store and the lake `vars` describe.
struct Adapter {
    handle: QuacklabHandle,
    addr: String,
}

fn boot(vars: &[(&str, String)]) -> Result<Adapter, String> {
    pink_lake::database::register();
    let lookup = env(vars);
    let config = LakeConfig::from_lookup(&lookup)
        .map_err(|e| e.to_string())?
        .expect("GRAIN_DUCKLAKE is set");
    let options = QuacklabOptions {
        sql_token: TOKEN.to_string(),
        read_only: config.read_only,
        warehouse: config.alias.clone(),
        lease_ttl_floor: Duration::from_millis(100),
        ..QuacklabOptions::default()
    };
    let db = Database::new();
    let warehouse = LakeWarehouse::boot(db.clone(), config).map_err(|e| e.to_string())?;
    let server =
        QuacklabServer::bind_with_warehouse("127.0.0.1:0", db, options, Arc::new(warehouse))
            .map_err(|e| e.to_string())?;
    let addr = server.local_addr().to_string();
    let handle = server.spawn().map_err(|e| e.to_string())?;
    Ok(Adapter { handle, addr })
}

impl Drop for Adapter {
    fn drop(&mut self) {
        self.handle.shutdown();
        let _ = self.handle.join();
    }
}

/// The outside writer a lease makes room for: another store attaching the
/// same lake writable (it takes the lake's writer lease), writing, and
/// detaching.
fn outside_write(setup: &[String], attach: &str, statements: &[&str]) {
    let db = Database::new();
    let mut conn = db.connect();
    for statement in setup {
        conn.execute(statement).expect("outside setup");
    }
    conn.execute(attach).expect("the outside writer attaches");
    for statement in statements {
        conn.execute(statement)
            .unwrap_or_else(|e| panic!("outside {statement}: {e}"));
    }
    conn.execute("DETACH lw")
        .expect("the outside writer detaches");
}

/// The whole lifecycle over one lake: `vars` boot the adapter, `outside` is
/// the attach statement (plus any secrets) an outside writer uses.
fn lifecycle(vars: Vec<(&str, String)>, outside_setup: Vec<String>, outside_attach: String) {
    let adapter = boot(&vars).expect("the lake warehouse boots");
    let addr = adapter.addr.clone();

    // Requests start in the lake.
    let body = ok_sql(&addr, "SELECT current_database() AS db");
    assert!(body.contains("\"rows\":[[\"warehouse\"]]"), "{body}");
    ok_sql(&addr, "CREATE SCHEMA marts");
    ok_sql(
        &addr,
        "CREATE TABLE marts.orders (id BIGINT, amount DECIMAL(10,2), note VARCHAR)",
    );
    let body = ok_sql(
        &addr,
        "INSERT INTO marts.orders VALUES (1, 9.50, 'a'), (2, 12.25, 'b')",
    );
    assert!(body.contains("\"rowsAffected\":2"), "{body}");
    let body = ok_sql(&addr, "SELECT id, amount FROM marts.orders ORDER BY id");
    assert!(
        body.contains(
            "\"rows\":[[1,{\"Width\":10,\"Scale\":2,\"Value\":950}],\
             [2,{\"Width\":10,\"Scale\":2,\"Value\":1225}]]"
        ),
        "{body}"
    );
    let body = ok_sql(&addr, "SHOW DATABASES");
    assert!(body.contains("warehouse"), "{body}");

    // A lease detaches the lake: the outside writer can take it.
    let (status, grant) = post(
        &addr,
        "/write-lease/acquire",
        serde_json::json!({"owner": "dbt", "ttlSeconds": 600}),
    );
    assert_eq!(status, 200, "{grant}");
    let lease = grant["leaseId"].as_str().unwrap().to_string();
    let (status, body) = sql(&addr, "SELECT 1");
    assert_eq!(status, 423, "{body}");
    outside_write(
        &outside_setup,
        &outside_attach,
        &[
            "INSERT INTO lw.marts.orders VALUES (3, 1.00, 'outside')",
            "CREATE TABLE lw.marts.dims (k VARCHAR)",
        ],
    );
    let (status, released) = post(
        &addr,
        "/write-lease/release",
        serde_json::json!({ "leaseId": lease }),
    );
    assert_eq!(status, 200, "{released}");

    // Release re-attached at the new head.
    let body = ok_sql(&addr, "SELECT count(*) AS n FROM marts.orders");
    assert!(body.contains("\"rows\":[[3]]"), "{body}");
    ok_sql(&addr, "SELECT * FROM marts.dims");

    // The holder's own SQL reattaches lazily, like Go's admitWriteLease.
    let (_, grant) = post(
        &addr,
        "/write-lease/acquire",
        serde_json::json!({"owner": "etl", "ttlSeconds": 600}),
    );
    let lease = grant["leaseId"].as_str().unwrap().to_string();
    let (status, body) = post(
        &addr,
        "/write-lease/sql",
        serde_json::json!({"leaseId": lease, "sql": "UPDATE marts.orders SET note = 'held' WHERE id = 1"}),
    );
    assert_eq!(status, 200, "{body}");
    let (status, _) = post(
        &addr,
        "/write-lease/release",
        serde_json::json!({ "leaseId": lease }),
    );
    assert_eq!(status, 200);
    let body = ok_sql(&addr, "SELECT note FROM marts.orders WHERE id = 1");
    assert!(body.contains("\"rows\":[[\"held\"]]"), "{body}");

    // /recover is detach + attach; the data is still there.
    let (status, body) = request(&addr, "POST", "/recover", Some(""));
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"ok\":true"), "{body}");
    let body = ok_sql(&addr, "SELECT count(*) AS n FROM marts.orders");
    assert!(body.contains("\"rows\":[[3]]"), "{body}");
    let (status, body) = request(&addr, "GET", "/health", None);
    assert_eq!((status, body.as_str()), (200, "{\"ok\":true}"));

    // A second writer is refused while this one serves the lake…
    let second = boot(&vars);
    match second {
        Ok(second) => {
            let (status, body) = sql(&second.addr, "SELECT 1");
            assert_eq!(status, 503, "{body}");
            assert!(body.contains("conn_error"), "{body}");
            assert!(body.contains("already served by"), "{body}");
            drop(adapter);
            // …and takes over once it is gone, on its next request.
            let body = ok_sql(&second.addr, "SELECT count(*) AS n FROM marts.orders");
            assert!(body.contains("\"rows\":[[3]]"), "{body}");
            drop(second);
        }
        Err(message) => panic!("a held lake must not fail the boot: {message}"),
    }

    // The read-only service reads the lake and refuses writes.
    let mut ro_vars = vars.clone();
    ro_vars.push(("DUCKDB_READ_ONLY", "1".to_string()));
    let ro = boot(&ro_vars).expect("the read-only warehouse boots");
    let body = ok_sql(&ro.addr, "SELECT count(*) AS n FROM marts.orders");
    assert!(body.contains("\"rows\":[[3]]"), "{body}");
    let (status, body) = sql(&ro.addr, "INSERT INTO marts.orders VALUES (9, 0, 'x')");
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("Invalid Input Error"), "{body}");
}

#[test]
fn a_local_lake_round_trip() {
    let scratch = Scratch::new("local");
    let catalog = format!("pink:{}", scratch.0.join("catalog.pink").display());
    let data = scratch.0.join("data").display().to_string();
    let vars = vec![
        ("GRAIN_DUCKLAKE", "1".to_string()),
        ("PINKDB_LAKE_CATALOG", catalog.clone()),
        ("DUCKLAKE_DATA_PATH", data.clone()),
    ];
    let attach = format!("ATTACH 'lake:{catalog}' AS lw (DATA '{data}/')");
    lifecycle(vars, Vec::new(), attach);
}

#[test]
fn a_bad_catalog_fails_the_boot_naming_the_step_and_no_secret() {
    let err = boot(&[
        ("GRAIN_DUCKLAKE", "1".to_string()),
        (
            "DUCKLAKE_PG_URI",
            "host=127.0.0.1 port=1 dbname=nope user=u password=hunter2".to_string(),
        ),
        ("DUCKLAKE_DATA_PATH", "/nonexistent/lake".to_string()),
    ])
    .err()
    .expect("an unreachable catalog fails the boot");
    assert!(err.contains("at the attach step"), "{err}");
    assert!(!err.contains("hunter2"), "{err}");
}

#[test]
fn encrypted_lakes_are_refused_at_boot() {
    let err = boot(&[
        ("GRAIN_DUCKLAKE", "1".to_string()),
        ("DUCKLAKE_ENCRYPTED", "1".to_string()),
    ])
    .err()
    .expect("DUCKLAKE_ENCRYPTED is refused");
    assert!(err.contains("DUCKLAKE_ENCRYPTED=0"), "{err}");
}

#[cfg(all(feature = "lake-s3", feature = "catalog-postgres"))]
#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

/// QuackLab's own shape: a Postgres catalog (`DUCKLAKE_PG_URI` as a libpq
/// conninfo with a password) and data in S3 behind the R2 variables.
#[cfg(all(feature = "lake-s3", feature = "catalog-postgres"))]
#[test]
fn a_postgres_and_s3_lake_round_trip() {
    let Ok(url) = std::env::var("PINKDB_TEST_POSTGRES_URL") else {
        eprintln!("skipped: set PINKDB_TEST_POSTGRES_URL to run the Postgres lake test");
        return;
    };
    let schema = format!("quacklab_{}", std::process::id());
    let conninfo = postgres_conninfo(&url, &schema);
    let mock = mock_s3::MockS3::start();
    let data = mock.url("lake", "env1");
    let vars = vec![
        ("GRAIN_DUCKLAKE", "1".to_string()),
        ("DUCKLAKE_PG_URI", conninfo.clone()),
        ("DUCKLAKE_DATA_PATH", data.clone()),
        ("R2_ACCESS_KEY_ID", mock.access_key_id()),
        ("R2_SECRET_ACCESS_KEY", mock.secret_access_key()),
        ("R2_ENDPOINT", mock.endpoint()),
    ];
    let (location, password) =
        pinkdb::lake_db::split_catalog_password(&format!("postgres:{conninfo}"));
    let mut setup = vec![format!(
        "CREATE SECRET outside_s3 (TYPE s3, KEY_ID '{}', SECRET '{}', ENDPOINT '{}', \
         URL_STYLE 'path', REGION 'auto', USE_SSL false)",
        mock.access_key_id(),
        mock.secret_access_key(),
        mock.endpoint().trim_start_matches("http://")
    )];
    if let Some(password) = password {
        setup.push(format!(
            "CREATE SECRET outside_pg (TYPE postgres, PASSWORD '{password}', SCOPE '{location}')"
        ));
    }
    let attach = format!("ATTACH 'lake:{location}' AS lw (DATA '{data}/')");
    lifecycle(vars, setup, attach);
    assert!(
        mock.verified_requests() > 0,
        "the lake's data went through the mock S3"
    );
}

/// `postgres://user:pw@host:port/db` as a libpq conninfo whose catalog lives
/// in its own schema (`options=-csearch_path=…` is what libpq would take;
/// pink-lake reads `schema=` from a URL), so parallel runs don't collide.
#[cfg(all(feature = "lake-s3", feature = "catalog-postgres"))]
fn postgres_conninfo(url: &str, schema: &str) -> String {
    let rest = url
        .trim_start_matches("postgresql://")
        .trim_start_matches("postgres://");
    let (authority, tail) = rest.split_once('/').unwrap_or((rest, "postgres"));
    let dbname = tail.split('?').next().unwrap_or("postgres");
    let (userinfo, hostport) = authority.rsplit_once('@').unwrap_or(("", authority));
    let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let (host, port) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let mut conninfo = format!("host={host} port={port} dbname={dbname}");
    if !user.is_empty() {
        conninfo.push_str(&format!(" user={user}"));
    }
    if !password.is_empty() {
        conninfo.push_str(&format!(" password={password}"));
    }
    conninfo.push_str(&format!(" schema={schema}"));
    conninfo
}
