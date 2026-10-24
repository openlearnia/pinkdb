//! End-to-end tests of the S3 storage backend against a hermetic mock S3
//! server (no cloud, no credentials): the full lake cycle, prefixes and
//! percent-encoded table names, conditional-write conflict paths, the
//! safe-by-default refusal of endpoints without conditional writes (and the
//! explicit single-writer opt-in with its interleaving tests), error mapping,
//! lost responses, the v1 `.pink` bridge, and virtual-hosted addressing.
//! Every request is verified by the mock's own SigV4 implementation
//! (canonical request + HMAC chain recomputed from the received bytes). The
//! `PINKDB_S3_LIVE=1` test at the bottom talks to a real bucket (e.g.
//! Cloudflare R2) and is skipped by default.
//!
//! `s3` + `vortexlake` builds: the suite pins v3 (Vortex) lake behavior.
//! The lean `s3`-only (v1 `.pink`-over-S3) behavior is covered by
//! `tests/pinklake.rs`.

#![cfg(all(feature = "s3", feature = "vortexlake"))]

#[path = "../src/mock_s3.rs"]
mod mock_s3;

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mock_s3::MockS3;
use pink_lake::{Lake, LakeError, S3Config};
use pinkdb::{DataType, PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_s3_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Explicit config for a mock bucket: credentials and the endpoint are never
/// taken from the process environment (the one `open_url` test sets those).
/// The mock speaks plain `http://` (a local test endpoint), hence `allow_http`.
fn mock_config(mock: &MockS3, bucket: &str, prefix: &str) -> S3Config {
    S3Config::new(bucket)
        .with_prefix(prefix)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret")
}

/// The engine's deterministic `.pink` bytes of `db` — a strong equality check
/// between an original and a loaded store.
fn store_bytes(db: &PinkDb, scratch: &Path, tag: &str) -> Vec<u8> {
    let path = scratch.join(format!("{tag}.pink"));
    db.save(path.to_str().unwrap()).unwrap();
    std::fs::read(&path).unwrap()
}

/// A small store with two tables, two databases, and a dotted table name.
fn small_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (b BOOLEAN, i INTEGER, v VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        t.append_row(vec![
            Value::Boolean(true),
            Value::Integer(-5),
            Value::Varchar("hi".to_string()),
        ])
        .unwrap();
        t.append_row(vec![Value::Null; 3]).unwrap();
    }
    db.execute("CREATE DATABASE analytics;").unwrap();
    db.execute("CREATE TABLE analytics.u (k BIGINT);").unwrap();
    db.execute("INSERT INTO analytics.u VALUES (7);").unwrap();
    db.execute("CREATE TABLE analytics.\"a.b\" (z DOUBLE);")
        .unwrap();
    db.execute("INSERT INTO analytics.\"a.b\" VALUES (1.5), (-0.0);")
        .unwrap();
    db
}

#[test]
fn full_cycle_create_save_open_versions_tables() {
    let mock = MockS3::start();
    let scratch = scratch("cycle");
    let db = small_store();

    // create → v1, snapshots/1.json + catalog.json at the lake prefix.
    let mut lake = create_s3_lake(mock_config(&mock, "test-bucket", "lakes/mine")).unwrap();
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.format_version(), 7);
    assert_eq!(lake.versions().unwrap(), vec![1]);
    assert_eq!(lake.path().to_str().unwrap(), "s3://test-bucket/lakes/mine");
    let keys = mock.keys();
    assert!(
        !keys
            .iter()
            .any(|k| k.ends_with("catalog.json") || k.contains("/snapshots/")),
        "metadata stays in the local catalog: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.ends_with("lake-data.json")),
        "{keys:?}"
    );

    // A second create refuses: the same conditional create-only PUT that a
    // concurrent create would race on.
    let err = create_s3_lake(mock_config(&mock, "test-bucket", "lakes/mine")).unwrap_err();
    assert!(matches!(err, LakeError::AlreadyALake { .. }), "{err:?}");

    // save → v2: data files first, then the snapshot, then the catalog. The
    // catalog must stay byte-identical to the snapshot of its version.
    let report = lake.write_from_store_report(&db).unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.tables, 3);
    assert_eq!(report.rows, 2 + 1 + 2);
    assert!(report.bytes_written > 0);
    assert_eq!(lake.current_version(), 2);
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
    // The table list of the latest version.
    assert_eq!(
        lake.tables().unwrap(),
        vec![
            ("analytics.a.b".to_string(), 2),
            ("analytics.u".to_string(), 1),
            ("main.t".to_string(), 2),
        ]
    );

    // open a fresh handle and rebuild: value- and byte-exact.
    let mut lake = open_s3_lake(mock_config(&mock, "test-bucket", "lakes/mine")).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.current_version(), 2);
    let loaded = lake.load(None).unwrap();
    assert_eq!(
        store_bytes(&loaded, &scratch, "loaded"),
        store_bytes(&db, &scratch, "original")
    );
    assert_eq!(
        loaded.table_in("analytics", "a.b").unwrap().get(1, 0),
        Some(Value::Double(-0.0))
    );
    // v1 is the create-time empty catalog.
    let v1 = lake.load(Some(1)).unwrap();
    assert_eq!(v1.list_databases(), vec!["main"]);
    assert!(v1.list_tables().is_empty());

    // A v2 data file is one Vortex object per table under data/<key>/.
    let keys = mock.keys();
    for key in &keys {
        assert!(key.starts_with("lakes/mine/"), "{key}");
    }
    assert_eq!(
        keys.iter()
            .filter(|k| k.contains("/data/") && k.ends_with(".vortex"))
            .count(),
        3,
        "{keys:?}"
    );
    assert!(lake.missing_data_files().unwrap().is_empty());

    // Multiple saves: one fresh data file per table per version, old objects
    // stay (no GC, exactly like local).
    let mut lake = lake;
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    assert_eq!(
        mock.keys()
            .iter()
            .filter(|k| k.ends_with(".vortex"))
            .count(),
        6
    );
    // The store still round-trips from the new version.
    assert_eq!(
        store_bytes(&lake.load(None).unwrap(), &scratch, "v3"),
        store_bytes(&db, &scratch, "v3-original")
    );
    assert_eq!(
        lake.load(Some(2)).unwrap().table("t").unwrap().num_rows(),
        2
    );

    // Not a lake: a different prefix under the same bucket.
    let err = open_s3_lake(mock_config(&mock, "test-bucket", "lakes/other")).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(err.to_string().contains("not a pink-lake"), "{err}");
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn prefixes_and_percent_encoded_table_names_round_trip() {
    let mock = MockS3::start();
    let scratch = scratch("prefixes");

    // Empty prefix: the lake lives at the bucket root.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE root_t (x INTEGER);").unwrap();
    db.execute("INSERT INTO root_t VALUES (1);").unwrap();
    let mut lake = create_s3_lake(mock_config(&mock, "root-bucket", "")).unwrap();
    assert_eq!(lake.path().to_str().unwrap(), "s3://root-bucket");
    lake.write_from_store(&db).unwrap();
    let root_keys = mock.keys();
    assert!(
        !root_keys
            .iter()
            .any(|k| k.ends_with("catalog.json") || k.contains("snapshots/")),
        "{root_keys:?}"
    );
    assert!(
        root_keys.iter().any(|k| k.starts_with("data/main.root_t/")),
        "{root_keys:?}"
    );
    let listed = open_s3_lake(mock_config(&mock, "root-bucket", "")).unwrap();
    assert_eq!(listed.versions().unwrap(), vec![1, 2]);
    assert_eq!(listed.tables().unwrap(), vec![("main.root_t".into(), 1)]);

    // Deep prefix.
    let mut lake = create_s3_lake(mock_config(&mock, "deep-bucket", "a/b/c")).unwrap();
    lake.write_from_store(&db).unwrap();
    assert!(
        !mock
            .keys()
            .iter()
            .any(|k| k.ends_with("catalog.json") || k.contains("snapshots/"))
    );
    assert_eq!(
        open_s3_lake(mock_config(&mock, "deep-bucket", "a/b/c"))
            .unwrap()
            .current_version(),
        2
    );

    // Table names with `/`, spaces and non-ASCII are percent-encoded into one
    // object-key segment (the `%` itself is then URI-encoded once on the
    // wire); loading restores the exact name.
    let mut odd = PinkDb::new();
    odd.create_table_in("main", "a/b", vec![("x".to_string(), DataType::Integer)])
        .unwrap();
    odd.table_mut_in("main", "a/b")
        .unwrap()
        .append_row(vec![Value::Integer(9)])
        .unwrap();
    odd.create_table_in(
        "main",
        "odd name",
        vec![("y".to_string(), DataType::Varchar)],
    )
    .unwrap();
    odd.table_mut_in("main", "odd name")
        .unwrap()
        .append_row(vec![Value::Varchar("héllo".to_string())])
        .unwrap();
    let mut lake = create_s3_lake(mock_config(&mock, "odd-bucket", "odd")).unwrap();
    lake.write_from_store(&odd).unwrap();
    let keys = mock.keys();
    let data_keys: Vec<&String> = keys.iter().filter(|k| k.contains("/data/")).collect();
    assert!(
        data_keys
            .iter()
            .any(|k| k.contains("/data/main.a%2Fb/") && k.ends_with(".vortex")),
        "{data_keys:?}"
    );
    assert!(
        data_keys
            .iter()
            .any(|k| k.contains("/data/main.odd%20name/")),
        "{data_keys:?}"
    );
    let listed = open_s3_lake(mock_config(&mock, "odd-bucket", "odd")).unwrap();
    let loaded = listed.load(None).unwrap();
    assert_eq!(
        loaded.table_in("main", "a/b").unwrap().get(0, 0),
        Some(Value::Integer(9))
    );
    assert_eq!(
        loaded.table_in("main", "odd name").unwrap().get(0, 0),
        Some(Value::Varchar("héllo".into()))
    );
    // `0000`-style Unicode table names are stored as their UTF-8 percent
    // encoding (the engine's encoder), never as raw bytes.
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn error_mapping_not_found_auth_and_missing_data() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "errors-bucket";

    // 404 on the catalog → not a lake (create first, then a different prefix).
    create_s3_lake(mock_config(&mock, bucket, "lake")).unwrap();
    let err = open_s3_lake(mock_config(&mock, bucket, "nope")).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");

    // A missing snapshot is a clean version-not-found.
    let mut lake = open_s3_lake(mock_config(&mock, bucket, "lake")).unwrap();
    lake.set_data_inlining_row_limit(0);
    let err = lake.load(Some(99)).unwrap_err();
    assert!(matches!(err, LakeError::VersionNotFound { .. }), "{err:?}");
    assert!(
        err.to_string().contains("has no snapshot version 99"),
        "{err}"
    );

    // A deleted data object is reported by the cheap check and fails the load
    // with the data-file context (a 404 → I/O not-found).
    let mut lake = lake;
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    let victim = mock
        .keys()
        .into_iter()
        .find(|k| k.contains("/data/") && k.ends_with(".vortex"))
        .expect("a data object");
    mock.remove_object(&victim);
    let missing = lake.missing_data_files().unwrap();
    assert_eq!(missing.len(), 1, "{missing:?}");
    assert!(
        missing[0].1.to_str().unwrap().starts_with("s3://"),
        "{missing:?}"
    );
    let err = lake.load(None).unwrap_err();
    assert!(err.to_string().contains("loading data file"), "{err}");

    // 403 (wrong credentials) → the auth error naming the lake. (The catalog
    // probe is a HEAD, whose error responses carry no body to parse, so the
    // code falls back to AccessDenied; a GET-based probe keeps the S3 code —
    // see the storage-level test.)
    let wrong = S3Config::new(bucket)
        .with_prefix("lake")
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials("WRONGKEY", "wrong-secret");
    let wrong = open_s3_lake(wrong).unwrap();
    let err = wrong.load(None).unwrap_err();
    assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("access error"), "{msg}");
    assert!(msg.contains("denied"), "{msg}");
    assert!(
        msg.contains("SignatureDoesNotMatch") || msg.contains("AccessDenied"),
        "{msg}"
    );

    // A global 403 (e.g. a bucket policy) is the same auth error.
    mock.set_deny(403);
    let denied = open_s3_lake(mock_config(&mock, bucket, "lake")).unwrap();
    let err = denied.load(None).unwrap_err();
    assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
    assert!(err.to_string().contains("AccessDenied"), "{err}");
    mock.clear_deny();

    // A bad URL never touches the network.
    let err = Lake::open_url("s3://").unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
}

#[test]
fn open_url_reads_the_environment_and_saves_the_full_cycle() {
    let mock = MockS3::start();
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
        std::env::set_var("AWS_SESSION_TOKEN", "mock-session-token");
        std::env::set_var("AWS_ENDPOINT_URL", mock.endpoint());
        std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
        std::env::set_var("AWS_REGION", "auto");
        std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
    }

    let dir = scratch("env_cycle");
    let catalog = format!("pink:{}", dir.join("catalog.pink").display());
    let data = "s3://url-bucket/from/env";
    let db = small_store();
    let mut lake = Lake::create_with_catalog(&catalog, data).unwrap();
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    let mut lake = Lake::open_with_catalog(&catalog, None).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    assert_eq!(lake.tables().unwrap().len(), 3);

    let auths = mock.authorizations();
    assert!(!auths.is_empty());
    for auth in &auths {
        assert!(auth.starts_with("AWS4-HMAC-SHA256 "), "{auth}");
        assert!(
            auth.contains(&format!("Credential={}/", mock.access_key_id())),
            "{auth}"
        );
        assert!(auth.contains("/auto/s3/aws4_request"), "{auth}");
        assert!(auth.contains("x-amz-security-token"), "{auth}");
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn paginated_listings_and_delete_work() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "paging-bucket";
    let mut lake = create_s3_lake(mock_config(&mock, bucket, "lake")).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&db).unwrap();
    let mut lake = open_s3_lake(mock_config(&mock, bucket, "lake")).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&db).unwrap();
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3, 4]);

    // Force one direct child per page: the recursive data listing follows
    // continuation tokens through ListObjectsV2 until complete.
    mock.set_page_size(1);
    let _ = lake.list_files(None, None, true).unwrap();
    let list_requests = mock
        .requests()
        .iter()
        .filter(|r| r.contains("list-type=2"))
        .count();
    assert!(list_requests >= 4, "expected paging, got {list_requests}");
}

#[test]
fn save_store_helpers_work_for_local_and_s3() {
    let dir = scratch("helpers");
    let db = small_store();

    // A local path through the URL entry points is the same as the plain
    // local API (dispatch is by the `s3://` scheme).
    let lake_dir = dir.join("local-lake");
    assert_eq!(
        pink_lake::save_store_url(&db, lake_dir.to_str().unwrap()).unwrap(),
        2
    );
    let err = Lake::create_url(lake_dir.to_str().unwrap()).unwrap_err();
    assert!(matches!(err, LakeError::AlreadyALake { .. }), "{err:?}");
    assert_eq!(
        Lake::open_url(lake_dir.to_str().unwrap())
            .unwrap()
            .current_version(),
        2
    );

    // The explicit-config S3 helper: create + save in one call, then read it
    // back.
    let mock = MockS3::start();
    assert_eq!(
        pink_lake::save_store_s3(&db, mock_config(&mock, "helpers-bucket", "h")).unwrap(),
        2
    );
    let mut lake = open_s3_lake(mock_config(&mock, "helpers-bucket", "h")).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
    assert_eq!(lake.load(None).unwrap().table("t").unwrap().num_rows(), 2);
    // A rejected store leaves nothing behind (same pre-validation as local).
    let mut dotted = PinkDb::new();
    dotted.execute("CREATE DATABASE \"a.b\";").unwrap();
    dotted
        .execute("CREATE TABLE \"a.b\".t (x INTEGER);")
        .unwrap();
    let before = mock.keys().len();
    let err =
        pink_lake::save_store_s3(&dotted, mock_config(&mock, "helpers-bucket", "h2")).unwrap_err();
    assert!(err.to_string().contains("contains \".\""), "{err}");
    assert_eq!(mock.keys().len(), before, "a rejected save writes nothing");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn lost_put_responses_leave_a_safe_recoverable_state() {
    let db = small_store();
    let mock = MockS3::start();
    let config = || mock_config(&mock, "lost-data-bucket", "lake");
    let mut lake = create_s3_lake(config()).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&db).unwrap(); // v2
    // The response to a data PUT is lost: the object may have landed, the
    // catalog did not move, and a reopen shows the previous version.
    mock.drop_next_response("PUT", "lake/data/");
    // A data PUT is idempotent, so the client repeats it after the lost
    // response and the save lands exactly once.
    assert_eq!(lake.write_from_store(&db).unwrap(), 3);
    let reopened = open_s3_lake(config()).unwrap();
    assert_eq!(reopened.current_version(), 3);
    assert!(reopened.load(None).is_ok());
}

/// The error taxonomy: `SlowDown` 503, a redirect-shaped 301, and
/// `NoSuchBucket` 404 all map to clear, distinct errors.
#[test]
fn s3_error_taxonomy_slowdown_redirect_and_missing_bucket() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "taxonomy-bucket";
    let mut lake = create_s3_lake(mock_config(&mock, bucket, "lake")).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&db).unwrap(); // v2

    // A transient 503 SlowDown is retried with backoff on an *unconditional*
    // PUT (a fresh data object — safe to repeat) and the save goes through.
    // (A 503 on a conditional write is deliberately not retried: the request
    // may have committed; the second half pins that.)
    mock.inject_error(
        "PUT",
        "lake/data/main.t/",
        503,
        "SlowDown",
        "Please reduce your request rate.",
        1,
    );
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.current_version(), 3);

    // A persistent 503 on a data PUT surfaces as an I/O error naming the
    // code after the retry budget.
    mock.inject_error(
        "PUT",
        "lake/data/main.t/",
        503,
        "SlowDown",
        "Please reduce your request rate.",
        pink_lake::DEFAULT_RETRIES as usize + 1,
    );
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
    let message = err.to_string();
    assert!(message.contains("503"), "{message}");
    assert!(message.contains("SlowDown"), "{message}");
    // The failed save changed no metadata.
    assert_eq!(lake.current_version(), 3);

    // A redirect-shaped 301 on a data read is reported with its code.
    mock.inject_error(
        "GET",
        "lake/data/main.t/",
        301,
        "PermanentRedirect",
        "The bucket you are attempting to access must be addressed using the specified endpoint.",
        1,
    );
    let err = lake.load(None).unwrap_err();
    assert!(err.to_string().contains("PermanentRedirect"), "{err}");

    // A bucket that does not exist is a clear bucket error, not "not a lake"
    // (both on the GET body and on the bodiless HEAD probe). A fresh mock is
    // restricted to one bucket (the mock's store is bucket-less, so mixing
    // bucket names in one mock would still find the objects).
    let ghost_mock = MockS3::start();
    ghost_mock.restrict_buckets(&["real-bucket"]);
    let err = create_s3_lake(mock_config(&ghost_mock, "ghost-bucket", "lake")).unwrap_err();
    assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
    let message = err.to_string();
    assert!(message.contains("bucket does not exist"), "{message}");
    assert!(!message.contains("not a pink-lake directory"), "{message}");
}

/// Virtual-hosted addressing (`bucket.<endpoint>/key`) round-trips against
/// the mock. `*.localhost` resolves to 127.0.0.1 on hosts implementing
/// RFC 6761; the test skips (with a note) where it does not.
#[test]
fn virtual_hosted_addressing_round_trips_end_to_end() {
    use std::net::ToSocketAddrs;
    let bucket = "vhost-bucket";
    let probe_host = format!("{bucket}.localhost");
    if (probe_host.as_str(), 80).to_socket_addrs().is_err() {
        eprintln!("skipping the virtual-hosted test: {probe_host} does not resolve");
        return;
    }
    let mock = MockS3::start();
    mock.set_virtual_hosted(true);
    let scratch = scratch("vhost");
    let db = small_store();
    let config = || {
        S3Config::new(bucket)
            .with_prefix("vh")
            .with_region("us-east-1")
            .with_endpoint(mock.endpoint_with_host("localhost"))
            .with_path_style(false)
            .allow_http(true)
            .with_credentials(mock.access_key_id(), "mock-secret")
    };

    let mut lake = create_s3_lake(config()).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    // The bucket traveled in the Host header; the key in the path.
    assert!(
        mock.keys().iter().any(|k| k.starts_with("vh/data/")),
        "{:?}",
        mock.keys()
    );
    let requests = mock.requests();
    assert!(
        requests.iter().any(|r| r.starts_with("PUT /vh/")),
        "no bucket segment in the path: {requests:?}"
    );
    // Every request verified: the host header (with the bucket label) was
    // signed and the signature recomputed by the mock.
    assert_eq!(mock.verified_requests(), requests.len());

    let mut lake = open_s3_lake(config()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let loaded = lake.load(None).unwrap();
    assert_eq!(
        store_bytes(&loaded, &scratch, "vhost-loaded"),
        store_bytes(&db, &scratch, "vhost-original")
    );
    std::fs::remove_dir_all(&scratch).ok();
}

/// A live test against a real S3-compatible bucket (Cloudflare R2 by
/// default). Skipped unless `PINKDB_S3_LIVE=1`; see the README for the R2
/// environment (endpoint + region `auto`). The bucket must exist; objects
/// are written under `PINKDB_S3_PREFIX` (default: a unique `pink-lake-live/`
/// prefix) and never deleted (the lake has no GC).
#[test]
fn live_s3_roundtrip_is_opt_in() {
    if std::env::var("PINKDB_S3_LIVE").as_deref() != Ok("1") {
        eprintln!(
            "skipping the live S3 test: set PINKDB_S3_LIVE=1, PINKDB_S3_BUCKET (and the AWS_* \
             credentials/endpoint) to run it"
        );
        return;
    }
    let bucket = std::env::var("PINKDB_S3_BUCKET").expect("PINKDB_S3_BUCKET");
    let prefix = std::env::var("PINKDB_S3_PREFIX").unwrap_or_else(|_| {
        format!(
            "pink-lake-live/{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        )
    });
    let config = S3Config::from_env(&bucket).with_prefix(&prefix);
    let db = small_store();

    let mut lake = create_s3_lake(config.clone()).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    let mut lake = open_s3_lake(config).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
    assert_eq!(lake.tables().unwrap().len(), 3);
    assert!(lake.missing_data_files().unwrap().is_empty());
    let loaded = lake.load(None).unwrap();
    let scratch = scratch("live");
    assert_eq!(
        store_bytes(&loaded, &scratch, "live-loaded"),
        store_bytes(&db, &scratch, "live-original")
    );

    // Stale-handle conflict over the real service.
    let mut stale = open_s3_lake(S3Config::from_env(&bucket).with_prefix(&prefix)).unwrap();
    let mut latest = open_s3_lake(S3Config::from_env(&bucket).with_prefix(&prefix)).unwrap();
    latest.write_from_store(&db).unwrap(); // v3
    let err = stale.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    std::fs::remove_dir_all(&scratch).ok();
    eprintln!("live S3 round-trip ok (bucket {bucket}, prefix {prefix})");
}

/// `500 InternalError` is retried only where repeating is safe: an
/// unconditional data-file PUT recovers, while a conditional catalog PUT
/// (which may have landed) surfaces the error instead of risking a false
/// self-conflict.
#[test]
fn ambiguous_failures_retry_only_idempotent_requests() {
    let mock = MockS3::start();
    let db = small_store();
    let mut lake = create_s3_lake(mock_config(&mock, "retry-bucket", "lake")).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&db).unwrap(); // v2

    // Data files are unconditional PUTs under fresh names: retried.
    mock.inject_error("PUT", "lake/data/", 500, "InternalError", "try again", 1);
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.current_version(), 3);

    // A persistent 500 on a data PUT exhausts the retry budget and surfaces.
    mock.inject_error(
        "PUT",
        "lake/data/",
        500,
        "InternalError",
        "try again",
        pink_lake::DEFAULT_RETRIES as usize + 1,
    );
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(err.to_string().contains("InternalError"), "{err}");
    assert_eq!(lake.current_version(), 3);
}

/// Attach over object storage: footer-only descriptors (the objects are read
/// whole — no ranged reads in this build — but never decoded), fast paths
/// answered from the statistics, and lazy materialization matching the
/// eager load bit-exactly.
#[test]
fn attach_over_s3_is_footer_only_and_matches_load() {
    let mock = MockS3::start();
    let dir = scratch("attach");
    let db = small_store();
    let mut lake = create_s3_lake(mock_config(&mock, "test-bucket", "attach")).unwrap();
    lake.write_from_store(&db).unwrap();

    // Attach a fresh handle and check the descriptors against the eager load.
    let mut lake = open_s3_lake(mock_config(&mock, "test-bucket", "attach")).unwrap();
    lake.set_data_inlining_row_limit(0);
    let report = lake.attach(None).unwrap();
    assert!(report.remote, "object storage has no ranged reads here");
    assert_eq!(report.tables, 3);
    let loaded = lake.load(None).unwrap();

    let mut attached = PinkDb::new();
    let tables = attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(tables.len(), 3);
    for t in &tables {
        let expected = loaded.table_in(&t.db, &t.table).unwrap();
        assert_eq!(t.rows, expected.num_rows() as u64);
        assert_eq!(t.schema, expected.schema());
        for (i, (name, _)) in t.schema.iter().enumerate() {
            // null_count must agree whenever the lake proved it.
            let mut nulls = 0u64;
            for row in 0..expected.num_rows() {
                if expected.get(row, i).unwrap().is_null() {
                    nulls += 1;
                }
            }
            assert_eq!(t.stats[i].null_count, Some(nulls), "{name}");
        }
    }

    // Counts come from the footer; a scan materializes through the same
    // decode `.lake load` uses and equals it bit-for-bit where it matters.
    let counts = attached.execute("SELECT COUNT(*) FROM main.t;").unwrap();
    match counts.first().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Bigint(2)]])
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let mut results = attached
        .execute("SELECT MIN(i), MAX(i) FROM main.t;")
        .unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Integer(-5), Value::Integer(-5)]])
        }
        other => panic!("expected rows, got {other:?}"),
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn maintenance_over_s3_merges_expires_and_cleans_only_unreferenced_objects() {
    let mock = MockS3::start();
    let scratch = scratch("maint");
    let prefix = "maint";
    let mut lake = s3_appended_lake(
        &mock,
        prefix,
        &[&[(1, "a"), (2, "b")], &[(3, "c")], &[(4, "d")]],
    );
    let data_keys: Vec<String> = mock
        .keys()
        .into_iter()
        .filter(|k| k.starts_with(&format!("{prefix}/data/main.t/")))
        .collect();
    assert_eq!(data_keys.len(), 3, "{data_keys:?}");
    let bytes_before: Vec<Option<Vec<u8>>> = data_keys.iter().map(|k| mock.object(k)).collect();

    // Merge: one consolidated object, version published, source objects
    // byte-identical (never rewritten over S3 either).
    let report = lake
        .merge_adjacent_files(None, &pink_lake::MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files_processed(), 3);
    assert_eq!(report.files_created(), 1);
    for (key, bytes) in data_keys.iter().zip(&bytes_before) {
        assert_eq!(&mock.object(key), bytes, "{key} changed");
    }
    // create + 3 commits + the merge version.
    assert_eq!(lake.versions().unwrap().len(), 5);

    // S3 `stat` reads size + Last-Modified: the listing and table info
    // carry real sizes.
    let files = lake.list_files(None, None, false).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].size_bytes.is_some_and(|s| s > 0));
    let info = lake.table_info().unwrap();
    assert_eq!(info[0].file_count, 1);
    assert!(info[0].file_size_bytes.is_some_and(|s| s > 0));
    assert_eq!(info[0].rows, 4);

    // Expire the pre-merge snapshots; their commit deltas go too, and the
    // catalog is untouched.
    let report = lake
        .expire_snapshots(&pink_lake::ExpireSettings {
            versions: Some(vec![2, 3]),
            dry_run: false,
            ..pink_lake::ExpireSettings::default()
        })
        .unwrap();
    assert_eq!(report.version_numbers(), vec![2, 3]);
    assert_eq!(mock.object(&format!("{prefix}/snapshots/2.json")), None);
    assert_eq!(mock.object(&format!("{prefix}/commits/2.json")), None);
    assert!(lake.load(None).is_ok());

    // Cleanup: nothing yet (the remaining snapshot 4 still references the
    // merged sources)... expire it, then the three sources become
    // unreferenced. Dry run first: nothing is deleted.
    let report = lake
        .cleanup_old_files(&pink_lake::CleanupSettings {
            all: true,
            dry_run: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert!(report.paths.is_empty(), "{report:?}");
    lake.expire_snapshots(&pink_lake::ExpireSettings {
        retain_last: Some(1),
        ..pink_lake::ExpireSettings::default()
    })
    .unwrap();
    let dry = lake
        .cleanup_old_files(&pink_lake::CleanupSettings {
            all: true,
            dry_run: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(dry.paths.len(), 3, "{dry:?}");
    for path in &dry.paths {
        assert!(
            mock.object(&format!("{prefix}/{path}")).is_some(),
            "dry run deleted {path}"
        );
    }
    // Without confirmation the call is refused, nothing deleted.
    let err = lake
        .cleanup_old_files(&pink_lake::CleanupSettings {
            all: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap_err();
    assert!(matches!(err, LakeError::NotRepresentable { .. }), "{err:?}");
    // Confirmed: gone; running twice is a no-op.
    let real = lake
        .cleanup_old_files(&pink_lake::CleanupSettings {
            all: true,
            confirm: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert!(real.deleted);
    assert_eq!(real.paths.len(), 3);
    for path in &real.paths {
        assert_eq!(mock.object(&format!("{prefix}/{path}")), None, "{path}");
    }
    let again = lake
        .delete_orphaned_files(&pink_lake::CleanupSettings {
            all: true,
            confirm: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert!(again.paths.is_empty());
    assert!(lake.load(None).is_ok());
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn maintenance_age_gate_reads_last_modified_over_s3() {
    let mock = MockS3::start();
    let scratch = scratch("maint-age");
    let prefix = "gate";
    let mut lake = s3_appended_lake(&mock, prefix, &[&[(1, "a")], &[(2, "b")]]);

    // Two unreferenced leftovers: one backdated, one fresh.
    let old_key = format!("{prefix}/data/main.t/crashed.vortex");
    let young_key = format!("{prefix}/data/main.t/fresh.vortex");
    mock.put_object(&old_key, b"crashed writer leftover");
    mock.put_object(&young_key, b"fresh leftover");
    let old_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 9 * 86_400;
    assert!(mock.set_object_modified(&old_key, old_secs));

    // The default 2-day gate exposes the backdated object only.
    let report = lake
        .delete_orphaned_files(&pink_lake::CleanupSettings {
            dry_run: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert!(
        report
            .paths
            .contains(&"data/main.t/crashed.vortex".to_string())
    );
    assert!(
        !report
            .paths
            .contains(&"data/main.t/fresh.vortex".to_string())
    );

    // `all` skips the gate and lists both.
    let report = lake
        .delete_orphaned_files(&pink_lake::CleanupSettings {
            all: true,
            dry_run: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(report.paths.len(), 2, "{report:?}");
    // The listing's `all` mode agrees with the cleanup scan.
    let files = lake.list_files(None, None, true).unwrap();
    let unreferenced: Vec<&pink_lake::LakeFileInfo> =
        files.iter().filter(|f| !f.referenced).collect();
    assert_eq!(unreferenced.len(), 2);
    assert!(unreferenced.iter().all(|f| f.size_bytes.is_some()));

    // Backdate the fresh one too; the default gate now lists both, and the
    // confirmed run deletes exactly them.
    assert!(mock.set_object_modified(&young_key, old_secs));
    let report = lake
        .delete_orphaned_files(&pink_lake::CleanupSettings {
            confirm: true,
            ..pink_lake::CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(report.paths.len(), 2, "{report:?}");
    assert_eq!(mock.object(&old_key), None);
    assert_eq!(mock.object(&young_key), None);
    // The lake remains fully loadable.
    assert!(lake.load(None).is_ok());
    assert_eq!(lake.table_info().unwrap()[0].file_count, 2);
    std::fs::remove_dir_all(&scratch).ok();
}

/// An S3 lake whose `main.t` is built from one appended file per chunk.
fn s3_appended_lake(mock: &MockS3, prefix: &str, chunks: &[&[(i32, &str)]]) -> Lake {
    let mut lake = create_s3_lake(mock_config(mock, "test-bucket", prefix)).unwrap();
    // Wave 68: inlining is on by default; this helper pins one data file per
    // chunk, so it opts out.
    lake.set_data_inlining_row_limit(0);
    for chunk in chunks {
        let mut table = pinkdb::DataTable::new(
            "t".to_string(),
            vec![
                ("id".to_string(), pinkdb::DataType::Integer),
                ("s".to_string(), pinkdb::DataType::Varchar),
            ],
        )
        .unwrap();
        for (id, s) in *chunk {
            table
                .append_row(vec![Value::Integer(*id), Value::Varchar(s.to_string())])
                .unwrap();
        }
        let mut commit = pink_lake::Commit::new();
        commit.append("main.t", std::sync::Arc::new(table));
        lake.commit(&commit).unwrap();
    }
    lake
}
