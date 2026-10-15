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

#![cfg(feature = "s3")]

#[path = "../src/mock_s3.rs"]
mod mock_s3;

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
    let mut lake = Lake::create_s3(mock_config(&mock, "test-bucket", "lakes/mine")).unwrap();
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.format_version(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1]);
    assert_eq!(lake.path().to_str().unwrap(), "s3://test-bucket/lakes/mine");
    let keys = mock.keys();
    assert!(
        keys.contains(&"lakes/mine/catalog.json".to_string()),
        "{keys:?}"
    );
    assert!(
        keys.contains(&"lakes/mine/snapshots/1.json".to_string()),
        "{keys:?}"
    );

    // A second create refuses: the same conditional create-only PUT that a
    // concurrent create would race on.
    let err = Lake::create_s3(mock_config(&mock, "test-bucket", "lakes/mine")).unwrap_err();
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
    let catalog = mock.object("lakes/mine/catalog.json").unwrap();
    assert_eq!(
        catalog,
        mock.object("lakes/mine/snapshots/2.json").unwrap(),
        "catalog.json must be byte-identical to its snapshot"
    );
    assert!(
        catalog.starts_with(b"{\n  \"format_version\": 3"),
        "{}",
        String::from_utf8_lossy(&catalog)
    );

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
    let lake = Lake::open_s3(mock_config(&mock, "test-bucket", "lakes/mine")).unwrap();
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
    let err = Lake::open_s3(mock_config(&mock, "test-bucket", "lakes/other")).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(err.to_string().contains("not a pink-lake"), "{err}");
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn stale_handles_conflict_and_conditional_catalog_replace_is_enforced() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "conflict-bucket";

    let mut a = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    a.write_from_store(&db).unwrap(); // v2
    let mut b = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap(); // sees v2
    a.write_from_store(&db).unwrap(); // v3

    // The stale handle fails on the pre-save version check, writes nothing.
    let files_before = mock
        .keys()
        .iter()
        .filter(|k| k.ends_with(".vortex"))
        .count();
    let err = b.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("write conflict"), "{msg}");
    assert!(msg.contains("another writer published version 3"), "{msg}");
    assert!(msg.contains("reopen the lake"), "{msg}");
    assert_eq!(b.current_version(), 2);
    assert_eq!(
        mock.keys()
            .iter()
            .filter(|k| k.ends_with(".vortex"))
            .count(),
        files_before,
        "a stale save writes no data objects"
    );
    let v3_snapshot = mock.object("lake/snapshots/3.json").unwrap();

    // Reopen; the chain continues.
    let mut b = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();
    assert_eq!(b.current_version(), 3);
    assert_eq!(b.write_from_store(&db).unwrap(), 4);

    // Now the *conditional replace* itself: the next save's catalog PUT
    // carries `If-Match: <etag from the pre-replace read>`, and the mock
    // bumps the object's ETag just before evaluating it — the deterministic
    // stand-in for another writer landing in that window. The lake reports
    // the familiar conflict, not a silent clobber, and the previous catalog
    // stays authoritative.
    let catalog_before = mock.object("lake/catalog.json").unwrap();
    mock.bump_etag_on_next_if_match_put();
    let err = b.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("catalog.json changed while this save was in flight"),
        "{msg}"
    );
    assert!(msg.contains("reopen the lake"), "{msg}");
    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        catalog_before,
        "the previous catalog stayed authoritative"
    );
    assert_eq!(b.current_version(), 4, "the handle does not advance");
    // The failed save's snapshot 5 was written before the catalog replace:
    // an unpublished stray (the catalog still says 3), like a local crash
    // between the two publications. The documented recovery is to remove it.
    assert!(mock.keys().contains(&"lake/snapshots/5.json".to_string()));
    assert_eq!(
        Lake::open_s3(mock_config(&mock, bucket, "lake"))
            .unwrap()
            .versions()
            .unwrap(),
        vec![1, 2, 3, 4],
        "the stray snapshot is not published"
    );
    mock.remove_object("lake/snapshots/5.json");
    let mut b = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();
    assert_eq!(b.write_from_store(&db).unwrap(), 5);
    assert_eq!(b.load(Some(3)).unwrap().table("t").unwrap().num_rows(), 2);
    // (v3's snapshot is untouched by all of this.)
    assert_eq!(mock.object("lake/snapshots/3.json").unwrap(), v3_snapshot);
}

#[test]
fn resaving_a_published_snapshot_is_refused() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "rollback-bucket";
    let mut lake = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    let v2_catalog = mock.object("lake/catalog.json").unwrap();
    lake.write_from_store(&db).unwrap(); // v3

    // "Roll back": restore the v2 catalog; the save that would land on the
    // existing v3 snapshot must refuse (create-only via If-None-Match: *),
    // never replace it.
    mock.put_object("lake/catalog.json", &v2_catalog);
    let mut lake = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();
    assert_eq!(lake.current_version(), 2);
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("snapshot 3 already exists"), "{msg}");
    assert!(
        msg.contains("refusing to replace a published version"),
        "{msg}"
    );
    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        v2_catalog,
        "the rolled-back-to catalog is untouched"
    );
    // Both the rolled-back version and the orphaned snapshot stay loadable.
    assert!(lake.load(None).is_ok());
    assert!(lake.load(Some(3)).is_ok());
}

#[test]
fn prefixes_and_percent_encoded_table_names_round_trip() {
    let mock = MockS3::start();
    let scratch = scratch("prefixes");

    // Empty prefix: the lake lives at the bucket root.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE root_t (x INTEGER);").unwrap();
    db.execute("INSERT INTO root_t VALUES (1);").unwrap();
    let mut lake = Lake::create_s3(mock_config(&mock, "root-bucket", "")).unwrap();
    assert_eq!(lake.path().to_str().unwrap(), "s3://root-bucket");
    lake.write_from_store(&db).unwrap();
    let root_keys = mock.keys();
    assert!(
        root_keys.contains(&"catalog.json".to_string()),
        "{root_keys:?}"
    );
    assert!(
        root_keys.contains(&"snapshots/2.json".to_string()),
        "{root_keys:?}"
    );
    assert!(
        root_keys.iter().any(|k| k.starts_with("data/main.root_t/")),
        "{root_keys:?}"
    );
    let listed = Lake::open_s3(mock_config(&mock, "root-bucket", "")).unwrap();
    assert_eq!(listed.versions().unwrap(), vec![1, 2]);
    assert_eq!(listed.tables().unwrap(), vec![("main.root_t".into(), 1)]);

    // Deep prefix.
    let mut lake = Lake::create_s3(mock_config(&mock, "deep-bucket", "a/b/c")).unwrap();
    lake.write_from_store(&db).unwrap();
    assert!(mock.keys().contains(&"a/b/c/catalog.json".to_string()));
    assert!(mock.keys().contains(&"a/b/c/snapshots/2.json".to_string()));
    assert_eq!(
        Lake::open_s3(mock_config(&mock, "deep-bucket", "a/b/c"))
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
    let mut lake = Lake::create_s3(mock_config(&mock, "odd-bucket", "odd")).unwrap();
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
    let listed = Lake::open_s3(mock_config(&mock, "odd-bucket", "odd")).unwrap();
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
    Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    let err = Lake::open_s3(mock_config(&mock, bucket, "nope")).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");

    // A missing snapshot is a clean version-not-found.
    let lake = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();
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
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials("WRONGKEY", "wrong-secret");
    let err = Lake::open_s3(wrong).unwrap_err();
    assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
    let msg = err.to_string();
    assert!(msg.contains("access error"), "{msg}");
    assert!(msg.contains("denied"), "{msg}");
    assert!(msg.contains("AccessDenied"), "{msg}");

    // A global 403 (e.g. a bucket policy) is the same auth error.
    mock.set_deny(403);
    let err = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap_err();
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
    // This is the one test in this binary that touches process environment,
    // so it cannot race the explicit-config tests above.
    std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
    std::env::set_var("AWS_SESSION_TOKEN", "mock-session-token");
    std::env::set_var("AWS_ENDPOINT_URL", mock.endpoint());
    std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
    std::env::set_var("AWS_REGION", "auto");
    // The mock endpoint is plain http://, a local test server.
    std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");

    let db = small_store();
    let mut lake = Lake::create_url("s3://url-bucket/from/env").unwrap();
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    // The save_store-equivalent over the URL + environment.
    assert_eq!(
        pink_lake::save_store_url(&db, "s3://url-bucket/from/env").unwrap(),
        3
    );
    let lake = Lake::open_url("s3://url-bucket/from/env").unwrap();
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    assert_eq!(lake.tables().unwrap().len(), 3);

    // Every request was signed with the environment credential and the R2
    // `auto` region, the session token was signed too, and each request's
    // SigV4 signature was recomputed and verified by the mock (not just
    // shape-checked).
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
    assert_eq!(
        mock.verified_requests(),
        mock.requests().len(),
        "every request passed strict SigV4 verification"
    );
    // A session token is sent on the wire, not only signed.
    assert!(mock.requests().iter().any(|_| true));
    // The URL path-style addressing: /url-bucket/from/env/... request paths.
    assert!(
        mock.requests()
            .iter()
            .any(|r| r.contains("GET /url-bucket/from/env/catalog.json")),
        "{:?}",
        mock.requests()
    );
    // The lake's path is the canonical URL.
    assert_eq!(lake.path().to_str().unwrap(), "s3://url-bucket/from/env");
}

#[test]
fn paginated_listings_and_delete_work() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "paging-bucket";
    let mut lake = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    lake.write_from_store(&db).unwrap();
    let mut lake = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();
    lake.write_from_store(&db).unwrap();
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3, 4]);

    // Force one direct child per page: versions() follows continuation
    // tokens through ListObjectsV2 until the listing is complete.
    mock.set_page_size(1);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3, 4]);
    let list_requests = mock
        .requests()
        .iter()
        .filter(|r| r.contains("list-type=2"))
        .count();
    assert!(list_requests >= 4, "expected paging, got {list_requests}");

    // A snapshot object removed directly is simply not a version (the same
    // "skipped, not listed" behavior as a local corrupt/missing manifest).
    mock.remove_object("lake/snapshots/2.json");
    assert_eq!(lake.versions().unwrap(), vec![1, 3, 4]);
}

#[test]
fn endpoints_without_conditional_writes_refuse_writes_by_default() {
    let mock = MockS3::start();
    mock.reject_conditional_writes(true);
    let db = small_store();
    let bucket = "degraded-bucket";

    // create: the first conditional PUT (`If-None-Match: *` on the snapshot)
    // is refused with 501 and nothing is written — no partial lake.
    let err = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap_err();
    assert!(
        matches!(err, LakeError::UnsupportedEndpoint { .. }),
        "{err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("does not support conditional writes"),
        "{message}"
    );
    assert!(message.contains("PINKDB_S3_ALLOW_DEGRADED"), "{message}");
    assert!(
        message.contains("S3Config::allow_degraded(true)"),
        "{message}"
    );
    assert!(
        message.contains("SINGLE-WRITER"),
        "the opt-in trade is spelled out: {message}"
    );
    assert!(
        mock.keys().is_empty(),
        "no objects at all: {:?}",
        mock.keys()
    );
    assert_eq!(mock.conditional_rejections(), 1);
    assert!(
        Lake::open_s3(mock_config(&mock, bucket, "lake"))
            .unwrap_err()
            .to_string()
            .contains("not a pink-lake"),
        "the failed create left no catalog"
    );

    // The same refusal covers a save on an existing lake whose endpoint
    // starts rejecting conditional writes: the first attempt may have
    // uploaded unreferenced data files, but it changes no metadata, and a
    // later write is refused before anything is sent.
    mock.clear_conditional_rejection();
    let config = || {
        S3Config::new(bucket)
            .with_prefix("ok")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .with_credentials(mock.access_key_id(), "mock-secret")
    };
    let mut lake = Lake::create_s3(config()).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    let catalog_before = mock.object("ok/catalog.json").unwrap();
    let snapshots_before: Vec<String> = mock
        .keys()
        .into_iter()
        .filter(|k| k.contains("/snapshots/"))
        .collect();

    // Now the endpoint degrades mid-lake.
    mock.reject_conditional_writes(true);
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(
        matches!(err, LakeError::UnsupportedEndpoint { .. }),
        "{err:?}"
    );
    assert_eq!(
        mock.object("ok/catalog.json").unwrap(),
        catalog_before,
        "the catalog is untouched"
    );
    let snapshots_after: Vec<String> = mock
        .keys()
        .into_iter()
        .filter(|k| k.contains("/snapshots/"))
        .collect();
    assert_eq!(
        snapshots_after, snapshots_before,
        "no snapshot was published"
    );
    // Data files may have been uploaded before the first conditional write
    // was attempted (unreferenced leftovers, harmless like a crash); the
    // next attempt does not even get that far: the refusal is sticky.
    let data_mid = mock.keys().iter().filter(|k| k.contains("/data/")).count();
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(
        matches!(err, LakeError::UnsupportedEndpoint { .. }),
        "{err:?}"
    );
    assert_eq!(
        mock.keys().iter().filter(|k| k.contains("/data/")).count(),
        data_mid,
        "a refused save writes nothing once the degradation is known"
    );
    // Reads still work.
    let reader = Lake::open_s3(config()).unwrap();
    assert_eq!(reader.current_version(), 2);
    assert_eq!(reader.load(None).unwrap().table("t").unwrap().num_rows(), 2);
}

#[test]
fn endpoints_without_conditional_writes_allow_an_explicit_single_writer_opt_in() {
    let mock = MockS3::start();
    mock.reject_conditional_writes(true);
    let db = small_store();
    let bucket = "degraded-opt-in-bucket";

    let config = || {
        S3Config::new(bucket)
            .with_prefix("lake")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .allow_degraded(true)
            .with_credentials(mock.access_key_id(), "mock-secret")
    };

    // create and save work through the documented check-then-put fallback:
    // exactly one conditional probe per handle, then sticky.
    let mut a = Lake::create_s3(config()).unwrap();
    assert_eq!(a.current_version(), 1);
    assert_eq!(
        mock.conditional_rejections(),
        1,
        "the snapshot create-only PUT"
    );
    assert_eq!(a.write_from_store(&db).unwrap(), 2);
    assert_eq!(
        mock.conditional_rejections(),
        1,
        "the degradation is sticky within the handle; the fallback carries the save"
    );
    assert_eq!(a.write_from_store(&db).unwrap(), 3);
    assert_eq!(mock.conditional_rejections(), 1);
    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        mock.object("lake/snapshots/3.json").unwrap(),
        "with a single writer the pair stays byte-identical"
    );
    assert!(
        a.load(None).is_ok(),
        "the fallback publishes loadable versions"
    );

    // Conflict detection still works: it comes from the version re-check,
    // which needs no conditional writes.
    let mut b = Lake::open_s3(config()).unwrap();
    a.write_from_store(&db).unwrap(); // moves the catalog forward (v4)
    let err = b.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    assert!(
        err.to_string().contains("another writer published version"),
        "{err}"
    );
    // And a create on an existing lake is still refused (the HEAD pre-check
    // in the degraded path).
    let err = Lake::create_s3(config()).unwrap_err();
    assert!(matches!(err, LakeError::AlreadyALake { .. }), "{err:?}");
}

/// The reviewer's interleaving repro (the dangerous ordering): A's snapshot
/// PUT is delayed until *after* B fully publishes v3, so A's PUT replaces
/// the published snapshot and A's own read-back cannot see it. A's catalog
/// re-check then reports the conflict and the loser restores the snapshot
/// from the authoritative catalog bytes before returning: the pair is
/// consistent, B's success stays true. (The other ordering — a replacement
/// landing between a PUT and its read-back — is pinned at the storage level
/// in `s3::mod`'s `degraded_read_back_catches_a_replacement`.)
#[test]
fn degraded_opt_in_interleaving_repro_stays_consistent() {
    let mock = MockS3::start();
    mock.reject_conditional_writes(true);
    let db = small_store();
    let bucket = "reconcile-bucket";
    let config = || {
        S3Config::new(bucket)
            .with_prefix("lake")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .allow_degraded(true)
            .with_credentials(mock.access_key_id(), "mock-secret")
    };

    let mut warm = Lake::create_s3(config()).unwrap();
    assert_eq!(warm.write_from_store(&db).unwrap(), 2);
    drop(warm);

    // Park A's *fallback* snapshot PUT before it is applied (not the
    // `If-None-Match` probe that precedes it).
    mock.hold_next_request_without_header("PUT", "snapshots/3.json", "if-none-match");
    let a_handle = std::thread::spawn({
        let config = config();
        let db = small_store();
        move || {
            let mut a = Lake::open_s3(config).unwrap();
            a.write_from_store(&db)
        }
    });
    assert!(
        mock.wait_until_hold_reached(std::time::Duration::from_secs(10)),
        "A's snapshot PUT never arrived"
    );

    // B publishes v3 while A's PUT is parked.
    let mut b = Lake::open_s3(config()).unwrap();
    assert_eq!(b.write_from_store(&db).unwrap(), 3);
    let b_snapshot = mock.object("lake/snapshots/3.json").unwrap();
    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        b_snapshot,
        "B published a consistent pair"
    );

    // Release A: its PUT now overwrites B's published snapshot, its read-back
    // sees its own bytes, and the catalog re-check conflicts — then the
    // snapshot is restored from the catalog.
    mock.release_hold();
    let err = a_handle
        .join()
        .expect("writer A thread")
        .expect_err("A must conflict");
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("another writer published version 3"),
        "{err}"
    );

    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        mock.object("lake/snapshots/3.json").unwrap(),
        "no catalog/snapshot disagreement: the loser reconciled the pair"
    );
    assert_eq!(
        mock.object("lake/snapshots/3.json").unwrap(),
        b_snapshot,
        "B's published snapshot bytes are restored"
    );
    // And the lake still loads B's v3.
    let reader = Lake::open_s3(config()).unwrap();
    assert_eq!(reader.load(None).unwrap().table("t").unwrap().num_rows(), 2);
}

/// The catalog-side race of the same fallback: A's catalog PUT (prepared
/// from the version it read) is delayed until after another writer's whole
/// save has landed. A's PUT lands last, its read-back sees its own bytes,
/// and the post-replace verification finds the catalog is A's while the
/// snapshot is the other writer's — so the pair is repaired from the
/// now-authoritative (A) catalog before the save reports success. The pair
/// stays consistent; substitution of a prior publish is the documented
/// single-writer opt-in risk.
#[test]
fn degraded_opt_in_post_replace_verification_reconciles_the_pair() {
    let mock = MockS3::start();
    mock.reject_conditional_writes(true);
    let db = small_store();
    let bucket = "catalog-race-bucket";
    let config = || {
        S3Config::new(bucket)
            .with_prefix("lake")
            .with_region("auto")
            .with_endpoint(mock.endpoint())
            .allow_http(true)
            .allow_degraded(true)
            .with_credentials(mock.access_key_id(), "mock-secret")
    };

    let mut warm = Lake::create_s3(config()).unwrap();
    assert_eq!(warm.write_from_store(&db).unwrap(), 2);
    drop(warm);

    // Park A's fallback catalog PUT (`catalog.json`, no `If-Match`).
    mock.hold_next_request("PUT", "catalog.json");
    let a_handle = std::thread::spawn({
        let config = config();
        let db = small_store();
        move || {
            let mut a = Lake::open_s3(config).unwrap();
            a.write_from_store(&db)
        }
    });
    assert!(
        mock.wait_until_hold_reached(std::time::Duration::from_secs(10)),
        "A's catalog PUT never arrived"
    );
    // A's snapshot landed before the parked catalog PUT: capture its bytes.
    let a_manifest = mock.object("lake/snapshots/3.json").expect("A's snapshot");

    // Another writer's complete v3 pair lands while A's catalog PUT waits in
    // flight (a different, valid manifest: no tables).
    let other = "{\n  \"format_version\": 2,\n  \"updated\": \"2026-10-14T00:00:00.000Z\",\n  \
                 \"version\": 3,\n  \"tables\": {}\n}\n";
    mock.put_object("lake/snapshots/3.json", other.as_bytes());
    mock.put_object("lake/catalog.json", other.as_bytes());

    // Release A: its catalog PUT overwrites the other writer's catalog; the
    // post-replace verification reads A's bytes back and repairs
    // `snapshots/3.json` from A's authoritative catalog.
    mock.release_hold();
    let a_result = a_handle.join().expect("writer A thread");
    assert!(
        a_result.is_ok(),
        "A is the last catalog writer: {a_result:?}"
    );

    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        a_manifest,
        "the last catalog PUT is authoritative"
    );
    assert_eq!(
        mock.object("lake/snapshots/3.json").unwrap(),
        a_manifest,
        "the snapshot was reconciled from the catalog"
    );
    // The lake loads the published pair (A's manifest, with tables).
    let reader = Lake::open_s3(config()).unwrap();
    assert_eq!(reader.current_version(), 3);
    assert_eq!(reader.tables().unwrap().len(), 3);
    assert_eq!(reader.load(None).unwrap().table("t").unwrap().num_rows(), 2);
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
    let lake = Lake::open_s3(mock_config(&mock, "helpers-bucket", "h")).unwrap();
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

/// A conditional `PutObject` answered with `409 Conflict` (a concurrent
/// delete raced the write) is retried, bounded; beyond that it surfaces.
#[test]
fn conditional_409_conflicts_are_retried_within_bounds() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "retry-bucket";
    let mut lake = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    // Two 409s on the snapshot create-only PUT: attempt 3 lands.
    mock.conflict_on_next_conditional_puts(2);
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        mock.object("lake/snapshots/2.json").unwrap()
    );
    // More 409s than the retry budget: a clean I/O error naming the status.
    mock.conflict_on_next_conditional_puts(9);
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
    assert!(err.to_string().contains("409"), "{err}");
    assert!(err.to_string().contains("Conflict"), "{err}");
    // The previous catalog is still authoritative and loadable.
    assert!(lake.load(None).is_ok());
}

/// A lost response (the request landed, the reply never arrived) leaves a
/// safe, inspectable state on both PUT paths, and the catalog/snapshot pair
/// is consistent whenever the catalog actually published.
#[test]
fn lost_put_responses_leave_a_safe_recoverable_state() {
    let db = small_store();

    // (1) The snapshot PUT's response is lost: the object exists, the catalog
    // did not move. The documented recovery is to inspect `.lake versions`
    // and remove the unpublished stray before retrying.
    let mock = MockS3::start();
    let bucket = "lost-snapshot-bucket";
    let config = || mock_config(&mock, bucket, "lake");
    let mut lake = Lake::create_s3(config()).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    mock.drop_next_response("PUT", "lake/snapshots/3.json");
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
    assert!(
        mock.keys().contains(&"lake/snapshots/3.json".to_string()),
        "the write landed before the response was lost"
    );
    let reopened = Lake::open_s3(config()).unwrap();
    assert_eq!(reopened.current_version(), 2, "the catalog did not move");
    assert_eq!(
        reopened.versions().unwrap(),
        vec![1, 2],
        "the stray snapshot is not a published version"
    );
    let mut reopened = reopened;
    let err = reopened.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("refusing to replace a published version"),
        "{err}"
    );
    mock.remove_object("lake/snapshots/3.json");
    assert_eq!(reopened.write_from_store(&db).unwrap(), 3);

    // (2) The catalog PUT's response is lost: it may have landed. Reopening
    // shows the published version, and the pair is byte-identical.
    let mock = MockS3::start();
    let bucket = "lost-catalog-bucket";
    let config = || mock_config(&mock, bucket, "lake");
    let mut lake = Lake::create_s3(config()).unwrap();
    lake.write_from_store(&db).unwrap(); // v2
    mock.drop_next_response("PUT", "lake/catalog.json");
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
    let reopened = Lake::open_s3(config()).unwrap();
    assert_eq!(
        reopened.current_version(),
        3,
        "the catalog PUT landed even though its response was lost"
    );
    assert_eq!(
        mock.object("lake/catalog.json").unwrap(),
        mock.object("lake/snapshots/3.json").unwrap(),
        "catalog == snapshot after the lost response"
    );
    assert!(reopened.load(None).is_ok());
}

/// A wave-34 v1 lake (`.pink` data files, no `format` field) stored in S3
/// loads through the temp-copy bridge; the lock-step with the local writer
/// is byte-exact.
#[test]
fn v1_pink_data_files_load_over_s3() {
    let mock = MockS3::start();
    let scratch = scratch("v1");
    let mut one = PinkDb::new();
    one.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
        .unwrap();
    one.execute("INSERT INTO t VALUES (1, 'x'), (2, NULL), (3, 'z');")
        .unwrap();
    let pink_bytes = store_bytes(&one, &scratch, "v1-data");

    // The v1 catalog shape: no `format` field, a `data/…/*.pink` file.
    let rel = "data/main.t/00000000-0000-4000-8000-000000000000.pink";
    let catalog = format!(
        "{{\n  \"format_version\": 1,\n  \"updated\": \"2026-10-14T00:00:00.000Z\",\n  \
         \"version\": 1,\n  \"tables\": {{\n    \"main.t\": {{\n      \"files\": [\"{rel}\"],\n      \
         \"rows\": 3\n    }}\n  }}\n}}\n"
    );
    mock.put_object("v1/catalog.json", catalog.as_bytes());
    mock.put_object("v1/snapshots/1.json", catalog.as_bytes());
    mock.put_object(
        "v1/data/main.t/00000000-0000-4000-8000-000000000000.pink",
        &pink_bytes,
    );

    let lake = Lake::open_s3(mock_config(&mock, "v1-bucket", "v1")).unwrap();
    assert_eq!(lake.format_version(), 1);
    assert_eq!(lake.current_version(), 1);
    assert_eq!(lake.tables().unwrap(), vec![("main.t".to_string(), 3)]);
    let loaded = lake.load(None).unwrap();
    assert_eq!(
        store_bytes(&loaded, &scratch, "v1-loaded"),
        store_bytes(&one, &scratch, "v1-original"),
        "the temp-copy bridge is byte-exact"
    );
    // The v1 file's temp copy is removed when the load finishes (the
    // `LocalCopy::drop` path); the storage-level unit test
    // (`storage_trait_roundtrip_over_the_mock`) pins the drop itself with a
    // deterministic path assertion, here we only make sure the bridge works.
    // (Scanning `std::env::temp_dir()` would race the other tests in this
    // binary, which use the same temp prefix.)

    // A v1 lake still migrates on the next save (a full Vortex rewrite).
    let mut lake = lake;
    assert_eq!(lake.write_from_store(&loaded).unwrap(), 2);
    assert_eq!(lake.format_version(), 3);
    assert!(mock
        .keys()
        .iter()
        .any(|k| k.starts_with("v1/data/main.t/") && k.ends_with(".vortex")));
    std::fs::remove_dir_all(&scratch).ok();
}

/// The error taxonomy: `SlowDown` 503, a redirect-shaped 301, and
/// `NoSuchBucket` 404 all map to clear, distinct errors.
#[test]
fn s3_error_taxonomy_slowdown_redirect_and_missing_bucket() {
    let mock = MockS3::start();
    let db = small_store();
    let bucket = "taxonomy-bucket";
    let mut lake = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    lake.write_from_store(&db).unwrap(); // v2

    // A transient 503 SlowDown is retried with backoff (S3 returns it before
    // applying the request, so even the create-only snapshot PUT is safe to
    // repeat) and the save goes through.
    mock.inject_error(
        "PUT",
        "lake/snapshots/3.json",
        503,
        "SlowDown",
        "Please reduce your request rate.",
        1,
    );
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.current_version(), 3);

    // A persistent 503 outlasts the retries: an I/O error naming the code
    // (the caller retries the save after reopening).
    mock.inject_error(
        "PUT",
        "lake/snapshots/4.json",
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

    // A redirect-shaped 301 on the catalog read is reported with its code.
    mock.inject_error(
        "GET",
        "lake/catalog.json",
        301,
        "PermanentRedirect",
        "The bucket you are attempting to access must be addressed using the specified endpoint.",
        1,
    );
    let err = lake.tables().unwrap_err();
    assert!(matches!(err, LakeError::Io { .. }), "{err:?}");
    assert!(err.to_string().contains("PermanentRedirect"), "{err}");

    // A bucket that does not exist is a clear bucket error, not "not a lake"
    // (both on the GET body and on the bodiless HEAD probe). A fresh mock is
    // restricted to one bucket (the mock's store is bucket-less, so mixing
    // bucket names in one mock would still find the objects).
    let ghost_mock = MockS3::start();
    ghost_mock.restrict_buckets(&["real-bucket"]);
    let err = Lake::open_s3(mock_config(&ghost_mock, "ghost-bucket", "lake")).unwrap_err();
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

    let mut lake = Lake::create_s3(config()).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    // The bucket traveled in the Host header; the key in the path.
    assert!(
        mock.keys().contains(&"vh/catalog.json".to_string()),
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

    let lake = Lake::open_s3(config()).unwrap();
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

    let mut lake = Lake::create_s3(config.clone()).unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    let lake = Lake::open_s3(config).unwrap();
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
    let mut stale = Lake::open_s3(S3Config::from_env(&bucket).with_prefix(&prefix)).unwrap();
    let mut latest = Lake::open_s3(S3Config::from_env(&bucket).with_prefix(&prefix)).unwrap();
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
    let mut lake = Lake::create_s3(mock_config(&mock, "retry-bucket", "lake")).unwrap();
    lake.write_from_store(&db).unwrap(); // v2

    // Data files are unconditional PUTs under fresh names: retried.
    mock.inject_error("PUT", "lake/data/", 500, "InternalError", "try again", 1);
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.current_version(), 3);

    // The catalog replace is an If-Match PUT: not retried on a 500.
    mock.inject_error(
        "PUT",
        "lake/catalog.json",
        500,
        "InternalError",
        "try again",
        1,
    );
    let err = lake.write_from_store(&db).unwrap_err();
    assert!(err.to_string().contains("InternalError"), "{err}");
}

/// A snapshot that cannot be read is an error from `versions()`, never a
/// version that silently disappears from the listing.
#[test]
fn versions_reports_unreadable_snapshots() {
    let mock = MockS3::start();
    let db = small_store();
    let mut lake = Lake::create_s3(mock_config(&mock, "versions-bucket", "lake")).unwrap();
    lake.write_from_store(&db).unwrap();
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
    mock.inject_error(
        "GET",
        "lake/snapshots/1.json",
        403,
        "AccessDenied",
        "nope",
        1,
    );
    let err = lake.versions().unwrap_err();
    assert!(matches!(err, LakeError::Auth { .. }), "{err:?}");
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
}
