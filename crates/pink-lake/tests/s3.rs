//! End-to-end tests of the S3 storage backend against a hermetic mock S3
//! server (no cloud, no credentials): the full lake cycle, prefixes and
//! percent-encoded table names, conditional-write conflict paths, error
//! mapping, and the documented degradation for endpoints without conditional
//! writes. The `PINKDB_S3_LIVE=1` test at the bottom talks to a real bucket
//! (e.g. Cloudflare R2) and is skipped by default.

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
fn mock_config(mock: &MockS3, bucket: &str, prefix: &str) -> S3Config {
    S3Config::new(bucket)
        .with_prefix(prefix)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
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
    assert_eq!(lake.format_version(), 2);
    assert_eq!(lake.versions().unwrap(), vec![1]);
    assert_eq!(lake.path().to_str().unwrap(), "s3://test-bucket/lakes/mine");
    let keys = mock.keys();
    assert!(keys.contains(&"lakes/mine/catalog.json".to_string()), "{keys:?}");
    assert!(keys.contains(&"lakes/mine/snapshots/1.json".to_string()), "{keys:?}");

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
    assert!(catalog.starts_with(b"{\n  \"format_version\": 2"), "{}", String::from_utf8_lossy(&catalog));

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
    assert_eq!(lake.load(Some(2)).unwrap().table("t").unwrap().num_rows(), 2);

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
    assert!(mock
        .keys()
        .contains(&"lake/snapshots/5.json".to_string()));
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
    assert_eq!(
        mock.object("lake/snapshots/3.json").unwrap(),
        v3_snapshot
    );
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
    assert!(msg.contains("refusing to replace a published version"), "{msg}");
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
    assert!(root_keys.contains(&"catalog.json".to_string()), "{root_keys:?}");
    assert!(root_keys.contains(&"snapshots/2.json".to_string()), "{root_keys:?}");
    assert!(
        root_keys
            .iter()
            .any(|k| k.starts_with("data/main.root_t/")),
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
    odd.create_table_in("main", "odd name", vec![("y".to_string(), DataType::Varchar)])
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
    assert!(err.to_string().contains("has no snapshot version 99"), "{err}");

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
    assert!(missing[0].1.to_str().unwrap().starts_with("s3://"), "{missing:?}");
    let err = lake.load(None).unwrap_err();
    assert!(err.to_string().contains("loading data file"), "{err}");

    // 403 (wrong credentials) → the auth error naming the lake. (The catalog
    // probe is a HEAD, whose error responses carry no body to parse, so the
    // code falls back to AccessDenied; a GET-based probe keeps the S3 code —
    // see the storage-level test.)
    let wrong = S3Config::new(bucket)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
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
    // `auto` region, and the session token was signed too.
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
    // A session token is sent on the wire, not only signed.
    assert!(mock
        .requests()
        .iter()
        .any(|_| true));
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
fn endpoints_without_conditional_writes_degrade_with_a_documented_window() {
    let mock = MockS3::start();
    mock.reject_conditional_writes(true);
    let db = small_store();
    let bucket = "degraded-bucket";

    // create: `If-None-Match: *` is refused (501), the client falls back to
    // HEAD-then-PUT and still publishes the v1 pair.
    let a = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap();
    assert_eq!(a.current_version(), 1);
    assert_eq!(mock.conditional_rejections(), 1, "the snapshot create-only PUT");
    let mut a = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();

    // A fresh handle tries `If-None-Match: *` once (the snapshot manifest),
    // records that the endpoint refuses, and the same save's catalog replace
    // already uses the fallback. Later saves never probe again.
    assert_eq!(a.write_from_store(&db).unwrap(), 2);
    assert_eq!(
        mock.conditional_rejections(),
        2,
        "exactly one conditional attempt before the degradation is sticky"
    );
    assert_eq!(a.write_from_store(&db).unwrap(), 3);
    assert_eq!(mock.conditional_rejections(), 2);
    assert!(
        a.load(None).is_ok(),
        "the degraded path still publishes loadable versions"
    );

    // Conflict detection still works: it comes from the version re-check,
    // which needs no conditional writes.
    let mut b = Lake::open_s3(mock_config(&mock, bucket, "lake")).unwrap();
    a.write_from_store(&db).unwrap(); // moves the catalog forward
    let err = b.write_from_store(&db).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    assert!(
        err.to_string().contains("another writer published version"),
        "{err}"
    );
    // And a create on an existing lake is still refused (the HEAD pre-check
    // in the degraded path).
    let err = Lake::create_s3(mock_config(&mock, bucket, "lake")).unwrap_err();
    assert!(matches!(err, LakeError::AlreadyALake { .. }), "{err:?}");
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
    dotted.execute("CREATE TABLE \"a.b\".t (x INTEGER);").unwrap();
    let before = mock.keys().len();
    let err = pink_lake::save_store_s3(&dotted, mock_config(&mock, "helpers-bucket", "h2"))
        .unwrap_err();
    assert!(err.to_string().contains("contains \".\""), "{err}");
    assert_eq!(mock.keys().len(), before, "a rejected save writes nothing");
    std::fs::remove_dir_all(&dir).ok();
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
