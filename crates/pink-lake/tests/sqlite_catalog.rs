//! The SQLite catalog (`catalog-sqlite`, wave 96a): the existing lake suites'
//! core paths against a split lake — metadata in `sqlite:<path>`, data files
//! in a separate directory (or on mock S3). The tests are the parameterized
//! subset the plan names: create/save/open, commit/conflict/rebase with two
//! handles and with threads, time travel, expire + cleanup, CDC, attach
//! statistics + `count(*)` with zero data requests, replication state,
//! encryption, crash injection, and data-location ownership.
//!
//! Every metadata read here goes through the SQL rows; the data location keeps
//! the ordinary local/S3 paths, so the assertions are about routing,
//! transactions and bytes — not a second copy of the deeper lake suites.

#![cfg(feature = "catalog-sqlite")]

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{
    CleanupSettings, Commit, CommitSettings, ExpireSettings, Lake, LakeError, catalog_stored_bytes,
    fail_next_publish_after_snapshot,
};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_sqlite_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// The catalog location string for a SQLite file.
fn catalog_url(path: &Path) -> String {
    format!("sqlite:{}", path.display())
}

/// `main.t (i INTEGER, s VARCHAR)` with rows `100..100+n`.
fn store(rows: i64) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    for row in 0..rows {
        db.table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![
                Value::Integer(100 + row as i32),
                Value::Varchar(format!("v{row}")),
            ])
            .unwrap();
    }
    db
}

/// A same-schema sub-table holding `rows` (the shape `Commit::append` takes).
fn sub_table(rows: &[(i32, &str)]) -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    for (i, s) in rows {
        table
            .append_row(vec![Value::Integer(*i), Value::Varchar(s.to_string())])
            .unwrap();
    }
    Arc::new(table)
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(v) => i64::from(*v),
        Value::Bigint(v) => *v,
        other => panic!("expected an integer count, got {other:?}"),
    }
}

/// Every file under `dir`, recursively, sorted (the data location).
fn files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn count(lake: &Lake, sql: &str) -> i64 {
    let mut db = lake.load(None).unwrap();
    as_i64(&rows(&mut db, sql)[0][0])
}

/// Create a split lake, save `db` as version 2, and return the handle.
fn saved_catalog(dir: &Path, db: &PinkDb) -> (String, PathBuf, Lake) {
    let catalog = catalog_url(&dir.join("catalog.db"));
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(db).unwrap(), 2);
    (catalog, data, lake)
}

/// Create/save/open through the catalog URL alone; the data location is
/// recorded in `pink_lake_meta`, and every metadata object lives in SQL.
#[test]
fn create_save_open_round_trip() {
    let dir = scratch("roundtrip");
    let (catalog, data, lake) = saved_catalog(&dir, &store(3));
    assert_eq!(lake.current_version(), 2);
    // `Lake::path()` is the data location for a split lake, not the catalog.
    assert_eq!(lake.path(), data.as_path());
    drop(lake);

    // The catalog file exists and the data root has the marker + data files;
    // no catalog/snapshot objects are in the data location.
    assert!(dir.join("catalog.db").is_file());
    let data_files = files(&data);
    assert!(
        data_files
            .iter()
            .any(|path| path.ends_with("lake-data.json")),
        "the ownership marker is a data object: {data_files:?}"
    );
    assert!(
        data_files
            .iter()
            .any(|path| path.extension().is_some_and(|ext| ext != "json")),
        "the data files live at the data location: {data_files:?}"
    );
    assert!(!data.join("catalog.json").exists());
    assert!(!data.join("snapshots").exists());

    // Every metadata object is one catalog row; nothing is duplicated onto
    // disk.
    for rel in [
        "lake.json",
        "catalog.json",
        "highest.json",
        "snapshots/2.json",
    ] {
        assert!(
            catalog_stored_bytes(&catalog, rel).unwrap().is_some(),
            "{rel} must be a catalog row"
        );
    }
    assert!(
        catalog_stored_bytes(&catalog, "commits/2.json")
            .unwrap()
            .is_some()
    );

    // Open with only the catalog URL: the recorded data location is used.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(lake.current_version(), 2);
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 3);
    assert_eq!(lake.commits().unwrap(), vec![2]);

    // Opening an existing SQL catalog by URL alone works (the recorded data
    // location is used); `open_or_create` on a *fresh* one explains that a
    // create needs `data`.
    let reopened = Lake::open_url(&catalog).unwrap();
    assert_eq!(reopened.current_version(), 2);
    let fresh = catalog_url(&dir.join("fresh.db"));
    let err = Lake::open_or_create_url(&fresh).unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("data location"), "{err}");

    cleanup(&dir);
}

/// Two handles opened at the same version: the second rebases and the
/// published catalog is byte-identical to its snapshot row.
#[test]
fn two_handles_conflict_then_rebase() {
    let dir = scratch("rebase");
    let (catalog, _, lake) = saved_catalog(&dir, &store(3));
    drop(lake);
    let mut a = Lake::open_with_catalog(&catalog, None).unwrap();
    let mut b = Lake::open_with_catalog(&catalog, None).unwrap();
    a.set_data_inlining_row_limit(0);
    b.set_data_inlining_row_limit(0);
    let fast = CommitSettings {
        max_retry_count: 10,
        retry_backoff: 1.0,
        retry_wait_ms: 1,
    };

    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(200, "a")]));
    assert_eq!(a.commit_with_settings(&commit, &fast).unwrap().version, 3);

    // B's base is v2; the publish conflicts, rebases once and lands on v4.
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(300, "b")]));
    let report = b.commit_with_settings(&commit, &fast).unwrap();
    assert_eq!(report.version, 4);
    assert_eq!(report.retries, 1);

    // Row count comes from the catalog manifest of the head.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 5);
    let catalog_bytes = catalog_stored_bytes(&catalog, "catalog.json")
        .unwrap()
        .unwrap();
    let snapshot_bytes = catalog_stored_bytes(&catalog, "snapshots/4.json")
        .unwrap()
        .unwrap();
    assert_eq!(catalog_bytes, snapshot_bytes, "catalog == its snapshot");

    cleanup(&dir);
}

/// Eight writers on separate handles publish contiguous versions; SQLite's
/// write lock serializes them and every loser rebases instead of losing work.
#[test]
fn eight_threads_publish_contiguous_versions() {
    let dir = scratch("threads");
    let (catalog, _, lake) = saved_catalog(&dir, &store(3));
    drop(lake);

    let settings = CommitSettings {
        max_retry_count: 40,
        retry_backoff: 1.0,
        retry_wait_ms: 10,
    };
    let mut threads = Vec::new();
    for i in 0..8i32 {
        let catalog = catalog.clone();
        let settings = settings.clone();
        threads.push(std::thread::spawn(move || {
            let mut lake = Lake::open_with_catalog(&catalog, None).unwrap();
            lake.set_data_inlining_row_limit(0);
            let mut commit = Commit::new();
            commit.append("main.t", sub_table(&[(1000 + i, "t")]));
            lake.commit_with_settings(&commit, &settings)
                .unwrap()
                .version
        }));
    }
    let mut versions: Vec<u64> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    versions.sort_unstable();
    assert_eq!(versions, (3..=10).collect::<Vec<_>>());

    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(lake.versions().unwrap(), (1..=10).collect::<Vec<_>>());
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 11);
    let catalog_bytes = catalog_stored_bytes(&catalog, "catalog.json")
        .unwrap()
        .unwrap();
    let snapshot_bytes = catalog_stored_bytes(&catalog, "snapshots/10.json")
        .unwrap()
        .unwrap();
    assert_eq!(catalog_bytes, snapshot_bytes);

    cleanup(&dir);
}

/// Older versions load through the catalog exactly like the head.
#[test]
fn time_travel_reads_older_versions() {
    let dir = scratch("time-travel");
    let (catalog, _, mut lake) = saved_catalog(&dir, &store(3));
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(400, "later")]));
    assert_eq!(lake.commit(&commit).unwrap().version, 3);
    drop(lake);

    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 4);
    let mut old = lake.load(Some(2)).unwrap();
    assert_eq!(as_i64(&rows(&mut old, "SELECT count(*) FROM t")[0][0]), 3);
    let err = lake.load(Some(99)).unwrap_err();
    assert!(matches!(err, LakeError::VersionNotFound { .. }), "{err:?}");

    cleanup(&dir);
}

/// Expire removes old catalog rows; cleanup then deletes the data file only
/// that expired version referenced — after verifying the ownership marker.
#[test]
fn expire_and_cleanup_on_a_split_lake() {
    let dir = scratch("expire-cleanup");
    let (catalog, data, mut lake) = saved_catalog(&dir, &store(3));
    // Dropping the table removes its data file from the head; the retained
    // version 2 still references it, so expire is what makes it an orphan.
    let mut commit = Commit::new();
    commit.drop_table("main.t");
    assert_eq!(lake.commit(&commit).unwrap().version, 3);

    // Version 2's data file is now only referenced by the expired history.
    let report = lake
        .expire_snapshots(&ExpireSettings {
            versions: Some(vec![2]),
            older_than_micros: None,
            retain_last: None,
            dry_run: false,
        })
        .unwrap();
    assert_eq!(report.version_numbers(), vec![2]);
    assert!(
        catalog_stored_bytes(&catalog, "snapshots/2.json")
            .unwrap()
            .is_none(),
        "the expired snapshot row is gone"
    );

    let cleaned = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(cleaned.deleted, "{cleaned:?}");
    assert!(
        !cleaned.paths.is_empty(),
        "the expired file is unreferenced"
    );
    // Every deleted object is gone from the data location; every other data
    // object (version 3's file and the marker) survives.
    for path in &cleaned.paths {
        assert!(!data.join(path).exists(), "{path} was not deleted");
    }
    assert!(data.join("lake-data.json").exists());
    assert!(
        !files(&data)
            .iter()
            .any(|path| path.extension().is_some_and(|ext| ext != "json")),
        "the dropped table's file was the only data object"
    );

    cleanup(&dir);
}

/// CDC reads the commit deltas from the catalog rows.
#[test]
fn cdc_reports_the_committed_rows() {
    let dir = scratch("cdc");
    let (_catalog, _, mut lake) = saved_catalog(&dir, &store(3));
    // Inlining keeps the change in the manifest, the CDC path's other half
    // (the data-file path is covered by the existing lake suites).
    lake.set_data_inlining_row_limit(10);
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(600, "c1"), (601, "c2")]));
    assert_eq!(lake.commit(&commit).unwrap().version, 3);

    let changes = lake.table_changes("main.t", 3, None).unwrap();
    assert_eq!(changes.len(), 2, "{changes:?}");
    for change in &changes {
        assert_eq!(
            change.change_type,
            pink_lake::ChangeKind::Insert,
            "{change:?}"
        );
    }

    cleanup(&dir);
}

/// Attach + `count(*)` over a SQLite catalog with data on mock S3 read **no
/// data object**: the metadata round trips that dominated an R2-only lake are
/// local SQL rows now, attach only HEADs the files for integrity, and the row
/// count is answered from the manifest statistics without a request.
#[cfg(feature = "s3")]
#[test]
fn attach_and_count_issue_no_data_requests_over_s3() {
    use mock_s3::MockS3;

    let mock = MockS3::start();
    // The only process-environment user in this test binary. No other test
    // here reads the AWS variables, so this does not race a peer.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
        std::env::set_var("AWS_ENDPOINT_URL", mock.endpoint());
        std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
        std::env::set_var("AWS_REGION", "auto");
        std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
    }

    let dir = scratch("s3-attach");
    let catalog = catalog_url(&dir.join("catalog.db"));
    let mut lake = Lake::create_with_catalog(&catalog, "s3://catalog-bucket/split-data").unwrap();
    // Pin the `.pink` format so attach never needs a data-file footer (a
    // `vortexlake` build would otherwise default to Vortex).
    lake.set_write_format("pink").unwrap();
    assert_eq!(lake.write_from_store(&store(4)).unwrap(), 2);
    drop(lake);

    let before = mock.requests().len();
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.version, 2);
    // With the catalog in SQLite and no data-file probes at attach (wave
    // 96a, like DuckLake), a statistics-only attach sends nothing to S3.
    assert_eq!(
        mock.requests().len(),
        before,
        "attach must not send any request to S3: {:?}",
        &mock.requests()[before..]
    );
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let before_query = mock.requests().len();
    assert_eq!(as_i64(&rows(&mut db, "SELECT count(*) FROM t")[0][0]), 4);
    assert_eq!(
        mock.requests().len(),
        before_query,
        "count(*) is answered from the catalog statistics, with no data request: {:?}",
        &mock.requests()[before_query..]
    );

    cleanup(&dir);
}

/// Replication job state is a `replication/…` metadata row and survives a
/// reopen (the write goes through the publish scope).
#[test]
fn replication_state_round_trips() {
    let dir = scratch("replication");
    let source = dir.join("source");
    pink_lake::save_store(&store(2), &source).unwrap();
    let (catalog, _, destination) = saved_catalog(&dir, &store(1));
    let mut destination = destination;

    let job = pink_lake::replication::create(
        &mut destination,
        pink_lake::replication::JobSpec::new(pink_lake::replication::Source::Lake {
            location: source.to_str().unwrap().to_string(),
        }),
    )
    .unwrap();
    assert_eq!(job, 1);
    drop(destination);

    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    let jobs = pink_lake::replication::jobs(&lake).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, 1);
    assert!(matches!(
        jobs[0].source,
        pink_lake::replication::Source::Lake { .. }
    ));
    assert!(
        catalog_stored_bytes(&catalog, "replication/state.json")
            .unwrap()
            .is_some()
    );

    cleanup(&dir);
}

/// A keyed split lake seals the catalog rows: the bytes in SQL are `PNKLAKE1`
/// containers, not plaintext metadata. The keyed open round-trips; a wrong
/// key fails closed at the catalog.
#[cfg(feature = "encryption")]
#[test]
fn encrypted_catalog_rows_are_sealed() {
    use pink_lake::EncryptionKey;

    let dir = scratch("encryption");
    let catalog = catalog_url(&dir.join("catalog.db"));
    let data = dir.join("data-root");
    let key = EncryptionKey::from_passphrase("wave-96a-sqlite");
    let mut lake =
        Lake::create_with_catalog_with_key(&catalog, data.to_str().unwrap(), key.clone()).unwrap();
    assert_eq!(lake.write_from_store(&store(3)).unwrap(), 2);
    drop(lake);

    let bytes = catalog_stored_bytes(&catalog, "catalog.json")
        .unwrap()
        .unwrap();
    assert!(bytes.starts_with(b"PNKLAKE1"), "the row must be sealed");
    assert!(
        !bytes.windows(14).any(|w| w == b"format_version"),
        "no plaintext metadata in the catalog"
    );

    let lake = Lake::open_with_catalog_with_key(&catalog, None, key).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 3);
    let wrong = Lake::open_with_catalog_with_key(
        &catalog,
        None,
        EncryptionKey::from_passphrase("not-the-key"),
    )
    .unwrap_err();
    assert!(matches!(wrong, LakeError::WrongKey { .. }), "{wrong:?}");

    cleanup(&dir);
}

/// An injected failure after the snapshot insert rolls the whole publication
/// back: no new snapshot row, no head movement, no new commit row. (The data
/// file written before the scope may remain; that is the documented crash
/// shape and `delete_orphaned_files` removes it.)
#[test]
fn a_failure_after_the_snapshot_insert_rolls_back_the_publication() {
    let dir = scratch("crash");
    let (catalog, _, mut lake) = saved_catalog(&dir, &store(3));
    let head_before = catalog_stored_bytes(&catalog, "catalog.json")
        .unwrap()
        .unwrap();

    fail_next_publish_after_snapshot(lake.lake_id());
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(700, "crash")]));
    // No retry budget: the default path would retry the (now disarmed)
    // publication and succeed, which also proves the rollback is clean. Here
    // the first failure is what the caller sees. The retry wrapper reports a
    // non-conflict publish error as an exhausted-conflict, so match on the
    // message rather than the variant.
    let err = lake
        .commit_with_settings(
            &commit,
            &CommitSettings {
                max_retry_count: 0,
                retry_backoff: 1.0,
                retry_wait_ms: 1,
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");

    // The head is untouched and the attempted version left no metadata row.
    assert_eq!(lake.current_version(), 2);
    assert!(
        catalog_stored_bytes(&catalog, "snapshots/3.json")
            .unwrap()
            .is_none()
    );
    assert!(
        catalog_stored_bytes(&catalog, "commits/3.json")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        catalog_stored_bytes(&catalog, "catalog.json")
            .unwrap()
            .unwrap(),
        head_before
    );
    let reopened = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(reopened.current_version(), 2);
    assert_eq!(count(&reopened, "SELECT count(*) FROM t"), 3);

    cleanup(&dir);
}

/// Create refuses a data location that already belongs to a lake, and the
/// destructive maintenance paths verify `lake-data.json` before deleting.
#[test]
fn data_location_ownership_is_enforced() {
    let dir = scratch("ownership");
    let (catalog, data, mut lake) = saved_catalog(&dir, &store(2));
    drop(catalog);

    // A full object-store lake at the same data location is refused.
    let plain = dir.join("plain");
    pink_lake::save_store(&store(1), &plain).unwrap();
    let err = Lake::create_with_catalog(
        &catalog_url(&dir.join("over-plain.db")),
        plain.to_str().unwrap(),
    )
    .unwrap_err();
    assert!(matches!(err, LakeError::AlreadyALake { .. }), "{err:?}");

    // So is a data location that already holds this lake's data files.
    let err = Lake::create_with_catalog(
        &catalog_url(&dir.join("over-data.db")),
        data.to_str().unwrap(),
    )
    .unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("already holds lake data"), "{err}");

    // A data location owned by a split lake that has no data files yet is
    // refused too: the owner marker alone claims it, so a second catalog can
    // never overwrite the marker and then treat the first lake's files as
    // orphans.
    let fresh_data = dir.join("fresh-data");
    let _fresh = Lake::create_with_catalog(
        &catalog_url(&dir.join("fresh.db")),
        fresh_data.to_str().unwrap(),
    )
    .unwrap();
    let err = Lake::create_with_catalog(
        &catalog_url(&dir.join("over-fresh.db")),
        fresh_data.to_str().unwrap(),
    )
    .unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(
        err.to_string().contains("owned by another catalog"),
        "{err}"
    );

    // A second catalog pointed at the same data root cannot delete: the
    // marker names the first lake.
    let other_catalog = catalog_url(&dir.join("other.db"));
    let other_data = dir.join("other-data");
    let _other = Lake::create_with_catalog(&other_catalog, other_data.to_str().unwrap()).unwrap();
    std::fs::create_dir_all(data.join("data")).unwrap();
    std::fs::write(data.join("data").join("orphan.pink"), b"orphan").unwrap();
    let mut other = Lake::open_with_catalog(&other_catalog, data.to_str()).unwrap();
    let err = other
        .delete_orphaned_files(&CleanupSettings {
            all: true,
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("belongs to lake"),
        "the ownership refusal must name the mismatch: {err}"
    );

    // The original lake still cleans up its own orphan.
    let report = lake
        .delete_orphaned_files(&CleanupSettings {
            all: true,
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(report.deleted);
    assert!(!data.join("data").join("orphan.pink").exists());

    cleanup(&dir);
}

/// `sqlite::memory:` and an empty path cannot back a persistent lake.
#[test]
fn sqlite_location_validation() {
    for location in ["sqlite:", "sqlite::memory:"] {
        let err = Lake::open_url(location).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("must persist"), "{err}");
    }
    // Opening a catalog that does not exist is `NotALake` and creates
    // nothing (a mistyped path never leaves an empty database behind).
    let dir = scratch("missing-catalog");
    let missing = dir.join("typo.db");
    let err = Lake::open_url(&catalog_url(&missing)).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(
        !missing.exists(),
        "open must not create {}",
        missing.display()
    );
    cleanup(&dir);

    // The data half of a split lake cannot itself be a catalog.
    let err = Lake::create_with_catalog("sqlite:/tmp/a.db", "sqlite:/tmp/b.db").unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("data location"), "{err}");
}
