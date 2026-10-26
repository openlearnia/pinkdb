//! Mock-only coverage for the `pink+s3://` catalog database.
#![cfg(feature = "catalog-s3")]

#[path = "../src/mock_s3.rs"]
mod mock_s3;

use std::sync::{Arc, Mutex, MutexGuard};

use mock_s3::MockS3;
use pink_lake::{Commit, EncryptionKey, Lake, LakeError};
use pinkdb::{DataTable, DataType, PinkDb, Value};

static ENV: Mutex<()> = Mutex::new(());

fn mock_env() -> (MutexGuard<'static, ()>, MockS3) {
    let guard = ENV.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mock = MockS3::start();
    // This integration-test binary serializes every environment user with
    // `ENV`; other test binaries are separate processes.
    unsafe {
        // Guard against a panic in a previous test leaving an override behind.
        std::env::remove_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL");
        std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
        std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
        std::env::set_var("AWS_REGION", "auto");
        std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
    }
    (guard, mock)
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR)").unwrap();
    db.execute("INSERT INTO t VALUES (0, 'seed')").unwrap();
    db
}

fn row(i: i32) -> Arc<DataTable> {
    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    table
        .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
        .unwrap();
    Arc::new(table)
}

fn commit_row(lake: &mut Lake, i: i32) -> u64 {
    let mut commit = Commit::new();
    commit.append("main.t", row(i));
    lake.commit(&commit).unwrap().version
}

fn rows(lake: &Lake) -> usize {
    lake.load(None).unwrap().table("t").unwrap().num_rows()
}

fn catalog_rows(catalog: &str) -> usize {
    rows(&Lake::open_url(catalog).unwrap())
}

fn log_sequences(mock: &MockS3) -> Vec<u64> {
    let mut sequences: Vec<u64> = mock
        .keys()
        .into_iter()
        .filter_map(|key| {
            key.rsplit_once("/catalog/log/")
                .map(|(_, name)| name.to_string())
        })
        .filter_map(|name| name.parse().ok())
        .collect();
    sequences.sort_unstable();
    sequences
}

fn checkpoint_sequences(mock: &MockS3) -> Vec<u64> {
    let mut sequences: Vec<u64> = mock
        .keys()
        .into_iter()
        .filter_map(|key| {
            key.rsplit_once("/catalog/checkpoint/")
                .and_then(|(_, name)| name.strip_suffix(".pink").map(str::to_string))
        })
        .filter_map(|name| name.parse().ok())
        .collect();
    sequences.sort_unstable();
    sequences
}

fn last_log_bytes(mock: &MockS3) -> usize {
    let key = mock
        .keys()
        .into_iter()
        .filter(|key| key.contains("/catalog/log/"))
        .max()
        .expect("a catalog log object");
    mock.object(&key).expect("a readable log").len()
}

fn data_files(mock: &MockS3, prefix: &str) -> usize {
    let prefix = format!("{prefix}/data/");
    mock.keys()
        .into_iter()
        .filter(|key| key.starts_with(&prefix) && key.ends_with(".pink"))
        .count()
}

#[test]
fn create_250_commits_reopen_from_checkpoint_and_scan() {
    let (_env, mock) = mock_env();
    let catalog = "pink+s3://catalog-bucket/checkpoint";
    let mut lake = Lake::create_url(catalog).unwrap();
    lake.set_data_inlining_row_limit(1_000);
    assert_eq!(lake.write_from_store(&store()).unwrap(), 2);
    for i in 1..=250 {
        let mut commit = Commit::new();
        commit.append("main.t", row(i));
        lake.commit(&commit).unwrap();
    }
    assert_eq!(lake.current_version(), 252);
    drop(lake);

    let keys = mock.keys();
    assert!(
        keys.iter().any(|key| key.contains("/catalog/checkpoint/")),
        "{keys:?}"
    );
    let before = mock.requests().len();
    let lake = Lake::open_url(catalog).unwrap();
    assert_eq!(lake.load(None).unwrap().table("t").unwrap().num_rows(), 251);
    let requests = mock.requests();
    let reopen = &requests[before..];
    let checkpoint_gets = reopen
        .iter()
        .filter(|request| request.starts_with("GET ") && request.contains("/catalog/checkpoint/"))
        .count();
    let log_gets = reopen
        .iter()
        .filter(|request| request.starts_with("GET ") && request.contains("/catalog/log/"))
        .count();
    assert!(checkpoint_gets <= 1, "{reopen:?}");
    assert!(
        log_gets < 100,
        "latest checkpoint bounds replay: {log_gets}"
    );
}

#[test]
fn a_lost_race_on_one_log_object_retries_or_reports_a_conflict() {
    let (_env, mock) = mock_env();
    let catalog = "pink+s3://catalog-bucket/race";
    let mut lake = Lake::create_url(catalog).unwrap();
    lake.set_data_inlining_row_limit(1_000);
    assert_eq!(lake.write_from_store(&store()).unwrap(), 2);
    drop(lake);
    let base = log_sequences(&mock);
    assert_eq!(base, (1..=base.len() as u64).collect::<Vec<_>>());

    let mut a = Lake::open_url(catalog).unwrap();
    let mut b = Lake::open_url(catalog).unwrap();
    a.set_data_inlining_row_limit(1_000);
    b.set_data_inlining_row_limit(1_000);

    // The loser's conditional create of `log/<next>` is answered with the
    // precondition failure a lost race produces. With no retry budget that is
    // a clear conflict, and the loser reloads the winner's catalog instead of
    // keeping its own version.
    mock.inject_error(
        "PUT",
        "/catalog/log/",
        412,
        "PreconditionFailed",
        "precondition failed",
        1,
    );
    let mut commit = Commit::new();
    commit.append("main.t", row(10));
    let settings = pink_lake::CommitSettings {
        max_retry_count: 0,
        ..Default::default()
    };
    let error = a.commit_with_settings(&commit, &settings).unwrap_err();
    assert!(matches!(error, LakeError::Conflict { .. }), "{error:?}");
    assert_eq!(rows(&a), 1, "the loser must reload the published catalog");

    // The same lost race with the default retry budget rebases and commits,
    // and nothing is lost or published twice.
    mock.inject_error(
        "PUT",
        "/catalog/log/",
        412,
        "PreconditionFailed",
        "precondition failed",
        1,
    );
    let va = commit_row(&mut a, 10);
    let vb = commit_row(&mut b, 11);
    assert_eq!((va, vb), (3, 4));
    assert_eq!(
        log_sequences(&mock),
        (1..=base.len() as u64 + 2).collect::<Vec<_>>(),
        "exactly one log object per commit, in sequence"
    );
    let lake = Lake::open_url(catalog).unwrap();
    assert_eq!(lake.current_version(), 4);
    assert_eq!(rows(&lake), 3);
}

#[test]
fn unsupported_conditional_writes_never_fall_back_to_check_then_put() {
    let (_env, mock) = mock_env();
    let catalog = "pink+s3://catalog-bucket/read-only";
    let mut lake = Lake::create_url(catalog).unwrap();
    lake.set_data_inlining_row_limit(10);
    lake.write_from_store(&store()).unwrap();
    drop(lake);

    mock.set_conditional_rejection(501, "NotImplemented");
    let mut lake = Lake::open_url(catalog).unwrap();
    assert_eq!(rows(&lake), 1);
    let before = log_sequences(&mock);
    let mut commit = Commit::new();
    commit.append("main.t", row(1));
    let error = lake.commit(&commit).unwrap_err();
    assert!(matches!(error, LakeError::Io { .. }), "{error:?}");
    assert!(error.to_string().contains("conditional writes"), "{error}");
    assert_eq!(log_sequences(&mock), before);
    assert_eq!(rows(&lake), 1);
    assert_eq!(catalog_rows(catalog), 1);
}

#[test]
fn a_commit_uploads_only_its_delta_however_many_files_the_lake_has() {
    let (_env, mock) = mock_env();
    unsafe { std::env::set_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL", "1000000") };
    let catalog = "pink+s3://catalog-bucket/delta";
    let mut lake = Lake::create_url(catalog).unwrap();
    // No inlining, so every appended row is its own data file and the lake
    // really grows one file per row.
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&store()).unwrap();
    for i in 1..=9 {
        commit_row(&mut lake, i);
    }
    assert_eq!(data_files(&mock, "delta"), 10);
    commit_row(&mut lake, 10);
    let small = last_log_bytes(&mock);

    // Appends in one commit are merged into one file, so grow the lake with
    // one commit per file.
    for i in 11..=1_010 {
        commit_row(&mut lake, i);
    }
    let files = data_files(&mock, "delta");
    assert!(files >= 1_000, "{files} data files");
    commit_row(&mut lake, 9_999);
    let big = last_log_bytes(&mock);
    assert!(
        big <= small + 128,
        "a commit logged {small} bytes with 10 data files and {big} with {files}"
    );
    drop(lake);
    unsafe { std::env::remove_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL") };
}

#[test]
fn crash_windows_recover_from_the_objects_alone() {
    let (_env, mock) = mock_env();
    let catalog = "pink+s3://catalog-bucket/crash";
    let mut lake = Lake::create_url(catalog).unwrap();
    lake.set_data_inlining_row_limit(1_000);
    lake.write_from_store(&store()).unwrap();
    for i in 1..=120 {
        commit_row(&mut lake, i);
    }
    drop(lake);
    assert!(!checkpoint_sequences(&mock).is_empty());

    // A checkpoint is durable but the head hint never landed.
    mock.remove_object("catalog-bucket/crash/catalog/head");
    assert_eq!(catalog_rows(catalog), 121);

    // The head hint names a checkpoint whose upload never landed.
    mock.put_object("catalog-bucket/crash/catalog/head", b"999999");
    assert_eq!(catalog_rows(catalog), 121);

    // The logs are durable but the writing process died before applying them:
    // a new process starts from the checkpoint and the log listing, with no
    // local checkpoint cache to help it.
    let _ = std::fs::remove_dir_all(std::env::temp_dir().join("pinkdb-catalog-s3-cache"));
    mock.remove_object("catalog-bucket/crash/catalog/head");
    let before = mock.requests().len();
    assert_eq!(catalog_rows(catalog), 121);
    let reopen = mock.requests()[before..].to_vec();
    let checkpoint_gets = reopen
        .iter()
        .filter(|request| request.starts_with("GET ") && request.contains("/catalog/checkpoint/"))
        .count();
    let log_gets = reopen
        .iter()
        .filter(|request| request.starts_with("GET ") && request.contains("/catalog/log/"))
        .count();
    assert_eq!(checkpoint_gets, 1, "{reopen:?}");
    assert!(log_gets > 0 && log_gets <= 100, "replay: {log_gets}");
}

#[test]
fn time_travel_across_checkpoint_and_expire_prunes_logs() {
    let (_env, mock) = mock_env();
    unsafe { std::env::set_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL", "3") };
    let catalog = "pink+s3://catalog-bucket/timetravel";
    let mut lake = Lake::create_url(catalog).unwrap();
    lake.set_data_inlining_row_limit(1_000);
    lake.write_from_store(&store()).unwrap(); // version 2, 1 row
    for i in 1..=8 {
        commit_row(&mut lake, i);
    }
    assert_eq!(
        lake.load(Some(2)).unwrap().table("t").unwrap().num_rows(),
        1
    );
    assert_eq!(
        lake.load(Some(5)).unwrap().table("t").unwrap().num_rows(),
        4
    );
    assert_eq!(rows(&lake), 9);
    assert!(!checkpoint_sequences(&mock).is_empty());

    lake.expire_snapshots(&pink_lake::ExpireSettings {
        versions: Some(vec![2, 3]),
        ..Default::default()
    })
    .unwrap();
    let newest = *checkpoint_sequences(&mock).last().unwrap();
    assert_eq!(
        checkpoint_sequences(&mock),
        vec![newest],
        "expire replaces superseded checkpoint images with a new one"
    );
    assert!(
        log_sequences(&mock)
            .iter()
            .all(|sequence| *sequence > newest),
        "expire deletes only the logs folded into the newest checkpoint"
    );
    assert!(
        lake.load(Some(2)).is_err(),
        "expired snapshot must not load"
    );
    drop(lake);
    unsafe { std::env::remove_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL") };

    // The catalog survives the prune: the newest checkpoint carries the whole
    // history, so retained snapshots still time travel.
    let lake = Lake::open_url(catalog).unwrap();
    assert_eq!(rows(&lake), 9);
    assert_eq!(
        lake.load(Some(6)).unwrap().table("t").unwrap().num_rows(),
        5
    );
    assert!(lake.load(Some(2)).is_err());
}

#[test]
fn migration_round_trips_through_pink_and_sqlite() {
    let (_env, _mock) = mock_env();
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_catalog_s3_migrate_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let s3_catalog = "pink+s3://catalog-bucket/migrate";
    let image = |catalog: &str| Lake::open_url(catalog).unwrap().load(None).unwrap();

    // pink: -> pink+s3://
    let pink_catalog = format!("pink:{}", dir.join("a.pink").display());
    let mut source =
        Lake::create_with_catalog(&pink_catalog, dir.join("data").to_str().unwrap()).unwrap();
    source.write_from_store(&store()).unwrap();
    let source_image = source.load(None).unwrap();
    drop(source);
    let mut target = Lake::create_url(s3_catalog).unwrap();
    target.write_from_store(&source_image).unwrap();
    drop(target);
    assert_eq!(catalog_rows(s3_catalog), 1);

    // pink+s3:// -> pink:
    let back_catalog = format!("pink:{}", dir.join("back.pink").display());
    let mut target =
        Lake::create_with_catalog(&back_catalog, dir.join("data2").to_str().unwrap()).unwrap();
    target.write_from_store(&image(s3_catalog)).unwrap();
    drop(target);
    assert_eq!(catalog_rows(&back_catalog), 1);

    // sqlite: -> pink+s3:// and pink+s3:// -> sqlite:, where that backend is
    // built (the `catalog-s3` gate does not turn on `catalog-sqlite`).
    #[cfg(feature = "catalog-sqlite")]
    {
        let sqlite_catalog = format!("sqlite:{}", dir.join("c.db").display());
        let mut source =
            Lake::create_with_catalog(&sqlite_catalog, dir.join("data3").to_str().unwrap())
                .unwrap();
        source.write_from_store(&store()).unwrap();
        let source_image = source.load(None).unwrap();
        drop(source);
        let mut target = Lake::create_url("pink+s3://catalog-bucket/migrate-sqlite").unwrap();
        target.write_from_store(&source_image).unwrap();
        drop(target);
        assert_eq!(catalog_rows("pink+s3://catalog-bucket/migrate-sqlite"), 1);

        let sqlite_back = format!("sqlite:{}", dir.join("d.db").display());
        let mut target =
            Lake::create_with_catalog(&sqlite_back, dir.join("data4").to_str().unwrap()).unwrap();
        target
            .write_from_store(&image("pink+s3://catalog-bucket/migrate-sqlite"))
            .unwrap();
        drop(target);
        assert_eq!(catalog_rows(&sqlite_back), 1);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "encryption")]
#[test]
fn keyed_checkpoint_and_logs_hide_table_names() {
    let (_env, mock) = mock_env();
    unsafe {
        std::env::set_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL", "1");
    }
    let catalog = "pink+s3://catalog-bucket/encrypted";
    let key = EncryptionKey::from_passphrase("mock catalog key");
    let mut lake = Lake::create_url_with_key(catalog, key.clone()).unwrap();
    lake.write_from_store(&store()).unwrap();
    drop(lake);

    let objects: Vec<Vec<u8>> = mock
        .keys()
        .into_iter()
        .filter(|name| name.contains("/catalog/log/") || name.contains("/catalog/checkpoint/"))
        .filter_map(|name| mock.object(&name))
        .collect();
    assert!(!objects.is_empty());
    for bytes in objects {
        assert!(bytes.starts_with(b"PINKENC1"));
        assert!(
            !bytes
                .windows(b"ducklake_table".len())
                .any(|window| window == b"ducklake_table")
        );
        assert!(!bytes.windows(6).any(|window| window == b"main.t"));
    }
    assert_eq!(
        Lake::open_url_with_key(catalog, key)
            .unwrap()
            .load(None)
            .unwrap()
            .table("t")
            .unwrap()
            .num_rows(),
        1
    );
    unsafe {
        std::env::remove_var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL");
    }
}
