//! The shared catalog-store test suite (wave 96a): every assertion the SQLite
//! catalog suite makes, runnable against SQLite (always) and Postgres (when
//! `PINKDB_TEST_POSTGRES_URL` names a local test server).
//!
//! The suite covers the plan's parameterized subset: create/save/open,
//! commit/conflict/rebase with two handles and with threads, time travel,
//! expire + cleanup, CDC, attach statistics + `count(*)` with zero data
//! requests, replication state, encryption, crash injection, data-location
//! ownership, and catalog migration in all three directions.
//!
//! The two backends differ only in how a catalog URL is minted. A Postgres
//! run allocates a fresh `w96a_<pid>_<seq>` schema per catalog and drops it
//! when the suite value is dropped; a SQLite run uses one file per catalog in
//! the test's scratch directory.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{
    CleanupSettings, Commit, CommitSettings, ExpireSettings, Lake, LakeError,
    fail_next_publish_after_snapshot,
};
use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A fresh scratch directory for one test.
pub fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_catalog_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// How one backend mints a catalog URL for a test.
pub trait Suite {
    /// A catalog for `name` (the same name may be used for a second catalog
    /// in another scratch directory).
    fn catalog(&mut self, dir: &Path, name: &str) -> String;
}

/// SQLite: one database file per catalog inside the test's scratch directory.
#[derive(Debug, Default)]
pub struct SqliteSuite;

impl SqliteSuite {
    pub fn new() -> Self {
        Self
    }
}

impl Suite for SqliteSuite {
    fn catalog(&mut self, dir: &Path, name: &str) -> String {
        format!("sqlite:{}", dir.join(format!("{name}.db")).display())
    }
}

/// pinkdb `.pink`: one durable database file per catalog in the test's
/// scratch directory.
#[derive(Debug, Default)]
pub struct PinkSuite;

impl PinkSuite {
    pub fn new() -> Self {
        Self
    }
}

impl Suite for PinkSuite {
    fn catalog(&mut self, dir: &Path, name: &str) -> String {
        format!("pink:{}", dir.join(format!("{name}.pink")).display())
    }
}

/// Postgres: one schema per catalog, named `w96a_<pid>_<seq>`, dropped on
/// drop.
#[cfg(feature = "catalog-postgres")]
#[derive(Debug)]
pub struct PostgresSuite {
    /// The URL with any `schema=` parameter removed.
    base: String,
    schemas: Vec<String>,
}

#[cfg(feature = "catalog-postgres")]
impl PostgresSuite {
    /// `None` (with a printed skip) when `PINKDB_TEST_POSTGRES_URL` is unset
    /// or empty.
    pub fn from_env(test: &str) -> Option<Self> {
        let Ok(url) = std::env::var("PINKDB_TEST_POSTGRES_URL") else {
            println!(
                "skip Postgres catalog test {test}: PINKDB_TEST_POSTGRES_URL is not set \
                 (start a local server and set it, e.g. \
                 postgres://postgres@localhost:55432/pinktest)"
            );
            return None;
        };
        let url = url.trim();
        if url.is_empty() {
            println!("skip Postgres catalog test {test}: PINKDB_TEST_POSTGRES_URL is empty");
            return None;
        }
        Some(Self {
            base: without_schema(url),
            schemas: Vec::new(),
        })
    }

    /// Print the pass line (the schemas are dropped by `Drop`).
    pub fn finish(&self, test: &str) {
        println!(
            "Postgres catalog test {test}: PASS ({} schema{})",
            self.schemas.len(),
            if self.schemas.len() == 1 { "" } else { "s" }
        );
    }

    /// Whether `catalog`'s schema exists in the test database.
    pub fn schema_exists(&self, catalog: &str) -> bool {
        let Some(schema) = catalog.rsplit("schema=").next() else {
            return false;
        };
        let Ok(mut config) = self.base.parse::<postgres::Config>() else {
            return false;
        };
        config
            .ssl_mode(postgres::config::SslMode::Disable)
            .application_name("pinkdb-lake-test-check");
        let Ok(mut client) = config.connect(postgres::NoTls) else {
            return false;
        };
        client
            .query_opt(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
                &[&schema],
            )
            .ok()
            .flatten()
            .is_some()
    }

    /// Run one statement in `catalog`'s schema (corruption tests).
    pub fn sql(&self, catalog: &str, sql: &str) {
        let (_, schema) = catalog
            .rsplit_once("schema=")
            .expect("the suite's catalog URL carries a schema");
        let mut config = self.base.parse::<postgres::Config>().expect("test URL");
        config
            .ssl_mode(postgres::config::SslMode::Disable)
            .application_name("pinkdb-lake-test-corrupt");
        let mut client = config.connect(postgres::NoTls).expect("test server");
        client
            .batch_execute(&format!("SET search_path TO \"{schema}\"; {sql}"))
            .expect("the corruption statement");
    }
}

#[cfg(feature = "catalog-postgres")]
impl Suite for PostgresSuite {
    fn catalog(&mut self, _dir: &Path, _name: &str) -> String {
        // Process-wide, not per-suite: tests in one binary run in parallel and
        // each suite instance starts its own counter, so a per-suite sequence
        // would hand two tests the same schema (and drop it early).
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let schema = format!("w96a_{}_{n}", std::process::id());
        self.schemas.push(schema.clone());
        let separator = if self.base.contains('?') { '&' } else { '?' };
        format!("{}{separator}schema={schema}", self.base)
    }
}

#[cfg(feature = "catalog-postgres")]
impl Drop for PostgresSuite {
    fn drop(&mut self) {
        if self.schemas.is_empty() {
            return;
        }
        let Ok(mut config) = self.base.parse::<postgres::Config>() else {
            eprintln!(
                "warning: could not parse PINKDB_TEST_POSTGRES_URL to drop schemas {:?}",
                self.schemas
            );
            return;
        };
        config
            .ssl_mode(postgres::config::SslMode::Disable)
            .application_name("pinkdb-lake-test-cleanup");
        let Ok(mut client) = config.connect(postgres::NoTls) else {
            eprintln!(
                "warning: could not connect to drop test schemas {:?}",
                self.schemas
            );
            return;
        };
        for schema in &self.schemas {
            let sql = format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
            if let Err(e) = client.batch_execute(&sql) {
                eprintln!("warning: dropping test schema {schema} failed: {e}");
            }
        }
    }
}

/// Strip an existing `schema=` parameter (the suite appends its own).
#[cfg(feature = "catalog-postgres")]
fn without_schema(url: &str) -> String {
    let (main, query) = url.split_once('?').unwrap_or((url, ""));
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.starts_with("schema="))
        .collect();
    if kept.is_empty() {
        main.to_string()
    } else {
        format!("{main}?{}", kept.join("&"))
    }
}

// ---------- fixtures ----------

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
fn saved_catalog(suite: &mut dyn Suite, dir: &Path, db: &PinkDb) -> (String, PathBuf, Lake) {
    let catalog = suite.catalog(dir, "catalog");
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(db).unwrap(), 2);
    (catalog, data, lake)
}

// ---------- shared tests ----------

/// Create/save/open through the catalog URL alone; the data location is
/// recorded in `pink_lake_meta`, and every metadata object lives in SQL.
pub fn create_save_open_round_trip(suite: &mut dyn Suite) {
    let dir = scratch("roundtrip");
    let (catalog, data, lake) = saved_catalog(suite, &dir, &store(3));
    assert_eq!(lake.current_version(), 2);
    // `Lake::path()` is the data location for a split lake, not the catalog.
    assert_eq!(lake.path(), data.as_path());
    drop(lake);

    // The data root has the marker + data files; no catalog/snapshot objects
    // are in the data location.
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
    let mut names: Vec<String> = std::fs::read_dir(&data)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["data".to_string(), "lake-data.json".to_string()]
    );

    // Open with only the catalog URL: the recorded data location is used.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(lake.current_version(), 2);
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 3);
    assert_eq!(lake.commits().unwrap(), vec![2]);

    // Opening an existing SQL catalog by URL alone works (the recorded data
    // location is used). A fresh SQLite catalog gets DuckLake's default data
    // location (`<catalog>.files/`), so `open_or_create` creates it there; a
    // Postgres catalog (wave 103 T2) still says a create needs `data`.
    let reopened = Lake::open_url(&catalog).unwrap();
    assert_eq!(reopened.current_version(), 2);
    let fresh = suite.catalog(&dir, "fresh");
    if fresh.starts_with("sqlite:") || fresh.starts_with("pink:") {
        let created = Lake::open_or_create_url(&fresh).unwrap();
        assert_eq!(created.current_version(), 1);
        let created_path = fresh
            .strip_prefix("sqlite:")
            .or_else(|| fresh.strip_prefix("pink:"))
            .unwrap();
        assert!(
            std::path::Path::new(&format!("{created_path}.files"))
                .join("lake-data.json")
                .is_file(),
            "the default data root sits next to the catalog"
        );
    } else {
        let err = Lake::open_or_create_url(&fresh).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("data location"), "{err}");
    }

    cleanup(&dir);
}

/// Two handles opened at the same version: the second rebases and the
/// published catalog is byte-identical to its snapshot row.
pub fn two_handles_conflict_then_rebase(suite: &mut dyn Suite) {
    let dir = scratch("rebase");
    let (catalog, _, lake) = saved_catalog(suite, &dir, &store(3));
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

    cleanup(&dir);
}

/// Eight writers on separate handles publish contiguous versions; the SQL
/// write lock serializes them and every loser rebases instead of losing work.
pub fn eight_threads_publish_contiguous_versions(suite: &mut dyn Suite) {
    let dir = scratch("threads");
    let (catalog, _, lake) = saved_catalog(suite, &dir, &store(3));
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

    cleanup(&dir);
}

/// Older versions load through the catalog exactly like the head.
pub fn time_travel_reads_older_versions(suite: &mut dyn Suite) {
    let dir = scratch("time-travel");
    let (catalog, _, mut lake) = saved_catalog(suite, &dir, &store(3));
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
pub fn expire_and_cleanup_on_a_split_lake(suite: &mut dyn Suite) {
    let dir = scratch("expire-cleanup");
    let (_catalog, data, mut lake) = saved_catalog(suite, &dir, &store(3));
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
        lake.load(Some(2)).is_err(),
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

/// CDC reads the change facts from the catalog rows.
pub fn cdc_reports_the_committed_rows(suite: &mut dyn Suite) {
    let dir = scratch("cdc");
    let (_catalog, _, mut lake) = saved_catalog(suite, &dir, &store(3));
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

/// Attach + `count(*)` over a SQL catalog with data on mock S3 sends **no**
/// request to S3: the metadata round trips are local SQL rows and the row
/// count comes from the manifest statistics.
#[cfg(feature = "s3")]
pub fn attach_and_count_issue_no_data_requests_over_s3(suite: &mut dyn Suite) {
    use crate::mock_s3::MockS3;

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
    let catalog = suite.catalog(&dir, "catalog");
    let mut lake = Lake::create_with_catalog(&catalog, "s3://catalog-bucket/split-data").unwrap();
    assert_eq!(lake.write_from_store(&store(4)).unwrap(), 2);
    drop(lake);

    let before = mock.requests().len();
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    let report = lake.attach(None).unwrap();
    assert_eq!(report.version, 2);
    // With the catalog in SQL and no data-file probes at attach (wave 96a,
    // like DuckLake), a statistics-only attach sends nothing to S3.
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
pub fn replication_state_round_trips(suite: &mut dyn Suite) {
    let dir = scratch("replication");
    let source = dir.join("source");
    pink_lake::save_store(&store(2), &source).unwrap();
    let (catalog, _, destination) = saved_catalog(suite, &dir, &store(1));
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

    cleanup(&dir);
}

/// A keyed SQL lake follows DuckLake's encryption model: metadata rows are
/// plaintext, every data/delete file is a `PNKLAKE1` container with its own
/// random 256-bit key in that row's `encryption_key` column, and a `pink:`
/// catalog database may itself be engine-encrypted (its file holds no
/// plaintext key bytes). There is no passphrase-derived lake key.
#[cfg(feature = "encryption")]
pub fn encrypted_catalog_rows_are_sealed(suite: &mut dyn Suite) {
    use pink_lake::EncryptionKey;

    let dir = scratch("encryption");
    let catalog = suite.catalog(&dir, "catalog");
    let data = dir.join("data-root");
    let key = EncryptionKey::from_passphrase("wave-103-t2-catalog");
    let mut lake =
        Lake::create_with_catalog_with_key(&catalog, data.to_str().unwrap(), key.clone()).unwrap();
    // One row per file: a real data file must exist so it gets its own key.
    lake.set_write_format("pink").unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(&store(3)).unwrap(), 2);
    drop(lake);

    // Every data/delete file row carries its own 256-bit key.
    let keys = pink_lake::catalog_encryption_keys(&catalog, Some(key.clone())).unwrap();
    assert!(!keys.is_empty(), "a data file must have a per-file key");
    for key_bytes in &keys {
        assert_eq!(key_bytes.len(), 32, "each key is 256 bits");
    }
    let unique: std::collections::BTreeSet<&Vec<u8>> = keys.iter().collect();
    assert_eq!(unique.len(), keys.len(), "each file gets a fresh key");

    // The file on disk is a PNKLAKE1 container, never the format's plaintext.
    let data_file = files(&data)
        .into_iter()
        .find(|path| path.extension().is_some_and(|ext| ext == "pink"))
        .expect("the write produced a .pink data file");
    let head = std::fs::read(&data_file).unwrap();
    assert!(head.starts_with(b"PNKLAKE1"), "{}", data_file.display());

    // A keyed `pink:` catalog database is itself engine-encrypted: no
    // plaintext per-file key survives in the file.
    if catalog.starts_with("pink:") {
        let path = catalog.trim_start_matches("pink:");
        // The durable database may still be all WAL (no checkpoint yet), so
        // both files are part of "the database file".
        let mut db = std::fs::read(path).unwrap_or_default();
        db.extend(std::fs::read(format!("{path}.wal")).unwrap_or_default());
        for key_bytes in &keys {
            assert!(
                !db.windows(key_bytes.len())
                    .any(|window| window == key_bytes),
                "the catalog file must not hold a plaintext per-file key"
            );
        }
    }

    let lake = Lake::open_with_catalog_with_key(&catalog, None, key.clone()).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 3);

    // A keyed `pink:` database rejects the wrong key at the engine; SQLite and
    // Postgres catalog databases are protected by their operator, and their
    // per-file keys live inside the catalog, so the user key never decrypts
    // data there.
    if catalog.starts_with("pink:") {
        let wrong = Lake::open_with_catalog_with_key(
            &catalog,
            None,
            EncryptionKey::from_passphrase("not-the-key"),
        )
        .unwrap_err();
        assert!(matches!(wrong, LakeError::WrongKey { .. }), "{wrong:?}");
    }

    cleanup(&dir);
}

/// An injected failure after the snapshot insert rolls the whole publication
/// back: no new snapshot row, no head movement, no new commit row. (The data
/// file written before the scope may remain; that is the documented crash
/// shape and `delete_orphaned_files` removes it.)
pub fn a_failure_after_the_snapshot_insert_rolls_back_the_publication(suite: &mut dyn Suite) {
    let dir = scratch("crash");
    let (catalog, _, mut lake) = saved_catalog(suite, &dir, &store(3));

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
    assert!(lake.load(Some(3)).is_err(), "no version 3 row");
    let reopened = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(reopened.current_version(), 2);
    assert_eq!(count(&reopened, "SELECT count(*) FROM t"), 3);

    cleanup(&dir);
}

/// Create refuses a data location that already belongs to a lake, and the
/// destructive maintenance paths verify `lake-data.json` before deleting.
pub fn data_location_ownership_is_enforced(suite: &mut dyn Suite) {
    let dir = scratch("ownership");
    let (_catalog, data, mut lake) = saved_catalog(suite, &dir, &store(2));

    // So is a data location that already holds this lake's data files.
    let err = Lake::create_with_catalog(&suite.catalog(&dir, "over-data"), data.to_str().unwrap())
        .unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("already holds lake data"), "{err}");

    // A data location owned by a split lake that has no data files yet is
    // refused too: the owner marker alone claims it, so a second catalog can
    // never overwrite the marker and then treat the first lake's files as
    // orphans.
    let fresh_data = dir.join("fresh-data");
    let _fresh =
        Lake::create_with_catalog(&suite.catalog(&dir, "fresh"), fresh_data.to_str().unwrap())
            .unwrap();
    let err = Lake::create_with_catalog(
        &suite.catalog(&dir, "over-fresh"),
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
    let other_catalog = suite.catalog(&dir, "other");
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

// ---------- lake-wide operations on a SQL catalog ----------
//
// These are the regressions behind the table-less head: every caller that
// walks the catalog must see the tables. Each test asserts the effect (rows,
// files, deletes, settings), not merely that the call returned `Ok`.

fn catalog_entry(catalog: &str, key: &str) -> pink_lake::TableInfo {
    let lake = Lake::open_with_catalog(catalog, None).unwrap();
    lake.table_info()
        .unwrap()
        .into_iter()
        .find(|info| info.table == key)
        .unwrap_or_else(|| panic!("table {key} is not in the catalog"))
}

fn entry_file_path(catalog: &str, key: &str) -> String {
    let lake = Lake::open_with_catalog(catalog, None).unwrap();
    lake.list_files(Some(key), None, false)
        .unwrap()
        .into_iter()
        .next()
        .expect("the table has a data file")
        .path
}

fn file_count(entry: &pink_lake::TableInfo) -> usize {
    entry.file_count as usize
}

fn live_inlined(entry: &pink_lake::TableInfo) -> usize {
    entry.inlined_rows as usize
}

pub fn flush_moves_inlined_rows_to_a_data_file(suite: &mut dyn Suite) {
    let dir = scratch("flush");
    let catalog = suite.catalog(&dir, "catalog");
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(10);
    assert_eq!(lake.write_from_store(&store(0)).unwrap(), 2);
    let mut commit = Commit::new();
    commit.append("main.t", sub_table(&[(1, "a"), (2, "b"), (3, "c")]));
    assert_eq!(lake.commit(&commit).unwrap().version, 3);

    let before = catalog_entry(&catalog, "main.t");
    assert_eq!(live_inlined(&before), 3, "{before:?}");
    let files_before = file_count(&before);

    let report = lake.flush_inlined_data(Some("main.t")).unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.tables, vec![("main.t".to_string(), 3)], "{report:?}");
    assert!(report.data_files >= 1, "{report:?}");
    assert_eq!(report.version, 4);
    drop(lake);

    let after = catalog_entry(&catalog, "main.t");
    assert_eq!(live_inlined(&after), 0, "flushed rows are ended: {after:?}");
    assert!(
        file_count(&after) > files_before,
        "the flush wrote a data file: {after:?}"
    );
    assert_eq!(after.inlined_history, 3);
    assert_eq!(after.inlined_rows, 0);

    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(lake.current_version(), 4);
    let mut db = lake.load(None).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 3);
    assert_eq!(
        rows(&mut db, "SELECT i, s FROM t ORDER BY i"),
        vec![
            vec![Value::Integer(1), Value::Varchar("a".into())],
            vec![Value::Integer(2), Value::Varchar("b".into())],
            vec![Value::Integer(3), Value::Varchar("c".into())],
        ]
    );

    cleanup(&dir);
}

pub fn merge_adjacent_files_reduces_the_file_count(suite: &mut dyn Suite) {
    let dir = scratch("merge");
    let catalog = suite.catalog(&dir, "catalog");
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(&store(0)).unwrap(), 2);
    for i in 0..4 {
        let mut commit = Commit::new();
        commit.append("main.t", sub_table(&[(i, "m")]));
        lake.commit(&commit).unwrap();
    }
    let before = catalog_entry(&catalog, "main.t");
    let files_before = file_count(&before);
    assert!(files_before >= 4, "{before:?}");

    let report = lake
        .merge_adjacent_files(Some("main.t"), &Default::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    assert!(report.files_processed() >= 2, "{report:?}");
    assert_eq!(report.files_created(), 1, "{report:?}");
    drop(lake);

    let after = catalog_entry(&catalog, "main.t");
    assert!(
        file_count(&after) < files_before,
        "files {} -> {}: {after:?}",
        files_before,
        file_count(&after)
    );
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 4);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i FROM t ORDER BY i"),
        vec![
            vec![Value::Integer(0)],
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    );

    cleanup(&dir);
}

pub fn rewrite_with_deletes_drops_the_delete_files(suite: &mut dyn Suite) {
    let dir = scratch("rewrite");
    let catalog = suite.catalog(&dir, "catalog");
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(&store(3)).unwrap(), 2);
    let file = entry_file_path(&catalog, "main.t");

    let mut commit = Commit::new();
    commit.delete_positions("main.t", &file, &[1]);
    assert_eq!(lake.commit(&commit).unwrap().version, 3);
    let with_delete = catalog_entry(&catalog, "main.t");
    assert_eq!(with_delete.delete_file_count, 1);

    let report = lake
        .rewrite_data_files(
            Some("main.t"),
            &pink_lake::RewriteSettings {
                delete_threshold: 0.0,
                max_compacted_files: None,
            },
        )
        .unwrap();
    assert!(report.committed, "{report:?}");
    drop(lake);

    let after = catalog_entry(&catalog, "main.t");
    assert_eq!(
        after.delete_file_count, 0,
        "the rewrite baked the deletes in"
    );
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 2);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i FROM t ORDER BY i"),
        vec![vec![Value::Integer(100)], vec![Value::Integer(102)]]
    );

    cleanup(&dir);
}

pub fn add_data_files_registers_an_existing_file(suite: &mut dyn Suite) {
    let dir = scratch("add-files");
    // A source lake writes a real data file for `main.t`.
    let source_dir = dir.join("source");
    std::fs::create_dir_all(&source_dir).unwrap();
    let source_catalog = suite.catalog(&source_dir, "source");
    let source_data = source_dir.join("data");
    let mut source =
        Lake::create_with_catalog(&source_catalog, source_data.to_str().unwrap()).unwrap();
    source.set_data_inlining_row_limit(0);
    assert_eq!(source.write_from_store(&store(2)).unwrap(), 2);
    drop(source);
    let rel = entry_file_path(&source_catalog, "main.t");
    let source_file = source_data.join(&rel);
    assert!(source_file.is_file(), "{}", source_file.display());

    // A target with the same table schema, then copy the file in.
    let catalog = suite.catalog(&dir, "target");
    let data = dir.join("target-data");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert_eq!(lake.write_from_store(&store(0)).unwrap(), 2);
    let before = catalog_entry(&catalog, "main.t");
    let target_file = data.join(&rel);
    std::fs::create_dir_all(target_file.parent().unwrap()).unwrap();
    std::fs::copy(&source_file, &target_file).unwrap();

    let report = lake
        .add_data_files_with_settings(
            "main.t",
            std::slice::from_ref(&rel),
            &CommitSettings::default(),
        )
        .unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.files.len(), 1, "{report:?}");
    assert_eq!(report.files[0].1, rel);
    assert_eq!(report.files[0].2, 2, "{report:?}");
    drop(lake);

    let after = catalog_entry(&catalog, "main.t");
    assert_eq!(
        file_count(&after),
        file_count(&before) + 1,
        "the registered file joins the entry: {after:?}"
    );
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 2);
    let mut db = lake.load(None).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i FROM t ORDER BY i"),
        vec![vec![Value::Integer(100)], vec![Value::Integer(101)]]
    );

    cleanup(&dir);
}

pub fn partition_and_inlining_settings_publish(suite: &mut dyn Suite) {
    let dir = scratch("settings");
    let catalog = suite.catalog(&dir, "catalog");
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    assert_eq!(lake.write_from_store(&store(2)).unwrap(), 2);

    let report = lake
        .set_partition_spec("main.t", &["i".to_string()], &CommitSettings::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.version, 3);
    let entry = catalog_entry(&catalog, "main.t");
    assert_eq!(entry.partition_by, vec!["i".to_string()]);

    let report = lake
        .set_inlining_limit("main.t", Some(5), &CommitSettings::default())
        .unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.version, 4);
    let entry = catalog_entry(&catalog, "main.t");
    assert_eq!(entry.inlined_limit, Some(5));
    assert_eq!(count(&lake, "SELECT count(*) FROM t"), 2);

    cleanup(&dir);
}
