//! `--catalog` mode (wave 96b): a stateless server whose store lives in a
//! pink-lake lake.
//!
//! The lake catalog holds the engine's metadata as one image
//! ([`pinkdb::catalog_image`]) and the lake's data files hold the rows; the
//! server keeps no durable state of its own. Every commit — one autocommit
//! statement, or one `COMMIT` — publishes both through the engine's commit
//! hook ([`pinkdb::PinkDb::set_commit_hook`]), in the one catalog transaction
//! pink-lake's commit loop already performs. A restart on another machine
//! therefore needs only the catalog URL and the data location.
//!
//! # Phase 1: one server (wave 96b)
//!
//! One server owns the lake at a time, enforced by a lease in the catalog
//! ([`pink_lake::EngineLease`]): a second server pointed at the same catalog
//! is refused with an `already served by …` error, and a server whose lease
//! was taken over refuses to commit. Several servers on one catalog — cache
//! invalidation between them, leases with takeover — is phase 2; see
//! `docs/wave-96b-plan.md`.
//!
//! # Local state
//!
//! None: the image carries databases, tables (schema, constraints, defaults,
//! indexes), views, macros, materialized views, procedures, triggers, RBAC and
//! the loaded extension paths, and the lake carries the rows. The engine's WAL
//! stays what it is for `--file` mode — an in-memory WAL plus the periodic
//! checkpoint — but with no checkpoint file in this mode the catalog version
//! is the durable unit: a crash between two commits loses nothing, because
//! every commit was published before the statement returned.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use pink_lake::{CommitSettings, EngineLease, Lake, WritableAttachment};
use pinkdb::{DataTable, Database, PinkDb};

/// How often the lease heartbeat is refreshed. The takeover window is a
/// multiple of this (see [`pink_lake::EngineLease`]), so an idle server keeps
/// its lease and a crashed one frees it.
const HEARTBEAT: Duration = Duration::from_millis(1000);

/// A catalog-backed store: a [`Database`] plus the lake its commits publish to.
pub struct CatalogStore {
    db: Database,
    state: Arc<Mutex<LakeState>>,
    stop: Arc<AtomicBool>,
    heartbeat: Option<JoinHandle<()>>,
}

/// The published lake plus the base its next commit is computed against.
struct LakeState {
    lake: Lake,
    /// Version of `base`: the lake content the engine was loaded from or last
    /// published, so the next commit diffs against what the catalog holds.
    version: u64,
    /// The lake's tables at `version`, keyed as the catalog keys them.
    base: Vec<(String, Arc<DataTable>)>,
}

impl CatalogStore {
    /// Open the store at `catalog` over `data`, or create it when the catalog
    /// holds no lake yet. `owner` names this server in the catalog lease
    /// (`host:pid`). Fails with an `already served by …` message when another
    /// server holds the lease (phase 1: one server per catalog).
    pub fn open(
        catalog: &str,
        data: &str,
        owner: &str,
    ) -> std::result::Result<CatalogStore, String> {
        let lake = open_lake(catalog, data)?;
        let lease = Arc::new(EngineLease::acquire(&lake, owner).map_err(|err| err.to_string())?);
        let attachment = WritableAttachment::attach(lake, None).map_err(|err| err.to_string())?;
        let (lake, version, base) = attachment.into_parts();
        let image = lake.engine_catalog().map_err(|err| err.to_string())?;
        let mut store = match image {
            Some((_, image)) => {
                let rows: Vec<pinkdb::catalog_image::LakeRows> = base
                    .iter()
                    .map(|(key, table)| {
                        let (dbname, name) = split_key(key);
                        (dbname, name, Arc::clone(table))
                    })
                    .collect();
                PinkDb::load_catalog_image(&image, &rows).map_err(|err| err.to_string())?
            }
            None if base.is_empty() => PinkDb::new(),
            None => {
                return Err(format!(
                    "the lake at {} holds tables but no engine catalog image: it was \
                     not written by a `--catalog` server",
                    lake.path().display()
                ));
            }
        };
        let state = Arc::new(Mutex::new(LakeState {
            lake,
            version,
            base,
        }));
        let publish = Arc::clone(&state);
        let checked = Arc::clone(&lease);
        store.set_commit_hook(move |store| publish_state(&publish, &checked, store));
        let db = Database::from_store(store);

        let stop = Arc::new(AtomicBool::new(false));
        let heartbeat = {
            let stop = Arc::clone(&stop);
            let lease = Arc::clone(&lease);
            std::thread::Builder::new()
                .name("pinkdb-catalog-lease".to_string())
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(HEARTBEAT);
                        // A stolen lease is reported by the next commit; the
                        // heartbeat only stops beating.
                        if lease.heartbeat().is_err() {
                            break;
                        }
                    }
                })
                .map_err(|err| format!("cannot start the catalog lease heartbeat: {err}"))?
        };
        Ok(CatalogStore {
            db,
            state,
            stop,
            heartbeat: Some(heartbeat),
        })
    }

    /// The store to serve.
    pub fn database(&self) -> Database {
        self.db.clone()
    }

    /// The lake catalog's version this server last published (diagnostics).
    pub fn version(&self) -> u64 {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).version
    }
}

impl Drop for CatalogStore {
    /// Stop the heartbeat before the lease drops; every commit is already
    /// published, so there is nothing else to flush.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(heartbeat) = self.heartbeat.take() {
            let _ = heartbeat.join();
        }
    }
}

/// Publish one commit: the engine catalog image and this commit's table data
/// as one lake version.
fn publish_state(
    state: &Mutex<LakeState>,
    lease: &EngineLease,
    store: &PinkDb,
) -> pinkdb::Result<()> {
    let image = store.to_catalog_image()?;
    // Check the lease before taking the state lock: a server whose lease was
    // taken over must not publish at all.
    lease
        .verify()
        .map_err(|err| pinkdb::PinkDbError::Execution(err.to_string()))?;
    let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
    let current = current_tables(store, &state.base)?;
    let LakeState {
        lake,
        version,
        base,
    } = &mut *state;
    lake.set_engine_catalog(image.clone());
    let report = lake
        .commit_from_tables(*version, base, &current, &CommitSettings::default())
        .map_err(|err| pinkdb::PinkDbError::Execution(err.to_string()))?;
    if report.committed {
        *version = report.version;
    } else {
        // Nothing but metadata changed (a view, a grant, a rename of nothing):
        // publish the image on its own, still one catalog transaction.
        lake.publish_engine_catalog(&image)
            .map_err(|err| pinkdb::PinkDbError::Execution(err.to_string()))?;
        *version = lake.current_version();
    }
    *base = current;
    Ok(())
}

/// The store's tables, keyed the way the catalog keys them: an existing lake
/// key wins over the engine's spelling, so a table whose display name differs
/// only in case is updated, not dropped and re-created.
fn current_tables(
    store: &PinkDb,
    base: &[(String, Arc<DataTable>)],
) -> pinkdb::Result<Vec<(String, Arc<DataTable>)>> {
    let mut tables = Vec::new();
    for dbname in store.list_databases() {
        for name in store.list_tables_in(&dbname) {
            let table = store.table_in(&dbname, &name)?;
            let display = format!("{dbname}.{name}");
            let key = base
                .iter()
                .map(|(key, _)| key)
                .find(|key| key.eq_ignore_ascii_case(&display))
                .cloned()
                .unwrap_or(display);
            tables.push((key, Arc::new(table.clone())));
        }
    }
    Ok(tables)
}

/// Split a catalog table key into its database and table parts (the catalog's
/// key format is `db.table`, as [`pink_lake::WritableAttachment::tables`]
/// reports it).
fn split_key(key: &str) -> (String, String) {
    match key.split_once('.') {
        Some((dbname, name)) => (dbname.to_string(), name.to_string()),
        None => (key.to_string(), String::new()),
    }
}

/// Open the lake that `data` holds under `catalog`, creating it when the
/// catalog is empty.
fn open_lake(catalog: &str, data: &str) -> std::result::Result<Lake, String> {
    if let Some(rest) = data.strip_prefix("s3://") {
        return open_lake_s3(catalog, rest);
    }
    let opened = Lake::open_with_catalog(catalog, Some(data));
    match opened {
        Ok(lake) => Ok(lake),
        Err(pink_lake::LakeError::NotALake { .. }) => {
            Ok(Lake::create_with_catalog(catalog, data).map_err(|err| err.to_string())?)
        }
        Err(pink_lake::LakeError::AlreadyALake { .. }) => {
            Ok(Lake::open_with_catalog(catalog, Some(data)).map_err(|err| err.to_string())?)
        }
        Err(err) => Err(err.to_string()),
    }
}

/// S3 data: `data` is `s3://bucket/prefix`, and the credentials come from the
/// environment exactly as the lake's own S3 tests and the CLI expect.
#[cfg(feature = "lake-s3")]
fn open_lake_s3(catalog: &str, rest: &str) -> std::result::Result<Lake, String> {
    let (bucket, prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket, prefix),
        None => (rest, ""),
    };
    let config = pink_lake::S3Config::from_env(bucket).with_prefix(prefix);
    match Lake::open_with_catalog_s3(catalog, config.clone()) {
        Ok(lake) => Ok(lake),
        Err(pink_lake::LakeError::NotALake { .. }) => {
            Lake::create_with_catalog_s3(catalog, config).map_err(|err| err.to_string())
        }
        Err(err) => Err(err.to_string()),
    }
}

#[cfg(not(feature = "lake-s3"))]
fn open_lake_s3(_catalog: &str, _rest: &str) -> std::result::Result<Lake, String> {
    Err(
        "this build has no S3 data support: rebuild pinkdb-server with \
         --features lake-s3"
            .to_string(),
    )
}
