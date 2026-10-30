//! Lake databases (wave 124): pink-lake's [`pinkdb::DatabaseOpener`].
//!
//! The engine knows a database's storage but not how to open a lake; a host
//! calls [`register`] once before it opens a store, and the engine asks this
//! opener for every `CREATE DATABASE … (TYPE LAKE)`, `ATTACH` and reopen.
//!
//! - A **read-only** lake is a [`Lake::attach`] at the latest (or the pinned)
//!   version: lazy, statistics-first reads. No lease is taken; any number of
//!   readers share a lake.
//! - A **writable** lake takes the catalog's [`EngineLease`] (refreshed by a
//!   heartbeat thread, released on drop), materializes the head version, and
//!   publishes each commit with [`Lake::commit_from_tables`] plus the
//!   database's engine image — one lake version per commit, exactly what the
//!   stateless server of wave 96b did for its whole store, now per database.
//!
//! A lake exposes one schema: its tables must all live in one (`main` for a
//! lake this module creates). More than one is refused.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use pinkdb::{
    AttachedLake, DataTable, DatabaseOpener, LakeContent, LakeDatabase, LakeOpenRequest,
    PinkDbError,
};

use crate::{CommitSettings, EngineLease, Lake, LakeError, LeaseError, WritableAttachment};

/// The schema a lake this module creates keeps its tables in.
const SCHEMA: &str = "main";

/// How often a writable lake's lease heartbeat is refreshed.
const HEARTBEAT: Duration = Duration::from_millis(1000);

/// pink-lake's opener; [`register`] installs it.
#[derive(Default)]
pub struct LakeOpener {
    /// The at-rest key every lake this process opens or creates uses (the
    /// CLI's `PINKDB_KEY`); `None` = plaintext lakes.
    key: Option<crate::EncryptionKey>,
}

/// Register pink-lake as the engine's lake opener (idempotent). Call it
/// before opening a store whose checkpoint or WAL names lake databases.
pub fn register() {
    register_with_key(None);
}

/// [`register`] with an at-rest key for every lake the process opens (a
/// keyed open of a plaintext lake is refused, like every keyed lake open).
pub fn register_with_key(key: Option<crate::EncryptionKey>) {
    pinkdb::register_database_opener(Arc::new(LakeOpener { key }));
}

fn lake_error(error: LakeError) -> PinkDbError {
    PinkDbError::Execution(error.to_string())
}

fn lease_error(error: LeaseError) -> PinkDbError {
    PinkDbError::Execution(error.to_string())
}

/// A catalog location as `Lake::create_with_catalog` wants it: a bare path
/// is a `pink:` catalog file.
fn catalog_url(catalog: &str) -> String {
    if catalog.contains("://") || catalog.starts_with("pink:") || catalog.starts_with("sqlite:") {
        catalog.to_string()
    } else {
        format!("pink:{catalog}")
    }
}

fn open_lake(
    request: &LakeOpenRequest,
    key: Option<&crate::EncryptionKey>,
) -> pinkdb::Result<Lake> {
    let catalog = catalog_url(&request.catalog);
    if request.create {
        let created = match (&request.data, key) {
            (Some(data), None) => Lake::create_with_catalog(&catalog, data),
            (None, None) => Lake::create_url(&catalog),
            (Some(data), Some(key)) => {
                Lake::create_with_catalog_with_key(&catalog, data, key.clone())
            }
            (None, Some(key)) => Lake::create_url_with_key(&catalog, key.clone()),
        };
        return created.map_err(|e| match e {
            LakeError::AlreadyALake { .. } => PinkDbError::Catalog(format!(
                "a lake already exists at {}: ATTACH it instead",
                crate::redact_location(&request.catalog)
            )),
            e => lake_error(e),
        });
    }
    match key {
        Some(key) => {
            Lake::open_with_catalog_with_key(&catalog, request.data.as_deref(), key.clone())
        }
        None => Lake::open_with_catalog(&catalog, request.data.as_deref()),
    }
    .map_err(lake_error)
}

/// The one schema of a lake's catalog keys (`schema.table`), refusing more.
fn one_schema<'a>(
    location: &str,
    keys: impl Iterator<Item = &'a String>,
) -> pinkdb::Result<String> {
    let mut schemas: BTreeSet<String> = BTreeSet::new();
    for key in keys {
        if let Some((schema, _)) = key.split_once('.') {
            schemas.insert(schema.to_string());
        }
    }
    if schemas.len() > 1 {
        return Err(pinkdb::lake_db::multi_schema_error(
            location,
            &schemas.into_iter().collect::<Vec<_>>(),
        ));
    }
    Ok(schemas.pop_first().unwrap_or_else(|| SCHEMA.to_string()))
}

/// The table name of a `schema.table` key.
fn table_of(key: &str) -> &str {
    key.split_once('.').map(|(_, t)| t).unwrap_or(key)
}

/// Every table of the lake at `version`, materialized, by table name.
fn tables_at(lake: &Lake, version: u64) -> pinkdb::Result<Vec<(String, Arc<DataTable>)>> {
    let report = lake.attach(Some(version)).map_err(lake_error)?;
    let reader = report.lake;
    let mut out = Vec::new();
    for t in reader.tables() {
        out.push((t.table.clone(), reader.load(&t.db, &t.table)?));
    }
    Ok(out)
}

impl DatabaseOpener for LakeOpener {
    fn open(&self, request: &LakeOpenRequest) -> pinkdb::Result<Arc<dyn LakeDatabase>> {
        let lake = open_lake(request, self.key.as_ref())?;
        let location = crate::redact_location(&request.catalog);
        if request.read_only {
            let report = lake.attach(request.version).map_err(lake_error)?;
            let keys: Vec<String> = report
                .lake
                .tables()
                .iter()
                .map(|t| format!("{}.{}", t.db, t.table))
                .collect();
            one_schema(&location, keys.iter())?;
            return Ok(Arc::new(ReadOnlyLake {
                location,
                version: report.version,
                reader: report.lake,
                lake: Mutex::new(lake),
            }));
        }
        let lease = acquire_lease(&lake, request.startup)?;
        let mut attachment = WritableAttachment::attach(lake, None).map_err(lake_error)?;
        if let Some(limit) = inlining_limit() {
            attachment.set_data_inlining_row_limit(limit);
        }
        let (lake, version, base) = attachment.into_parts();
        let schema = one_schema(&location, base.iter().map(|(k, _)| k))?;
        let image = lake.engine_catalog().map_err(lake_error)?.map(|(_, i)| i);
        WritableLake::start(location, lake, version, base, image, schema, lease)
    }
}

/// `PINKDB_LAKE_INLINING_ROW_LIMIT`: a writable lake database's default
/// `data_inlining_row_limit` (DuckLake's
/// `ducklake_default_data_inlining_row_limit`; 0 disables inlining so every
/// commit writes data files). Unset keeps the handle default; a table's
/// persisted `.lake inlining` limit always wins.
fn inlining_limit() -> Option<u64> {
    let raw = std::env::var("PINKDB_LAKE_INLINING_ROW_LIMIT").ok()?;
    raw.trim().parse::<u64>().ok()
}

/// Take a writable lake's lease. While the store is opening, a lease a
/// crashed predecessor left behind is waited out (it goes stale after
/// [`crate::engine::LEASE_TIMEOUT`]) instead of leaving the lake unavailable.
fn acquire_lease(lake: &Lake, startup: bool) -> pinkdb::Result<Arc<EngineLease>> {
    let owner = lease_owner();
    match EngineLease::acquire(lake, &owner) {
        Ok(lease) => Ok(Arc::new(lease)),
        Err(LeaseError::Held { age_ms, .. }) if startup => {
            let timeout = crate::engine::LEASE_TIMEOUT.as_millis() as u64;
            let wait = timeout.saturating_sub(age_ms) + 250;
            std::thread::sleep(Duration::from_millis(wait));
            EngineLease::acquire(lake, &owner)
                .map(Arc::new)
                .map_err(lease_error)
        }
        Err(e) => Err(lease_error(e)),
    }
}

/// `host:pid:n`: unique per open, so two writable handles on one lake in one
/// process are refused by the lease like two processes are.
fn lease_owner() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty() && !h.contains(['\t', '\n']))
        .unwrap_or_else(|| "localhost".to_string());
    format!(
        "{host}:{}:{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// A read-only lake database.
#[derive(Debug)]
struct ReadOnlyLake {
    location: String,
    version: u64,
    reader: Arc<dyn AttachedLake>,
    /// The handle, for older versions (a WAL record's pinned read).
    lake: Mutex<Lake>,
}

impl LakeDatabase for ReadOnlyLake {
    fn location(&self) -> String {
        self.location.clone()
    }
    fn read_only(&self) -> bool {
        true
    }
    fn version(&self) -> u64 {
        self.version
    }
    fn reader(&self) -> pinkdb::Result<Arc<dyn AttachedLake>> {
        Ok(Arc::clone(&self.reader))
    }
    fn content(&self) -> pinkdb::Result<LakeContent> {
        Err(PinkDbError::Execution(format!(
            "lake {} is attached read-only",
            self.location
        )))
    }
    fn publish(&self, _image: &[u8], _tables: &[(String, Arc<DataTable>)]) -> pinkdb::Result<u64> {
        Err(PinkDbError::Execution(format!(
            "lake {} is attached read-only",
            self.location
        )))
    }
    fn tables_at(&self, version: u64) -> pinkdb::Result<Vec<(String, Arc<DataTable>)>> {
        tables_at(
            &self.lake.lock().unwrap_or_else(|p| p.into_inner()),
            version,
        )
    }
}

/// A writable lake database: the lease, its heartbeat, and the base the next
/// commit diffs against.
struct WritableLake {
    location: String,
    schema: String,
    state: Arc<Mutex<WritableState>>,
    /// The image the lake carried when it was opened (taken by `content`).
    image: Mutex<Option<Vec<u8>>>,
    lease: Arc<EngineLease>,
    stop: Arc<AtomicBool>,
    heartbeat: Mutex<Option<JoinHandle<()>>>,
}

struct WritableState {
    lake: Lake,
    /// The version `base` is: the last one opened or published.
    version: u64,
    /// The lake's tables at `version`, keyed as the catalog keys them.
    base: Vec<(String, Arc<DataTable>)>,
}

impl std::fmt::Debug for WritableLake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WritableLake")
            .field("location", &self.location)
            .field("schema", &self.schema)
            .finish()
    }
}

impl WritableLake {
    fn start(
        location: String,
        lake: Lake,
        version: u64,
        base: Vec<(String, Arc<DataTable>)>,
        image: Option<Vec<u8>>,
        schema: String,
        lease: Arc<EngineLease>,
    ) -> pinkdb::Result<Arc<dyn LakeDatabase>> {
        let stop = Arc::new(AtomicBool::new(false));
        let heartbeat = {
            let stop = Arc::clone(&stop);
            let lease = Arc::clone(&lease);
            std::thread::Builder::new()
                .name("pinkdb-lake-lease".to_string())
                .spawn(move || {
                    let tick = Duration::from_millis(100);
                    let mut since = Duration::ZERO;
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(tick);
                        since += tick;
                        if since >= HEARTBEAT {
                            since = Duration::ZERO;
                            // A stolen lease is reported by the next publish;
                            // the heartbeat only stops beating.
                            if lease.heartbeat().is_err() {
                                break;
                            }
                        }
                    }
                })
                .map_err(|e| {
                    PinkDbError::Execution(format!("cannot start the lake lease heartbeat: {e}"))
                })?
        };
        Ok(Arc::new(WritableLake {
            location,
            schema,
            state: Arc::new(Mutex::new(WritableState {
                lake,
                version,
                base,
            })),
            image: Mutex::new(image),
            lease,
            stop,
            heartbeat: Mutex::new(Some(heartbeat)),
        }))
    }
}

impl Drop for WritableLake {
    /// Stop the heartbeat; the lease is released when its last holder drops.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(heartbeat) = self
            .heartbeat
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let _ = heartbeat.join();
        }
    }
}

impl LakeDatabase for WritableLake {
    fn location(&self) -> String {
        self.location.clone()
    }
    fn read_only(&self) -> bool {
        false
    }
    fn version(&self) -> u64 {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).version
    }
    fn reader(&self) -> pinkdb::Result<Arc<dyn AttachedLake>> {
        Err(PinkDbError::Execution(format!(
            "lake {} is writable: its tables live in the store",
            self.location
        )))
    }
    fn content(&self) -> pinkdb::Result<LakeContent> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        Ok(LakeContent {
            image: self.image.lock().unwrap_or_else(|p| p.into_inner()).take(),
            tables: state
                .base
                .iter()
                .map(|(key, table)| (table_of(key).to_string(), Arc::clone(table)))
                .collect(),
        })
    }
    fn publish(&self, image: &[u8], tables: &[(String, Arc<DataTable>)]) -> pinkdb::Result<u64> {
        // A server whose lease was taken over must not publish at all.
        self.lease.verify().map_err(lease_error)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // An existing catalog key wins over the engine's spelling, so a table
        // whose name differs only in case is updated, not dropped and
        // re-created.
        let current: Vec<(String, Arc<DataTable>)> = tables
            .iter()
            .map(|(name, table)| {
                let key = state
                    .base
                    .iter()
                    .map(|(key, _)| key)
                    .find(|key| table_of(key).eq_ignore_ascii_case(name))
                    .cloned()
                    .unwrap_or_else(|| format!("{}.{name}", self.schema));
                (key, Arc::clone(table))
            })
            .collect();
        let WritableState {
            lake,
            version,
            base,
        } = &mut *state;
        lake.set_engine_catalog(image.to_vec());
        let report = lake
            .commit_from_tables(*version, base, &current, &CommitSettings::default())
            .map_err(lake_error)?;
        if report.committed {
            *version = report.version;
        } else {
            // Only metadata changed (a macro, an index, a constraint): publish
            // the image on its own.
            lake.publish_engine_catalog(image).map_err(lake_error)?;
            *version = lake.current_version();
        }
        *base = current;
        Ok(*version)
    }
    fn tables_at(&self, version: u64) -> pinkdb::Result<Vec<(String, Arc<DataTable>)>> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        tables_at(&state.lake, version)
    }
}
