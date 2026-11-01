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
//! A lake schema is a database schema: DuckLake's `s` becomes `database.s`,
//! in its own spelling, and lookups are case-insensitive like every catalog
//! name. A lake this module creates starts with `main` only. The engine's
//! schema list rides every commit (`LakeContent`/`publish`/`tables_at` key
//! tables by `(schema, table)`), so an empty `CREATE SCHEMA` is a
//! `ducklake_schema` row and a `DROP SCHEMA` ends it.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use pinkdb::{
    AttachedLake, DataTable, DataType, DatabaseOpener, LakeContent, LakeDatabase, LakeFunctionCall,
    LakeOpenRequest, PinkDbError, Value,
};

use crate::{
    CommitSettings, DuckLakeCall, EngineLease, FnArg, Lake, LakeError, LeaseError,
    WritableAttachment,
};

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
    if catalog.contains("://")
        || catalog.starts_with("pink:")
        || catalog.starts_with("sqlite:")
        || catalog.starts_with("postgres:")
    {
        catalog.to_string()
    } else {
        format!("pink:{catalog}")
    }
}

/// The engine's evaluated values as pink-lake's argument shape.
fn function_call(call: &LakeFunctionCall) -> DuckLakeCall {
    fn arg(value: &Value) -> FnArg {
        match value {
            Value::Null => FnArg::Null,
            Value::Boolean(v) => FnArg::Bool(*v),
            Value::Integer(v) => FnArg::Int(*v as i64),
            Value::Bigint(v) => FnArg::Int(*v),
            Value::Double(v) => FnArg::Double(*v),
            Value::Varchar(v) => FnArg::Text(v.clone()),
            Value::Date(v) => FnArg::Timestamp(*v as i64 * 86_400_000_000),
            Value::Timestamp(v) => FnArg::Timestamp(*v),
            Value::Decimal { value, scale, .. } => {
                FnArg::Double(*value as f64 / 10f64.powi(*scale as i32))
            }
            Value::Array(a) => match &a.values {
                pinkdb::array::ArrayElems::Ints(v) => FnArg::IntList(v.clone()),
                pinkdb::array::ArrayElems::Floats(v) => {
                    FnArg::TextList(v.iter().map(|x| x.to_string()).collect())
                }
                pinkdb::array::ArrayElems::Floats32(v) => {
                    FnArg::TextList(v.iter().map(|x| x.to_string()).collect())
                }
            },
            _ => FnArg::Null,
        }
    }
    DuckLakeCall {
        name: call.name.clone(),
        positional: call.positional.iter().map(arg).collect(),
        named: call
            .named
            .iter()
            .map(|(name, value)| (name.clone(), arg(value)))
            .collect(),
    }
}
fn credentials(request: &LakeOpenRequest) -> crate::LakeCredentials {
    #[cfg(feature = "s3")]
    let s3 = match (&request.s3, request.data.as_deref()) {
        (Some(secret), Some(data)) => s3_config(data, secret),
        _ => None,
    };
    crate::LakeCredentials {
        postgres_password: request
            .postgres_password
            .as_ref()
            .map(|password| password.expose().to_string()),
        #[cfg(feature = "s3")]
        s3,
    }
}

/// An `s3://` data location's bucket/prefix with a secret's credentials.
#[cfg(feature = "s3")]
fn s3_config(data: &str, secret: &pinkdb::secret::S3Secret) -> Option<crate::S3Config> {
    let Ok(crate::Location::S3 { bucket, prefix }) = crate::Location::parse(data) else {
        return None;
    };
    Some(pinkdb_httpfs::s3::config_from_secret(bucket, secret).with_prefix(prefix))
}

fn open_lake(
    request: &LakeOpenRequest,
    key: Option<&crate::EncryptionKey>,
) -> pinkdb::Result<Lake> {
    let catalog = catalog_url(&request.catalog);
    let credentials = credentials(request);
    let open = || match key {
        Some(key) => Lake::open_with_catalog_credentials_with_key(
            &catalog,
            request.data.as_deref(),
            key.clone(),
            &credentials,
        ),
        None => {
            Lake::open_with_catalog_credentials(&catalog, request.data.as_deref(), &credentials)
        }
    };
    let create = || match (&request.data, key) {
        (Some(data), None) => Lake::create_with_catalog_credentials(&catalog, data, &credentials),
        (None, None) => Lake::create_url(&catalog),
        (Some(data), Some(key)) => Lake::create_with_catalog_credentials_with_key(
            &catalog,
            data,
            key.clone(),
            &credentials,
        ),
        (None, Some(key)) => Lake::create_url_with_key(&catalog, key.clone()),
    };
    if request.create || request.create_if_missing {
        // CREATE_IF_NOT_EXISTS: an existing lake is opened, a missing one is
        // created. A force create still refuses to overwrite.
        if request.create_if_missing && !request.create {
            match open() {
                Ok(lake) => return Ok(lake),
                Err(LakeError::NotALake { .. }) => {}
                Err(e) => return Err(lake_error(e)),
            }
        }
        return create().map_err(|e| match e {
            LakeError::AlreadyALake { .. } => PinkDbError::Catalog(format!(
                "a lake already exists at {}: ATTACH it instead",
                crate::redact_location(&request.catalog)
            )),
            e => lake_error(e),
        });
    }
    match key {
        Some(key) => Lake::open_with_catalog_credentials_with_key(
            &catalog,
            request.data.as_deref(),
            key.clone(),
            &credentials,
        ),
        None => {
            Lake::open_with_catalog_credentials(&catalog, request.data.as_deref(), &credentials)
        }
    }
    .map_err(lake_error)
}

/// The format a lake's new tables are written in (wave 132b): DuckLake's
/// `data_file_format` global option when the catalog records one, else the
/// attach's own declaration — `ATTACH 'ducklake:…'` declares Parquet, which is
/// also what DuckDB writes into a catalog it creates. A pinkdb lake database
/// declares neither and keeps the build default.
fn declared_format(lake: &Lake, request: &LakeOpenRequest) -> Option<String> {
    let recorded = lake
        .store
        .metadata_options()
        .ok()?
        .into_iter()
        .find(|row| row.scope.is_none() && row.key.eq_ignore_ascii_case("data_file_format"))
        .map(|row| row.value);
    recorded.or_else(|| request.data_file_format.clone())
}

/// Whether a `schema.table` catalog key names `(schema, table)`.
fn key_matches(key: &str, schema: &str, table: &str) -> bool {
    match key.split_once('.') {
        Some((s, t)) => s.eq_ignore_ascii_case(schema) && t.eq_ignore_ascii_case(table),
        None => false,
    }
}

/// Every table of the lake at `version`, materialized, by schema and table.
fn tables_at(lake: &Lake, version: u64) -> pinkdb::Result<Vec<(String, String, Arc<DataTable>)>> {
    let report = lake.attach(Some(version)).map_err(lake_error)?;
    let reader = report.lake;
    let mut out = Vec::new();
    for t in reader.tables() {
        out.push((t.db.clone(), t.table.clone(), reader.load(&t.db, &t.table)?));
    }
    Ok(out)
}

impl DatabaseOpener for LakeOpener {
    fn open(&self, request: &LakeOpenRequest) -> pinkdb::Result<Arc<dyn LakeDatabase>> {
        let mut lake = open_lake(request, self.key.as_ref())?;
        // Wave 132b: a catalog another writer made (stock DuckDB's, always
        // Parquet) keeps its data-file format, so a table pinkdb creates in it
        // is readable there too.
        if !request.read_only
            && let Some(format) = declared_format(&lake, request)
        {
            lake.set_data_file_format(&format);
        }
        let location = crate::redact_location(&request.catalog);
        if request.read_only {
            let report = lake.attach(request.version).map_err(lake_error)?;
            let schemas = lake.schemas().map_err(lake_error)?;
            return Ok(Arc::new(ReadOnlyLake {
                location,
                version: report.version,
                schemas,
                reader: report.lake,
                lake: Mutex::new(lake),
            }));
        }
        let lease = acquire_lease(&lake, request.startup)?;
        let catalog_limit = catalog_inlining_limit(&lake);
        let mut attachment = WritableAttachment::attach(lake, None).map_err(lake_error)?;
        if let Some(limit) = request
            .data_inlining_row_limit
            .or(catalog_limit)
            .or_else(inlining_limit)
        {
            attachment.set_data_inlining_row_limit(limit);
        }
        let (mut lake, version, base) = attachment.into_parts();
        apply_table_inlining_limits(&mut lake);
        let image = lake.engine_catalog().map_err(lake_error)?.map(|(_, i)| i);
        WritableLake::start(location, lake, version, base, image, lease)
    }
}

/// `PINKDB_LAKE_INLINING_ROW_LIMIT`: a writable lake database's default
/// `data_inlining_row_limit` (DuckLake's
/// `ducklake_default_data_inlining_row_limit`; 0 disables inlining so every
/// commit writes data files). Unset keeps the handle default; a table's
/// persisted `.lake inlining` limit always wins.
/// The catalog's global `data_inlining_row_limit` option, if it has one: a
/// lake written by DuckDB (or by a previous `ducklake_set_option`) keeps the
/// limit there, and a fresh handle must apply it.
fn catalog_inlining_limit(lake: &Lake) -> Option<u64> {
    let row = lake
        .store
        .metadata_options()
        .ok()?
        .into_iter()
        .find(|row| {
            row.scope.is_none() && row.key.eq_ignore_ascii_case("data_inlining_row_limit")
        })?;
    row.value.trim().parse().ok()
}

/// Per-table `data_inlining_row_limit` options written by DuckLake's
/// `set_option` live only in `ducklake_metadata`; a fresh handle applies them
/// to the tables they name.
fn apply_table_inlining_limits(lake: &mut Lake) {
    let Ok(options) = lake.store.metadata_options() else {
        return;
    };
    let Ok(tables) = lake.store.catalog_table_ids() else {
        return;
    };
    for row in options {
        if row.scope.as_deref() != Some("table")
            || !row.key.eq_ignore_ascii_case("data_inlining_row_limit")
        {
            continue;
        }
        let Some(id) = row.scope_id else {
            continue;
        };
        let Some(identity) = tables.iter().find(|table| table.table_id == id) else {
            continue;
        };
        if let Ok(limit) = row.value.trim().parse() {
            lake.set_table_inlining_limit(&identity.key, Some(limit));
        }
    }
}

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
    schemas: Vec<String>,
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
    fn schemas(&self) -> pinkdb::Result<Vec<String>> {
        Ok(self.schemas.clone())
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
    fn publish(
        &self,
        _image: &[u8],
        _schemas: &[String],
        _tables: &[(String, String, Arc<DataTable>)],
    ) -> pinkdb::Result<u64> {
        Err(PinkDbError::Execution(format!(
            "lake {} is attached read-only",
            self.location
        )))
    }
    fn tables_at(&self, version: u64) -> pinkdb::Result<Vec<(String, String, Arc<DataTable>)>> {
        tables_at(
            &self.lake.lock().unwrap_or_else(|p| p.into_inner()),
            version,
        )
    }
    fn function_schema(
        &self,
        call: &LakeFunctionCall,
    ) -> pinkdb::Result<Option<Vec<(String, DataType)>>> {
        if !crate::functions::is_known(&call.name) {
            return Ok(None);
        }
        let lake = self.lake.lock().unwrap_or_else(|p| p.into_inner());
        crate::functions::output_schema(&lake, &function_call(call)).map_err(lake_error)
    }
    fn function_mutates(&self, name: &str) -> bool {
        crate::functions::is_mutating(name)
    }
    fn call_read(&self, call: &LakeFunctionCall) -> pinkdb::Result<DataTable> {
        let lake = self.lake.lock().unwrap_or_else(|p| p.into_inner());
        crate::functions::call_read(&lake, &function_call(call)).map_err(lake_error)
    }
}

/// A writable lake database: the lease, its heartbeat, and the base the next
/// commit diffs against.
struct WritableLake {
    location: String,
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
    fn schemas(&self) -> pinkdb::Result<Vec<String>> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.lake.schemas().map_err(lake_error)
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
                .map(|(key, table)| match key.split_once('.') {
                    Some((schema, name)) => {
                        (schema.to_string(), name.to_string(), Arc::clone(table))
                    }
                    None => ("main".to_string(), key.clone(), Arc::clone(table)),
                })
                .collect(),
        })
    }
    fn publish(
        &self,
        image: &[u8],
        schemas: &[String],
        tables: &[(String, String, Arc<DataTable>)],
    ) -> pinkdb::Result<u64> {
        // A server whose lease was taken over must not publish at all.
        self.lease.verify().map_err(lease_error)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // An existing catalog key wins over the engine's spelling, so a table
        // whose name differs only in case is updated, not dropped and
        // re-created.
        let current: Vec<(String, Arc<DataTable>)> = tables
            .iter()
            .map(|(schema, name, table)| {
                let key = state
                    .base
                    .iter()
                    .map(|(key, _)| key)
                    .find(|key| key_matches(key, schema, name))
                    .cloned()
                    .unwrap_or_else(|| format!("{schema}.{name}"));
                (key, Arc::clone(table))
            })
            .collect();
        // Only a schema list that differs from the lake's is worth a catalog
        // write; the comparison is case-insensitive, like the lookups.
        let live = state.lake.schemas().map_err(lake_error)?;
        let same_schemas = live.len() == schemas.len()
            && schemas
                .iter()
                .all(|s| live.iter().any(|l| l.eq_ignore_ascii_case(s)));
        let WritableState {
            lake,
            version,
            base,
        } = &mut *state;
        lake.set_engine_catalog(image.to_vec());
        let report = lake
            .commit_from_tables(
                *version,
                base,
                &current,
                (!same_schemas).then_some(schemas),
                &CommitSettings::default(),
            )
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
    fn tables_at(&self, version: u64) -> pinkdb::Result<Vec<(String, String, Arc<DataTable>)>> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        tables_at(&state.lake, version)
    }
    fn function_schema(
        &self,
        call: &LakeFunctionCall,
    ) -> pinkdb::Result<Option<Vec<(String, DataType)>>> {
        if !crate::functions::is_known(&call.name) {
            return Ok(None);
        }
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        crate::functions::output_schema(&state.lake, &function_call(call)).map_err(lake_error)
    }
    fn function_mutates(&self, name: &str) -> bool {
        crate::functions::is_mutating(name)
    }
    fn call_read(&self, call: &LakeFunctionCall) -> pinkdb::Result<DataTable> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        crate::functions::call_read(&state.lake, &function_call(call)).map_err(lake_error)
    }
    fn call_write(&self, call: &LakeFunctionCall) -> pinkdb::Result<DataTable> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let before = state.lake.current_version();
        let table = crate::functions::call_write(&mut state.lake, &function_call(call))
            .map_err(lake_error)?;
        let after = state.lake.current_version();
        if after != before {
            state.version = after;
            state.base = tables_at(&state.lake, after)?
                .into_iter()
                .map(|(schema, name, table)| (format!("{schema}.{name}"), table))
                .collect();
        }
        Ok(table)
    }
}
