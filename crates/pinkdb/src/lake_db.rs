//! Lake databases (wave 124): a database whose rows and table metadata live
//! in a lake instead of the store.
//!
//! A database has a **storage** ([`DbStorage`]): `local` keeps its rows in
//! the store (checkpoint + WAL, as always); `lake` keeps them in a pink-lake
//! lake with its own lease and version history. The store records only the
//! lake's location (persisted per database in the checkpoint footer, and the
//! `CREATE DATABASE … (TYPE LAKE)` / `ATTACH` / `DETACH` statements in the
//! WAL); the lake holds the rows and, for a writable lake, the database's
//! engine-only metadata as one image ([`PinkDb::to_database_image`]).
//!
//! The engine never depends on pink-lake. A host registers a
//! [`DatabaseOpener`] process-globally ([`register_database_opener`]) before
//! it opens a store, so a reopen and a WAL replay can open the lakes the
//! store names. The opener returns a [`LakeDatabase`].
//!
//! # Reads and writes
//!
//! - Lake schemas are the database's schemas: a lake schema `s` of database
//!   `d` is schema `d.s`, spelled as the lake spells it and matched
//!   case-insensitively. A lake this engine creates starts with `main`.
//! - A **read-only** lake is served by the lake's lazy, statistics-first
//!   [`AttachedLake`], aliased once per lake schema, so `s.t` in the lake is
//!   `<name>.s.t` here.
//! - A **writable** lake is materialized into the store when it is opened.
//!   Statements change those tables like any others; after every committed
//!   unit the engine publishes each lake database whose tables or image
//!   changed since its last publish ([`PinkDb::route_committed_unit`]), and
//!   a unit that wrote only lake databases is not appended to the local WAL.
//!   A failed publish restores the database's last published state.
//! - A unit (one autocommit statement, or one transaction) writes **one
//!   storage**: the local store or a single lake database. The binder-level
//!   check refuses a statement that would write two; the commit-level check
//!   catches writes a `CALL` or trigger hid and rolls them back.
//!
//! # Pinned reads
//!
//! A logged local write that read lake databases records the lake versions
//! it read in its WAL record — inside the record's memo section, under
//! [`PIN_NAME`], a name no SQL function can have. Replay swaps each lake
//! database's tables for that version's while the record runs, so
//! `INSERT INTO local SELECT … FROM lake` replays exactly after the lake
//! moved on.
//!
//! # Unavailable lakes
//!
//! A lake that cannot be opened when the store opens (or when a replayed
//! statement attaches it) keeps its database, with the reason: it is listed
//! as `unavailable`, every statement that touches it errors, and every other
//! database works.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::attached::{AtSpec, AttachedLake, AttachedScan, AttachedTable, PrunePredicate};
use crate::catalog_image::load_database_image;
use crate::storage::{DataTable, DbCatalog, PinkDb, SchemaCatalog};
use crate::types::{PinkDbError, Result, Value};
use crate::vector::DataChunk;

/// The memo name a WAL record's lake-version pins are stored under. The
/// leading control character keeps it out of reach of every SQL identifier.
pub(crate) const PIN_NAME: &str = "\u{1}lake_version";

/// Where a database's rows live.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DbStorage {
    /// In the store itself (checkpoint and WAL).
    #[default]
    Local,
    /// In a lake.
    Lake(LakeSpec),
}

impl DbStorage {
    /// `"local"` or `"lake"`, as `SHOW DATABASES` prints it.
    pub fn kind(&self) -> &'static str {
        match self {
            DbStorage::Local => "local",
            DbStorage::Lake(_) => "lake",
        }
    }
}

/// A lake database's location, as the statement that created or attached it
/// gave it (persisted in the checkpoint footer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSpec {
    /// The catalog location (`pink:<path>`, `sqlite:<path>`, `postgres://…`
    /// or a bare path). `None` is a **managed** lake: the store keeps it
    /// under its lake directory, at `<lake dir>/<database>/`.
    pub catalog: Option<String>,
    /// The data location, when it is not the one the catalog records.
    pub data: Option<String>,
    /// Attached `READ_ONLY`: reads only, served lazily from the lake.
    pub read_only: bool,
    /// `VERSION n`: a read-only attachment pinned to one lake version.
    pub version: Option<u64>,
    /// `DATA_INLINING_ROW_LIMIT n`: rows per data file before a new one is
    /// started. The lake catalog's own setting applies when `None`.
    pub data_inlining_row_limit: Option<u64>,
    /// `CREATE_IF_NOT_EXISTS`: create the lake when it does not exist yet.
    pub create_if_not_exists: bool,
    /// The data-file format the attachment declares for new tables (wave
    /// 132b): `Some("parquet")` for `ATTACH 'ducklake:…'`, whose catalogs are
    /// Parquet so another DuckLake reader can read what pinkdb writes. `None`
    /// keeps the build default except where the catalog records its own
    /// `data_file_format` option.
    pub data_file_format: Option<String>,
}

/// What a host's [`DatabaseOpener`] is asked to open: the spec resolved
/// against the store (relative paths made absolute, the managed location
/// filled in).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeOpenRequest {
    /// The database the lake will be known as.
    pub database: String,
    /// The resolved catalog location.
    pub catalog: String,
    /// The resolved data location, if the request names one.
    pub data: Option<String>,
    /// Read-only (no lease, lazy reads).
    pub read_only: bool,
    /// A pinned version (read-only only).
    pub version: Option<u64>,
    /// Create the lake (`CREATE DATABASE … TYPE LAKE`); otherwise it must
    /// exist.
    pub create: bool,
    /// The lake lives under the store's lake directory.
    pub managed: bool,
    /// The store is opening (a reopen or a replay): an opener may wait out a
    /// stale lease a crashed predecessor left behind.
    pub startup: bool,
    /// The S3 credentials of a secret whose scope covers the data (or the
    /// catalog) location; `None` lets the opener fall back to the environment.
    pub s3: Option<crate::secret::S3Secret>,
    /// The Postgres password of a secret whose scope covers the catalog; the
    /// catalog location itself never carries one.
    pub postgres_password: Option<crate::secret::Password>,
    /// `DATA_INLINING_ROW_LIMIT` (`None`: the lake catalog's own setting).
    pub data_inlining_row_limit: Option<u64>,
    /// The data-file format the attachment declares for new tables (see
    /// [`LakeSpec::data_file_format`]).
    pub data_file_format: Option<String>,
}

/// A writable lake's content at the version it was opened at.
#[derive(Debug, Default)]
pub struct LakeContent {
    /// The database image ([`PinkDb::to_database_image`]) the lake carries,
    /// if any (a lake written by another tool has none).
    pub image: Option<Vec<u8>>,
    /// The lake's tables, by schema and table name.
    pub tables: Vec<(String, String, Arc<DataTable>)>,
}

/// One open lake database, as the engine sees it. Implemented by pink-lake.
pub trait LakeDatabase: fmt::Debug + Send + Sync {
    /// The lake's location for display, with any password redacted.
    fn location(&self) -> String;
    /// Whether the lake was opened read-only.
    fn read_only(&self) -> bool;
    /// The version the database currently reflects: the pinned version of
    /// a read-only lake, the last published one of a writable lake.
    fn version(&self) -> u64;
    /// Every schema the lake offers, in the lake's spelling. Empty schemas
    /// are included: the engine keeps one database schema per name.
    fn schemas(&self) -> Result<Vec<String>>;
    /// A read-only lake's lazy reader, its tables keyed by the lake's own
    /// schema (the engine aliases each one to the database).
    fn reader(&self) -> Result<Arc<dyn AttachedLake>>;
    /// A writable lake's image and tables at [`LakeDatabase::version`].
    fn content(&self) -> Result<LakeContent>;
    /// Publish one commit of a writable lake: `schemas` are the database's
    /// schemas and `tables` its tables after the commit, written as one lake
    /// version. Returns the new version.
    fn publish(
        &self,
        image: &[u8],
        schemas: &[String],
        tables: &[(String, String, Arc<DataTable>)],
    ) -> Result<u64>;
    /// The lake's tables at an older version (a WAL record's pinned read).
    fn tables_at(&self, version: u64) -> Result<Vec<(String, String, Arc<DataTable>)>>;
}

/// Opens lakes for the engine. Registered once per process by the host.
pub trait DatabaseOpener: Send + Sync {
    /// Open (or create) the lake a request names.
    fn open(&self, request: &LakeOpenRequest) -> Result<Arc<dyn LakeDatabase>>;
}

/// A host's check on every lake a statement creates or attaches (the
/// server's location policy). Not consulted on a reopen or a replay.
pub type LakePolicy = Arc<dyn Fn(&LakeOpenRequest) -> Result<()> + Send + Sync>;

fn opener_slot() -> &'static Mutex<Option<Arc<dyn DatabaseOpener>>> {
    static OPENER: Mutex<Option<Arc<dyn DatabaseOpener>>> = Mutex::new(None);
    &OPENER
}

/// Register the process's lake opener (a later call replaces it). Hosts call
/// this before opening a store, so its reopen and replay can open lakes.
pub fn register_database_opener(opener: Arc<dyn DatabaseOpener>) {
    *opener_slot().lock().unwrap_or_else(|p| p.into_inner()) = Some(opener);
}

fn database_opener() -> Result<Arc<dyn DatabaseOpener>> {
    opener_slot()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .ok_or_else(|| {
            PinkDbError::Execution(
                "this build cannot open lake databases (no lake support was registered; \
                 build the host with pink-lake)"
                    .to_string(),
            )
        })
}

/// One database as `SHOW DATABASES` and `pinkdb_catalog()` describe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseInfo {
    pub name: String,
    /// `local` or `lake`.
    pub storage: String,
    /// The lake location (redacted), `None` for a local database.
    pub location: Option<String>,
    pub read_only: bool,
    /// The lake version the database reflects, `None` for a local one.
    pub version: Option<u64>,
    /// `ok`, or `unavailable: <reason>`.
    pub status: String,
}

/// The process-local lake state of a store, shared by every clone.
#[derive(Clone, Default)]
pub(crate) struct LakeRuntime {
    /// Lowercased database name → its open (or unavailable) lake.
    slots: Arc<Mutex<HashMap<String, Arc<LakeSlot>>>>,
    /// Where managed lakes live (`None`: managed lakes are refused).
    pub(crate) dir: Option<PathBuf>,
    /// The host's location check for new lakes.
    pub(crate) policy: Option<LakePolicy>,
}

impl fmt::Debug for LakeRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        f.debug_struct("LakeRuntime")
            .field("databases", &slots.len())
            .field("dir", &self.dir)
            .finish()
    }
}

/// One lake database's process-local state.
pub(crate) struct LakeSlot {
    spec: LakeSpec,
    /// The open lake, or why it could not be opened.
    state: std::result::Result<Arc<dyn LakeDatabase>, String>,
    /// A read-only lake's aliased readers, one per schema, as registered
    /// with the store.
    readers: Vec<Arc<dyn AttachedLake>>,
    /// Every schema the lake offers, in the lake's spelling (empty schemas
    /// included): a lake database's catalog holds one schema per name.
    schemas: Vec<String>,
    /// A writable lake's last published state: the dirty check compares
    /// against it, and a failed publish restores it.
    published: Mutex<Option<Published>>,
}

struct Published {
    catalog: DbCatalog,
    image: Vec<u8>,
}

impl LakeSlot {
    fn handle(&self) -> Option<&Arc<dyn LakeDatabase>> {
        self.state.as_ref().ok()
    }

    fn writable(&self) -> bool {
        self.handle().is_some_and(|h| !h.read_only())
    }
}

/// The storage one write lands in: the local store or one lake database.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StorageKey {
    Local,
    Lake(String),
}

impl fmt::Display for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageKey::Local => f.write_str("the local store"),
            StorageKey::Lake(db) => write!(f, "lake database \"{db}\""),
        }
    }
}

/// Whether a location URL carries a password (`scheme://user:secret@host`).
pub fn location_has_password(location: &str) -> bool {
    let Some((_, rest)) = location.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    match authority.rsplit_once('@') {
        Some((userinfo, _)) => userinfo.contains(':'),
        None => false,
    }
}

/// A location for display: the password of a `scheme://user:secret@host`
/// URL replaced by `***`.
pub fn redact_location(location: &str) -> String {
    if !location_has_password(location) {
        return location.to_string();
    }
    let (scheme, rest) = location.split_once("://").expect("checked above");
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let (userinfo, host) = authority.rsplit_once('@').expect("checked above");
    let user = userinfo.split_once(':').map(|(u, _)| u).unwrap_or(userinfo);
    format!("{scheme}://{user}:***@{host}{tail}")
}

/// The local path inside a catalog location, if it names one (`pink:`,
/// `sqlite:` or a bare path; never a URL).
fn local_catalog_path(location: &str) -> Option<(&str, &str)> {
    // A URL, a libpq conninfo (`host=… dbname=…`) and a named remote catalog
    // are never paths: `postgres:` takes the rest of the string as libpq
    // conninfo, which contains `=` and no `://`.
    if location.contains("://")
        || location.starts_with("postgres:")
        || location.split_whitespace().any(|word| word.contains('='))
    {
        return None;
    }
    for prefix in ["pink:", "sqlite:"] {
        if let Some(rest) = location.strip_prefix(prefix) {
            return Some((prefix, rest));
        }
    }
    Some(("", location))
}

/// Split a password out of a Postgres catalog location, returning the
/// location without it and the password. Both libpq spellings are handled: a
/// URI (`postgres://user:pw@host/db`) and a conninfo
/// (`postgres:host=h dbname=d password=pw`). The password is percent-decoded
/// (URI) or unquoted (conninfo) — it is about to become a secret value.
pub(crate) fn split_catalog_password(location: &str) -> (String, Option<String>) {
    if let Some((scheme, rest)) = location.split_once("://") {
        let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(end);
        let Some((userinfo, host)) = authority.rsplit_once('@') else {
            return (location.to_string(), None);
        };
        let (user, password) = match userinfo.split_once(':') {
            Some((user, password)) => (user, Some(percent_decode(password))),
            None => (userinfo, None),
        };
        return (format!("{scheme}://{user}@{host}{tail}"), password);
    }
    let (prefix, body) = match location.strip_prefix("postgres:") {
        Some(body) => ("postgres:", body),
        None => ("", location),
    };
    let (body, password) = remove_conninfo_password(body);
    (format!("{prefix}{body}"), password)
}

/// Drop every `password=…` pair from a libpq conninfo, keeping the other
/// pairs (and their quoting) as written.
fn remove_conninfo_password(input: &str) -> (String, Option<String>) {
    let bytes = input.as_bytes();
    let mut pairs: Vec<String> = Vec::new();
    let mut password = None;
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == bytes.len() {
            break;
        }
        let start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = &input[start..i];
        if i == bytes.len() || bytes[i] != b'=' {
            // Not a `key=value` pair: keep the rest verbatim.
            pairs.push(input[start..].trim().to_string());
            break;
        }
        i += 1;
        let value_start = i;
        if i < bytes.len() && bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' if i + 1 < bytes.len() => i += 2,
                    b'\'' => {
                        i += 1;
                        break;
                    }
                    _ => i += 1,
                }
            }
        } else {
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
        }
        let raw = &input[value_start..i];
        if key.eq_ignore_ascii_case("password") {
            password = Some(unquote_conninfo(raw));
        } else {
            pairs.push(format!("{key}={raw}"));
        }
    }
    (pairs.join(" "), password)
}

fn unquote_conninfo(raw: &str) -> String {
    if let Some(inner) = raw.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            out.push(if c == '\\' {
                chars.next().unwrap_or(c)
            } else {
                c
            });
        }
        out
    } else {
        raw.to_string()
    }
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(&text[i + 1..i + 3], 16)
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A relative local path resolved against `base`.
fn resolve_path(base: Option<&Path>, path: &str) -> String {
    let p = Path::new(path);
    match base {
        Some(base) if p.is_relative() => base.join(p).to_string_lossy().into_owned(),
        _ => path.to_string(),
    }
}

/// An aliased read-only lake schema: the lake schema `s` of database `d` is
/// the database's schema `s`, so `d.s.t` reads the lake's `s.t`.
#[derive(Debug)]
struct AliasedLake {
    inner: Arc<dyn AttachedLake>,
    /// The database the lake was attached as.
    alias: String,
    /// The lake schema this reader serves, in the lake's spelling.
    schema: String,
    tables: Vec<AttachedTable>,
}

impl AliasedLake {
    /// The reader for one lake schema of a read-only lake database.
    fn new(inner: Arc<dyn AttachedLake>, alias: &str, schema: &str) -> Self {
        let mut display = schema.to_string();
        let mut tables = Vec::new();
        for t in inner.tables() {
            if !t.db.eq_ignore_ascii_case(schema) {
                continue;
            }
            display = t.db.clone();
            tables.push(AttachedTable {
                db: alias.to_string(),
                schema_name: t.db.clone(),
                ..t.clone()
            });
        }
        Self {
            inner,
            alias: alias.to_string(),
            schema: display,
            tables,
        }
    }

    fn rename(&self, mut t: AttachedTable) -> AttachedTable {
        t.db = self.alias.clone();
        t.schema_name = self.schema.clone();
        t
    }
}

impl AttachedLake for AliasedLake {
    fn location(&self) -> &str {
        self.inner.location()
    }
    fn tables(&self) -> &[AttachedTable] {
        &self.tables
    }
    fn load(&self, _db: &str, table: &str) -> Result<Arc<DataTable>> {
        self.inner.load(&self.schema, table)
    }
    fn load_projected(
        &self,
        _db: &str,
        table: &str,
        columns: Option<&[usize]>,
    ) -> Result<AttachedScan> {
        self.inner.load_projected(&self.schema, table, columns)
    }
    fn load_filtered(
        &self,
        _db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[PrunePredicate],
    ) -> Result<AttachedScan> {
        self.inner
            .load_filtered(&self.schema, table, columns, predicates)
    }
    fn decode_stats(&self) -> (u64, u64, u64) {
        self.inner.decode_stats()
    }
    fn scan_streamed(
        &self,
        _db: &str,
        table: &str,
        columns: Option<&[usize]>,
        predicates: &[PrunePredicate],
        sink: &mut dyn FnMut(DataChunk) -> Result<()>,
    ) -> Result<u64> {
        self.inner
            .scan_streamed(&self.schema, table, columns, predicates, sink)
    }
    fn streams_scan(&self, _db: &str, table: &str) -> bool {
        self.inner.streams_scan(&self.schema, table)
    }
    fn table_at(&self, _db: &str, table: &str, at: AtSpec) -> Result<AttachedTable> {
        Ok(self.rename(self.inner.table_at(&self.schema, table, at)?))
    }
    fn load_at(&self, _db: &str, table: &str, at: AtSpec) -> Result<Arc<DataTable>> {
        self.inner.load_at(&self.schema, table, at)
    }
    fn load_projected_at(
        &self,
        _db: &str,
        table: &str,
        at: AtSpec,
        columns: Option<&[usize]>,
    ) -> Result<AttachedScan> {
        self.inner
            .load_projected_at(&self.schema, table, at, columns)
    }
    fn load_filtered_at(
        &self,
        _db: &str,
        table: &str,
        at: AtSpec,
        columns: Option<&[usize]>,
        predicates: &[PrunePredicate],
    ) -> Result<AttachedScan> {
        self.inner
            .load_filtered_at(&self.schema, table, at, columns, predicates)
    }
}

/// Saved tables of the lake databases a replayed record pinned to an older
/// version, restored once the record ran.
pub(crate) type PinBackup = Vec<(String, BTreeMap<String, HashMap<String, DataTable>>, u64)>;

impl PinkDb {
    /// The directory managed lakes live in (`CREATE DATABASE n (TYPE LAKE)`
    /// with no `CATALOG` puts the lake at `<dir>/<n>/`), and the base that
    /// relative local lake paths resolve against. `None` refuses managed
    /// lakes and leaves relative paths relative to the working directory.
    pub fn set_lake_dir(&mut self, dir: Option<PathBuf>) {
        self.lakes.dir = dir;
    }

    /// The host's check on every lake a statement creates or attaches. Not
    /// consulted when the store reopens its lakes or replays its WAL.
    pub fn set_lake_policy(&mut self, policy: Option<LakePolicy>) {
        self.lakes.policy = policy;
    }

    /// The storage of database `name`, if it exists.
    pub fn database_storage(&self, name: &str) -> Option<DbStorage> {
        self.databases
            .get(&name.to_ascii_lowercase())
            .map(|c| c.storage.clone())
    }

    /// Every database with its storage facts, sorted by name.
    pub fn database_infos(&self) -> Vec<DatabaseInfo> {
        let mut out: Vec<DatabaseInfo> = self
            .databases
            .iter()
            .map(|(key, cat)| match &cat.storage {
                DbStorage::Local => DatabaseInfo {
                    name: cat.name.clone(),
                    storage: "local".to_string(),
                    location: None,
                    read_only: false,
                    version: None,
                    status: "ok".to_string(),
                },
                DbStorage::Lake(spec) => {
                    let slot = self.lake_slot(key);
                    let (location, version, status) = match slot.as_ref().map(|s| &s.state) {
                        Some(Ok(handle)) => {
                            (handle.location(), Some(cat.lake_version), "ok".to_string())
                        }
                        Some(Err(reason)) => (
                            self.spec_display(&cat.name, spec),
                            None,
                            format!("unavailable: {reason}"),
                        ),
                        None => (
                            self.spec_display(&cat.name, spec),
                            None,
                            "unavailable: not opened".to_string(),
                        ),
                    };
                    DatabaseInfo {
                        name: cat.name.clone(),
                        storage: "lake".to_string(),
                        location: Some(location),
                        read_only: spec.read_only,
                        version,
                        status,
                    }
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// A spec's location for display, before (or without) an open.
    fn spec_display(&self, name: &str, spec: &LakeSpec) -> String {
        match &spec.catalog {
            Some(catalog) => redact_location(catalog),
            None => match &self.lakes.dir {
                Some(dir) => dir.join(name.to_ascii_lowercase()).display().to_string(),
                None => format!("(managed lake {name})"),
            },
        }
    }

    pub(crate) fn lake_slot(&self, db: &str) -> Option<Arc<LakeSlot>> {
        self.lakes
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&db.to_ascii_lowercase())
            .cloned()
    }

    /// True when the store has at least one lake database.
    pub(crate) fn has_lake_databases(&self) -> bool {
        self.databases
            .values()
            .any(|c| matches!(c.storage, DbStorage::Lake(_)))
    }

    /// Resolve a spec into what the opener sees.
    fn lake_request(
        &self,
        name: &str,
        spec: &LakeSpec,
        create: bool,
        startup: bool,
    ) -> Result<LakeOpenRequest> {
        for location in spec.catalog.iter().chain(spec.data.iter()) {
            if location_has_password(location) {
                return Err(PinkDbError::Catalog(format!(
                    "lake location {} carries a password: it would be stored in the WAL and \
                     the checkpoint. Give credentials through the environment (PGPASSWORD, \
                     AWS_*) instead",
                    redact_location(location)
                )));
            }
        }
        if spec.version.is_some() && !spec.read_only {
            return Err(PinkDbError::Catalog(
                "VERSION pins a read-only attachment: add READ_ONLY".to_string(),
            ));
        }
        let base = self.lakes.dir.as_deref();
        let (catalog, data, managed) = match &spec.catalog {
            Some(catalog) => {
                let catalog = match local_catalog_path(catalog) {
                    Some((prefix, path)) => format!("{prefix}{}", resolve_path(base, path)),
                    None => catalog.clone(),
                };
                let data = spec.data.as_ref().map(|d| {
                    if d.contains("://") {
                        d.clone()
                    } else {
                        resolve_path(base, d)
                    }
                });
                (catalog, data, false)
            }
            None => {
                let Some(dir) = base else {
                    return Err(PinkDbError::Catalog(format!(
                        "lake database \"{name}\" needs a CATALOG location: this store has no \
                         lake directory for managed lakes"
                    )));
                };
                let root = dir.join(name.to_ascii_lowercase());
                let data = match &spec.data {
                    Some(d) if d.contains("://") => d.clone(),
                    Some(d) => resolve_path(base, d),
                    None => root.join("data").to_string_lossy().into_owned(),
                };
                (
                    format!("pink:{}", root.join("catalog.pink").display()),
                    Some(data),
                    true,
                )
            }
        };
        Ok(LakeOpenRequest {
            database: name.to_string(),
            // A secret scoped to the data location (or the catalog, for an
            // S3 catalog) supplies the object-store credentials; without one
            // the opener falls back to `AWS_*` as before.
            s3: self.s3_secret_for(&catalog, &self.session).or_else(|| {
                data.as_deref()
                    .and_then(|d| self.s3_secret_for(d, &self.session))
            }),
            postgres_password: self.postgres_password_for(&catalog, &self.session),
            data_inlining_row_limit: spec.data_inlining_row_limit,
            data_file_format: spec.data_file_format.clone(),
            catalog,
            data,
            read_only: spec.read_only,
            version: spec.version,
            create: create || (spec.create_if_not_exists && !spec.read_only),
            managed,
            startup,
        })
    }

    /// Open a lake into a slot: the policy check (new lakes only), the opener,
    /// the read-only alias or the writable content. An open that fails while
    /// the store is opening or replaying yields an unavailable slot instead
    /// of an error.
    fn open_lake_slot(
        &self,
        name: &str,
        spec: &LakeSpec,
        create: bool,
        startup: bool,
    ) -> Result<Arc<LakeSlot>> {
        let opened = (|| {
            let request = self.lake_request(name, spec, create, startup)?;
            if !startup && let Some(policy) = &self.lakes.policy {
                policy(&request)?;
            }
            if request.managed
                && create
                && let Some(dir) = &self.lakes.dir
            {
                std::fs::create_dir_all(dir.join(name.to_ascii_lowercase())).map_err(|e| {
                    PinkDbError::Execution(format!(
                        "cannot create the lake directory for \"{name}\": {e}"
                    ))
                })?;
            }
            let handle = database_opener()?.open(&request)?;
            let storage = DbStorage::Lake(spec.clone());
            let mut schemas = handle.schemas()?;
            if schemas.is_empty() {
                schemas.push("main".to_string());
            }
            if handle.read_only() {
                let reader = handle.reader()?;
                // A pinned version can hold a schema the head no longer has:
                // its tables need a reader too.
                for t in reader.tables() {
                    if !schemas.iter().any(|s| s.eq_ignore_ascii_case(&t.db)) {
                        schemas.push(t.db.clone());
                    }
                }
                let readers = schemas
                    .iter()
                    .map(|schema| {
                        Arc::new(AliasedLake::new(Arc::clone(&reader), name, schema))
                            as Arc<dyn AttachedLake>
                    })
                    .collect();
                return Ok::<_, PinkDbError>((handle, readers, schemas, None));
            }
            let content = handle.content()?;
            let mut catalog =
                load_database_image(name, storage, content.image.as_deref(), &content.tables)?;
            catalog.lake_version = handle.version();
            // The lake's schema list is the truth: an image older than a
            // schema the lake gained (or one written by another tool) still
            // gets that schema.
            for schema in &schemas {
                catalog
                    .schemas
                    .entry(schema.to_ascii_lowercase())
                    .or_insert_with(|| SchemaCatalog::empty(schema));
            }
            schemas = catalog.schemas.values().map(|s| s.name.clone()).collect();
            let image = self.catalog_image_of(&catalog)?;
            Ok((
                handle,
                Vec::new(),
                schemas,
                Some(Published { catalog, image }),
            ))
        })();
        match opened {
            Ok((handle, readers, schemas, published)) => Ok(Arc::new(LakeSlot {
                spec: spec.clone(),
                state: Ok(handle),
                readers,
                schemas,
                published: Mutex::new(published),
            })),
            Err(e) if startup => Ok(unavailable_slot(spec, e.to_string())),
            Err(e) => Err(e),
        }
    }

    /// Make `slot` the lake behind database `name`: (re)build the database's
    /// catalog from the slot and register it. Replaces whatever catalog the
    /// database had.
    fn install_lake_slot(&mut self, name: &str, slot: Arc<LakeSlot>) {
        let key = name.to_ascii_lowercase();
        let storage = DbStorage::Lake(slot.spec.clone());
        let published = slot
            .published
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|p| p.catalog.clone());
        let catalog = match (&slot.state, published) {
            (Ok(_), Some(mut cat)) => {
                cat.name = name.to_string();
                cat
            }
            (Ok(handle), None) => {
                let mut cat = DbCatalog::empty(name, storage);
                cat.lake_version = handle.version();
                for schema in &slot.schemas {
                    cat.schemas
                        .entry(schema.to_ascii_lowercase())
                        .or_insert_with(|| SchemaCatalog::empty(schema));
                }
                cat
            }
            (Err(_), _) => DbCatalog::empty(name, storage),
        };
        for reader in &slot.readers {
            if !self.attached_lakes.iter().any(|l| Arc::ptr_eq(l, reader)) {
                self.attached_lakes.push(Arc::clone(reader));
            }
        }
        self.databases.insert(key.clone(), catalog);
        self.lakes
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, slot);
    }

    /// Refuse a lake whose location another database of this store already
    /// serves: two databases on one lake would publish over each other.
    fn refuse_duplicate_lake(&self, name: &str, spec: &LakeSpec) -> Result<()> {
        let request = self.lake_request(name, spec, false, true)?;
        for (key, cat) in &self.databases {
            if key.eq_ignore_ascii_case(name) {
                continue;
            }
            if let DbStorage::Lake(other) = &cat.storage
                && let Ok(theirs) = self.lake_request(&cat.name, other, false, true)
                && theirs.catalog == request.catalog
            {
                return Err(PinkDbError::Catalog(format!(
                    "lake {} is already attached as database \"{}\"",
                    redact_location(&request.catalog),
                    cat.name
                )));
            }
        }
        Ok(())
    }

    /// `CREATE DATABASE name (TYPE LAKE …)`: create the lake and attach it.
    /// A replay opens the lake the original statement created instead.
    pub(crate) fn create_lake_database(&mut self, name: &str, spec: LakeSpec) -> Result<()> {
        self.attach_lake_database_inner(name, spec, true)
    }

    /// `ATTACH '<location>' AS name (…)`: open an existing lake as database
    /// `name`.
    pub(crate) fn attach_lake_database(&mut self, name: &str, spec: LakeSpec) -> Result<()> {
        self.attach_lake_database_inner(name, spec, false)
    }

    fn attach_lake_database_inner(
        &mut self,
        name: &str,
        spec: LakeSpec,
        create: bool,
    ) -> Result<()> {
        if self.database_exists(name) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" already exists"
            )));
        }
        self.refuse_duplicate_lake(name, &spec)?;
        // A replayed statement re-uses the lake this process already holds
        // for the database (a rebuilt store replays its own ATTACH), and
        // opens rather than creates.
        let slot = match self.lake_slot(name) {
            Some(slot) if self.replaying && slot.spec == spec => slot,
            _ => self.open_lake_slot(name, &spec, create && !self.replaying, self.replaying)?,
        };
        self.install_lake_slot(name, slot);
        Ok(())
    }

    /// `DETACH name`: forget a lake database. The lake keeps its data.
    pub(crate) fn detach_lake_database(&mut self, name: &str, if_exists: bool) -> Result<bool> {
        let key = name.to_ascii_lowercase();
        let Some(cat) = self.databases.get(&key) else {
            if if_exists {
                return Ok(false);
            }
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" not found"
            )));
        };
        if !matches!(cat.storage, DbStorage::Lake(_)) {
            return Err(PinkDbError::Catalog(format!(
                "database \"{name}\" is a local database: DETACH forgets lake databases \
                 (DROP DATABASE removes a local one)"
            )));
        }
        if self.databases.len() == 1 {
            return Err(PinkDbError::Catalog(format!(
                "cannot detach the last database \"{name}\""
            )));
        }
        let slot = self
            .lakes
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key);
        if let Some(slot) = slot.as_ref() {
            for reader in &slot.readers {
                self.attached_lakes.retain(|l| !Arc::ptr_eq(l, reader));
            }
        }
        self.databases.remove(&key);
        Ok(true)
    }

    /// Close every lake this store holds (a host's shutdown): the lakes'
    /// handles are dropped, so a writable lake releases its lease. The
    /// databases stay listed as unavailable; every clone of the store shares
    /// the closing.
    pub fn close_lake_databases(&mut self) {
        let slots: Vec<Arc<LakeSlot>> = self
            .lakes
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
            .map(|(_, slot)| slot)
            .collect();
        for slot in &slots {
            for reader in &slot.readers {
                self.attached_lakes.retain(|l| !Arc::ptr_eq(l, reader));
            }
        }
        drop(slots);
    }

    /// Open every lake database a loaded checkpoint names (the durable open,
    /// before the WAL replays). A lake that cannot be opened is listed as
    /// unavailable; the open continues.
    pub(crate) fn reopen_lake_databases(&mut self) -> Result<()> {
        let lakes: Vec<(String, LakeSpec)> = self
            .databases
            .values()
            .filter_map(|c| match &c.storage {
                DbStorage::Lake(spec) => Some((c.name.clone(), spec.clone())),
                DbStorage::Local => None,
            })
            .collect();
        for (name, spec) in lakes {
            let slot = self.open_lake_slot(&name, &spec, false, true)?;
            self.install_lake_slot(&name, slot);
        }
        Ok(())
    }

    /// After a full rebuild adopted a reloaded checkpoint: give every lake
    /// database back the lake this process holds for it (a database this
    /// process has not opened yet is opened).
    pub(crate) fn reinstall_lake_databases(&mut self) {
        let lakes: Vec<(String, LakeSpec)> = self
            .databases
            .values()
            .filter_map(|c| match &c.storage {
                DbStorage::Lake(spec) => Some((c.name.clone(), spec.clone())),
                DbStorage::Local => None,
            })
            .collect();
        for (name, spec) in lakes {
            let slot = match self.lake_slot(&name) {
                Some(slot) if slot.spec == spec => slot,
                _ => self
                    .open_lake_slot(&name, &spec, false, true)
                    .unwrap_or_else(|e| unavailable_slot(&spec, e.to_string())),
            };
            self.install_lake_slot(&name, slot);
        }
    }

    /// Whether `lake` is the reader of one of this store's lake databases
    /// (rather than a session attachment made through the Rust API).
    pub(crate) fn is_lake_reader(&self, lake: &Arc<dyn AttachedLake>) -> bool {
        self.lakes
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .any(|s| s.readers.iter().any(|r| Arc::ptr_eq(r, lake)))
    }

    /// The storage a write to database `db` lands in.
    pub(crate) fn storage_key(&self, db: &str) -> StorageKey {
        let key = db.to_ascii_lowercase();
        match self.databases.get(&key).map(|c| &c.storage) {
            Some(DbStorage::Lake(_)) => StorageKey::Lake(key),
            _ => StorageKey::Local,
        }
    }

    /// The storages the databases in `dbs` write to, refusing a write to a
    /// read-only or unavailable lake database.
    pub(crate) fn write_storages<'a>(
        &self,
        dbs: impl IntoIterator<Item = &'a String>,
    ) -> Result<BTreeSet<StorageKey>> {
        let mut out = BTreeSet::new();
        for db in dbs {
            let key = self.storage_key(db);
            if let StorageKey::Lake(name) = &key {
                match self.lake_slot(name).map(|s| s.state.clone()) {
                    Some(Ok(handle)) if handle.read_only() => {
                        return Err(PinkDbError::Execution(format!(
                            "database \"{}\" is a read-only lake ({}); attach it without \
                             READ_ONLY to write",
                            self.database_display_name(name),
                            handle.location()
                        )));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(reason)) => return Err(unavailable_error(name, &reason)),
                    None => return Err(unavailable_error(name, "not opened")),
                }
            }
            out.insert(key);
        }
        Ok(out)
    }

    /// Refuse a catalog write (a schema create or drop) in a read-only or
    /// unavailable lake database; a local database passes.
    pub(crate) fn refuse_lake_write(&self, db: &str) -> Result<()> {
        if let StorageKey::Lake(name) = self.storage_key(db) {
            self.write_storages(std::iter::once(&name))?;
        }
        Ok(())
    }

    /// Refuse a unit that writes more than one storage.
    pub(crate) fn refuse_mixed_storages(
        &self,
        storages: &BTreeSet<StorageKey>,
        unit: &str,
    ) -> Result<()> {
        if storages.len() <= 1 {
            return Ok(());
        }
        let names: Vec<String> = storages.iter().map(|s| s.to_string()).collect();
        Err(PinkDbError::Execution(format!(
            "this {unit} writes {}: a statement or transaction may write the local store or \
             one lake database, not both (each storage commits on its own)",
            names.join(" and ")
        )))
    }

    /// The databases a bound plan writes (tables and the per-database
    /// catalog objects; views are session-local and count for nothing).
    pub(crate) fn plan_written_dbs(
        &self,
        plan: &crate::plan::LogicalPlan,
        default_db: &str,
    ) -> BTreeSet<String> {
        use crate::plan::LogicalPlan as P;
        let mut out: BTreeSet<String> = self
            .plan_writes_of(plan, &crate::storage::SchemaPath::main(default_db))
            .into_iter()
            .map(|(ns, _)| ns.db().to_string())
            .collect();
        let db_of = |ns: &Option<crate::storage::SchemaPath>| {
            ns.as_ref()
                .map(|n| n.db().to_string())
                .unwrap_or_else(|| default_db.to_string())
        };
        match plan {
            P::CreateMacro { ns, .. }
            | P::DropMacro { ns, .. }
            | P::CreateProcedure { ns, .. }
            | P::DropProcedure { ns, .. }
            | P::CreateTrigger { ns, .. }
            | P::DropTrigger { ns, .. } => {
                out.insert(db_of(ns));
            }
            _ => {}
        }
        out
    }

    /// The bind-time check: refuse a statement that writes a read-only or
    /// unavailable lake, or two storages (counting what the open transaction
    /// already wrote). Free when the store has no lake database.
    pub(crate) fn check_statement_storages(
        &self,
        plan: &crate::plan::LogicalPlan,
        default_db: &str,
        txn_storages: &BTreeSet<StorageKey>,
    ) -> Result<BTreeSet<StorageKey>> {
        if !self.has_lake_databases() {
            return Ok(BTreeSet::new());
        }
        let dbs = self.plan_written_dbs(plan, default_db);
        let mut storages = self.write_storages(&dbs)?;
        let unit = if txn_storages.is_empty() {
            "statement"
        } else {
            "transaction"
        };
        storages.extend(txn_storages.iter().cloned());
        self.refuse_mixed_storages(&storages, unit)?;
        Ok(storages)
    }

    /// A snapshot of the databases taken before an autocommit statement
    /// whose writes the plan cannot name (a `CALL`, or any statement while
    /// triggers exist), so a hidden cross-storage write can be undone.
    pub(crate) fn hidden_write_snapshot(
        &self,
        plan: &crate::plan::LogicalPlan,
    ) -> Option<HashMap<String, DbCatalog>> {
        let may_hide = matches!(plan, crate::plan::LogicalPlan::Call { .. }) || self.has_triggers();
        (may_hide && crate::plan::writes_tables(plan) && self.has_lake_databases())
            .then(|| self.databases.clone())
    }

    /// The execution-time check: the writes a statement actually performed
    /// (its plan's plus what a `CALL` or trigger did) stay in one storage.
    pub(crate) fn check_executed_storages(
        &self,
        plan: &crate::plan::LogicalPlan,
        default_db: &str,
        txn_storages: &BTreeSet<StorageKey>,
    ) -> Result<BTreeSet<StorageKey>> {
        if !self.has_lake_databases() {
            return Ok(BTreeSet::new());
        }
        let mut dbs = self.plan_written_dbs(plan, default_db);
        for (ns, _) in self.proc_frame.writes() {
            dbs.insert(ns.db().to_string());
        }
        let mut storages = self.write_storages(&dbs)?;
        storages.extend(txn_storages.iter().cloned());
        self.refuse_mixed_storages(
            &storages,
            if txn_storages.is_empty() {
                "statement"
            } else {
                "transaction"
            },
        )?;
        Ok(storages)
    }

    /// Run one statement under the storage rules: execute it, then check the
    /// writes it actually performed (see [`PinkDb::check_executed_storages`]).
    /// A refused statement whose writes the plan could not name is undone
    /// from a snapshot of the databases taken before it ran. Returns the
    /// result and the storages written so far (`prior` plus this
    /// statement's).
    pub(crate) fn run_storage_checked(
        &mut self,
        plan: &crate::plan::LogicalPlan,
        default_db: &str,
        prior: &BTreeSet<StorageKey>,
        run: impl FnOnce(&mut PinkDb) -> Result<crate::QueryResult>,
    ) -> Result<(crate::QueryResult, BTreeSet<StorageKey>)> {
        let snapshot = self.hidden_write_snapshot(plan);
        let result = run(self)?;
        match self.check_executed_storages(plan, default_db, prior) {
            Ok(storages) => Ok((result, storages)),
            Err(e) => {
                if let Some(databases) = snapshot {
                    self.databases = databases;
                }
                Err(e)
            }
        }
    }

    /// The image of one catalog (see [`PinkDb::to_database_image`]).
    fn catalog_image_of(&self, cat: &DbCatalog) -> Result<Vec<u8>> {
        let mut scratch = PinkDb::with_databases(Vec::<String>::new());
        let mut cat = cat.clone();
        cat.storage = DbStorage::Local;
        scratch.databases.insert(cat.name.to_ascii_lowercase(), cat);
        scratch.metadata_image()
    }

    /// The writable lake databases whose tables or metadata changed since
    /// their last publish.
    pub(crate) fn dirty_lake_databases(&self) -> Result<Vec<String>> {
        let slots: Vec<(String, Arc<LakeSlot>)> = self
            .lakes
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(_, s)| s.writable())
            .map(|(k, s)| (k.clone(), Arc::clone(s)))
            .collect();
        let mut out = Vec::new();
        for (key, slot) in slots {
            let Some(cat) = self.databases.get(&key) else {
                continue;
            };
            let published = slot.published.lock().unwrap_or_else(|p| p.into_inner());
            let Some(published) = published.as_ref() else {
                continue;
            };
            let same_tables = cat.schemas.iter().all(|(schema, sc)| {
                published.catalog.schemas.get(schema).is_some_and(|p| {
                    sc.tables.len() == p.tables.len()
                        && sc.tables.iter().all(|(name, t)| {
                            p.tables.get(name).is_some_and(|p| p.version == t.version)
                        })
                })
            }) && cat.schemas.len() == published.catalog.schemas.len();
            if !same_tables || self.catalog_image_of(cat)? != published.image {
                out.push(key);
            }
        }
        out.sort();
        Ok(out)
    }

    /// Publish the given lake databases (each as one lake version). A failed
    /// publish restores that database's last published state and fails.
    pub(crate) fn publish_lake_databases(&mut self, dbs: &[String]) -> Result<()> {
        for key in dbs {
            let Some(slot) = self.lake_slot(key) else {
                continue;
            };
            let Some(handle) = slot.handle().cloned() else {
                continue;
            };
            let Some(cat) = self.databases.get(key) else {
                continue;
            };
            let image = self.catalog_image_of(cat)?;
            let schemas: Vec<String> = cat.schemas.values().map(|s| s.name.clone()).collect();
            let tables: Vec<(String, String, Arc<DataTable>)> = cat
                .schemas
                .values()
                .flat_map(|sc| {
                    sc.tables
                        .values()
                        .map(|t| (sc.name.clone(), t.name.clone(), Arc::new(t.clone())))
                })
                .collect();
            match handle.publish(&image, &schemas, &tables) {
                Ok(version) => {
                    if let Some(cat) = self.databases.get_mut(key) {
                        cat.lake_version = version;
                        *slot.published.lock().unwrap_or_else(|p| p.into_inner()) =
                            Some(Published {
                                catalog: cat.clone(),
                                image,
                            });
                    }
                }
                Err(e) => {
                    let restored = slot
                        .published
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .as_ref()
                        .map(|p| p.catalog.clone());
                    if let Some(cat) = restored {
                        self.databases.insert(key.clone(), cat);
                    }
                    return Err(PinkDbError::Execution(format!(
                        "publishing lake database \"{}\" failed, and its changes were rolled \
                         back: {e}",
                        self.database_display_name(key)
                    )));
                }
            }
        }
        Ok(())
    }

    /// Route one committed unit: publish every lake database it changed.
    /// Returns true when it changed a lake database (the unit is then not
    /// appended to the local WAL — the bind and execution checks guarantee
    /// it wrote nothing local).
    pub(crate) fn route_committed_unit(&mut self) -> Result<bool> {
        if !self.has_lake_databases() {
            return Ok(false);
        }
        let dirty = self.dirty_lake_databases()?;
        if dirty.is_empty() {
            return Ok(false);
        }
        self.publish_lake_databases(&dirty)?;
        Ok(true)
    }

    /// The lake versions a logged unit read, as memo entries for its WAL
    /// record: one per lake database among the frame's touched tables.
    pub(crate) fn lake_pins(
        &self,
        sets: Option<&crate::wal::FrameSets>,
    ) -> Vec<crate::wal::ExtCall> {
        let Some(sets) = sets else {
            return Vec::new();
        };
        if !self.has_lake_databases() {
            return Vec::new();
        }
        let mut dbs: BTreeSet<&str> = BTreeSet::new();
        for (db, _, _) in sets.table_refs() {
            dbs.insert(db.as_str());
        }
        dbs.into_iter()
            .filter_map(|db| {
                let cat = self.databases.get(db)?;
                matches!(cat.storage, DbStorage::Lake(_)).then(|| crate::wal::ExtCall {
                    name: PIN_NAME.to_string(),
                    args: vec![Value::Varchar(db.to_string())],
                    result: Value::Bigint(cat.lake_version as i64),
                })
            })
            .collect()
    }

    /// Before a replayed record runs: give every lake database it pinned the
    /// tables of the pinned version. Returns what to restore afterwards.
    pub(crate) fn apply_replay_pins(&mut self, memo: &[crate::wal::ExtCall]) -> Result<PinBackup> {
        let mut backup: PinBackup = Vec::new();
        for call in memo.iter().filter(|c| c.name == PIN_NAME) {
            let (Some(Value::Varchar(db)), Value::Bigint(version)) =
                (call.args.first(), &call.result)
            else {
                continue;
            };
            let version = *version as u64;
            let Some(cat) = self.databases.get(db) else {
                continue;
            };
            if cat.lake_version == version {
                continue;
            }
            let display = cat.name.clone();
            let handle = match self.lake_slot(db).map(|s| s.state.clone()) {
                Some(Ok(handle)) => handle,
                Some(Err(reason)) => {
                    return Err(PinkDbError::Execution(format!(
                        "a WAL record read lake database \"{display}\" at version {version}, \
                         but the lake is unavailable ({reason}); restore access to it and \
                         reopen"
                    )));
                }
                None => continue,
            };
            let tables = handle.tables_at(version).map_err(|e| {
                PinkDbError::Execution(format!(
                    "a WAL record read lake database \"{display}\" at version {version}, which \
                     the lake no longer serves (expired?): {e}"
                ))
            })?;
            let cat = self.databases.get_mut(db).expect("checked above");
            let mut pinned: BTreeMap<String, HashMap<String, DataTable>> = BTreeMap::new();
            for (schema, name, table) in tables {
                let mut table = (*table).clone();
                table.name = name.clone();
                pinned
                    .entry(schema.to_ascii_lowercase())
                    .or_default()
                    .insert(name.to_ascii_lowercase(), table);
            }
            let mut saved = BTreeMap::new();
            for (key, schema) in &mut cat.schemas {
                let tables = pinned.get(key).cloned().unwrap_or_default();
                saved.insert(key.clone(), std::mem::replace(&mut schema.tables, tables));
            }
            let saved_version = std::mem::replace(&mut cat.lake_version, version);
            backup.push((db.clone(), saved, saved_version));
        }
        Ok(backup)
    }

    /// After a replayed record ran: put back what [`PinkDb::apply_replay_pins`]
    /// swapped out.
    pub(crate) fn restore_replay_pins(&mut self, backup: PinBackup) {
        for (db, schemas, version) in backup {
            if let Some(cat) = self.databases.get_mut(&db) {
                for (key, tables) in schemas {
                    if let Some(schema) = cat.schemas.get_mut(&key) {
                        schema.tables = tables;
                    }
                }
                cat.lake_version = version;
            }
        }
    }

    /// Per read-only lake database (and session attachment): its database
    /// (`None` for a session attachment), location and data-file decode
    /// counters `(loads, columns, files)`, sorted by database.
    pub fn lake_decode_stats(&self) -> Vec<(Option<String>, String, u64, u64, u64)> {
        let slots = self.lakes.slots.lock().unwrap_or_else(|p| p.into_inner());
        // A lake database has one reader per lake schema, all sharing the
        // lake's counters: report the lake once, from its first schema.
        let mut out: Vec<(Option<String>, String, u64, u64, u64)> = self
            .attached_lakes
            .iter()
            .filter_map(|lake| {
                let database = slots
                    .iter()
                    .find(|(_, s)| s.readers.first().is_some_and(|r| Arc::ptr_eq(r, lake)))
                    .map(|(key, _)| self.database_display_name(key));
                let in_slot = slots
                    .iter()
                    .any(|(_, s)| s.readers.iter().any(|r| Arc::ptr_eq(r, lake)));
                if in_slot && database.is_none() {
                    return None;
                }
                let (loads, columns, files) = lake.decode_stats();
                Some((database, lake.location().to_string(), loads, columns, files))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The error for a statement that names an unavailable lake database, if
    /// `db` is one.
    pub(crate) fn unavailable_lake(&self, db: &str) -> Option<PinkDbError> {
        let key = db.to_ascii_lowercase();
        let cat = self.databases.get(&key)?;
        if !matches!(cat.storage, DbStorage::Lake(_)) {
            return None;
        }
        match self.lake_slot(&key).map(|s| s.state.clone()) {
            Some(Ok(_)) => None,
            Some(Err(reason)) => Some(unavailable_error(&cat.name, &reason)),
            None => Some(unavailable_error(&cat.name, "not opened")),
        }
    }
}

fn unavailable_slot(spec: &LakeSpec, reason: String) -> Arc<LakeSlot> {
    Arc::new(LakeSlot {
        spec: spec.clone(),
        state: Err(reason),
        readers: Vec::new(),
        schemas: Vec::new(),
        published: Mutex::new(None),
    })
}

fn unavailable_error(db: &str, reason: &str) -> PinkDbError {
    PinkDbError::Catalog(format!("lake database \"{db}\" is unavailable: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passwords_in_urls_are_found_and_redacted() {
        assert!(location_has_password("postgres://u:secret@h/db"));
        assert!(!location_has_password("postgres://u@h/db"));
        assert!(!location_has_password("sqlite:/tmp/a:b.sqlite"));
        assert!(!location_has_password("s3://bucket/a@b"));
        assert_eq!(
            redact_location("postgres://u:secret@h:5432/db?x=1"),
            "postgres://u:***@h:5432/db?x=1"
        );
        assert_eq!(redact_location("pink:/x"), "pink:/x");
    }

    #[test]
    fn relative_local_catalogs_resolve_against_the_lake_dir() {
        let mut db = PinkDb::new();
        db.set_lake_dir(Some(PathBuf::from("/data/lakes")));
        let spec = |catalog: &str| LakeSpec {
            catalog: Some(catalog.to_string()),
            data: None,
            read_only: false,
            version: None,
            data_inlining_row_limit: None,
            create_if_not_exists: false,
            data_file_format: None,
        };
        let r = db
            .lake_request("a", &spec("sqlite:x.sqlite"), false, false)
            .unwrap();
        assert_eq!(r.catalog, "sqlite:/data/lakes/x.sqlite");
        let r = db
            .lake_request("a", &spec("pink:/abs/c.pink"), false, false)
            .unwrap();
        assert_eq!(r.catalog, "pink:/abs/c.pink");
        let r = db
            .lake_request("a", &spec("postgres://h/db"), false, false)
            .unwrap();
        assert_eq!(r.catalog, "postgres://h/db");
        let managed = LakeSpec {
            catalog: None,
            data: None,
            read_only: false,
            version: None,
            data_inlining_row_limit: None,
            create_if_not_exists: false,
            data_file_format: None,
        };
        let r = db.lake_request("Sales", &managed, true, false).unwrap();
        assert_eq!(r.catalog, "pink:/data/lakes/sales/catalog.pink");
        assert_eq!(r.data.as_deref(), Some("/data/lakes/sales/data"));
        assert!(r.managed);
        // A libpq conninfo carries no `://`; it is still a remote catalog and
        // must never be resolved against the lake directory.
        let r = db
            .lake_request("a", &spec("postgres:host=h dbname=db"), false, false)
            .unwrap();
        assert_eq!(r.catalog, "postgres:host=h dbname=db");
        let err = db
            .lake_request("a", &spec("postgres://u:pw@h/db"), false, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("carries a password"), "{err}");
        assert!(!err.contains("pw@"), "{err}");
    }
}
