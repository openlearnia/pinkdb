//! The lake warehouse (wave 138c): with `GRAIN_DUCKLAKE=1` the database every
//! request runs in is a pinkdb **lake database** (wave 124) instead of a local
//! one.
//!
//! The Go service attaches a DuckLake (`quacklab-vps-duckdb/ducklake.go`,
//! `duckLakeAttachSQL`). This mirrors its behaviour, not its plumbing: the
//! same environment picks the catalog, the data path and the object-store
//! credentials, the same retry rule runs every bootstrap statement, and the
//! same lifecycle closes and reopens the warehouse for a write lease and
//! `/recover`. What it attaches is a lake **pinkdb** writes (its own `.pink`
//! data files, its own catalog tables): QuackLab's test drive starts from a
//! fresh lake, and pinkdb has no DuckLake compatibility goal since 2026-11-03.
//!
//! The boot sequence ([`LakeConfig::steps`]):
//!
//! 1. the object-store secret, chosen from the environment exactly as Go's
//!    `r2S3SecretSQL` chooses it (`GRAIN_OBJECT_STORE_PROVIDER` = `aws` |
//!    `bunny` | anything else = R2), only for a remote data path, as a
//!    `PERSISTENT` secret so every request's session sees it;
//! 2. the catalog's Postgres password, split out of `DUCKLAKE_PG_URI` into a
//!    `postgres` secret scoped to the catalog, so no statement, WAL record or
//!    footer location carries it;
//! 3. `ATTACH 'lake:<catalog>' AS <alias> (DATA '<path>/',
//!    CREATE_IF_NOT_EXISTS)` — a new lake on the first boot (what `CREATE
//!    DATABASE … (TYPE LAKE, CATALOG, DATA)` makes), the existing one after —
//!    or `(DATA …, READ_ONLY)` on the read-only service.
//!
//! DuckDB-only steps have no counterpart and are skipped: `SET
//! home_directory`/`extension_directory`, `INSTALL`/`LOAD`, `SET
//! httpfs_connection_caching`, and the five `CALL <alias>.set_option(…)`
//! writes (Parquet compression and version describe Parquet files a pinkdb
//! lake does not write; pinkdb's lake keeps its own snapshot and file
//! retention, see `docs/quacklab-test-drive.md`).
//!
//! A write lease **detaches** the lake (releasing pinkdb's writer lease on the
//! catalog so an outside writer can own it) and release **attaches** it again
//! at the new head. `/recover` is the same detach and attach. A lake the boot
//! could not attach because another writer still held it (a Cloud Run
//! revision rollout overlaps the old instance) is attached by the first
//! request after that writer lets go; until then `/sql` answers `503
//! conn_error` and `/health` `503 not_ready`, which QuackLab's client retries.

use std::sync::Mutex;
use std::time::Duration;

use pinkdb::Database;

use crate::app::quoted;
use crate::errors::engine_message;
use crate::lease::Warehouse;

/// Go's `execBootstrap`: eight attempts, a linear 200 ms backoff (~5.6 s).
const BOOTSTRAP_ATTEMPTS: u32 = 8;
const BOOTSTRAP_BACKOFF: Duration = Duration::from_millis(200);

/// Go's truthy rule for `GRAIN_DUCKLAKE`, `DUCKDB_READ_ONLY` and
/// `DUCKLAKE_ENCRYPTED` (`duckLakeEnabled`): `1`, or `true` / `yes` in any
/// case, trimmed. (`on` is not one of them.)
pub fn go_truthy(raw: Option<&str>) -> bool {
    let Some(raw) = raw else {
        return false;
    };
    let raw = raw.trim();
    raw == "1" || raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("yes")
}

/// The DuckDB-only settings a lake warehouse reads and does not use. Named so
/// the server can log once that they are ignored.
pub const LAKE_IGNORED_ENVIRONMENT: &[&str] = &[
    "DUCKLAKE_AUTOMATIC_MIGRATION",
    "GRAIN_DUCKLAKE_AUTOMATIC_MIGRATION",
    "DUCKLAKE_PRE_MIGRATE_BACKUP_DIR",
    "GRAIN_DUCKLAKE_EXTENSION_PATH",
    "GRAIN_DUCKDB_STAGED_EXTENSIONS_DIR",
    "GRAIN_DUCKDB_EXTENSION_REPOSITORY",
    "GRAIN_DUCKDB_EXTENSION_DIRECTORY",
    "DUCKDB_HOME_DIRECTORY",
];

/// The default Postgres catalog when `DUCKLAKE_PG_URI` is unset: the Go
/// service's own default (a local catalog database).
const DEFAULT_PG_URI: &str =
    "dbname=ducklake_catalog host=127.0.0.1 port=5432 user=ducklake password=ducklake";
/// The Go service's default `DUCKLAKE_DATA_PATH`.
const DEFAULT_DATA_PATH: &str = "/opt/quacklab/data/lake";

/// The lake warehouse's configuration, read from the environment the Go
/// service reads.
#[derive(Clone, PartialEq, Eq)]
pub struct LakeConfig {
    /// `DUCKLAKE_ALIAS`: the database name requests run in.
    pub alias: String,
    /// The lake catalog location, without any password: `postgres:<conninfo>`
    /// or a `postgres://` URL from `DUCKLAKE_PG_URI`, or whatever
    /// `PINKDB_LAKE_CATALOG` names (`sqlite:`, `pink:`, `s3://`…).
    pub catalog: String,
    /// The catalog's Postgres password, which becomes a secret.
    pub catalog_password: Option<String>,
    /// `DUCKLAKE_DATA_PATH`, with the trailing slash Go adds.
    pub data: String,
    /// `DUCKDB_READ_ONLY`: attach `READ_ONLY` (no writer lease taken).
    pub read_only: bool,
    /// The object-store secret, when the data path is remote and the
    /// provider's variables are all set.
    pub object_store: Option<ObjectStoreSecret>,
}

impl std::fmt::Debug for LakeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeConfig")
            .field("alias", &self.alias)
            .field("catalog", &self.catalog)
            .field(
                "catalog_password",
                &self.catalog_password.as_ref().map(|_| "redacted"),
            )
            .field("data", &self.data)
            .field("read_only", &self.read_only)
            .field("object_store", &self.object_store)
            .finish()
    }
}

/// A `CREATE OR REPLACE PERSISTENT SECRET <name> (TYPE S3, …)`.
#[derive(Clone, PartialEq, Eq)]
pub struct ObjectStoreSecret {
    /// `r2_lake`, `aws_lake` or `bunny_lake`, as Go names them.
    pub name: &'static str,
    /// `(option, value)` in statement order.
    pub fields: Vec<(&'static str, String)>,
}

impl std::fmt::Debug for ObjectStoreSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shown: Vec<(&str, &str)> = self
            .fields
            .iter()
            .map(|(key, value)| match *key {
                "SECRET" | "SESSION_TOKEN" | "KEY_ID" => (*key, "redacted"),
                _ => (*key, value.as_str()),
            })
            .collect();
        f.debug_struct("ObjectStoreSecret")
            .field("name", &self.name)
            .field("fields", &shown)
            .finish()
    }
}

/// One bootstrap statement. Its `Debug` and every error name the step, never
/// the SQL: two of the three steps carry a credential.
#[derive(Clone, PartialEq, Eq)]
pub struct Step {
    /// What the step does, for logs and errors.
    pub name: &'static str,
    pub sql: String,
}

impl std::fmt::Debug for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Step").field("name", &self.name).finish()
    }
}

/// Why the lake configuration was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl LakeConfig {
    /// The lake warehouse the process environment asks for: `None` unless
    /// `GRAIN_DUCKLAKE` (or pinkdb's own `PINKDB_QUACKLAB_LAKE`) is truthy.
    pub fn from_env() -> Result<Option<Self>, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`LakeConfig::from_env`] over any lookup (tests pass a map).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, ConfigError> {
        let var = |name: &str| {
            get(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        if !go_truthy(get("GRAIN_DUCKLAKE").as_deref())
            && !go_truthy(get("PINKDB_QUACKLAB_LAKE").as_deref())
        {
            return Ok(None);
        }
        if go_truthy(get("DUCKLAKE_ENCRYPTED").as_deref()) {
            // Never silently unencrypted: the deployment asked for encrypted
            // data files and this build cannot give it per-lake keys.
            return Err(ConfigError(
                "DUCKLAKE_ENCRYPTED is set, but a pinkdb lake has no per-attachment encryption \
                 (DuckLake's ENCRYPTED option): set DUCKLAKE_ENCRYPTED=0 to attach an \
                 unencrypted pinkdb lake (the object store's own server-side encryption still \
                 applies)"
                    .to_string(),
            ));
        }
        let alias = var("DUCKLAKE_ALIAS").unwrap_or_else(|| crate::DEFAULT_WAREHOUSE.to_string());
        let (catalog, catalog_password) = match var("PINKDB_LAKE_CATALOG") {
            Some(location) => pinkdb::lake_db::split_catalog_password(&location),
            None => {
                let uri = var("DUCKLAKE_PG_URI").unwrap_or(DEFAULT_PG_URI.into());
                let (catalog, password) = postgres_catalog(&uri);
                match var("PINKDB_LAKE_CATALOG_SCHEMA") {
                    Some(schema) => (with_schema(&catalog, &schema)?, password),
                    None => (catalog, password),
                }
            }
        };
        let mut data = var("DUCKLAKE_DATA_PATH").unwrap_or_else(|| DEFAULT_DATA_PATH.to_string());
        if !data.ends_with('/') && !data.ends_with(std::path::MAIN_SEPARATOR) {
            data.push('/');
        }
        let object_store = if remote_data_path(&data) {
            object_store_secret(&var)
        } else {
            None
        };
        Ok(Some(Self {
            alias,
            catalog,
            catalog_password,
            data,
            read_only: go_truthy(get("DUCKDB_READ_ONLY").as_deref()),
            object_store,
        }))
    }

    /// The bootstrap statements, in order.
    pub fn steps(&self) -> Vec<Step> {
        let mut steps = Vec::new();
        if let Some(secret) = &self.object_store {
            let fields: Vec<String> = std::iter::once("TYPE s3".to_string())
                .chain(
                    secret
                        .fields
                        .iter()
                        .map(|(key, value)| format!("{key} {}", single_quoted(value))),
                )
                .collect();
            steps.push(Step {
                name: "object-store secret",
                sql: format!(
                    "CREATE OR REPLACE PERSISTENT SECRET {} ({})",
                    secret.name,
                    fields.join(", ")
                ),
            });
        }
        if let Some(password) = &self.catalog_password {
            steps.push(Step {
                name: "catalog secret",
                sql: format!(
                    "CREATE OR REPLACE PERSISTENT SECRET {} (TYPE postgres, PASSWORD {}, SCOPE {})",
                    self.catalog_secret_name(),
                    single_quoted(password),
                    single_quoted(&self.catalog)
                ),
            });
        }
        let options = if self.read_only {
            format!("DATA {}, READ_ONLY", single_quoted(&self.data))
        } else {
            format!("DATA {}, CREATE_IF_NOT_EXISTS", single_quoted(&self.data))
        };
        steps.push(Step {
            name: "attach",
            sql: format!(
                "ATTACH {} AS {} ({options})",
                single_quoted(&format!("lake:{}", self.catalog)),
                quoted(&self.alias)
            ),
        });
        steps
    }

    /// The secret the catalog's password lives in.
    fn catalog_secret_name(&self) -> String {
        let alias: String = self
            .alias
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect();
        format!("quacklab_{alias}_catalog")
    }

    /// One line for the boot log: no credential, no catalog host.
    pub fn summary(&self) -> String {
        format!(
            "mode=lake alias={} data={}{}",
            self.alias,
            self.data,
            if self.read_only { " read_only=1" } else { "" }
        )
    }
}

/// `DUCKLAKE_PG_URI` as a lake catalog: a URL keeps its scheme, a libpq
/// conninfo gets pink-lake's `postgres:` prefix; the password is split out.
fn postgres_catalog(uri: &str) -> (String, Option<String>) {
    let uri = uri.trim();
    let location = if uri.starts_with("postgres://")
        || uri.starts_with("postgresql://")
        || uri.starts_with("postgres:")
    {
        uri.to_string()
    } else {
        format!("postgres:{uri}")
    };
    pinkdb::lake_db::split_catalog_password(&location)
}

/// `PINKDB_LAKE_CATALOG_SCHEMA`: keep the lake's catalog tables in their own
/// Postgres schema of the `DUCKLAKE_PG_URI` database (pink-lake's `schema`
/// parameter), so a test drive can share QuackLab's catalog database without
/// touching the DuckLake tables in it.
fn with_schema(catalog: &str, schema: &str) -> Result<String, ConfigError> {
    if !schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        || schema.starts_with(|c: char| c.is_ascii_digit())
    {
        return Err(ConfigError(format!(
            "PINKDB_LAKE_CATALOG_SCHEMA {schema:?} is not a plain Postgres identifier"
        )));
    }
    if catalog.contains("://") {
        let separator = if catalog.contains('?') { '&' } else { '?' };
        Ok(format!("{catalog}{separator}schema={schema}"))
    } else {
        Ok(format!("{catalog} schema={schema}"))
    }
}

/// Go's `remoteDataPath`.
pub fn remote_data_path(path: &str) -> bool {
    let path = path.trim();
    ["s3://", "https://", "http://", "gs://", "az://", "gcs://"]
        .iter()
        .any(|scheme| path.starts_with(scheme))
}

/// Go's `r2S3SecretSQL`, as option pairs. An `http://` endpoint (a local S3
/// mock; R2 and Bunny are always HTTPS) adds `USE_SSL false`, which Go never
/// needs because it only runs against real object stores.
fn object_store_secret(var: &impl Fn(&str) -> Option<String>) -> Option<ObjectStoreSecret> {
    let provider = var("GRAIN_OBJECT_STORE_PROVIDER").unwrap_or_default();
    if provider.eq_ignore_ascii_case("aws") {
        let region = var("AWS_REGION").or_else(|| var("AWS_DEFAULT_REGION"))?;
        let key = var("AWS_ACCESS_KEY_ID")?;
        let secret = var("AWS_SECRET_ACCESS_KEY")?;
        let mut fields = vec![("KEY_ID", key), ("SECRET", secret)];
        if let Some(token) = var("AWS_SESSION_TOKEN") {
            fields.push(("SESSION_TOKEN", token));
        }
        fields.push(("REGION", region));
        return Some(ObjectStoreSecret {
            name: "aws_lake",
            fields,
        });
    }
    let (name, key, secret, endpoint) = if provider.eq_ignore_ascii_case("bunny") {
        (
            "bunny_lake",
            var("BUNNY_S3_KEY_ID")?,
            var("BUNNY_S3_SECRET")?,
            var("BUNNY_S3_ENDPOINT")?,
        )
    } else {
        (
            "r2_lake",
            var("R2_ACCESS_KEY_ID")?,
            var("R2_SECRET_ACCESS_KEY")?,
            var("R2_ENDPOINT")?,
        )
    };
    let plain_http = endpoint.starts_with("http://");
    let endpoint = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    if endpoint.is_empty() {
        return None;
    }
    let mut fields = vec![
        ("KEY_ID", key),
        ("SECRET", secret),
        ("ENDPOINT", endpoint),
        ("URL_STYLE", "path".to_string()),
        ("REGION", "auto".to_string()),
    ];
    if plain_http {
        fields.push(("USE_SSL", "false".to_string()));
    }
    Some(ObjectStoreSecret { name, fields })
}

fn single_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Go's `serializationConflict`, plus pinkdb's writer lease: a catalog another
/// writer still serves (its heartbeat goes stale 6 s after it stops) is worth
/// waiting for the same way.
fn retryable(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("could not serialize access")
        || message.contains("concurrent update")
        || message.contains("serialization failure")
        || lease_held(&message)
}

fn lease_held(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("is already served by")
}

/// Go's `execBootstrap` over any statement runner.
fn run_with_retry(
    mut run: impl FnMut() -> Result<(), String>,
    mut sleep: impl FnMut(Duration),
) -> Result<(), String> {
    let mut attempt = 0;
    loop {
        match run() {
            Ok(()) => return Ok(()),
            Err(message) if retryable(&message) && attempt + 1 < BOOTSTRAP_ATTEMPTS => {
                attempt += 1;
                sleep(BOOTSTRAP_BACKOFF * attempt);
            }
            Err(message) => return Err(message),
        }
    }
}

/// The lake warehouse: attached while open, detached while a lease holds it.
pub struct LakeWarehouse {
    db: Database,
    config: LakeConfig,
    open: Mutex<bool>,
}

/// Why [`LakeWarehouse::boot`] failed.
#[derive(Debug)]
pub struct BootError(pub String);

impl std::fmt::Display for BootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BootError {}

impl LakeWarehouse {
    /// Attach the lake at start. A lake another writer still serves leaves the
    /// warehouse closed (the first request attaches it once that writer is
    /// gone) instead of failing the boot; every other failure fails it, as a
    /// failed bootstrap exits the Go service.
    pub fn boot(db: Database, config: LakeConfig) -> Result<Self, BootError> {
        let warehouse = Self {
            db,
            config,
            open: Mutex::new(false),
        };
        warehouse.refuse_local_database()?;
        match warehouse.open() {
            Ok(()) => {}
            Err(message) if lease_held(&message) => {
                eprintln!(
                    "pinkdb-server: the lake warehouse is still served by another writer; \
                     /sql answers 503 until it is released: {message}"
                );
            }
            Err(message) => return Err(BootError(message)),
        }
        Ok(warehouse)
    }

    pub fn config(&self) -> &LakeConfig {
        &self.config
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, bool> {
        self.open.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The store must not already hold a *local* database of the warehouse's
    /// name (a data directory that served a local warehouse before): attaching
    /// over it is refused rather than hiding its rows.
    fn refuse_local_database(&self) -> Result<(), BootError> {
        let alias = &self.config.alias;
        if let Some(info) = self
            .db
            .database_infos()
            .into_iter()
            .find(|info| info.name.eq_ignore_ascii_case(alias))
            && info.storage != "lake"
        {
            return Err(BootError(format!(
                "lake warehouse: the store already has a local database \"{alias}\"; \
                 GRAIN_DUCKLAKE needs that name for the lake (start on a fresh data \
                 directory, or --in-memory)"
            )));
        }
        Ok(())
    }

    /// Detach the warehouse database if the store has it (a data directory
    /// replays the last attach; an unavailable one is reattached fresh).
    fn detach(&self) -> Result<(), String> {
        let alias = &self.config.alias;
        let present = self
            .db
            .database_infos()
            .into_iter()
            .any(|info| info.name.eq_ignore_ascii_case(alias));
        if !present {
            return Ok(());
        }
        self.db
            .connect()
            .execute(&format!("DETACH DATABASE IF EXISTS {}", quoted(alias)))
            .map(|_| ())
            .map_err(|e| format!("lake warehouse: detach failed: {}", engine_message(&e)))
    }

    fn attach(&self) -> Result<(), String> {
        // A store replaying its WAL may already have the lake open; reattach
        // so every open starts at the lake's head with this boot's settings.
        self.detach()?;
        let mut conn = self.db.connect();
        for step in self.config.steps() {
            run_with_retry(
                || {
                    conn.execute(&step.sql)
                        .map(|_| ())
                        .map_err(|e| engine_message(&e))
                },
                std::thread::sleep,
            )
            .map_err(|message| {
                format!(
                    "lake warehouse bootstrap failed at the {} step (DATA={}): {message}",
                    step.name, self.config.data
                )
            })?;
        }
        Ok(())
    }
}

impl Warehouse for LakeWarehouse {
    fn close(&self) -> Result<(), String> {
        let mut open = self.lock();
        if *open {
            self.detach()?;
            *open = false;
        }
        Ok(())
    }

    fn open(&self) -> Result<(), String> {
        let mut open = self.lock();
        if !*open {
            self.attach()?;
            *open = true;
        }
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), String> {
        self.open()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(pairs: &[(&str, &str)]) -> Result<Option<LakeConfig>, ConfigError> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        LakeConfig::from_lookup(|name| map.get(name).cloned())
    }

    #[test]
    fn the_flag_is_read_the_way_go_reads_it() {
        assert!(config(&[]).unwrap().is_none());
        for off in ["0", "false", "on", "no", ""] {
            assert!(
                config(&[("GRAIN_DUCKLAKE", off)]).unwrap().is_none(),
                "{off}"
            );
        }
        for on in ["1", "true", "TRUE", " yes "] {
            assert!(config(&[("GRAIN_DUCKLAKE", on)]).unwrap().is_some(), "{on}");
        }
        assert!(config(&[("PINKDB_QUACKLAB_LAKE", "1")]).unwrap().is_some());
    }

    #[test]
    fn go_defaults_apply() {
        let c = config(&[("GRAIN_DUCKLAKE", "1")]).unwrap().unwrap();
        assert_eq!(c.alias, "warehouse");
        assert_eq!(c.data, "/opt/quacklab/data/lake/");
        assert_eq!(
            c.catalog,
            "postgres:dbname=ducklake_catalog host=127.0.0.1 port=5432 user=ducklake"
        );
        assert_eq!(c.catalog_password.as_deref(), Some("ducklake"));
        assert!(!c.read_only);
        assert!(
            c.object_store.is_none(),
            "a local data path needs no secret"
        );
    }

    #[test]
    fn encrypted_is_refused_not_ignored() {
        let err = config(&[("GRAIN_DUCKLAKE", "1"), ("DUCKLAKE_ENCRYPTED", "1")]).unwrap_err();
        assert!(err.0.contains("DUCKLAKE_ENCRYPTED=0"), "{err}");
        assert!(
            config(&[("GRAIN_DUCKLAKE", "1"), ("DUCKLAKE_ENCRYPTED", "0")])
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn a_url_catalog_keeps_its_scheme_and_loses_its_password() {
        let c = config(&[
            ("GRAIN_DUCKLAKE", "1"),
            (
                "DUCKLAKE_PG_URI",
                "postgresql://lake:s%40cret@db.example/cat?sslmode=require",
            ),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            c.catalog,
            "postgresql://lake@db.example/cat?sslmode=require"
        );
        assert_eq!(c.catalog_password.as_deref(), Some("s@cret"));
    }

    #[test]
    fn a_catalog_schema_keeps_the_lake_apart() {
        let conninfo = config(&[
            ("GRAIN_DUCKLAKE", "1"),
            (
                "DUCKLAKE_PG_URI",
                "dbname=cat host=h port=5432 user=u password=p sslmode=require",
            ),
            ("PINKDB_LAKE_CATALOG_SCHEMA", "pinkdb_lake"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            conninfo.catalog,
            "postgres:dbname=cat host=h port=5432 user=u sslmode=require schema=pinkdb_lake"
        );
        let url = config(&[
            ("GRAIN_DUCKLAKE", "1"),
            ("DUCKLAKE_PG_URI", "postgres://u:p@h/cat?sslmode=require"),
            ("PINKDB_LAKE_CATALOG_SCHEMA", "pinkdb_lake"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            url.catalog,
            "postgres://u@h/cat?sslmode=require&schema=pinkdb_lake"
        );
        assert!(
            config(&[
                ("GRAIN_DUCKLAKE", "1"),
                ("PINKDB_LAKE_CATALOG_SCHEMA", "x; drop"),
            ])
            .is_err()
        );
    }

    #[test]
    fn the_pinkdb_catalog_variable_wins() {
        let c = config(&[
            ("GRAIN_DUCKLAKE", "1"),
            ("DUCKLAKE_PG_URI", "host=x password=y"),
            ("PINKDB_LAKE_CATALOG", "sqlite:/tmp/cat.sqlite"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(c.catalog, "sqlite:/tmp/cat.sqlite");
        assert!(c.catalog_password.is_none());
    }

    fn secret_of(pairs: &[(&str, &str)]) -> Option<ObjectStoreSecret> {
        let mut all = vec![
            ("GRAIN_DUCKLAKE", "1"),
            ("DUCKLAKE_DATA_PATH", "s3://lake/env"),
        ];
        all.extend_from_slice(pairs);
        config(&all).unwrap().unwrap().object_store
    }

    #[test]
    fn the_r2_secret_matches_go() {
        let s = secret_of(&[
            ("R2_ACCESS_KEY_ID", "k"),
            ("R2_SECRET_ACCESS_KEY", "s"),
            ("R2_ENDPOINT", "https://acct.r2.cloudflarestorage.com/"),
        ])
        .unwrap();
        assert_eq!(s.name, "r2_lake");
        assert_eq!(
            s.fields,
            vec![
                ("KEY_ID", "k".to_string()),
                ("SECRET", "s".to_string()),
                ("ENDPOINT", "acct.r2.cloudflarestorage.com".to_string()),
                ("URL_STYLE", "path".to_string()),
                ("REGION", "auto".to_string()),
            ]
        );
        // Any missing variable skips the secret, as in Go.
        assert!(secret_of(&[("R2_ACCESS_KEY_ID", "k"), ("R2_SECRET_ACCESS_KEY", "s")]).is_none());
        // A plain-HTTP mock endpoint keeps working.
        let mock = secret_of(&[
            ("R2_ACCESS_KEY_ID", "k"),
            ("R2_SECRET_ACCESS_KEY", "s"),
            ("R2_ENDPOINT", "http://127.0.0.1:9000"),
        ])
        .unwrap();
        assert!(mock.fields.contains(&("USE_SSL", "false".to_string())));
    }

    #[test]
    fn the_aws_and_bunny_secrets_match_go() {
        let aws = secret_of(&[
            ("GRAIN_OBJECT_STORE_PROVIDER", "AWS"),
            ("AWS_DEFAULT_REGION", "eu-west-1"),
            ("AWS_ACCESS_KEY_ID", "k"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
            ("AWS_SESSION_TOKEN", "t"),
        ])
        .unwrap();
        assert_eq!(aws.name, "aws_lake");
        assert_eq!(
            aws.fields.iter().map(|f| f.0).collect::<Vec<_>>(),
            ["KEY_ID", "SECRET", "SESSION_TOKEN", "REGION"]
        );
        // AWS needs a region; R2 variables don't stand in for it.
        assert!(
            secret_of(&[
                ("GRAIN_OBJECT_STORE_PROVIDER", "aws"),
                ("AWS_ACCESS_KEY_ID", "k"),
                ("AWS_SECRET_ACCESS_KEY", "s"),
            ])
            .is_none()
        );
        let bunny = secret_of(&[
            ("GRAIN_OBJECT_STORE_PROVIDER", "bunny"),
            ("BUNNY_S3_KEY_ID", "zone"),
            ("BUNNY_S3_SECRET", "pw"),
            ("BUNNY_S3_ENDPOINT", "https://de-s3.storage.bunnycdn.com"),
        ])
        .unwrap();
        assert_eq!(bunny.name, "bunny_lake");
        assert!(
            bunny
                .fields
                .contains(&("ENDPOINT", "de-s3.storage.bunnycdn.com".to_string()))
        );
    }

    #[test]
    fn steps_are_built_in_go_order_and_never_print_a_credential() {
        let c = config(&[
            ("GRAIN_DUCKLAKE", "1"),
            ("DUCKLAKE_ALIAS", "wh"),
            ("DUCKLAKE_DATA_PATH", "s3://lake/env"),
            ("DUCKLAKE_PG_URI", "host=h dbname=d user=u password='p w'"),
            ("R2_ACCESS_KEY_ID", "AKID"),
            ("R2_SECRET_ACCESS_KEY", "TOPSECRET"),
            ("R2_ENDPOINT", "acct.r2.example"),
        ])
        .unwrap()
        .unwrap();
        let steps = c.steps();
        let names: Vec<&str> = steps.iter().map(|s| s.name).collect();
        assert_eq!(names, ["object-store secret", "catalog secret", "attach"]);
        assert!(steps[0].sql.starts_with("CREATE OR REPLACE PERSISTENT SECRET r2_lake (TYPE s3, KEY_ID 'AKID', SECRET 'TOPSECRET'"));
        assert_eq!(
            steps[1].sql,
            "CREATE OR REPLACE PERSISTENT SECRET quacklab_wh_catalog (TYPE postgres, \
             PASSWORD 'p w', SCOPE 'postgres:host=h dbname=d user=u')"
        );
        assert_eq!(
            steps[2].sql,
            "ATTACH 'lake:postgres:host=h dbname=d user=u' AS \"wh\" \
             (DATA 's3://lake/env/', CREATE_IF_NOT_EXISTS)"
        );
        let shown = format!("{steps:?} {c:?} {}", c.summary());
        for credential in ["AKID", "TOPSECRET", "p w"] {
            assert!(!shown.contains(credential), "{shown}");
        }
    }

    #[test]
    fn read_only_attaches_read_only() {
        let c = config(&[
            ("GRAIN_DUCKLAKE", "1"),
            ("DUCKDB_READ_ONLY", "true"),
            ("PINKDB_LAKE_CATALOG", "sqlite:/c.sqlite"),
            ("DUCKLAKE_DATA_PATH", "/d"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            c.steps().last().unwrap().sql,
            "ATTACH 'lake:sqlite:/c.sqlite' AS \"warehouse\" (DATA '/d/', READ_ONLY)"
        );
    }

    #[test]
    fn conflicts_are_retried_like_go_and_other_errors_are_not() {
        let mut calls = 0;
        let mut slept = Vec::new();
        let result = run_with_retry(
            || {
                calls += 1;
                if calls < 3 {
                    Err("ERROR: could not serialize access due to concurrent update".into())
                } else {
                    Ok(())
                }
            },
            |d| slept.push(d),
        );
        assert!(result.is_ok());
        assert_eq!(calls, 3);
        assert_eq!(
            slept,
            [Duration::from_millis(200), Duration::from_millis(400)]
        );

        let mut calls = 0;
        let result = run_with_retry(
            || {
                calls += 1;
                Err("Catalog Error: no such lake".into())
            },
            |_| {},
        );
        assert!(result.is_err());
        assert_eq!(calls, 1);

        let mut calls = 0;
        let result = run_with_retry(
            || {
                calls += 1;
                Err("the lake is already served by h:1:1 (heartbeat 0.5s ago)".into())
            },
            |_| {},
        );
        assert!(result.is_err());
        assert_eq!(calls, BOOTSTRAP_ATTEMPTS);
    }
}
