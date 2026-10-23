//! The Postgres catalog store (wave 96a).
//!
//! The same two tables and the same primitive semantics as the SQLite store
//! ([`super::sqlite`]), one lake per Postgres **schema**: `pink_lake_meta`
//! holds the lake's facts (`schema_version`, `lake_id`, `data_location`,
//! `created_at`, `moved_to`) and `pink_lake_objects` holds every metadata
//! object as a row — path, kind, version, the exact object bytes (a sealed
//! `PNKLAKE1` blob for an encrypted lake), a monotonically bumped tag and an
//! update time.
//!
//! The URL's `?schema=<name>` selects the schema (default `public`). It is
//! stripped before the `postgres` client sees the URL because libpq has no
//! `schema` parameter; every table reference is schema-qualified instead.
//!
//! The publish scope is one `BEGIN` plus `pg_advisory_xact_lock(<key>)` on a
//! dedicated connection. The advisory lock is keyed by the schema name, so
//! lakes in one database do not contend; it is used instead of
//! `SELECT … FOR UPDATE` because `catalog.json` does not exist yet during a
//! create, so there is no row to lock. Every publication's commit delta,
//! snapshot, catalog CAS and highest marker land atomically or not at all.
//!
//! `put_new` is `INSERT … ON CONFLICT (path) DO NOTHING` and checks the
//! affected-row count: a caught unique violation would abort the surrounding
//! transaction, which is exactly what a concurrent writer must not do.
//!
//! TLS uses the workspace's rustls + ring + webpki roots. `verify-full`
//! verifies the chain and hostname (against `sslrootcert=<pem file>` when
//! given, else the webpki roots), `sslmode=require` encrypts without
//! certificate verification (libpq semantics, explicit opt-in only), and
//! `disable` is allowed only for loopback hosts. The default is `verify-full`
//! for remote hosts and `disable` for loopback. Passwords come from the URL or `PGPASSWORD`; every
//! error and `abs()` renders the URL with the password replaced by `***`.

use std::cell::RefCell;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use postgres::config::SslMode;
use postgres::error::SqlState;
use postgres::{Client, Config, NoTls};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::CatalogStore;
use super::url::{PostgresUrl, redact_location};
use crate::local::CATALOG_FILE;
use crate::storage::{Head, LocalCopy, ObjectInfo, PublishScope, Storage, VersionTag};
use crate::{HIGHEST_FILE, LAKE_FILE, LakeError, Result};

/// The catalog schema this build writes. A row with another value is a newer
/// (or unrelated) database: refused, never guessed at.
const SCHEMA_VERSION: i64 = 1;

/// One live publish transaction on this thread. Mirrors the SQLite store's
/// thread-local slot: a thread runs at most one store's publication at a
/// time (the lake code is synchronous).
struct ActiveTx {
    store_id: u64,
    conn: Client,
}

thread_local! {
    static ACTIVE_TX: RefCell<Option<ActiveTx>> = const { RefCell::new(None) };
}

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);
static INSTALL_PROVIDER: Once = Once::new();

/// The URL in its two safe-to-keep forms plus the passwords that must never
/// appear in an error string.
struct UrlInfo {
    raw: String,
    redacted: String,
    passwords: Vec<String>,
}

impl UrlInfo {
    fn new(url: &PostgresUrl) -> Self {
        Self {
            raw: url.connection_url(),
            redacted: url.redacted(),
            passwords: url
                .password_spellings()
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }

    /// Scrub every password spelling and the raw URL out of a client error.
    /// The `postgres` client does not print passwords today, but the
    /// invariant is enforced here rather than trusted.
    fn scrub(&self, text: &str) -> String {
        let mut out = text.replace(&self.raw, &self.redacted);
        for password in &self.passwords {
            if !password.is_empty() {
                out = out.replace(password, "***");
            }
        }
        out
    }

    fn io_error(&self, context: &str, error: postgres::Error) -> LakeError {
        let message = self.scrub(&error.to_string());
        LakeError::Io {
            context: format!("{context} ({message})"),
            source: std::io::Error::other(message),
        }
    }

    fn bad_location(&self, message: impl fmt::Display) -> LakeError {
        LakeError::BadLocation {
            location: self.redacted.clone(),
            message: message.to_string(),
        }
    }
}

struct Inner {
    id: u64,
    url: PostgresUrl,
    info: UrlInfo,
    meta: String,
    objects: String,
    /// `pg_advisory_xact_lock` key derived from the schema name.
    lock_key: i64,
    client: Mutex<Client>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("id", &self.id)
            .field("url", &self.info.redacted)
            .finish()
    }
}

/// The Postgres catalog backend.
#[derive(Debug)]
pub(crate) struct PostgresCatalog {
    inner: Arc<Inner>,
}

impl PostgresCatalog {
    /// Open the one lake in the URL's schema. With `create`, an absent schema
    /// and the two tables are created. Without it, a schema without **both**
    /// tables is `NotALake` and nothing is created — a mistyped open never
    /// leaves an empty schema behind.
    pub(crate) fn open(url: &str, create: bool) -> Result<Self> {
        let parsed = PostgresUrl::parse(url).map_err(|message| LakeError::BadLocation {
            location: redact_location(url),
            message,
        })?;
        let info = UrlInfo::new(&parsed);
        let policy = tls_policy(&parsed, &info)?;
        let mut client = connect(&parsed, &info, policy)?;
        let schema_ident = quote_ident(parsed.schema());
        let meta = format!("{schema_ident}.pink_lake_meta");
        let objects = format!("{schema_ident}.pink_lake_objects");
        if create {
            let ddl = format!(
                "CREATE SCHEMA IF NOT EXISTS {schema_ident};\n\
                 CREATE TABLE IF NOT EXISTS {meta} (\n\
                     key   TEXT PRIMARY KEY,\n\
                     value TEXT NOT NULL\n\
                 );\n\
                 CREATE TABLE IF NOT EXISTS {objects} (\n\
                     path       TEXT PRIMARY KEY,\n\
                     kind       TEXT NOT NULL,\n\
                     version    BIGINT,\n\
                     bytes      BYTEA NOT NULL,\n\
                     tag        BIGINT NOT NULL,\n\
                     updated_at BIGINT NOT NULL\n\
                 );\n\
                 CREATE INDEX IF NOT EXISTS pink_lake_objects_kind_version\n\
                     ON {objects} (kind, version);"
            );
            create_schema(&mut client, &ddl, &info)?;
        } else {
            let meta_exists = table_exists(&mut client, parsed.schema(), "pink_lake_meta")
                .map_err(|e| info.io_error("checking the Postgres catalog schema", e))?;
            let objects_exist = table_exists(&mut client, parsed.schema(), "pink_lake_objects")
                .map_err(|e| info.io_error("checking the Postgres catalog schema", e))?;
            if !meta_exists || !objects_exist {
                return Err(LakeError::NotALake {
                    path: PathBuf::from(info.redacted.clone()),
                });
            }
        }
        let store = Self {
            inner: Arc::new(Inner {
                id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
                lock_key: advisory_key(parsed.schema()),
                url: parsed,
                info,
                meta,
                objects,
                client: Mutex::new(client),
            }),
        };
        store.init_meta()?;
        Ok(store)
    }

    fn init_meta(&self) -> Result<()> {
        let existing = self.query_meta("schema_version")?;
        match existing {
            Some(value) => {
                if value != SCHEMA_VERSION.to_string() {
                    let found = value.parse::<u32>().unwrap_or(0);
                    return Err(LakeError::Unsupported {
                        path: PathBuf::from(self.inner.info.redacted.clone()),
                        found,
                    });
                }
            }
            None => self.put_meta("schema_version", &SCHEMA_VERSION.to_string())?,
        }
        if self.query_meta("created_at")?.is_none() {
            self.put_meta("created_at", &now_micros().to_string())?;
        }
        Ok(())
    }

    fn query_meta(&self, key: &str) -> Result<Option<String>> {
        let sql = format!("SELECT value FROM {} WHERE key = $1", self.inner.meta);
        self.with_conn(|conn| {
            conn.query_opt(&sql, &[&key])
                .map(|row| row.map(|row| row.get::<_, String>(0)))
                .map_err(|e| self.inner.info.io_error("reading pink_lake_meta", e))
        })
    }

    fn put_meta(&self, key: &str, value: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} (key, value) VALUES ($1, $2)\n\
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            self.inner.meta
        );
        self.with_conn(|conn| {
            conn.execute(&sql, &[&key, &value])
                .map(|_| ())
                .map_err(|e| self.inner.info.io_error("writing pink_lake_meta", e))
        })
    }

    /// Run `f` on the active publish transaction when this store owns one on
    /// this thread, else on the shared connection. The publish connection is
    /// exclusive to the publishing thread by construction.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Client) -> Result<T>) -> Result<T> {
        ACTIVE_TX.with(|slot| {
            let mut borrowed = slot.borrow_mut();
            if let Some(tx) = borrowed.as_mut()
                && tx.store_id == self.inner.id
            {
                return f(&mut tx.conn);
            }
            drop(borrowed);
            let mut conn = self
                .inner
                .client
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            f(&mut conn)
        })
    }

    /// Open the transaction connection for a publication: a fresh connection
    /// (so the shared one stays usable) that takes the schema's advisory lock.
    fn open_transaction(&self) -> Result<Client> {
        let mut conn = connect(&self.inner.url, &self.inner.info, {
            // The policy was already validated at open; recompute from the
            // same URL so the transaction connection has the same TLS mode.
            tls_policy(&self.inner.url, &self.inner.info)?
        })?;
        conn.batch_execute("BEGIN").map_err(|e| {
            self.inner
                .info
                .io_error("beginning the catalog transaction", e)
        })?;
        conn.query_one("SELECT pg_advisory_xact_lock($1)", &[&self.inner.lock_key])
            .map_err(|e| {
                self.inner
                    .info
                    .io_error("taking the catalog advisory lock", e)
            })?;
        Ok(conn)
    }

    fn path_of(&self, rel: &str) -> PathBuf {
        catalog_abs(&self.inner.info.redacted, rel)
    }

    /// `list`/`list_recursive` over the row paths. `direct` keeps only the
    /// remainder without a `/` (the manifest-facing direct-children listing).
    fn list_prefix(&self, rel: &str, direct: bool) -> Result<Vec<String>> {
        let prefix = if rel.is_empty() {
            String::new()
        } else {
            format!("{}/", rel.trim_end_matches('/'))
        };
        let sql = format!(
            "SELECT path FROM {} WHERE starts_with(path, $1) ORDER BY path",
            self.inner.objects
        );
        self.with_conn(|conn| {
            let rows = conn
                .query(&sql, &[&prefix])
                .map_err(|e| self.inner.info.io_error("listing catalog objects", e))?;
            let mut out = Vec::new();
            for row in rows {
                let path: String = row.get(0);
                let Some(tail) = path.strip_prefix(&prefix) else {
                    continue;
                };
                if direct && tail.contains('/') {
                    continue;
                }
                out.push(tail.to_string());
            }
            Ok(out)
        })
    }
}

impl Storage for PostgresCatalog {
    fn abs(&self, rel: &str) -> PathBuf {
        self.path_of(rel)
    }

    fn head(&self, rel: &str) -> Result<Head> {
        let sql = format!("SELECT 1 FROM {} WHERE path = $1", self.inner.objects);
        self.with_conn(|conn| {
            let exists = conn
                .query_opt(&sql, &[&rel])
                .map_err(|e| self.inner.info.io_error("checking a catalog object", e))?
                .is_some();
            Ok(if exists { Head::File } else { Head::Missing })
        })
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        let sql = format!(
            "SELECT length(bytes)::bigint, updated_at FROM {} WHERE path = $1",
            self.inner.objects
        );
        self.with_conn(|conn| {
            conn.query_opt(&sql, &[&rel])
                .map(|row| {
                    row.map(|row| {
                        let size: i64 = row.get(0);
                        let updated: i64 = row.get(1);
                        ObjectInfo {
                            size: u64::try_from(size).unwrap_or(0),
                            modified_micros: Some(updated),
                        }
                    })
                })
                .map_err(|e| {
                    self.inner
                        .info
                        .io_error("reading a catalog object's size", e)
                })
        })
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        self.read_optional(rel, context)?
            .ok_or_else(|| missing(self.abs(rel)))
    }

    fn read_optional(&self, rel: &str, _context: &str) -> Result<Option<Vec<u8>>> {
        let sql = format!("SELECT bytes FROM {} WHERE path = $1", self.inner.objects);
        self.with_conn(|conn| {
            conn.query_opt(&sql, &[&rel])
                .map(|row| row.map(|row| row.get::<_, Vec<u8>>(0)))
                .map_err(|e| self.inner.info.io_error("reading a catalog object", e))
        })
    }

    fn read_range(&self, _rel: &str, _offset: u64, _len: u64, _context: &str) -> Result<Vec<u8>> {
        Err(LakeError::Internal {
            message: "ranged reads are a data-location operation; the catalog store is never \
                      range-read"
                .to_string(),
        })
    }

    fn read_tagged(&self, rel: &str, _context: &str) -> Result<(Vec<u8>, VersionTag)> {
        let sql = format!(
            "SELECT bytes, tag FROM {} WHERE path = $1",
            self.inner.objects
        );
        self.with_conn(|conn| {
            let row = conn
                .query_opt(&sql, &[&rel])
                .map_err(|e| {
                    self.inner
                        .info
                        .io_error("reading a catalog object's tag", e)
                })?
                .ok_or_else(|| missing(self.abs(rel)))?;
            let bytes: Vec<u8> = row.get(0);
            let tag: i64 = row.get(1);
            Ok((bytes, VersionTag(tag.to_string())))
        })
    }

    fn read_with_stored(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let bytes = self.read(rel, context)?;
        Ok((bytes.clone(), bytes))
    }

    fn create_dir(&self, _rel: &str) -> Result<()> {
        // A catalog has keys, not directories.
        Ok(())
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let (kind, version) = kind_version(rel);
        let sql = format!(
            "INSERT INTO {} AS t (path, kind, version, bytes, tag, updated_at)\n\
             VALUES ($1, $2, $3, $4, 1, $5)\n\
             ON CONFLICT (path) DO UPDATE SET\n\
                 kind = EXCLUDED.kind,\n\
                 version = EXCLUDED.version,\n\
                 bytes = EXCLUDED.bytes,\n\
                 tag = t.tag + 1,\n\
                 updated_at = EXCLUDED.updated_at",
            self.inner.objects
        );
        self.with_conn(|conn| {
            conn.execute(&sql, &[&rel, &kind, &version, &bytes, &now_micros()])
                .map(|_| ())
                .map_err(|e| self.inner.info.io_error("replacing a catalog object", e))
        })
    }

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        let (kind, version) = kind_version(rel);
        let sql = format!(
            "INSERT INTO {} (path, kind, version, bytes, tag, updated_at)\n\
             VALUES ($1, $2, $3, $4, 1, $5)\n\
             ON CONFLICT (path) DO NOTHING",
            self.inner.objects
        );
        self.with_conn(|conn| {
            conn.execute(&sql, &[&rel, &kind, &version, &bytes, &now_micros()])
                .map(|changed| changed == 1)
                .map_err(|e| self.inner.info.io_error("creating a catalog object", e))
        })
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        let Ok(tag) = expected.0.parse::<i64>() else {
            // A tag that never came from this store cannot match any row.
            return Ok(false);
        };
        let sql = format!(
            "UPDATE {} SET bytes = $1, tag = tag + 1, updated_at = $2\n\
             WHERE path = $3 AND tag = $4",
            self.inner.objects
        );
        self.with_conn(|conn| {
            conn.execute(&sql, &[&bytes, &now_micros(), &rel, &tag])
                .map(|changed| changed == 1)
                .map_err(|e| {
                    self.inner
                        .info
                        .io_error("replacing a catalog object conditionally", e)
                })
        })
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        self.list_prefix(rel, true)
    }

    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        self.list_prefix(rel, false)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        let sql = format!("DELETE FROM {} WHERE path = $1", self.inner.objects);
        self.with_conn(|conn| {
            conn.execute(&sql, &[&rel])
                .map(|_| ())
                .map_err(|e| self.inner.info.io_error("deleting a catalog object", e))
        })
    }

    fn local_copy(&self, rel: &str, _context: &str) -> Result<LocalCopy> {
        Err(LakeError::Internal {
            message: format!(
                "the catalog object {} is not a data file; local copies are a data-location \
                 operation",
                self.abs(rel).display()
            ),
        })
    }

    fn local_path(&self, _rel: &str) -> Option<PathBuf> {
        None
    }

    fn reconcile_snapshot(&self, _version: u64, _bytes: &[u8]) -> Result<()> {
        // Conditional writes are transactional; nothing to repair.
        Ok(())
    }

    fn uses_degraded_fallback(&self) -> bool {
        false
    }

    fn begin_publish(&self) -> Result<PublishScope> {
        let already_active = ACTIVE_TX.with(|slot| match slot.borrow().as_ref() {
            Some(tx) if tx.store_id == self.inner.id => Ok(true),
            Some(_) => Err(LakeError::Internal {
                message: "another catalog store already has an open publish scope on this thread"
                    .to_string(),
            }),
            None => Ok(false),
        })?;
        if already_active {
            // A nested publication is part of the outer transaction; only the
            // outermost scope commits or rolls back.
            return Ok(PublishScope::noop());
        }
        let conn = self.open_transaction()?;
        let store_id = self.inner.id;
        ACTIVE_TX.with(|slot| {
            *slot.borrow_mut() = Some(ActiveTx { store_id, conn });
        });
        Ok(PublishScope::new(Box::new(move |commit| {
            finish_transaction(store_id, commit)
        })))
    }

    fn catalog_split(&self) -> bool {
        true
    }

    fn seal_manifest(&self, plain: &[u8], _lake_id: &str, _version: u64) -> Result<Vec<u8>> {
        Ok(plain.to_vec())
    }

    fn tighten_existing(&self) -> Result<()> {
        Ok(())
    }
}

impl CatalogStore for PostgresCatalog {
    fn data_location(&self) -> Result<Option<String>> {
        self.query_meta("data_location")
    }

    fn set_data_location(&self, location: &str) -> Result<()> {
        self.put_meta("data_location", location)
    }

    fn set_lake_id(&self, lake_id: &str) -> Result<()> {
        self.put_meta("lake_id", lake_id)
    }

    fn lake_id(&self) -> Result<Option<String>> {
        self.query_meta("lake_id")
    }

    fn moved_to(&self) -> Result<Option<String>> {
        self.query_meta("moved_to")
    }

    fn set_moved_to(&self, target: &str) -> Result<()> {
        self.put_meta("moved_to", target)
    }
}

/// Commit or roll back the thread's active transaction. Runs from
/// `PublishScope` (`commit()` or drop).
fn finish_transaction(store_id: u64, commit: bool) -> Result<()> {
    ACTIVE_TX.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(tx) = slot.take() else {
            return Ok(());
        };
        if tx.store_id != store_id {
            *slot = Some(tx);
            return Err(LakeError::Internal {
                message: "a publish scope finished another store's transaction".to_string(),
            });
        }
        let mut conn = tx.conn;
        if commit {
            conn.batch_execute("COMMIT").map_err(|e| LakeError::Io {
                context: format!("committing the Postgres catalog transaction: {e}"),
                source: std::io::Error::other(e.to_string()),
            })
        } else {
            let _ = conn.batch_execute("ROLLBACK");
            Ok(())
        }
    })
}

/// The TLS mode one connection uses.
#[derive(Clone, Copy, Debug)]
enum TlsPolicy {
    Disable,
    /// Encrypt, but do not verify the certificate (libpq `sslmode=require`;
    /// only when asked for explicitly).
    Require,
    /// Verify the chain and the hostname (`sslmode=verify-full`, the default
    /// for a non-local host).
    VerifyFull,
}

/// Resolve `sslmode` (or the local/remote default) before any connection is
/// attempted, so a refused `disable` on a remote host is a clear, immediate
/// error — and never needs the network.
fn tls_policy(url: &PostgresUrl, info: &UrlInfo) -> Result<TlsPolicy> {
    match url.sslmode() {
        Some("disable") => {
            if url.is_local() {
                Ok(TlsPolicy::Disable)
            } else {
                Err(info.bad_location(format!(
                    "sslmode=disable is refused for the non-local host {:?}: use \
                     sslmode=require or sslmode=verify-full (plaintext is allowed only for \
                     localhost, 127.0.0.1 and ::1)",
                    url.host()
                )))
            }
        }
        Some("require") => Ok(TlsPolicy::Require),
        Some("verify-full") => Ok(TlsPolicy::VerifyFull),
        Some(other) => Err(info.bad_location(format!(
            "unsupported sslmode {other:?}; use require, verify-full or disable"
        ))),
        None if url.is_local() => Ok(TlsPolicy::Disable),
        // A remote catalog holds the lake's whole metadata and receives the
        // password: by default the server must prove who it is. Encrypt-only
        // (`sslmode=require`) is an explicit opt-in, never the default.
        None => Ok(TlsPolicy::VerifyFull),
    }
}

/// The trust anchors for `verify-full`: the CA certificates in
/// `sslrootcert=<pem file>` when given (they replace the public roots, as in
/// libpq — a private CA such as a cloud provider's), else the webpki roots.
fn root_store(url: &PostgresUrl, info: &UrlInfo) -> Result<rustls::RootCertStore> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    let Some(path) = url.sslrootcert() else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        return Ok(roots);
    };
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<std::result::Result<Vec<_>, _>>())
        .map_err(|e| info.bad_location(format!("cannot read sslrootcert {path:?}: {e}")))?;
    let (added, _ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err(info.bad_location(format!(
            "sslrootcert {path:?} holds no usable CA certificate"
        )));
    }
    Ok(roots)
}

/// Run the idempotent catalog DDL, retrying a concurrent first-open race.
///
/// PostgreSQL's `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT
/// EXISTS` can still fail with a unique violation when two connections race
/// on the same fresh schema (the existence check and the insert are not
/// atomic). The DDL is idempotent, so rerunning it after the loser's failed
/// attempt succeeds. These are the only two SQLSTATEs retried.
fn create_schema(client: &mut Client, ddl: &str, info: &UrlInfo) -> Result<()> {
    let mut attempts = 0;
    loop {
        match client.batch_execute(ddl) {
            Ok(()) => return Ok(()),
            Err(e)
                if attempts < 4
                    && matches!(
                        e.code(),
                        Some(code)
                            if *code == SqlState::DUPLICATE_SCHEMA
                                || *code == SqlState::UNIQUE_VIOLATION
                    ) =>
            {
                attempts += 1;
            }
            Err(e) => {
                return Err(info.io_error("creating the Postgres catalog schema", e));
            }
        }
    }
}

fn connect(url: &PostgresUrl, info: &UrlInfo, policy: TlsPolicy) -> Result<Client> {
    let connection = url.connection_url();
    let mut config: Config = connection
        .parse()
        .map_err(|_| info.bad_location("the URL is not a valid Postgres connection string"))?;
    if url.password().is_none()
        && let Ok(password) = std::env::var("PGPASSWORD")
        && !password.is_empty()
    {
        config.password(&password);
    }
    config.application_name("pinkdb-lake");
    config.connect_timeout(Duration::from_secs(10));
    match policy {
        TlsPolicy::Disable => {
            config.ssl_mode(SslMode::Disable);
            config
                .connect(NoTls)
                .map_err(|e| info.io_error("connecting to the Postgres catalog", e))
        }
        TlsPolicy::Require | TlsPolicy::VerifyFull => {
            config.ssl_mode(SslMode::Require);
            INSTALL_PROVIDER.call_once(|| {
                let _ = rustls::crypto::ring::default_provider().install_default();
            });
            let builder = rustls::ClientConfig::builder();
            let tls = match policy {
                TlsPolicy::Require => {
                    let schemes = rustls::crypto::ring::default_provider()
                        .signature_verification_algorithms
                        .supported_schemes();
                    builder
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification {
                            schemes,
                        }))
                        .with_no_client_auth()
                }
                TlsPolicy::VerifyFull => builder
                    .with_root_certificates(root_store(url, info)?)
                    .with_no_client_auth(),
                TlsPolicy::Disable => unreachable!("handled above"),
            };
            config
                .connect(MakeRustlsConnect::new(tls))
                .map_err(|e| info.io_error("connecting to the Postgres catalog over TLS", e))
        }
    }
}

/// `sslmode=require` encrypts but does not verify (libpq's documented
/// semantics). The trait is used only for that mode; `verify-full` uses the
/// webpki verifier.
#[derive(Debug)]
struct NoCertificateVerification {
    schemes: Vec<SignatureScheme>,
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

/// The catalog URL plus the object path, for error messages:
/// `postgres://user:***@host/db?schema=s#rel`.
fn catalog_abs(redacted_url: &str, rel: &str) -> PathBuf {
    if rel.is_empty() {
        PathBuf::from(redacted_url)
    } else {
        PathBuf::from(format!("{redacted_url}#{rel}"))
    }
}

fn table_exists(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> std::result::Result<bool, postgres::Error> {
    Ok(client
        .query_opt(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2",
            &[&schema, &table],
        )?
        .is_some())
}

/// Double-quote a schema identifier. The name is validated by
/// [`PostgresUrl`] first, so this is defense in depth.
fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// A stable 64-bit advisory-lock key from the schema name (FNV-1a). It must
/// be identical across processes, so it is not `DefaultHasher`.
fn advisory_key(schema: &str) -> i64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in schema.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// The catalog row's `kind`/`version` columns, derived from the path the way
/// the object layout encodes them. `version` is only meaningful for snapshot
/// and commit rows.
fn kind_version(rel: &str) -> (&'static str, Option<i64>) {
    if rel == LAKE_FILE {
        ("lake", None)
    } else if rel == CATALOG_FILE {
        ("catalog", None)
    } else if rel == HIGHEST_FILE {
        ("highest", None)
    } else if let Some(version) = snapshot_version(rel) {
        ("snapshot", Some(version as i64))
    } else if let Some(version) = commit_version(rel) {
        ("commit", Some(version as i64))
    } else if rel.starts_with("replication/") {
        ("replication", None)
    } else {
        ("metadata", None)
    }
}

fn snapshot_version(rel: &str) -> Option<u64> {
    let stem = rel.strip_prefix("snapshots/")?.strip_suffix(".json")?;
    crate::canonical_version_number(stem)
}

fn commit_version(rel: &str) -> Option<u64> {
    let stem = rel.strip_prefix("commits/")?.strip_suffix(".json")?;
    crate::canonical_version_number(stem)
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn missing(path: PathBuf) -> LakeError {
    LakeError::Io {
        context: format!("reading {}", path.display()),
        source: std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no catalog object at {}", path.display()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every error a bad catalog URL produces redacts the password. The cases
    /// fail before connecting where possible, so the test needs no server.
    #[test]
    fn errors_never_contain_the_password() {
        let password = "hunter2";
        for raw in [
            // `disable` on a remote host: refused before the network.
            format!("postgres://alice:{password}@db.example.com/lakes?sslmode=disable"),
            // Unsupported sslmode.
            format!("postgres://alice:{password}@db.example.com/lakes?sslmode=bogus"),
            // Invalid schema name.
            format!("postgres://alice:{password}@db.example.com/lakes?schema=not-a-schema"),
            // Invalid port: a parse error, not a connection.
            format!("postgres://alice:{password}@db.example.com:99999/lakes"),
            // A percent-encoded password: both spellings are scrubbed.
            "postgres://alice:hunter%32@db.example.com/lakes?sslmode=disable".to_string(),
        ] {
            let err = PostgresCatalog::open(&raw, false).unwrap_err();
            let text = err.to_string();
            assert!(!text.contains(password), "{text}");
            assert!(!text.contains("hunter%32"), "{text}");
            assert!(
                text.contains("***") || text.contains("Postgres") || text.contains("sslmode"),
                "the error must stay useful: {text}"
            );
        }
    }

    #[test]
    fn plaintext_is_refused_for_a_remote_host() {
        let raw = "postgres://alice:hunter2@db.example.com/lakes?sslmode=disable";
        let url = PostgresUrl::parse(raw).unwrap();
        let info = UrlInfo::new(&url);
        let err = tls_policy(&url, &info).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("sslmode=disable"), "{err}");
        assert!(!err.to_string().contains("hunter2"), "{err}");

        // Loopback is the documented exception.
        for local in [
            "postgres://alice@localhost/lakes?sslmode=disable",
            "postgres://alice@127.0.0.1/lakes?sslmode=disable",
            "postgres://alice@[::1]/lakes?sslmode=disable",
        ] {
            let url = PostgresUrl::parse(local).unwrap();
            let info = UrlInfo::new(&url);
            assert!(tls_policy(&url, &info).is_ok(), "{local}");
        }
    }

    /// A remote host without `sslmode` verifies the server; encrypt-only is
    /// an explicit choice.
    #[test]
    fn remote_hosts_verify_the_server_by_default() {
        let url = PostgresUrl::parse("postgres://alice@db.example.com/lakes").unwrap();
        let info = UrlInfo::new(&url);
        assert!(matches!(
            tls_policy(&url, &info).unwrap(),
            TlsPolicy::VerifyFull
        ));
        let url =
            PostgresUrl::parse("postgres://alice@db.example.com/lakes?sslmode=require").unwrap();
        let info = UrlInfo::new(&url);
        assert!(matches!(
            tls_policy(&url, &info).unwrap(),
            TlsPolicy::Require
        ));
        let url = PostgresUrl::parse("postgres://alice@localhost/lakes").unwrap();
        let info = UrlInfo::new(&url);
        assert!(matches!(
            tls_policy(&url, &info).unwrap(),
            TlsPolicy::Disable
        ));
    }

    /// `sslrootcert` replaces the public roots; an unreadable or empty file is
    /// a clear error before any connection.
    #[test]
    fn sslrootcert_replaces_the_public_roots() {
        let url = PostgresUrl::parse("postgres://alice@db.example.com/lakes").unwrap();
        let info = UrlInfo::new(&url);
        assert!(!root_store(&url, &info).unwrap().is_empty());

        let url = PostgresUrl::parse(
            "postgres://alice@db.example.com/lakes?sslrootcert=/nonexistent/ca.pem",
        )
        .unwrap();
        let info = UrlInfo::new(&url);
        let err = root_store(&url, &info).unwrap_err().to_string();
        assert!(err.contains("cannot read sslrootcert"), "{err}");
        assert!(
            !url.connection_url().contains("sslrootcert"),
            "the client never sees sslrootcert"
        );

        let empty = std::env::temp_dir().join(format!("w96a-empty-ca-{}.pem", std::process::id()));
        std::fs::write(&empty, b"").unwrap();
        let raw = format!(
            "postgres://alice@db.example.com/lakes?sslrootcert={}",
            empty.display()
        );
        let url = PostgresUrl::parse(&raw).unwrap();
        let info = UrlInfo::new(&url);
        let err = root_store(&url, &info).unwrap_err().to_string();
        std::fs::remove_file(&empty).ok();
        assert!(err.contains("no usable CA certificate"), "{err}");
    }

    #[test]
    fn abs_redacts_and_names_the_object() {
        let redacted = "postgres://alice:***@db.example.com/lakes?schema=s";
        let path = catalog_abs(redacted, "catalog.json");
        assert_eq!(
            path.to_string_lossy(),
            "postgres://alice:***@db.example.com/lakes?schema=s#catalog.json"
        );
        assert_eq!(catalog_abs(redacted, "").to_string_lossy(), redacted);
    }

    #[test]
    fn advisory_keys_are_stable_and_schema_scoped() {
        assert_eq!(advisory_key("public"), advisory_key("public"));
        assert_ne!(advisory_key("public"), advisory_key("w96a_1"));
        assert_ne!(advisory_key("public"), advisory_key("w96a_2"));
    }
}
