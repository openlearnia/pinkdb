//! The Postgres connection adapter (wave 103 T2, `catalog-postgres`).
//!
//! The DuckLake schema, every statement and the commit loop live in
//! [`super::store`]; this file is only the Postgres half of
//! [`SqlConnection`]. One lake lives in one Postgres **schema** (the URL's
//! `?schema=<name>`), so several lakes can share a database.
//!
//! # Serialization
//!
//! DuckLake's commit point is the `ducklake_snapshot` primary key. Two
//! writers that both read head `v` and try to insert snapshot `v+1` race on
//! that key; the loser gets SQLSTATE `23505` (unique violation), which this
//! adapter maps to [`LakeError::Conflict`] — the same retryable conflict
//! SQLite reports as `SQLITE_BUSY` and pinkdb as a write conflict. The whole
//! transaction is rolled back and the lake re-plans against the new head.
//! No advisory lock is needed: the unique key is the commit point, exactly as
//! DuckLake specifies.
//!
//! # Placeholders and values
//!
//! The shared SQL uses numbered placeholders (`?1`, `?2`, …). Postgres uses
//! `$n`; [`rewrite`] maps each distinct index to a `$n`. A `NULL` bind is
//! emitted as the literal `NULL` instead: a bare typed parameter for a NULL
//! is not assignable to every column type, and a NULL carries no user data.
//! The same index may appear several times; it binds once.
//!
//! TLS, URL parsing and redaction are 96a's (see [`crate::catalog_store::url`]):
//! `verify-full` for remote hosts by default, `require` as an explicit
//! opt-in, `disable` only for loopback, and `sslrootcert` replaces the public
//! roots. Every error renders the URL with its password replaced by `***`.

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use postgres::config::SslMode;
use postgres::error::SqlState;
use postgres::types::private::BytesMut;
use postgres::types::{IsNull, ToSql, Type};
use postgres::{Client, Config, NoTls, Row};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::conn::{SqlConnection, SqlRow, SqlValue};
use super::dialect::Dialect;
use super::store::SqlMetadataStore;
use crate::catalog_store::url::PostgresUrl;
use crate::{LakeError, Result};

static INSTALL_PROVIDER: Once = Once::new();

/// The URL in its safe-to-keep forms plus the passwords that must never
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
        let mut message = self.scrub(&error.to_string());
        if message == "db error" {
            // The client's Display hides the server's detail; Debug carries
            // the SQLSTATE and message.
            message = self.scrub(&format!("{error:?}"));
        }
        if is_conflict(&error) {
            return LakeError::Conflict {
                path: PathBuf::from(self.redacted.clone()),
                message,
            };
        }
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

/// A primary-key / unique-violation is the retryable catalog conflict: the
/// snapshot insert (or a side-table key) lost a race.
fn is_conflict(error: &postgres::Error) -> bool {
    matches!(
        error.code(),
        Some(code) if *code == SqlState::UNIQUE_VIOLATION
            || *code == SqlState::T_R_SERIALIZATION_FAILURE
            || *code == SqlState::T_R_DEADLOCK_DETECTED
    )
}

/// Open (or create) the one lake in `url`'s schema.
pub(crate) fn open(url: &str, create: bool) -> Result<SqlMetadataStore> {
    let parsed = PostgresUrl::parse(url).map_err(|message| LakeError::BadLocation {
        location: crate::redact_location(url),
        message,
    })?;
    let info = UrlInfo::new(&parsed);
    let policy = tls_policy(&parsed, &info)?;
    let client = connect(&parsed, &info, policy)?;
    let schema_ident = quote_ident(parsed.schema());
    SqlMetadataStore::open_remote(url, create, |_url, _path, create| {
        let mut client = client;
        if create {
            // The schema must exist before the DuckLake DDL runs; the DDL
            // itself is idempotent, so a concurrent first open can rerun it.
            create_schema(&mut client, &schema_ident, &info)?;
        }
        set_search_path(&mut client, &schema_ident, &info)?;
        let store = Arc::new(Mutex::new(client));
        Ok(Box::new(PostgresConnection {
            client: store,
            url: info.redacted.clone(),
            in_transaction: false,
            total: AtomicU64::new(0),
        }))
    })
}

/// A connection to one Postgres catalog. The shared handle serializes
/// statements; a transaction handle owns its own `Client` so the shared one
/// stays usable by readers.
pub(crate) struct PostgresConnection {
    client: Arc<Mutex<Client>>,
    url: String,
    in_transaction: bool,
    total: AtomicU64,
}

impl fmt::Debug for PostgresConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnection")
            .field("url", &self.url)
            .field("in_transaction", &self.in_transaction)
            .finish()
    }
}

impl PostgresConnection {
    fn lock(&self) -> std::sync::MutexGuard<'_, Client> {
        self.client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn info(&self) -> UrlInfo {
        UrlInfo {
            raw: self.url.clone(),
            redacted: self.url.clone(),
            passwords: Vec::new(),
        }
    }
}

impl SqlConnection for PostgresConnection {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        let (sql, values) = rewrite(sql, params)?;
        let owned: Vec<Owned> = values.into_iter().map(Owned::from).collect();
        let refs: Vec<&(dyn ToSql + Sync)> = owned.iter().map(|value| value.as_sql()).collect();
        let mut client = self.lock();
        let changed = client
            .execute(&sql, &refs)
            .map_err(|e| self.info().io_error("executing a catalog statement", e))?;
        self.total.fetch_add(changed, Ordering::Relaxed);
        Ok(changed as usize)
    }

    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        let (sql, values) = rewrite(sql, params)?;
        let owned: Vec<Owned> = values.into_iter().map(Owned::from).collect();
        let refs: Vec<&(dyn ToSql + Sync)> = owned.iter().map(|value| value.as_sql()).collect();
        let mut client = self.lock();
        let rows = client
            .query(&sql, &refs)
            .map_err(|e| self.info().io_error("running a catalog query", e))?;
        rows.iter().map(row_values).collect()
    }

    fn execute_batch(&self, sql: &str) -> Result<()> {
        self.lock()
            .batch_execute(sql)
            .map_err(|e| self.info().io_error("running catalog DDL", e))
    }

    fn begin(&self) -> Result<Box<dyn SqlConnection>> {
        if self.in_transaction {
            return Err(LakeError::Internal {
                message: "a Postgres catalog transaction cannot nest".to_string(),
            });
        }
        let parsed = PostgresUrl::parse(&self.url).map_err(|message| LakeError::BadLocation {
            location: self.url.clone(),
            message,
        })?;
        let info = UrlInfo::new(&parsed);
        let policy = tls_policy(&parsed, &info)?;
        let mut client = connect(&parsed, &info, policy)?;
        set_search_path(&mut client, &quote_ident(parsed.schema()), &info)?;
        client
            .batch_execute(Dialect::Postgres.begin_write())
            .map_err(|e| info.io_error("beginning the catalog transaction", e))?;
        Ok(Box::new(PostgresConnection {
            client: Arc::new(Mutex::new(client)),
            url: self.url.clone(),
            in_transaction: true,
            total: AtomicU64::new(0),
        }))
    }

    fn commit(&self) -> Result<()> {
        if !self.in_transaction {
            return Err(LakeError::Internal {
                message: "commit on a non-transaction Postgres handle".to_string(),
            });
        }
        self.lock().batch_execute("COMMIT").map_err(|e| {
            self.info()
                .io_error("committing the catalog transaction", e)
        })
    }

    fn rollback(&self) -> Result<()> {
        if !self.in_transaction {
            return Ok(());
        }
        self.lock().batch_execute("ROLLBACK").map_err(|e| {
            self.info()
                .io_error("rolling back the catalog transaction", e)
        })
    }

    fn total_changes(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// A bound value owned by the call, so [`PostgresConnection::execute`] can
/// hand the client a vector of trait objects with one lifetime.
#[derive(Debug)]
enum Owned {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<SqlValue> for Owned {
    fn from(value: SqlValue) -> Self {
        match value {
            SqlValue::Null => Owned::Text(String::new()),
            SqlValue::Integer(v) => Owned::Integer(v),
            SqlValue::Real(v) => Owned::Real(v),
            SqlValue::Text(v) => Owned::Text(v),
            SqlValue::Blob(v) => Owned::Blob(v),
        }
    }
}

impl Owned {
    fn as_sql(&self) -> &(dyn ToSql + Sync) {
        self
    }
}

impl ToSql for Owned {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Owned::Integer(v) => ToSql::to_sql(v, ty, out),
            Owned::Real(v) => ToSql::to_sql(v, ty, out),
            Owned::Text(v) => ToSql::to_sql(v, ty, out),
            Owned::Blob(v) => ToSql::to_sql(&v.as_slice(), ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        // The DuckLake schema's columns are BIGINT/VARCHAR (and Postgres's
        // information_schema uses domain-over-name); the server coerces a
        // matching wire value. The variant's own `to_sql` writes the bytes.
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err(format!("cannot bind a catalog value as {ty}").into());
        }
        self.to_sql(ty, out)
    }
}

/// Rewrite the core's numbered placeholders into Postgres `$n` and drop NULL
/// binds (they become the literal `NULL`; a null carries no user data).
fn rewrite(sql: &str, params: &[SqlValue]) -> Result<(String, Vec<SqlValue>)> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut values: Vec<SqlValue> = Vec::new();
    let mut slot: Vec<Option<usize>> = vec![None; params.len()];
    let mut index = 0;
    let mut last = 0;
    while index < bytes.len() {
        if bytes[index] != b'?' {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        let number: usize = sql[start..end].parse().map_err(|_| LakeError::Internal {
            message: format!("the catalog SQL has an unnumbered placeholder: {sql}"),
        })?;
        if number == 0 || number > params.len() {
            return Err(LakeError::Internal {
                message: format!(
                    "the catalog SQL binds placeholder ?{number}, but {} value(s) were supplied",
                    params.len()
                ),
            });
        }
        out.push_str(&sql[last..index]);
        match &params[number - 1] {
            SqlValue::Null => out.push_str("NULL"),
            value => {
                let target = match slot[number - 1] {
                    Some(target) => target,
                    None => {
                        values.push(value.clone());
                        let target = values.len();
                        slot[number - 1] = Some(target);
                        target
                    }
                };
                out.push('$');
                out.push_str(&target.to_string());
            }
        }
        last = end;
        index = end;
    }
    out.push_str(&sql[last..]);
    Ok((out, values))
}

/// Materialize one row's values from the server's own column types. A SQL
/// `NULL` stays `SqlValue::Null`; collapsing it to a type's default would
/// turn `MAX(snapshot_id)` over an empty catalog into snapshot 0.
fn row_values(row: &Row) -> Result<SqlRow> {
    macro_rules! int {
        ($row:expr, $index:expr, $ty:ty) => {
            match $row.try_get::<_, Option<$ty>>($index) {
                Ok(Some(v)) => SqlValue::Integer(i64::from(v)),
                _ => SqlValue::Null,
            }
        };
    }
    let mut values = Vec::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        let value = match *column.type_() {
            Type::INT2 => int!(row, index, i16),
            Type::INT4 => int!(row, index, i32),
            Type::INT8 => int!(row, index, i64),
            Type::FLOAT4 => match row.try_get::<_, Option<f32>>(index) {
                Ok(Some(v)) => SqlValue::Real(f64::from(v)),
                _ => SqlValue::Null,
            },
            Type::FLOAT8 => match row.try_get::<_, Option<f64>>(index) {
                Ok(Some(v)) => SqlValue::Real(v),
                _ => SqlValue::Null,
            },
            Type::BOOL => match row.try_get::<_, Option<bool>>(index) {
                Ok(Some(v)) => SqlValue::Integer(i64::from(v)),
                _ => SqlValue::Null,
            },
            Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(index) {
                Ok(Some(v)) => SqlValue::Blob(v),
                _ => SqlValue::Null,
            },
            _ => match row.try_get::<_, Option<String>>(index) {
                Ok(Some(v)) => SqlValue::Text(v),
                _ => SqlValue::Null,
            },
        };
        values.push(value);
    }
    Ok(SqlRow::new(values))
}

/// The TLS mode one connection uses.
#[derive(Clone, Copy, Debug)]
enum TlsPolicy {
    Disable,
    Require,
    VerifyFull,
}

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
        None => Ok(TlsPolicy::VerifyFull),
    }
}

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

/// Create the catalog's schema. The DDL itself is posted by the shared store.
fn create_schema(client: &mut Client, schema_ident: &str, info: &UrlInfo) -> Result<()> {
    let ddl = format!("CREATE SCHEMA IF NOT EXISTS {schema_ident}");
    let mut attempts = 0;
    loop {
        match client.batch_execute(&ddl) {
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
            Err(e) => return Err(info.io_error("creating the Postgres catalog schema", e)),
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

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Every statement in the shared core uses unqualified DuckLake table names,
/// so each connection resolves them in its lake's own schema.
fn set_search_path(client: &mut Client, schema_ident: &str, info: &UrlInfo) -> Result<()> {
    client
        .batch_execute(&format!("SET search_path TO {schema_ident}"))
        .map_err(|e| info.io_error("setting the Postgres search path", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_store::url::PostgresUrl;

    #[test]
    fn errors_never_contain_the_password() {
        let password = "hunter2";
        for raw in [
            format!("postgres://alice:{password}@db.example.com/lakes?sslmode=disable"),
            format!("postgres://alice:{password}@db.example.com/lakes?sslmode=bogus"),
            format!("postgres://alice:{password}@db.example.com/lakes?schema=not-a-schema"),
            format!("postgres://alice:{password}@db.example.com:99999/lakes"),
            "postgres://alice:hunter%32@db.example.com/lakes?sslmode=disable".to_string(),
        ] {
            let err = open(&raw, false).unwrap_err();
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
    fn placeholders_become_dollar_numbers_and_null_is_literal() {
        let params = [
            SqlValue::Integer(7),
            SqlValue::Text("x".to_string()),
            SqlValue::Null,
        ];
        let (sql, values) = rewrite("A ?1 B ?2 A ?1 C ?3", &params).unwrap();
        assert_eq!(sql, "A $1 B $2 A $1 C NULL");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqlValue::Integer(7));
        assert_eq!(values[1], SqlValue::Text("x".to_string()));
    }

    #[test]
    fn catalog_paths_redact() {
        let redacted = "postgres://alice:***@db.example.com/lakes?schema=s";
        assert_eq!(crate::redact_location(redacted), redacted);
    }
}
