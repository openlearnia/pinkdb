//! DuckDB-style secrets (wave 132b): the only place a credential lives.
//!
//! A secret holds an S3/R2/GCS credential or a Postgres password. It is the
//! only carrier of a credential: statement text, the WAL and the checkpoint
//! footer never hold a value (see the redacting [`fmt::Debug`] impls and
//! `crate::rbac::wal_statement_text`). Temporary secrets belong to one
//! [`Session`]; persistent ones belong to the store and are checkpointed in
//! their own footer section, sealed with the rest of the footer when the
//! store has an encryption key.

use std::collections::BTreeMap;
use std::fmt;

use crate::storage::{PinkDb, Session};
use crate::types::{PinkDbError, Result};

/// The S3-family secret whose scope is the longest prefix of `url`, from a
/// snapshotted set (`None` when no secret covers it). Table sources receive
/// this snapshot instead of reaching into a store.
pub fn s3_secret_for_url(secrets: &[Secret], url: &str) -> Option<S3Secret> {
    secrets
        .iter()
        .filter(|secret| {
            matches!(
                secret.type_(),
                SecretType::S3 | SecretType::R2 | SecretType::Gcs
            ) && secret.matches(url)
        })
        .max_by_key(|secret| (secret.scope_len(url), secret.name().to_string()))
        .and_then(Secret::s3_credential)
}

/// What `duckdb_secrets()` shows in place of a value.
pub const REDACTED: &str = "redacted";

/// The secret kinds DuckDB's `TYPE` accepts (plus `postgresql`, which DuckDB
/// also takes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SecretType {
    S3,
    R2,
    Gcs,
    Postgres,
}

impl SecretType {
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "s3" => Some(Self::S3),
            "r2" => Some(Self::R2),
            "gcs" => Some(Self::Gcs),
            "postgres" | "postgresql" => Some(Self::Postgres),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::R2 => "r2",
            Self::Gcs => "gcs",
            Self::Postgres => "postgres",
        }
    }

    /// The name an unnamed secret gets, DuckDB's rule.
    pub fn default_name(self) -> String {
        format!("__default_{}", self.as_str())
    }

    /// The stable tag the checkpoint section stores (not a public format
    /// contract: the footer version guards it).
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::S3 => 0,
            Self::R2 => 1,
            Self::Gcs => 2,
            Self::Postgres => 3,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::S3),
            1 => Some(Self::R2),
            2 => Some(Self::Gcs),
            3 => Some(Self::Postgres),
            _ => None,
        }
    }

    fn fields(self) -> &'static [&'static str] {
        match self {
            Self::S3 | Self::R2 | Self::Gcs => &[
                "KEY_ID",
                "SECRET",
                "SESSION_TOKEN",
                "REGION",
                "ENDPOINT",
                "URL_STYLE",
                "USE_SSL",
                "ACCOUNT_ID",
                "SCOPE",
                "PROVIDER",
            ],
            Self::Postgres => &[
                "HOST", "PORT", "DATABASE", "USER", "PASSWORD", "SCOPE", "PROVIDER",
            ],
        }
    }

    /// The URLs a secret of this type covers when it names no `SCOPE`.
    fn default_scopes(self) -> &'static [&'static str] {
        match self {
            Self::S3 | Self::R2 => &["s3://"],
            Self::Gcs => &["gcs://", "gs://"],
            // A Postgres secret names one server; without a SCOPE it matches
            // every catalog URL of its type, like `PGPASSWORD`.
            Self::Postgres => &[],
        }
    }
}

/// A credential, addressed by name. `Debug` prints the options but never a
/// value: an error message or a plan dump must not leak a password.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret {
    name: String,
    type_: SecretType,
    /// Canonical (uppercased) option -> value; `SCOPE` is kept in `scopes`.
    fields: BTreeMap<String, String>,
    /// Empty = the type's default scope (every URL of that type).
    scopes: Vec<String>,
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secret")
            .field("name", &self.name)
            .field("type", &self.type_)
            .field("scopes", &self.scopes)
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Secret {
    /// Build a secret from parsed SQL fields, validating the type's option
    /// names. `name = None` (or an empty name) selects the default name.
    pub fn new(
        name: Option<String>,
        type_: SecretType,
        fields: Vec<(String, String)>,
    ) -> Result<Self> {
        let name = match name {
            Some(name) if !name.trim().is_empty() => name,
            _ => type_.default_name(),
        };
        let mut options = BTreeMap::new();
        let mut scopes = Vec::new();
        for (raw_key, value) in fields {
            let key = raw_key.trim().to_ascii_uppercase();
            if key == "TYPE" {
                return Err(secret_error(
                    "TYPE is given once, before the secret options".to_string(),
                ));
            }
            if key == "SCOPE" {
                scopes.extend(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|scope| !scope.is_empty())
                        .map(str::to_string),
                );
                continue;
            }
            if !type_.fields().contains(&key.as_str()) {
                return Err(secret_error(format!(
                    "secret type {} has no option \"{key}\" (supported: {})",
                    type_.as_str(),
                    type_.fields().join(", ")
                )));
            }
            if options.insert(key.clone(), value).is_some() {
                return Err(secret_error(format!("secret option \"{key}\" given twice")));
            }
        }
        // DuckDB gives S3/R2/GCS secrets their scheme as a default scope so
        // `which_secret('s3://…','s3')` matches without an explicit SCOPE.
        let scopes = if scopes.is_empty() {
            type_
                .default_scopes()
                .iter()
                .map(|scope| scope.to_string())
                .collect()
        } else {
            scopes
        };
        let mut secret = Self {
            name,
            type_,
            fields: options,
            scopes,
        };
        secret.validate()?;
        Ok(secret)
    }

    fn validate(&mut self) -> Result<()> {
        if let Some(provider) = self.field("PROVIDER")
            && !provider.eq_ignore_ascii_case("config")
        {
            return Err(secret_error(format!(
                "secret PROVIDER \"{provider}\" is not supported (only config)"
            )));
        }
        if self.type_ == SecretType::Postgres
            && let Some(port) = self.field("PORT")
        {
            port.parse::<u16>().map_err(|_| {
                secret_error(format!("secret PORT \"{port}\" is not a port number"))
            })?;
        }
        if let Some(style) = self.field("URL_STYLE")
            && !matches!(
                style.to_ascii_lowercase().as_str(),
                "path" | "vhost" | "auto"
            )
        {
            return Err(secret_error(format!(
                "secret URL_STYLE \"{style}\" is not supported (path, vhost, auto)"
            )));
        }
        if let Some(ssl) = self.field("USE_SSL") {
            parse_bool("USE_SSL", ssl)?;
        }
        if self.type_ == SecretType::R2 {
            // DuckDB's r2 secret derives the endpoint and path style from
            // ACCOUNT_ID; accepting both spellings would let them disagree.
            if self.field("ENDPOINT").is_some() {
                return Err(secret_error(
                    "TYPE r2 derives ENDPOINT from ACCOUNT_ID; drop ENDPOINT".to_string(),
                ));
            }
            let account = self.field("ACCOUNT_ID").ok_or_else(|| {
                secret_error("TYPE r2 needs ACCOUNT_ID (or use TYPE s3 with ENDPOINT)".to_string())
            })?;
            let endpoint = format!("{account}.r2.cloudflarestorage.com");
            self.fields.insert("ENDPOINT".to_string(), endpoint);
            self.fields
                .insert("URL_STYLE".to_string(), "path".to_string());
        } else if self.field("ACCOUNT_ID").is_some() {
            return Err(secret_error(format!(
                "ACCOUNT_ID is an r2 option; use TYPE r2, not TYPE {}",
                self.type_.as_str()
            )));
        }
        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_(&self) -> SecretType {
        self.type_
    }

    /// One option's value (canonical uppercase key).
    pub fn field(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }

    /// The configured scopes; empty means the type's default.
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    /// The stored options, in canonical key order (for the checkpoint
    /// section; not a display API — use [`Secret::secret_string`]).
    pub(crate) fn fields(&self) -> &BTreeMap<String, String> {
        &self.fields
    }

    /// Rebuild a secret read from a checkpoint section. The reader validates
    /// the shape, so this skips the parser's normalisation.
    pub(crate) fn from_stored(
        name: String,
        type_: SecretType,
        fields: BTreeMap<String, String>,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            name,
            type_,
            fields,
            scopes,
        }
    }

    fn effective_scopes(&self) -> Vec<&str> {
        if self.scopes.is_empty() {
            self.type_.default_scopes().to_vec()
        } else {
            self.scopes.iter().map(String::as_str).collect()
        }
    }

    /// How specifically this secret covers `url`; 0 = it does not. The
    /// longest matching scope wins in [`PinkDb::secret_for`].
    pub fn scope_len(&self, url: &str) -> usize {
        if self.scopes.is_empty() {
            // No scope: the secret covers its type's every URL.
            return 1;
        }
        let url = url.strip_prefix("ducklake:").unwrap_or(url);
        let bare = url.strip_prefix("postgres:").unwrap_or(url);
        let mut best = 0;
        for scope in self.effective_scopes() {
            for candidate in [url, bare] {
                if candidate.starts_with(scope) {
                    best = best.max(scope.len());
                }
            }
        }
        best
    }

    pub fn matches(&self, url: &str) -> bool {
        self.scope_len(url) > 0
    }

    /// The S3-compatible credential (S3, R2 and GCS), `None` for Postgres.
    pub fn s3_credential(&self) -> Option<S3Secret> {
        match self.type_ {
            SecretType::Postgres => None,
            SecretType::S3 | SecretType::R2 | SecretType::Gcs => Some(S3Secret {
                key_id: self.field("KEY_ID").map(str::to_string),
                secret: self.field("SECRET").map(str::to_string),
                session_token: self.field("SESSION_TOKEN").map(str::to_string),
                region: self.field("REGION").map(str::to_string),
                endpoint: self.field("ENDPOINT").map(str::to_string),
                path_style: self.field("URL_STYLE").and_then(|s| match s {
                    "path" => Some(true),
                    "vhost" => Some(false),
                    _ => None,
                }),
                use_ssl: self
                    .field("USE_SSL")
                    .map(|value| parse_bool("USE_SSL", value).unwrap_or(true)),
            }),
        }
    }

    pub fn postgres_password(&self) -> Option<&str> {
        self.field("PASSWORD")
    }

    /// `duckdb_secrets().scope`: DuckDB's list rendered as text (pinkdb has
    /// no LIST type), e.g. `['s3://bucket/one']`.
    pub fn scope_text(&self) -> String {
        let scopes = self.effective_scopes();
        if scopes.is_empty() {
            return "[]".to_string();
        }
        let quoted: Vec<String> = scopes.iter().map(|s| format!("'{s}'")).collect();
        format!("[{}]", quoted.join(", "))
    }

    /// `duckdb_secrets().secret_string`: DuckDB's `key=value;...` form with
    /// every value-bearing option replaced by `redacted`.
    pub fn secret_string(&self) -> String {
        let mut out = format!(
            "name={};type={};provider=config;serializable=true;scope",
            self.name,
            self.type_.as_str()
        );
        if !self.scopes.is_empty() {
            out.push('=');
            out.push_str(&self.scopes.join(","));
        }
        out.push(';');
        for (key, value) in &self.fields {
            let shown = match key.as_str() {
                "SECRET" | "SESSION_TOKEN" | "PASSWORD" => REDACTED,
                _ => value.as_str(),
            };
            let lower = key.to_ascii_lowercase();
            let key = if self.type_ == SecretType::Postgres && key == "DATABASE" {
                // DuckDB's postgres secret spells this `dbname`.
                "dbname"
            } else {
                lower.as_str()
            };
            out.push_str(key);
            out.push('=');
            out.push_str(shown);
            out.push(';');
        }
        out
    }
}

/// The resolved S3-compatible credential handed to pink-lake and httpfs.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct S3Secret {
    pub key_id: Option<String>,
    pub secret: Option<String>,
    pub session_token: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub path_style: Option<bool>,
    pub use_ssl: Option<bool>,
}

impl S3Secret {
    /// `true` when the secret names no credential at all (public bucket).
    pub fn is_empty(&self) -> bool {
        self.key_id.is_none() && self.secret.is_none() && self.session_token.is_none()
    }
}

impl fmt::Debug for S3Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Secret")
            .field("key_id", &self.key_id.is_some())
            .field("secret", &self.secret.is_some())
            .field("session_token", &self.session_token.is_some())
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("path_style", &self.path_style)
            .field("use_ssl", &self.use_ssl)
            .finish()
    }
}

/// A password that must never reach a log line: `Debug` shows only that it
/// is set.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Password(String);

impl Password {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("Password(empty)")
        } else {
            f.write_str("Password(redacted)")
        }
    }
}

/// One visible secret and the store it lives in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretEntry {
    pub secret: Secret,
    pub persistent: bool,
}

impl SecretEntry {
    /// `which_secret`'s second column.
    pub fn persistence(&self) -> &'static str {
        if self.persistent {
            "PERSISTENT"
        } else {
            "TEMPORARY"
        }
    }

    /// `duckdb_secrets()`'s `storage` column.
    pub fn storage(&self) -> &'static str {
        if self.persistent {
            "local_file"
        } else {
            "memory"
        }
    }
}

fn secret_error(message: String) -> PinkDbError {
    PinkDbError::Catalog(message)
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(secret_error(format!(
            "secret {key} \"{value}\" is not a boolean (true, false)"
        ))),
    }
}

impl PinkDb {
    /// The store's persistent secrets, name order.
    pub fn persistent_secrets(&self) -> Vec<Secret> {
        self.secrets.values().cloned().collect()
    }

    /// Every secret this session can use: the store's persistent secrets
    /// first, then the session's temporary ones (a temporary secret shadows a
    /// persistent one of the same name).
    pub fn visible_secrets(&self, session: &Session) -> Vec<SecretEntry> {
        let mut out: Vec<SecretEntry> = self
            .secrets
            .values()
            .map(|secret| SecretEntry {
                secret: secret.clone(),
                persistent: true,
            })
            .collect();
        for secret in session.secrets().into_values() {
            if let Some(slot) = out.iter_mut().find(|e| e.secret.name() == secret.name()) {
                *slot = SecretEntry {
                    secret,
                    persistent: false,
                };
            } else {
                out.push(SecretEntry {
                    secret,
                    persistent: false,
                });
            }
        }
        out
    }

    /// The visible secret called `name`.
    pub fn secret(&self, name: &str, session: &Session) -> Option<SecretEntry> {
        self.visible_secrets(session)
            .into_iter()
            .find(|entry| entry.secret.name() == name)
    }

    /// The secret a URL of `type_` resolves to: the longest matching SCOPE,
    /// a temporary secret before a persistent one on a tie. An attach that
    /// carries a password creates one scoped to the catalog itself, so it
    /// outranks a broader credential.
    pub fn secret_for(
        &self,
        url: &str,
        type_: SecretType,
        session: &Session,
    ) -> Option<SecretEntry> {
        let mut visible = self.visible_secrets(session);
        visible.sort_by(|a, b| {
            b.secret
                .scope_len(url)
                .cmp(&a.secret.scope_len(url))
                .then_with(|| a.persistent.cmp(&b.persistent))
                .then_with(|| a.secret.name().cmp(b.secret.name()))
        });
        visible
            .into_iter()
            .find(|entry| entry.secret.type_() == type_ && entry.secret.matches(url))
    }

    /// The S3-compatible credential `url` resolves to, if any secret covers
    /// it (S3 before R2 before GCS on equally specific scopes).
    pub fn s3_secret_for(&self, url: &str, session: &Session) -> Option<S3Secret> {
        let mut best: Option<SecretEntry> = None;
        for type_ in [SecretType::S3, SecretType::R2, SecretType::Gcs] {
            if let Some(entry) = self.secret_for(url, type_, session) {
                let better = best
                    .as_ref()
                    .is_none_or(|b| entry.secret.scope_len(url) > b.secret.scope_len(url));
                if better {
                    best = Some(entry);
                }
            }
        }
        best.and_then(|entry| entry.secret.s3_credential())
    }

    /// The Postgres password for a catalog URL, from a `TYPE postgres`
    /// secret. The environment (`PGPASSWORD`, `.pgpass`) stays the fallback.
    pub fn postgres_password_for(&self, url: &str, session: &Session) -> Option<Password> {
        self.secret_for(url, SecretType::Postgres, session)
            .and_then(|entry| entry.secret.postgres_password().map(Password::new))
    }

    /// Install `secret` in the persistent store or in `session`. Returns the
    /// stored entry. `or_replace` and `if_not_exists` are the statement's
    /// clauses; passing neither refuses an existing name.
    pub fn put_secret(
        &mut self,
        secret: Secret,
        persistent: bool,
        or_replace: bool,
        if_not_exists: bool,
        session: &Session,
    ) -> Result<SecretEntry> {
        let name = secret.name().to_string();
        let exists = if persistent {
            self.secrets.contains_key(&name)
        } else {
            session.secrets().contains_key(&name)
        };
        if exists && !or_replace {
            if if_not_exists {
                let existing = if persistent {
                    self.secrets.get(&name).cloned()
                } else {
                    session.secrets().get(&name).cloned()
                };
                return existing
                    .map(|secret| SecretEntry { secret, persistent })
                    .ok_or_else(|| {
                        secret_error(format!("Secret with name \"{name}\" already exists"))
                    });
            }
            return Err(secret_error(format!(
                "Secret with name \"{name}\" already exists"
            )));
        }
        if persistent {
            self.secrets.insert(name, secret.clone());
        } else {
            session.put_secret(secret.clone());
        }
        Ok(SecretEntry { secret, persistent })
    }

    /// Drop one secret. `persistent = None` drops a temporary secret first
    /// and a persistent one with the same name after it.
    pub fn remove_secret(
        &mut self,
        name: &str,
        persistent: Option<bool>,
        if_exists: bool,
        session: &Session,
    ) -> Result<bool> {
        let in_session = session.secrets().contains_key(name);
        let in_store = self.secrets.contains_key(name);
        let dropped = match persistent {
            Some(true) if in_store => {
                self.secrets.remove(name);
                true
            }
            Some(false) if in_session => {
                session.remove_secret(name);
                true
            }
            None if in_session => {
                session.remove_secret(name);
                true
            }
            None if in_store => {
                self.secrets.remove(name);
                true
            }
            _ => false,
        };
        if dropped || if_exists {
            Ok(dropped)
        } else {
            Err(secret_error(format!(
                "Failed to remove non-existent secret with name \"{name}\""
            )))
        }
    }

    /// Replay/checkpoint installation: replace the whole persistent set with
    /// a record's snapshot. `None` leaves the set untouched (a WAL record
    /// from before wave 132b has no secret section).
    pub(crate) fn install_persistent_secrets(&mut self, secrets: Vec<Secret>) {
        self.secrets = secrets
            .into_iter()
            .map(|secret| (secret.name().to_string(), secret))
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(fields: &[(&str, &str)]) -> Secret {
        Secret::new(
            None,
            SecretType::S3,
            fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn unnamed_secrets_take_duckdb_default_names() {
        assert_eq!(s3(&[]).name(), "__default_s3");
        let pg = Secret::new(
            None,
            SecretType::Postgres,
            vec![("PASSWORD".to_string(), "pw".to_string())],
        )
        .unwrap();
        assert_eq!(pg.name(), "__default_postgres");
    }

    #[test]
    fn r2_derives_endpoint_and_path_style() {
        let secret = Secret::new(
            Some("r2_lake".to_string()),
            SecretType::R2,
            vec![
                ("account_id".to_string(), "acct".to_string()),
                ("key_id".to_string(), "k".to_string()),
                ("secret".to_string(), "s".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            secret.field("ENDPOINT"),
            Some("acct.r2.cloudflarestorage.com")
        );
        assert_eq!(secret.field("URL_STYLE"), Some("path"));
        let endpoint = Secret::new(
            Some("x".to_string()),
            SecretType::R2,
            vec![
                ("ACCOUNT_ID".to_string(), "acct".to_string()),
                ("ENDPOINT".to_string(), "e".to_string()),
            ],
        );
        assert!(endpoint.is_err(), "ENDPOINT and ACCOUNT_ID conflict");
    }

    #[test]
    fn unknown_options_are_refused_by_name() {
        let err = Secret::new(
            None,
            SecretType::S3,
            vec![("ENCRYPTION_KEY".to_string(), "x".to_string())],
        )
        .unwrap_err();
        assert!(err.to_string().contains("ENCRYPTION_KEY"), "{err}");
        assert!(
            !format!("{err:?}").contains('x'),
            "the refused value must not be echoed: {err:?}"
        );
    }

    #[test]
    fn debug_and_secret_string_never_hold_a_value() {
        let secret = s3(&[("KEY_ID", "AKIA"), ("SECRET", "S3CRET")]);
        let debug = format!("{secret:?}");
        assert!(!debug.contains("S3CRET"), "{debug}");
        assert!(!debug.contains("AKIA"), "{debug}");
        assert!(debug.contains("SECRET"));
        let text = secret.secret_string();
        assert!(text.contains("secret=redacted"), "{text}");
        assert!(!text.contains("S3CRET"));
    }

    #[test]
    fn scope_matching_prefers_the_longest_prefix() {
        let broad = s3(&[("SCOPE", "s3://bucket/")]);
        let narrow = s3(&[("SCOPE", "s3://bucket/one/")]);
        assert_eq!(broad.scope_len("s3://bucket/one/x"), 12);
        assert_eq!(narrow.scope_len("s3://bucket/one/x"), 16);
        assert_eq!(narrow.scope_len("s3://other/x"), 0);
        let unscoped = s3(&[]);
        assert!(unscoped.matches("s3://anything/at/all"));
        let pg = Secret::new(
            None,
            SecretType::Postgres,
            vec![("PASSWORD".to_string(), "pw".to_string())],
        )
        .unwrap();
        assert!(
            pg.matches("postgres:postgresql://u@h/d"),
            "no scope covers every postgres URL"
        );
        let scoped = Secret::new(
            None,
            SecretType::Postgres,
            vec![
                ("PASSWORD".to_string(), "pw".to_string()),
                ("SCOPE".to_string(), "postgres://u@h/d".to_string()),
            ],
        )
        .unwrap();
        assert!(scoped.matches("postgres:postgres://u@h/d?sslmode=require"));
        assert!(scoped.matches("postgres://u@h/d"));
        assert!(!scoped.matches("postgres://u@other/d"));
    }

    #[test]
    fn scope_text_renders_duckdb_list_shape() {
        assert_eq!(s3(&[]).scope_text(), "['s3://']");
        let scoped = s3(&[("SCOPE", "s3://a/,s3://b/")]);
        assert_eq!(scoped.scope_text(), "['s3://a/', 's3://b/']");
    }
}
