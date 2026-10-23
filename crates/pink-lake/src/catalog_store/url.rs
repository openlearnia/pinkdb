//! Parsing and redaction of Postgres catalog URLs (wave 96a).
//!
//! This module is compiled in every build, with or without the
//! `catalog-postgres` feature: `Location::parse` must recognize a
//! `postgres://` URL (to give the clear rebuild hint), and every error that
//! names a catalog location must redact the password even when the backend is
//! not compiled in. It has no dependencies.
//!
//! A catalog URL is a libpq-style URL:
//!
//! ```text
//! postgres://user:password@host:5432/dbname?schema=lake&sslmode=require
//! ```
//!
//! `schema` selects the schema holding the two lake tables (default
//! `public`); it is stripped before the URL reaches the `postgres` client,
//! because libpq has no `schema` connection parameter (the schema is applied
//! by qualifying every table name instead). The password may also come from
//! `PGPASSWORD`, which the store applies when the URL has none.

/// A parsed Postgres catalog URL.
///
/// The raw pieces are kept because the connection URL must preserve the
/// original percent-encoding (the `postgres` client decodes it); the display
/// form is always [`PostgresUrl::redacted`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostgresUrl {
    scheme: String,
    /// The raw `user[:password]` userinfo, percent-encoded as given.
    userinfo: Option<String>,
    user: Option<String>,
    /// The raw password from the URL (percent-encoded as given).
    password: Option<String>,
    /// The percent-decoded password, for scrubbing error text that the
    /// client decoded on its own.
    password_plain: Option<String>,
    /// The bare host (IPv6 brackets stripped), for the local/remote decision.
    host: String,
    /// The host as written, brackets included for IPv6.
    host_port: String,
    port: Option<u16>,
    db: String,
    params: Vec<(String, String)>,
    schema: String,
}

#[cfg_attr(not(feature = "catalog-postgres"), allow(dead_code))]
impl PostgresUrl {
    /// Parse `postgres://` or `postgresql://`. The error string never contains
    /// the raw URL (a caller may attach it to a `BadLocation` message).
    pub(crate) fn parse(raw: &str) -> std::result::Result<Self, String> {
        let (scheme, rest) = if let Some(rest) = raw.strip_prefix("postgres://") {
            ("postgres", rest)
        } else if let Some(rest) = raw.strip_prefix("postgresql://") {
            ("postgresql", rest)
        } else {
            return Err("expected a postgres:// or postgresql:// catalog URL".to_string());
        };
        // A fragment is not meaningful for a catalog URL, but `abs()` renders
        // one; tolerate and drop it here.
        let rest = rest.split_once('#').map_or(rest, |(before, _)| before);
        let (main, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (authority, db) = main.split_once('/').unwrap_or((main, ""));
        let (userinfo, host_port) = match authority.rsplit_once('@') {
            Some((userinfo, host)) => (Some(userinfo), host),
            None => (None, authority),
        };
        let (user, password) = match userinfo {
            Some(info) => match info.split_once(':') {
                Some((user, password)) => (
                    (!user.is_empty()).then(|| user.to_string()),
                    Some(password.to_string()),
                ),
                None => ((!info.is_empty()).then(|| info.to_string()), None),
            },
            None => (None, None),
        };
        let (host, port) = split_host_port(host_port)?;
        if host.is_empty() {
            return Err("the URL has no host".to_string());
        }
        let mut params = Vec::new();
        let mut schema = "public".to_string();
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_decode(key);
            let value = percent_decode(value);
            if key == "schema" {
                schema = value;
            } else {
                params.push((key, value));
            }
        }
        if !valid_schema(&schema) {
            return Err(format!(
                "the `schema` parameter {schema:?} is not a valid Postgres identifier \
                 (letters, digits and underscores, not starting with a digit)"
            ));
        }
        Ok(Self {
            scheme: scheme.to_string(),
            userinfo: userinfo.map(str::to_string),
            user,
            password_plain: password.as_deref().map(percent_decode),
            password,
            host,
            host_port: host_port.to_string(),
            port,
            db: db.to_string(),
            params,
            schema,
        })
    }

    /// The schema holding the lake tables.
    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    /// The bare host, for the local/remote `sslmode` decision.
    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    /// Whether the host is a loopback name/address. `sslmode=disable` is
    /// allowed only for these.
    pub(crate) fn is_local(&self) -> bool {
        self.host.eq_ignore_ascii_case("localhost")
            || self.host == "127.0.0.1"
            || self.host == "::1"
    }

    /// The `sslmode` parameter as written (after percent decoding).
    pub(crate) fn sslmode(&self) -> Option<&str> {
        self.param("sslmode")
    }

    /// The `sslrootcert` parameter: a PEM file of CA certificates that
    /// replaces the public webpki roots for `verify-full` (libpq semantics).
    pub(crate) fn sslrootcert(&self) -> Option<&str> {
        self.param("sslrootcert")
    }

    fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The password from the URL, if any. `PGPASSWORD` is applied by the
    /// store (not here) so this module stays dependency-free.
    pub(crate) fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// The same URL with the password replaced by `***`, the effective schema
    /// made explicit, and the `schema` parameter first. This is the only form
    /// that reaches an error message or the CLI.
    pub(crate) fn redacted(&self) -> String {
        let mut out = String::new();
        out.push_str(&self.scheme);
        out.push_str("://");
        if let Some(info) = &self.userinfo {
            let user = info.split_once(':').map_or(info.as_str(), |(user, _)| user);
            out.push_str(user);
            if self.password.is_some() {
                out.push_str(":***");
            }
            out.push('@');
        }
        out.push_str(&self.host_port);
        out.push('/');
        out.push_str(&self.db);
        out.push_str("?schema=");
        out.push_str(&self.schema);
        for (key, value) in &self.params {
            out.push('&');
            out.push_str(key);
            if !value.is_empty() {
                out.push('=');
                out.push_str(value);
            }
        }
        out
    }

    /// The URL handed to the `postgres` client: `schema`, `sslmode` and
    /// `sslrootcert` are removed (the store applies them itself). The password is preserved
    /// verbatim; `PGPASSWORD` is applied by the store when absent.
    pub(crate) fn connection_url(&self) -> String {
        let mut out = String::new();
        out.push_str(&self.scheme);
        out.push_str("://");
        if let Some(info) = &self.userinfo {
            out.push_str(info);
            out.push('@');
        }
        out.push_str(&self.host_port);
        out.push('/');
        out.push_str(&self.db);
        let mut separator = '?';
        for (key, value) in &self.params {
            if key == "sslmode" || key == "sslrootcert" {
                continue;
            }
            out.push(separator);
            separator = '&';
            out.push_str(key);
            if !value.is_empty() {
                out.push('=');
                out.push_str(value);
            }
        }
        out
    }

    /// Every password spelling that must never survive in error text: the raw
    /// (encoded) value and its decoded form.
    pub(crate) fn password_spellings(&self) -> Vec<&str> {
        let mut out = Vec::new();
        if let Some(password) = &self.password {
            out.push(password.as_str());
        }
        if let Some(password) = &self.password_plain
            && Some(password.as_str()) != self.password.as_deref()
        {
            out.push(password.as_str());
        }
        out
    }

    /// The port, when the URL names one (unused by the connection URL, which
    /// keeps the original authority, but kept for diagnostics/tests).
    #[allow(dead_code)]
    pub(crate) fn port(&self) -> Option<u16> {
        self.port
    }
}

/// Redact a catalog location string for display, whether or not it parses.
/// Non-Postgres locations (directories, `s3://`, `sqlite:`) are returned
/// unchanged: they have no credentials.
pub(crate) fn redact_location(location: &str) -> String {
    if location.starts_with("postgres://") || location.starts_with("postgresql://") {
        PostgresUrl::parse(location)
            .map_or_else(|_| redact_best_effort(location), |url| url.redacted())
    } else {
        location.to_string()
    }
}

/// Best-effort redaction for a URL too malformed to parse: anything after the
/// first `:` of the userinfo is a password. Never echoes the part after `@`.
fn redact_best_effort(raw: &str) -> String {
    let Some((scheme, rest)) = raw.split_once("://") else {
        return raw.to_string();
    };
    let Some((userinfo, tail)) = rest.split_once('@') else {
        return raw.to_string();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{tail}"),
        None => raw.to_string(),
    }
}

fn split_host_port(host_port: &str) -> std::result::Result<(String, Option<u16>), String> {
    if let Some(rest) = host_port.strip_prefix('[') {
        let Some((host, after)) = rest.split_once(']') else {
            return Err("the IPv6 host is missing its closing `]`".to_string());
        };
        let port = match after.strip_prefix(':') {
            Some(port) if !port.is_empty() => Some(parse_port(port)?),
            Some(_) => None,
            None if after.is_empty() => None,
            None => return Err("unexpected text after the IPv6 host".to_string()),
        };
        return Ok((host.to_string(), port));
    }
    match host_port.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
            Ok((host.to_string(), Some(parse_port(port)?)))
        }
        _ => Ok((host_port.to_string(), None)),
    }
}

fn parse_port(port: &str) -> std::result::Result<u16, String> {
    port.parse::<u16>()
        .map_err(|_| format!("invalid port {port:?} (expected 1–65535)"))
}

fn valid_schema(schema: &str) -> bool {
    let mut bytes = schema.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Decode `%XX` escapes (the only escapes a catalog URL needs: userinfo and
/// schema names). Invalid escapes are left verbatim.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(high), Some(low)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(high * 16 + low);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| value.to_string())
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_parse_and_redact() {
        let url = PostgresUrl::parse(
            "postgres://alice:s3cret@db.example.com:5544/lakes?schema=w96a&sslmode=verify-full",
        )
        .unwrap();
        assert_eq!(url.host(), "db.example.com");
        assert_eq!(url.port(), Some(5544));
        assert_eq!(url.schema(), "w96a");
        assert!(!url.is_local());
        assert_eq!(
            url.redacted(),
            "postgres://alice:***@db.example.com:5544/lakes?schema=w96a&sslmode=verify-full"
        );
        assert!(!url.redacted().contains("s3cret"));
        assert_eq!(
            url.connection_url(),
            "postgres://alice:s3cret@db.example.com:5544/lakes"
        );

        let local = PostgresUrl::parse("postgresql://postgres@localhost/pinktest").unwrap();
        assert!(local.is_local());
        assert_eq!(local.schema(), "public");
        assert_eq!(
            local.redacted(),
            "postgresql://postgres@localhost/pinktest?schema=public"
        );
        assert_eq!(
            local.connection_url(),
            "postgresql://postgres@localhost/pinktest"
        );

        let v6 = PostgresUrl::parse("postgres://u:p@[::1]:5432/db?sslmode=disable").unwrap();
        assert!(v6.is_local());
        assert_eq!(v6.port(), Some(5432));
        assert_eq!(v6.sslmode(), Some("disable"));
        assert!(!v6.redacted().contains(":p@"));
    }

    #[test]
    fn percent_encoded_passwords_are_scrubbed_in_both_forms() {
        let url = PostgresUrl::parse("postgres://u:p%40ss@host/db?schema=s1").unwrap();
        assert!(url.password_spellings().contains(&"p%40ss"));
        assert!(url.password_spellings().contains(&"p@ss"));
        assert_eq!(url.redacted(), "postgres://u:***@host/db?schema=s1");
        assert!(!redact_location("postgres://u:p%40ss@host/db").contains("p%40ss"));
        assert!(!redact_location("postgres://u:p%40ss@host/db").contains("p@ss"));
    }

    #[test]
    fn malformed_urls_still_redact() {
        let redacted = redact_location("postgres://user:secret@[broken/db");
        assert!(!redacted.contains("secret"), "{redacted}");
        assert!(redacted.contains("***"), "{redacted}");
        assert_eq!(redact_location("/tmp/lake"), "/tmp/lake");
        assert_eq!(redact_location("sqlite:lake.db"), "sqlite:lake.db");
        assert_eq!(redact_location("s3://bucket/prefix"), "s3://bucket/prefix");
    }

    #[test]
    fn invalid_schema_names_are_refused() {
        for schema in ["", "9lake", "lake-name", "lake.name", "lake name"] {
            let raw = format!("postgres://u:p@host/db?schema={schema}");
            let err = PostgresUrl::parse(&raw).unwrap_err();
            assert!(!err.contains(":p@"), "{err}");
        }
    }
}
