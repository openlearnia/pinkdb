//! The warehouse browsing UI (wave 78).
//!
//! The console itself lives in the sibling crate
//! [`pinkdb_server_ui`](https://docs.rs/pinkdb-server-ui); this module
//! re-exports its surface so an embedder of `pinkdb-server` does not have to
//! depend on the console crate directly:
//!
//! ```no_run
//! use pinkdb::Database;
//! use pinkdb_server::ui::{UiAuth, UiOptions, UiServer};
//!
//! let opts = UiOptions {
//!     auth: UiAuth::Password,
//!     ..UiOptions::default()
//! };
//! let server = UiServer::bind("127.0.0.1:8080", Database::new(), opts)?;
//! let handle = server.spawn()?;
//! println!("console on {}", handle.local_addr());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The console is **feature-gated**: it exists only in a build with the
//! `ui` cargo feature. It asks for sign-in by default, binds loopback unless
//! it has a certificate ([`UiTls`]), and refuses to serve a store that has
//! users without sign-in. See `docs/server-ui.md`.

pub use pinkdb_server_ui::{
    ConsoleTls, LiveFacts, StorageInfo, UiAuth, UiError, UiHandle, UiOptions, UiServer, UiTls,
    check_auth_policy, check_bind_policy, redact,
};

/// The catalog backend a lake catalog location names (pink-lake's location
/// rules: `postgres://`, `sqlite:`, `pink:`, `pink+s3://`, else a local
/// directory).
pub fn catalog_backend(catalog: &str) -> &'static str {
    let lower = catalog.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        "Postgres"
    } else if lower.starts_with("sqlite:") {
        "SQLite"
    } else if lower.starts_with("pink+s3://") {
        "pinkdb (.pink on S3)"
    } else if lower.starts_with("pink:") {
        "pinkdb (.pink file)"
    } else {
        "directory"
    }
}

/// What the Storage panel shows for a server that serves `file` (durable),
/// a lake `catalog` over `data`, or memory. Secrets in the locations are
/// redacted here; `version` reads a lake's published version per request.
pub fn storage_info(
    file: Option<&str>,
    catalog: Option<&str>,
    data: Option<&str>,
    version: Option<std::sync::Arc<dyn Fn() -> u64 + Send + Sync>>,
    import_formats: Vec<String>,
) -> StorageInfo {
    match (file, catalog) {
        (Some(path), _) => StorageInfo {
            mode: "durable".to_string(),
            location: path.to_string(),
            facts: vec![("format".to_string(), ".pink file + WAL".to_string())],
            live: None,
            import_formats,
        },
        (None, Some(catalog)) => {
            let data = data.unwrap_or_default();
            StorageInfo {
                mode: "lake".to_string(),
                location: redact(catalog),
                facts: vec![
                    ("catalog".to_string(), catalog_backend(catalog).to_string()),
                    (
                        "data".to_string(),
                        if data.starts_with("s3://") {
                            format!("{} (S3)", redact(data))
                        } else {
                            redact(data)
                        },
                    ),
                ],
                live: version.map(|version| {
                    std::sync::Arc::new(move || {
                        vec![("lake version".to_string(), version().to_string())]
                    })
                        as std::sync::Arc<dyn Fn() -> Vec<(String, String)> + Send + Sync>
                }),
                import_formats,
            }
        }
        (None, None) => StorageInfo {
            mode: "in-memory".to_string(),
            location: "(memory)".to_string(),
            facts: Vec::new(),
            live: None,
            import_formats,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lake_locations_name_their_backend_without_secrets() {
        assert_eq!(catalog_backend("postgres://u:p@h/db"), "Postgres");
        assert_eq!(catalog_backend("sqlite:/x/cat.db"), "SQLite");
        assert_eq!(catalog_backend("pink:/x/cat.pink"), "pinkdb (.pink file)");
        let info = storage_info(
            None,
            Some("postgres://pink:hunter2@db/lake"),
            Some("s3://bucket/lake"),
            Some(std::sync::Arc::new(|| 7)),
            vec![],
        );
        assert_eq!(info.mode, "lake");
        assert!(!info.location.contains("hunter2"), "{}", info.location);
        assert_eq!(info.facts[1].1, "s3://bucket/lake (S3)");
        assert_eq!((info.live.unwrap())()[0].1, "7");
        assert_eq!(
            storage_info(None, None, None, None, vec![]).mode,
            "in-memory"
        );
    }
}
