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

/// What the Storage panel shows for a server that keeps its data in
/// `data_dir` (durable) or in memory (wave 124). Every database's own
/// storage — local or a lake — comes from the catalog, per request.
pub fn storage_info(
    data_dir: Option<&crate::DataDir>,
    lake_roots: &[std::path::PathBuf],
    import_formats: Vec<String>,
) -> StorageInfo {
    let roots = || {
        lake_roots
            .iter()
            .map(|r| r.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    match data_dir {
        Some(dir) => {
            let mut facts = vec![
                ("store".to_string(), "pinkdb.pink + WAL".to_string()),
                (
                    "managed lakes".to_string(),
                    dir.lakes().display().to_string(),
                ),
            ];
            if !lake_roots.is_empty() {
                facts.push(("lake roots".to_string(), roots()));
            }
            StorageInfo {
                mode: "durable".to_string(),
                location: dir.root().display().to_string(),
                facts,
                live: None,
                import_formats,
            }
        }
        None => StorageInfo {
            mode: "in-memory".to_string(),
            location: "(memory: nothing is saved)".to_string(),
            facts: if lake_roots.is_empty() {
                Vec::new()
            } else {
                vec![("lake roots".to_string(), roots())]
            },
            live: None,
            import_formats,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_names_the_data_directory_or_memory() {
        let base = std::env::temp_dir().join(format!("pinkdb_ui_storage_{}", std::process::id()));
        let dir = crate::DataDir::prepare(&base).unwrap();
        let info = storage_info(Some(&dir), &[], vec![]);
        assert_eq!(info.mode, "durable");
        assert_eq!(info.location, dir.root().display().to_string());
        assert!(info.facts.iter().any(|(k, _)| k == "managed lakes"));
        let info = storage_info(None, &[std::path::PathBuf::from("/srv/lakes")], vec![]);
        assert_eq!(info.mode, "in-memory");
        assert_eq!(info.facts[0].1, "/srv/lakes");
        let _ = std::fs::remove_dir_all(&base);
    }
}
