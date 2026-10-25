//! Extension installation: one implementation for SQL `INSTALL`,
//! `LOAD <name>`, and the CLI's `.install`.
//!
//! An extension is installed by resolving `name[@version]` in the configured
//! repositories (`$PINKDB_HOME/repositories.json`), downloading it into the
//! per-user cache `$PINKDB_HOME/extensions/<name>/<version>/<platform>/` after
//! verifying the signed catalog and the artifact's SHA-256 (see the `repo`
//! module and `docs/extension-signing.md`), and then loading the cached file.
//!
//! **The cache path is the path the loader opens**, so it is also the source
//! the WAL records when a later statement calls the extension's functions —
//! a recorded path must keep existing for replay to be able to resolve it.
//! `INSTALL` therefore re-hashes the cached file right before the load
//! (fail closed on a mismatch) instead of copying it to a private temporary
//! directory that would be deleted and could never be replayed. `LOAD <name>`
//! resolves the artifact in that same cache and trusts it: only this user can
//! write there, and it was hash-verified when `INSTALL` put it there.
//!
//! **WAL.** `INSTALL` is not logged: it downloads and caches a file, which is
//! not replay-deterministic, and a replay must never reach the network. The
//! load it performs is not a record of its own either — like any load, it is
//! re-established when a logged statement that calls the extension is
//! replayed (the WAL prefixes `LOAD EXTENSION '<cache path>'`, and the replay
//! rules apply). `LOAD <name>` *is* logged: its statement text is recorded
//! and re-resolved on replay through the same rules.
//!
//! **Gating.** Both statements need the `repo` feature; without it the engine
//! reports [`NO_REPO_SUPPORT`] and nothing else changes. Executing them also
//! requires the connection's SQL-load switch (`allow_sql_load_extension`) to
//! be on, exactly like `LOAD EXTENSION`: it is off by default, and the server
//! turns it on only with `--allow-load-extension`.

#[cfg(feature = "repo")]
use std::fs;
use std::path::{Path, PathBuf};

use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result};

/// A successfully installed (downloaded or reused) artifact.
#[derive(Debug, Clone)]
pub struct InstalledExtension {
    /// Base URL of the repository the artifact came from.
    pub repository: String,
    pub name: String,
    pub version: String,
    pub platform: String,
    /// Local cache path: `$PINKDB_HOME/extensions/<name>/<version>/<platform>/<file>`.
    pub path: PathBuf,
    /// Hex SHA-256 of the bytes on disk (verified against the catalog).
    pub sha256: String,
    /// False when a previously cached copy was reused.
    pub downloaded: bool,
    /// Id of the catalog signing key; `None` only in the unsigned development
    /// mode (`PINKDB_EXT_ALLOW_UNSIGNED=1`).
    pub signed_by: Option<String>,
}

/// The error a build without the `repo` feature reports for `INSTALL`,
/// `LOAD <name>`, `.install` and the repository dot-commands.
pub const NO_REPO_SUPPORT: &str = "this build has no extension repository support (rebuild with \
                                    --features repo, or use the pinkdb-repo release variant)";

#[cfg(not(feature = "repo"))]
fn no_repo() -> PinkDbError {
    PinkDbError::Execution(NO_REPO_SUPPORT.to_string())
}

#[cfg(feature = "repo")]
fn execution(message: String) -> PinkDbError {
    PinkDbError::Execution(message)
}

impl PinkDb {
    /// Resolve, download, verify and cache `name[@version]`, then re-hash the
    /// cached bytes. The caller loads [`InstalledExtension::path`] (see
    /// [`Self::load_installed_extension`]); `.install` and `INSTALL` are the
    /// two callers.
    ///
    /// `from` overrides the configured repositories with one base URL. With
    /// `force`, a cached copy is never reused (it is downloaded and verified
    /// again).
    #[cfg(feature = "repo")]
    pub fn install_extension(
        &mut self,
        name: &str,
        version: Option<&str>,
        from: Option<&str>,
        force: bool,
    ) -> Result<InstalledExtension> {
        let home = crate::repo::pinkdb_home().map_err(execution)?;
        let repositories = match from {
            Some(url) => vec![crate::repo::validate_repository_url(url).map_err(execution)?],
            None => crate::repo::load_repositories().map_err(execution)?,
        };
        let installed = crate::repo::install(
            &crate::repo::agent(),
            &repositories,
            &home,
            name,
            version,
            force,
        )
        .map_err(execution)?;
        // The file the loader will map is the cache file: verify it again,
        // against the hash the signed catalog gave, so a mismatch between the
        // download and the load is impossible.
        let actual = crate::repo::client::sha256_file(&installed.path).map_err(execution)?;
        if actual != installed.sha256 {
            return Err(execution(format!(
                "cached artifact {} failed re-hash: catalog says {}, file has {}; refusing to \
                 load it",
                installed.path.display(),
                installed.sha256,
                actual
            )));
        }
        Ok(installed)
    }

    /// Without the `repo` feature there is no repository to resolve `name` in.
    #[cfg(not(feature = "repo"))]
    pub fn install_extension(
        &mut self,
        _name: &str,
        _version: Option<&str>,
        _from: Option<&str>,
        _force: bool,
    ) -> Result<InstalledExtension> {
        Err(no_repo())
    }

    /// Fail before any network work when this build cannot talk to a
    /// repository, so `INSTALL` names the real cause (a missing feature, not a
    /// closed connection gate).
    pub fn require_repository_support(&self) -> Result<()> {
        #[cfg(feature = "repo")]
        {
            Ok(())
        }
        #[cfg(not(feature = "repo"))]
        {
            Err(no_repo())
        }
    }

    /// Load the installed artifact at `path`, unless that exact source is
    /// already loaded (installing or loading twice is a no-op, not the
    /// duplicate-name error a second `LOAD EXTENSION` raises).
    pub fn load_installed_extension(&mut self, path: &Path) -> Result<String> {
        let source = path.to_string_lossy().into_owned();
        if let Some(name) = self.loaded_extension_name_for_path(&source) {
            return Ok(name);
        }
        self.load_extension(&source)
    }

    /// The cache path of the highest installed version of `name` for this
    /// platform — what `LOAD <name>` loads.
    #[cfg(feature = "repo")]
    pub fn installed_extension_path(&self, name: &str) -> Result<PathBuf> {
        let home = crate::repo::pinkdb_home().map_err(execution)?;
        installed_extension_path_in(&home, name).map_err(execution)
    }

    /// Without the `repo` feature nothing is ever installed.
    #[cfg(not(feature = "repo"))]
    pub fn installed_extension_path(&self, _name: &str) -> Result<PathBuf> {
        Err(no_repo())
    }
}

/// Resolve `name` in the extension cache under `home`: the highest version
/// that has an artifact for this platform. The name must be one path
/// component (SQL identifiers are; the CLI's `.install` is the other entry
/// point).
#[cfg(feature = "repo")]
fn installed_extension_path_in(home: &Path, name: &str) -> std::result::Result<PathBuf, String> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
        return Err(format!("{name:?} is not a valid extension name"));
    }
    let platform = crate::repo::client::platform_tag()?;
    let root = crate::repo::config::extensions_dir(home).join(name);
    let mut versions: Vec<(String, PathBuf)> = fs::read_dir(&root)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().is_dir())
                .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
                .collect()
        })
        .unwrap_or_default();
    if versions.is_empty() {
        return Err(format!(
            "extension {name:?} is not installed; run INSTALL {name} (cache: {})",
            root.display()
        ));
    }
    versions.sort_by(|a, b| crate::repo::catalog::compare_versions(&a.0, &b.0));
    let installed = versions
        .iter()
        .map(|(v, _)| v.clone())
        .collect::<Vec<_>>()
        .join(", ");
    versions.reverse();
    for (_, dir) in &versions {
        let Ok(files) = fs::read_dir(dir.join(&platform)) else {
            continue;
        };
        let mut files: Vec<PathBuf> = files
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && !p.to_string_lossy().contains(".tmp"))
            .collect();
        files.sort();
        if let Some(path) = files.into_iter().next() {
            return Ok(path);
        }
    }
    Err(format!(
        "extension {name:?} has no artifact for platform {platform:?} (installed versions: {})",
        installed
    ))
}

#[cfg(all(test, feature = "repo"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> TempDir {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "pinkdb_install_{tag}_{}_{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            TempDir(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn touch(dir: &Path, version: &str, platform: &str, file: &str) {
        let path = dir.join(version).join(platform);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(file), b"artifact").unwrap();
    }

    #[test]
    fn installed_path_picks_the_highest_version_for_this_platform() {
        let home = TempDir::new("versions");
        let ext = home.0.join("extensions/math_ext");
        let platform = crate::repo::client::platform_tag().unwrap();
        touch(&ext, "0.1.0", &platform, "ext.dylib");
        touch(&ext, "0.10.0", &platform, "ext.dylib");
        touch(&ext, "0.9.0", &platform, "ext.dylib");
        // A higher version for another platform must not win.
        touch(&ext, "9.0.0", "plan9-sparc", "ext.dylib");
        assert_eq!(
            installed_extension_path_in(&home.0, "math_ext").unwrap(),
            home.0
                .join("extensions/math_ext/0.10.0")
                .join(platform)
                .join("ext.dylib")
        );
    }

    #[test]
    fn installed_path_ignores_interrupted_downloads() {
        let home = TempDir::new("tmpfiles");
        let platform = crate::repo::client::platform_tag().unwrap();
        let dir = home.0.join("extensions/math_ext/0.1.0").join(&platform);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("math_ext.dylib.tmp-1-2"), b"partial").unwrap();
        fs::write(dir.join("math_ext.dylib"), b"artifact").unwrap();
        assert_eq!(
            installed_extension_path_in(&home.0, "math_ext").unwrap(),
            dir.join("math_ext.dylib")
        );
    }

    #[test]
    fn missing_or_platformless_extensions_are_clear_errors() {
        let home = TempDir::new("missing");
        let err = installed_extension_path_in(&home.0, "math_ext").unwrap_err();
        assert!(err.contains("is not installed"), "{err}");
        assert!(err.contains("run INSTALL math_ext"), "{err}");

        touch(
            &home.0.join("extensions/math_ext"),
            "0.1.0",
            "plan9-sparc",
            "ext.dylib",
        );
        let err = installed_extension_path_in(&home.0, "math_ext").unwrap_err();
        assert!(err.contains("no artifact for platform"), "{err}");
        assert!(err.contains("installed versions: 0.1.0"), "{err}");
    }

    #[test]
    fn a_name_outside_the_cache_root_is_refused() {
        let home = TempDir::new("escape");
        for name in ["", ".", "..", "../math_ext", "a/b"] {
            let err = installed_extension_path_in(&home.0, name).unwrap_err();
            assert!(err.contains("not a valid extension name"), "{name}: {err}");
        }
    }
}
