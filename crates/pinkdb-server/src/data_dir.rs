//! The server's data directory (wave 124) and its lake location policy.
//!
//! A server owns one directory (`--data-dir`, default `./pinkdb-data`):
//!
//! ```text
//! pinkdb-data/
//!   pinkdb.pink (+ .wal, .lock)   local databases, users/roles/grants, and
//!                                 the list of lake databases
//!   state.pink  (+ sidecars)      console worksheets, vault, agent, memory
//!   lakes/<name>/                 lakes created with no explicit location
//!   tmp/                          upload staging
//! ```
//!
//! Lakes are databases of the served store (`CREATE DATABASE … (TYPE LAKE)`,
//! `ATTACH`): the engine records their locations, and pink-lake opens them.
//! The server confines where they may live ([`lake_policy`]): a local lake
//! path must resolve inside `lakes/` or a `--lake-root`, a remote catalog is
//! `postgres://` and remote data `s3://`, and a URL may not carry a password
//! (the engine refuses those outright: the location lands in the WAL).
//! `ATTACH`/`DETACH`/`CREATE DATABASE` are ADMIN-only through RBAC.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use pinkdb::{LakeOpenRequest, LakePolicy, PinkDbError};

/// The default `--data-dir`.
pub const DEFAULT_DATA_DIR: &str = "./pinkdb-data";

/// A server's data directory, created on first start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDir {
    root: PathBuf,
}

impl DataDir {
    /// Create (or reuse) the layout under `root`. The directories are created
    /// private (0700 on Unix): they hold the store and its credentials.
    pub fn prepare(root: impl AsRef<Path>) -> std::io::Result<DataDir> {
        let root = root.as_ref();
        for dir in [root.to_path_buf(), root.join("lakes"), root.join("tmp")] {
            create_private_dir(&dir)?;
        }
        // Canonical, so the policy compares real paths (a symlinked data
        // directory resolves once, here).
        Ok(DataDir {
            root: root.canonicalize()?,
        })
    }

    /// The data directory itself.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The served store: `pinkdb.pink`.
    pub fn store(&self) -> PathBuf {
        self.root.join("pinkdb.pink")
    }

    /// The state store: `state.pink`.
    pub fn state(&self) -> PathBuf {
        self.root.join("state.pink")
    }

    /// Where managed lakes live, and where relative lake paths resolve.
    pub fn lakes(&self) -> PathBuf {
        self.root.join("lakes")
    }

    /// Upload staging.
    pub fn tmp(&self) -> PathBuf {
        self.root.join("tmp")
    }
}

fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// The lake location policy for a store whose managed lakes live in `lakes`
/// (`None` for an in-memory server) with the extra `roots`
/// (`--lake-root`, canonicalized by the caller).
pub fn lake_policy(lakes: Option<PathBuf>, roots: Vec<PathBuf>) -> LakePolicy {
    let mut allowed: Vec<PathBuf> = Vec::new();
    for dir in lakes.into_iter().chain(roots) {
        allowed.push(dir.canonicalize().unwrap_or(dir));
    }
    Arc::new(move |request: &LakeOpenRequest| check_request(&allowed, request))
}

fn refused(message: String) -> PinkDbError {
    PinkDbError::Execution(format!("lake location refused: {message}"))
}

fn check_request(allowed: &[PathBuf], request: &LakeOpenRequest) -> pinkdb::Result<()> {
    check_location(allowed, &request.catalog, true)?;
    if let Some(data) = &request.data {
        check_location(allowed, data, false)?;
    }
    Ok(())
}

/// One catalog or data location: a URL of an allowed scheme, or a local path
/// inside an allowed directory.
fn check_location(allowed: &[PathBuf], location: &str, catalog: bool) -> pinkdb::Result<()> {
    if let Some((scheme, _)) = location.split_once("://") {
        let scheme = scheme.to_ascii_lowercase();
        let ok = if catalog {
            matches!(scheme.as_str(), "postgres" | "postgresql" | "pink+s3")
        } else {
            scheme == "s3"
        };
        if !ok {
            return Err(refused(format!(
                "a lake {} may be a local path{}; {scheme}:// is not served",
                if catalog { "catalog" } else { "data location" },
                if catalog {
                    ", postgres:// or pink+s3://"
                } else {
                    " or s3://"
                },
            )));
        }
        return Ok(());
    }
    let path = location
        .strip_prefix("pink:")
        .or_else(|| location.strip_prefix("sqlite:"))
        .unwrap_or(location);
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(refused(format!(
            "{} is not an absolute path",
            path.display()
        )));
    }
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(refused(format!("{} contains `..`", path.display())));
    }
    let real = existing_ancestor_canonical(path);
    if allowed.iter().any(|dir| real.starts_with(dir)) {
        return Ok(());
    }
    Err(refused(if allowed.is_empty() {
        format!(
            "{} is a local path, and this server has no directory for lakes (start it with \
             --data-dir or --lake-root DIR)",
            path.display()
        )
    } else {
        format!(
            "{} is outside the server's lake directories ({}); start the server with \
             --lake-root DIR to allow another",
            path.display(),
            allowed
                .iter()
                .map(|d| d.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }))
}

/// `path` with its longest existing ancestor canonicalized (symlinks
/// resolved), so a link inside an allowed directory cannot point outside it.
fn existing_ancestor_canonical(path: &Path) -> PathBuf {
    let mut rest: Vec<&std::ffi::OsStr> = Vec::new();
    let mut cursor = path;
    loop {
        if let Ok(real) = cursor.canonicalize() {
            let mut out = real;
            for part in rest.iter().rev() {
                out.push(part);
            }
            return out;
        }
        match (cursor.parent(), cursor.file_name()) {
            (Some(parent), Some(name)) => {
                rest.push(name);
                cursor = parent;
            }
            _ => return path.to_path_buf(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(catalog: &str, data: Option<&str>) -> LakeOpenRequest {
        LakeOpenRequest {
            database: "x".to_string(),
            catalog: catalog.to_string(),
            data: data.map(str::to_string),
            read_only: false,
            version: None,
            create: true,
            create_if_missing: false,
            managed: false,
            startup: false,
            s3: None,
            postgres_password: None,
            data_inlining_row_limit: None,
            data_file_format: None,
        }
    }

    #[test]
    fn local_lakes_stay_inside_the_allowed_directories() {
        let base = std::env::temp_dir().join(format!("pinkdb_lake_policy_{}", std::process::id()));
        let dir = DataDir::prepare(&base).unwrap();
        let root = base.join("archive");
        std::fs::create_dir_all(&root).unwrap();
        let policy = lake_policy(Some(dir.lakes()), vec![root.clone()]);
        let lakes = dir.lakes();
        let inside = format!("pink:{}/a/catalog.pink", lakes.display());
        assert!(policy(&request(&inside, None)).is_ok());
        let rooted = format!("sqlite:{}/b.sqlite", root.canonicalize().unwrap().display());
        assert!(policy(&request(&rooted, None)).is_ok());
        let outside = request("pink:/etc/lake/catalog.pink", None);
        let err = policy(&outside).unwrap_err().to_string();
        assert!(
            err.contains("outside the server's lake directories"),
            "{err}"
        );
        let dotdot = format!("pink:{}/../escape.pink", lakes.display());
        assert!(policy(&request(&dotdot, None)).is_err());
        assert!(policy(&request("postgres://h/db", Some("s3://b/p"))).is_ok());
        let err = policy(&request("http://h/x", None))
            .unwrap_err()
            .to_string();
        assert!(err.contains("http:// is not served"), "{err}");
        let err = policy(&request(&inside, Some("/tmp/elsewhere")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("outside"), "{err}");
        // An in-memory server with no --lake-root serves no local lakes.
        let none = lake_policy(None, Vec::new());
        let err = none(&request(&inside, None)).unwrap_err().to_string();
        assert!(err.contains("no directory for lakes"), "{err}");
        let _ = std::fs::remove_dir_all(&base);
    }
}
