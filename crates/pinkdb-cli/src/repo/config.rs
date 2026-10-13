//! Repository configuration: `$PINKDB_HOME/repositories.json`.
//!
//! `$PINKDB_HOME` defaults to `$HOME/.pinkdb`. The config is a JSON object
//! with one ordered array of repository base URLs:
//!
//! ```json
//! {
//!   "repositories": [
//!     "https://pinkdb-ext-repo.defaultcms.workers.dev"
//!   ]
//! }
//! ```
//!
//! When the file does not exist the list defaults to the official repository
//! ([`DEFAULT_REPOSITORY`]); the file is only created by the first mutation
//! (`.repositories add` / `.repositories remove`), which materializes the
//! default into the file. The order is significant: the first repository with
//! a matching artifact wins. Writes are atomic (temp file + rename in the
//! same directory), so a crash can never leave a truncated config behind.

use std::fs;
use std::path::{Path, PathBuf};
use std::process;

/// The official extension repository, used when `repositories.json` is absent.
pub const DEFAULT_REPOSITORY: &str = "https://pinkdb-ext-repo.defaultcms.workers.dev";

/// `$PINKDB_HOME`, defaulting to `$HOME/.pinkdb` (`%USERPROFILE%` on Windows).
pub fn pinkdb_home() -> Result<PathBuf, String> {
    if let Some(value) = std::env::var_os("PINKDB_HOME") {
        if !value.is_empty() {
            return Ok(PathBuf::from(value));
        }
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| {
            "cannot determine the pinkdb home directory: set PINKDB_HOME or HOME".to_string()
        })?;
    Ok(PathBuf::from(home).join(".pinkdb"))
}

/// `$PINKDB_HOME/repositories.json`.
pub fn repositories_path(home: &Path) -> PathBuf {
    home.join("repositories.json")
}

/// `$PINKDB_HOME/extensions` — the artifact cache root.
pub fn extensions_dir(home: &Path) -> PathBuf {
    home.join("extensions")
}

/// The configured repositories, in resolution order.
pub fn load_repositories() -> Result<Vec<String>, String> {
    let home = pinkdb_home()?;
    load_repositories_from(&repositories_path(&home))
}

/// Like [`load_repositories`], from an explicit config path (testable without
/// touching the environment).
pub fn load_repositories_from(path: &Path) -> Result<Vec<String>, String> {
    match fs::read(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(vec![DEFAULT_REPOSITORY.to_string()])
        }
        Err(e) => Err(format!("cannot read {}: {e}", path.display())),
        Ok(bytes) => parse_repositories(path, &bytes),
    }
}

fn parse_repositories(path: &Path, bytes: &[u8]) -> Result<Vec<String>, String> {
    let invalid = |detail: String| format!("invalid repositories config {}: {detail}", path.display());
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| invalid(e.to_string()))?;
    let list = value
        .get("repositories")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid("expected an object with a \"repositories\" array".to_string()))?;
    let mut urls = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        let url = item
            .as_str()
            .ok_or_else(|| invalid(format!("repositories[{i}] is not a string")))?;
        let canonical = validate_repository_url(url)
            .map_err(|e| invalid(format!("repositories[{i}]: {e}")))?;
        urls.push(canonical);
    }
    Ok(urls)
}

/// Persist the repository list, creating `$PINKDB_HOME` as needed.
pub fn save_repositories(urls: &[String]) -> Result<(), String> {
    let home = pinkdb_home()?;
    save_repositories_to(&repositories_path(&home), urls)
}

/// Like [`save_repositories`], to an explicit config path. Atomic: the JSON is
/// written to a sibling temp file and renamed over the target, so readers
/// never observe a partial file.
pub fn save_repositories_to(path: &Path, urls: &[String]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("cannot determine the parent directory of {}", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    let value = serde_json::json!({ "repositories": urls });
    let mut body = serde_json::to_string_pretty(&value)
        .map_err(|e| format!("cannot serialize repositories config: {e}"))?;
    body.push('\n');
    let tmp = temp_path(path);
    if let Err(e) = fs::write(&tmp, &body) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("cannot write {}: {e}", tmp.display()));
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("cannot replace {}: {e}", path.display()));
    }
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "repositories.json".to_string());
    path.with_file_name(format!("{name}.tmp-{}", process::id()))
}

/// Append `url` (canonicalized) unless it is already configured; returns the
/// canonical URL that was added. Order is preserved: new entries go last.
pub fn add_repository(urls: &mut Vec<String>, url: &str) -> Result<String, String> {
    let canonical = validate_repository_url(url)?;
    if urls.iter().any(|u| u == &canonical) {
        return Err(format!("repository {canonical} is already configured"));
    }
    urls.push(canonical.clone());
    Ok(canonical)
}

/// Remove `url` (canonicalized); returns the canonical URL that was removed.
pub fn remove_repository(urls: &mut Vec<String>, url: &str) -> Result<String, String> {
    let canonical = validate_repository_url(url)?;
    let before = urls.len();
    urls.retain(|u| u != &canonical);
    if urls.len() == before {
        return Err(format!("repository {canonical} is not configured"));
    }
    Ok(canonical)
}

/// Validate and canonicalize a repository base URL.
///
/// Accepted: `http://` and `https://` base URLs with a non-empty host, an
/// optional port and an optional path prefix. Trailing slashes are stripped so
/// `https://host/` and `https://host` are the same repository. Rejected:
/// other schemes, empty hosts, whitespace, backslashes, `?`/`#` (the client
/// appends `/index.json` and `/<file>` itself), and userinfo (`@`). Plain
/// `http://` is allowed for local/testing repositories; use `https://` for
/// anything remote.
pub fn validate_repository_url(url: &str) -> Result<String, String> {
    let url = url.trim();
    if url.is_empty() {
        return Err("repository URL is empty".to_string());
    }
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| format!("repository URL {url:?} must start with http:// or https://"))?;
    if url
        .chars()
        .any(|c| c.is_whitespace() || c == '\\' || c == '?' || c == '#')
    {
        return Err(format!(
            "repository URL {url:?} contains unsupported characters (whitespace, backslash, '?', '#')"
        ));
    }
    if rest.is_empty() || rest.starts_with('/') {
        return Err(format!("repository URL {url:?} has no host"));
    }
    let host = rest.split('/').next().unwrap_or("");
    if !host.chars().any(|c| c.is_ascii_alphanumeric()) {
        return Err(format!("repository URL {url:?} has no host"));
    }
    if host.contains('@') {
        return Err(format!(
            "repository URL {url:?} must not contain userinfo ('@')"
        ));
    }
    Ok(url.trim_end_matches('/').to_string())
}

/// One cached artifact file found under `$PINKDB_HOME/extensions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledFile {
    pub name: String,
    pub version: String,
    pub platform: String,
    pub path: PathBuf,
}

/// List cached artifacts: `extensions/<name>/<version>/<platform>/<file>`.
/// Sorted by name, version, platform, then file name. Missing cache root is
/// not an error (nothing installed yet); other I/O errors are reported.
pub fn list_installed(home: &Path) -> Result<Vec<InstalledFile>, String> {
    let root = extensions_dir(home);
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut names = read_dirs(&root)?;
    names.sort();
    for name in names {
        let mut versions = read_dirs(&name)?;
        versions.sort();
        for version in versions {
            let mut platforms = read_dirs(&version)?;
            platforms.sort();
            for platform in platforms {
                let mut files: Vec<PathBuf> = fs::read_dir(&platform)
                    .map_err(|e| format!("cannot list {}: {e}", platform.display()))?
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| {
                        // Interrupted downloads leave `<file>.tmp-<pid>`
                        // siblings behind; they are not installed artifacts.
                        let temp = p
                            .file_name()
                            .is_some_and(|n| n.to_string_lossy().contains(".tmp"));
                        p.is_file() && !temp
                    })
                    .collect();
                files.sort();
                for path in files {
                    out.push(InstalledFile {
                        name: dir_name(&name),
                        version: dir_name(&version),
                        platform: dir_name(&platform),
                        path,
                    });
                }
            }
        }
    }
    Ok(out)
}

/// Immediate subdirectories of `dir`, sorted by the caller.
fn read_dirs(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| format!("cannot list {}: {e}", dir.display()))? {
        let entry = entry.map_err(|e| format!("cannot list {}: {e}", dir.display()))?;
        let path = entry.path();
        let hidden = path
            .file_name()
            .map(|n| n.to_string_lossy().starts_with('.'))
            .unwrap_or(false);
        if path.is_dir() && !hidden {
            out.push(path);
        }
    }
    Ok(out)
}

fn dir_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A self-cleaning temp directory (unit tests cannot use
    /// `CARGO_TARGET_TMPDIR`, which is integration-test only).
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> TempDir {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "pinkdb_cli_repo_{tag}_{}_{}",
                process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            TempDir(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn absent_config_defaults_to_the_official_repository() {
        let dir = TempDir::new("default");
        let path = dir.path().join("repositories.json");
        let urls = load_repositories_from(&path).unwrap();
        assert_eq!(urls, vec![DEFAULT_REPOSITORY.to_string()]);
        // Reading the default does not create the file.
        assert!(!path.exists());
    }

    #[test]
    fn config_round_trips_and_is_atomic() {
        let dir = TempDir::new("roundtrip");
        let path = dir.path().join("repositories.json");
        let urls = vec![
            "https://one.example".to_string(),
            "http://127.0.0.1:8080/prefix".to_string(),
        ];
        save_repositories_to(&path, &urls).unwrap();
        assert_eq!(load_repositories_from(&path).unwrap(), urls);
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"repositories\""), "{text}");
        assert!(text.ends_with('\n'), "{text:?}");
        // No temp file survives a successful write.
        let leftovers: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files left behind: {leftovers:?}");
    }

    #[test]
    fn empty_list_round_trips_as_empty_not_default() {
        let dir = TempDir::new("empty");
        let path = dir.path().join("repositories.json");
        save_repositories_to(&path, &[]).unwrap();
        assert!(load_repositories_from(&path).unwrap().is_empty());
    }

    #[test]
    fn malformed_config_is_a_clear_error() {
        let dir = TempDir::new("malformed");
        let path = dir.path().join("repositories.json");
        fs::write(&path, b"not json").unwrap();
        let err = load_repositories_from(&path).unwrap_err();
        assert!(err.contains("invalid repositories config"), "{err}");
        fs::write(&path, b"{\"repositories\": \"nope\"}").unwrap();
        let err = load_repositories_from(&path).unwrap_err();
        assert!(err.contains("\"repositories\" array"), "{err}");
        fs::write(&path, b"{\"repositories\": [42]}").unwrap();
        let err = load_repositories_from(&path).unwrap_err();
        assert!(err.contains("repositories[0] is not a string"), "{err}");
    }

    #[test]
    fn url_validation_and_canonicalization() {
        assert_eq!(
            validate_repository_url("https://host/").unwrap(),
            "https://host"
        );
        assert_eq!(
            validate_repository_url("http://127.0.0.1:8080///").unwrap(),
            "http://127.0.0.1:8080"
        );
        assert_eq!(
            validate_repository_url(" https://host/path ").unwrap(),
            "https://host/path"
        );
        for bad in [
            "",
            "host",
            "ftp://host",
            "https://",
            "https:///path",
            "https://us er",
            "https://host?x=1",
            "https://host#frag",
            "https://user@host",
            "https://host\\x",
        ] {
            assert!(
                validate_repository_url(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn add_dedupes_and_remove_keeps_order() {
        let mut urls = vec!["https://a".to_string(), "https://b".to_string()];
        assert_eq!(add_repository(&mut urls, "https://c/").unwrap(), "https://c");
        assert_eq!(urls, ["https://a", "https://b", "https://c"]);
        // Duplicates (canonical form) are refused.
        let err = add_repository(&mut urls, "https://c").unwrap_err();
        assert!(err.contains("already configured"), "{err}");
        assert_eq!(urls.len(), 3);
        // Removal is by canonical URL and keeps the remaining order.
        assert_eq!(remove_repository(&mut urls, "https://b/").unwrap(), "https://b");
        assert_eq!(urls, ["https://a", "https://c"]);
        let err = remove_repository(&mut urls, "https://b").unwrap_err();
        assert!(err.contains("not configured"), "{err}");
        // Removing every entry is allowed (installs then fail with a note).
        remove_repository(&mut urls, "https://a").unwrap();
        remove_repository(&mut urls, "https://c").unwrap();
        assert!(urls.is_empty());
    }

    #[test]
    fn list_installed_walks_the_cache_tree() {
        let dir = TempDir::new("installed");
        let home = dir.path();
        // Missing root: empty, not an error.
        assert!(list_installed(home).unwrap().is_empty());
        let artifact = extensions_dir(home).join("math_ext/0.1.0/darwin-arm64/math_ext.dylib");
        fs::create_dir_all(artifact.parent().unwrap()).unwrap();
        fs::write(&artifact, b"bytes").unwrap();
        // A stray temp file from an interrupted download is not listed.
        fs::write(
            artifact.with_file_name("math_ext.dylib.tmp-99999"),
            b"partial",
        )
        .unwrap();
        // Hidden directories and loose files are ignored.
        fs::create_dir_all(extensions_dir(home).join(".hidden/x")).unwrap();
        fs::write(extensions_dir(home).join("loose.txt"), b"x").unwrap();
        let listed = list_installed(home).unwrap();
        assert_eq!(
            listed,
            vec![InstalledFile {
                name: "math_ext".to_string(),
                version: "0.1.0".to_string(),
                platform: "darwin-arm64".to_string(),
                path: artifact,
            }]
        );
    }
}
