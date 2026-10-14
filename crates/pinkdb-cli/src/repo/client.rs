//! The HTTP side of the repository client: fetch `<base>/index.json`,
//! resolve `name[@version]` for the current platform, download the artifact
//! to the local cache (atomic temp + rename), verify its SHA-256, and report
//! where it landed.
//!
//! Resolution order is the configured repository order; the first repository
//! that has a matching artifact wins. A repository whose catalog cannot be
//! fetched is a hard error (no silent fallback to a later repository). The
//! current platform tag is derived at runtime from the OS and architecture
//! (`darwin-arm64` on an Apple-silicon Mac) and must match an entry exactly;
//! only ABI v1 artifacts are accepted. When no version is given, the highest
//! semver-ish version for the platform is chosen.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Duration;

use sha2::{Digest, Sha256};
use ureq::Agent;

use super::catalog::{lookup, Catalog, Entry, Lookup, ABI_VERSION};
use super::config::extensions_dir;

/// Catalog size cap (a catalog is a small JSON document).
pub const CATALOG_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Artifact size cap; a downloaded file larger than this is refused.
pub const ARTIFACT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Whole-request timeout (connect + transfer).
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// The platform tag used to match catalog entries, derived at runtime from
/// the OS and architecture (`std::env::consts`), e.g. `darwin-arm64`.
pub fn platform_tag() -> Result<String, String> {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        "linux" => "linux",
        "windows" => "windows",
        other => {
            return Err(format!(
                "this platform is not supported by the extension repository: os {other:?}"
            ))
        }
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => {
            return Err(format!(
                "this platform is not supported by the extension repository: architecture {other:?}"
            ))
        }
    };
    Ok(format!("{os}-{arch}"))
}

/// One shared HTTPS agent (connection pool + timeouts). `https_only` also
/// covers redirects: an `https://` repository can never be downgraded to
/// plain HTTP by a redirect, since the artifact it serves is loaded as native
/// code.
pub fn agent() -> Agent {
    Agent::config_builder()
        .timeout_global(Some(HTTP_TIMEOUT))
        .user_agent(concat!("pinkdb-cli/", env!("CARGO_PKG_VERSION")))
        .https_only(true)
        .build()
        .into()
}

/// Agent for a plain `http://` repository (loopback, or opted in with
/// `PINKDB_REPO_ALLOW_HTTP=1`; see `validate_repository_url`). Redirects are
/// not followed, so such a repository cannot bounce a download to another
/// host.
fn plain_http_agent() -> Agent {
    Agent::config_builder()
        .timeout_global(Some(HTTP_TIMEOUT))
        .user_agent(concat!("pinkdb-cli/", env!("CARGO_PKG_VERSION")))
        .max_redirects(0)
        .build()
        .into()
}

/// Fetch and parse one repository catalog.
pub fn fetch_catalog(agent: &Agent, base: &str) -> Result<Catalog, String> {
    let url = format!("{base}/index.json");
    let mut response = agent
        .get(&url)
        .call()
        .map_err(|e| format!("repository {base}: GET {url} failed: {e}"))?;
    let bytes = response
        .body_mut()
        .with_config()
        .limit(CATALOG_MAX_BYTES)
        .read_to_vec()
        .map_err(|e| read_error("catalog", base, &url, e, CATALOG_MAX_BYTES))?;
    Catalog::parse(&bytes).map_err(|e| format!("repository {base}: {e}"))
}

/// A successfully installed (downloaded or reused) artifact.
#[derive(Debug, Clone)]
pub struct Installed {
    /// Base URL of the repository the artifact came from.
    pub repository: String,
    pub entry: Entry,
    /// Local cache path: `$PINKDB_HOME/extensions/<name>/<version>/<platform>/<file>`.
    pub path: PathBuf,
    /// Hex SHA-256 of the bytes on disk (verified against the catalog).
    pub sha256: String,
    /// False when a previously cached copy was reused.
    pub downloaded: bool,
}

/// Resolve, download, and verify `name[@version]` from the configured
/// repositories (first match wins).
pub fn install(
    agent: &Agent,
    repositories: &[String],
    home: &Path,
    name: &str,
    version: Option<&str>,
) -> Result<Installed, String> {
    if repositories.is_empty() {
        return Err(
            "no repositories are configured; add one with `.repositories add <url>`".to_string(),
        );
    }
    let platform = platform_tag()?;
    let mut reasons = Vec::with_capacity(repositories.len());
    for base in repositories {
        let plain;
        let agent = if base.starts_with("http://") {
            plain = plain_http_agent();
            &plain
        } else {
            agent
        };
        let catalog = fetch_catalog(agent, base)?;
        match lookup(&catalog, name, version, &platform, ABI_VERSION) {
            Lookup::Found(entry) => {
                let entry = entry.clone();
                let dest = artifact_path(home, &entry);
                let (sha256, downloaded) = ensure_artifact(agent, base, &entry, &dest)?;
                return Ok(Installed {
                    repository: base.clone(),
                    entry,
                    path: dest,
                    sha256,
                    downloaded,
                });
            }
            Lookup::NoMatch(reason) => reasons.push(format!("{base}: {reason}")),
        }
    }
    let wanted = match version {
        Some(v) => format!("{name}@{v}"),
        None => name.to_string(),
    };
    Err(format!(
        "extension {wanted:?} was not found in any configured repository ({})",
        reasons.join("; ")
    ))
}

/// The cache path for one entry: the artifact's base name under
/// `extensions/<name>/<version>/<platform>/`.
pub fn artifact_path(home: &Path, entry: &Entry) -> PathBuf {
    let file = entry.file.rsplit('/').next().unwrap_or(&entry.file);
    extensions_dir(home)
        .join(&entry.name)
        .join(&entry.version)
        .join(&entry.platform)
        .join(file)
}

/// Return the artifact at `dest`, downloading it unless an already cached
/// copy matches the catalog's SHA-256. Downloads are written to a temp file
/// and renamed into place only after the hash is verified; a mismatch deletes
/// the temp file and errors, leaving nothing behind.
fn ensure_artifact(
    agent: &Agent,
    base: &str,
    entry: &Entry,
    dest: &Path,
) -> Result<(String, bool), String> {
    if dest.is_file() {
        if let Ok(sha) = sha256_file(dest) {
            if sha == entry.sha256 {
                return Ok((sha, false));
            }
        }
        // A cached file that does not match the catalog is replaced below.
    }
    let url = format!("{base}/{}", entry.file);
    let mut response = agent
        .get(&url)
        .call()
        .map_err(|e| format!("repository {base}: GET {url} failed: {e}"))?;
    let bytes = response
        .body_mut()
        .with_config()
        .limit(ARTIFACT_MAX_BYTES)
        .read_to_vec()
        .map_err(|e| read_error("artifact", base, &url, e, ARTIFACT_MAX_BYTES))?;
    let sha = sha256_hex(&bytes);

    let parent = dest
        .parent()
        .ok_or_else(|| format!("cannot determine the parent directory of {}", dest.display()))?;
    fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    let tmp = temp_path(dest);
    if let Err(e) = fs::write(&tmp, &bytes) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("cannot write {}: {e}", tmp.display()));
    }
    if sha != entry.sha256 {
        let _ = fs::remove_file(&tmp);
        return Err(format!(
            "sha256 mismatch for {} {} ({}) from {base}: catalog says {}, download has {}; artifact discarded",
            entry.name, entry.version, entry.platform, entry.sha256, sha
        ));
    }
    if let Err(e) = fs::rename(&tmp, dest) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("cannot publish {}: {e}", dest.display()));
    }
    Ok((sha, true))
}

fn read_error(what: &str, base: &str, url: &str, e: ureq::Error, cap: u64) -> String {
    if matches!(e, ureq::Error::BodyExceedsLimit(_)) {
        format!("repository {base}: {what} at {url} exceeds the {cap}-byte limit")
    } else {
        format!("repository {base}: cannot read {what} at {url}: {e}")
    }
}

fn temp_path(dest: &Path) -> PathBuf {
    let name = dest
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "artifact".to_string());
    dest.with_file_name(format!("{name}.tmp-{}", process::id()))
}

/// Hex SHA-256 of a byte slice.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Hex SHA-256 of a file, streamed in chunks.
pub fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

fn hex(digest: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn sample_entry(file: &str, sha256: &str) -> Entry {
        Entry {
            name: "math_ext".to_string(),
            version: "0.1.0".to_string(),
            abi: 1,
            platform: "darwin-arm64".to_string(),
            file: file.to_string(),
            sha256: sha256.to_string(),
            description: None,
        }
    }

    #[test]
    fn sha256_matches_the_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn platform_tag_is_derived_at_runtime() {
        let tag = platform_tag().unwrap();
        assert!(tag.contains('-'), "{tag}");
        assert_eq!(tag.split('-').count(), 2, "{tag}");
    }

    #[test]
    fn artifact_path_uses_the_file_base_name() {
        let home = Path::new("/home/user/.pinkdb");
        let entry = sample_entry(
            "artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib",
            &sha256_hex(b"x"),
        );
        assert_eq!(
            artifact_path(home, &entry),
            home.join("extensions/math_ext/0.1.0/darwin-arm64/math_ext.dylib")
        );
    }

    #[test]
    fn install_without_repositories_is_a_clear_error() {
        let agent = agent();
        let err = install(&agent, &[], Path::new("/nonexistent"), "math_ext", None).unwrap_err();
        assert!(err.contains("no repositories are configured"), "{err}");
    }

    #[test]
    fn cached_artifact_is_reused_without_network() {
        let dir = TempDir::new("cache");
        let entry = sample_entry(
            "artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib",
            &sha256_hex(b"cached bytes"),
        );
        let dest = artifact_path(dir.path(), &entry);
        fs::create_dir_all(dest.parent().unwrap()).unwrap();
        fs::write(&dest, b"cached bytes").unwrap();
        // The base URL is unreachable; a cache hit must not touch the network.
        let agent = agent();
        let (sha, downloaded) =
            ensure_artifact(&agent, "http://127.0.0.1:1", &entry, &dest).unwrap();
        assert_eq!(sha, entry.sha256);
        assert!(!downloaded);
    }

    #[test]
    fn corrupt_cached_artifact_is_not_reused() {
        let dir = TempDir::new("corrupt");
        let entry = sample_entry(
            "artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib",
            &sha256_hex(b"good bytes"),
        );
        let dest = artifact_path(dir.path(), &entry);
        fs::create_dir_all(dest.parent().unwrap()).unwrap();
        fs::write(&dest, b"corrupt").unwrap();
        // The cached copy does not match the catalog, so a download is
        // attempted (the unreachable base proves it was not reused).
        let agent = agent();
        let err = ensure_artifact(&agent, "http://127.0.0.1:1", &entry, &dest).unwrap_err();
        assert!(err.contains("GET http://127.0.0.1:1/"), "{err}");
        // The old file stays untouched unless a verified download replaces it.
        assert_eq!(fs::read(&dest).unwrap(), b"corrupt");
    }

    #[test]
    fn sha256_file_streams_and_matches() {
        let dir = TempDir::new("hashfile");
        let path = dir.path().join("blob");
        let big = vec![0xab; 200_000];
        fs::write(&path, &big).unwrap();
        assert_eq!(sha256_file(&path).unwrap(), sha256_hex(&big));
    }
}
