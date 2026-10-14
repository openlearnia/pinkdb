//! Repository client tests (`repo` feature): a tiny std-only HTTP fixture
//! server serves a catalog and artifact bytes over loopback, and the real
//! CLI binary is driven through `.repositories` / `.install` / `.installed`
//! end to end — config round-trip, resolution order, version selection,
//! platform/ABI errors, SHA-256 refusal, atomic downloads, cache reuse, and a
//! full install → `LOAD EXTENSION` → query round trip.
//!
//! Everything here is hermetic (loopback only); the one live test against the
//! official repository is skipped unless `PINKDB_REPO_LIVE=1`.

#![cfg(feature = "repo")]

mod common;

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use sha2::{Digest, Sha256};

const OFFICIAL: &str = "https://pinkdb-ext-repo.defaultcms.workers.dev";

// ---------- temp homes ----------

/// A unique per-test `PINKDB_HOME`. The directory is deliberately NOT created
/// up front: config writes must create it.
struct TempHome(PathBuf);

impl TempHome {
    fn new(tag: &str) -> TempHome {
        let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("pinkdb_cli_repo_{tag}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        TempHome(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempHome {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ---------- the CLI and the fixture server ----------

/// Run the real CLI binary with `PINKDB_HOME` pointed at `home`, feeding it
/// `script` on stdin; returns stdout.
fn run_cli(home: &Path, script: &str) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.env("PINKDB_HOME", home);
    let (out, status) = common::run_merged(command, script);
    common::assert_script_exit(status);
    out
}

/// A loopback HTTP server serving `/index.json` and the artifact paths, with
/// request recording so tests can assert what was (not) fetched.
struct Fixture {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Fixture {
    fn start(catalog: String, artifacts: Vec<(String, Vec<u8>)>) -> Fixture {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        listener
            .set_nonblocking(true)
            .expect("nonblocking fixture server");
        let addr = listener.local_addr().expect("fixture address");
        // Keys are requested with a leading slash; accept either form.
        let artifacts: HashMap<String, Vec<u8>> = artifacts
            .into_iter()
            .map(|(path, bytes)| {
                let key = if path.starts_with('/') {
                    path
                } else {
                    format!("/{path}")
                };
                (key, bytes)
            })
            .collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (record, flag) = (Arc::clone(&requests), Arc::clone(&stop));
        let handle = thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => serve(stream, &catalog, &artifacts, &record),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Fixture {
            addr,
            requests,
            stop,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn artifact_requests(&self) -> usize {
        self.requests()
            .iter()
            .filter(|path| path.starts_with("/artifacts/"))
            .count()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve(
    mut stream: TcpStream,
    catalog: &str,
    artifacts: &HashMap<String, Vec<u8>>,
    requests: &Mutex<Vec<String>>,
) {
    // Accepted streams inherit the listener's nonblocking flag on some
    // platforms (macOS/BSD); the handler is strictly blocking.
    let _ = stream.set_nonblocking(false);
    let mut data = Vec::new();
    let mut buf = [0u8; 2048];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                data.extend_from_slice(&buf[..n]);
                if data.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }
    let text = String::from_utf8_lossy(&data);
    let path = text.split_whitespace().nth(1).unwrap_or("/").to_string();
    requests.lock().unwrap().push(path.clone());
    let (status, content_type, body): (&str, &str, &[u8]) = match path.as_str() {
        "/index.json" => ("200 OK", "application/json", catalog.as_bytes()),
        other => match artifacts.get(other) {
            Some(bytes) => ("200 OK", "application/octet-stream", bytes.as_slice()),
            None => ("404 Not Found", "text/plain", b"fixture: not found"),
        },
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

// ---------- catalog fixtures ----------

struct FixtureEntry {
    name: &'static str,
    version: &'static str,
    abi: u64,
    platform: String,
    file: String,
    sha256: String,
}

fn entry(
    name: &'static str,
    version: &'static str,
    abi: u64,
    platform: &str,
    file: &str,
    sha256: &str,
) -> FixtureEntry {
    FixtureEntry {
        name,
        version,
        abi,
        platform: platform.to_string(),
        file: file.to_string(),
        sha256: sha256.to_string(),
    }
}

fn catalog_document(entries: &[FixtureEntry]) -> String {
    let extensions: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            serde_json::json!({
                "name": e.name,
                "version": e.version,
                "abi": e.abi,
                "platform": e.platform,
                "file": e.file,
                "sha256": e.sha256,
                "description": "fixture artifact",
            })
        })
        .collect();
    serde_json::json!({
        "format_version": 1,
        "updated": "2026-10-14T00:00:00Z",
        "extensions": extensions,
    })
    .to_string()
}

/// The platform tag this test expects the client to derive, computed the same
/// way (independently) from the compile-time OS/arch.
fn host_platform() -> String {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    format!("{os}-{arch}")
}

fn artifact_file(version: &str, platform: &str) -> String {
    format!(
        "artifacts/repo_ext/{version}/{platform}/repo_ext{}",
        std::env::consts::DLL_SUFFIX
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Compile `tests/fixtures/repo_ext.c` once per test process with the system
/// `cc` (same pattern as the `pinkdb-ext-loader` tests) and return its path.
fn fixture_extension() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
            let source = manifest.join("tests/fixtures/repo_ext.c");
            let include = manifest.join("../pinkdb-ext-loader/include");
            let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
                .join(format!("pinkdb_cli_repo_fixture_{}", std::process::id()));
            fs::create_dir_all(&dir).expect("create fixture extension directory");
            let library = dir.join(format!("repo_ext{}", std::env::consts::DLL_SUFFIX));
            let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
            let out = Command::new(&cc)
                .arg("-std=c99")
                .arg("-O1")
                .arg("-fPIC")
                .arg("-shared")
                .arg("-I")
                .arg(&include)
                .arg("-o")
                .arg(&library)
                .arg(&source)
                .output()
                .unwrap_or_else(|e| panic!("could not run the C compiler `{cc}`: {e} (set CC?)"));
            assert!(
                out.status.success(),
                "compiling repo_ext.c failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            library
        })
        .clone()
}

// ---------- small assertions helpers ----------

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn assert_no_temp_files(root: &Path) {
    let mut files = Vec::new();
    walk_files(root, &mut files);
    let leftovers: Vec<&PathBuf> = files
        .iter()
        .filter(|p| p.to_string_lossy().contains(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp files left behind: {leftovers:?}"
    );
}

// ---------- tests ----------

#[test]
fn repository_config_management() {
    let home = TempHome::new("config");
    let script = format!(
        ".repositories list\n\
         .repositories add http://127.0.0.1:9/\n\
         .repositories add http://127.0.0.1:9\n\
         .repositories add not-a-url\n\
         .repositories list\n\
         .repositories remove http://127.0.0.1:9\n\
         .repositories remove {OFFICIAL}\n\
         .repositories list\n\
         .repositories remove {OFFICIAL}\n\
         .repositories add http://127.0.0.1:9/\n\
         .quit\n"
    );
    let out = run_cli(home.path(), &script);

    // File absent: the official repository is the default.
    assert!(out.contains(&format!("1. {OFFICIAL}")), "{out}");
    // Add: canonicalized (trailing slash stripped), duplicates refused.
    assert!(out.contains("Added repository http://127.0.0.1:9"), "{out}");
    assert!(out.contains("already configured"), "{out}");
    assert!(out.contains("must start with http:// or https://"), "{out}");
    // Order is preserved: the default stays first, the new one goes last.
    assert!(
        out.contains(&format!("1. {OFFICIAL}\n  2. http://127.0.0.1:9")),
        "{out}"
    );
    assert!(
        out.contains("Removed repository http://127.0.0.1:9"),
        "{out}"
    );
    // An empty list is allowed, with a note.
    assert!(
        out.contains("Note: no repositories are configured"),
        "{out}"
    );
    assert!(out.contains("(none configured"), "{out}");
    assert!(out.contains("is not configured"), "{out}");
    // The final state is just the re-added local repository — and the config
    // directory did not exist before the first write.
    let config: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(home.path().join("repositories.json")).unwrap())
            .unwrap();
    assert_eq!(
        config["repositories"],
        serde_json::json!(["http://127.0.0.1:9"])
    );
    assert_no_temp_files(home.path());
}

#[test]
fn install_end_to_end_from_fixture_server() {
    let home = TempHome::new("e2e");
    let bytes = fs::read(fixture_extension()).unwrap();
    let platform = host_platform();
    let file = artifact_file("0.1.0", &platform);
    let sha = sha256_hex(&bytes);
    let server = Fixture::start(
        catalog_document(&[entry("repo_ext", "0.1.0", 1, &platform, &file, &sha)]),
        vec![(file.clone(), bytes.clone())],
    );
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {base}\n.install repo_ext\nSELECT ext_repo_triple(CAST(14 AS BIGINT));\n.installed\n.quit\n",
            base = server.base_url()
        ),
    );

    assert!(
        out.contains(&format!("Installed repo_ext 0.1.0 ({platform})")),
        "{out}"
    );
    assert!(
        out.contains(&format!("repository: {}", server.base_url())),
        "{out}"
    );
    assert!(out.contains(&format!("sha256:     {sha}")), "{out}");
    assert!(out.contains("Extension repo_ext loaded."), "{out}");
    assert!(out.contains("│ 42"), "{out}");
    assert!(out.contains("1 row"), "{out}");
    // `.installed` lists the cached artifact with its path...
    let path = home
        .path()
        .join("extensions/repo_ext/0.1.0")
        .join(&platform)
        .join(format!("repo_ext{}", std::env::consts::DLL_SUFFIX));
    assert!(out.contains(path.to_str().unwrap()), "{out}");
    assert!(out.contains(&format!("repo_ext 0.1.0 {platform}")), "{out}");
    // ...and the bytes on disk are exactly the fixture's.
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(
        server.artifact_requests(),
        1,
        "exactly one artifact download"
    );
    assert_no_temp_files(home.path());
}

#[test]
fn resolution_follows_config_order_and_picks_the_highest_version() {
    let bytes = fs::read(fixture_extension()).unwrap();
    let platform = host_platform();
    let sha = sha256_hex(&bytes);
    let files: Vec<String> = ["0.1.0", "0.2.0", "0.9.9", "0.10.0"]
        .iter()
        .map(|v| artifact_file(v, &platform))
        .collect();
    let f1 = &files[0];
    let f2 = &files[1];
    let f9 = &files[2];
    let f10 = &files[3];
    let serve_all = || {
        files
            .iter()
            .map(|f| (f.clone(), bytes.clone()))
            .collect::<Vec<_>>()
    };

    let a = Fixture::start(
        catalog_document(&[
            entry("repo_ext", "0.1.0", 1, &platform, f1, &sha),
            entry("repo_ext", "0.2.0", 1, &platform, f2, &sha),
        ]),
        vec![(f1.clone(), bytes.clone()), (f2.clone(), bytes.clone())],
    );
    let b = Fixture::start(
        catalog_document(&[entry("repo_ext", "0.9.9", 1, &platform, f9, &sha)]),
        vec![(f9.clone(), bytes.clone())],
    );

    // First match wins: repository A (0.2.0) beats B's higher 0.9.9, and B is
    // never even consulted.
    let home = TempHome::new("order-first");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.repositories add {}\n.install repo_ext\n.quit\n",
            a.base_url(),
            b.base_url()
        ),
    );
    assert!(out.contains("Installed repo_ext 0.2.0"), "{out}");
    assert!(
        b.requests().is_empty(),
        "second repository was consulted: {:?}",
        b.requests()
    );

    // A repository without the extension falls through to the next one.
    let empty = Fixture::start(catalog_document(&[]), vec![]);
    let home = TempHome::new("order-fallthrough");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.repositories add {}\n.install repo_ext\n.quit\n",
            empty.base_url(),
            b.base_url()
        ),
    );
    assert!(out.contains("Installed repo_ext 0.9.9"), "{out}");

    // No version: the highest semver-ish version (0.10.0 > 0.2.0 numerically).
    let c = Fixture::start(
        catalog_document(&[
            entry("repo_ext", "0.2.0", 1, &platform, f2, &sha),
            entry("repo_ext", "0.10.0", 1, &platform, f10, &sha),
        ]),
        serve_all(),
    );
    let home = TempHome::new("order-highest");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.install repo_ext\n.quit\n",
            c.base_url()
        ),
    );
    assert!(out.contains("Installed repo_ext 0.10.0"), "{out}");

    // An exact version is honored.
    let home = TempHome::new("order-exact");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.install repo_ext@0.2.0\n.quit\n",
            c.base_url()
        ),
    );
    assert!(out.contains("Installed repo_ext 0.2.0"), "{out}");
}

#[test]
fn platform_abi_and_format_errors_are_clear() {
    let bytes = fs::read(fixture_extension()).unwrap();
    let platform = host_platform();
    let sha = sha256_hex(&bytes);
    let file = artifact_file("0.1.0", &platform);
    let other = if platform == "linux-x64" {
        "darwin-arm64"
    } else {
        "linux-x64"
    };

    // Platform mismatch: the error names both the wanted and the available one.
    let wrong_platform = Fixture::start(
        catalog_document(&[entry("repo_ext", "0.1.0", 1, other, &file, &sha)]),
        vec![(file.clone(), bytes.clone())],
    );
    let home = TempHome::new("err-platform");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.install repo_ext\n.quit\n",
            wrong_platform.base_url()
        ),
    );
    assert!(
        out.contains("was not found in any configured repository"),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "no artifact for \"repo_ext\" on platform {platform} with ABI 1"
        )),
        "{out}"
    );
    assert!(out.contains(other), "{out}");

    // ABI mismatch.
    let wrong_abi = Fixture::start(
        catalog_document(&[entry("repo_ext", "0.1.0", 2, &platform, &file, &sha)]),
        vec![(file.clone(), bytes.clone())],
    );
    let home = TempHome::new("err-abi");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.install repo_ext\n.quit\n",
            wrong_abi.base_url()
        ),
    );
    assert!(out.contains("with ABI 1 (available:"), "{out}");
    assert!(out.contains("(ABI 2)"), "{out}");

    // Unsupported repository format version.
    let bad_format = Fixture::start(
        r#"{"format_version": 2, "extensions": []}"#.to_string(),
        vec![],
    );
    let home = TempHome::new("err-format");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.install repo_ext\n.quit\n",
            bad_format.base_url()
        ),
    );
    assert!(
        out.contains("unsupported repository format version 2 (expected 1)"),
        "{out}"
    );

    // An unreachable repository is a clear fetch error, not a silent skip.
    let home = TempHome::new("err-unreachable");
    let out = run_cli(
        home.path(),
        ".repositories add http://127.0.0.1:1\n.install repo_ext\n.quit\n",
    );
    assert!(
        out.contains("GET http://127.0.0.1:1/index.json failed"),
        "{out}"
    );

    // Usage errors do not touch the network.
    let home = TempHome::new("err-usage");
    let out = run_cli(home.path(), ".install\n.install repo_ext 0.1.0\n.quit\n");
    assert_eq!(
        out.matches("Usage: .install <name[@version]>").count(),
        2,
        "{out}"
    );
}

#[test]
fn sha256_mismatch_refuses_the_artifact_and_leaves_nothing() {
    let bytes = fs::read(fixture_extension()).unwrap();
    let platform = host_platform();
    let file = artifact_file("0.1.0", &platform);
    let wrong = "0".repeat(64);
    let server = Fixture::start(
        catalog_document(&[entry("repo_ext", "0.1.0", 1, &platform, &file, &wrong)]),
        vec![(file.clone(), bytes.clone())],
    );
    let home = TempHome::new("sha");
    let out = run_cli(
        home.path(),
        &format!(
            ".repositories add {}\n.install repo_ext\n.quit\n",
            server.base_url()
        ),
    );
    assert!(out.contains("sha256 mismatch for repo_ext 0.1.0"), "{out}");
    assert!(out.contains("artifact discarded"), "{out}");
    assert!(out.contains(&wrong), "{out}");
    assert!(!out.contains("Extension repo_ext loaded."), "{out}");

    // No artifact was published and no temp file survived.
    let mut files = Vec::new();
    walk_files(&home.path().join("extensions"), &mut files);
    assert!(files.is_empty(), "files left behind: {files:?}");
    assert_no_temp_files(home.path());
}

#[test]
fn second_install_reuses_the_cached_copy() {
    let bytes = fs::read(fixture_extension()).unwrap();
    let platform = host_platform();
    let file = artifact_file("0.1.0", &platform);
    let sha = sha256_hex(&bytes);
    let server = Fixture::start(
        catalog_document(&[entry("repo_ext", "0.1.0", 1, &platform, &file, &sha)]),
        vec![(file.clone(), bytes.clone())],
    );
    let home = TempHome::new("cache");
    let script = format!(
        ".repositories add {base}\n.install repo_ext\n.quit\n",
        base = server.base_url()
    );
    let first = run_cli(home.path(), &script);
    assert!(first.contains("Extension repo_ext loaded."), "{first}");
    assert_eq!(server.artifact_requests(), 1);

    let second = run_cli(home.path(), &script);
    assert!(second.contains("cached:     yes"), "{second}");
    assert!(second.contains("Extension repo_ext loaded."), "{second}");
    assert_eq!(
        server.artifact_requests(),
        1,
        "the cached artifact must not be downloaded again"
    );
    assert_no_temp_files(home.path());
}

/// Opt-in live test against the official repository:
/// `PINKDB_REPO_LIVE=1 cargo test -p pinkdb-cli --features repo live_install`.
#[test]
fn live_install_math_ext_and_call_ext_clamp() {
    if std::env::var("PINKDB_REPO_LIVE").ok().as_deref() != Some("1") {
        eprintln!("skipping live repository test: set PINKDB_REPO_LIVE=1 to run it");
        return;
    }
    let platform = host_platform();
    let home = TempHome::new("live");
    // No repositories.json: the official repository is the default.
    let out = run_cli(
        home.path(),
        ".install math_ext\n\
         SELECT ext_clamp(CAST(42 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));\n\
         SELECT ext_clamp(CAST(-7 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));\n\
         SELECT ext_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));\n\
         .installed\n\
         .quit\n",
    );
    assert!(out.contains("Installed math_ext "), "{out}");
    assert!(out.contains(&format!("({platform})")), "{out}");
    assert!(out.contains("sha256:     "), "{out}");
    assert!(out.contains("Extension math_ext loaded."), "{out}");
    assert!(out.contains("│ 10"), "{out}");
    assert!(out.contains("│ 0"), "{out}");
    assert!(out.contains("│ 5"), "{out}");
    assert!(
        out.contains(&home.path().join("extensions").display().to_string()),
        "{out}"
    );
}
