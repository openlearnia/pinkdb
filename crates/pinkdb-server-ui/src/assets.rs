//! The console's static assets, **embedded in the binary**.
//!
//! There is no CDN, no external font, no external script and no runtime
//! file dependency. The browser app (wave 121) is Leptos compiled to wasm:
//! `scripts/build-console.sh` builds `crates/pinkdb-server-ui-web` and
//! commits its gzipped output to `web-dist/`, which is compiled in here and
//! served as is (`Content-Encoding: gzip`; every browser accepts it). The
//! stylesheet and the tiny boot module are plain files next to this one.

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

/// The console's stylesheet (self-hosted; system fonts only).
pub const CONSOLE_CSS: &str = include_str!("assets/console.css");

/// The module that loads the app.
pub const BOOT_JS: &str = include_str!("assets/boot.js");

/// The app's wasm-bindgen glue, gzipped.
pub const APP_JS_GZ: &[u8] = include_bytes!("../web-dist/pinkdb-console.js.gz");

/// The app, gzipped.
pub const APP_WASM_GZ: &[u8] = include_bytes!("../web-dist/pinkdb-console_bg.wasm.gz");

/// `GET /assets/console.css`
pub async fn css() -> Response {
    asset("text/css; charset=utf-8", CONSOLE_CSS)
}

/// `GET /assets/boot.js`
pub async fn boot() -> Response {
    asset("text/javascript; charset=utf-8", BOOT_JS)
}

/// `GET /assets/pinkdb-console.js`
pub async fn app_js(headers: HeaderMap) -> Response {
    gzipped(&headers, "text/javascript; charset=utf-8", APP_JS_GZ)
}

/// `GET /assets/pinkdb-console_bg.wasm`
pub async fn app_wasm(headers: HeaderMap) -> Response {
    gzipped(&headers, "application/wasm", APP_WASM_GZ)
}

/// Serve one embedded asset.
fn asset(content_type: &'static str, body: &'static str) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            // The assets never change while the server runs, but a new
            // server may serve new ones: revalidate.
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

/// Serve a gzipped asset. A client that says it cannot take gzip gets a
/// `406` rather than bytes it cannot read (browsers always accept it).
fn gzipped(headers: &HeaderMap, content_type: &'static str, body: &'static [u8]) -> Response {
    let accepts_gzip = headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.split(',').any(|e| e.trim().starts_with("gzip")));
    if !accepts_gzip {
        return crate::api::json_error(
            StatusCode::NOT_ACCEPTABLE,
            "the console's app is served gzip-encoded (Accept-Encoding: gzip)",
        );
    }
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_ENCODING, "gzip"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::VARY, "Accept-Encoding"),
        ],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// `scripts/console_hash.py`, in Rust: FNV-1a 64 over the app's sources.
    fn source_hash(root: &Path) -> String {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    out.push(path);
                }
            }
        }
        let mut files = Vec::new();
        for crate_dir in ["crates/pinkdb-server-ui-web", "crates/pinkdb-server-ui-api"] {
            for name in ["Cargo.toml", "Cargo.lock", "index.html"] {
                let path = root.join(crate_dir).join(name);
                if path.is_file() {
                    files.push(path);
                }
            }
            walk(&root.join(crate_dir).join("src"), &mut files);
        }
        let mut relative: Vec<(String, PathBuf)> = files
            .into_iter()
            .map(|p| {
                let rel = p
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                (rel, p)
            })
            .collect();
        relative.sort();
        let mut h: u64 = 0xcbf29ce484222325;
        let mut feed = |bytes: &[u8]| {
            for b in bytes {
                h ^= u64::from(*b);
                h = h.wrapping_mul(0x100000001b3);
            }
        };
        for (rel, path) in relative {
            feed(rel.as_bytes());
            feed(&[0]);
            feed(&std::fs::read(path).unwrap());
            feed(&[0]);
        }
        format!("{h:016x}")
    }

    /// The committed app was built from the current sources: run
    /// `scripts/build-console.sh` after changing the browser app or the
    /// shared API crate.
    #[test]
    fn web_dist_is_fresh() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = root.canonicalize().unwrap();
        let recorded = include_str!("../web-dist/SOURCE_HASH").trim();
        assert_eq!(
            source_hash(&root),
            recorded,
            "crates/pinkdb-server-ui/web-dist is stale: run scripts/build-console.sh"
        );
    }

    #[test]
    fn the_embedded_app_is_gzip() {
        assert_eq!(&super::APP_JS_GZ[..2], &[0x1f, 0x8b]);
        assert_eq!(&super::APP_WASM_GZ[..2], &[0x1f, 0x8b]);
    }
}
