//! The console's static assets, **embedded in the binary**.
//!
//! There is no CDN, no external font, no external script and no runtime
//! file dependency: the stylesheet and the script are compiled into the
//! binary with `include_str!` and served from `/assets/…`.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

/// The console's stylesheet (self-hosted; system fonts only).
pub const CONSOLE_CSS: &str = include_str!("assets/console.css");

/// The console's script (self-hosted; no framework, no external import).
pub const CONSOLE_JS: &str = include_str!("assets/console.js");

/// `GET /assets/console.css`
pub async fn css() -> Response {
    asset("text/css; charset=utf-8", CONSOLE_CSS)
}

/// `GET /assets/console.js`
pub async fn script() -> Response {
    asset("application/javascript; charset=utf-8", CONSOLE_JS)
}

/// Serve one embedded asset.
fn asset(content_type: &'static str, body: &'static str) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            // The assets never change while the server runs.
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}
