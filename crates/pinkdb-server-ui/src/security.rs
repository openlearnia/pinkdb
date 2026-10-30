//! The console's browser-facing defenses (wave 121): response headers, the
//! `Host` check, the `Origin` check, cookies and random tokens.
//!
//! The console runs SQL now, so a page on another site must not be able to
//! drive it through the user's browser:
//!
//! * **CSRF.** Every state-changing request is a `POST` that needs the
//!   session cookie (`SameSite=Strict`, so another site's request does not
//!   carry it) *and* the session's CSRF token in the `X-CSRF-Token` header
//!   (which a cross-site form cannot set), and an `Origin`, when the browser
//!   sends one, equal to the console's own.
//! * **DNS rebinding.** A page whose name was rebound to 127.0.0.1 reaches
//!   the console with its *own* host name in `Host`; every request's `Host`
//!   must be a loopback name, the bind address or an operator-listed name.
//! * **Output.** A strict Content-Security-Policy (scripts and styles only
//!   from the console itself; no inline script, no `eval`), `nosniff`, no
//!   framing, no referrer.
//!
//! The app renders every value as a text node (Leptos never builds markup
//! from data), so a cell holding `<script>` stays text.

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine as _;
use std::sync::Arc;

use crate::api::json_error;
use crate::server::Shared;

/// The session cookie's name.
pub const SESSION_COOKIE: &str = "pinkdb_session";

/// The header that carries the session's CSRF token.
pub const CSRF_HEADER: &str = "x-csrf-token";

/// The console's Content-Security-Policy: everything from the console's own
/// origin, nothing inline, nothing framed. `'wasm-unsafe-eval'` lets the page
/// compile its own WebAssembly (the Leptos app, served from `'self'`); it
/// does not allow `eval` or `new Function`.
pub const CONTENT_SECURITY_POLICY: &str = "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; \
     style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; \
     form-action 'self'; frame-ancestors 'none'";

/// 256 random bits from the OS, base64url without padding (43 characters).
pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    // The OS random source does not fail on the platforms the console
    // supports; if it ever does, refusing to mint a token is the only safe
    // answer, and a panic on the blocking task becomes a 500.
    getrandom::getrandom(&mut bytes).expect("the OS random source is unavailable");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// The value of the session cookie, if the request carries one.
pub fn session_cookie(headers: &HeaderMap) -> Option<String> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(text) = value.to_str() else { continue };
        for pair in text.split(';') {
            if let Some((name, value)) = pair.trim().split_once('=')
                && name == SESSION_COOKIE
                && !value.is_empty()
            {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// The `Set-Cookie` value that installs a session.
///
/// `HttpOnly` (no script can read it), `SameSite=Strict` (no cross-site
/// request carries it), `Path=/`, and `Secure` when the console is served
/// over TLS. No `Max-Age`: a browser session cookie, while the server's idle
/// and absolute timeouts decide how long the session itself lives.
pub fn set_session_cookie(token: &str, secure: bool) -> String {
    format!(
        "{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/{}",
        if secure { "; Secure" } else { "" }
    )
}

/// The `Set-Cookie` value that removes the session cookie.
pub fn clear_session_cookie(secure: bool) -> String {
    format!(
        "{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{}",
        if secure { "; Secure" } else { "" }
    )
}

/// The host name of a `Host` header value, without its port, lowercased.
fn host_name(host: &str) -> String {
    let host = host.trim();
    let name = if let Some(rest) = host.strip_prefix('[') {
        // `[::1]:8080` → `::1`
        rest.split(']').next().unwrap_or_default()
    } else {
        host.rsplit_once(':').map_or(host, |(name, _)| name)
    };
    name.to_ascii_lowercase()
}

/// Is `host` (a `Host` header value) one the console answers to?
pub fn host_allowed(shared: &Shared, host: &str) -> bool {
    let name = host_name(host);
    if matches!(name.as_str(), "localhost" | "127.0.0.1" | "::1") {
        return true;
    }
    if name == shared.bind_ip().to_string() {
        return true;
    }
    shared
        .opts
        .allowed_hosts
        .iter()
        .any(|allowed| host_name(allowed) == name)
}

/// The `Origin` check for a state-changing request: absent (a non-browser
/// client) or the console's own origin, `http(s)://<Host>`.
pub fn origin_allowed(headers: &HeaderMap, secure: bool) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) else {
        return false;
    };
    let scheme = if secure { "https" } else { "http" };
    origin.eq_ignore_ascii_case(&format!("{scheme}://{host}"))
}

/// Middleware: refuse a request whose `Host` is not the console's.
pub async fn check_host(
    State(shared): State<Arc<Shared>>,
    request: Request,
    next: Next,
) -> Response {
    let allowed = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| host_allowed(&shared, host));
    if !allowed {
        return json_error(
            StatusCode::MISDIRECTED_REQUEST,
            "this console answers only to its own host name (a loopback name, its bind \
             address or a --ui-allowed-host); the request's Host header is not one of them",
        );
    }
    next.run(request).await
}

/// Middleware: the security headers on every response.
pub async fn security_headers(request: Request, next: Next) -> Response {
    let api = request.uri().path().starts_with("/api/");
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        "cross-origin-opener-policy",
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("same-origin"),
    );
    if api && !headers.contains_key(header::CACHE_CONTROL) {
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_names_drop_the_port() {
        assert_eq!(host_name("localhost:8080"), "localhost");
        assert_eq!(host_name("LOCALHOST"), "localhost");
        assert_eq!(host_name("[::1]:8080"), "::1");
        assert_eq!(host_name("127.0.0.1"), "127.0.0.1");
        assert_eq!(host_name("evil.example:80"), "evil.example");
    }

    #[test]
    fn tokens_are_long_and_distinct() {
        let a = random_token();
        let b = random_token();
        assert_eq!(a.len(), 43);
        assert_ne!(a, b);
        assert!(
            a.bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        );
    }

    #[test]
    fn the_cookie_is_read_among_others() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("a=1; pinkdb_session=tok; b=2"),
        );
        assert_eq!(session_cookie(&headers).as_deref(), Some("tok"));
        let cookie = set_session_cookie("tok", false);
        assert!(cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"));
        assert!(!cookie.contains("Secure"));
        assert!(set_session_cookie("tok", true).ends_with("; Secure"));
    }

    #[test]
    fn a_foreign_origin_is_refused() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:8080"));
        assert!(origin_allowed(&headers, false));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:8080"),
        );
        assert!(origin_allowed(&headers, false));
        assert!(!origin_allowed(&headers, true));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );
        assert!(!origin_allowed(&headers, false));
        headers.insert(header::ORIGIN, HeaderValue::from_static("null"));
        assert!(!origin_allowed(&headers, false));
    }
}
