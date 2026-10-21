//! The console's authentication and its login throttling.
//!
//! The console reuses the **engine's** password verification path: a Basic
//! credential is checked with [`pinkdb::Connection::login`], i.e. the same
//! Argon2id verification (and the same "unknown user" dummy derivation) the
//! wire protocol uses. A verified session then runs under that user's
//! grants, so RBAC applies to the console exactly as it applies to a wire
//! client.
//!
//! The **throttling** mirrors the pass-5 policy of the wire server
//! (`crates/pinkdb-server/src/server.rs`): those helpers are crate-private
//! there, so wave 78 re-implements the same bounds here (same constants,
//! same semantics) instead of duplicating the server's module:
//!
//! * at most [`MAX_CONCURRENT_LOGINS`] Argon2id verifications run at once —
//!   an excess request is refused immediately instead of queueing (each
//!   derivation allocates the work factor's memory);
//! * a client address that accumulates [`IP_MAX_FAILURES`] failures inside
//!   [`IP_WINDOW`] is blocked for [`IP_BLOCK`], **before** any hashing;
//! * a blocked/expired entry is pruned on access and the map is bounded
//!   ([`IP_MAX_ENTRIES`]), so a rotating client population cannot grow it
//!   without bound;
//! * a successful authentication clears the address's failure history.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;

use crate::policy::UiAuth;

/// Global cap on concurrent Argon2id verifications (mirrors the wire
/// server's `DEFAULT_MAX_CONCURRENT_LOGINS`).
pub const MAX_CONCURRENT_LOGINS: usize = 4;

/// Failed authentications one client address may make inside [`IP_WINDOW`]
/// before it is blocked (mirrors the wire server's `LOGIN_IP_MAX_FAILURES`).
pub const IP_MAX_FAILURES: u32 = 20;

/// The window the per-address failure budget is counted in.
pub const IP_WINDOW: Duration = Duration::from_secs(60);

/// How long a blocked address stays blocked.
pub const IP_BLOCK: Duration = Duration::from_secs(60);

/// Bound on the per-address map (the oldest-observed entry is evicted when
/// the map is full and nothing has expired).
pub const IP_MAX_ENTRIES: usize = 4096;

/// The credentials a request carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Credentials {
    /// No `Authorization` header.
    None,
    /// HTTP Basic credentials.
    Basic { user: String, password: String },
    /// A bearer token.
    Token(String),
}

/// Why a request was not authenticated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthFailure {
    /// No (or malformed) credentials.
    Missing,
    /// The credentials were wrong.
    Rejected,
    /// The client address is blocked by the failed-login limiter.
    Blocked,
    /// Too many verifications are already running.
    Busy,
}

/// The console's authentication state.
pub struct AuthState {
    mode: UiAuth,
    gate: Arc<LoginGate>,
    ips: Mutex<HashMap<IpAddr, IpEntry>>,
}

impl AuthState {
    /// Build the state for the configured mode.
    pub fn new(mode: UiAuth) -> Self {
        Self {
            mode,
            gate: Arc::new(LoginGate::new(MAX_CONCURRENT_LOGINS)),
            ips: Mutex::new(HashMap::new()),
        }
    }

    /// The configured mode.
    pub fn mode(&self) -> &UiAuth {
        &self.mode
    }

    /// Parse the `Authorization` header into credentials.
    pub fn parse(header: Option<&str>) -> Credentials {
        let Some(value) = header else {
            return Credentials::None;
        };
        let value = value.trim();
        if let Some(rest) = strip_prefix_ci(value, "basic ") {
            return match decode_basic(rest.trim()) {
                Some((user, password)) => Credentials::Basic { user, password },
                None => Credentials::None,
            };
        }
        if let Some(rest) = strip_prefix_ci(value, "bearer ") {
            return Credentials::Token(rest.trim().to_string());
        }
        Credentials::None
    }

    /// Check a bearer token in constant time.
    pub fn token_matches(&self, token: &str) -> bool {
        match &self.mode {
            UiAuth::Token(expected) => constant_time_eq(expected.as_bytes(), token.as_bytes()),
            _ => false,
        }
    }

    /// Reserve a slot for one password verification (`None` when the global
    /// cap is reached).
    pub fn try_verify(&self) -> Option<LoginPermit> {
        self.gate.try_acquire()
    }

    /// Is this address blocked by the failed-login limiter?
    pub fn blocked_for(&self, ip: IpAddr) -> Option<Duration> {
        let now = Instant::now();
        let mut entries = self.lock();
        let entry = entries.get(&ip)?;
        match entry.blocked_until {
            Some(until) if until > now => Some(until - now),
            Some(_) => {
                entries.remove(&ip);
                None
            }
            None => {
                if now.duration_since(entry.window_start) > IP_WINDOW {
                    entries.remove(&ip);
                }
                None
            }
        }
    }

    /// Record one failed authentication for `ip`.
    pub fn record_failure(&self, ip: IpAddr) {
        let now = Instant::now();
        let mut entries = self.lock();
        if entries.len() >= IP_MAX_ENTRIES {
            // Expiry first: a blocked entry is live, a recent window is live.
            entries.retain(|_, entry| {
                let blocked = entry.blocked_until.is_some_and(|until| until > now);
                let recent = now.duration_since(entry.window_start) <= IP_WINDOW;
                blocked || recent
            });
            // Still full: evict the entry observed longest ago.
            if entries.len() >= IP_MAX_ENTRIES
                && let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_seen)
                    .map(|(ip, _)| *ip)
            {
                entries.remove(&oldest);
            }
        }
        let entry = entries.entry(ip).or_insert_with(|| IpEntry {
            window_start: now,
            last_seen: now,
            failures: 0,
            blocked_until: None,
        });
        if now.duration_since(entry.window_start) > IP_WINDOW {
            entry.window_start = now;
            entry.failures = 0;
        }
        entry.last_seen = now;
        entry.failures = entry.failures.saturating_add(1);
        if entry.failures >= IP_MAX_FAILURES {
            entry.blocked_until = Some(now + IP_BLOCK);
            entry.failures = 0;
        }
    }

    /// A successful authentication clears the address's failure history.
    pub fn record_success(&self, ip: IpAddr) {
        self.lock().remove(&ip);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<IpAddr, IpEntry>> {
        self.ips
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One client address's failure history.
struct IpEntry {
    window_start: Instant,
    last_seen: Instant,
    failures: u32,
    blocked_until: Option<Instant>,
}

/// A non-queueing cap on concurrent password verifications.
struct LoginGate {
    active: AtomicUsize,
    max: usize,
}

impl LoginGate {
    fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max: max.max(1),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<LoginPermit> {
        let mut current = self.active.load(Ordering::Acquire);
        loop {
            if current >= self.max {
                return None;
            }
            match self.active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(LoginPermit {
                        gate: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

/// Releases a verification slot when the verification (or its unwind) ends.
pub struct LoginPermit {
    gate: Arc<LoginGate>,
}

impl Drop for LoginPermit {
    fn drop(&mut self) {
        self.gate.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A counter used by the tests to observe the gate.
#[cfg(test)]
impl AuthState {
    fn in_flight(&self) -> usize {
        self.gate.active.load(Ordering::Acquire)
    }
}

/// Strip a case-insensitive prefix.
fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.len() < prefix.len() {
        return None;
    }
    let (head, tail) = value.split_at(prefix.len());
    if head.eq_ignore_ascii_case(prefix) {
        Some(tail)
    } else {
        None
    }
}

/// Decode an HTTP Basic credential (`base64(user:password)`).
fn decode_basic(encoded: &str) -> Option<(String, String)> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    // Only the first colon separates the user from the password (a password
    // may contain colons).
    let (user, password) = text.split_once(':')?;
    Some((user.to_string(), password.to_string()))
}

/// Compare two byte strings without leaking their contents through timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Fold the length difference into the accumulator so the comparison does
    // not return early on a length mismatch.
    let mut diff = (a.len() ^ b.len()) as u32;
    let len = a.len().min(b.len());
    for (x, y) in a.iter().zip(b.iter()).take(len) {
        diff |= u32::from(x ^ y);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_credentials() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:secret");
        let header = format!("Basic {encoded}");
        assert_eq!(
            AuthState::parse(Some(&header)),
            Credentials::Basic {
                user: "alice".to_string(),
                password: "secret".to_string()
            }
        );
    }

    #[test]
    fn parses_a_password_with_a_colon() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:a:b");
        let header = format!("Basic {encoded}");
        assert_eq!(
            AuthState::parse(Some(&header)),
            Credentials::Basic {
                user: "alice".to_string(),
                password: "a:b".to_string()
            }
        );
    }

    #[test]
    fn parses_bearer_tokens_and_rejects_junk() {
        assert_eq!(
            AuthState::parse(Some("Bearer s3cret")),
            Credentials::Token("s3cret".to_string())
        );
        assert_eq!(AuthState::parse(Some("Basic !!!")), Credentials::None);
        assert_eq!(AuthState::parse(None), Credentials::None);
        assert_eq!(AuthState::parse(Some("Digest x")), Credentials::None);
    }

    #[test]
    fn tokens_are_compared_in_constant_time() {
        let state = AuthState::new(UiAuth::Token("s3cret".to_string()));
        assert!(state.token_matches("s3cret"));
        assert!(!state.token_matches("s3cre"));
        assert!(!state.token_matches("s3cretx"));
        assert!(!state.token_matches(""));
    }

    #[test]
    fn the_gate_bounds_concurrent_verifications() {
        let state = AuthState::new(UiAuth::Basic);
        let first = state.try_verify().expect("first permit");
        let second = state.try_verify().expect("second permit");
        let third = state.try_verify().expect("third permit");
        let fourth = state.try_verify().expect("fourth permit");
        assert!(state.try_verify().is_none(), "the gate must be bounded");
        assert_eq!(state.in_flight(), MAX_CONCURRENT_LOGINS);
        drop(first);
        assert!(state.try_verify().is_some());
        drop((second, third, fourth));
    }

    #[test]
    fn repeated_failures_block_the_address() {
        let state = AuthState::new(UiAuth::Basic);
        let ip: IpAddr = "127.0.0.1".parse().expect("ip");
        for _ in 0..IP_MAX_FAILURES {
            state.record_failure(ip);
        }
        assert!(
            state.blocked_for(ip).is_some(),
            "the address must be blocked"
        );
    }
}
