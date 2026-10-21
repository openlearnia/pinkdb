//! The console's exposure policy: what it may bind to, and who may read it.
//!
//! Both checks **fail closed**: the console refuses to start rather than
//! serving a store it cannot protect.

use std::fmt;
use std::net::IpAddr;

/// How the console authenticates a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UiAuth {
    /// No authentication at all.
    ///
    /// Only valid while the store has **no users**: an empty RBAC catalog
    /// means every connection is trusted (the engine's documented
    /// empty-catalog policy), so the console needs no credential. As soon as
    /// the store has a user, the console refuses to serve (see
    /// [`check_auth_policy`]).
    None,
    /// HTTP Basic authentication against the store's own user catalog.
    ///
    /// The password is verified through the engine's verification path
    /// ([`pinkdb::Connection::login`]), so the console's statements run under
    /// that user's grants: RBAC applies to the console exactly as it applies
    /// to a wire client. A store with no users has nothing to authenticate
    /// against, so this mode requires at least one user.
    Basic,
    /// A static bearer token (`Authorization: Bearer <token>`).
    ///
    /// Compared in constant time. Independent of the store's user catalog:
    /// it is the operator's own console secret.
    Token(String),
}

/// Whether the console has a TLS transport of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsoleTls {
    /// No console TLS: the console speaks plaintext HTTP.
    ///
    /// This is wave 78's state — the console is a browser UI and cannot use
    /// the *protocol's* TLS (that transport speaks the binary pinkdb
    /// protocol, not HTTP).
    Absent,
    /// The console has its own TLS transport (a later wave).
    Configured,
}

/// Why the console refused to start (or to serve).
#[derive(Debug)]
pub enum UiError {
    /// The exposure policy refused the configuration.
    Policy(String),
    /// The listener could not be bound.
    Io(std::io::Error),
}

impl fmt::Display for UiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UiError::Policy(msg) => write!(f, "{msg}"),
            UiError::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for UiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UiError::Policy(_) => None,
            UiError::Io(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for UiError {
    fn from(err: std::io::Error) -> Self {
        UiError::Io(err)
    }
}

/// Is this address a loopback address (the only one the console may bind
/// without console TLS)?
pub fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}

/// The bind policy: a non-loopback bind needs console TLS.
///
/// The console is a *browser* UI: the server's TLS transport (wave 46)
/// speaks the binary pinkdb protocol and cannot carry HTTP, so it does not
/// protect the console. Until the console has its own TLS (and RBAC, wave
/// 48), a non-loopback bind is refused outright — the console is a
/// localhost tool, not a network service.
pub fn check_bind_policy(bind: IpAddr, tls: ConsoleTls) -> Result<(), UiError> {
    if is_loopback(bind) {
        return Ok(());
    }
    if tls == ConsoleTls::Configured {
        return Ok(());
    }
    Err(UiError::Policy(format!(
        "the warehouse console refuses to bind {bind}: it is not a loopback address. \
         The console is a browser UI and cannot use the server's TLS transport, so a \
         non-localhost bind needs console TLS and RBAC first. Bind 127.0.0.1 (the \
         default) instead."
    )))
}

/// The auth policy: a store with users is never served unauthenticated.
///
/// * No users in the store and no auth configured: allowed (the empty-catalog
///   policy) — the console prints a loud warning.
/// * Users in the store and no auth configured: **refused**.
/// * HTTP Basic configured: the store must have at least one user, otherwise
///   there is nothing to authenticate against.
/// * A bearer token: always allowed (it is the operator's own secret and
///   does not depend on the catalog).
pub fn check_auth_policy(has_users: bool, auth: &UiAuth) -> Result<(), UiError> {
    match auth {
        UiAuth::None => {
            if has_users {
                Err(UiError::Policy(
                    "the store has users, so the console must not be served unauthenticated. \
                     Start it with --ui-auth basic (HTTP Basic against the store's users) or \
                     with --ui-token <secret>."
                        .to_string(),
                ))
            } else {
                Ok(())
            }
        }
        UiAuth::Basic => {
            if has_users {
                Ok(())
            } else {
                Err(UiError::Policy(
                    "--ui-auth basic needs at least one user in the store to authenticate \
                     against (the store has none). Create a user first, or use \
                     --ui-token <secret> instead."
                        .to_string(),
                ))
            }
        }
        UiAuth::Token(_) => Ok(()),
    }
}
