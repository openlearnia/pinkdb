//! The console's exposure policy: what it may bind to, and who may read it.
//!
//! Both checks **fail closed**: the console refuses to start rather than
//! serving a store it cannot protect.

use std::fmt;
use std::net::IpAddr;

/// How the console authenticates a request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum UiAuth {
    /// Sign-in with the store's own users (the default, wave 121d).
    ///
    /// The password is verified through the engine's verification path
    /// ([`pinkdb::Connection::login`]), so the console's statements run under
    /// that user's grants: RBAC applies to the console exactly as it applies
    /// to a wire client. A user may add a TOTP second factor; scripts use
    /// personal access tokens. A store with no users starts in **first-run
    /// setup**: the first account is created with the setup code the server
    /// prints, and becomes an administrator.
    #[default]
    Password,
    /// No authentication at all (`--ui-auth none`), an explicit opt-in.
    ///
    /// Only valid while the store has **no users** and the console is on
    /// loopback without TLS: an empty RBAC catalog means every connection is
    /// trusted (the engine's documented empty-catalog policy). As soon as the
    /// store has a user, the console refuses to serve (see
    /// [`check_auth_policy`]).
    None,
}

/// Whether the console has a TLS transport of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsoleTls {
    /// No console TLS: the console speaks plaintext HTTP, so it stays on
    /// loopback.
    Absent,
    /// The console serves HTTPS (`--ui-tls-cert` / `--ui-tls-key`, wave
    /// 121d).
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

/// The bind policy: a non-loopback bind needs console TLS and sign-in.
///
/// Without TLS the console is a localhost tool: passwords, session cookies
/// and data would cross the network in clear text. With TLS it may listen on
/// any address — but never without sign-in.
pub fn check_bind_policy(bind: IpAddr, tls: ConsoleTls, auth: &UiAuth) -> Result<(), UiError> {
    if is_loopback(bind) {
        return Ok(());
    }
    if tls == ConsoleTls::Absent {
        return Err(UiError::Policy(format!(
            "the warehouse console refuses to bind {bind}: it is not a loopback address and \
             the console has no TLS. Give it a certificate (--ui-tls-cert and --ui-tls-key) \
             to serve HTTPS beyond localhost, or bind 127.0.0.1 (the default)."
        )));
    }
    if *auth == UiAuth::None {
        return Err(UiError::Policy(format!(
            "the warehouse console refuses to bind {bind} with --ui-auth none: a console \
             reachable from the network must ask for sign-in."
        )));
    }
    Ok(())
}

/// The auth policy: a store with users is never served unauthenticated.
///
/// * `--ui-auth none` on a store without users: allowed (the empty-catalog
///   policy) — the console prints a loud warning.
/// * `--ui-auth none` on a store with users: **refused**.
/// * Password sign-in (the default): always allowed; a store without users
///   starts in first-run setup.
pub fn check_auth_policy(has_users: bool, auth: &UiAuth) -> Result<(), UiError> {
    match auth {
        UiAuth::None if has_users => Err(UiError::Policy(
            "the store has users, so the console must not be served without sign-in: drop \
             --ui-auth none (password sign-in is the default)."
                .to_string(),
        )),
        UiAuth::None | UiAuth::Password => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_policies() {
        let lan: IpAddr = "192.168.1.5".parse().unwrap();
        let lo: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(check_bind_policy(lo, ConsoleTls::Absent, &UiAuth::None).is_ok());
        assert!(check_bind_policy(lan, ConsoleTls::Absent, &UiAuth::Password).is_err());
        assert!(check_bind_policy(lan, ConsoleTls::Configured, &UiAuth::Password).is_ok());
        assert!(check_bind_policy(lan, ConsoleTls::Configured, &UiAuth::None).is_err());
        assert!(check_auth_policy(true, &UiAuth::None).is_err());
        assert!(check_auth_policy(false, &UiAuth::None).is_ok());
        assert!(check_auth_policy(false, &UiAuth::Password).is_ok());
        assert_eq!(UiAuth::default(), UiAuth::Password);
    }
}
