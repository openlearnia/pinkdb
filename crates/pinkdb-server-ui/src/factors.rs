//! The console's credentials beyond the password (wave 121d): TOTP codes,
//! recovery codes, personal access tokens and the first-run setup code.
//!
//! * **TOTP** is RFC 6238 over HMAC-SHA1 (ring): 6 digits, 30-second steps,
//!   one step of clock drift either way. The caller remembers the last step
//!   it accepted, so a code is never accepted twice.
//! * **Recovery codes** and **access tokens** are random, shown once, and
//!   kept only as their SHA-256 (high-entropy secrets need no slow hash).
//! * The **setup code** guards the first-run account: it is printed in the
//!   server's log, so only someone who can read the log can claim the
//!   console.

use std::time::{SystemTime, UNIX_EPOCH};

use data_encoding::BASE32_NOPAD;
use ring::{digest, hmac};

use crate::auth::constant_time_eq;

/// A TOTP code's digits.
pub const TOTP_DIGITS: u32 = 6;

/// A TOTP step, in seconds.
pub const TOTP_STEP: u64 = 30;

/// Steps of clock drift accepted either way.
pub const TOTP_SKEW: u64 = 1;

/// How many recovery codes an enrolment hands out.
pub const RECOVERY_CODES: usize = 10;

/// The prefix of a personal access token (recognizable in a secret scanner
/// and in an `Authorization` header).
pub const TOKEN_PREFIX: &str = "pdbt_";

/// `n` random bytes from the OS.
fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::getrandom(&mut bytes).expect("the OS random source is unavailable");
    bytes
}

/// A new TOTP secret: 160 random bits, base32 (what authenticator apps
/// read).
pub fn new_totp_secret() -> String {
    BASE32_NOPAD.encode(&random_bytes::<20>())
}

/// The code of `secret` (base32) for `step`; `None` for a malformed secret.
pub fn totp_at(secret: &str, step: u64) -> Option<String> {
    let key = BASE32_NOPAD
        .decode(secret.trim_end_matches('=').to_ascii_uppercase().as_bytes())
        .ok()?;
    let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, &key);
    let tag = hmac::sign(&key, &step.to_be_bytes());
    let mac = tag.as_ref();
    // RFC 4226 dynamic truncation.
    let offset = usize::from(mac[mac.len() - 1] & 0x0f);
    let binary = (u32::from(mac[offset] & 0x7f) << 24)
        | (u32::from(mac[offset + 1]) << 16)
        | (u32::from(mac[offset + 2]) << 8)
        | u32::from(mac[offset + 3]);
    let code = binary % 10u32.pow(TOTP_DIGITS);
    Some(format!("{code:0width$}", width = TOTP_DIGITS as usize))
}

/// The TOTP step of a time.
pub fn step_of(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) / TOTP_STEP
}

/// The step a code matches, within the drift window around `now`, if any
/// and if it is later than `last_used` (a used step is never accepted
/// again).
pub fn totp_verify(secret: &str, code: &str, now: u64, last_used: Option<u64>) -> Option<u64> {
    let code: String = code.chars().filter(|c| !c.is_whitespace()).collect();
    if code.len() != TOTP_DIGITS as usize || !code.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut found = None;
    for step in now.saturating_sub(TOTP_SKEW)..=now + TOTP_SKEW {
        // Every candidate is computed and compared, so the time taken does
        // not say which step matched.
        if let Some(expected) = totp_at(secret, step)
            && constant_time_eq(expected.as_bytes(), code.as_bytes())
            && last_used.is_none_or(|used| step > used)
        {
            found = Some(step);
        }
    }
    found
}

/// The `otpauth://` URI an authenticator app reads (the QR code holds it).
pub fn otpauth_uri(issuer: &str, user: &str, secret: &str) -> String {
    let label = format!("{}:{}", uri_encode(issuer), uri_encode(user));
    format!(
        "otpauth://totp/{label}?secret={secret}&issuer={}&algorithm=SHA1&digits={TOTP_DIGITS}&period={TOTP_STEP}",
        uri_encode(issuer)
    )
}

fn uri_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// A QR code of `text` as one SVG path (`M x y h1 v1 h-1 z` per dark
/// module) on a `size` × `size` grid with a 4-module quiet zone, so the
/// browser draws it without any markup from the server.
pub fn qr_path(text: &str) -> Option<(String, usize)> {
    use qrcodegen::{QrCode, QrCodeEcc};
    let qr = QrCode::encode_text(text, QrCodeEcc::Medium).ok()?;
    let border = 4;
    let size = qr.size() as usize + 2 * border;
    let mut path = String::new();
    for y in 0..qr.size() {
        for x in 0..qr.size() {
            if qr.get_module(x, y) {
                path.push_str(&format!(
                    "M{} {}h1v1h-1z",
                    x as usize + border,
                    y as usize + border
                ));
            }
        }
    }
    Some((path, size))
}

/// The SHA-256 of a secret, hex (how tokens and recovery codes are kept).
pub fn secret_hash(secret: &str) -> String {
    let digest = digest::digest(&digest::SHA256, secret.as_bytes());
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// A new personal access token: `pdbt_` + 256 random bits (base64url).
pub fn new_token() -> String {
    format!("{TOKEN_PREFIX}{}", crate::security::random_token())
}

/// Is this the shape of a personal access token?
pub fn looks_like_token(text: &str) -> bool {
    text.strip_prefix(TOKEN_PREFIX).is_some_and(|rest| {
        rest.len() == 43
            && rest
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

/// The part of a token shown to recognize it later (`pdbt_AbCd…`).
pub fn token_hint(token: &str) -> String {
    token.chars().take(TOKEN_PREFIX.len() + 4).collect()
}

/// A short code a person types: `n` base32 characters in groups of four
/// (`ABCD-EFGH`).
fn grouped_code(chars: usize) -> String {
    let raw = BASE32_NOPAD.encode(&random_bytes::<16>());
    raw.chars()
        .take(chars)
        .collect::<Vec<_>>()
        .chunks(4)
        .map(|c| c.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join("-")
}

/// Ten new recovery codes (`ABCD-EFGH`, 40 bits each).
pub fn new_recovery_codes() -> Vec<String> {
    (0..RECOVERY_CODES).map(|_| grouped_code(8)).collect()
}

/// A new setup code (`ABCD-EFGH-IJKL`, 60 bits).
pub fn new_setup_code() -> String {
    grouped_code(12)
}

/// A typed code normalized for comparison: no dashes or spaces, upper case.
pub fn normalize_code(code: &str) -> String {
    code.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6238 appendix B, SHA-1 (the 20-byte secret "12345678901234567890"),
    /// cut to six digits.
    #[test]
    fn totp_matches_the_rfc_vectors() {
        let secret = BASE32_NOPAD.encode(b"12345678901234567890");
        for (time, code) in [
            (59u64, "287082"),
            (1_111_111_109, "081804"),
            (1_111_111_111, "050471"),
            (1_234_567_890, "005924"),
            (2_000_000_000, "279037"),
            (20_000_000_000, "353130"),
        ] {
            assert_eq!(totp_at(&secret, time / TOTP_STEP).unwrap(), code, "{time}");
        }
        assert!(totp_at("not base32!", 1).is_none());
    }

    #[test]
    fn a_code_is_accepted_once_and_within_the_window() {
        let secret = new_totp_secret();
        let now = 1_000_000;
        let code = totp_at(&secret, now).unwrap();
        assert_eq!(totp_verify(&secret, &code, now, None), Some(now));
        assert_eq!(
            totp_verify(&secret, &code, now, Some(now)),
            None,
            "replayed"
        );
        let late = totp_at(&secret, now - 1).unwrap();
        assert_eq!(totp_verify(&secret, &late, now, None), Some(now - 1));
        let stale = totp_at(&secret, now - 2).unwrap();
        assert_eq!(totp_verify(&secret, &stale, now, None), None);
        assert_eq!(totp_verify(&secret, "12345", now, None), None);
        assert_eq!(totp_verify(&secret, "abcdef", now, None), None);
        let spaced = format!("{} {}", &code[..3], &code[3..]);
        assert_eq!(totp_verify(&secret, &spaced, now, None), Some(now));
    }

    #[test]
    fn tokens_codes_and_uris() {
        let token = new_token();
        assert!(looks_like_token(&token), "{token}");
        assert!(!looks_like_token("pdbt_short"));
        assert!(!looks_like_token(&token.replace("pdbt_", "nope_")));
        assert_eq!(token_hint(&token).len(), 9);
        assert_eq!(secret_hash("abc").len(), 64);
        assert_ne!(secret_hash("abc"), secret_hash("abd"));
        let codes = new_recovery_codes();
        assert_eq!(codes.len(), RECOVERY_CODES);
        assert!(
            codes
                .iter()
                .all(|c| c.len() == 9 && c.as_bytes()[4] == b'-')
        );
        let setup = new_setup_code();
        assert_eq!(setup.len(), 14);
        assert_eq!(normalize_code(" abcd-efgh "), "ABCDEFGH");
        let uri = otpauth_uri("pinkdb", "ana smith", "JBSWY3DPEHPK3PXP");
        assert!(uri.starts_with("otpauth://totp/pinkdb:ana%20smith?secret=JBSWY3DPEHPK3PXP"));
        let (path, size) = qr_path(&uri).unwrap();
        assert!(size > 21 && path.starts_with('M'));
    }
}
