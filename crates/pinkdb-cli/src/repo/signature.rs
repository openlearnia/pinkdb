//! Detached Ed25519 signatures over the repository catalog (`index.json`).
//!
//! The contract is `docs/extension-signing.md`. `index.json.sig` is one ASCII
//! line:
//!
//! ```text
//! PINKDB-SIG1 <keyid> <base64 Ed25519 signature over the exact index.json bytes>
//! ```
//!
//! `keyid` is the first 16 lowercase hex characters of `SHA-256(SPKI DER)` of
//! the signing key. The repository's public key is pinned in the binary at
//! compile time (`ext-repo/keys/extension-signing.pub.pem`, embedded with
//! `include_str!`); `PINKDB_EXT_REPO_PUBKEY` replaces that pin for tests/dev
//! only and prints a warning.
//!
//! Verification is fail-closed: a missing, malformed, unknown-key or invalid
//! signature refuses the catalog before any artifact is requested. The only
//! way to load an unsigned catalog is the explicitly insecure
//! `PINKDB_EXT_ALLOW_UNSIGNED=1` development escape hatch.

use std::fs;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::{Signature, VerifyingKey};
use sha2::{Digest, Sha256};

/// Envelope prefix (format version 1).
pub const ENVELOPE_PREFIX: &str = "PINKDB-SIG1";
/// The signed catalog's detached signature file name.
pub const SIGNATURE_FILE: &str = "index.json.sig";
/// The pinned public key shipped in the repository tree, embedded verbatim.
pub const PINNED_PUBKEY_PEM: &str =
    include_str!("../../../../ext-repo/keys/extension-signing.pub.pem");
/// Test/dev override: PEM content or a path to a PEM file.
pub const PUBKEY_ENV: &str = "PINKDB_EXT_REPO_PUBKEY";
/// Explicit, insecure development escape hatch for unsigned catalogs.
pub const ALLOW_UNSIGNED_ENV: &str = "PINKDB_EXT_ALLOW_UNSIGNED";

/// Ed25519 SubjectPublicKeyInfo DER: `SEQUENCE { AlgorithmIdentifier, BIT STRING }`
/// with the raw 32-byte key in the BIT STRING.
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// A parsed `index.json.sig` envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// Lowercase 16-hex-character key id.
    pub key_id: String,
    /// Raw 64-byte Ed25519 signature.
    pub signature: [u8; 64],
}

impl Envelope {
    /// Parse and validate the one-line envelope.
    pub fn parse(bytes: &[u8]) -> Result<Envelope, String> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| "catalog signature is not valid UTF-8/ASCII".to_string())?;
        let mut lines = text.lines().filter(|line| !line.trim().is_empty());
        let line = lines
            .next()
            .ok_or_else(|| "malformed catalog signature: file is empty".to_string())?;
        if lines.next().is_some() {
            return Err("malformed catalog signature: expected exactly one line".to_string());
        }

        let mut fields = line.split_whitespace();
        let prefix = fields.next().unwrap_or_default();
        if prefix != ENVELOPE_PREFIX {
            return Err(format!(
                "malformed catalog signature: expected {ENVELOPE_PREFIX:?}, found {prefix:?}"
            ));
        }
        let key_id = fields
            .next()
            .ok_or_else(|| "malformed catalog signature: missing key id".to_string())?;
        let encoded = fields
            .next()
            .ok_or_else(|| "malformed catalog signature: missing signature bytes".to_string())?;
        if fields.next().is_some() {
            return Err("malformed catalog signature: trailing fields".to_string());
        }
        if key_id.len() != 16 || !key_id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!(
                "malformed catalog signature: key id {key_id:?} is not 16 hex characters"
            ));
        }
        let raw = STANDARD
            .decode(encoded)
            .map_err(|e| format!("malformed catalog signature: invalid base64: {e}"))?;
        let signature = <[u8; 64]>::try_from(raw.as_slice()).map_err(|_| {
            format!(
                "malformed catalog signature: expected 64 signature bytes, found {}",
                raw.len()
            )
        })?;
        Ok(Envelope {
            key_id: key_id.to_ascii_lowercase(),
            signature,
        })
    }
}

/// One trusted catalog-signing key.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicKey {
    /// First 16 lowercase hex characters of SHA-256(SPKI DER).
    pub key_id: String,
    /// Where the key came from, for diagnostics (`pinned` / env var name).
    pub label: String,
    verifying: VerifyingKey,
}

impl PublicKey {
    /// Parse an Ed25519 SubjectPublicKeyInfo DER blob.
    pub fn from_spki_der(der: &[u8], label: impl Into<String>) -> Result<PublicKey, String> {
        if der.len() != 44 || der[..12] != ED25519_SPKI_PREFIX {
            return Err(format!(
                "{}: not an Ed25519 SubjectPublicKeyInfo DER blob (expected 44 bytes, found {})",
                label.into(),
                der.len()
            ));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&der[12..]);
        let verifying = VerifyingKey::from_bytes(&bytes)
            .map_err(|e| format!("invalid Ed25519 public key: {e}"))?;
        let key_id = hex(&Sha256::digest(der)[..8]);
        Ok(PublicKey {
            key_id,
            label: label.into(),
            verifying,
        })
    }

    /// Parse an SPKI PEM (`-----BEGIN PUBLIC KEY-----`).
    pub fn from_pem(pem: &str, label: impl Into<String>) -> Result<PublicKey, String> {
        const BEGIN: &str = "-----BEGIN PUBLIC KEY-----";
        const END: &str = "-----END PUBLIC KEY-----";
        let label = label.into();
        let text = pem.trim();
        let body = text
            .strip_prefix(BEGIN)
            .and_then(|rest| rest.strip_suffix(END))
            .ok_or_else(|| format!("{label}: expected a PEM public key block"))?;
        let encoded: String = body.split_whitespace().collect();
        let der = STANDARD
            .decode(encoded)
            .map_err(|e| format!("{label}: invalid PEM base64: {e}"))?;
        PublicKey::from_spki_der(&der, label)
    }
}

/// The result of verifying one catalog fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trust {
    /// Signature verified against the named pinned key.
    Signed { key_id: String },
    /// `PINKDB_EXT_ALLOW_UNSIGNED=1` was set; nothing was authenticated.
    UnsignedDev,
}

/// A set of trusted public keys plus the (insecure) unsigned escape hatch.
#[derive(Debug, Clone)]
pub struct Verifier {
    keys: Vec<PublicKey>,
    allow_unsigned: bool,
}

impl Verifier {
    /// Build a verifier from explicit keys (tests, tools).
    pub fn new(keys: Vec<PublicKey>, allow_unsigned: bool) -> Verifier {
        Verifier {
            keys,
            allow_unsigned,
        }
    }

    /// Build the production verifier: the embedded pin, or the dev override.
    ///
    /// The override *replaces* the pin (so tests can exercise unknown-key and
    /// rotation scenarios); setting it is always loudly warned about.
    pub fn from_env() -> Result<Verifier, String> {
        let allow_unsigned = match std::env::var(ALLOW_UNSIGNED_ENV) {
            Ok(v) if v == "1" => {
                eprintln!(
                    "WARNING: {ALLOW_UNSIGNED_ENV}=1: catalog signatures are NOT verified. \
                     This loads unsigned native code and is INSECURE; use it only for local development."
                );
                true
            }
            _ => false,
        };

        let keys = match std::env::var(PUBKEY_ENV) {
            Ok(value) => {
                if value.trim().is_empty() {
                    return Err(format!("{PUBKEY_ENV} is set but empty"));
                }
                eprintln!(
                    "WARNING: {PUBKEY_ENV} is set: trusting the supplied catalog public key \
                     instead of the pinned key. This is INSECURE and intended for tests/dev only."
                );
                let pem = if value.contains("BEGIN PUBLIC KEY") {
                    value
                } else {
                    fs::read_to_string(&value).map_err(|e| {
                        format!("{PUBKEY_ENV}={value:?}: cannot read public key file: {e}")
                    })?
                };
                vec![PublicKey::from_pem(&pem, PUBKEY_ENV)?]
            }
            Err(std::env::VarError::NotPresent) => {
                vec![PublicKey::from_pem(PINNED_PUBKEY_PEM, "pinned")?]
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(format!("{PUBKEY_ENV} is not valid UTF-8"));
            }
        };
        Ok(Verifier::new(keys, allow_unsigned))
    }

    /// Verify the exact catalog bytes against a detached signature.
    ///
    /// `Envelope::None` is a missing `index.json.sig` (typically HTTP 404).
    pub fn verify(&self, catalog: &[u8], envelope: Option<&[u8]>) -> Result<Trust, String> {
        let Some(bytes) = envelope else {
            if self.allow_unsigned {
                eprintln!(
                    "WARNING: catalog is UNSIGNED and {ALLOW_UNSIGNED_ENV}=1: loading anyway \
                     (INSECURE, development only)."
                );
                return Ok(Trust::UnsignedDev);
            }
            return Err(format!(
                "catalog signature is missing ({SIGNATURE_FILE}): refusing an unsigned catalog \
                 before downloading any artifact; if you are developing offline, set \
                 {ALLOW_UNSIGNED_ENV}=1 (INSECURE)"
            ));
        };
        if self.allow_unsigned {
            eprintln!(
                "WARNING: catalog signature is NOT being checked ({ALLOW_UNSIGNED_ENV}=1); \
                 this is INSECURE and development-only."
            );
            return Ok(Trust::UnsignedDev);
        }

        let envelope = Envelope::parse(bytes)?;
        let key = self
            .keys
            .iter()
            .find(|key| key.key_id == envelope.key_id)
            .ok_or_else(|| {
                let pinned = self
                    .keys
                    .iter()
                    .map(|key| format!("{} ({})", key.key_id, key.label))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "catalog signature uses unknown key id {} (trusted: {pinned}); refusing the catalog",
                    envelope.key_id
                )
            })?;
        let signature = Signature::from_bytes(&envelope.signature);
        key.verifying
            .verify_strict(catalog, &signature)
            .map_err(|_| {
                format!(
                    "catalog signature verification failed for key {} ({})",
                    key.key_id, key.label
                )
            })?;
        Ok(Trust::Signed {
            key_id: key.key_id.clone(),
        })
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    const PINNED_KEY_ID: &str = "c84e54bfea6b2d09";

    fn signing_key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    fn spki_der(key: &SigningKey) -> Vec<u8> {
        let mut der = ED25519_SPKI_PREFIX.to_vec();
        der.extend_from_slice(key.verifying_key().as_bytes());
        der
    }

    fn public_key(byte: u8) -> PublicKey {
        PublicKey::from_spki_der(&spki_der(&signing_key(byte)), "test").unwrap()
    }

    fn envelope(key: &SigningKey, message: &[u8]) -> Vec<u8> {
        let signature = key.sign(message);
        let key_id = hex(&Sha256::digest(spki_der(key))[..8]);
        format!(
            "{ENVELOPE_PREFIX} {key_id} {}\n",
            STANDARD.encode(signature.to_bytes())
        )
        .into_bytes()
    }

    fn verifier(byte: u8) -> Verifier {
        Verifier::new(vec![public_key(byte)], false)
    }

    #[test]
    fn pinned_key_id_matches_the_documented_value() {
        let key = PublicKey::from_pem(PINNED_PUBKEY_PEM, "pinned").unwrap();
        assert_eq!(key.key_id, PINNED_KEY_ID);
        assert_eq!(key.key_id.len(), 16);
    }

    #[test]
    fn parses_and_verifies_a_valid_envelope() {
        let key = signing_key(7);
        let catalog = br#"{"format_version":1,"extensions":[]}"#;
        let sig = envelope(&key, catalog);
        let env = Envelope::parse(&sig).unwrap();
        assert_eq!(env.key_id, public_key(7).key_id);
        assert_eq!(
            verifier(7).verify(catalog, Some(&sig)).unwrap(),
            Trust::Signed {
                key_id: public_key(7).key_id
            }
        );
    }

    #[test]
    fn rejects_tampered_catalog_bytes() {
        let key = signing_key(7);
        let sig = envelope(&key, b"the catalog the publisher signed");
        let err = verifier(7)
            .verify(b"a different catalog", Some(&sig))
            .unwrap_err();
        assert!(
            err.contains("catalog signature verification failed"),
            "{err}"
        );
    }

    #[test]
    fn rejects_an_envelope_that_names_the_wrong_key() {
        let signer = signing_key(7);
        let other = public_key(8);
        let signature = signer.sign(b"catalog");
        let sig = format!(
            "{ENVELOPE_PREFIX} {} {}\n",
            other.key_id,
            STANDARD.encode(signature.to_bytes())
        );
        let err = Verifier::new(vec![public_key(7), other], false)
            .verify(b"catalog", Some(sig.as_bytes()))
            .unwrap_err();
        assert!(err.contains("verification failed"), "{err}");
    }

    #[test]
    fn rejects_a_missing_signature() {
        let err = verifier(7).verify(b"unsigned catalog", None).unwrap_err();
        assert!(err.contains("signature is missing"), "{err}");
        assert!(err.contains(ALLOW_UNSIGNED_ENV), "{err}");
        assert!(err.contains("refusing an unsigned catalog"), "{err}");
    }

    #[test]
    fn rejects_unknown_keys() {
        let sig = envelope(&signing_key(7), b"catalog");
        let err = verifier(8).verify(b"catalog", Some(&sig)).unwrap_err();
        assert!(err.contains("unknown key id"), "{err}");
        assert!(err.contains(&public_key(7).key_id), "{err}");
        assert!(err.contains(&public_key(8).key_id), "{err}");
    }

    #[test]
    fn rejects_malformed_base64_and_wrong_lengths() {
        let key = public_key(7);
        let cases = [
            format!("{ENVELOPE_PREFIX} {} !!!not-base64!!!\n", key.key_id),
            format!(
                "{ENVELOPE_PREFIX} {} {}\n",
                key.key_id,
                STANDARD.encode([0u8; 32])
            ),
        ];
        for sig in cases {
            let err = verifier(7)
                .verify(b"catalog", Some(sig.as_bytes()))
                .unwrap_err();
            assert!(
                err.contains("malformed catalog signature"),
                "{sig:?}: {err}"
            );
        }
    }

    #[test]
    fn rejects_malformed_envelopes() {
        let key = public_key(7);
        let valid = envelope(&signing_key(7), b"catalog");
        let valid_line = String::from_utf8(valid).unwrap();
        let cases = [
            String::new(),
            "PINKDB-SIG1\n".to_string(),
            "PINKDB-SIG1 nothex 0000\n".to_string(),
            format!("PINKDB-SIG1 {} \n", key.key_id),
            format!("PINKDB-SIG1 {} abc\n", key.key_id),
            format!("PINKDB-SIG2 {} abc\n", key.key_id),
            format!("{}extra\n", valid_line),
            format!("PINKDB-SIG1 {} {}\n", key.key_id, "A".repeat(88)),
            format!("{valid_line}{valid_line}"),
            "PINKDB-SIG1 0000000000000000 AAAA extra\n".to_string(),
        ];
        for sig in cases {
            let err = verifier(7)
                .verify(b"catalog", Some(sig.as_bytes()))
                .unwrap_err();
            assert!(
                err.contains("malformed catalog signature"),
                "{sig:?}: {err}"
            );
        }
    }

    #[test]
    fn unsigned_dev_escape_hatch_is_explicit() {
        let dev = Verifier::new(vec![public_key(7)], true);
        assert_eq!(dev.verify(b"unsigned", None).unwrap(), Trust::UnsignedDev);
        // With the escape hatch, even a present-but-invalid signature is not
        // authenticated; the caller must print/log the UnsignedDev trust.
        assert_eq!(
            dev.verify(b"unsigned", Some(b"garbage")).unwrap(),
            Trust::UnsignedDev
        );
    }

    #[test]
    fn pem_parsing_accepts_multiline_and_rejects_garbage() {
        let der = spki_der(&signing_key(7));
        let encoded = STANDARD.encode(&der);
        let wrapped = format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n{}\n-----END PUBLIC KEY-----\n",
            &encoded[..32],
            &encoded[32..]
        );
        assert_eq!(
            PublicKey::from_pem(&wrapped, "test").unwrap(),
            public_key(7)
        );
        for bad in [
            "",
            "not a key",
            "-----BEGIN PUBLIC KEY-----\n!!!!\n-----END PUBLIC KEY-----",
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----",
        ] {
            assert!(PublicKey::from_pem(bad, "test").is_err(), "{bad:?}");
        }
        assert!(PublicKey::from_spki_der(&[0u8; 12], "test").is_err());
    }
}
