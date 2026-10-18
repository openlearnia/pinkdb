//! At-rest encryption for a pink-lake (cargo feature `encryption`; no crypto
//! is linked without it).
//!
//! This module mirrors the engine's `pinkdb::crypto` approach — the same two
//! audited RustCrypto crates (AES-256-GCM, Argon2id), the same KDF defaults
//! and parameter caps, the same "wrong key vs tampering" key-check trick — but
//! the container is the lake's own: a lake **artifact** is wrapped, not an
//! engine `.pink` file. The magic is deliberately different (`PNKLAKE1` vs
//! the engine's `PINKENC1`) so the two layers can never be confused, and a
//! lake never stores a `PINKENC1` file.
//!
//! # Container format (`PNKLAKE1`, all integers little-endian)
//!
//! ```text
//! magic "PNKLAKE1" (8 bytes)
//! container version u32 (= 1)
//! cipher id u8 (1 = AES-256-GCM)
//! kdf id u8 (0 = raw 32-byte key, 1 = Argon2id)
//! domain u8 (1 = manifest, 2 = data file, 3 = commit delta, 4 = delete file)
//! key-check length u32 (= the key-check plaintext's length)
//! nonce prefix [8] (fresh random bytes *per container*)
//! binding length u32
//! binding bytes ("" for a manifest; the lake-relative path otherwise)
//! [kdf id == 1] m_cost u32 KiB | t_cost u32 | p_cost u32 | salt_len u32 | salt
//! key-check: AEAD of the fixed key-check plaintext, + 16 bytes
//! payload: frames of `plain_len u32 LE | ciphertext (plain_len + 16)`,
//!          each frame at most 1 MiB of plaintext; the last frame is a
//!          terminal zero-length frame (a container cut at a frame boundary
//!          is detected by its absence)
//! ```
//!
//! Every AEAD unit uses nonce `nonce_prefix || counter u32 LE`: counter 0 is
//! the key-check, then 1, 2, … per payload frame. The prefix is fresh random
//! bytes **per container** (never shared across the containers of one save),
//! so a (key, nonce) pair is never reused — not within one container, and not
//! across the data files, the manifest, or the delete files of a version. The
//! AAD binds the whole public header (magic, version, cipher, KDF id, domain,
//! binding, prefix, and the KDF parameters — all of it is `aad_base`) plus
//! the counter, so a container cannot be reinterpreted across domains,
//! paths, or parameters.
//!
//! The binding is the whole integrity story for *identity*: a data or delete
//! file's container authenticates the lake-relative path it was written for,
//! so swapping two files (even of the same shape) fails authentication
//! instead of silently loading different values. A **manifest** is bound to
//! nothing path-like on purpose: `catalog.json` and `snapshots/<v>.json` are
//! byte-identical by construction (the same sealed bytes are published to
//! both — see [`Crypto::seal`]), and a path binding would break that
//! invariant. Commit delta manifests (`commits/<v>.json`) are bound to their
//! own path (their audit value depends on not being interchangeable).
//!
//! # Key material and the KDF policy
//!
//! [`EncryptionKey`] is either a raw 32-byte key or a passphrase; passphrases
//! are stretched with Argon2id and the salt/parameters live in **every**
//! container header, so a container written with a different configuration
//! still opens. The salt and KDF parameters live in the *plaintext container
//! header* (not in a sidecar file): a container is self-describing, so any
//! single object — including a snapshot whose catalog is gone — opens with
//! just the user's key, and there is no second object to lose or keep in
//! sync. The header is public data by construction, and the AEAD binds it, so
//! tampering with the salt/parameters fails authentication instead of
//! silently changing the derived key. Argon2id is the one *expensive*
//! primitive here, so the policy is **one derivation per lake session**: a
//! [`Crypto`] session mints one write salt when it starts, every container of
//! that session records it, and the derived-key cache is keyed by
//! `(kdf, m, t, p, salt)` — so writing a whole version (N data files + delete
//! files + manifest) and reading a whole version (snapshot + N files) each
//! cost exactly one Argon2id run, whatever the file count.
//!
//! Key bytes are zeroed on drop on a best-effort basis (safe writes only — no
//! `unsafe`; a compiler may in principle elide the overwrite). No key material
//! is ever printed ([`EncryptionKey`]'s `Debug` is redacted).
//!
//! # Fail-closed reads
//!
//! With `encryption` compiled in: a keyed read of a plaintext artifact is
//! `NotEncrypted`, a keyless read of a container is `Encrypted`, a key-check
//! failure is `WrongKey`, and any payload/header authentication or truncation
//! problem is `Tampered` — never a partial read. Without the feature the
//! keyed constructors fail with a clear "not compiled in" error and
//! containers are recognized to report the build hint instead of a JSON
//! parse error.

// Without the `encryption` feature this module is mostly a stub: format
// constants and crypto internals are intentionally unreferenced.
#![cfg_attr(not(feature = "encryption"), allow(dead_code))]

use std::fmt;
use std::path::Path;

use crate::{LakeError, Result};

/// Container magic. Also what a keyless read recognizes to produce the
/// "encrypted; open it with a key" error.
pub(crate) const ENC_MAGIC: &[u8; 8] = b"PNKLAKE1";

/// The engine's container magic (`pinkdb::crypto`): recognized only to give a
/// precise error when one is mistaken for a lake artifact.
const ENGINE_ENC_MAGIC: &[u8; 8] = b"PINKENC1";

/// Container format version written and read by this build. A newer version
/// is refused with a clean message (the format stays additive-friendly).
pub(crate) const CONTAINER_VERSION: u32 = 1;
const CIPHER_AES256_GCM: u8 = 1;
const KDF_RAW: u8 = 0;
const KDF_ARGON2ID: u8 = 1;

const KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;
const NONCE_PREFIX_LEN: usize = 8;

/// Plaintext authenticated by the key-check block so a wrong key is reported
/// as `wrong key` instead of a parse error.
const KEY_CHECK_PLAIN: &[u8] = b"pinkdb lake encryption key-check/v1";

/// magic(8) + version(4) + cipher(1) + kdf(1) + domain(1) + key-check
/// length(4) + nonce prefix(8) + binding length(4).
const HEADER_FIXED_LEN: usize = 31;
const KDF_PARAMS_LEN: usize = 16; // m_cost, t_cost, p_cost, salt_len
const DEFAULT_SALT_LEN: u32 = 16;
const MIN_SALT_LEN: u32 = 8;
const MAX_SALT_LEN: u32 = 64;

/// Payloads are sealed in 1 MiB plaintext frames (the engine's checkpoint
/// segment size), so a whole file never has to be held twice in memory.
pub(crate) const SEGMENT_PLAIN_MAX: usize = 1 << 20;

/// Default Argon2id work factors (OWASP-recommended configuration): 19 MiB,
/// 2 iterations, 1 lane — the exact engine defaults, so a lake passphrase
/// costs the same as a `.pink` one. Stored in every header, so raising them
/// later keeps existing files readable.
const ARGON2_M_COST_KIB: u32 = 19 * 1024;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;
/// Upper bounds accepted from a file header: a crafted header must not be
/// able to make an open burn minutes of Argon2id time or allocate unbounded
/// memory (the same envelope the engine enforces).
const ARGON2_MAX_M_COST_KIB: u32 = 4 * ARGON2_M_COST_KIB;
const ARGON2_MAX_T_COST: u32 = 4 * ARGON2_T_COST;
const ARGON2_MAX_P_COST: u32 = 4 * ARGON2_P_COST;
const ARGON2_MAX_WORK_KIB: u64 = 8 * (ARGON2_M_COST_KIB as u64) * (ARGON2_T_COST as u64);

// The defaults must stay inside the accepted envelope (compile-time check).
const _: () = {
    assert!(ARGON2_M_COST_KIB <= ARGON2_MAX_M_COST_KIB);
    assert!(ARGON2_T_COST <= ARGON2_MAX_T_COST);
    assert!(ARGON2_P_COST <= ARGON2_MAX_P_COST);
    assert!((ARGON2_M_COST_KIB as u64) * (ARGON2_T_COST as u64) <= ARGON2_MAX_WORK_KIB);
};

/// Which kind of lake artifact a container holds; the AAD binds it, so a
/// manifest can never authenticate as a data file (or the reverse).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Domain {
    /// `catalog.json` and `snapshots/<v>.json` (bound to nothing path-like:
    /// the pair is the same bytes, see the module docs).
    Manifest,
    /// One `data/<table>/<uuid>.{pink,vortex}` file, bound to its path.
    DataFile,
    /// One `commits/<v>.json` delta manifest, bound to its path.
    Commit,
    /// One `data/<table>/<uuid>.pinkdel` delete file, bound to its path.
    DeleteFile,
}

impl Domain {
    fn id(self) -> u8 {
        match self {
            Domain::Manifest => 1,
            Domain::DataFile => 2,
            Domain::Commit => 3,
            Domain::DeleteFile => 4,
        }
    }

    fn name(id: u8) -> &'static str {
        match id {
            1 => "a manifest (catalog/snapshot)",
            2 => "a data file",
            3 => "a commit delta manifest",
            4 => "a delete file",
            _ => "an unknown artifact",
        }
    }
}

// ---------- error constructors ----------

/// A keyless read of an encrypted lake artifact.
pub(crate) fn encrypted_error(path: &Path) -> LakeError {
    #[cfg(feature = "encryption")]
    let hint = "open it with a key (the lake's *_with_key constructors, or PINKDB_KEY in the CLI)";
    #[cfg(not(feature = "encryption"))]
    let hint = "rebuild with the `encryption` feature to read it";
    LakeError::Encrypted {
        path: path.to_path_buf(),
        message: hint.to_string(),
    }
}

/// A keyed operation in a build without the `encryption` feature.
#[cfg(not(feature = "encryption"))]
pub(crate) fn unavailable() -> LakeError {
    LakeError::FeatureMissing {
        feature: "encryption",
        message: "at-rest encryption is not compiled into this pink-lake build; rebuild with \
                  the `encryption` feature (for the CLI: `cargo build -p pinkdb-cli \
                  --features vortexlake,s3,encryption`)"
            .to_string(),
    }
}

/// A keyed read of a plaintext lake artifact (refused so a key can never be
/// silently ignored).
pub(crate) fn not_encrypted_error(path: &Path) -> LakeError {
    LakeError::NotEncrypted {
        path: path.to_path_buf(),
        message: "the lake is not encrypted (a key was supplied); open it without a key"
            .to_string(),
    }
}

fn wrong_key(path: &Path, message: &str) -> LakeError {
    LakeError::WrongKey {
        path: path.to_path_buf(),
        message: message.to_string(),
    }
}

fn tampered(path: &Path, message: impl Into<String>) -> LakeError {
    LakeError::Tampered {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

/// Does `data` start with the lake container magic?
pub(crate) fn is_container(data: &[u8]) -> bool {
    data.starts_with(ENC_MAGIC)
}

// ---------- key material ----------

/// Key material for at-rest encryption: a raw 32-byte key or a passphrase
/// (mirrors the engine's `pinkdb::crypto::EncryptionKey`).
///
/// With the `encryption` feature off, the type still exists and keys can be
/// constructed, but every operation returns the "not compiled in" error —
/// callers get a clear failure instead of silently writing plaintext.
#[derive(Clone, Default)]
pub struct EncryptionKey {
    raw: Option<[u8; KEY_LEN]>,
    passphrase: Option<Vec<u8>>,
}

impl EncryptionKey {
    /// A raw 32-byte key (e.g. from a key manager or a file kept out of band).
    pub fn from_raw(bytes: [u8; 32]) -> Self {
        Self {
            raw: Some(bytes),
            passphrase: None,
        }
    }

    /// A passphrase, stretched with Argon2id when a container is written or
    /// read. The salt and parameters are stored in the container headers.
    pub fn from_passphrase(passphrase: &str) -> Self {
        Self {
            raw: None,
            passphrase: Some(passphrase.as_bytes().to_vec()),
        }
    }

    /// The CLI convention: a non-empty `PINKDB_KEY` environment variable is a
    /// passphrase. `None` when unset or empty (plaintext mode) — the same
    /// rule the engine's `crypto::EncryptionKey::from_env` and the CLI use. A
    /// set-but-non-UTF-8 value is a hard error: silently ignoring it would
    /// make a keyed invocation write plaintext.
    pub fn from_env() -> Result<Option<Self>> {
        match std::env::var_os("PINKDB_KEY") {
            None => Ok(None),
            Some(v) if v.is_empty() => Ok(None),
            Some(v) => match v.into_string() {
                Ok(k) => Ok(Some(Self::from_passphrase(&k))),
                Err(_) => Err(LakeError::BadKey {
                    message: "PINKDB_KEY is not valid UTF-8; set a UTF-8 passphrase or unset it"
                        .to_string(),
                }),
            },
        }
    }

    fn holds_material(&self) -> bool {
        self.raw.is_some() || self.passphrase.is_some()
    }
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.raw.is_some() {
            "raw"
        } else if self.passphrase.is_some() {
            "passphrase"
        } else {
            "empty"
        };
        // Never print key material.
        write!(f, "EncryptionKey({kind}, redacted)")
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        // Best-effort zeroization with safe writes only (no `unsafe`).
        if let Some(raw) = self.raw.as_mut() {
            for b in raw.iter_mut() {
                *b = 0;
            }
        }
        if let Some(pass) = self.passphrase.as_mut() {
            for b in pass.iter_mut() {
                *b = 0;
            }
        }
    }
}

pub(crate) use imp::Crypto;

#[cfg(feature = "encryption")]
mod imp {
    use super::*;

    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use aes_gcm::aead::rand_core::{OsRng, RngCore};
    use aes_gcm::aead::{Aead, KeyInit, Payload};
    use aes_gcm::{Aes256Gcm, Nonce};

    /// A derived 32-byte key, zeroed on drop (best-effort).
    struct DerivedKey {
        bytes: [u8; KEY_LEN],
    }

    impl Drop for DerivedKey {
        fn drop(&mut self) {
            for b in self.bytes.iter_mut() {
                *b = 0;
            }
        }
    }

    impl fmt::Debug for DerivedKey {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("DerivedKey(redacted)")
        }
    }

    /// The KDF identity of one key: either the raw key, or the exact Argon2id
    /// parameters and salt a container was written with. The session cache is
    /// keyed by it, so a whole version costs one derivation.
    #[derive(Clone, PartialEq, Eq, Hash)]
    enum Derivation {
        Raw,
        Argon2 {
            m_cost: u32,
            t_cost: u32,
            p_cost: u32,
            salt: Vec<u8>,
        },
    }

    impl fmt::Debug for Derivation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Derivation::Raw => write!(f, "Derivation(raw key)"),
                Derivation::Argon2 {
                    m_cost,
                    t_cost,
                    p_cost,
                    ..
                } => write!(
                    f,
                    "Derivation(Argon2id m={m_cost} KiB t={t_cost} p={p_cost}, salt redacted)"
                ),
            }
        }
    }

    /// One lake session's key: the user's key material, the derivation every
    /// *container this session writes* records, and the derived-key cache, so
    /// Argon2id runs once per distinct salt, not once per file.
    pub(crate) struct Crypto {
        key: EncryptionKey,
        /// What this session seals with: the raw key, or Argon2id with one
        /// freshly minted salt — the session's one KDF specification. Every
        /// container of the session carries it (so any single object opens
        /// even if the catalog is gone).
        write: Derivation,
        cache: Mutex<HashMap<Derivation, Arc<DerivedKey>>>,
    }

    impl fmt::Debug for Crypto {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Crypto({:?}, {:?})", self.key, self.write)
        }
    }

    fn random_prefix() -> [u8; NONCE_PREFIX_LEN] {
        let mut prefix = [0u8; NONCE_PREFIX_LEN];
        OsRng.fill_bytes(&mut prefix);
        prefix
    }

    fn nonce_for(prefix: &[u8; NONCE_PREFIX_LEN], counter: u32) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..NONCE_PREFIX_LEN].copy_from_slice(prefix);
        nonce[NONCE_PREFIX_LEN..].copy_from_slice(&counter.to_le_bytes());
        nonce
    }

    /// The AAD of one unit: the whole public header (which already binds the
    /// domain and the binding) plus the counter.
    fn unit_aad(aad_base: &[u8], counter: u32) -> Vec<u8> {
        let mut v = Vec::with_capacity(aad_base.len() + 4);
        v.extend_from_slice(aad_base);
        v.extend_from_slice(&counter.to_le_bytes());
        v
    }

    fn seal_unit(
        key: &DerivedKey,
        prefix: &[u8; NONCE_PREFIX_LEN],
        counter: u32,
        aad: &[u8],
        plain: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(&key.bytes)
            .map_err(|_| internal("bad key length for AES-256-GCM"))?;
        cipher
            .encrypt(
                Nonce::from_slice(&nonce_for(prefix, counter)),
                Payload { msg: plain, aad },
            )
            .map_err(|_| internal("AEAD encryption failed"))
    }

    /// `None` = authentication failed (wrong key or tampering).
    fn try_open_unit(
        key: &DerivedKey,
        prefix: &[u8; NONCE_PREFIX_LEN],
        counter: u32,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Option<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(&key.bytes).ok()?;
        cipher
            .decrypt(
                Nonce::from_slice(&nonce_for(prefix, counter)),
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .ok()
    }

    fn internal(message: impl Into<String>) -> LakeError {
        LakeError::Internal {
            message: format!("pink-lake encryption: {}", message.into()),
        }
    }

    fn argon2_derive(
        passphrase: &[u8],
        m_cost: u32,
        t_cost: u32,
        p_cost: u32,
        salt: &[u8],
    ) -> Result<DerivedKey> {
        let params = argon2::Params::new(m_cost, t_cost, p_cost, Some(KEY_LEN))
            .map_err(|e| internal(format!("invalid Argon2id parameters: {e}")))?;
        let argon =
            argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let mut bytes = [0u8; KEY_LEN];
        argon
            .hash_password_into(passphrase, salt, &mut bytes)
            .map_err(|e| internal(format!("Argon2id derivation failed: {e}")))?;
        Ok(DerivedKey { bytes })
    }

    /// Serialize the public header up to (but excluding) the key-check block.
    /// This is also the AAD base of every unit, so a change to any field —
    /// including the container version, the nonce prefix, the domain, the
    /// binding, and the KDF parameters — breaks every unit instead of being a
    /// cheap header edit.
    fn header_base(
        derivation: &Derivation,
        prefix: &[u8; NONCE_PREFIX_LEN],
        domain: Domain,
        binding: &str,
    ) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEADER_FIXED_LEN + KDF_PARAMS_LEN + MAX_SALT_LEN as usize);
        v.extend_from_slice(ENC_MAGIC);
        v.extend_from_slice(&CONTAINER_VERSION.to_le_bytes());
        v.push(CIPHER_AES256_GCM);
        v.push(match derivation {
            Derivation::Raw => KDF_RAW,
            Derivation::Argon2 { .. } => KDF_ARGON2ID,
        });
        v.push(domain.id());
        v.extend_from_slice(&(KEY_CHECK_PLAIN.len() as u32).to_le_bytes());
        v.extend_from_slice(prefix);
        v.extend_from_slice(&(binding.len() as u32).to_le_bytes());
        v.extend_from_slice(binding.as_bytes());
        if let Derivation::Argon2 {
            m_cost,
            t_cost,
            p_cost,
            salt,
        } = derivation
        {
            v.extend_from_slice(&m_cost.to_le_bytes());
            v.extend_from_slice(&t_cost.to_le_bytes());
            v.extend_from_slice(&p_cost.to_le_bytes());
            v.extend_from_slice(&(salt.len() as u32).to_le_bytes());
            v.extend_from_slice(salt);
        }
        v
    }

    /// The public part of a parsed container header.
    struct ParsedHeader {
        domain: u8,
        binding: String,
        derivation: Derivation,
        prefix: [u8; NONCE_PREFIX_LEN],
        aad_base: Vec<u8>,
        key_check: Vec<u8>,
        /// Offset where the payload frames begin (also the header length).
        payload_start: usize,
    }

    /// Structural header parse (no crypto). Every structural problem is a
    /// clear [`LakeError::Tampered`] except an unsupported container *version*,
    /// which is [`crate::LakeError::UnsupportedContainer`] (written by a newer
    /// pinkdb, not corruption): the header of a lake container is authenticated
    /// by the key-check, so a header that does not even parse is a damaged or
    /// foreign file, never a wrong-key case.
    fn parse_header(path: &Path, data: &[u8]) -> Result<ParsedHeader> {
        if !is_container(data) {
            return Err(tampered(
                path,
                "not a pink-lake encrypted container (bad magic)",
            ));
        }
        if data.len() < HEADER_FIXED_LEN {
            return Err(tampered(path, "truncated container header"));
        }
        let version = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
        if version != CONTAINER_VERSION {
            return Err(crate::LakeError::UnsupportedContainer {
                path: path.to_path_buf(),
                found: version,
            });
        }
        let cipher = data[12];
        if cipher != CIPHER_AES256_GCM {
            return Err(tampered(path, format!("unsupported cipher id {cipher}")));
        }
        let kdf = data[13];
        let domain = data[14];
        if !(1..=4).contains(&domain) {
            return Err(tampered(path, format!("unknown container domain {domain}")));
        }
        let key_check_len = u32::from_le_bytes(data[15..19].try_into().expect("4 bytes")) as usize;
        if key_check_len != KEY_CHECK_PLAIN.len() {
            return Err(tampered(
                path,
                format!("bad key-check length {key_check_len}"),
            ));
        }
        let mut prefix = [0u8; NONCE_PREFIX_LEN];
        prefix.copy_from_slice(&data[19..27]);
        let binding_len = u32::from_le_bytes(data[27..31].try_into().expect("4 bytes")) as usize;
        let mut pos = HEADER_FIXED_LEN;
        // Checked: on a 32-bit target `pos + binding_len` can wrap for a crafted
        // (up to u32::MAX) length, letting the slice below panic.
        let Some(binding_end) = pos
            .checked_add(binding_len)
            .filter(|end| *end <= data.len())
        else {
            return Err(tampered(path, "truncated binding"));
        };
        let binding = std::str::from_utf8(&data[pos..binding_end])
            .map_err(|_| tampered(path, "binding is not valid UTF-8"))?
            .to_string();
        pos = binding_end;
        let derivation = match kdf {
            KDF_RAW => Derivation::Raw,
            KDF_ARGON2ID => {
                if data.len() < pos + KDF_PARAMS_LEN {
                    return Err(tampered(path, "truncated Argon2id parameters"));
                }
                let u32_at = |off: usize| {
                    u32::from_le_bytes(data[off..off + 4].try_into().expect("4 bytes"))
                };
                let m_cost = u32_at(pos);
                let t_cost = u32_at(pos + 4);
                let p_cost = u32_at(pos + 8);
                let salt_len = u32_at(pos + 12);
                if !(8..=ARGON2_MAX_M_COST_KIB).contains(&m_cost)
                    || !(1..=ARGON2_MAX_T_COST).contains(&t_cost)
                    || !(1..=ARGON2_MAX_P_COST).contains(&p_cost)
                    || m_cost < 8 * p_cost
                    || (m_cost as u64) * (t_cost as u64) > ARGON2_MAX_WORK_KIB
                {
                    return Err(tampered(
                        path,
                        format!(
                            "implausible Argon2id parameters (m={m_cost} KiB, t={t_cost}, \
                             p={p_cost}; caps m<={ARGON2_MAX_M_COST_KIB}, \
                             t<={ARGON2_MAX_T_COST}, p<={ARGON2_MAX_P_COST}, \
                             m*t<={ARGON2_MAX_WORK_KIB})"
                        ),
                    ));
                }
                if !(MIN_SALT_LEN..=MAX_SALT_LEN).contains(&salt_len) {
                    return Err(tampered(path, format!("bad salt length {salt_len}")));
                }
                let salt_len = salt_len as usize;
                let salt_start = pos + KDF_PARAMS_LEN;
                if data.len() < salt_start + salt_len {
                    return Err(tampered(path, "truncated salt"));
                }
                let salt = data[salt_start..salt_start + salt_len].to_vec();
                pos = salt_start + salt_len;
                Derivation::Argon2 {
                    m_cost,
                    t_cost,
                    p_cost,
                    salt,
                }
            }
            other => return Err(tampered(path, format!("unsupported KDF id {other}"))),
        };
        if data.len() < pos + key_check_len + TAG_LEN {
            return Err(tampered(path, "truncated key-check block"));
        }
        let key_check = data[pos..pos + key_check_len + TAG_LEN].to_vec();
        let payload_start = pos + key_check_len + TAG_LEN;
        Ok(ParsedHeader {
            domain,
            binding,
            derivation,
            prefix,
            aad_base: data[..pos].to_vec(),
            key_check,
            payload_start,
        })
    }

    impl Crypto {
        /// One keyed lake session: the key plus the session's KDF
        /// specification. For a passphrase the write salt is minted here (and
        /// recorded in every container this session writes); a raw key needs
        /// no salt.
        pub(crate) fn new(key: EncryptionKey) -> Result<Self> {
            if !key.holds_material() {
                return Err(internal("encryption key holds no material"));
            }
            let write = match (&key.raw, &key.passphrase) {
                (Some(_), _) => Derivation::Raw,
                (None, Some(_)) => {
                    let mut salt = vec![0u8; DEFAULT_SALT_LEN as usize];
                    OsRng.fill_bytes(&mut salt);
                    Derivation::Argon2 {
                        m_cost: ARGON2_M_COST_KIB,
                        t_cost: ARGON2_T_COST,
                        p_cost: ARGON2_P_COST,
                        salt,
                    }
                }
                (None, None) => return Err(internal("encryption key holds no material")),
            };
            Ok(Self {
                key,
                write,
                cache: Mutex::new(HashMap::new()),
            })
        }

        /// Derive (or fetch from the session cache) the key of one header.
        fn derive(&self, path: &Path, derivation: &Derivation) -> Result<Arc<DerivedKey>> {
            if let Some(hit) = self
                .cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(derivation)
            {
                return Ok(Arc::clone(hit));
            }
            let derived = Arc::new(match derivation {
                Derivation::Raw => match self.key.raw {
                    Some(raw) => DerivedKey { bytes: raw },
                    None => {
                        return Err(wrong_key(
                            path,
                            "the file uses a raw 32-byte key, but a passphrase was supplied",
                        ))
                    }
                },
                Derivation::Argon2 {
                    m_cost,
                    t_cost,
                    p_cost,
                    salt,
                } => match &self.key.passphrase {
                    Some(pass) => argon2_derive(pass, *m_cost, *t_cost, *p_cost, salt)?,
                    None => {
                        return Err(wrong_key(
                            path,
                            "the file is passphrase-encrypted (Argon2id), but a raw key was \
                             supplied",
                        ))
                    }
                },
            });
            self.cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(derivation.clone(), Arc::clone(&derived));
            Ok(derived)
        }

        /// Seal one whole artifact into a container. Every call mints a fresh
        /// random nonce prefix (so no two containers of a session ever reuse
        /// a (key, nonce) pair), while the derivation — and therefore the
        /// Argon2id cost — is the session's one specification.
        ///
        /// Because the prefix is per container, sealing the same plaintext
        /// twice produces two different containers; the
        /// `catalog.json == snapshots/<v>.json` invariant is preserved by
        /// sealing **once per version** and publishing those exact bytes to
        /// both objects (the storage decorator's seal-manifest path), never
        /// by sealing twice and hoping for determinism.
        pub(crate) fn seal(
            &self,
            path: &Path,
            domain: Domain,
            binding: &str,
            plain: &[u8],
        ) -> Result<Vec<u8>> {
            let key = self.derive(path, &self.write)?;
            let prefix = random_prefix();
            let aad = header_base(&self.write, &prefix, domain, binding);
            let mut out = aad.clone();
            out.extend_from_slice(&seal_unit(
                &key,
                &prefix,
                0,
                &unit_aad(&aad, 0),
                KEY_CHECK_PLAIN,
            )?);
            let mut counter: u32 = 1;
            let mut pos = 0usize;
            while pos < plain.len() {
                let end = (pos + SEGMENT_PLAIN_MAX).min(plain.len());
                if counter == u32::MAX {
                    return Err(internal("too many container frames"));
                }
                let frame = &plain[pos..end];
                let ct = seal_unit(&key, &prefix, counter, &unit_aad(&aad, counter), frame)?;
                out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                out.extend_from_slice(&ct);
                counter += 1;
                pos = end;
            }
            if counter == u32::MAX {
                return Err(internal("too many container frames"));
            }
            let terminal = seal_unit(&key, &prefix, counter, &unit_aad(&aad, counter), &[])?;
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&terminal);
            Ok(out)
        }

        /// A writer that seals what is written through it into `sink` (used
        /// by the Vortex data-file path so a big file is never materialized
        /// whole). Call [`SealWriter::finish`] when the plaintext is done.
        #[cfg_attr(not(feature = "vortexlake"), allow(dead_code))]
        pub(crate) fn sealing_writer<'a>(
            &'a self,
            path: &Path,
            domain: Domain,
            binding: &str,
            sink: &'a mut (dyn Write + Send),
        ) -> Result<SealWriter<'a>> {
            let key = self.derive(path, &self.write)?;
            let prefix = random_prefix();
            let aad = header_base(&self.write, &prefix, domain, binding);
            let mut header = aad.clone();
            header.extend_from_slice(&seal_unit(
                &key,
                &prefix,
                0,
                &unit_aad(&aad, 0),
                KEY_CHECK_PLAIN,
            )?);
            sink.write_all(&header).map_err(|e| LakeError::Io {
                context: format!(
                    "writing the encrypted container header of {}",
                    path.display()
                ),
                source: e,
            })?;
            Ok(SealWriter {
                path: path.to_path_buf(),
                key,
                prefix,
                aad,
                sink,
                buf: Vec::with_capacity(SEGMENT_PLAIN_MAX),
                counter: 1,
                finished: false,
            })
        }

        /// Verify the key-check and open a whole container. All of the
        /// failure modes are authenticated: see the module docs.
        pub(crate) fn open(
            &self,
            path: &Path,
            domain: Domain,
            binding: &str,
            data: &[u8],
        ) -> Result<Vec<u8>> {
            if data.starts_with(ENGINE_ENC_MAGIC) {
                return Err(tampered(
                    path,
                    "this is an engine `.pink` encryption container (PINKENC1), not a \
                     pink-lake container (PNKLAKE1); a lake never stores the engine's \
                     containers",
                ));
            }
            if !is_container(data) {
                return Err(not_encrypted_error(path));
            }
            let header = parse_header(path, data)?;
            if header.domain != domain.id() {
                return Err(tampered(
                    path,
                    format!(
                        "this container holds {}, not {}",
                        Domain::name(header.domain),
                        Domain::name(domain.id())
                    ),
                ));
            }
            if header.binding != binding {
                return Err(tampered(
                    path,
                    format!(
                        "this container is bound to {:?}, not to {:?}; a bound artifact is \
                         not interchangeable",
                        header.binding, binding
                    ),
                ));
            }
            let key = self.derive(path, &header.derivation)?;
            if try_open_unit(
                &key,
                &header.prefix,
                0,
                &unit_aad(&header.aad_base, 0),
                &header.key_check,
            )
            .is_none()
            {
                return Err(wrong_key(
                    path,
                    "the encryption key-check block did not authenticate (wrong key, wrong \
                     key type, or a tampered header)",
                ));
            }
            let mut out = Vec::new();
            let mut pos = header.payload_start;
            let mut counter: u32 = 1;
            while pos < data.len() {
                if data.len() - pos < 4 {
                    return Err(tampered(path, "truncated frame length"));
                }
                let n =
                    u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
                if n > SEGMENT_PLAIN_MAX {
                    return Err(tampered(
                        path,
                        format!(
                            "a frame declares {n} plaintext bytes, over the {SEGMENT_PLAIN_MAX}-byte \
                             frame size"
                        ),
                    ));
                }
                let start = pos + 4;
                let Some(end) = start.checked_add(n + TAG_LEN) else {
                    return Err(tampered(path, "frame length overflow"));
                };
                if end > data.len() {
                    return Err(tampered(path, format!("truncated frame {counter}")));
                }
                let plain = try_open_unit(
                    &key,
                    &header.prefix,
                    counter,
                    &unit_aad(&header.aad_base, counter),
                    &data[start..end],
                )
                .ok_or_else(|| {
                    tampered(
                        path,
                        format!(
                            "frame {counter} failed authentication (the ciphertext was \
                             modified, truncated, or does not belong here)"
                        ),
                    )
                })?;
                if n == 0 {
                    // The terminal frame must be the very last one.
                    if end != data.len() {
                        return Err(tampered(path, "data after the terminal frame"));
                    }
                    out.extend_from_slice(&plain);
                    return Ok(out);
                }
                out.extend_from_slice(&plain);
                pos = end;
                counter = counter
                    .checked_add(1)
                    .ok_or_else(|| internal("too many container frames"))?;
            }
            Err(tampered(
                path,
                "truncated container: the terminal frame is missing",
            ))
        }

        /// Number of distinct derived keys cached (test/diagnostic only).
        #[cfg(test)]
        pub(crate) fn cache_len(&self) -> usize {
            self.cache.lock().unwrap_or_else(|p| p.into_inner()).len()
        }
    }

    /// [`Crypto::sealing_writer`]'s sink: buffers plaintext up to one frame
    /// and seals each full frame straight into the underlying writer, so a
    /// data file is never held in memory whole. Only the `vortexlake`
    /// writer path uses it (a v1 `.pink` data file is sealed in one pass).
    #[cfg_attr(not(feature = "vortexlake"), allow(dead_code))]
    pub(crate) struct SealWriter<'a> {
        path: std::path::PathBuf,
        key: Arc<DerivedKey>,
        prefix: [u8; NONCE_PREFIX_LEN],
        aad: Vec<u8>,
        sink: &'a mut (dyn Write + Send),
        buf: Vec<u8>,
        counter: u32,
        finished: bool,
    }

    #[cfg_attr(not(feature = "vortexlake"), allow(dead_code))]
    impl SealWriter<'_> {
        fn seal_buffered(&mut self) -> std::io::Result<()> {
            if self.buf.is_empty() {
                return Ok(());
            }
            if self.counter == u32::MAX {
                return Err(std::io::Error::other(
                    "pink-lake encryption: too many container frames",
                ));
            }
            let plain = std::mem::take(&mut self.buf);
            let ct = seal_unit(
                &self.key,
                &self.prefix,
                self.counter,
                &unit_aad(&self.aad, self.counter),
                &plain,
            )
            .map_err(|e| std::io::Error::other(e.to_string()))?;
            let mut frame = (plain.len() as u32).to_le_bytes().to_vec();
            frame.extend_from_slice(&ct);
            self.sink.write_all(&frame)?;
            self.counter += 1;
            self.buf = plain;
            self.buf.clear();
            Ok(())
        }

        /// Flush the pending partial frame, seal the terminal zero-length
        /// frame, and flush the underlying writer. Must be called exactly
        /// once, after the last [`std::io::Write::write`]; an unfinished
        /// writer leaves no valid container behind (the storage layer
        /// discards its temp file on the error path).
        pub(crate) fn finish(mut self) -> Result<()> {
            let out = (|| -> std::io::Result<()> {
                self.seal_buffered()?;
                if self.counter == u32::MAX {
                    return Err(std::io::Error::other(
                        "pink-lake encryption: too many container frames",
                    ));
                }
                let terminal = seal_unit(
                    &self.key,
                    &self.prefix,
                    self.counter,
                    &unit_aad(&self.aad, self.counter),
                    &[],
                )
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                self.sink.write_all(&0u32.to_le_bytes())?;
                self.sink.write_all(&terminal)?;
                self.sink.flush()?;
                Ok(())
            })();
            self.finished = true;
            out.map_err(|e| LakeError::Io {
                context: format!("sealing the encrypted container of {}", self.path.display()),
                source: e,
            })
        }
    }

    #[cfg_attr(not(feature = "vortexlake"), allow(dead_code))]
    impl Write for SealWriter<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.finished {
                return Err(std::io::Error::other(
                    "pink-lake encryption: write after finish",
                ));
            }
            let mut rest = buf;
            while !rest.is_empty() {
                let room = SEGMENT_PLAIN_MAX - self.buf.len();
                let take = room.min(rest.len());
                self.buf.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if self.buf.len() == SEGMENT_PLAIN_MAX {
                    self.seal_buffered()?;
                }
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.sink.flush()
        }
    }
}

#[cfg(not(feature = "encryption"))]
mod imp {
    use super::*;

    use std::io::Write;

    /// Stub: unconstructible without the feature, so a keyed lake handle can
    /// never exist and every artifact stays plaintext.
    #[derive(Debug)]
    pub(crate) struct Crypto(());

    #[derive(Debug)]
    #[allow(dead_code)]
    pub(crate) struct SealWriter<'a>(std::marker::PhantomData<&'a ()>);

    #[allow(unused_variables)]
    impl Crypto {
        pub(crate) fn new(key: EncryptionKey) -> Result<Self> {
            Err(unavailable())
        }
        pub(crate) fn seal(
            &self,
            path: &Path,
            domain: Domain,
            binding: &str,
            plain: &[u8],
        ) -> Result<Vec<u8>> {
            Err(unavailable())
        }
        #[allow(dead_code)]
        pub(crate) fn sealing_writer<'a>(
            &'a self,
            path: &Path,
            domain: Domain,
            binding: &str,
            sink: &'a mut (dyn Write + Send),
        ) -> Result<SealWriter<'a>> {
            Err(unavailable())
        }
        pub(crate) fn open(
            &self,
            path: &Path,
            domain: Domain,
            binding: &str,
            data: &[u8],
        ) -> Result<Vec<u8>> {
            Err(unavailable())
        }
    }

    /// The stub must still satisfy `Write` so the shared `vortexlake`
    /// writer path type-checks; it can never be constructed
    /// ([`Crypto::sealing_writer`] always fails first).
    impl std::io::Write for SealWriter<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[allow(dead_code)]
    impl SealWriter<'_> {
        pub(crate) fn finish(self) -> Result<()> {
            Err(unavailable())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> std::path::PathBuf {
        std::path::PathBuf::from("catalog.json")
    }

    #[cfg(feature = "encryption")]
    fn seal(crypto: &Crypto, domain: Domain, binding: &str, plain: &[u8]) -> Vec<u8> {
        crypto.seal(&path(), domain, binding, plain).unwrap()
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn round_trips_manifests_and_data_files() {
        let crypto = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        for (domain, binding) in [
            (Domain::Manifest, ""),
            (Domain::DataFile, "data/main.t/x.vortex"),
            (Domain::Commit, "commits/2.json"),
            (Domain::DeleteFile, "data/main.t/x.pinkdel"),
        ] {
            for plain in [
                b"".to_vec(),
                b"{}".to_vec(),
                vec![0xABu8; 3 * SEGMENT_PLAIN_MAX + 17],
            ] {
                let sealed = seal(&crypto, domain, binding, &plain);
                assert!(is_container(&sealed));
                let opened = crypto.open(&path(), domain, binding, &sealed).unwrap();
                assert_eq!(opened, plain);
            }
        }
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn the_kdf_runs_once_per_session_whatever_the_file_count() {
        let crypto = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        assert_eq!(crypto.cache_len(), 0, "no eager derivation");
        let mut containers = Vec::new();
        for plain in [
            b"a".to_vec(),
            b"b".to_vec(),
            vec![7u8; SEGMENT_PLAIN_MAX + 1],
        ] {
            containers.push(seal(&crypto, Domain::DataFile, "data/t/x.vortex", &plain));
        }
        let manifest = seal(&crypto, Domain::Manifest, "", b"manifest");
        assert_eq!(
            crypto.cache_len(),
            1,
            "one session salt: one Argon2id derivation for every container"
        );
        for (i, plain) in [b"a".to_vec(), b"b".to_vec()].into_iter().enumerate() {
            assert_eq!(
                crypto
                    .open(&path(), Domain::DataFile, "data/t/x.vortex", &containers[i])
                    .unwrap(),
                plain
            );
        }
        assert_eq!(
            crypto
                .open(&path(), Domain::Manifest, "", &manifest)
                .unwrap(),
            b"manifest"
        );
        assert_eq!(crypto.cache_len(), 1, "opens reuse the session cache");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn every_container_has_a_fresh_nonce_prefix_and_cannot_be_replayed() {
        let crypto = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        let a = seal(&crypto, Domain::Manifest, "", b"same bytes");
        let b = seal(&crypto, Domain::Manifest, "", b"same bytes");
        assert_ne!(
            a, b,
            "two containers must never share a nonce prefix (keystream reuse)"
        );
        assert_eq!(
            crypto.open(&path(), Domain::Manifest, "", &a).unwrap(),
            b"same bytes"
        );
        assert_eq!(
            crypto.open(&path(), Domain::Manifest, "", &b).unwrap(),
            b"same bytes"
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn wrong_key_is_distinguished_from_tampering() {
        let good = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        let bad = Crypto::new(EncryptionKey::from_passphrase("not it")).unwrap();
        let sealed = seal(&good, Domain::Manifest, "", b"payload");
        let err = bad
            .open(&path(), Domain::Manifest, "", &sealed)
            .unwrap_err();
        assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");

        // Wrong key *type* is a wrong key, not a structural error.
        let raw = Crypto::new(EncryptionKey::from_raw([9u8; 32])).unwrap();
        let err = raw
            .open(&path(), Domain::Manifest, "", &sealed)
            .unwrap_err();
        assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");

        // A flipped payload byte with the right key is tampering.
        let mut flipped = sealed.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0x01;
        let err = good
            .open(&path(), Domain::Manifest, "", &flipped)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");

        // A flipped header byte (the nonce prefix) fails the key-check: the
        // error says wrong key *or* a tampered header, the engine's contract.
        let mut flipped = sealed.clone();
        flipped[19] ^= 0x01;
        let err = good
            .open(&path(), Domain::Manifest, "", &flipped)
            .unwrap_err();
        assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");

        // A truncated payload (terminal frame lost) is tampering.
        let cut = &sealed[..sealed.len() - (4 + TAG_LEN)];
        let err = good.open(&path(), Domain::Manifest, "", cut).unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");

        // A container sealed for another domain/binding is refused.
        let data = seal(&good, Domain::DataFile, "data/a/x.pink", b"p");
        let err = good.open(&path(), Domain::Manifest, "", &data).unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        let err = good
            .open(&path(), Domain::DataFile, "data/b/y.pink", &data)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        // ... including a commit delta used as a data file.
        let commit = seal(&good, Domain::Commit, "commits/2.json", b"c");
        let err = good
            .open(&path(), Domain::DataFile, "commits/2.json", &commit)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn keyed_reads_of_plaintext_and_engine_containers_are_refused() {
        let crypto = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        let err = crypto
            .open(&path(), Domain::Manifest, "", b"{\"format_version\":4}")
            .unwrap_err();
        assert!(matches!(err, LakeError::NotEncrypted { .. }), "{err:?}");
        // An engine container mistaken for a lake one names the mixup.
        let mut engine = ENGINE_ENC_MAGIC.to_vec();
        engine.extend_from_slice(&[0u8; 64]);
        let err = crypto
            .open(&path(), Domain::Manifest, "", &engine)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        assert!(err.to_string().contains("PINKENC1"), "{err}");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn an_unsupported_container_version_is_not_tampering() {
        // A container whose version this build does not know is an upgrade
        // error, not corruption/tampering.
        let crypto = Crypto::new(EncryptionKey::from_passphrase("lake version")).unwrap();
        let mut data = ENC_MAGIC.to_vec();
        data.extend_from_slice(&99u32.to_le_bytes());
        data.resize(128, 0);
        let err = crypto
            .open(&path(), Domain::Manifest, "", &data)
            .unwrap_err();
        assert!(
            matches!(err, LakeError::UnsupportedContainer { found: 99, .. }),
            "{err:?}"
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn a_raw_key_round_trips_and_never_uses_argon2() {
        let crypto = Crypto::new(EncryptionKey::from_raw([7u8; 32])).unwrap();
        let sealed = seal(&crypto, Domain::DataFile, "data/t/f.vortex", b"bytes");
        // The header carries kdf = raw and no parameters.
        assert_eq!(sealed[13], KDF_RAW);
        assert_eq!(
            crypto
                .open(&path(), Domain::DataFile, "data/t/f.vortex", &sealed)
                .unwrap(),
            b"bytes"
        );
        // A passphrase cannot open a raw-key container and vice versa.
        let pass = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        let err = pass
            .open(&path(), Domain::DataFile, "data/t/f.vortex", &sealed)
            .unwrap_err();
        assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn the_sealing_writer_matches_whole_buffer_sealing() {
        use std::io::Write;
        let crypto = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap();
        let plain = vec![0x5Au8; SEGMENT_PLAIN_MAX + 12345];
        let whole = crypto
            .seal(&path(), Domain::DataFile, "data/t/x.vortex", &plain)
            .unwrap();
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut w = crypto
                .sealing_writer(&path(), Domain::DataFile, "data/t/x.vortex", &mut sink)
                .unwrap();
            for chunk in plain.chunks(1000) {
                w.write_all(chunk).unwrap();
            }
            w.finish().unwrap();
        }
        // The streamed container has its own fresh prefix, so it is not
        // byte-equal to `whole`; what must match is the decrypted payload.
        assert_ne!(sink, whole, "each container mints its own prefix");
        assert_eq!(
            crypto
                .open(&path(), Domain::DataFile, "data/t/x.vortex", &sink)
                .unwrap(),
            plain
        );
        assert_eq!(
            crypto
                .open(&path(), Domain::DataFile, "data/t/x.vortex", &whole)
                .unwrap(),
            plain
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn a_session_with_key_material_is_required() {
        let err = Crypto::new(EncryptionKey::default()).unwrap_err();
        assert!(matches!(err, LakeError::Internal { .. }), "{err:?}");
    }

    #[cfg(not(feature = "encryption"))]
    #[test]
    fn without_the_feature_every_keyed_operation_names_the_missing_support() {
        let key = EncryptionKey::from_passphrase("lake sesame");
        let err = Crypto::new(key).unwrap_err();
        assert!(matches!(
            err,
            LakeError::FeatureMissing {
                feature: "encryption",
                ..
            }
        ));
        assert!(err.to_string().contains("encryption"), "{err}");
    }
}
