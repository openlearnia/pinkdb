//! At-rest encryption for a pink-lake (cargo feature `encryption`; no crypto
//! is linked without it).
//!
//! This module mirrors the engine's `pinkdb::crypto` approach — the same
//! audited RustCrypto AES-256-GCM, the same "wrong key vs tampering"
//! key-check trick — but the container is the lake's own: a lake **artifact**
//! is wrapped, not an engine `.pink` file. The magic is deliberately
//! different (`PNKLAKE1` vs the engine's `PINKENC1`) so the two layers can
//! never be confused, and a lake never stores a `PINKENC1` file.
//!
//! # Container format (`PNKLAKE1`, all integers little-endian)
//!
//! ```text
//! magic "PNKLAKE1" (8 bytes)
//! container version u32 (= 2)
//! cipher id u8 (1 = AES-256-GCM)
//! kdf id u8 (0 = raw 32-byte key)
//! domain u8 (1 = manifest, 2 = data file, 3 = commit delta, 4 = delete file)
//! key-check length u32 (= the key-check plaintext's length)
//! nonce base [16] (fresh 128-bit random value *per container*)
//! binding length u32
//! binding bytes ("" for a manifest; the lake-relative path otherwise)
//! key-check: AEAD of the fixed key-check plaintext, + 16 bytes
//! payload: frames of `plain_len u32 LE | ciphertext (plain_len + 16)`,
//!          each frame at most 1 MiB of plaintext; the last frame is a
//!          terminal zero-length frame (a container cut at a frame boundary
//!          is detected by its absence)
//! ```
//!
//! Every AEAD unit uses a 96-bit nonce derived as
//! `truncate96(SHA-256("pinkdb-nonce-v4" || domain || nonce_base || counter))`
//! (L-4): counter 0 is the key-check, then 1, 2, … per payload frame. The
//! base is fresh 128-bit random data **per container** (never shared across
//! the containers of one save), so a (key, nonce) pair is never reused — not
//! within one container, and not across the data files, the manifest, or the
//! delete files of a version. Older container versions are not read: wave 104
//! removed the version-1 reader. The AAD binds the whole public header
//! (magic, version, cipher, KDF id, domain, binding, nonce base) plus the
//! counter, so a container cannot be reinterpreted across domains, paths, or
//! contexts.
//!
//! The binding is the whole integrity story for *identity*: a data or delete
//! file's container authenticates the lake-relative path it was written for,
//! so swapping two files (even of the same shape) fails authentication
//! instead of silently loading different values. Both live under the same
//! path binding the store records for them.
//!
//! # Key material
//!
//! Per-file data keys are raw 32-byte values recorded in each file's catalog
//! row (DuckLake semantics). No KDF runs here: a passphrase in
//! [`EncryptionKey`] is for the engine-encrypted `pink:` catalog database
//! only and is passed to `pinkdb::crypto`; a [`Crypto`] session rejects it.
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

/// Container format version written by this build (L-4: 128-bit nonce base).
/// Any other version is refused with a clean message; wave 104 removed the
/// version-1 reader.
pub(crate) const CONTAINER_VERSION: u32 = 2;
const CIPHER_AES256_GCM: u8 = 1;
const KDF_RAW: u8 = 0;

const KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;
/// The random per-container nonce base.
const NONCE_BASE_LEN: usize = 16;

/// Plaintext authenticated by the key-check block so a wrong key is reported
/// as `wrong key` instead of a parse error.
const KEY_CHECK_PLAIN: &[u8] = b"pinkdb lake encryption key-check/v1";

/// magic(8) + version(4) + cipher(1) + kdf(1) + domain(1) + key-check
/// length(4) + nonce base(16) + binding length(4).
const HEADER_FIXED_LEN: usize = 39;

/// Payloads are sealed in 1 MiB plaintext frames (the engine's checkpoint
/// segment size), so a whole file never has to be held twice in memory.
pub(crate) const SEGMENT_PLAIN_MAX: usize = 1 << 20;

/// Which kind of lake artifact a container holds; the AAD binds it, so a
/// data file can never authenticate as a delete file (or the reverse).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(dead_code)]
pub(crate) enum Domain {
    /// One `data/<table>/<uuid>.pink` file, bound to its path.
    DataFile,
    /// One `data/<table>/<uuid>.pinkdel` delete file, bound to its path.
    DeleteFile,
}

impl Domain {
    /// The AAD tag; the two live values are never renumbered.
    fn id(self) -> u8 {
        match self {
            Domain::DataFile => 2,
            Domain::DeleteFile => 4,
        }
    }

    fn name(id: u8) -> &'static str {
        match id {
            2 => "a data file",
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
                  --features pinklake,s3,encryption`)"
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

    /// A passphrase for the engine-encrypted `pink:` catalog database. Lake
    /// data files never derive from it — they use raw per-file keys.
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

    /// The same key as the engine's type, for opening a `pink:` catalog
    /// database (the engine's own `PINKENC1` at-rest encryption). A keyed
    /// lake's data files never use this key — each gets its own random key —
    /// but the catalog database may.
    pub(crate) fn engine_key(&self) -> Option<pinkdb::EncryptionKey> {
        if let Some(raw) = self.raw {
            return Some(pinkdb::EncryptionKey::from_raw(raw));
        }
        self.passphrase
            .as_ref()
            .map(|pass| pinkdb::EncryptionKey::from_passphrase(&String::from_utf8_lossy(pass)))
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

    /// One lake session's key: the raw 32-byte value the per-file catalog
    /// rows record.
    pub(crate) struct Crypto {
        key: EncryptionKey,
    }

    impl fmt::Debug for Crypto {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Crypto({:?})", self.key)
        }
    }

    fn random_nonce_base() -> [u8; NONCE_BASE_LEN] {
        let mut base = [0u8; NONCE_BASE_LEN];
        OsRng.fill_bytes(&mut base);
        base
    }

    /// The 96-bit nonce of one unit (L-4): a 128-bit random base plus the
    /// domain and counter.
    pub(crate) fn nonce_for(base: &[u8; NONCE_BASE_LEN], domain: Domain, counter: u32) -> [u8; 12] {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(b"pinkdb-nonce-v4\0");
        h.update([domain.id()]);
        h.update(base);
        h.update(counter.to_le_bytes());
        let digest = h.finalize();
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&digest[..12]);
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
        base: &[u8; NONCE_BASE_LEN],
        domain: Domain,
        counter: u32,
        aad: &[u8],
        plain: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(&key.bytes)
            .map_err(|_| internal("bad key length for AES-256-GCM"))?;
        cipher
            .encrypt(
                Nonce::from_slice(&nonce_for(base, domain, counter)),
                Payload { msg: plain, aad },
            )
            .map_err(|_| internal("AEAD encryption failed"))
    }

    /// `None` = authentication failed (wrong key or tampering).
    fn try_open_unit(
        key: &DerivedKey,
        base: &[u8; NONCE_BASE_LEN],
        domain: Domain,
        counter: u32,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Option<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(&key.bytes).ok()?;
        cipher
            .decrypt(
                Nonce::from_slice(&nonce_for(base, domain, counter)),
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

    /// Serialize the public header up to (but excluding) the key-check block.
    /// This is also the AAD base of every unit, so a change to any field —
    /// including the container version, the nonce base, the domain, or the
    /// binding — breaks every unit instead of being a cheap header edit.
    fn header_base(nonce_base: &[u8; NONCE_BASE_LEN], domain: Domain, binding: &str) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEADER_FIXED_LEN + binding.len());
        v.extend_from_slice(ENC_MAGIC);
        v.extend_from_slice(&CONTAINER_VERSION.to_le_bytes());
        v.push(CIPHER_AES256_GCM);
        v.push(KDF_RAW);
        v.push(domain.id());
        v.extend_from_slice(&(KEY_CHECK_PLAIN.len() as u32).to_le_bytes());
        v.extend_from_slice(nonce_base);
        v.extend_from_slice(&(binding.len() as u32).to_le_bytes());
        v.extend_from_slice(binding.as_bytes());
        v
    }

    /// The public part of a parsed container header.
    struct ParsedHeader {
        domain: u8,
        binding: String,
        nonce_base: [u8; NONCE_BASE_LEN],
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
        if data[13] != KDF_RAW {
            return Err(tampered(
                path,
                format!(
                    "unsupported KDF id {} (wave 104 removed the KDF path; per-file keys \
                     are raw)",
                    data[13]
                ),
            ));
        }
        let domain = data[14];
        if !(1..=5).contains(&domain) {
            return Err(tampered(path, format!("unknown container domain {domain}")));
        }
        let key_check_len = u32::from_le_bytes(data[15..19].try_into().expect("4 bytes")) as usize;
        if key_check_len != KEY_CHECK_PLAIN.len() {
            return Err(tampered(
                path,
                format!("bad key-check length {key_check_len}"),
            ));
        }
        let mut nonce_base = [0u8; NONCE_BASE_LEN];
        nonce_base.copy_from_slice(&data[19..19 + NONCE_BASE_LEN]);
        let len_at = 19 + NONCE_BASE_LEN;
        let binding_len =
            u32::from_le_bytes(data[len_at..len_at + 4].try_into().expect("4 bytes")) as usize;
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
        if data.len() < pos + key_check_len + TAG_LEN {
            return Err(tampered(path, "truncated key-check block"));
        }
        let key_check = data[pos..pos + key_check_len + TAG_LEN].to_vec();
        let payload_start = pos + key_check_len + TAG_LEN;
        Ok(ParsedHeader {
            domain,
            binding,
            nonce_base,
            aad_base: data[..pos].to_vec(),
            key_check,
            payload_start,
        })
    }

    impl Crypto {
        /// One keyed lake session over a raw 32-byte key. A passphrase key is
        /// rejected here: per-file keys are raw, and a passphrase exists only
        /// to unlock the engine-encrypted catalog database.
        pub(crate) fn new(key: EncryptionKey) -> Result<Self> {
            if key.raw.is_none() {
                return Err(match key.passphrase {
                    Some(_) => internal(
                        "a lake data file needs a raw 32-byte key; the passphrase is only for \
                         the engine-encrypted catalog",
                    ),
                    None => internal("encryption key holds no material"),
                });
            }
            Ok(Self { key })
        }

        /// The session's raw 32-byte key.
        fn derive(&self, path: &Path) -> Result<DerivedKey> {
            match self.key.raw {
                Some(raw) => Ok(DerivedKey { bytes: raw }),
                None => Err(wrong_key(
                    path,
                    "the file uses a raw 32-byte key, but a passphrase was supplied",
                )),
            }
        }

        /// Seal one whole artifact into a container. Every call mints a fresh
        /// random nonce base (so no two containers of a session ever reuse a
        /// (key, nonce) pair).
        ///
        /// Because the base is per container, sealing the same plaintext
        /// twice produces two different containers; nothing may rely on
        /// sealing to be deterministic.
        pub(crate) fn seal(
            &self,
            path: &Path,
            domain: Domain,
            binding: &str,
            plain: &[u8],
        ) -> Result<Vec<u8>> {
            let key = self.derive(path)?;
            let nonce_base = random_nonce_base();
            let aad = header_base(&nonce_base, domain, binding);
            let mut out = aad.clone();
            out.extend_from_slice(&seal_unit(
                &key,
                &nonce_base,
                domain,
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
                let ct = seal_unit(
                    &key,
                    &nonce_base,
                    domain,
                    counter,
                    &unit_aad(&aad, counter),
                    frame,
                )?;
                out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                out.extend_from_slice(&ct);
                counter += 1;
                pos = end;
            }
            if counter == u32::MAX {
                return Err(internal("too many container frames"));
            }
            let terminal = seal_unit(
                &key,
                &nonce_base,
                domain,
                counter,
                &unit_aad(&aad, counter),
                &[],
            )?;
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&terminal);
            Ok(out)
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
            let key = self.derive(path)?;
            if try_open_unit(
                &key,
                &header.nonce_base,
                domain,
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
                    &header.nonce_base,
                    domain,
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
    }
}

#[cfg(not(feature = "encryption"))]
mod imp {
    use super::*;

    /// Stub: unconstructible without the feature, so a keyed lake handle can
    /// never exist and every artifact stays plaintext.
    #[derive(Debug)]
    pub(crate) struct Crypto(());

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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> std::path::PathBuf {
        std::path::PathBuf::from("data/main.t/x.pink")
    }

    #[cfg(feature = "encryption")]
    fn crypto() -> Crypto {
        Crypto::new(EncryptionKey::from_raw([7u8; 32])).unwrap()
    }

    #[cfg(feature = "encryption")]
    fn seal(crypto: &Crypto, domain: Domain, binding: &str, plain: &[u8]) -> Vec<u8> {
        crypto.seal(&path(), domain, binding, plain).unwrap()
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn round_trips_data_and_delete_files() {
        let crypto = crypto();
        for (domain, binding) in [
            (Domain::DataFile, "data/main.t/x.pink"),
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
    fn every_container_has_a_fresh_nonce_base_and_cannot_be_replayed() {
        let crypto = crypto();
        let a = seal(&crypto, Domain::DataFile, "", b"same bytes");
        let b = seal(&crypto, Domain::DataFile, "", b"same bytes");
        assert_ne!(
            a, b,
            "two containers must never share a nonce base (keystream reuse)"
        );
        assert_eq!(
            crypto.open(&path(), Domain::DataFile, "", &a).unwrap(),
            b"same bytes"
        );
        assert_eq!(
            crypto.open(&path(), Domain::DataFile, "", &b).unwrap(),
            b"same bytes"
        );
    }

    /// L-4: containers carry a 128-bit nonce base; the derived nonce
    /// separates domains and counters.
    #[cfg(feature = "encryption")]
    #[test]
    fn nonce_base_is_128_bits_and_domain_separated() {
        let crypto = crypto();
        let a = seal(&crypto, Domain::DataFile, "", b"same");
        let b = seal(&crypto, Domain::DataFile, "", b"same");
        assert_eq!(u32::from_le_bytes(a[8..12].try_into().unwrap()), 2);
        let base_a: [u8; 16] = a[19..35].try_into().unwrap();
        let base_b: [u8; 16] = b[19..35].try_into().unwrap();
        assert!(base_a.iter().any(|x| *x != 0));
        assert_ne!(base_a, base_b);
        assert_ne!(
            imp::nonce_for(&base_a, Domain::DataFile, 0),
            imp::nonce_for(&base_a, Domain::DeleteFile, 0)
        );
        assert_ne!(
            imp::nonce_for(&base_a, Domain::DataFile, 0),
            imp::nonce_for(&base_a, Domain::DataFile, 1)
        );
    }

    /// Wave 104 removed the version-1 reader: an old container is refused
    /// as unsupported, never decoded with the legacy nonce layout.
    #[cfg(feature = "encryption")]
    #[test]
    fn legacy_v1_containers_are_refused() {
        let crypto = crypto();
        let new = seal(&crypto, Domain::DataFile, "data/t/x.pink", b"bytes");
        let mut old = new.clone();
        old[8..12].copy_from_slice(&1u32.to_le_bytes());
        let err = crypto
            .open(&path(), Domain::DataFile, "data/t/x.pink", &old)
            .unwrap_err();
        assert!(
            matches!(err, LakeError::UnsupportedContainer { found: 1, .. }),
            "{err:?}"
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn wrong_key_is_distinguished_from_tampering() {
        let good = crypto();
        let bad = Crypto::new(EncryptionKey::from_raw([8u8; 32])).unwrap();
        let sealed = seal(&good, Domain::DataFile, "", b"payload");
        let err = bad
            .open(&path(), Domain::DataFile, "", &sealed)
            .unwrap_err();
        assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");

        // A flipped payload byte with the right key is tampering.
        let mut flipped = sealed.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0x01;
        let err = good
            .open(&path(), Domain::DataFile, "", &flipped)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");

        // A flipped header byte (the nonce base) fails the key-check: the
        // error says wrong key *or* a tampered header, the engine's contract.
        let mut flipped = sealed.clone();
        flipped[19] ^= 0x01;
        let err = good
            .open(&path(), Domain::DataFile, "", &flipped)
            .unwrap_err();
        assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");

        // A truncated payload (terminal frame lost) is tampering.
        let cut = &sealed[..sealed.len() - (4 + TAG_LEN)];
        let err = good.open(&path(), Domain::DataFile, "", cut).unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");

        // A container sealed for another domain/binding is refused.
        let data = seal(&good, Domain::DataFile, "data/a/x.pink", b"p");
        let err = good.open(&path(), Domain::DataFile, "", &data).unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        let err = good
            .open(&path(), Domain::DataFile, "data/b/y.pink", &data)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        // ... including a delete file used as a data file.
        let delete = seal(&good, Domain::DeleteFile, "data/main.t/x.pinkdel", b"c");
        let err = good
            .open(&path(), Domain::DataFile, "data/main.t/x.pinkdel", &delete)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn keyed_reads_of_plaintext_and_engine_containers_are_refused() {
        let crypto = crypto();
        let err = crypto
            .open(&path(), Domain::DataFile, "", b"{\"format_version\":7}")
            .unwrap_err();
        assert!(matches!(err, LakeError::NotEncrypted { .. }), "{err:?}");
        // An engine container mistaken for a lake one names the mixup.
        let mut engine = ENGINE_ENC_MAGIC.to_vec();
        engine.extend_from_slice(&[0u8; 64]);
        let err = crypto
            .open(&path(), Domain::DataFile, "", &engine)
            .unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        assert!(err.to_string().contains("PINKENC1"), "{err}");
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn an_unsupported_container_version_is_not_tampering() {
        // A container whose version this build does not know is an upgrade
        // error, not corruption/tampering.
        let crypto = crypto();
        let mut data = ENC_MAGIC.to_vec();
        data.extend_from_slice(&99u32.to_le_bytes());
        data.resize(128, 0);
        let err = crypto
            .open(&path(), Domain::DataFile, "", &data)
            .unwrap_err();
        assert!(
            matches!(err, LakeError::UnsupportedContainer { found: 99, .. }),
            "{err:?}"
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn a_raw_key_round_trips_and_the_header_has_no_kdf() {
        let crypto = crypto();
        let sealed = seal(&crypto, Domain::DataFile, "data/t/f.pink", b"bytes");
        // The header carries kdf = raw and no parameters.
        assert_eq!(sealed[13], KDF_RAW);
        assert_eq!(
            crypto
                .open(&path(), Domain::DataFile, "data/t/f.pink", &sealed)
                .unwrap(),
            b"bytes"
        );
        // A passphrase is only for the engine-encrypted catalog; the per-file
        // container needs the raw key.
        let err = Crypto::new(EncryptionKey::from_passphrase("lake sesame")).unwrap_err();
        assert!(err.to_string().contains("raw 32-byte key"), "{err}");
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
        let key = EncryptionKey::from_raw([7u8; 32]);
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
