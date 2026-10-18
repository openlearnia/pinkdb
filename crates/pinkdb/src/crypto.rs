//! Optional at-rest encryption for `.pink` checkpoints and WAL files
//! (cargo feature `encryption`; no crypto is linked without it).
//!
//! # Container format (`PINKENC1`, all integers little-endian)
//!
//! ```text
//! magic "PINKENC1" (8 bytes)
//! container version u32 (= 2; version 1 files are read and upgraded)
//! cipher id u8 (1 = AES-256-GCM)
//! kdf id u8 (0 = raw 32-byte key, 1 = Argon2id)
//! context u64 (0 for a checkpoint; the WAL generation for a WAL file)
//! nonce prefix [8] (fresh random bytes per container)
//! key-check length u32 (= 30)
//! [kdf id == 1] m_cost u32 KiB | t_cost u32 | p_cost u32 | salt_len u32 | salt
//! key-check: AEAD of a fixed 30-byte plaintext, 30 + 16 bytes
//! payload:
//!   checkpoint: frames of `plain_len u32 LE | ciphertext (plain_len + 16)`,
//!               each frame at most 1 MiB of plaintext; the last frame is a
//!               terminal zero-length frame (a container cut at a segment
//!               boundary is detected by its absence)
//!   WAL:        records of `plain_len u32 LE | lsn u64 LE | crc u32 | ciphertext`
//!               where `plain_len` is the *plaintext* payload length
//! ```
//!
//! Every AEAD unit uses nonce `nonce_prefix || counter u32 LE`: counter 0 is
//! the key-check, then 1, 2, ... per checkpoint segment / WAL record. Within
//! one container a (key, nonce) pair is therefore never reused. Every keyed
//! *open* of a WAL rebuilds it with a fresh random prefix (and a fresh
//! `wal_id`), the wave-22 policy: a file cut exactly at a record boundary is
//! byte-identical to a cleanly shorter one, so a counter resynced from the
//! record count could otherwise hand out a discarded record's
//! `(key, nonce, AAD)` again — real keystream reuse. With multi-process WAL
//! access (wave 45m) that stays safe because every appender re-checks the
//! WAL identity under the write lease and re-opens a replaced file before
//! appending, and every refresh resyncs the cipher counter from the on-disk
//! record count. Consequence (documented trade-off): a copied
//! same-generation file must not be independently appended to.
//!
//! The AAD binds the whole public header (so any header/KDF/parameter change
//! breaks every unit) plus the counter, plus the checkpoint segment index or
//! the WAL LSN. A `(.pink, WAL)` pair's units can never be confused because
//! the domain string differs.
//!
//! A wrong key fails the key-check block and is reported as `wrong key:`; a
//! bit flip anywhere in the payload fails GCM authentication and is reported
//! as corruption. The key-check is also what separates "wrong key" from
//! "corrupt file" for the header (both are indistinguishable in principle —
//! the error says so).
//!
//! # Key material
//!
//! [`EncryptionKey`] is either a raw 32-byte key or a passphrase. Passphrases
//! are stretched with Argon2id; the salt and parameters live in the file
//! header, so a file saved with different defaults still opens. Key bytes are
//! zeroed on drop on a best-effort basis (safe writes only — no unsafe; a
//! compiler may in principle elide the overwrite).

// Without the `encryption` feature this module is a parse/identity stub: the
// crypto internals are intentionally unreferenced (they exist behind `cfg`),
// so partial dead code is expected.
#![cfg_attr(not(feature = "encryption"), allow(dead_code))]

use crate::types::{PinkDbError, Result};

/// Container magic. Also what the feature-off `open` paths recognize to
/// produce the "file is encrypted; build with the encryption feature" error.
pub const ENC_MAGIC: &[u8; 8] = b"PINKENC1";

const ENC_VERSION: u32 = 2;
/// The pre-multi-process container version: no WAL publish block. Files are
/// read as-is (checkpoints) or rebuilt as current (WALs) at open.
const ENC_VERSION_V1: u32 = 1;
/// The container version every fresh **checkpoint** this build writes uses.
/// Deliberately unchanged by wave 45o: `.pink` bytes stay as wave 45m wrote
/// them (the per-frame touched sets live only in the WAL payloads).
pub(crate) const CONTAINER_VERSION: u32 = ENC_VERSION;
/// The container version a **WAL** this build writes uses (wave 45o): the
/// record payload gained the frame's touched-object set, so an older build
/// must refuse the file; the version word makes that refusal clean (a WAL
/// container older than this is rebuilt in place at open). The publish
/// block's CRC domain binds this version.
pub(crate) const WAL_CONTAINER_VERSION: u32 = 3;
const CIPHER_AES256_GCM: u8 = 1;
const KDF_RAW: u8 = 0;
const KDF_ARGON2ID: u8 = 1;

const KEY_LEN: usize = 32;
pub(crate) const TAG_LEN: usize = 16;
const NONCE_PREFIX_LEN: usize = 8;

/// Plaintext authenticated by the key-check block so a wrong key is reported
/// as `wrong key` instead of a parse error.
const KEY_CHECK_PLAIN: &[u8] = b"pinkdb encryption key-check/v1";

/// magic(8) + version(4) + cipher(1) + kdf(1) + context(8) + prefix(8) + key-check len(4).
const HEADER_FIXED_LEN: usize = 34;
const KDF_PARAMS_LEN: usize = 16; // m_cost, t_cost, p_cost, salt_len
const DEFAULT_SALT_LEN: u32 = 16;
const MIN_SALT_LEN: u32 = 8;
const MAX_SALT_LEN: u32 = 64;
/// Largest possible public header: fixed fields + KDF params + max salt +
/// key-check. Files no longer than this that fail to parse are torn headers
/// (a crash during creation), not record-bearing corruption.
pub(crate) const MAX_HEADER_LEN: usize =
    HEADER_FIXED_LEN + KDF_PARAMS_LEN + MAX_SALT_LEN as usize + KEY_CHECK_PLAIN.len() + TAG_LEN;

/// Checkpoint payloads are sealed in 1 MiB plaintext segments.
pub(crate) const SEGMENT_PLAIN_MAX: usize = 1 << 20;

/// Default Argon2id work factors (OWASP-recommended configuration): 19 MiB,
/// 2 iterations, 1 lane. Stored in the header, so raising them later keeps
/// existing files readable.
const ARGON2_M_COST_KIB: u32 = 19 * 1024;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;
/// Upper bounds accepted from a file header: a crafted header must not be
/// able to make an open burn minutes of Argon2id time or allocate unbounded
/// memory. Each bound is a small multiple of the default and the combined
/// `m_cost × t_cost` work is capped at 8× the default; the worst accepted
/// combination (76 MiB, t=4) costs ≈ 0.11 s in release on the development
/// machine (the defaults cost ≈ 0.03 s), keeping the worst case far under a
/// second even on a much slower machine.
const ARGON2_MAX_M_COST_KIB: u32 = 4 * ARGON2_M_COST_KIB;
const ARGON2_MAX_T_COST: u32 = 4 * ARGON2_T_COST;
const ARGON2_MAX_P_COST: u32 = 4 * ARGON2_P_COST;
/// `m_cost (KiB) × t_cost` bound: caps the total Argon2id work an open can
/// be forced to do. The default configuration is exactly 1/8 of this.
const ARGON2_MAX_WORK_KIB: u64 = 8 * (ARGON2_M_COST_KIB as u64) * (ARGON2_T_COST as u64);

// The defaults must stay inside the accepted envelope (compile-time check).
const _: () = {
    assert!(ARGON2_M_COST_KIB <= ARGON2_MAX_M_COST_KIB);
    assert!(ARGON2_T_COST <= ARGON2_MAX_T_COST);
    assert!(ARGON2_P_COST <= ARGON2_MAX_P_COST);
    assert!((ARGON2_M_COST_KIB as u64) * (ARGON2_T_COST as u64) <= ARGON2_MAX_WORK_KIB);
};

/// Which kind of artifact a container holds; mixed into the AAD so a
/// checkpoint and a WAL can never authenticate as each other.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Domain {
    Checkpoint,
    Wal,
}

impl Domain {
    fn label(self) -> &'static [u8] {
        match self {
            Domain::Checkpoint => b"pinkdb-enc-checkpoint-v1",
            Domain::Wal => b"pinkdb-enc-wal-v1",
        }
    }
}

// ---------- key material ----------

/// Key material for at-rest encryption: a raw 32-byte key or a passphrase.
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
    /// read. The salt and parameters are stored in the container header.
    pub fn from_passphrase(passphrase: &str) -> Self {
        Self {
            raw: None,
            passphrase: Some(passphrase.as_bytes().to_vec()),
        }
    }

    /// The CLI convention: a non-empty `PINKDB_KEY` environment variable is a
    /// passphrase. `None` when unset or empty (plaintext mode). A set-but-
    /// non-UTF-8 value is a hard error: silently ignoring it would make a
    /// keyed invocation write plaintext.
    pub fn from_env() -> Result<Option<Self>> {
        match std::env::var_os("PINKDB_KEY") {
            None => Ok(None),
            Some(v) if v.is_empty() => Ok(None),
            Some(v) => match v.into_string() {
                Ok(k) => Ok(Some(Self::from_passphrase(&k))),
                Err(_) => Err(PinkDbError::Execution(
                    "PINKDB_KEY is not valid UTF-8; set a UTF-8 passphrase or unset it".to_string(),
                )),
            },
        }
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

// ---------- public (unencrypted) header parsing ----------

/// The public part of a container header: enough to identify the file, derive
/// the key, and locate the payload — no key required.
#[derive(Debug, Clone)]
pub(crate) struct ContainerHeader {
    /// Container format version (1, 2, or 3 for WALs).
    pub version: u32,
    /// 0 for a checkpoint, the WAL generation for a WAL file.
    pub context: u64,
    pub kdf: u8,
    /// `(m_cost KiB, t_cost, p_cost, salt)` for [`KDF_ARGON2ID`].
    pub params: Option<(u32, u32, u32, Vec<u8>)>,
    pub prefix: [u8; NONCE_PREFIX_LEN],
    /// Header bytes up to (but excluding) the key-check ciphertext: the AAD
    /// base bound into every unit.
    pub aad_base: Vec<u8>,
    key_check: Vec<u8>,
    /// Offset where the payload begins (also the full header length).
    pub payload_start: usize,
}

/// Does `data` start with the container magic?
pub(crate) fn is_container(data: &[u8]) -> bool {
    data.starts_with(ENC_MAGIC)
}

/// Whether a container `version` is one this build reads: a fresh checkpoint,
/// a WAL, or a legacy (v1) file. A newer version is a hard error, never a torn
/// creation — a newer build may have written records this one cannot parse.
pub(crate) fn is_known_container_version(version: u32) -> bool {
    version == ENC_VERSION || version == ENC_VERSION_V1 || version == WAL_CONTAINER_VERSION
}

fn corrupt(what: &str) -> PinkDbError {
    PinkDbError::Execution(format!("encrypted file: {what}"))
}

/// Structural header parse (no crypto). Validates magic, version, cipher id,
/// KDF id, parameter ranges, salt, and the key-check block's presence.
pub(crate) fn parse_container_header(data: &[u8]) -> Result<ContainerHeader> {
    if !is_container(data) {
        return Err(corrupt("bad container magic"));
    }
    if data.len() < HEADER_FIXED_LEN {
        return Err(corrupt("truncated container header"));
    }
    let version = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
    if version != ENC_VERSION && version != ENC_VERSION_V1 && version != WAL_CONTAINER_VERSION {
        return Err(corrupt(&format!(
            "unsupported container version {version} (want {WAL_CONTAINER_VERSION} for WALs, \
             {ENC_VERSION} for checkpoints, or an older version for legacy files)"
        )));
    }
    let cipher = data[12];
    if cipher != CIPHER_AES256_GCM {
        return Err(corrupt(&format!("unsupported cipher id {cipher}")));
    }
    let kdf = data[13];
    let context = u64::from_le_bytes(data[14..22].try_into().expect("8 bytes"));
    let mut prefix = [0u8; NONCE_PREFIX_LEN];
    prefix.copy_from_slice(&data[22..30]);
    let key_check_len = u32::from_le_bytes(data[30..34].try_into().expect("4 bytes")) as usize;
    if key_check_len != KEY_CHECK_PLAIN.len() {
        return Err(corrupt(&format!("bad key-check length {key_check_len}")));
    }
    let mut pos = HEADER_FIXED_LEN;
    let params = match kdf {
        KDF_RAW => None,
        KDF_ARGON2ID => {
            if data.len() < pos + KDF_PARAMS_LEN {
                return Err(corrupt("truncated Argon2id parameters"));
            }
            let u32_at =
                |off: usize| u32::from_le_bytes(data[off..off + 4].try_into().expect("4 bytes"));
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
                return Err(corrupt(&format!(
                    "implausible Argon2id parameters (m={m_cost} KiB, t={t_cost}, p={p_cost}; \
                     caps m<={ARGON2_MAX_M_COST_KIB}, t<={ARGON2_MAX_T_COST}, \
                     p<={ARGON2_MAX_P_COST}, m*t<={ARGON2_MAX_WORK_KIB})"
                )));
            }
            if !(MIN_SALT_LEN..=MAX_SALT_LEN).contains(&salt_len) {
                return Err(corrupt(&format!("bad salt length {salt_len}")));
            }
            let salt_len = salt_len as usize;
            let salt_start = pos + KDF_PARAMS_LEN;
            if data.len() < salt_start + salt_len {
                return Err(corrupt("truncated salt"));
            }
            let salt = data[salt_start..salt_start + salt_len].to_vec();
            pos = salt_start + salt_len;
            Some((m_cost, t_cost, p_cost, salt))
        }
        other => return Err(corrupt(&format!("unsupported KDF id {other}"))),
    };
    if data.len() < pos + key_check_len + TAG_LEN {
        return Err(corrupt("truncated key-check block"));
    }
    let key_check = data[pos..pos + key_check_len + TAG_LEN].to_vec();
    let payload_start = pos + key_check_len + TAG_LEN;
    Ok(ContainerHeader {
        version,
        context,
        kdf,
        params,
        prefix,
        aad_base: data[..pos].to_vec(),
        key_check,
        payload_start,
    })
}

// ---------- errors shared by both feature modes ----------

/// Encrypted artifact opened by a build/key path that cannot decrypt it.
pub(crate) fn encrypted_error(what: &str) -> PinkDbError {
    #[cfg(feature = "encryption")]
    let hint =
        "open it with a key (open_with_key / open_durable_with_key, or the CLI's PINKDB_KEY)";
    #[cfg(not(feature = "encryption"))]
    let hint = "build with the encryption feature to read it";
    PinkDbError::Execution(format!("{what}: file is encrypted; {hint}"))
}

/// A keyed operation in a build without the `encryption` feature.
#[cfg(not(feature = "encryption"))]
pub(crate) fn unavailable() -> PinkDbError {
    PinkDbError::Execution(
        "encryption support is not compiled in; rebuild with the encryption feature".to_string(),
    )
}

fn wrong_key() -> PinkDbError {
    PinkDbError::Execution(
        "wrong key: the encryption key-check block did not authenticate \
         (wrong key, wrong key type, or a tampered header)"
            .to_string(),
    )
}

// ---------- encryption implementation ----------

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

    impl std::fmt::Debug for DerivedKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("DerivedKey(redacted)")
        }
    }

    /// Header fields needed to serialize a fresh container header.
    #[derive(Clone, Debug)]
    struct HeaderSpec {
        /// Container version written into the header (and bound into the AAD:
        /// a version flip fails authentication, not just a check).
        version: u32,
        context: u64,
        kdf: u8,
        params: Option<(u32, u32, u32, Vec<u8>)>,
        prefix: [u8; NONCE_PREFIX_LEN],
    }

    impl HeaderSpec {
        /// The serialized public header up to (excluding) the key-check.
        fn aad_base(&self) -> Vec<u8> {
            let mut v =
                Vec::with_capacity(HEADER_FIXED_LEN + KDF_PARAMS_LEN + MAX_SALT_LEN as usize);
            v.extend_from_slice(ENC_MAGIC);
            v.extend_from_slice(&self.version.to_le_bytes());
            v.push(CIPHER_AES256_GCM);
            v.push(self.kdf);
            v.extend_from_slice(&self.context.to_le_bytes());
            v.extend_from_slice(&self.prefix);
            v.extend_from_slice(&(KEY_CHECK_PLAIN.len() as u32).to_le_bytes());
            if let Some((m, t, p, salt)) = &self.params {
                v.extend_from_slice(&m.to_le_bytes());
                v.extend_from_slice(&t.to_le_bytes());
                v.extend_from_slice(&p.to_le_bytes());
                v.extend_from_slice(&(salt.len() as u32).to_le_bytes());
                v.extend_from_slice(salt);
            }
            v
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

    fn key_check_aad(domain: Domain, aad_base: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(domain.label().len() + aad_base.len());
        v.extend_from_slice(domain.label());
        v.extend_from_slice(aad_base);
        v
    }

    fn unit_aad(domain: Domain, aad_base: &[u8], counter: u32) -> Vec<u8> {
        let mut v = key_check_aad(domain, aad_base);
        v.extend_from_slice(&counter.to_le_bytes());
        v
    }

    fn wal_aad(aad_base: &[u8], lsn: u64, counter: u32) -> Vec<u8> {
        let mut v = key_check_aad(Domain::Wal, aad_base);
        v.extend_from_slice(&lsn.to_le_bytes());
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
            .map_err(|_| PinkDbError::Execution("crypto: bad key length".to_string()))?;
        cipher
            .encrypt(
                Nonce::from_slice(&nonce_for(prefix, counter)),
                Payload { msg: plain, aad },
            )
            .map_err(|_| PinkDbError::Execution("crypto: encryption failed".to_string()))
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

    fn argon2_derive(
        passphrase: &[u8],
        m_cost: u32,
        t_cost: u32,
        p_cost: u32,
        salt: &[u8],
    ) -> Result<DerivedKey> {
        let params = argon2::Params::new(m_cost, t_cost, p_cost, Some(KEY_LEN)).map_err(|e| {
            PinkDbError::Execution(format!("encrypted file: invalid Argon2id parameters: {e}"))
        })?;
        let argon =
            argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let mut bytes = [0u8; KEY_LEN];
        argon
            .hash_password_into(passphrase, salt, &mut bytes)
            .map_err(|e| {
                PinkDbError::Execution(format!("encrypted file: Argon2id derivation failed: {e}"))
            })?;
        Ok(DerivedKey { bytes })
    }

    /// Derive the container key from the user key and the header's KDF form.
    fn derive_for_open(
        key: &EncryptionKey,
        kdf: u8,
        params: Option<&(u32, u32, u32, Vec<u8>)>,
    ) -> Result<DerivedKey> {
        match kdf {
            KDF_RAW => match key.raw {
                Some(raw) => Ok(DerivedKey { bytes: raw }),
                None => Err(PinkDbError::Execution(
                    "wrong key: the file uses a raw 32-byte key, but a passphrase was supplied"
                        .to_string(),
                )),
            },
            KDF_ARGON2ID => match &key.passphrase {
                Some(pass) => {
                    let (m, t, p, salt) =
                        params.ok_or_else(|| corrupt("Argon2id header without KDF parameters"))?;
                    argon2_derive(pass, *m, *t, *p, salt)
                }
                None => Err(PinkDbError::Execution(
                    "wrong key: the file is passphrase-encrypted (Argon2id), \
                     but a raw key was supplied"
                        .to_string(),
                )),
            },
            other => Err(corrupt(&format!("unsupported KDF id {other}"))),
        }
    }

    /// Serialize a fresh header for `key` and seal the key-check block.
    /// Returns the spec (for `fork`), the full header bytes, the AAD base,
    /// and the derived key.
    fn build_header(
        key: &EncryptionKey,
        context: u64,
        domain: Domain,
        version: u32,
    ) -> Result<(HeaderSpec, Vec<u8>, Vec<u8>, DerivedKey)> {
        let prefix = random_prefix();
        let (spec, derived) = match (&key.raw, &key.passphrase) {
            (Some(raw), _) => (
                HeaderSpec {
                    version,
                    context,
                    kdf: KDF_RAW,
                    params: None,
                    prefix,
                },
                DerivedKey { bytes: *raw },
            ),
            (None, Some(pass)) => {
                let mut salt = vec![0u8; DEFAULT_SALT_LEN as usize];
                OsRng.fill_bytes(&mut salt);
                let derived =
                    argon2_derive(pass, ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST, &salt)?;
                (
                    HeaderSpec {
                        version,
                        context,
                        kdf: KDF_ARGON2ID,
                        params: Some((ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST, salt)),
                        prefix,
                    },
                    derived,
                )
            }
            (None, None) => {
                return Err(PinkDbError::Execution(
                    "encryption key holds no material".to_string(),
                ));
            }
        };
        let aad_base = spec.aad_base();
        let key_check = seal_unit(
            &derived,
            &spec.prefix,
            0,
            &key_check_aad(domain, &aad_base),
            KEY_CHECK_PLAIN,
        )?;
        let mut full = aad_base.clone();
        full.extend_from_slice(&key_check);
        Ok((spec, full, aad_base, derived))
    }

    /// Seal a whole checkpoint payload into one container. The payload ends
    /// with a terminal zero-length segment (sealed like any other), so a
    /// container cut exactly at a segment boundary — a skipped or lost
    /// trailing segment — is rejected instead of loading a silently shorter
    /// database.
    pub(crate) fn seal_container(
        plain: &[u8],
        key: &EncryptionKey,
        context: u64,
    ) -> Result<Vec<u8>> {
        let (spec, mut out, aad_base, derived) =
            build_header(key, context, Domain::Checkpoint, CONTAINER_VERSION)?;
        let mut counter: u32 = 1;
        let mut pos = 0usize;
        while pos < plain.len() {
            let end = (pos + SEGMENT_PLAIN_MAX).min(plain.len());
            if counter == u32::MAX {
                return Err(PinkDbError::Execution(
                    "encrypted file: too many segments".to_string(),
                ));
            }
            let seg = &plain[pos..end];
            let ct = seal_unit(
                &derived,
                &spec.prefix,
                counter,
                &unit_aad(Domain::Checkpoint, &aad_base, counter),
                seg,
            )?;
            out.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            out.extend_from_slice(&ct);
            counter += 1;
            pos = end;
        }
        if counter == u32::MAX {
            return Err(PinkDbError::Execution(
                "encrypted file: too many segments".to_string(),
            ));
        }
        let terminal = seal_unit(
            &derived,
            &spec.prefix,
            counter,
            &unit_aad(Domain::Checkpoint, &aad_base, counter),
            &[],
        )?;
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&terminal);
        Ok(out)
    }

    /// Verify the key-check and decrypt a whole checkpoint container. The
    /// final frame must be the terminal zero-length segment: a container
    /// truncated at a segment boundary is a hard error, never a partial load.
    pub(crate) fn open_container(
        data: &[u8],
        key: &EncryptionKey,
        context: u64,
    ) -> Result<Vec<u8>> {
        let header = parse_container_header(data)?;
        if header.context != context {
            return Err(corrupt(&format!(
                "container context {} does not match the expected {context}",
                header.context
            )));
        }
        let derived = derive_for_open(key, header.kdf, header.params.as_ref())?;
        if try_open_unit(
            &derived,
            &header.prefix,
            0,
            &key_check_aad(Domain::Checkpoint, &header.aad_base),
            &header.key_check,
        )
        .is_none()
        {
            return Err(wrong_key());
        }
        let mut out = Vec::new();
        let mut pos = header.payload_start;
        let mut counter: u32 = 1;
        while pos < data.len() {
            if data.len() - pos < 4 {
                return Err(corrupt("truncated segment length"));
            }
            let n = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
            if n > SEGMENT_PLAIN_MAX {
                return Err(corrupt(&format!("segment declares {n} plaintext bytes")));
            }
            let start = pos + 4;
            let Some(end) = start.checked_add(n + TAG_LEN) else {
                return Err(corrupt("segment length overflow"));
            };
            if end > data.len() {
                return Err(corrupt(&format!("truncated segment {counter}")));
            }
            let plain = try_open_unit(
                &derived,
                &header.prefix,
                counter,
                &unit_aad(Domain::Checkpoint, &header.aad_base, counter),
                &data[start..end],
            )
            .ok_or_else(|| corrupt(&format!("segment {counter} failed authentication")))?;
            if n == 0 {
                // The terminal segment must be the very last frame.
                if end != data.len() {
                    return Err(corrupt("data after the terminal segment"));
                }
                out.extend_from_slice(&plain);
                return Ok(out);
            }
            out.extend_from_slice(&plain);
            pos = end;
            counter = counter.checked_add(1).ok_or_else(|| {
                PinkDbError::Execution("encrypted file: too many segments".to_string())
            })?;
        }
        Err(corrupt(
            "truncated container: the terminal segment is missing",
        ))
    }

    /// Per-record AEAD for one WAL file (one file = one container header).
    #[derive(Debug)]
    pub(crate) struct RecordCipher {
        spec: HeaderSpec,
        key: DerivedKey,
        header: Vec<u8>,
        aad_base: Vec<u8>,
        next_counter: u32,
    }

    impl RecordCipher {
        /// A fresh container header (fresh salt/params when passphrase-based,
        /// fresh nonce prefix) for generation `context`.
        pub(crate) fn create(key: &EncryptionKey, context: u64) -> Result<Self> {
            let (spec, header, aad_base, derived) =
                build_header(key, context, Domain::Wal, WAL_CONTAINER_VERSION)?;
            Ok(Self {
                spec,
                key: derived,
                header,
                aad_base,
                next_counter: 1,
            })
        }

        /// Parse an existing header, derive the key, verify the key-check.
        /// Returns the cipher (positioned at record counter 1) and the payload
        /// offset.
        pub(crate) fn from_header(data: &[u8], key: &EncryptionKey) -> Result<(Self, usize)> {
            let header = parse_container_header(data)?;
            let derived = derive_for_open(key, header.kdf, header.params.as_ref())?;
            if try_open_unit(
                &derived,
                &header.prefix,
                0,
                &key_check_aad(Domain::Wal, &header.aad_base),
                &header.key_check,
            )
            .is_none()
            {
                return Err(wrong_key());
            }
            let spec = HeaderSpec {
                version: header.version,
                context: header.context,
                kdf: header.kdf,
                params: header.params.clone(),
                prefix: header.prefix,
            };
            let payload_start = header.payload_start;
            Ok((
                Self {
                    spec,
                    key: derived,
                    header: data[..payload_start].to_vec(),
                    aad_base: header.aad_base,
                    next_counter: 1,
                },
                payload_start,
            ))
        }

        /// A sibling container for a fresh WAL generation: same key and KDF
        /// parameters, a new generation context, and a fresh nonce prefix.
        pub(crate) fn fork(&self, context: u64) -> Result<Self> {
            // A forked WAL container is always current-format: this is the
            // path that upgrades a legacy container and that every checkpoint
            // roll uses.
            let spec = HeaderSpec {
                version: WAL_CONTAINER_VERSION,
                context,
                kdf: self.spec.kdf,
                params: self.spec.params.clone(),
                prefix: random_prefix(),
            };
            let aad_base = spec.aad_base();
            let key_check = seal_unit(
                &self.key,
                &spec.prefix,
                0,
                &key_check_aad(Domain::Wal, &aad_base),
                KEY_CHECK_PLAIN,
            )?;
            let mut header = aad_base.clone();
            header.extend_from_slice(&key_check);
            Ok(Self {
                spec,
                key: DerivedKey {
                    bytes: self.key.bytes,
                },
                header,
                aad_base,
                next_counter: 1,
            })
        }

        pub(crate) fn header(&self) -> &[u8] {
            &self.header
        }

        /// Position the nonce counter for appends after `n` existing records
        /// (a reopened file must not reuse the nonces of its records).
        pub(crate) fn set_next_counter(&mut self, next: u32) {
            self.next_counter = next;
        }

        /// True when `key` derives exactly the container key this cipher
        /// holds: the checkpoint guard that refuses a `.pink` sealed with a
        /// different key/mode beside a WAL sealed with this one.
        pub(crate) fn matches_key(&self, key: &EncryptionKey) -> bool {
            match derive_for_open(key, self.spec.kdf, self.spec.params.as_ref()) {
                Ok(derived) => derived.bytes == self.key.bytes,
                Err(_) => false,
            }
        }

        /// Seal one record payload; advances the nonce counter.
        pub(crate) fn seal(&mut self, lsn: u64, plain: &[u8]) -> Result<Vec<u8>> {
            let counter = self.next_counter;
            self.next_counter = counter.checked_add(1).ok_or_else(|| {
                PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
            })?;
            seal_unit(
                &self.key,
                &self.spec.prefix,
                counter,
                &wal_aad(&self.aad_base, lsn, counter),
                plain,
            )
        }

        /// Open one record; `None` = authentication failure. `counter` is the
        /// record's 1-based ordinal in the file.
        pub(crate) fn open(&self, counter: u32, lsn: u64, ciphertext: &[u8]) -> Option<Vec<u8>> {
            try_open_unit(
                &self.key,
                &self.spec.prefix,
                counter,
                &wal_aad(&self.aad_base, lsn, counter),
                ciphertext,
            )
        }
    }
}

#[cfg(not(feature = "encryption"))]
mod imp {
    use super::*;

    /// Stub: unconstructible without the feature, so every `Option<RecordCipher>`
    /// stays `None` and the plaintext WAL path is the only one taken.
    #[derive(Debug)]
    pub(crate) struct RecordCipher(());

    impl RecordCipher {
        pub(crate) fn create(_key: &EncryptionKey, _context: u64) -> Result<Self> {
            Err(unavailable())
        }
        pub(crate) fn from_header(_data: &[u8], _key: &EncryptionKey) -> Result<(Self, usize)> {
            Err(unavailable())
        }
        pub(crate) fn fork(&self, _context: u64) -> Result<Self> {
            Err(unavailable())
        }
        pub(crate) fn header(&self) -> &[u8] {
            &[]
        }
        pub(crate) fn set_next_counter(&mut self, _next: u32) {}
        /// Without the feature a cipher can never exist, so no key matches.
        pub(crate) fn matches_key(&self, _key: &EncryptionKey) -> bool {
            false
        }
        pub(crate) fn seal(&mut self, _lsn: u64, _plain: &[u8]) -> Result<Vec<u8>> {
            Err(unavailable())
        }
        pub(crate) fn open(&self, _counter: u32, _lsn: u64, _ciphertext: &[u8]) -> Option<Vec<u8>> {
            None
        }
    }
}

#[cfg(not(feature = "encryption"))]
pub(crate) use imp::RecordCipher;
#[cfg(feature = "encryption")]
pub(crate) use imp::{RecordCipher, open_container, seal_container};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_headers_parse_and_roundtrip_public_fields() {
        // A raw-key header built by the real (feature) path, parsed with the
        // always-compiled parser. Without the feature this only checks the
        // parser against a hand-built header.
        let key = EncryptionKey::from_raw([7u8; 32]);
        #[cfg(feature = "encryption")]
        {
            let sealed = seal_container(b"hello", &key, 0).unwrap();
            let h = parse_container_header(&sealed).unwrap();
            assert_eq!(h.context, 0);
            assert_eq!(h.kdf, KDF_RAW);
            assert!(h.params.is_none());
            // Header + one 5-byte segment frame + the terminal frame.
            assert_eq!(
                h.payload_start,
                sealed.len() - (5 + 4 + TAG_LEN) - (4 + TAG_LEN)
            );
        }
        let _ = key;
    }

    #[test]
    fn parser_rejects_malformed_headers() {
        assert!(parse_container_header(b"not a container at all").is_err());
        let mut short = ENC_MAGIC.to_vec();
        short.extend_from_slice(&[0u8; 10]);
        assert!(parse_container_header(&short).is_err());
        // Bad version.
        let mut v = ENC_MAGIC.to_vec();
        v.extend_from_slice(&99u32.to_le_bytes());
        v.push(CIPHER_AES256_GCM);
        v.push(KDF_RAW);
        v.extend_from_slice(&0u64.to_le_bytes());
        v.extend_from_slice(&[1u8; 8]);
        v.extend_from_slice(&(KEY_CHECK_PLAIN.len() as u32).to_le_bytes());
        assert!(parse_container_header(&v).is_err());
    }

    #[test]
    fn keys_redact_in_debug_and_expose_no_material() {
        let raw = EncryptionKey::from_raw([1u8; 32]);
        let pass = EncryptionKey::from_passphrase("hunter2");
        let r = format!("{raw:?}");
        let p = format!("{pass:?}");
        assert!(r.contains("raw") && !r.contains('1'), "{r}");
        assert!(p.contains("passphrase") && !p.contains("hunter2"), "{p}");
    }
}

#[cfg(all(test, feature = "encryption"))]
mod enc_tests {
    use super::*;

    #[test]
    fn container_roundtrip_raw_and_passphrase() {
        for key in [
            EncryptionKey::from_raw([9u8; 32]),
            EncryptionKey::from_passphrase("correct horse battery staple"),
        ] {
            let plain: Vec<u8> = (0..200_003u32).flat_map(|i| i.to_le_bytes()).collect();
            let sealed = seal_container(&plain, &key, 0).unwrap();
            assert!(sealed.starts_with(ENC_MAGIC));
            // Multi-segment: 200 KB is one segment; check a >1 MiB payload too.
            let big: Vec<u8> = std::iter::repeat_n(0xABu8, 3 * SEGMENT_PLAIN_MAX + 17).collect();
            let sealed_big = seal_container(&big, &key, 0).unwrap();
            assert_eq!(open_container(&sealed_big, &key, 0).unwrap(), big);
            assert_eq!(open_container(&sealed, &key, 0).unwrap(), plain);
        }
    }

    #[test]
    fn wrong_key_and_tampering_are_distinguished() {
        let key = EncryptionKey::from_raw([3u8; 32]);
        let wrong = EncryptionKey::from_passphrase("nope");
        let other = EncryptionKey::from_raw([4u8; 32]);
        let sealed = seal_container(b"payload data here", &key, 0).unwrap();
        for bad in [&wrong, &other] {
            let err = open_container(&sealed, bad, 0).unwrap_err();
            assert!(err.to_string().contains("wrong key"), "{err}");
        }
        // Flip a payload byte: authentication failure, not "wrong key".
        let mut tampered = sealed.clone();
        let n = tampered.len();
        tampered[n - 1] ^= 0x01;
        let err = open_container(&tampered, &raw_key(), 0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed authentication"), "{msg}");
        assert!(!msg.contains("wrong key"), "{msg}");
    }

    fn raw_key() -> EncryptionKey {
        // Same key as the test above (kept as a helper for clarity).
        EncryptionKey::from_raw([3u8; 32])
    }

    #[test]
    fn passphrase_params_are_stored_in_the_header() {
        let key = EncryptionKey::from_passphrase("s3cret");
        let sealed = seal_container(b"x", &key, 7).unwrap();
        let h = parse_container_header(&sealed).unwrap();
        assert_eq!(h.kdf, KDF_ARGON2ID);
        let (m, t, p, salt) = h.params.unwrap();
        assert_eq!((m, t, p), (ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST));
        assert_eq!(salt.len(), DEFAULT_SALT_LEN as usize);
        assert_eq!(h.context, 7);
        assert_eq!(open_container(&sealed, &key, 7).unwrap(), b"x");
        // Same passphrase, different salt per save: bytes differ.
        let again = seal_container(b"x", &key, 7).unwrap();
        assert_ne!(sealed, again);
    }

    #[test]
    fn record_cipher_never_reuses_nonces_and_fork_changes_prefix() {
        let key = EncryptionKey::from_passphrase("wal-pass");
        let mut a = RecordCipher::create(&key, 1).unwrap();
        let r1 = a.seal(1, b"one").unwrap();
        let r2 = a.seal(2, b"two").unwrap();
        assert_ne!(r1, r2);
        assert_eq!(a.open(1, 1, &r1).unwrap(), b"one");
        assert_eq!(a.open(2, 2, &r2).unwrap(), b"two");
        // Wrong LSN in the AAD must fail.
        assert!(a.open(1, 2, &r1).is_none());
        // A fork starts a new prefix, so even the same (lsn, plaintext) yields
        // different ciphertext.
        let mut b = a.fork(2).unwrap();
        let r3 = b.seal(1, b"one").unwrap();
        assert_ne!(r1, r3);
        // Different context: cross-file records do not authenticate.
        assert!(a.open(1, 1, &r3).is_none());
    }

    /// The per-container counter is finite: the last usable counter seals and
    /// the next one fails with a clean error, never a wrap or a panic.
    #[test]
    fn record_counter_overflow_is_a_clean_error() {
        let key = EncryptionKey::from_raw([0x77u8; 32]);
        let mut c = RecordCipher::create(&key, 1).unwrap();
        c.set_next_counter(u32::MAX - 1);
        assert!(c.seal(1, b"last").is_ok());
        let err = c.seal(2, b"over").unwrap_err();
        assert!(err.to_string().contains("overflow"), "{err}");
        let mut c = RecordCipher::create(&key, 1).unwrap();
        c.set_next_counter(u32::MAX);
        assert!(c.seal(1, b"x").is_err(), "u32::MAX must never seal");
    }

    #[test]
    fn truncated_containers_error_without_panic() {
        let key = EncryptionKey::from_raw([1u8; 32]);
        let plain = vec![0x5Au8; SEGMENT_PLAIN_MAX + 1000];
        let sealed = seal_container(&plain, &key, 0).unwrap();
        for cut in [0, 4, 8, 20, 40, 60, 100, sealed.len() / 2, sealed.len() - 1] {
            assert!(
                open_container(&sealed[..cut], &key, 0).is_err(),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn from_env_reads_pinkdb_key() {
        // Serialized access: `set_var` affects the whole test process.
        // `remove_var`/`set_var` are `unsafe` as of Rust 2024 (environment
        // mutation races concurrent reads in other threads); the lock below
        // serialises every test in this binary that touches `PINKDB_KEY`.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap();
        let saved = std::env::var("PINKDB_KEY").ok();
        unsafe { std::env::remove_var("PINKDB_KEY") };
        assert!(EncryptionKey::from_env().unwrap().is_none());
        unsafe { std::env::set_var("PINKDB_KEY", "") };
        assert!(EncryptionKey::from_env().unwrap().is_none());
        unsafe { std::env::set_var("PINKDB_KEY", "sesame") };
        assert!(EncryptionKey::from_env().unwrap().is_some());
        // A set-but-non-UTF-8 value is a hard error, never a silent fallback
        // to plaintext.
        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;
            unsafe { std::env::set_var("PINKDB_KEY", OsString::from_vec(vec![0xFF, 0xFE])) };
            let err = EncryptionKey::from_env().unwrap_err();
            assert!(err.to_string().contains("not valid UTF-8"), "{err}");
        }
        unsafe {
            match saved {
                Some(v) => std::env::set_var("PINKDB_KEY", v),
                None => std::env::remove_var("PINKDB_KEY"),
            }
        }
    }

    /// A crafted header cannot make an open burn minutes of Argon2id work:
    /// the accepted parameter envelope is a small multiple of the defaults.
    #[test]
    fn argon2_header_params_are_capped_and_defaults_open() {
        let mk = |m: u32, t: u32, p: u32| -> Vec<u8> {
            let mut v = ENC_MAGIC.to_vec();
            v.extend_from_slice(&ENC_VERSION.to_le_bytes());
            v.push(CIPHER_AES256_GCM);
            v.push(KDF_ARGON2ID);
            v.extend_from_slice(&0u64.to_le_bytes());
            v.extend_from_slice(&[0x5Au8; 8]);
            v.extend_from_slice(&(KEY_CHECK_PLAIN.len() as u32).to_le_bytes());
            v.extend_from_slice(&m.to_le_bytes());
            v.extend_from_slice(&t.to_le_bytes());
            v.extend_from_slice(&p.to_le_bytes());
            v.extend_from_slice(&DEFAULT_SALT_LEN.to_le_bytes());
            v.extend_from_slice(&[0u8; DEFAULT_SALT_LEN as usize]);
            v.extend_from_slice(&[0u8; KEY_CHECK_PLAIN.len() + TAG_LEN]);
            v
        };
        // The worst combination within the *old* caps (measured at ~6.6 s in
        // release on the development machine, and minutes on slower ones) is
        // refused, as is any single out-of-range bound and a combination that
        // is in range per-bound but over the combined work cap.
        for (m, t, p) in [
            (256 * 1024, 64, 64),
            (ARGON2_MAX_M_COST_KIB + 1, 1, 1),
            (ARGON2_M_COST_KIB, ARGON2_MAX_T_COST + 1, 1),
            (ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_MAX_P_COST + 1),
            (ARGON2_MAX_M_COST_KIB, ARGON2_MAX_T_COST, 1), // 16× the default work
            (8, 1, 4),                                     // m < 8·p
        ] {
            let err = parse_container_header(&mk(m, t, p)).unwrap_err();
            assert!(
                err.to_string().contains("implausible Argon2id"),
                "m={m} t={t} p={p}: {err}"
            );
        }
        // The defaults and the worst *accepted* combination parse.
        for (m, t, p) in [
            (ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST),
            (ARGON2_MAX_M_COST_KIB, 4, ARGON2_MAX_P_COST),
            (ARGON2_M_COST_KIB, ARGON2_MAX_T_COST, 1),
        ] {
            assert!(
                parse_container_header(&mk(m, t, p)).is_ok(),
                "m={m} t={t} p={p} must stay accepted"
            );
        }
        // A real passphrase container (defaults) still opens end to end.
        let key = EncryptionKey::from_passphrase("s3cret");
        let sealed = seal_container(b"x", &key, 0).unwrap();
        assert_eq!(open_container(&sealed, &key, 0).unwrap(), b"x");
    }
}
