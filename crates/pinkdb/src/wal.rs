//! Write-ahead log: durable commits without changing the `.pink` format.
//!
//! For `<path>.pink` the log is a sidecar `<path>.pink.wal`:
//!
//! ```text
//! header:  magic "PINKWAL1" (8 bytes) + format version u32 (=4)
//!          + generation u64 + wal_id [16] + crc32(0..36) u32
//!          + commit slot A (24 bytes) + commit slot B (24 bytes)
//!          + reserved zero u32×2
//! record:  payload_len u32 | lsn u64 | crc32(payload) u32 | payload
//! payload: stmt_count u32 | sets_len u32 | sets | per statement: len u32 + UTF-8 SQL text
//! sets:    format u8 (=1) | table_count u32 | (db_len u16 + db, name_len u16 + name)*
//!          | db_count u32 | (db_len u16 + db)*
//! ```
//!
//! Version 3 (wave 45m) adds `wal_id` (a fresh 16-byte incarnation id per
//! file creation, the ABA guard across checkpoints) and the dual 24-byte
//! commit slots that publish the committed LSN. Version 4 (wave 45o) adds
//! the frame's **touched set** ([`FrameSets`]): every catalog table the
//! frame's statements read or wrote (plus databases its `CREATE/DROP
//! DATABASE` names), cleared to `sets_len = u32::MAX` for legacy records an
//! upgrade adopted. Version 2/3 WALs are accepted and upgraded in place under
//! the write lease (records are re-encoded with unknown sets); older builds
//! refuse version 4 cleanly through the `BadVersion` path before reading any
//! record. Encrypted **WAL** containers are version 3; encrypted checkpoints
//! stay at container version 2, so `.pink` bytes are unchanged by this wave.
//!
//! # Multi-process access (wave 45m) and optimistic commits (wave 45o)
//!
//! Any number of local processes may open a durable database. Mutual
//! exclusion is a per-operation **write lease**: an exclusive `flock` on
//! `<path>.pink.lock` taken for the mutation spans (append/fsync/publish,
//! whole checkpoints, recovery, refresh) or just `[refresh]` (readers whose
//! lock-free epoch precheck says the on-disk state advanced). Since wave 45o
//! the lease is **not** held across statement or transaction *execution*:
//! an autocommit write executes without it and takes the lease only for
//! `[validate → apply others' frames → append → publish]`, and `BEGIN` takes
//! it only for the initial refresh. Inside one process a refcounted gate
//! shares the fd's lock, because `flock` locks are per open file
//! description; in-process mutations stay serialized by the store lock and
//! the Wal mutex exactly as before. The lock
//! file's first 48 bytes carry two epoch slots (dual slots + CRC); every lease
//! holder that changed something a reader must notice bumps one slot, and
//! readers compare a cached value with one `pread` (a read error or an
//! absent/torn pair counts as changed). The lock file must never be deleted
//! or replaced while processes have the database open: `flock` cannot protect
//! a name that is unlinked and recreated, so every acquisition (and every WAL
//! mutation boundary) re-checks that the locked fd is still the file named at
//! the lock path and fails closed otherwise (see [`Lease`]).
//!
//! Every process keeps a full in-memory store with an applied position
//! (`wal_id`, generation, WAL file identity, byte/record offsets, checkpoint
//! identity). Refresh (design section 4) replays only the frames past the
//! applied position, with the WAL handle detached (never re-logging), the
//! WAL *lineage session* restored around the replay, and the local session
//! restored afterwards; a changed WAL incarnation or checkpoint identity
//! triggers a full reload of the checkpoint plus a replay of the live WAL.
//! Checkpoints refresh first under the lease, so a checkpoint can never
//! publish a state that omits another process's committed records.
//!
//! **Commit validation (wave 45o).** A unit committing at base
//! `(wal_id, generation, applied LSN, checkpoint identity)` may commit iff
//! the incarnation is unchanged and **no frame recorded after the base
//! touches a table the unit read or wrote** (frames carry their touched set,
//! so validation never has to re-bind another writer's SQL against a store
//! that may hold uncommitted state). The applied journal
//! (a pruned `Vec` of `(lsn, touched set)` entries, emptied when nothing is
//! open) records the sets of frames this process applied or appended since
//! the last full reload; the pairs written by others are scanned from the
//! applied position. A conflict names
//! the object and the frame's LSN — the WAL position is the cross-process
//! "version": `DataTable::version` is process-local and never persisted, so
//! it cannot be compared across writers. Clean commits apply the frames that
//! arrived for other tables, then append; conflicts leave the store exactly
//! as it was (the caller aborts the transaction, or rebuilds and
//! re-executes an autocommit statement under the held lease).
//!
//! One record = one committed transaction unit: an autocommit statement logs
//! individually; `BEGIN … COMMIT` logs all of the transaction's mutating
//! statements as one record at `COMMIT` (a `ROLLBACK`, or a write-conflict
//! abort, logs nothing). Replay is logical: the recorded SQL is re-parsed and
//! re-executed, which is deterministic in v1 (no random()/now()). Because
//! replay re-executes SQL text, a durable commit is refused when its bound
//! plan depends on a session-local view (views are never persisted, so replay
//! could not rebuild it) — and a writing transaction's reads participate in
//! commit validation for the same reason.
//!
//! Durability policy ([`SyncPolicy`]): `Sync` fsyncs after every record,
//! `Batch` leaves records in the page cache and fsyncs on
//! [`PinkDb::flush_wal`] / checkpoint. Recovery stops at the first record
//! that is short, fails its CRC, or does not continue the strictly increasing
//! LSN sequence (the standard torn-tail / framing-corruption case) and
//! truncates the file there; a CRC-valid record that fails to parse, bind, or
//! execute is a hard error — committed data is never silently dropped.
//!
//! # Checkpoints and the recovery state machine
//!
//! Checkpointing persists the in-memory state atomically and rolls the WAL
//! generation. Every generation is named, and the generation lives in the WAL
//! header, so no artifact can be confused with one from another generation:
//!
//! ```text
//! <path>.pink              the published checkpoint
//! <path>.pink.tmp          checkpoint being written (never trusted when live records exist)
//! <path>.pink.wal            live WAL, the header names its generation
//! <path>.pink.wal.<gen>.roll   a rolled-away generation (name == header generation)
//! ```
//!
//! The protocol (every step ordered so recovery can tell crash windows apart):
//!
//! 1. write `.pink.tmp` (full state) and fsync the file *and its directory*;
//! 2. rename the live WAL to `.wal.<gen>.roll` (the name carries the
//!    generation, so a roll can never overwrite another roll) and create a
//!    fresh header-only WAL with the generation bumped by one, fsynced;
//! 3. rename `.pink.tmp` → `.pink` (atomic publish), fsync the directory;
//! 4. remove the roll.
//!
//! Invariant: the in-memory state at step 1 is `pink + every record in the
//! live WAL`, so a published `.tmp` always contains the records of the roll
//! that step 2 creates. Recovery therefore **publishes `.tmp` before removing
//! anything** — replaying a roll into memory and deleting its files would
//! leave the next open with no durable copy (the bug this state machine
//! fixes). Rules, given the live WAL header generation `L` and the on-disk
//! pieces:
//!
//! - `.tmp` exists **and the live WAL has intact records** (`L` known): the
//!   checkpoint never reached its roll and `.tmp` may even be a partial
//!   write — discard it, the live WAL is authoritative. Rolls, if any, are
//!   older generations already folded into `.pink` (their generation must be
//!   `< L`, otherwise recovery refuses rather than guess); remove them.
//! - `.tmp` exists, live WAL empty/absent/torn, and a roll exists: step 1
//!   completed before step 2 (the roll only appears after `.tmp` was fully
//!   written and fsynced, and its records are inside `.tmp`), so publish
//!   `.tmp` → `.pink` *first*, then remove the rolls. A crash anywhere in
//!   between re-runs recovery from the same on-disk state.
//! - `.tmp` exists, live WAL empty/absent/torn, no roll: crash during step 1
//!   (or a redundant `.tmp`) — `.tmp` is not provably complete, so discard
//!   it and keep/recreate the live WAL.
//! - no `.tmp`: either no checkpoint was in flight or its publish completed.
//!   Every surviving roll is therefore folded into `.pink`; the checkpoint
//!   must exist and (when the live header is readable) each roll generation
//!   must be `< L`. Otherwise recovery refuses instead of deleting records
//!   that may still be unfolded. Unremoved rolls are harmless: the same
//!   decision is reached on the next open.
//!
//! When the live WAL is missing or has a torn header, recovery recreates it
//! with generation `max(roll generations) + 1` so the sequence keeps moving
//! forward. Recovery is idempotent: every decision only depends on files that
//! a re-run re-inspects, and the publish is a single atomic rename.
//!
//! A WAL file whose header is invalid is only recreated when it is at most
//! one header long (a torn header from `create`); invalid magic/version on a
//! file that has payload bytes is a hard error. The pre-generation roll name
//! `<path>.pink.wal.roll` is refused: generations are what make a stale roll
//! distinguishable from a current one, so an unnamed roll cannot be
//! classified safely.
//!
//! # Encrypted WALs (feature `encryption`)
//!
//! With a key, every WAL file is a `PINKENC1` container (see [`crate::crypto`])
//! whose `context` field is the generation, followed by records of the same
//! `len | lsn | crc | body` framing, where `body` is the AEAD ciphertext of the
//! payload (the crc field is zero and unused — GCM authentication replaces it,
//! and a record that authenticates with a nonzero crc is refused as framing
//! tampering). Each record is one AEAD unit with the LSN and the record ordinal
//! bound into the AAD, and a per-file nonce prefix. Recovery policy (wave 22) is
//! preserved:
//!
//! - a short/torn tail (framing or ciphertext cut short) stops the scan and
//!   truncates at the last good record;
//! - a complete record that fails authentication is a hard error when more
//!   bytes follow (mid-file tampering), but at the very tail it is
//!   indistinguishable from a torn write, so the tail policy applies;
//! - a successfully decrypted record whose payload framing is broken is a
//!   hard error, exactly like a CRC-valid plaintext record.
//!
//! Truncation of an encrypted WAL never reuses a counter: a keyed open
//! rebuilds the file with a fresh random nonce prefix (re-sealing every
//! intact record, atomically via a temp file) before any append continues.
//! Reusing the old prefix after a tail loss would reuse the nonce of a
//! discarded record, which GCM forbids — and a file cut exactly at a record
//! boundary is indistinguishable from a cleanly shorter one, so the rebuild
//! happens on every keyed open, not only when trailing bytes were discarded.
//! A keyed open requires every artifact to be encrypted, and a plaintext
//! open refuses encrypted ones, so a key can never be silently ignored.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::crypto::{self, EncryptionKey, RecordCipher, TAG_LEN};
use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result};

pub const WAL_MAGIC: &[u8; 8] = b"PINKWAL1";
/// On-disk format version. Version 2 added the generation to the header;
/// version 3 (wave 45m) adds `wal_id` and the dual commit slots; version 4
/// (wave 45o) extends the record payload with the frame's touched-object set
/// (see [`FrameSets`]), which is what optimistic multi-writer commits
/// validate against. Every durable open rewrites legacy files in place under
/// the write lease; an older build refuses a v4 file with a clean
/// version error before it reads any record.
pub const WAL_VERSION: u32 = 4;
/// The wave-45m plaintext version (no per-frame sets): upgraded at open.
const WAL_VERSION_V3: u32 = 3;
/// The pre-multi-process plaintext version, accepted and upgraded at open.
const WAL_VERSION_V2: u32 = 2;
/// v3/v4 plaintext header: magic(8) + version(4) + generation(8) + wal_id(16) +
/// crc32(0..36)(4) + slot A(24) + slot B(24) + reserved(8).
pub(crate) const HEADER_LEN: usize = 96;
/// Where records begin in a v3/v4 plaintext WAL (== `HEADER_LEN`).
pub(crate) const RECORDS_START: usize = HEADER_LEN;
/// Where records begin in a v2 plaintext WAL (records are re-encoded by the
/// upgrade).
const V2_RECORDS_START: usize = 20;
const RECORD_HEADER_LEN: usize = 16;
const ROLL_SUFFIX: &str = ".roll";

/// A publication slot (24 bytes; dual slots + CRC make a torn or lost update
/// harmless): `seq u64 | value u64 | crc32(seq||value||domain) u32 | zero u32`.
const SLOT_LEN: usize = 24;
/// The lock file's epoch region: slot A at 0..24, slot B at 24..48.
const LOCK_SLOT_A: u64 = 0;
/// The plaintext v3 header's commit slots (A at 40..64, B at 64..88; the
/// slot chosen for a write follows the seq parity in `write_slot`).
#[cfg_attr(not(test), allow(dead_code))]
const WAL_SLOT_B: u64 = 64;
const WAL_SLOT_A: u64 = 40;
/// The authenticated publish block of a current (container v4) WAL at
/// `payload_start` (L-5):
///
/// ```text
/// meta:  wal_id [16] | generation u64 | block version u16 | reserved u16  (28)
/// meta-tag: nonce [12] | tag [16]                                        (28)
/// slot A: seq u64 | value u64 | nonce [12] | tag [16]                    (44)
/// slot B: seq u64 | value u64 | nonce [12] | tag [16]                    (44)
/// ```
///
/// `meta` and each slot are authenticated as their own AEAD units under the
/// WAL's data key: `meta` is static (it identifies the file), while the slots
/// mutate on every publish, so each slot carries a fresh random 96-bit nonce
/// and a tag over `(seq, value)`. Tampering with the meta is a hard recovery
/// error; a slot whose tag does not verify is ignored (it is only a commit
/// hint, and the records are authoritative).
const ENC_PUBLISH_BLOCK_LEN: usize = 144;
const ENC_PUBLISH_META_LEN: usize = 28;
/// The static meta tag follows the meta: nonce [12] then tag [16].
const ENC_PUBLISH_META_TAG_END: usize = ENC_PUBLISH_META_LEN + 12 + 16;
const ENC_PUBLISH_SLOT_LEN: usize = 44;
const ENC_PUBLISH_BLOCK_VERSION: u16 = 2;
const ENC_SLOT_A: u64 = 56;
const ENC_SLOT_B: u64 = 100;

/// The pre-v4 publish block (`wal_id | crc | reserved | slot A | slot B |
/// reserved`), read only long enough to upgrade the file.
#[cfg(feature = "encryption")]
const LEGACY_ENC_PUBLISH_BLOCK_LEN: usize = 88;
#[cfg(feature = "encryption")]
const LEGACY_ENC_SLOT_A: u64 = 24;
#[cfg(feature = "encryption")]
const LEGACY_ENC_SLOT_B: u64 = 48;

/// CRC domains keep a lock-file slot from verifying as a WAL slot.
const LOCK_SLOT_DOMAIN: &[u8] = b"pinkdb-lock-epoch-v1";
const WAL_SLOT_DOMAIN: &[u8] = b"pinkdb-wal-slot-v1";
/// A fresh WAL incarnation id: fresh on every file creation, so no ABA is
/// possible across checkpoints or recovery.
pub(crate) type WalId = [u8; 16];

// Test-only switch: force `try_lock` to report `Unsupported`, so the
// fail-closed path is testable on filesystems that do support locking.
// Thread-local: tests run in parallel threads of one process.
#[cfg(test)]
thread_local! {
    static FORCE_UNSUPPORTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn force_unsupported() -> bool {
    FORCE_UNSUPPORTED.with(|f| f.get())
}

#[cfg(not(test))]
fn force_unsupported() -> bool {
    false
}

/// A fresh 16-byte WAL incarnation id. Not cryptographic: it only has to
/// never repeat across file (re)creations. `RandomState` seeds from the OS;
/// the time and a process-local counter make collisions impossible in
/// practice even if two instances were seeded identically.
fn fresh_wal_id() -> WalId {
    use std::hash::{BuildHasher, Hasher};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let state = std::collections::hash_map::RandomState::new();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut a = state.build_hasher();
    a.write_u64(0x9E37_79B9_7F4A_7C15);
    a.write_u64(SEQ.fetch_add(1, Ordering::Relaxed));
    a.write_u64(std::process::id() as u64);
    a.write_u64(nanos);
    let x = a.finish();
    let mut b = state.build_hasher();
    b.write_u64(x);
    b.write_u64(!nanos);
    let y = b.finish();
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&x.to_le_bytes());
    id[8..].copy_from_slice(&y.to_le_bytes());
    id
}

/// Encode one publication slot; `Ok(crc)` is implied by construction.
fn slot_bytes(seq: u64, value: u64, domain: &[u8]) -> [u8; SLOT_LEN] {
    let mut b = [0u8; SLOT_LEN];
    b[..8].copy_from_slice(&seq.to_le_bytes());
    b[8..16].copy_from_slice(&value.to_le_bytes());
    let mut crc_input = Vec::with_capacity(16 + domain.len());
    crc_input.extend_from_slice(&seq.to_le_bytes());
    crc_input.extend_from_slice(&value.to_le_bytes());
    crc_input.extend_from_slice(domain);
    b[16..20].copy_from_slice(&crc32(&crc_input).to_le_bytes());
    b
}

/// Parse one slot: `Some((seq, value))` only when the CRC verifies and the
/// reserved word is zero.
fn parse_slot(buf: &[u8], domain: &[u8]) -> Option<(u64, u64)> {
    if buf.len() < SLOT_LEN || buf[20..24] != [0u8; 4] {
        return None;
    }
    let seq = u64::from_le_bytes(buf[..8].try_into().expect("8 bytes"));
    let value = u64::from_le_bytes(buf[8..16].try_into().expect("8 bytes"));
    let crc = u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes"));
    let mut crc_input = Vec::with_capacity(16 + domain.len());
    crc_input.extend_from_slice(&seq.to_le_bytes());
    crc_input.extend_from_slice(&value.to_le_bytes());
    crc_input.extend_from_slice(domain);
    (crc32(&crc_input) == crc).then_some((seq, value))
}

/// The highest valid slot seq in `buf` at `base` (0 when neither verifies).
fn slot_max_in(buf: &[u8], base: usize, domain: &[u8]) -> u64 {
    let mut max = 0u64;
    for off in [base, base + SLOT_LEN] {
        if let Some(slot) = buf
            .get(off..off + SLOT_LEN)
            .and_then(|s| parse_slot(s, domain))
        {
            max = max.max(slot.0);
        }
    }
    max
}

// ---------- positional file I/O (pread/pwrite; no shared offset) ----------

#[cfg(unix)]
fn file_read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn file_read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn file_read_at(_file: &File, _buf: &mut [u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positional file reads are not supported on this platform",
    ))
}

#[cfg(unix)]
fn file_write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

#[cfg(windows)]
fn file_write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn file_write_at(_file: &File, _buf: &[u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positional file writes are not supported on this platform",
    ))
}

fn file_read_exact_at(file: &File, buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    let mut pos = 0usize;
    while pos < buf.len() {
        match file_read_at(file, &mut buf[pos..], offset)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short positional read",
                ));
            }
            n => {
                pos += n;
                offset += n as u64;
            }
        }
    }
    Ok(())
}

fn file_write_all_at(file: &File, buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    let mut pos = 0usize;
    while pos < buf.len() {
        match file_write_at(file, &buf[pos..], offset)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "positional write wrote zero bytes",
                ));
            }
            n => {
                pos += n;
                offset += n as u64;
            }
        }
    }
    Ok(())
}

/// Highest valid epoch seq in a region holding two slots at `base`; a short
/// read (a fresh, still zero-length lock file) means "no valid slot" = 0.
fn read_slot_max(file: &File, base: u64, domain: &[u8]) -> std::io::Result<u64> {
    let mut buf = [0u8; 2 * SLOT_LEN];
    let n = file_read_at(file, &mut buf, base)?;
    Ok(slot_max_in(&buf[..n], 0, domain))
}

/// Write the next publication slot (alternating A/B by seq parity).
fn write_slot(file: &File, base: u64, seq: u64, value: u64, domain: &[u8]) -> std::io::Result<()> {
    let offset = base
        + if seq.is_multiple_of(2) {
            SLOT_LEN as u64
        } else {
            0
        };
    file_write_all_at(file, &slot_bytes(seq, value, domain), offset)
}

/// Warn once per process about a best-effort hint write failure (design 5.3:
/// slots are hints; the frames stay authoritative and the next lease holder
/// republishes).
fn warn_slot_once(what: &str) {
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Ordering::Relaxed) {
        eprintln!("pinkdb: warning: could not publish a {what} (hint only; readers may lag)");
    }
}

/// The immutable identity of an on-disk file incarnation: `(dev, ino)` where
/// available, else presence (non-Unix platforms). Used to detect a replaced
/// WAL or checkpoint.
#[cfg(unix)]
fn identity_of(m: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (m.dev(), m.ino())
}

#[cfg(not(unix))]
fn identity_of(_m: &std::fs::Metadata) -> (u64, u64) {
    (0, 0)
}

fn file_identity(path: &Path) -> Option<(u64, u64)> {
    std::fs::metadata(path).ok().map(|m| identity_of(&m))
}

// ---------- the write lease ----------

/// How durable opens coordinate with other processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LockMode {
    /// Take the exclusive `flock` lease for every operation; fail closed on
    /// filesystems without locking (the default).
    #[default]
    Required,
    /// Restore today's single-process assumption explicitly: in-process
    /// serialization stays, but a filesystem without `flock` (or a network
    /// filesystem with unreliable locking) is used without coordination.
    /// Two processes opening such a database can corrupt it.
    UnsafeSingleProcess,
}

/// Options for a durable open beyond the sync policy.
#[derive(Debug, Clone)]
pub struct DurableOpenOptions {
    pub policy: SyncPolicy,
    /// How long an operation waits for another process's lease before
    /// reporting the database busy (default 5 s). A short timeout is useful
    /// for hosts that want to retry themselves.
    pub busy_timeout: Duration,
    pub lock_mode: LockMode,
    /// Whether WAL replay may load an extension the WAL records (default
    /// **false**).
    ///
    /// A durable store that used a native extension no longer loads it when
    /// it is opened: the recorded path is never `dlopen`ed on the file's
    /// say-so. With this on, the recorded path is still only honoured when
    /// it resolves inside `extension_root` — an extension that is already
    /// loaded in this process is reused as it is, and anything else fails
    /// the open with an actionable error.
    pub allow_extension_replay: bool,
    /// The directory extension loads are confined to (`None` = no root).
    /// A recorded path outside the root is refused.
    pub extension_root: Option<PathBuf>,
}

impl Default for DurableOpenOptions {
    fn default() -> Self {
        Self {
            policy: SyncPolicy::Sync,
            busy_timeout: Duration::from_secs(5),
            lock_mode: LockMode::Required,
            allow_extension_replay: false,
            extension_root: None,
        }
    }
}

/// In-process lease gate: `flock` locks belong to the open file description,
/// so every thread of one process shares the fd's lock (`try_lock` from a
/// second thread succeeds silently). The gate is therefore a process-wide
/// *refcount*: the flock is taken on the 0→1 transition and released at
/// 1→0, and re-entrant/nested or overlapping acquisitions (a `CHECKPOINT`
/// reached through `exec::run` while the autocommit path already holds the
/// lease; two connections each holding a transaction lease on one thread)
/// simply increment it. In-process mutual exclusion of store *mutations* is
/// provided by the store write lock and the Wal mutex, exactly as before
/// this wave; the lease adds the cross-process guarantee on top.
#[derive(Debug, Default)]
struct Gate {
    /// Number of outstanding guards in this process.
    count: Mutex<u32>,
}

impl Gate {
    fn enter(&self) -> std::sync::MutexGuard<'_, u32> {
        self.count.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// The per-operation write lease of one durable store: a dedicated fd on
/// `<path>.pink.lock` (created if missing, never deleted — its first 48 bytes
/// carry the epoch), the in-process gate, the busy timeout, the lock mode,
/// and the epoch cache. One `Lease` per durable open; every clone of the
/// store and its transactions share it.
///
/// # Lock-file identity (deletion/replacement detection)
///
/// `flock` is per open file description, so *deleting or replacing*
/// `<path>.pink.lock` while processes hold leases makes two different inodes
/// answer to the same name: a process holding the old inode and a process
/// locking the new one can both believe they hold "the write lease" and
/// mutate `.pink`/`.pink.wal` concurrently. The lock file is documented as
/// never deleted, but external interference must fail closed instead of
/// corrupting. After every acquisition (and on every wait iteration) the
/// lease `stat`s the path and compares `(dev, ino)` with the locked fd:
/// a mismatch refuses the operation with a clear "lock file was deleted or
/// replaced" error, and the WAL re-checks at each mutation boundary
/// (`append`, `flush`, `checkpoint`, refresh) before writing. A process on
/// the *new* inode cannot see the old holders; the residual window is the
/// time between one of these checks and the file write it guards (and the
/// reads of a process that never takes the lease again). The check is
/// Unix-only (`dev`/`ino`); elsewhere it is a no-op.
#[derive(Debug)]
pub(crate) struct Lease {
    file: File,
    /// `<checkpoint>.pink.lock`: compared with the fd's identity by
    /// [`Lease::verify_lock_identity`].
    lock_path: PathBuf,
    checkpoint_path: PathBuf,
    gate: Gate,
    mode: LockMode,
    busy_timeout_ms: AtomicU64,
    /// The seq this process last wrote or observed while refreshing; the
    /// fast-path precheck compares it with one pread.
    epoch_cache: AtomicU64,
    /// The next epoch seq this process will write; synced from the slots on
    /// every outermost acquisition so concurrent writers never regress seq.
    next_epoch_seq: AtomicU64,
    /// Test-only fast-path evidence (wave 45n gap d): statement-boundary
    /// prechecks, how many of them skipped the refresh because the cached
    /// epoch was unchanged, and how many entered a refresh pass. Per handle,
    /// so parallel tests cannot interfere.
    #[cfg(test)]
    prechecks: AtomicU64,
    #[cfg(test)]
    fastpath_skips: AtomicU64,
    #[cfg(test)]
    refresh_passes: AtomicU64,
}

/// A held write lease. Drop releases the flock when the last in-process
/// holder lets go (1→0).
#[derive(Debug)]
pub(crate) struct LeaseGuard {
    lease: Arc<Lease>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        let mut count = self.lease.gate.enter();
        debug_assert!(*count > 0, "unbalanced lease release");
        *count -= 1;
        if *count == 0 {
            let _ = self.lease.file.unlock();
        }
    }
}

impl Lease {
    fn open(checkpoint: &Path, mode: LockMode, busy_timeout: Duration) -> Result<Arc<Lease>> {
        let lock_path = lock_path(checkpoint);
        // M-5: owner-only, and narrow a pre-existing 0644 lock file.
        crate::fsutil::tighten(&lock_path);
        let file = crate::fsutil::open_rw_private(&lock_path, false)?;
        let ms = busy_timeout.as_millis().min(u64::MAX as u128) as u64;
        Ok(Arc::new(Lease {
            file,
            lock_path,
            checkpoint_path: checkpoint.to_path_buf(),
            gate: Gate::default(),
            mode,
            busy_timeout_ms: AtomicU64::new(ms),
            epoch_cache: AtomicU64::new(0),
            next_epoch_seq: AtomicU64::new(1),
            #[cfg(test)]
            prechecks: AtomicU64::new(0),
            #[cfg(test)]
            fastpath_skips: AtomicU64::new(0),
            #[cfg(test)]
            refresh_passes: AtomicU64::new(0),
        }))
    }

    pub(crate) fn set_busy_timeout(&self, timeout: Duration) {
        let ms = timeout.as_millis().min(u64::MAX as u128) as u64;
        self.busy_timeout_ms.store(ms, Ordering::Relaxed);
    }

    fn busy_timeout(&self) -> Duration {
        Duration::from_millis(self.busy_timeout_ms.load(Ordering::Relaxed))
    }

    /// Acquire the lease. On the process's 0→1 transition, take the
    /// exclusive flock with the busy timeout (the in-process state mutex is
    /// held across that wait, so local threads queue instead of racing and
    /// skipping the flock); overlapping acquisitions just share it. On
    /// timeout the report names both causes: a process busy
    /// writing/checkpointing, and an older pinkdb build (which holds the
    /// same lock for its whole lifetime). The locked fd must still be the
    /// file named at the lock path, or the acquisition is refused (see
    /// [`Lease::verify_lock_identity`]).
    pub(crate) fn acquire(self: &Arc<Self>) -> Result<LeaseGuard> {
        let mut count = self.gate.enter();
        if *count > 0 {
            // The process already holds the flock through this fd: sharing
            // it needs no syscall. Still re-check the path identity so a
            // replacement during a held lease (transaction, checkpoint)
            // fails closed at the next acquisition too.
            self.verify_lock_identity()?;
            *count += 1;
            return Ok(LeaseGuard {
                lease: self.clone(),
            });
        }
        let deadline = Instant::now() + self.busy_timeout();
        loop {
            let attempt = if force_unsupported() {
                Err(std::fs::TryLockError::Error(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "locking forced unsupported by a test switch",
                )))
            } else {
                self.file.try_lock()
            };
            match attempt {
                Ok(()) => {
                    // The flock succeeded on *this fd*. If another process
                    // deleted/replaced the lock file while we were opening
                    // or waiting, this fd no longer answers to the path
                    // name: fail closed instead of becoming a second
                    // writer on a stray inode.
                    if let Err(e) = self.verify_lock_identity() {
                        let _ = self.file.unlock();
                        return Err(e);
                    }
                    break;
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    // Waiting on a lock that is no longer the named lock
                    // file can never succeed usefully (the holder is a
                    // stray): detect the replacement instead of burning the
                    // full timeout.
                    self.verify_lock_identity()?;
                    if Instant::now() >= deadline {
                        return Err(self.busy_error());
                    }
                    // 2 ms steps: the first window also absorbs
                    // `Command::spawn`'s brief fd/OFD duplicate in a forked
                    // child before exec closes it.
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(std::fs::TryLockError::Error(e))
                    if e.kind() == std::io::ErrorKind::Unsupported =>
                {
                    if self.mode == LockMode::UnsafeSingleProcess {
                        break;
                    }
                    return Err(PinkDbError::Execution(format!(
                        "file locking is not supported on the filesystem holding {}; durable \
                         multi-process access would be unsafe (open with \
                         LockMode::UnsafeSingleProcess only if you know a single process will \
                         use it)",
                        self.checkpoint_path.display()
                    )));
                }
                Err(std::fs::TryLockError::Error(e)) => {
                    return Err(PinkDbError::Io(e));
                }
            }
        }
        *count = 1;
        // Sync the epoch counter while the lease is held: every writer's
        // bumps are visible in the slots under the lease. (Overlapping
        // holders skip this: the first holder synced, and every bump under
        // the lease keeps the counter current.)
        let current = read_slot_max(&self.file, LOCK_SLOT_A, LOCK_SLOT_DOMAIN).unwrap_or(0);
        self.next_epoch_seq
            .store(current.saturating_add(1).max(1), Ordering::Relaxed);
        Ok(LeaseGuard {
            lease: self.clone(),
        })
    }

    /// Is the locked fd still the file currently named at the lock path?
    /// `Ok(false)` on a platform without `(dev, ino)` identity (the check is
    /// a no-op there), or when the path is missing; `Err` on an unexpected
    /// stat failure.
    fn identity_current(&self) -> Result<bool> {
        #[cfg(unix)]
        {
            let fd_meta = self.file.metadata().map_err(PinkDbError::Io)?;
            match std::fs::metadata(&self.lock_path) {
                Ok(path_meta) => Ok(identity_of(&fd_meta) == identity_of(&path_meta)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(PinkDbError::Io(e)),
            }
        }
        #[cfg(not(unix))]
        {
            Ok(true)
        }
    }

    /// Refuse when the lock file was deleted or replaced under this lease
    /// (see the type docs). Called after every acquisition, while waiting
    /// for the flock, and at every WAL mutation boundary.
    pub(crate) fn verify_lock_identity(&self) -> Result<()> {
        if self.identity_current()? {
            return Ok(());
        }
        Err(PinkDbError::Execution(format!(
            "the lock file {} was deleted or replaced while this process had the database open; \
             another process could hold the write lease on a different file, so this operation \
             was refused. Close and reopen the database (never delete or replace a .pink.lock \
             while a database is open)",
            self.lock_path.display()
        )))
    }

    /// The busy error is an `Execution` with a stable
    /// `database <path> is busy:` prefix. Wave 45n reviewed adding a typed
    /// `PinkDbError::Busy` variant and deliberately did not: `PinkDbError`
    /// is a public enum without `#[non_exhaustive]`, so a new variant is a
    /// breaking change for downstream exhaustive matches. Hosts that want
    /// to retry can match the prefix; promoting this to a variant is an
    /// explicit release-note decision.
    fn busy_error(&self) -> PinkDbError {
        PinkDbError::Execution(format!(
            "database {} is busy: another process has held the write lock for {} ms \
             (an older pinkdb build holds this lock for its whole lifetime); retry later or \
             raise the busy timeout",
            self.checkpoint_path.display(),
            self.busy_timeout_ms.load(Ordering::Relaxed),
        ))
    }

    /// Highest valid epoch seq on disk (`None` when the read failed).
    pub(crate) fn epoch_now(&self) -> Option<u64> {
        read_slot_max(&self.file, LOCK_SLOT_A, LOCK_SLOT_DOMAIN).ok()
    }

    /// True when the on-disk epoch differs from what this process last
    /// refreshed/observed; a read error counts as changed.
    pub(crate) fn stale(&self) -> bool {
        match self.epoch_now() {
            Some(e) => e != self.epoch_cache.load(Ordering::Relaxed),
            None => true,
        }
    }

    /// The cached epoch value (tests).
    #[cfg(test)]
    pub(crate) fn epoch_cache_value(&self) -> u64 {
        self.epoch_cache.load(Ordering::Relaxed)
    }

    /// Test-only fast-path counters: `(prechecks, fastpath_skips,
    /// refresh_passes)` (wave 45n gap d — white-box evidence that an idle
    /// reader serves statements with one epoch pread and no lease).
    #[cfg(test)]
    pub(crate) fn fast_path_counts(&self) -> (u64, u64, u64) {
        (
            self.prechecks.load(Ordering::Relaxed),
            self.fastpath_skips.load(Ordering::Relaxed),
            self.refresh_passes.load(Ordering::Relaxed),
        )
    }

    /// Cache the current on-disk epoch (after a refresh).
    pub(crate) fn cache_epoch_now(&self) {
        if let Some(e) = self.epoch_now() {
            self.epoch_cache.store(e, Ordering::Relaxed);
        }
    }

    /// Publish a change to other processes: write the next seq into the
    /// alternate slot and cache it. A write failure is a warning, never a
    /// correctness failure (the slots are hints; recovery re-derives the
    /// committed prefix from the frames).
    pub(crate) fn bump_epoch(&self, value: u64) {
        let seq = self.next_epoch_seq.fetch_add(1, Ordering::Relaxed);
        if write_slot(&self.file, LOCK_SLOT_A, seq, value, LOCK_SLOT_DOMAIN).is_err() {
            warn_slot_once("lock-file epoch slot");
        }
        self.epoch_cache.store(seq, Ordering::Relaxed);
    }
}

impl LeaseGuard {
    /// Fast-path freshness precheck under a held lease.
    pub(crate) fn epoch_stale(&self) -> bool {
        self.lease.stale()
    }

    /// Refuse when the lock file was deleted or replaced under this held
    /// lease (mutation-boundary re-check; see [`Lease`]).
    pub(crate) fn verify_lock_identity(&self) -> Result<()> {
        self.lease.verify_lock_identity()
    }

    pub(crate) fn cache_epoch_now(&self) {
        self.lease.cache_epoch_now();
    }

    pub(crate) fn bump_epoch(&self, value: u64) {
        self.lease.bump_epoch(value);
    }
}

/// The static, authenticated identity of a current publish block.
fn publish_meta(wal_id: &WalId, generation: u64) -> [u8; ENC_PUBLISH_META_LEN] {
    let mut meta = [0u8; ENC_PUBLISH_META_LEN];
    meta[..16].copy_from_slice(wal_id);
    meta[16..24].copy_from_slice(&generation.to_le_bytes());
    meta[24..26].copy_from_slice(&ENC_PUBLISH_BLOCK_VERSION.to_le_bytes());
    meta
}

/// Encode the encrypted WAL's authenticated publish block; an `initial` slot
/// edge seeds one slot for a rebuild/upgrade (design 9.2).
fn encode_publish_block(
    cipher: &crypto::RecordCipher,
    wal_id: &WalId,
    generation: u64,
    initial: Option<(u64, u64)>,
) -> Result<[u8; ENC_PUBLISH_BLOCK_LEN]> {
    let mut b = [0u8; ENC_PUBLISH_BLOCK_LEN];
    let meta = publish_meta(wal_id, generation);
    let (nonce, tag) = cipher.seal_publish_meta(&meta)?;
    b[..ENC_PUBLISH_META_LEN].copy_from_slice(&meta);
    b[ENC_PUBLISH_META_LEN..ENC_PUBLISH_META_LEN + 12].copy_from_slice(&nonce);
    b[ENC_PUBLISH_META_LEN + 12..ENC_PUBLISH_META_TAG_END].copy_from_slice(&tag);
    if let Some((seq, value)) = initial {
        let (slot, at) = if seq.is_multiple_of(2) {
            (1u8, ENC_SLOT_B as usize)
        } else {
            (0u8, ENC_SLOT_A as usize)
        };
        b[at..at + ENC_PUBLISH_SLOT_LEN]
            .copy_from_slice(&encode_publish_slot(cipher, &meta, slot, seq, value)?);
    }
    Ok(b)
}

fn encode_publish_slot(
    cipher: &crypto::RecordCipher,
    meta: &[u8; ENC_PUBLISH_META_LEN],
    slot: u8,
    seq: u64,
    value: u64,
) -> Result<[u8; ENC_PUBLISH_SLOT_LEN]> {
    let (nonce, tag) = cipher.seal_publish_slot(meta, slot, seq, value)?;
    let mut b = [0u8; ENC_PUBLISH_SLOT_LEN];
    b[..8].copy_from_slice(&seq.to_le_bytes());
    b[8..16].copy_from_slice(&value.to_le_bytes());
    b[16..28].copy_from_slice(&nonce);
    b[28..44].copy_from_slice(&tag);
    Ok(b)
}

/// Verify a current publish block: the static meta must authenticate (a
/// tampered identity is a hard recovery error), and the slots contribute
/// only when their own tag verifies.
fn open_publish_block(
    cipher: &crypto::RecordCipher,
    block: &[u8],
    generation: u64,
) -> std::result::Result<(WalId, u64), String> {
    if block.len() != ENC_PUBLISH_BLOCK_LEN {
        return Err("truncated publish block".to_string());
    }
    let meta: [u8; ENC_PUBLISH_META_LEN] = block[..ENC_PUBLISH_META_LEN]
        .try_into()
        .expect("fixed meta length");
    let nonce: [u8; 12] = block[ENC_PUBLISH_META_LEN..ENC_PUBLISH_META_LEN + 12]
        .try_into()
        .expect("12-byte nonce");
    let tag = &block[ENC_PUBLISH_META_LEN + 12..ENC_PUBLISH_META_TAG_END];
    if !cipher.open_publish_meta(&meta, &nonce, tag) {
        return Err(
            "publish block authentication failed (wal_id/generation was modified)".to_string(),
        );
    }
    let block_version = u16::from_le_bytes(meta[24..26].try_into().expect("2 bytes"));
    if block_version != ENC_PUBLISH_BLOCK_VERSION || meta[26..28] != [0u8; 2] {
        return Err(format!("unsupported publish block version {block_version}"));
    }
    if u64::from_le_bytes(meta[16..24].try_into().expect("8 bytes")) != generation {
        return Err("publish block generation does not match the container".to_string());
    }
    let mut wal_id = [0u8; 16];
    wal_id.copy_from_slice(&meta[..16]);
    let mut published_seq = 0u64;
    for slot in 0..2u8 {
        let at = if slot == 0 {
            ENC_SLOT_A as usize
        } else {
            ENC_SLOT_B as usize
        };
        let raw = &block[at..at + ENC_PUBLISH_SLOT_LEN];
        let seq = u64::from_le_bytes(raw[..8].try_into().expect("8 bytes"));
        let value = u64::from_le_bytes(raw[8..16].try_into().expect("8 bytes"));
        let slot_nonce: [u8; 12] = raw[16..28].try_into().expect("12-byte nonce");
        let slot_tag = &raw[28..44];
        if cipher.open_publish_slot(&meta, slot, seq, value, &slot_nonce, slot_tag) {
            published_seq = published_seq.max(seq);
        }
    }
    Ok((wal_id, published_seq))
}

/// Legacy publish-block CRC (container v2/v3). Read-only: current blocks are
/// authenticated by [`open_publish_block`].
#[cfg(feature = "encryption")]
fn publish_block_crc(wal_id: &WalId, generation: u64, version: u32) -> u32 {
    let mut v = Vec::with_capacity(16 + 12);
    v.extend_from_slice(wal_id);
    v.extend_from_slice(&generation.to_le_bytes());
    v.extend_from_slice(&version.to_le_bytes());
    crc32(&v)
}

/// Hand-rolled CRC-32 (IEEE, reflected polynomial 0xEDB88320) — no deps.
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// When committed records are fsynced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncPolicy {
    /// `File::sync_all` after every record: a committed statement is durable
    /// before it returns (the default for durable opens).
    #[default]
    Sync,
    /// Records are written but not fsynced; [`PinkDb::flush_wal`] and
    /// checkpoint make them durable.
    Batch,
}

/// What an [`PinkDb::open_durable`] call saw and did while recovering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryInfo {
    /// WAL records applied on top of the checkpoint.
    pub replayed_records: u64,
    /// SQL statements executed during replay.
    pub replayed_statements: u64,
    /// Trailing bytes discarded as a torn/corrupt WAL tail.
    pub truncated_bytes: u64,
    /// Interrupted checkpoints finished by recovery: a provably complete
    /// `.tmp` was published as the new `.pink`.
    pub checkpoints_completed: u64,
    /// Rolled WAL generations removed because they were provably folded into
    /// the checkpoint.
    pub stale_rolls_removed: u64,
}

/// One parsed WAL record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Record {
    lsn: u64,
    stmts: Vec<String>,
    /// The frame's touched-object set (payload v4); `None` = unknown (a
    /// record adopted from a legacy v2/v3 upgrade).
    sets: Option<FrameSets>,
}

/// Default WAL size that triggers an automatic checkpoint after a commit
/// (64 MiB): bounds WAL growth and restart replay time for long-running
/// servers and embedders that never call `checkpoint()` themselves.
pub const DEFAULT_AUTO_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;

/// An open WAL file positioned for appends (shared by every clone of the
/// owning store, so `txn.work` snapshots log to the same file).
#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
    checkpoint_path: PathBuf,
    policy: SyncPolicy,
    /// Generation of the live WAL; rolls carry the drained generation.
    generation: u64,
    /// This WAL file incarnation's fresh id (ABA guard across checkpoints).
    wal_id: WalId,
    last_lsn: u64,
    /// Byte offset where the first record begins (96 plaintext; the container
    /// payload start + 88 encrypted).
    records_start: u64,
    /// Byte offset of the header's commit slot A.
    slots_at: u64,
    /// Highest commit-slot seq this process published (or read at open).
    published_seq: u64,
    /// Set after a write/sync failure: appends are refused so a retry can
    /// never append a duplicate record after a partially durable write.
    poisoned: Option<String>,
    /// The replay session's database after the records appended so far
    /// (`None` = unknown, so the next append prefixes unconditionally).
    /// Multi-connection appends emit a `USE` prefix whenever their session
    /// differs, so every record replays in the database it was executed in.
    session_db: Option<String>,
    /// Extension paths whose `LOAD EXTENSION` the CURRENT WAL generation
    /// already covers: a record touching such an extension is not prefixed
    /// again. Seeded after recovery from the extensions replay loaded (the
    /// live generation is exactly what replay saw), updated when a record
    /// with a `LOAD EXTENSION` prefix or statement is appended, and RESET by
    /// a checkpoint — a checkpoint never persists an extension, so the fresh
    /// generation starts with nothing loaded and every extension a later
    /// record needs is prefixed again. Compared as exact strings (the path
    /// string is what replay passes to the loader).
    tracked_extensions: HashSet<String>,
    /// Per-record AEAD state for an encrypted WAL (`None` = plaintext).
    cipher: Option<RecordCipher>,
    /// WAL size (bytes) at which a committed write triggers an automatic
    /// checkpoint (`None` = never; see [`DEFAULT_AUTO_CHECKPOINT_BYTES`]).
    auto_checkpoint_bytes: Option<u64>,
    /// Bytes appended by this process since the current generation began.
    /// The auto-checkpoint additionally requires this process to own at
    /// least half of the live WAL's bytes, so N writers do not each rewrite
    /// the whole store round-robin (design 6.5). Single-process behavior is
    /// unchanged after the first generation; a process that reopens an
    /// existing WAL starts this at 0.
    bytes_appended_since_gen_start: u64,
    /// The write lease of the owning durable store (shared with the store).
    lease: Arc<Lease>,
    /// What the owning store's memory already includes of this file.
    applied: Applied,
    /// Frames applied or appended since the last full reload, with their
    /// touched sets: what an open optimistic base validates against. Pruned
    /// to the oldest registered base (`open_bases`); empty when nothing is
    /// open, which is the common case. Bounded by the current WAL
    /// incarnation (the auto-checkpoint default caps it).
    journal: Vec<JournalEntry>,
    /// Registered open bases: seq → base lsn. The minimum lsn here is the
    /// journal's prune threshold.
    open_bases: std::collections::BTreeMap<u64, u64>,
    next_base_seq: u64,
    /// Bytes discarded from a torn tail by the last scan (`0` = clean).
    truncated_last: u64,
    /// Whether the last scan rewrote the file (legacy upgrade or encrypted
    /// tail rebuild).
    upgraded_last: bool,
    /// Test-only evidence counters for the optimistic commit path:
    /// `(optimistic commits checked, conflicts detected, pessimistic
    /// fallbacks taken)`.
    #[cfg(test)]
    optimistic_counts: (u64, u64, u64),
}

impl Wal {
    /// Append one frame's sets to the journal (called after the frame is
    /// applied or appended). Entries stay in lsn order by construction.
    fn journal_push(&mut self, lsn: u64, sets: Option<FrameSets>) {
        self.journal.push(JournalEntry { lsn, sets });
    }

    /// Replace the journal with a fresh (empty) one after a full rebuild: the
    /// caller re-pushes the frames it replayed.
    fn reset_journal(&mut self) {
        self.journal.clear();
    }

    /// Drop journal entries no open base needs any more: everything at or
    /// below the oldest registered base (or everything, when none is open).
    fn prune_journal(&mut self) {
        let threshold = self.open_bases.values().copied().min().unwrap_or(u64::MAX);
        if let Some(pos) = self.journal.iter().position(|e| e.lsn > threshold) {
            self.journal.drain(..pos);
        } else {
            self.journal.clear();
        }
    }

    /// Register a new optimistic base at the current applied position.
    /// `None` when the WAL is poisoned (no valid position exists).
    fn register_base(&mut self) -> Option<(u64, BasePosition)> {
        if self.poisoned.is_some() {
            return None;
        }
        let position = BasePosition {
            wal_id: self.wal_id,
            generation: self.generation,
            lsn: self.applied.applied_lsn,
            // The cached meta is the last observed one; the commit check
            // re-stats the checkpoint, so a replacement between this capture
            // and the commit is still caught.
            ckpt: self.applied.ckpt,
        };
        let seq = self.next_base_seq;
        self.next_base_seq += 1;
        self.open_bases.insert(seq, position.lsn);
        self.prune_journal();
        Some((seq, position))
    }

    /// Unregister a base (its token dropped) and prune.
    fn close_base(&mut self, seq: u64) {
        self.open_bases.remove(&seq);
        self.prune_journal();
    }

    /// Validate `ours` against everything committed after `base`: the frame
    /// journal (frames already applied) first. Returns the conflicting
    /// object description. An unknown-set frame conflicts conservatively
    /// (only reachable for a base older than a legacy upgrade, which the
    /// incarnation check rejects first).
    fn check_journal(
        &self,
        base: &BasePosition,
        ours: &FrameSets,
    ) -> Option<(String, Option<u64>)> {
        for e in self.journal.iter().rev() {
            if e.lsn <= base.lsn {
                continue;
            }
            match &e.sets {
                Some(sets) => {
                    if let Some(object) = sets.conflict_with(ours) {
                        return Some((object, Some(e.lsn)));
                    }
                }
                None => {
                    return Some((
                        "an upgraded legacy frame (touched set unknown)".to_string(),
                        Some(e.lsn),
                    ));
                }
            }
        }
        None
    }

    /// Does the frame (by its sets) conflict with `ours`, given the base?
    /// Frames at or below the base are already part of the transaction's
    /// snapshot and cannot conflict.
    fn frame_conflicts(
        sets: Option<&FrameSets>,
        lsn: u64,
        base: &BasePosition,
        ours: &FrameSets,
    ) -> Option<(String, Option<u64>)> {
        if lsn <= base.lsn {
            return None;
        }
        match sets {
            Some(sets) => sets.conflict_with(ours).map(|object| (object, Some(lsn))),
            None => Some((
                "an upgraded legacy frame (touched set unknown)".to_string(),
                Some(lsn),
            )),
        }
    }

    /// Test-only evidence counters (see the field docs).
    #[cfg(test)]
    pub(crate) fn optimistic_counts(&self) -> (u64, u64, u64) {
        self.optimistic_counts
    }

    /// Test-only: record one optimistic commit check outcome (`conflict` =
    /// detected; `fallback` = the pessimistic path was taken).
    #[cfg(test)]
    pub(crate) fn note_optimistic_outcome(&mut self, conflict: bool, fallback: bool) {
        self.optimistic_counts.0 += 1;
        if conflict {
            self.optimistic_counts.1 += 1;
        }
        if fallback {
            self.optimistic_counts.2 += 1;
        }
    }

    /// Test-only: the journal's `(lsn, sets-known)` list (oldest first).
    #[cfg(test)]
    pub(crate) fn journal_for_test(&self) -> Vec<(u64, bool)> {
        self.journal
            .iter()
            .map(|e| (e.lsn, e.sets.is_some()))
            .collect()
    }

    #[cfg(test)]
    fn open_bases_for_test(&self) -> usize {
        self.open_bases.len()
    }
}

/// What the owning store's memory already includes of the on-disk state:
/// the WAL incarnation and position plus the checkpoint identity it was
/// built from. Compared under the lease to decide incremental vs. full
/// refresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Applied {
    wal_id: WalId,
    generation: u64,
    wal_dev_ino: Option<(u64, u64)>,
    records_start: u64,
    /// Byte offset just past the last applied frame.
    applied_end: u64,
    applied_lsn: u64,
    /// Number of applied records in this WAL incarnation (the encrypted
    /// nonce ordinal base for the next scan).
    applied_records: u64,
    /// The checkpoint file this store was built from (see [`CkptMeta`]: the
    /// meta catches an in-place `.save` rewrite that `(dev, ino)` cannot).
    ckpt: Option<CkptMeta>,
}

/// Enough to witness a checkpoint-file replacement without reading it:
/// identity, length, and mtime. A `.save` snapshot rewrites the target in
/// place, so `(dev, ino)` alone cannot see it; the length + mtime pair does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CkptMeta {
    dev_ino: Option<(u64, u64)>,
    len: u64,
    mtime_ns: Option<i128>,
}

/// A single-stat witness of a file's identity and length (the optimistic
/// commit's fast path: nothing changed on disk since the last apply).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileMeta {
    dev_ino: Option<(u64, u64)>,
    len: u64,
}

fn file_meta(path: &Path) -> Option<FileMeta> {
    let m = std::fs::metadata(path).ok()?;
    Some(FileMeta {
        dev_ino: Some(identity_of(&m)),
        len: m.len(),
    })
}

fn ckpt_meta(path: &Path) -> Option<CkptMeta> {
    let m = std::fs::metadata(path).ok()?;
    Some(CkptMeta {
        dev_ino: Some(identity_of(&m)),
        len: m.len(),
        mtime_ns: m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as i128),
    })
}

/// The position one optimistic unit (transaction or autocommit statement)
/// commits against: the WAL incarnation, the applied LSN at its start, and
/// the checkpoint file identity the store was built from. A commit conflicts
/// when the WAL incarnation changed (another process checkpointed or
/// recovered), when the checkpoint file was replaced (a `.save` snapshot —
/// which does not roll the WAL), or when any frame recorded after `lsn`
/// touched an object of the unit (see [`FrameSets`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BasePosition {
    wal_id: WalId,
    generation: u64,
    lsn: u64,
    ckpt: Option<CkptMeta>,
}

/// One registered open base: a `BaseToken` holds this and unregisters it on
/// drop, which is what keeps the journal bounded (entries a live base still
/// needs are never pruned).
#[derive(Debug)]
pub(crate) struct BaseToken {
    wal: Arc<Mutex<Wal>>,
    seq: u64,
    position: BasePosition,
}

impl BaseToken {
    pub(crate) fn position(&self) -> BasePosition {
        self.position
    }
}

impl Drop for BaseToken {
    fn drop(&mut self) {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.close_base(self.seq);
    }
}

/// One frame's touched set, kept in the in-memory journal while any open base
/// may still need it. `sets: None` = unknown (legacy frame).
#[derive(Debug, Clone)]
struct JournalEntry {
    lsn: u64,
    sets: Option<FrameSets>,
}

/// A cross-process commit conflict: the object whose frame(s) changed, why,
/// and the frame's LSN when one is known (the WAL position is the
/// cross-process "version" — `DataTable::version` is process-local and never
/// persisted, so it cannot be compared across writers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitConflict {
    /// e.g. `table "main.t"`, `database "db1"`, `the WAL generation`.
    pub(crate) object: String,
    /// e.g. `changed concurrently`.
    pub(crate) detail: String,
    pub(crate) lsn: Option<u64>,
}

impl CommitConflict {
    /// The user-facing message (stable `write conflict on …` prefix, so hosts
    /// can detect retryable conflicts without matching on a new error
    /// variant; `PinkDbError` is a public enum without `#[non_exhaustive]`,
    /// exactly like the wave-45n busy decision).
    pub(crate) fn message(&self, unit: &str) -> String {
        match self.lsn {
            Some(lsn) => format!(
                "write conflict on {} ({}, committed at lsn {lsn} by another writer since this \
                 {unit} started); retry the {unit}",
                self.object, self.detail
            ),
            None => format!(
                "write conflict on {} ({}); retry the {unit}",
                self.object, self.detail
            ),
        }
    }
}

/// One rolled-away WAL generation found on disk.
#[derive(Debug)]
struct Roll {
    generation: u64,
    path: PathBuf,
}

/// What a recovery pass sees in the live WAL before replaying it.
#[derive(Debug)]
struct WalProbe {
    /// Generation from a valid header (`None` for a torn/absent file).
    generation: Option<u64>,
    /// Whether the file holds at least one intact record.
    has_records: bool,
}

/// The parsed header of a live WAL (`generation`/identity/records start/slots).
#[derive(Debug)]
struct WalHeader {
    generation: u64,
    wal_id: WalId,
    /// Offset where the first record begins.
    records_start: usize,
    /// Byte offset of commit slot A in the file.
    slots_at: u64,
    /// Highest valid publish-slot seq in the header (0 when none).
    published_seq: u64,
    /// Container header for an encrypted WAL.
    enc: Option<crypto::ContainerHeader>,
    /// A pre-wave-45m file (plaintext v2 or container v1) to be upgraded.
    legacy: bool,
}

#[derive(Debug)]
enum HeaderError {
    TooShort,
    BadMagic,
    BadVersion(u32),
    /// A v3 header whose CRC over `magic..wal_id` does not verify.
    BadCrc,
    /// Reserved header bytes are nonzero (a format error, never a torn write
    /// of a valid header).
    Reserved,
    /// A `PINKENC1` container whose public header did not parse.
    #[cfg(feature = "encryption")]
    Container(String),
    /// A `PINKENC1` container whose version this build does not understand
    /// (never a torn creation).
    #[cfg(feature = "encryption")]
    ContainerVersion(u32),
    /// An encrypted v2 container whose fixed publish block is missing or
    /// malformed.
    #[cfg(feature = "encryption")]
    BadPublishBlock {
        payload_start: usize,
        block_len: usize,
        what: String,
    },
}

/// Every intact record of a freshly scanned WAL file plus its positioning.
struct OpenedWal {
    file: File,
    generation: u64,
    wal_id: WalId,
    records_start: u64,
    slots_at: u64,
    published_seq: u64,
    cipher: Option<RecordCipher>,
    records: Vec<Record>,
    truncated: u64,
    upgraded: bool,
}

impl Wal {
    /// A current-format (v4) plaintext header with the given incarnation id.
    fn header_for(generation: u64, wal_id: &WalId) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[..8].copy_from_slice(WAL_MAGIC);
        h[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
        h[12..20].copy_from_slice(&generation.to_le_bytes());
        h[20..36].copy_from_slice(wal_id);
        let crc = crc32(&h[..36]);
        h[36..40].copy_from_slice(&crc.to_le_bytes());
        h
    }

    /// A current header with a fresh incarnation id (test/raw fixtures).
    #[cfg(test)]
    fn header_fresh(generation: u64) -> [u8; HEADER_LEN] {
        Self::header_for(generation, &fresh_wal_id())
    }

    /// Create (or recreate) the WAL file at `path` with just a header and
    /// position it for appends: the plaintext v4 header (fresh `wal_id`), or
    /// — when `cipher` is given — an encrypted container v4 header plus the
    /// authenticated 144-byte publish block. The returned fd is opened `append`: a
    /// second process may append between this process's writes, and a
    /// non-append fd would overwrite from its cached offset.
    fn create_file(
        path: &Path,
        generation: u64,
        cipher: Option<RecordCipher>,
    ) -> Result<(File, Option<RecordCipher>, WalId)> {
        let wal_id = fresh_wal_id();
        {
            // No `append(true)` here: std rejects append+truncate on Unix,
            // and a fresh file needs only sequential writes from offset 0.
            // M-5: the WAL holds literal SQL text, so it is owner-only — and
            // an existing umask-created file is narrowed before truncation.
            crate::fsutil::tighten(path);
            let mut f = crate::fsutil::open_rw_private(path, true)?;
            match &cipher {
                Some(c) => {
                    f.write_all(c.header())?;
                    f.write_all(&encode_publish_block(c, &wal_id, generation, None)?)?;
                }
                None => f.write_all(&Self::header_for(generation, &wal_id))?,
            }
            f.sync_all()?;
        }
        // A fresh database's first WAL (or a recovery-recreated one) must have
        // its directory entry durable, or a crash can lose the file even under
        // `Sync`.
        sync_parent_dir(path);
        let file = OpenOptions::new().read(true).append(true).open(path)?;
        Ok((file, cipher, wal_id))
    }

    /// Path of the `.pink` checkpoint this WAL belongs to.
    pub fn checkpoint_path(&self) -> &Path {
        &self.checkpoint_path
    }

    /// True when this WAL is an encrypted container (the mode it was opened
    /// in; a live WAL's mode never changes).
    #[cfg(feature = "encryption")]
    pub(crate) fn is_encrypted(&self) -> bool {
        self.cipher.is_some()
    }

    /// True when `key` derives exactly the container key of this WAL. A
    /// checkpoint must never publish a `.pink` under a different key than
    /// the WAL generation it rolls into: the pair would be unopenable.
    #[cfg(feature = "encryption")]
    pub(crate) fn key_matches(&self, key: &EncryptionKey) -> bool {
        self.cipher.as_ref().is_some_and(|c| c.matches_key(key))
    }

    /// Open the WAL at `path` (creating it when absent) and parse every
    /// intact record. `recreate_gen` is the generation used when the file is
    /// missing or its header is torn; recovery computes it from the surviving
    /// roll files so the sequence never regresses. The file is positioned for
    /// appends right after the last intact record; a torn tail is truncated
    /// in place (plaintext) or rebuilt under a fresh nonce prefix
    /// (encrypted). Legacy formats are upgraded in place first: plaintext v2
    /// → v3, container v1 → v2 + publish block.
    pub(crate) fn open(
        path: &Path,
        checkpoint_path: PathBuf,
        policy: SyncPolicy,
        recreate_gen: u64,
        key: Option<&EncryptionKey>,
        lease: Arc<Lease>,
    ) -> Result<(Wal, Vec<Record>, u64)> {
        let opened = scan_wal(path, key, recreate_gen, true)?;
        let truncated = opened.truncated;
        let wal = Wal {
            file: opened.file,
            path: path.to_path_buf(),
            checkpoint_path,
            policy,
            generation: opened.generation,
            wal_id: opened.wal_id,
            records_start: opened.records_start,
            slots_at: opened.slots_at,
            published_seq: opened.published_seq,
            last_lsn: opened.records.last().map(|r| r.lsn).unwrap_or(0),
            poisoned: None,
            session_db: None,
            tracked_extensions: HashSet::new(),
            cipher: opened.cipher,
            auto_checkpoint_bytes: Some(DEFAULT_AUTO_CHECKPOINT_BYTES),
            bytes_appended_since_gen_start: 0,
            lease,
            applied: Applied::default(),
            journal: Vec::new(),
            open_bases: std::collections::BTreeMap::new(),
            next_base_seq: 0,
            truncated_last: opened.truncated,
            upgraded_last: opened.upgraded,
            #[cfg(test)]
            optimistic_counts: (0, 0, 0),
        };
        Ok((wal, opened.records, truncated))
    }

    /// Bytes discarded from a torn tail by the last scan.
    pub(crate) fn truncated_last(&self) -> u64 {
        self.truncated_last
    }

    /// Whether the last scan rewrote the file (legacy upgrade or rebuild).
    pub(crate) fn upgraded_last(&self) -> bool {
        self.upgraded_last
    }

    /// Mark the store's position in this WAL incarnation (after open or a
    /// refresh).
    pub(crate) fn mark_applied(&mut self, applied: Applied) {
        self.applied = applied;
    }

    /// True when the WAL or checkpoint file was replaced or removed under us,
    /// or the WAL grew past the applied position without a publication (a
    /// crashed publisher's durable-but-unpublished frames — design row W2):
    /// the caller must scan. Writer-side re-check (design 3.4): the epoch
    /// alone cannot see a crash between a file mutation and its epoch bump.
    fn needs_scan(&self) -> Result<bool> {
        let (id, len) = match std::fs::metadata(&self.path) {
            Ok(m) => (Some(identity_of(&m)), Some(m.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (None, None),
            Err(e) => return Err(e.into()),
        };
        if id != self.applied.wal_dev_ino || len != Some(self.applied.applied_end) {
            return Ok(true);
        }
        Ok(ckpt_meta(&self.checkpoint_path) != self.applied.ckpt)
    }

    /// Poison the handle (H6): a refresh/replay failure fail-stops every
    /// later statement exactly like a failed append.
    pub(crate) fn poison(&mut self, msg: String) {
        if self.poisoned.is_none() {
            self.poisoned = Some(msg);
        }
    }

    /// Test hook: simulate an append that failed on disk.
    #[cfg(test)]
    pub(crate) fn poison_for_test(&mut self, msg: &str) {
        self.poisoned = Some(msg.to_string());
    }

    /// Set the automatic-checkpoint threshold (`None` disables it).
    pub(crate) fn set_auto_checkpoint_bytes(&mut self, bytes: Option<u64>) {
        self.auto_checkpoint_bytes = bytes;
    }

    /// True when the live WAL has outgrown the auto-checkpoint threshold AND
    /// this process appended at least half of its bytes since the generation
    /// began (design 6.5: N writers must not round-robin the whole store).
    pub(crate) fn wants_checkpoint(&self) -> bool {
        match self.auto_checkpoint_bytes {
            Some(limit) if self.poisoned.is_none() => self.file.metadata().is_ok_and(|m| {
                m.len() >= limit
                    && self.bytes_appended_since_gen_start.saturating_mul(2)
                        >= m.len().saturating_sub(self.records_start)
            }),
            _ => false,
        }
    }

    /// Why the WAL stopped accepting writes, if it did (see `poisoned`).
    pub(crate) fn failure(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }

    /// Publish the durable commit position to other processes: write the next
    /// commit slot in the header (alternating slots, no fsync — a hint on top
    /// of the authoritative frames) and bump the lock-file epoch. Called
    /// after every successful append; failures warn once and never fail the
    /// commit.
    fn publish(&mut self) {
        let seq = self.published_seq + 1;
        // The data handle is opened with `append(true)` for atomic record
        // appends; it must NOT be used for positioned writes: on Linux,
        // `pwrite` on an O_APPEND descriptor ignores the offset and appends
        // to EOF (macOS honours the offset), which silently strands the
        // commit slot at the end of the file. Open a positioned handle for
        // the header slot — a hint write on the commit path, one extra open,
        // never an fsync.
        let wrote = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .and_then(|f| match &self.cipher {
                Some(cipher) => {
                    let meta = publish_meta(&self.wal_id, self.generation);
                    let slot = if seq.is_multiple_of(2) { 1 } else { 0 };
                    let bytes = encode_publish_slot(cipher, &meta, slot, seq, self.last_lsn)
                        .map_err(std::io::Error::other)?;
                    let offset = self.slots_at
                        + if slot == 0 {
                            0
                        } else {
                            ENC_PUBLISH_SLOT_LEN as u64
                        };
                    file_write_all_at(&f, &bytes, offset)
                }
                None => write_slot(&f, self.slots_at, seq, self.last_lsn, WAL_SLOT_DOMAIN),
            });
        if wrote.is_err() {
            warn_slot_once("WAL commit slot");
        }
        self.published_seq = seq;
        self.lease.bump_epoch(self.last_lsn);
    }

    /// Re-scan the live WAL under the lease after its identity changed:
    /// fresh fd, fresh header identity, cipher counter from the on-disk
    /// record count, `last_lsn` from the intact records, torn tail resolved.
    /// Returns every intact record of the new incarnation (the caller replays
    /// them all — there is no incremental continuity across incarnations).
    fn reopen_under_lease(&mut self, key: &Option<EncryptionKey>) -> Result<(Vec<Record>, u64)> {
        let opened = scan_wal(&self.path, key.as_ref(), self.generation.max(1), false)?;
        // The good end is the file length after scanning (a torn tail was
        // already truncated/rebuilt by the scan).
        let end = std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(opened.records_start);
        self.file = opened.file;
        self.generation = opened.generation;
        self.wal_id = opened.wal_id;
        self.records_start = opened.records_start;
        self.slots_at = opened.slots_at;
        self.published_seq = opened.published_seq;
        self.cipher = opened.cipher;
        self.last_lsn = opened.records.last().map(|r| r.lsn).unwrap_or(0);
        self.truncated_last = opened.truncated;
        self.upgraded_last = opened.upgraded;
        // A new incarnation has no applied continuity: reset the position to
        // the start of the file. The caller replays every returned record and
        // then records the final position.
        self.applied = Applied {
            wal_id: opened.wal_id,
            generation: opened.generation,
            wal_dev_ino: file_identity(&self.path),
            records_start: opened.records_start,
            applied_end: opened.records_start,
            applied_lsn: 0,
            applied_records: 0,
            ckpt: self.applied.ckpt,
        };
        if let Some(c) = self.cipher.as_mut() {
            let next = u32::try_from(opened.records.len())
                .map_err(|_| {
                    PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
                })?
                .checked_add(1)
                .ok_or_else(|| {
                    PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
                })?;
            c.set_next_counter(next);
        }
        Ok((opened.records, end))
    }

    /// Incremental scan: parse the bytes past `applied_end` for the same file
    /// incarnation. `None` when continuity is broken (a short read, framing
    /// corruption at the boundary, or an LSN that does not continue) — the
    /// caller then falls back to a full rebuild. A plaintext torn tail is
    /// truncated in place; an encrypted one returns `None` so the full path
    /// rebuilds it under a fresh nonce prefix.
    fn scan_increment(&mut self) -> Result<Option<(Vec<Record>, u64)>> {
        let len = self.file.metadata()?.len();
        if self.applied.applied_end > len {
            return Ok(None);
        }
        if self.applied.applied_end == len {
            return Ok(Some((Vec::new(), len)));
        }
        let mut buf = vec![0u8; (len - self.applied.applied_end) as usize];
        file_read_exact_at(&self.file, &mut buf, self.applied.applied_end)?;
        let (records, good_end) = parse_records(
            &buf,
            0,
            self.cipher.as_ref(),
            self.applied.applied_lsn,
            u32::try_from(self.applied.applied_records).unwrap_or(u32::MAX),
            true,
        )
        .map_err(PinkDbError::Execution)?;
        if let Some(first) = records.first()
            && self.applied.applied_lsn.checked_add(1) != Some(first.lsn)
        {
            return Ok(None);
        }
        let end = self.applied.applied_end + good_end as u64;
        if end < len {
            if self.cipher.is_some() {
                // An encrypted tail loss must rebuild under a fresh prefix;
                // the full path has every intact record and does exactly
                // that.
                return Ok(None);
            }
            // A crashed writer's torn tail: truncate at the last intact
            // record so the next append cannot land after garbage.
            self.file.set_len(end)?;
            self.file.sync_all()?;
            self.truncated_last = len - end;
        } else {
            self.truncated_last = 0;
        }
        self.upgraded_last = false;
        if let Some(last) = records.last() {
            self.last_lsn = last.lsn;
        }
        Ok(Some((records, end)))
    }

    /// Append one committed unit of statements as a single record. `sets` is
    /// the frame's touched-object set (payload v4): `None` is reserved for
    /// legacy re-encodes during an upgrade; every live append knows its set,
    /// so optimistic commits can validate against this record later.
    pub(crate) fn append(&mut self, stmts: &[String], sets: Option<&FrameSets>) -> Result<()> {
        if let Some(msg) = &self.poisoned {
            return Err(PinkDbError::Execution(format!(
                "WAL is unusable after an earlier write failure ({msg})"
            )));
        }
        // Mutation-boundary identity re-check: a lease that no longer names
        // the locked file must never write bytes. Fail-stop like any other
        // append failure: the statement's in-memory effect is not durable
        // and must never be observed through this handle.
        if let Err(e) = self.lease.verify_lock_identity() {
            let msg = e.to_string();
            self.poisoned = Some(msg.clone());
            return Err(PinkDbError::Execution(msg));
        }
        let mut sets = sets.cloned();
        if let Some(s) = &mut sets {
            s.canonicalize();
        }
        let payload = encode_payload(stmts, sets.as_ref())?;
        let len = record_len_guard(payload.len())?;
        let lsn = self.last_lsn.checked_add(1).ok_or_else(|| {
            PinkDbError::Execution("WAL LSN space exhausted (u64::MAX)".to_string())
        })?;
        // The applied position must be the pre-write EOF: every write path
        // refreshes (applies) another process's frames under the lease before
        // appending, so no committed frame is ever skipped.
        debug_assert_eq!(
            self.file.metadata().map(|m| m.len()).ok(),
            Some(self.applied.applied_end),
            "append without having applied the tail (lsn {lsn})"
        );
        let (crc, body): (u32, Vec<u8>) = match &mut self.cipher {
            // The crc field stays in the framing for an encrypted record but
            // is zero: GCM authentication replaces it.
            Some(cipher) => (0, cipher.seal(lsn, &payload)?),
            None => (crc32(&payload), payload),
        };
        let mut rec = Vec::with_capacity(RECORD_HEADER_LEN + body.len());
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(&lsn.to_le_bytes());
        rec.extend_from_slice(&crc.to_le_bytes());
        rec.extend_from_slice(&body);
        let result = self.file.write_all(&rec).and_then(|()| match self.policy {
            SyncPolicy::Sync => self.file.sync_all(),
            SyncPolicy::Batch => Ok(()),
        });
        match result {
            Ok(()) => {
                self.last_lsn = lsn;
                self.bytes_appended_since_gen_start += rec.len() as u64;
                // This process already applied the record: advance the
                // applied position so a later refresh reads only frames from
                // other processes. (The write paths refresh under the lease
                // before appending, so `applied_end` is the pre-write EOF.)
                self.applied.applied_end += rec.len() as u64;
                self.applied.applied_lsn = lsn;
                self.applied.applied_records += 1;
                // Journal the frame so open optimistic bases validate against
                // it even though it never goes through a replay pass.
                self.journal_push(lsn, sets);
                self.publish();
                Ok(())
            }
            Err(e) => {
                self.poisoned = Some(e.to_string());
                Err(PinkDbError::Io(e))
            }
        }
    }

    /// fsync everything written so far (no-op after a clean `Sync` append).
    pub(crate) fn flush(&mut self) -> Result<()> {
        if let Some(msg) = &self.poisoned {
            return Err(PinkDbError::Execution(format!(
                "WAL is unusable after an earlier write failure ({msg})"
            )));
        }
        self.lease.verify_lock_identity()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Record the replay-session database after the records appended so far.
    /// Called after recovery (the replay leaves the session wherever the last
    /// record did) and by the embedded path (whose session is the store's).
    pub(crate) fn set_session_db(&mut self, db: &str) {
        self.session_db = Some(db.to_string());
    }

    /// Mark extension paths as already covered by the current WAL generation.
    /// Called right after recovery: every extension the replayed records
    /// loaded (or kept mapped with zero functions) is covered by this
    /// generation, so a later append touching it must not emit a redundant
    /// `LOAD EXTENSION` prefix (replay would hit a duplicate load).
    pub(crate) fn seed_tracked_extensions(&mut self, paths: impl IntoIterator<Item = String>) {
        self.tracked_extensions.extend(paths);
    }

    /// Append one committed unit executed in session database `db_before`
    /// (when it still exists), leaving the replay session at `db_after`. When
    /// the WAL's replay session differs from `db_before`, a `USE db_before`
    /// statement is prepended so the record replays against the same database
    /// (multi-connection stores: `USE` is per-connection but the WAL is
    /// shared).
    ///
    /// `touched_extensions` lists the extension paths whose functions the
    /// statements call, `loaded_extensions` the paths the statements load
    /// themselves (`LOAD EXTENSION` text already in `stmts`). Every touched
    /// path the current generation has not covered yet — and that is not in
    /// `loaded_extensions` — is prefixed as `LOAD EXTENSION '<path>'`, so
    /// replay re-loads exactly the extensions this record needs (an
    /// extension folded away by an earlier checkpoint is re-loaded from its
    /// file; a missing file is then a hard recovery error). Both lists are
    /// marked covered only after the record was durably appended, so a failed
    /// append can never suppress a needed prefix later.
    ///
    /// `db_before` is `None` when that database no longer exists at append
    /// time: a `USE` prefix naming it could never replay (the `DROP` that
    /// removed it is already earlier in the WAL), so the record is appended
    /// bare. Callers pass `None` only when the statements are
    /// session-independent — the store lock held across execution and append
    /// guarantees it for the autocommit write path (a session-dependent
    /// statement against a vanished database would have failed at execution).
    /// The tracked replay session then becomes unknown, so the next append
    /// prefixes unconditionally.
    pub(crate) fn append_in_session(
        &mut self,
        db_before: Option<&str>,
        db_after: &str,
        stmts: &[String],
        touched_extensions: &[String],
        loaded_extensions: &[String],
        sets: Option<&FrameSets>,
    ) -> Result<()> {
        let mut rec: Vec<String> = Vec::with_capacity(stmts.len() + touched_extensions.len() + 1);
        if let Some(before) = db_before
            && !self
                .session_db
                .as_deref()
                .is_some_and(|s| s.eq_ignore_ascii_case(before))
        {
            // Quote the name: databases may hold spaces/keywords when
            // created with a quoted identifier (`CREATE DATABASE
            // "My Db"`).
            rec.push(format!("USE \"{}\"", before.replace('"', "\"\"")));
        }
        let mut prefixed: HashSet<&str> = HashSet::new();
        for path in touched_extensions {
            if self.tracked_extensions.contains(path)
                || loaded_extensions.iter().any(|p| p == path)
                || !prefixed.insert(path.as_str())
            {
                continue;
            }
            // Quote the path: `''` is the SQL escape for a single quote.
            rec.push(format!("LOAD EXTENSION '{}'", path.replace('\'', "''")));
        }
        rec.extend_from_slice(stmts);
        self.append(&rec, sets)?;
        // Covered only after a successful append.
        self.tracked_extensions
            .extend(touched_extensions.iter().cloned());
        self.tracked_extensions
            .extend(loaded_extensions.iter().cloned());
        match db_before {
            Some(_) => self.session_db = Some(db_after.to_string()),
            None => self.session_db = None,
        }
        Ok(())
    }

    /// Crash-safe checkpoint: publish the state `save_tmp` writes to `tmp` as
    /// the new checkpoint and roll the WAL generation (see module docs). The
    /// caller holds the write lease and has already refreshed (design 6.1:
    /// the state must include every committed frame of every process, or
    /// rolling the WAL would strand records in a roll that recovery then
    /// deletes). Steps 2–4 bump the epoch so readers notice the new
    /// incarnation.
    pub(crate) fn checkpoint(
        &mut self,
        guard: &LeaseGuard,
        save_tmp: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<()> {
        if self.poisoned.is_some() {
            return Err(PinkDbError::Execution(
                "cannot checkpoint: the WAL failed earlier in this session".to_string(),
            ));
        }
        // Mutation-boundary identity re-check (see [`Lease`]): a checkpoint
        // against a replaced lock file must not roll the WAL or publish.
        guard.verify_lock_identity()?;
        let tmp = tmp_path(&self.checkpoint_path);
        let roll = roll_path(&self.path, self.generation);
        // A roll of the live generation already existing proves a protocol
        // violation (a live generation is rolled exactly once); overwriting
        // it could destroy records, so refuse.
        if roll.exists() {
            let msg = format!(
                "a roll file for generation {} already exists",
                self.generation
            );
            self.poisoned = Some(msg.clone());
            return Err(PinkDbError::Execution(format!(
                "cannot checkpoint: {} ({msg})",
                roll.display()
            )));
        }
        // 1. Full state to `.tmp`, fsynced by the writer, then the directory:
        //    a surviving roll is recovery's proof that `.tmp` is complete.
        save_tmp(&tmp)?;
        sync_parent_dir(&tmp);
        // 2. Roll the generation: the old records move to a name carrying
        //    their generation (never overwritten), a fresh WAL starts at
        //    generation + 1 with a fresh `wal_id`. The fresh container cipher is
        //    prepared before the rename so a crypto failure cannot leave the
        //    WAL rolled.
        let fresh_gen = self
            .generation
            .checked_add(1)
            .ok_or_else(|| PinkDbError::Execution("WAL generation counter overflow".to_string()))?;
        let fresh_cipher = match &self.cipher {
            Some(c) => Some(c.fork(fresh_gen)?),
            None => None,
        };
        std::fs::rename(&self.path, &roll)?;
        match Self::create_file(&self.path, fresh_gen, fresh_cipher) {
            Ok((fresh, cipher, wal_id)) => {
                // Readers will now see a new incarnation: bump before/at the
                // rename visibility (a crash before this bump leaves the
                // identity re-check in `refresh` to catch it).
                guard.bump_epoch(self.last_lsn);
                self.file = fresh;
                self.cipher = cipher;
                self.generation = fresh_gen;
                self.wal_id = wal_id;
                self.last_lsn = 0;
                self.records_start = match &self.cipher {
                    Some(c) => c.header().len() as u64 + ENC_PUBLISH_BLOCK_LEN as u64,
                    None => RECORDS_START as u64,
                };
                self.slots_at = match &self.cipher {
                    Some(c) => c.header().len() as u64 + ENC_SLOT_A,
                    None => WAL_SLOT_A,
                };
                self.published_seq = 0;
                self.bytes_appended_since_gen_start = 0;
                // The rolled generation's frames are folded into the
                // checkpoint: no open base can validate against them any more
                // (their incarnation is gone), so the journal restarts empty.
                self.journal.clear();
                // The rolled generation's `LOAD EXTENSION` records are folded
                // into the checkpoint — but the checkpoint never stores an
                // extension, so the fresh generation covers nothing: the next
                // record touching an extension must be prefixed again.
                self.tracked_extensions.clear();
                // This process's memory is now exactly the checkpoint: the
                // fresh WAL contributes nothing yet.
                self.applied = Applied {
                    wal_id,
                    generation: fresh_gen,
                    wal_dev_ino: file_identity(&self.path),
                    records_start: self.records_start,
                    applied_end: self.records_start,
                    applied_lsn: 0,
                    applied_records: 0,
                    ckpt: self.applied.ckpt,
                };
            }
            Err(e) => {
                // The old WAL is intact in `.roll`; refuse further appends
                // (a reopen publishes `.tmp` and removes `.roll`).
                self.poisoned = Some(format!("failed to start a fresh WAL: {e}"));
                return Err(e);
            }
        }
        // The roll rename and the fresh WAL must be durable before the
        // publish that supersedes them.
        sync_parent_dir(&self.path);
        // 3. Publish the checkpoint atomically.
        if let Err(e) = std::fs::rename(&tmp, &self.checkpoint_path) {
            self.poisoned = Some(format!("failed to publish checkpoint: {e}"));
            return Err(e.into());
        }
        sync_parent_dir(&self.checkpoint_path);
        guard.bump_epoch(0);
        self.applied.ckpt = ckpt_meta(&self.checkpoint_path);
        // 4. The old generation is now folded into the published `.pink`. A
        //    failed unlink (or a crash before it) is harmless: the next open
        //    proves foldedness from the roll name + live generation and
        //    removes it again.
        let _ = std::fs::remove_file(&roll);
        guard.bump_epoch(0);
        Ok(())
    }
}

// ---------- payload + record encoding ----------

/// u32 framing guard: a payload this long cannot be written as one record.
fn record_len_guard(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| {
        PinkDbError::Execution(format!(
            "WAL record payload is {len} bytes, which exceeds the u32 record length limit"
        ))
    })
}

/// The catalog objects one committed unit touches: every table any of its
/// statements reads or writes, plus the databases its `CREATE/DROP DATABASE`
/// statements name. Wave 45o stores this in every WAL frame (payload v4), so
/// commit validation can compare a transaction's read∪write set with the
/// frames another writer published **without** re-binding their SQL against a
/// store that may hold uncommitted state.
///
/// Granularity is table-level, exactly like the in-process commit check
/// (`db.rs`), and the frame's *read* half is included: a transaction that
/// reads a table another frame re-wrote must conflict (serializable-style;
/// this is also what prevents write skew). Canonical form (sorted, deduped,
/// lowercased) keeps the encoded bytes deterministic.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct FrameSets {
    /// `(database, table)`, lowercased.
    tables: Vec<(String, String)>,
    /// Databases created or dropped by the frame, lowercased.
    dbs: Vec<String>,
}

impl FrameSets {
    /// Record a `(database, table)` dependency (called for both reads and
    /// writes; duplicates are removed by [`FrameSets::canonicalize`]).
    pub(crate) fn add_table(&mut self, db: &str, table: &str) {
        self.tables
            .push((db.to_ascii_lowercase(), table.to_ascii_lowercase()));
    }

    /// Record a database-level mutation (`CREATE/DROP DATABASE`).
    pub(crate) fn add_database(&mut self, db: &str) {
        self.dbs.push(db.to_ascii_lowercase());
    }

    /// The `(database, table)` pairs (canonical after
    /// [`FrameSets::canonicalize`]).
    pub(crate) fn table_pairs(&self) -> &[(String, String)] {
        &self.tables
    }

    /// Record a whole other touched-set (transaction accumulation).
    pub(crate) fn extend(&mut self, other: &FrameSets) {
        self.tables.extend_from_slice(&other.tables);
        self.dbs.extend_from_slice(&other.dbs);
    }

    /// Record only `other`'s database-level mutations. Used by COMMIT's
    /// conflict set, which takes its tables from the transaction's own
    /// read/write sets rather than its buffered statements' touched set.
    pub(crate) fn extend_databases(&mut self, other: &FrameSets) {
        self.dbs.extend_from_slice(&other.dbs);
    }

    /// Sort and dedupe (deterministic encoding and small linear comparison).
    pub(crate) fn canonicalize(&mut self) {
        self.tables.sort();
        self.tables.dedup();
        self.dbs.sort();
        self.dbs.dedup();
    }

    /// Does this set overlap `ours`? Returns a human-readable description of
    /// the first conflicting object, or `None` when the two are disjoint.
    /// Both sides must be canonicalized.
    pub(crate) fn conflict_with(&self, ours: &FrameSets) -> Option<String> {
        for db in &self.dbs {
            if ours.dbs.contains(db) {
                return Some(format!("database \"{db}\""));
            }
        }
        for (db, table) in &self.tables {
            if ours.tables.contains(&(db.clone(), table.clone())) || ours.dbs.contains(db) {
                return Some(format!("table \"{db}.{table}\""));
            }
        }
        // Our database-level `DROP DATABASE` conflicts with a frame whose
        // tables live in that database (conservative for `CREATE DATABASE`).
        for db in &ours.dbs {
            if self.tables.iter().any(|(tdb, _)| tdb == db) {
                return Some(format!("database \"{db}\""));
            }
        }
        None
    }
}

/// `sets_len` value marking a frame whose touched set is unknown (a record
/// adopted from a legacy v2/v3 upgrade). Unknown frames lie below every base
/// captured after the upgrade, so they never spuriously conflict; if one did
/// reach a validation window it is treated as conflicting (conservative).
const SETS_UNKNOWN: u32 = u32::MAX;

/// Encode the frame's touched set (payload v4, format 1):
/// `u8 ver(=1) | u32 n_tables | n*(u16 db_len | db | u16 t_len | t) |
///  u32 n_dbs | n*(u16 db_len | db)`.
fn encode_sets(sets: &FrameSets) -> Result<Vec<u8>> {
    let mut sets = sets.clone();
    sets.canonicalize();
    let mut out = Vec::with_capacity(1 + 4 + sets.dbs.len() * 10);
    out.push(1u8);
    let n = u32::try_from(sets.tables.len())
        .map_err(|_| PinkDbError::Execution("too many tables in one WAL record".to_string()))?;
    out.extend_from_slice(&n.to_le_bytes());
    for (db, table) in &sets.tables {
        for name in [db.as_str(), table.as_str()] {
            let l = u16::try_from(name.len()).map_err(|_| {
                PinkDbError::Execution("name too long for a WAL record".to_string())
            })?;
            out.extend_from_slice(&l.to_le_bytes());
            out.extend_from_slice(name.as_bytes());
        }
    }
    let n = u32::try_from(sets.dbs.len())
        .map_err(|_| PinkDbError::Execution("too many databases in one WAL record".to_string()))?;
    out.extend_from_slice(&n.to_le_bytes());
    for db in &sets.dbs {
        let l = u16::try_from(db.len())
            .map_err(|_| PinkDbError::Execution("database name too long".to_string()))?;
        out.extend_from_slice(&l.to_le_bytes());
        out.extend_from_slice(db.as_bytes());
    }
    Ok(out)
}

/// Decode the sets section written by [`encode_sets`] (payload format 1).
fn decode_sets(bytes: &[u8]) -> std::result::Result<FrameSets, String> {
    let mut pos = 0usize;
    let need = |pos: usize, n: usize| -> std::result::Result<(), String> {
        if pos.checked_add(n).is_none_or(|end| end > bytes.len()) {
            Err("truncated frame sets".to_string())
        } else {
            Ok(())
        }
    };
    need(pos, 1)?;
    if bytes[pos] != 1 {
        return Err(format!("unsupported frame-sets format {}", bytes[pos]));
    }
    pos += 1;
    let u32_at = |pos: &mut usize| -> std::result::Result<u32, String> {
        need(*pos, 4)?;
        let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().expect("4 bytes"));
        *pos += 4;
        Ok(v)
    };
    let str_at = |pos: &mut usize| -> std::result::Result<String, String> {
        need(*pos, 2)?;
        let n = u16::from_le_bytes(bytes[*pos..*pos + 2].try_into().expect("2 bytes")) as usize;
        *pos += 2;
        need(*pos, n)?;
        let s = std::str::from_utf8(&bytes[*pos..*pos + n])
            .map_err(|_| "frame-sets name is not UTF-8".to_string())?
            .to_string();
        *pos += n;
        Ok(s)
    };
    let nt = u32_at(&mut pos)? as usize;
    if nt > bytes.len() / 4 {
        return Err(format!("implausible frame-sets table count {nt}"));
    }
    let mut sets = FrameSets::default();
    for _ in 0..nt {
        let db = str_at(&mut pos)?;
        let table = str_at(&mut pos)?;
        sets.tables.push((db, table));
    }
    let nd = u32_at(&mut pos)? as usize;
    if nd > bytes.len() / 2 {
        return Err(format!("implausible frame-sets database count {nd}"));
    }
    for _ in 0..nd {
        sets.dbs.push(str_at(&mut pos)?);
    }
    if pos != bytes.len() {
        return Err("trailing bytes in frame sets".to_string());
    }
    Ok(sets)
}

/// Encode a record payload (v4 layout):
/// `u32 stmt_count | u32 sets_len | sets | stmt_count*(u32 len | UTF-8 SQL)`.
/// `sets_len` is [`SETS_UNKNOWN`] when the frame's touched set is unknown (a
/// legacy record adopted by an upgrade), else the [`encode_sets`] block's
/// byte length.
fn encode_payload(stmts: &[String], sets: Option<&FrameSets>) -> Result<Vec<u8>> {
    let count = u32::try_from(stmts.len()).map_err(|_| {
        PinkDbError::Execution("transaction has too many statements for one WAL record".to_string())
    })?;
    let sets_bytes = match sets {
        Some(s) => encode_sets(s)?,
        None => Vec::new(),
    };
    let sets_len: u32 = match sets {
        Some(_) => u32::try_from(sets_bytes.len())
            .map_err(|_| PinkDbError::Execution("frame sets too large".to_string()))?,
        None => SETS_UNKNOWN,
    };
    let mut total = 8usize + sets_bytes.len();
    for s in stmts {
        let n = u32::try_from(s.len()).map_err(|_| {
            PinkDbError::Execution("statement is too large for a WAL record".to_string())
        })?;
        total = total.checked_add(n as usize + 4).ok_or_else(|| {
            PinkDbError::Execution("transaction is too large for one WAL record".to_string())
        })?;
    }
    // Reject oversized payloads before allocating them.
    let _ = record_len_guard(total)?;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&sets_len.to_le_bytes());
    out.extend_from_slice(&sets_bytes);
    for s in stmts {
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    debug_assert_eq!(out.len(), total);
    Ok(out)
}

/// Decode `count` statements starting at `pos` (each `u32 len | UTF-8 SQL`);
/// the payload must end exactly there.
fn decode_statements(
    payload: &[u8],
    mut pos: usize,
    count: usize,
) -> std::result::Result<Vec<String>, String> {
    if count > payload.len() / 4 {
        return Err(format!("implausible statement count {count}"));
    }
    let mut stmts = Vec::with_capacity(count);
    for _ in 0..count {
        if pos.checked_add(4).is_none_or(|end| end > payload.len()) {
            return Err("truncated payload".to_string());
        }
        let n = u32::from_le_bytes(payload[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        pos += 4;
        if pos.checked_add(n).is_none_or(|end| end > payload.len()) {
            return Err("truncated statement text".to_string());
        }
        let s = std::str::from_utf8(&payload[pos..pos + n])
            .map_err(|_| "statement text is not UTF-8".to_string())?;
        stmts.push(s.to_string());
        pos += n;
    }
    if pos != payload.len() {
        return Err("trailing bytes in payload".to_string());
    }
    Ok(stmts)
}

/// Decode a legacy (v2/v3) payload: `u32 count | count*(u32 len | SQL)`. The
/// frame's touched set is unknown.
fn decode_payload_legacy(payload: &[u8]) -> std::result::Result<Vec<String>, String> {
    if payload.len() < 4 {
        return Err("truncated payload".to_string());
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
    decode_statements(payload, 4, count)
}

/// Bounds-checked v4 payload decode. `Err` means a CRC-valid record is
/// malformed (hard error on replay, never a silent drop).
fn decode_payload(payload: &[u8]) -> std::result::Result<(Vec<String>, Option<FrameSets>), String> {
    if payload.len() < 8 {
        return Err("truncated payload".to_string());
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
    let sets_len = u32::from_le_bytes(payload[4..8].try_into().expect("4 bytes"));
    let (sets, stmts_start) = if sets_len == SETS_UNKNOWN {
        (None, 8usize)
    } else {
        let n = sets_len as usize;
        if n == 0 {
            return Err("frame sets section is empty but marked known".to_string());
        }
        if 8 + n > payload.len() {
            return Err("truncated frame sets section".to_string());
        }
        (Some(decode_sets(&payload[8..8 + n])?), 8 + n)
    };
    let stmts = decode_statements(payload, stmts_start, count)?;
    Ok((stmts, sets))
}

/// Parse records from `start` onward. A short read, or an LSN that does not
/// continue the strictly increasing sequence, stops the scan (torn tail /
/// framing corruption); a malformed CRC-valid/authenticated payload is a hard
/// error. A CRC mismatch (plaintext) or authentication failure (encrypted) on
/// the last framed record is indistinguishable from a torn write, so the tail
/// policy applies there; a complete bad record with bytes after it is mid-file
/// tampering and is a hard error (both paths). `prev_lsn` is the LSN of the
/// last record before `start` (0 for a full scan) and `ordinal_base` the
/// number of records before it (the encrypted nonce ordinal base). Returns
/// the records and the offset just past the last intact record.
fn parse_records(
    data: &[u8],
    start: usize,
    cipher: Option<&RecordCipher>,
    prev_lsn: u64,
    ordinal_base: u32,
    current_payloads: bool,
) -> std::result::Result<(Vec<Record>, usize), String> {
    let mut pos = start;
    let mut prev = prev_lsn;
    // Encrypted records use their 1-based ordinal as the AEAD nonce counter.
    let mut ordinal: u32 = 0;
    let mut out = Vec::new();
    while let Some(payload_start) = pos.checked_add(RECORD_HEADER_LEN) {
        if payload_start > data.len() {
            break;
        }
        let payload_len =
            u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let lsn = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().expect("8 bytes"));
        let crc = u32::from_le_bytes(data[pos + 12..pos + 16].try_into().expect("4 bytes"));
        // The LSN is not covered by the payload CRC; a record that regresses
        // it is corrupt framing and must not extend the intact prefix (the
        // next append would otherwise reuse an LSN).
        if lsn <= prev {
            break;
        }
        // For an encrypted record `payload_len` is the plaintext length and
        // the ciphertext adds a tag.
        let body_len = match cipher {
            Some(_) => payload_len + TAG_LEN,
            None => payload_len,
        };
        let Some(end) = payload_start.checked_add(body_len) else {
            break;
        };
        if end > data.len() {
            break;
        }
        let (stmts, sets) = match cipher {
            Some(c) => {
                let next = ordinal_base
                    .checked_add(ordinal)
                    .and_then(|o| o.checked_add(1))
                    .ok_or_else(|| "encrypted WAL: record ordinal overflow".to_string())?;
                match c.open(next, lsn, &data[payload_start..end]) {
                    Some(plain) => {
                        // Encrypted records always frame `crc = 0` (GCM
                        // authentication replaces it) and the field is not
                        // part of the AAD, so a nonzero value is framing
                        // tampering: a torn write cannot produce a record
                        // that authenticates *and* carries a nonzero CRC.
                        // Hard error even at the tail (the ciphertext itself
                        // authenticated, so this is not a torn record).
                        if crc != 0 {
                            return Err(format!(
                                "corrupt WAL record at offset {pos} (lsn {lsn}): \
                                 nonzero CRC field {crc} on an encrypted record"
                            ));
                        }
                        if current_payloads {
                            decode_payload(&plain).map_err(|e| {
                                format!("corrupt WAL record at offset {pos} (lsn {lsn}): {e}")
                            })?
                        } else {
                            (
                                decode_payload_legacy(&plain).map_err(|e| {
                                    format!("corrupt WAL record at offset {pos} (lsn {lsn}): {e}")
                                })?,
                                None,
                            )
                        }
                    }
                    None => {
                        if end == data.len() {
                            // Tail: an auth failure here is indistinguishable
                            // from a torn write — the tail policy applies.
                            break;
                        }
                        return Err(format!(
                            "corrupt WAL record at offset {pos} (lsn {lsn}): failed authentication \
                             with more records after it (tampered or corrupted mid-file)"
                        ));
                    }
                }
            }
            None => {
                let payload = &data[payload_start..end];
                if crc32(payload) != crc {
                    if end == data.len() {
                        // Tail: a CRC mismatch here is indistinguishable from a
                        // torn write — the tail policy applies.
                        break;
                    }
                    return Err(format!(
                        "corrupt WAL record at offset {pos} (lsn {lsn}): CRC mismatch \
                         with more records after it (tampered or corrupted mid-file)"
                    ));
                }
                if current_payloads {
                    decode_payload(payload).map_err(|e| {
                        format!("corrupt WAL record at offset {pos} (lsn {lsn}): {e}")
                    })?
                } else {
                    (
                        decode_payload_legacy(payload).map_err(|e| {
                            format!("corrupt WAL record at offset {pos} (lsn {lsn}): {e}")
                        })?,
                        None,
                    )
                }
            }
        };
        out.push(Record { lsn, stmts, sets });
        prev = lsn;
        ordinal += 1;
        pos = end;
    }
    Ok((out, pos))
}

/// Rewrite an encrypted WAL whose tail was discarded or whose container is
/// legacy: a fresh nonce prefix and a fresh `wal_id` (a new file incarnation;
/// nonce reuse after a tail loss is forbidden), same key, KDF parameters and
/// generation, the kept records re-sealed in order (their payloads re-encoded
/// in the current format, preserving unknown touched sets), publish slots
/// seeded with the last intact LSN. Temp + rename, so a crash re-runs
/// recovery from the same on-disk state. Returns the cipher positioned after
/// the re-sealed records.
fn rewrite_encrypted(
    path: &Path,
    generation: u64,
    records: &[Record],
    old: &RecordCipher,
) -> Result<RecordCipher> {
    let wal_id = fresh_wal_id();
    let mut fresh = old.fork(generation)?;
    let last_lsn = records.last().map(|r| r.lsn).unwrap_or(0);
    let rewrite = rewrite_path(path);
    {
        crate::fsutil::tighten(&rewrite);
        let mut f = crate::fsutil::create_private(&rewrite)?;
        f.write_all(fresh.header())?;
        f.write_all(&encode_publish_block(
            &fresh,
            &wal_id,
            generation,
            Some((1, last_lsn)),
        )?)?;
        for rec in records {
            let payload = encode_payload(&rec.stmts, rec.sets.as_ref())?;
            let len = record_len_guard(payload.len())?;
            let ct = fresh.seal(rec.lsn, &payload)?;
            f.write_all(&len.to_le_bytes())?;
            f.write_all(&rec.lsn.to_le_bytes())?;
            f.write_all(&0u32.to_le_bytes())?;
            f.write_all(&ct)?;
        }
        f.sync_all()?;
    }
    std::fs::rename(&rewrite, path)?;
    sync_parent_dir(path);
    Ok(fresh)
}

/// `<wal>.<gen>.upgrade`: temp name for the in-place legacy→v4 plaintext
/// rewrite. Deliberately not matched by `scan_rolls` (only `.roll` names
/// are), so a crash mid-upgrade leaves an ignorable temp (design row R1).
fn upgrade_path(wal: &Path, generation: u64) -> PathBuf {
    let mut s = wal.as_os_str().to_os_string();
    s.push(format!(".{generation}.upgrade"));
    PathBuf::from(s)
}

/// Rewrite a legacy plaintext WAL (v2 or v3) in place as v4 under the lease:
/// fresh `wal_id`, publish slot A seeded with the last intact LSN, the intact
/// records re-encoded in the v4 payload layout (their touched sets are
/// unknown). A torn tail is simply not copied. Temp + rename; a crash re-runs
/// the upgrade (R1).
fn upgrade_plain(path: &Path, data: &[u8], generation: u64, records_start: usize) -> Result<()> {
    let (records, good_end) =
        parse_records(data, records_start, None, 0, 0, false).map_err(PinkDbError::Execution)?;
    let last_lsn = records.last().map(|r| r.lsn).unwrap_or(0);
    let wal_id = fresh_wal_id();
    let mut out = Vec::with_capacity(HEADER_LEN + (good_end - records_start));
    out.extend_from_slice(&Wal::header_for(generation, &wal_id));
    let slot = slot_bytes(1, last_lsn, WAL_SLOT_DOMAIN);
    out[WAL_SLOT_A as usize..WAL_SLOT_A as usize + SLOT_LEN].copy_from_slice(&slot);
    for rec in &records {
        let payload = encode_payload(&rec.stmts, None)?;
        let len = record_len_guard(payload.len())?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&rec.lsn.to_le_bytes());
        out.extend_from_slice(&crc32(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
    }
    let upgrade = upgrade_path(path, generation);
    {
        crate::fsutil::tighten(&upgrade);
        let mut f = crate::fsutil::create_private(&upgrade)?;
        f.write_all(&out)?;
        f.sync_all()?;
    }
    std::fs::rename(&upgrade, path)?;
    sync_parent_dir(path);
    Ok(())
}

/// True when the file is an older format that must be upgraded under the
/// lease before it is replayed: plaintext v2/v3, or an encrypted WAL
/// container older than the current version (which carries the per-frame
/// touched sets).
fn wal_legacy_format(path: &Path) -> Result<bool> {
    let mut f = File::open(path)?;
    let mut buf = [0u8; 512];
    let n = f.read(&mut buf)?;
    let buf = &buf[..n];
    if crypto::is_container(buf) {
        return Ok(match crypto::parse_container_header(buf) {
            Ok(h) => h.version < crypto::WAL_CONTAINER_VERSION,
            Err(_) => false, // torn: recovery recreates it
        });
    }
    Ok(buf.len() >= 12
        && &buf[..8] == WAL_MAGIC
        && matches!(
            u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes")),
            WAL_VERSION_V2 | WAL_VERSION_V3
        ))
}

/// Upgrade a legacy WAL in place (plaintext v2/v3 → v4; an older container →
/// the current WAL container version with a fresh nonce prefix and publish
/// block), under the lease.
fn upgrade_legacy_wal(
    path: &Path,
    data: &[u8],
    header: &WalHeader,
    key: Option<&EncryptionKey>,
) -> Result<()> {
    if let Some(enc) = &header.enc {
        let Some(k) = key else {
            return Err(crypto::encrypted_error(&path.display().to_string()));
        };
        let (old, _) = RecordCipher::from_header(data, k)?;
        let (records, _) = parse_records(data, enc.payload_start, Some(&old), 0, 0, false)
            .map_err(PinkDbError::Execution)?;
        rewrite_encrypted(path, header.generation, &records, &old)?;
    } else {
        upgrade_plain(path, data, header.generation, header.records_start)?;
    }
    Ok(())
}

/// Cheap refresh-path classification: is the live WAL missing or is its
/// header unusable (so the recovery state machine must run)? A hard error (a
/// record-bearing file with invalid magic, version, or CRC) is returned as
/// `Err`; the caller then surfaces it without touching anything.
fn wal_header_invalid(path: &Path) -> Result<bool> {
    let mut buf = [0u8; 512];
    let n = match File::open(path) {
        Ok(mut f) => f.read(&mut buf)?,
        Err(_) => return Ok(true),
    };
    let data = &buf[..n];
    #[cfg(not(feature = "encryption"))]
    refuse_container(path, data)?;
    match parse_wal_header(data) {
        Ok(_) => Ok(false),
        Err(e) => {
            if header_torn(data.len(), &e) {
                Ok(true)
            } else {
                Err(PinkDbError::Execution(header_error(path, &e)))
            }
        }
    }
}

/// The live WAL's on-disk identity, read with one open + header read + fstat:
/// incarnation `(generation, wal_id)`, where its records start, its file identity,
/// and its length. Used for the refresh and the optimistic-commit identity
/// comparisons; the file must already be in a current format (callers
/// upgrade legacy files first). Reads only the header region, never the whole
/// WAL.
fn read_wal_identity(path: &Path) -> Result<WalIdentity> {
    let mut f = File::open(path)?;
    let meta = f.metadata()?;
    // Enough for any container header (max salt + key-check) plus the
    // authenticated 144-byte publish block, or a plaintext v4 header.
    let mut buf = [0u8; 512];
    let n = f.read(&mut buf)?;
    let h =
        parse_wal_header(&buf[..n]).map_err(|e| PinkDbError::Execution(header_error(path, &e)))?;
    Ok(WalIdentity {
        generation: h.generation,
        wal_id: h.wal_id,
        records_start: h.records_start as u64,
        dev_ino: Some(identity_of(&meta)),
        len: meta.len(),
    })
}

/// See [`read_wal_identity`].
#[derive(Debug, Clone, Copy)]
struct WalIdentity {
    generation: u64,
    wal_id: WalId,
    #[allow(dead_code)] // kept for error context / future checks
    records_start: u64,
    dev_ino: Option<(u64, u64)>,
    #[allow(dead_code)] // the fast path uses `file_meta`; kept for error context
    len: u64,
}

/// Scan the live WAL (creating it when absent) and return every intact
/// record plus its positioning. Torn tails are truncated in place
/// (plaintext) or rebuilt under a fresh nonce prefix (encrypted); legacy
/// formats are upgraded first (then the scan re-runs on the new file).
fn scan_wal(
    path: &Path,
    key: Option<&EncryptionKey>,
    recreate_gen: u64,
    rebuild_encrypted: bool,
) -> Result<OpenedWal> {
    let fresh_cipher = |generation: u64| -> Result<Option<RecordCipher>> {
        match key {
            Some(k) => Ok(Some(RecordCipher::create(k, generation)?)),
            None => Ok(None),
        }
    };
    let created = |generation: u64, cipher: Option<RecordCipher>| -> Result<OpenedWal> {
        let (file, cipher, wal_id) = Wal::create_file(path, generation, cipher)?;
        let records_start = match &cipher {
            Some(c) => c.header().len() as u64 + ENC_PUBLISH_BLOCK_LEN as u64,
            None => RECORDS_START as u64,
        };
        let slots_at = match &cipher {
            Some(c) => c.header().len() as u64 + ENC_SLOT_A,
            None => WAL_SLOT_A,
        };
        Ok(OpenedWal {
            file,
            generation,
            wal_id,
            records_start,
            slots_at,
            published_seq: 0,
            cipher,
            records: Vec::new(),
            truncated: 0,
            upgraded: false,
        })
    };
    if !path.exists() {
        return created(recreate_gen, fresh_cipher(recreate_gen)?);
    }
    let data = std::fs::read(path)?;
    #[cfg(not(feature = "encryption"))]
    refuse_container(path, &data)?;
    let mut header = match parse_wal_header(&data) {
        Ok(h) => h,
        Err(e) if header_torn(data.len(), &e) => {
            // A torn creation (crash while creating/recreating the WAL): no
            // record can have been acknowledged yet, so the file is
            // recreated. A parse failure on a longer file is corruption and
            // stays a hard error.
            let mut out = created(recreate_gen, fresh_cipher(recreate_gen)?)?;
            out.truncated = data.len() as u64;
            return Ok(out);
        }
        Err(e) => return Err(PinkDbError::Execution(header_error(path, &e))),
    };
    // At-rest mode must match the key exactly (both directions).
    let cipher = match (&header.enc, key) {
        (None, Some(_)) => return Err(not_encrypted_with_key(path)),
        (Some(_), None) => return Err(crypto::encrypted_error(&path.display().to_string())),
        (Some(_), Some(k)) => {
            // A wrong key fails the key-check here, before anything runs.
            let (c, _) = RecordCipher::from_header(&data, k)?;
            Some(c)
        }
        (None, None) => None,
    };
    // L-5: a current publish block is authenticated once the cipher exists.
    // A tampered static identity is a hard error; a tampered slot is ignored
    // (frames are authoritative).
    if let Some(c) = &cipher
        && let Some(enc) = &header.enc
        && enc.version >= crypto::WAL_CONTAINER_VERSION
    {
        let end = enc.payload_start + ENC_PUBLISH_BLOCK_LEN;
        if data.len() < end {
            return Err(PinkDbError::Execution(
                "encrypted WAL: truncated publish block".to_string(),
            ));
        }
        let (wal_id, published_seq) =
            open_publish_block(c, &data[enc.payload_start..end], enc.context)
                .map_err(PinkDbError::Execution)?;
        header.wal_id = wal_id;
        header.published_seq = published_seq;
    }
    if header.legacy {
        // Upgrade under the lease, then re-scan the (now current) file.
        upgrade_legacy_wal(path, &data, &header, key)?;
        let mut out = scan_wal(path, key, header.generation, rebuild_encrypted)?;
        out.upgraded = true;
        return Ok(out);
    }
    let (records, good_end) = parse_records(
        &data,
        header.records_start,
        cipher.as_ref(),
        0,
        0,
        !header.legacy,
    )
    .map_err(PinkDbError::Execution)?;
    let truncated = (data.len() - good_end) as u64;
    let record_count = u32::try_from(records.len())
        .map_err(|_| PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string()))?;
    let (file, cipher, wal_id, published_seq, records_start, slots_at, upgraded) = match cipher {
        Some(old) if truncated > 0 || rebuild_encrypted => {
            // Rebuild under a fresh nonce prefix and a fresh wal_id.
            //
            // `Wal::open` (once per process open) always rebuilds, keeping
            // the wave-22 policy: a file cut exactly at a record boundary is
            // byte-identical to a cleanly shorter one, so a counter resynced
            // from the record count could hand out a discarded record's
            // `(key, nonce, AAD)` again — real keystream reuse, not a
            // theoretical one (the integration test detects it). This is a
            // deliberate deviation from the wave-45m design's "narrow the
            // rebuild to creation and upgrade only"; multi-process appends
            // stay safe because every appender re-checks the WAL identity
            // under the lease (`needs_scan`) and re-opens a replaced file
            // before appending, and every refresh resyncs the cipher counter
            // from the on-disk record count. Refreshes (`reopen_under_lease`)
            // pass `rebuild_encrypted = false` and keep the prefix, except
            // when they actually discard bytes (a crashed writer's torn
            // tail), where the same reuse argument forces a rebuild.
            let mut fresh = rewrite_encrypted(path, header.generation, &records, &old)?;
            fresh.set_next_counter(record_count.checked_add(1).ok_or_else(|| {
                PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
            })?);
            let file = OpenOptions::new().read(true).append(true).open(path)?;
            let rebuilt = std::fs::read(path)?;
            let h2 = parse_wal_header(&rebuilt)
                .map_err(|e| PinkDbError::Execution(header_error(path, &e)))?;
            (
                file,
                Some(fresh),
                h2.wal_id,
                h2.published_seq,
                h2.records_start as u64,
                h2.slots_at,
                true,
            )
        }
        Some(mut old) => {
            // Same prefix, positioned after the existing records.
            old.set_next_counter(record_count.checked_add(1).ok_or_else(|| {
                PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
            })?);
            let file = OpenOptions::new().read(true).append(true).open(path)?;
            (
                file,
                Some(old),
                header.wal_id,
                header.published_seq,
                header.records_start as u64,
                header.slots_at,
                false,
            )
        }
        None => {
            let file = OpenOptions::new().read(true).append(true).open(path)?;
            if truncated > 0 {
                file.set_len(good_end as u64)?;
                file.sync_all()?;
            }
            (
                file,
                None,
                header.wal_id,
                header.published_seq,
                header.records_start as u64,
                header.slots_at,
                false,
            )
        }
    };
    let _ = data;
    Ok(OpenedWal {
        file,
        generation: header.generation,
        wal_id,
        records_start,
        slots_at,
        published_seq,
        cipher,
        records,
        truncated,
        upgraded,
    })
}

// ---------- file naming + header parsing ----------

fn wal_path(checkpoint: &Path) -> PathBuf {
    let mut s = checkpoint.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

fn lock_path(checkpoint: &Path) -> PathBuf {
    let mut s = checkpoint.as_os_str().to_os_string();
    s.push(".lock");
    PathBuf::from(s)
}

fn tmp_path(checkpoint: &Path) -> PathBuf {
    let mut s = checkpoint.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// `<wal>.rewrite`: temp file for the atomic rewrite of a truncated encrypted
/// WAL (never matched by `scan_rolls`, which only looks at `.roll` names).
fn rewrite_path(wal: &Path) -> PathBuf {
    let mut s = wal.as_os_str().to_os_string();
    s.push(".rewrite");
    PathBuf::from(s)
}

/// `<wal>.<gen>.roll`: the generation in the name is what makes stale rolls
/// distinguishable from a roll of the live generation.
fn roll_path(wal: &Path, generation: u64) -> PathBuf {
    let mut s = wal.as_os_str().to_os_string();
    s.push(format!(".{generation}{ROLL_SUFFIX}"));
    PathBuf::from(s)
}

/// Parse a live WAL header: plaintext v2-v4, container v1-v4. No key
/// needed (the container key-check is verified separately when a cipher is
/// built). `legacy` marks a file whose record payloads predate the current
/// per-frame touched sets: those are upgraded under the lease before replay.
fn parse_wal_header(data: &[u8]) -> std::result::Result<WalHeader, HeaderError> {
    if crypto::is_container(data) {
        #[cfg(not(feature = "encryption"))]
        {
            // Refused before this point by `refuse_container`; keep a clean
            // error for any other caller.
            return Err(HeaderError::BadMagic);
        }
        #[cfg(feature = "encryption")]
        {
            // An unsupported container version is never a torn creation: a
            // newer build may have written records this one cannot parse, so
            // refuse with a hard error rather than recreate the file.
            if data.len() >= 12 {
                let version = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
                if !crypto::is_known_container_version(version) {
                    return Err(HeaderError::ContainerVersion(version));
                }
            }
            let enc = crypto::parse_container_header(data)
                .map_err(|e| HeaderError::Container(e.to_string()))?;
            let payload_start = enc.payload_start;
            if enc.version < 2 {
                // Container v1: no publish block; records start right after
                // the header. Upgraded under the lease by `scan_wal`.
                return Ok(WalHeader {
                    generation: enc.context,
                    wal_id: [0u8; 16],
                    records_start: payload_start,
                    slots_at: 0,
                    published_seq: 0,
                    enc: Some(enc),
                    legacy: true,
                });
            }
            let (block_len, slot_a, slot_b) = if enc.version >= crypto::WAL_CONTAINER_VERSION {
                (ENC_PUBLISH_BLOCK_LEN, ENC_SLOT_A, ENC_SLOT_B)
            } else {
                (
                    LEGACY_ENC_PUBLISH_BLOCK_LEN,
                    LEGACY_ENC_SLOT_A,
                    LEGACY_ENC_SLOT_B,
                )
            };
            if data.len() < payload_start + block_len {
                return Err(HeaderError::BadPublishBlock {
                    payload_start,
                    block_len,
                    what: "truncated publish block".to_string(),
                });
            }
            let block = &data[payload_start..payload_start + block_len];
            let mut wal_id = [0u8; 16];
            wal_id.copy_from_slice(&block[..16]);
            let published_seq;
            if enc.version >= crypto::WAL_CONTAINER_VERSION {
                // The static meta and the slots are authenticated later, once
                // the cipher (and therefore the key) exists. Until then the
                // unverified hint is zero; `scan_wal` overwrites it.
                published_seq = 0;
                let block_version = u16::from_le_bytes(block[24..26].try_into().expect("2 bytes"));
                if block_version != ENC_PUBLISH_BLOCK_VERSION || block[26..28] != [0u8; 2] {
                    return Err(HeaderError::Reserved);
                }
            } else {
                let crc = u32::from_le_bytes(block[16..20].try_into().expect("4 bytes"));
                if crc != publish_block_crc(&wal_id, enc.context, enc.version) {
                    return Err(HeaderError::BadPublishBlock {
                        payload_start,
                        block_len,
                        what: "publish block CRC mismatch".to_string(),
                    });
                }
                if block[20..24] != [0u8; 4] || block[72..88] != [0u8; 16] {
                    return Err(HeaderError::Reserved);
                }
                published_seq = slot_max_in(block, slot_a as usize, WAL_SLOT_DOMAIN);
            }
            if slot_b <= slot_a {
                return Err(HeaderError::Reserved);
            }
            return Ok(WalHeader {
                generation: enc.context,
                wal_id,
                records_start: payload_start + block_len,
                slots_at: payload_start as u64 + slot_a,
                published_seq,
                legacy: enc.version < crypto::WAL_CONTAINER_VERSION,
                enc: Some(enc),
            });
        }
    }
    if data.len() < V2_RECORDS_START {
        return Err(HeaderError::TooShort);
    }
    if &data[..8] != WAL_MAGIC {
        return Err(HeaderError::BadMagic);
    }
    let version = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
    match version {
        WAL_VERSION_V2 | WAL_VERSION_V3 => {
            // A corrupted current-format header whose version word was
            // flipped to 2 or 3 (one bit!) would otherwise be mistaken for a
            // genuine older file: its records would be misparsed from the
            // wrong payload layout and could be silently dropped by the
            // upgrade. The current header's CRC over `magic..wal_id` proves
            // it: correct the version word back and check the CRC (plus the
            // zero reserved tail). A genuine older file cannot satisfy this
            // except with cryptographic luck (2^-32); its own header has no
            // checksum to check (v3 headers do carry a CRC, but computed with
            // version 3 — correcting to the *current* version is what
            // distinguishes the two).
            if data.len() >= HEADER_LEN {
                let mut corrected = [0u8; HEADER_LEN];
                corrected.copy_from_slice(&data[..HEADER_LEN]);
                corrected[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
                let crc = u32::from_le_bytes(corrected[36..40].try_into().expect("4 bytes"));
                if data[88..96] == [0u8; 8] && crc == crc32(&corrected[..36]) {
                    return Err(HeaderError::BadCrc);
                }
            }
            Ok(WalHeader {
                generation: u64::from_le_bytes(data[12..20].try_into().expect("8 bytes")),
                wal_id: [0u8; 16],
                records_start: if version == WAL_VERSION_V2 {
                    V2_RECORDS_START
                } else {
                    HEADER_LEN
                },
                slots_at: 0,
                published_seq: 0,
                enc: None,
                legacy: true,
            })
        }
        WAL_VERSION => {
            if data.len() < HEADER_LEN {
                return Err(HeaderError::TooShort);
            }
            let crc = u32::from_le_bytes(data[36..40].try_into().expect("4 bytes"));
            if crc != crc32(&data[..36]) {
                return Err(HeaderError::BadCrc);
            }
            if data[88..96] != [0u8; 8] {
                return Err(HeaderError::Reserved);
            }
            let mut wal_id = [0u8; 16];
            wal_id.copy_from_slice(&data[20..36]);
            let published_seq = slot_max_in(data, WAL_SLOT_A as usize, WAL_SLOT_DOMAIN);
            Ok(WalHeader {
                generation: u64::from_le_bytes(data[12..20].try_into().expect("8 bytes")),
                wal_id,
                records_start: HEADER_LEN,
                slots_at: WAL_SLOT_A,
                published_seq,
                enc: None,
                legacy: false,
            })
        }
        _ => Err(HeaderError::BadVersion(version)),
    }
}

fn header_error(path: &Path, e: &HeaderError) -> String {
    match e {
        HeaderError::TooShort => format!("{}: WAL header is truncated", path.display()),
        HeaderError::BadMagic => {
            format!("{} is not a pinkdb WAL file (bad magic)", path.display())
        }
        HeaderError::BadVersion(v) => {
            format!("WAL format version {v} is not supported (want {WAL_VERSION})")
        }
        HeaderError::BadCrc => format!(
            "{}: WAL header checksum mismatch (corrupt header)",
            path.display()
        ),
        HeaderError::Reserved => format!(
            "{}: WAL header reserved bytes are nonzero (corrupt header)",
            path.display()
        ),
        #[cfg(feature = "encryption")]
        HeaderError::Container(msg) => format!("{}: {msg}", path.display()),
        #[cfg(feature = "encryption")]
        HeaderError::ContainerVersion(v) => format!(
            "{}: unsupported container version {v} (a newer build wrote this file)",
            path.display()
        ),
        #[cfg(feature = "encryption")]
        HeaderError::BadPublishBlock { what, .. } => {
            format!("{}: {what}", path.display())
        }
    }
}

/// May a failed header parse on a file of this length be treated as a torn
/// creation (recreated before anything is replayed), rather than
/// record-bearing corruption (a hard error)? A version mismatch is never
/// torn: a v1 or future-version file may carry records (the old rule only
/// recreated invalid magic/version on a file at most one header long).
fn header_torn(data_len: usize, e: &HeaderError) -> bool {
    match e {
        HeaderError::TooShort
        | HeaderError::BadMagic
        | HeaderError::BadCrc
        | HeaderError::Reserved => data_len <= HEADER_LEN,
        HeaderError::BadVersion(_) => false,
        #[cfg(feature = "encryption")]
        HeaderError::ContainerVersion(_) => false,
        #[cfg(feature = "encryption")]
        HeaderError::Container(_) => data_len <= crypto::MAX_HEADER_LEN + ENC_PUBLISH_BLOCK_LEN,
        #[cfg(feature = "encryption")]
        HeaderError::BadPublishBlock {
            payload_start,
            block_len,
            ..
        } => data_len <= payload_start + block_len,
    }
}

/// Reject an encrypted WAL in a build without the `encryption` feature,
/// before any torn-header recreation could silently replace the container.
#[cfg(not(feature = "encryption"))]
fn refuse_container(path: &Path, data: &[u8]) -> Result<()> {
    if crypto::is_container(data) {
        return Err(crypto::encrypted_error(&path.display().to_string()));
    }
    Ok(())
}

/// A keyed open over a plaintext artifact: refusing beats silently ignoring
/// the key (which would keep writing plaintext).
fn not_encrypted_with_key(path: &Path) -> PinkDbError {
    PinkDbError::Execution(format!(
        "{}: file is not encrypted, but a key was supplied; open it without a key",
        path.display()
    ))
}

fn parent_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Best-effort directory fsync so a rename survives a crash. (Portability:
/// opening a directory for `sync_all` is not supported everywhere, so the
/// failure is ignored — the file-level fsyncs remain.)
pub(crate) fn sync_parent_dir(path: &Path) {
    if let Ok(f) = File::open(parent_dir(path)) {
        let _ = f.sync_all();
    }
}

/// The live WAL's header state, before anything is replayed: `None` when the
/// file is absent; a torn header reports no generation and no records (the
/// file is recreated); invalid magic/version on a file that carries payload
/// bytes is a hard error. For an encrypted WAL the records are authenticated
/// (so a wrong key fails here); a key over a plaintext WAL (or vice versa) is
/// refused.
fn probe_wal(path: &Path, key: Option<&EncryptionKey>) -> Result<Option<WalProbe>> {
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read(path)?;
    #[cfg(not(feature = "encryption"))]
    refuse_container(path, &data)?;
    match parse_wal_header(&data) {
        Ok(h) => {
            let cipher = match (&h.enc, key) {
                (None, Some(_)) => return Err(not_encrypted_with_key(path)),
                (Some(_), None) => {
                    return Err(crypto::encrypted_error(&path.display().to_string()));
                }
                (Some(_), Some(k)) => {
                    let (c, _) = RecordCipher::from_header(&data, k)?;
                    Some(c)
                }
                (None, None) => None,
            };
            let (records, _) =
                parse_records(&data, h.records_start, cipher.as_ref(), 0, 0, !h.legacy)
                    .map_err(PinkDbError::Execution)?;
            Ok(Some(WalProbe {
                generation: Some(h.generation),
                has_records: !records.is_empty(),
            }))
        }
        Err(e) if header_torn(data.len(), &e) => Ok(Some(WalProbe {
            generation: None,
            has_records: false,
        })),
        Err(e) => Err(PinkDbError::Execution(header_error(path, &e))),
    }
}

/// Every rolled generation next to `wal`, sorted by generation. The
/// pre-generation fixed name `<wal>.roll` is refused: without a generation it
/// cannot be proven stale, and deleting it could destroy un-folded records.
fn scan_rolls(wal: &Path) -> Result<Vec<Roll>> {
    let Some(file_name) = wal.file_name().and_then(|n| n.to_str()) else {
        return Ok(Vec::new());
    };
    let prefix = format!("{file_name}.");
    let legacy = format!("{file_name}.roll");
    let Ok(entries) = std::fs::read_dir(parent_dir(wal)) else {
        return Ok(Vec::new());
    };
    let mut rolls = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name == legacy {
            return Err(PinkDbError::Execution(format!(
                "{} uses the pre-generation roll name; it cannot be classified as folded \
                 or unfolded — remove it manually after checking your data",
                entry.path().display()
            )));
        }
        if let Some(mid) = name
            .strip_prefix(&prefix)
            .and_then(|r| r.strip_suffix(ROLL_SUFFIX))
            && let Ok(generation) = mid.parse::<u64>()
        {
            rolls.push(Roll {
                generation,
                path: entry.path(),
            });
        }
    }
    rolls.sort_by_key(|r| r.generation);
    for r in &rolls {
        check_roll_header(r)?;
    }
    Ok(rolls)
}

/// A roll file must self-identify with the generation in its name: the name
/// drives the foldedness decision, so a mismatch means the files were
/// tampered with and nothing may be deleted. A roll whose header cannot be
/// read (shorter than a header, or garbage) is accepted: its records are
/// never replayed from here — recovery publishes the `.tmp` that contains
/// them or proves the generation older than the live WAL. Encrypted rolls
/// parse their generation from the container's plaintext context field, so
/// the self-identification check holds for both formats (no key needed).
fn check_roll_header(roll: &Roll) -> Result<()> {
    let mut f = match File::open(&roll.path) {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };
    let mut buf = vec![0u8; crypto::MAX_HEADER_LEN];
    let n = match f.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return Ok(()),
    };
    buf.truncate(n);
    match parse_wal_header(&buf) {
        Ok(h) if h.generation == roll.generation => Ok(()),
        Ok(h) => Err(PinkDbError::Execution(format!(
            "cannot recover: roll file {} is named generation {} but its header says {}; \
             refusing to delete it",
            roll.path.display(),
            roll.generation,
            h.generation
        ))),
        Err(_) => Ok(()),
    }
}

fn inconsistent_roll(path: &Path, generation: u64, live_gen: u64) -> PinkDbError {
    PinkDbError::Execution(format!(
        "cannot recover: rolled WAL generation {generation} ({}) does not precede the live WAL \
         generation {live_gen}; refusing to delete records that may not be folded into \
         the checkpoint",
        path.display()
    ))
}

/// Remove provably-folded rolls. Best-effort: a roll that survives is
/// harmless, because the next open reaches the same foldedness decision from
/// the same file names and generations. Returns how many were removed.
fn remove_rolls(rolls: &[Roll]) -> u64 {
    let mut removed = 0;
    for r in rolls {
        if std::fs::remove_file(&r.path).is_ok() {
            removed += 1;
        }
    }
    removed
}

/// The generation after every roll on disk (1 when there are none). Refuses
/// to wrap past `u64::MAX`: a crafted roll name at the top of the range must
/// yield a clean error, never a panic.
fn next_gen_after_rolls(rolls: &[Roll]) -> Result<u64> {
    match rolls.iter().map(|r| r.generation).max() {
        Some(g) => g.checked_add(1).ok_or_else(|| {
            PinkDbError::Execution(format!(
                "cannot recover: rolled WAL generation {g} is the highest representable; \
                 the generation sequence cannot continue"
            ))
        }),
        None => Ok(1),
    }
}

/// Recreate the live WAL when it is missing or has a torn header, continuing
/// the generation sequence past every roll so future rolls stay unique. With
/// `key` the recreated WAL is encrypted.
fn ensure_live_wal(
    wal: &Path,
    live_gen: Option<u64>,
    rolls: &[Roll],
    key: Option<&EncryptionKey>,
) -> Result<()> {
    if live_gen.is_some() {
        return Ok(());
    }
    let generation = next_gen_after_rolls(rolls)?;
    let cipher = match key {
        Some(k) => Some(RecordCipher::create(k, generation)?),
        None => None,
    };
    Wal::create_file(wal, generation, cipher).map(|_| ())
}
/// A keyed store must never publish over (or consume) an artifact of the
/// other at-rest mode: `.pink`/`.tmp` are checked by their first 8 bytes,
/// which needs no key. An encrypted artifact names the missing support (or
/// key) with the standard feature-aware wording.
fn check_artifact_mode(path: &Path, key: Option<&EncryptionKey>) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut f = File::open(path)?;
    let mut magic = [0u8; 8];
    let n = f.read(&mut magic)?;
    let encrypted = n == 8 && &magic == crypto::ENC_MAGIC;
    if encrypted == key.is_some() {
        return Ok(());
    }
    Err(if encrypted {
        crypto::encrypted_error(&path.display().to_string())
    } else {
        not_encrypted_with_key(path)
    })
}

/// A keyed open must never publish (or consume) an artifact of the other
/// at-rest mode: the recovery would silently flip the encryption of the
/// checkpoint.
fn check_recovery_mode(ckpt: &Path, tmp: &Path, key: Option<&EncryptionKey>) -> Result<()> {
    for path in [ckpt, tmp] {
        check_artifact_mode(path, key)?;
    }
    Ok(())
}

/// Finish an interrupted checkpoint (see the module docs for the full state
/// machine). Never removes anything before the recovered state is durable.
fn recover_checkpoint(
    ckpt: &Path,
    wal: &Path,
    tmp: &Path,
    rolls: &[Roll],
    probe: Option<&WalProbe>,
    info: &mut RecoveryInfo,
    key: Option<&EncryptionKey>,
) -> Result<()> {
    check_recovery_mode(ckpt, tmp, key)?;
    let live_gen = probe.and_then(|p| p.generation);
    let live_records = probe.map(|p| p.has_records).unwrap_or(false);

    if !tmp.exists() {
        if !rolls.is_empty() {
            // No `.tmp` means the publish that removes it completed, so every
            // rolled generation is folded into `.pink`. Require the target
            // checkpoint and cross-check generations before deleting.
            if !ckpt.exists() {
                return Err(PinkDbError::Execution(format!(
                    "cannot recover {}: rolled WAL generation(s) exist but the checkpoint \
                     is missing; refusing to delete records that may not be folded in",
                    ckpt.display()
                )));
            }
            if let Some(lgen) = live_gen {
                for r in rolls {
                    if r.generation >= lgen {
                        return Err(inconsistent_roll(&r.path, r.generation, lgen));
                    }
                }
            }
            info.stale_rolls_removed += remove_rolls(rolls);
        }
        return ensure_live_wal(wal, live_gen, rolls, key);
    }

    if live_records {
        // The live WAL still holds authoritative records, so `.tmp` belongs to
        // a checkpoint that never reached its roll — it may even be a partial
        // write. Discard it (skipping the removal repeats the same decision);
        // rolls present must be older generations already folded into `.pink`.
        let lgen = live_gen.expect("a WAL with intact records has a valid header");
        if !rolls.is_empty() && !ckpt.exists() {
            return Err(PinkDbError::Execution(format!(
                "cannot recover {}: rolled WAL generation(s) exist but the checkpoint \
                 is missing; refusing to delete records that may not be folded in",
                ckpt.display()
            )));
        }
        for r in rolls {
            if r.generation >= lgen {
                return Err(inconsistent_roll(&r.path, r.generation, lgen));
            }
        }
        let _ = std::fs::remove_file(tmp);
        info.stale_rolls_removed += remove_rolls(rolls);
        return ensure_live_wal(wal, live_gen, rolls, key);
    }

    if rolls.is_empty() {
        // Crash during step 1 (or a redundant `.tmp`): `.tmp` is not provably
        // complete, so it must never be published. The live WAL is the
        // authority; skipping the removal repeats the same decision.
        let _ = std::fs::remove_file(tmp);
        return ensure_live_wal(wal, live_gen, rolls, key);
    }

    // `.tmp` + a roll: the roll rename happens only after `.tmp` was fully
    // written and fsynced, so `.tmp` is complete and contains the roll's
    // records. Publish it *first* — this is the crash window where an older
    // recovery replayed the roll into memory, deleted `.tmp`/`.roll`, and
    // lost everything on the next open.
    let max_roll = rolls
        .iter()
        .map(|r| r.generation)
        .max()
        .expect("rolls is non-empty");
    if let Some(lgen) = live_gen
        && max_roll >= lgen
    {
        let path = rolls
            .iter()
            .find(|r| r.generation == max_roll)
            .map(|r| r.path.as_path())
            .expect("max comes from the list");
        return Err(inconsistent_roll(path, max_roll, lgen));
    }
    if let Err(e) = std::fs::rename(tmp, ckpt) {
        return Err(PinkDbError::Io(e));
    }
    sync_parent_dir(ckpt);
    info.checkpoints_completed += 1;
    info.stale_rolls_removed += remove_rolls(rolls);
    ensure_live_wal(wal, live_gen, rolls, key)
}

// ---------- durable open / checkpoint on PinkDb ----------

impl PinkDb {
    /// Open `path` for durable commits with the default [`SyncPolicy::Sync`]:
    /// load the checkpoint `.pink` (if present), recover an interrupted
    /// checkpoint, replay `<path>.pink.wal` (replay + torn-tail truncation),
    /// then log every commit to that WAL.
    /// A missing checkpoint *and* WAL starts a fresh durable database.
    pub fn open_durable(path: &str) -> Result<PinkDb> {
        Ok(Self::open_durable_impl(path, None, &DurableOpenOptions::default())?.0)
    }

    /// [`PinkDb::open_durable`] with an explicit sync policy; also returns
    /// what recovery replayed (for callers like the REPL that report it).
    pub fn open_durable_with(path: &str, policy: SyncPolicy) -> Result<(PinkDb, RecoveryInfo)> {
        Self::open_durable_impl(
            path,
            None,
            &DurableOpenOptions {
                policy,
                ..DurableOpenOptions::default()
            },
        )
    }

    /// [`PinkDb::open_durable`] with at-rest encryption: the checkpoint and
    /// the sidecar WAL must be encrypted (or absent) and are decrypted with
    /// `key`; commits are written encrypted. A plaintext artifact is refused
    /// so the key can never be silently ignored, and a wrong key fails the
    /// key-check block with a `wrong key` error before anything is replayed.
    /// Without the `encryption` feature this always fails with a clear
    /// "not compiled in" error.
    pub fn open_durable_with_key(path: &str, key: &EncryptionKey) -> Result<PinkDb> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = (path, key);
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            Ok(Self::open_durable_impl(path, Some(key), &DurableOpenOptions::default())?.0)
        }
    }

    /// [`PinkDb::open_durable_with_key`] with an explicit sync policy and
    /// recovery info.
    pub fn open_durable_with_key_and_policy(
        path: &str,
        policy: SyncPolicy,
        key: &EncryptionKey,
    ) -> Result<(PinkDb, RecoveryInfo)> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = (path, policy, key);
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            Self::open_durable_impl(
                path,
                Some(key),
                &DurableOpenOptions {
                    policy,
                    ..DurableOpenOptions::default()
                },
            )
        }
    }

    /// [`PinkDb::open_durable_with`] with the full options set (sync policy,
    /// busy timeout, lock mode). The recovery + initial replay happen under
    /// the write lease, which is then released (the fd stays open for the
    /// epoch precheck).
    pub fn open_durable_with_options(
        path: &str,
        opts: DurableOpenOptions,
    ) -> Result<(PinkDb, RecoveryInfo)> {
        Self::open_durable_impl(path, None, &opts)
    }

    /// [`PinkDb::open_durable_with_options`] with at-rest encryption.
    pub fn open_durable_with_key_and_options(
        path: &str,
        key: &EncryptionKey,
        opts: DurableOpenOptions,
    ) -> Result<(PinkDb, RecoveryInfo)> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = (path, key, opts);
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            Self::open_durable_impl(path, Some(key), &opts)
        }
    }

    /// Shared durable-open path for plaintext (`key == None`) and encrypted
    /// (`key == Some`) databases. Everything on disk must match the key mode;
    /// every artifact the open creates (fresh WAL, recreated WAL, completed
    /// checkpoint) is written in the mode of the open. Recovery, the format
    /// upgrade and the initial replay run under the per-operation lease.
    fn open_durable_impl(
        path: &str,
        key: Option<&EncryptionKey>,
        opts: &DurableOpenOptions,
    ) -> Result<(PinkDb, RecoveryInfo)> {
        let ckpt = PathBuf::from(path);
        let wal = wal_path(&ckpt);
        let tmp = tmp_path(&ckpt);
        let mut info = RecoveryInfo::default();

        // 0. Take the lease: recovery and appends from two processes would
        //    corrupt the WAL. Unlike the old lifetime lock, it is released
        //    when this open completes.
        //
        //    M-5 migration: narrow artifacts a pre-fix build created with the
        //    process umask (0644). Best-effort: an artifact we do not own is
        //    left alone and the open proceeds.
        crate::fsutil::tighten(&ckpt);
        crate::fsutil::tighten(&wal);
        crate::fsutil::tighten(&lock_path(&ckpt));
        crate::fsutil::tighten(&tmp);
        let lease = Lease::open(&ckpt, opts.lock_mode, opts.busy_timeout)?;
        let guard = lease.acquire()?;
        let mut mutated = false;

        // 1. Finish an interrupted checkpoint before loading anything.
        let rolls = scan_rolls(&wal)?;
        let probe = probe_wal(&wal, key)?;
        recover_checkpoint(&ckpt, &wal, &tmp, &rolls, probe.as_ref(), &mut info, key)?;
        if info.checkpoints_completed > 0 || info.stale_rolls_removed > 0 {
            mutated = true;
        }

        // 2. Checkpoint, if one exists, then replay the live WAL on top.
        //    `open`/`open_with_key` enforce the key mode of the checkpoint.
        let mut db = if ckpt.exists() {
            match key {
                Some(k) => PinkDb::open_with_key(path, k)?,
                None => PinkDb::open(path)?,
            }
        } else {
            PinkDb::new()
        };
        db.encryption_key = key.cloned();
        // The extension policy of this open, applied *before* the replay:
        // a recorded `LOAD EXTENSION` is resolved against these settings
        // (see `PinkDb::resolve_replay_extension`), never auto-loaded.
        db.allow_extension_replay = opts.allow_extension_replay;
        db.extension_root = opts.extension_root.clone();
        // When the live WAL is missing or torn, continue the generation
        // sequence past every roll (recovery has already recreated the file
        // with this generation when that was needed).
        let recreate_gen = match probe.as_ref().and_then(|p| p.generation) {
            Some(g) => g,
            None => next_gen_after_rolls(&rolls)?,
        };
        let (mut handle, records, truncated) = Wal::open(
            &wal,
            ckpt.clone(),
            opts.policy,
            recreate_gen,
            key,
            lease.clone(),
        )?;
        for rec in &records {
            // The same replay mode `replay_frames` uses (wave 80): a replayed
            // statement must not trigger clock-dependent or dirty-driven
            // materialized-view refreshes — the logged `REFRESH` statements
            // reproduce those — and a replayed `LOAD EXTENSION` of an already
            // loaded path is the no-op the WAL intends.
            db.replaying = true;
            let result = replay_record(&mut db, rec);
            db.replaying = false;
            result?;
        }
        info.replayed_records += records.len() as u64;
        info.replayed_statements += records.iter().map(|r| r.stmts.len() as u64).sum::<u64>();
        info.truncated_bytes = truncated;
        if truncated > 0 || handle.upgraded_last() {
            mutated = true;
        }
        // Replay leaves the session wherever the last record did (a logged
        // `USE` or the start at `main`): later appends must know that.
        handle.set_session_db(db.current_db());
        // Every extension replay loaded is covered by the current WAL
        // generation (the live WAL is exactly what was just replayed): a
        // later record touching it must not emit a redundant prefix.
        handle.seed_tracked_extensions(db.loaded_extension_paths());
        // The applied position: this store's memory now matches the file.
        let (wal_id_now, ckpt_now) = (
            file_identity(&wal),
            ckpt_meta(&handle.checkpoint_path.clone()),
        );
        handle.mark_applied(Applied {
            wal_id: handle.wal_id,
            generation: handle.generation,
            wal_dev_ino: wal_id_now,
            records_start: handle.records_start,
            applied_end: std::fs::metadata(&wal)
                .map(|m| m.len())
                .unwrap_or(handle.records_start),
            applied_lsn: handle.last_lsn,
            applied_records: records.len() as u64,
            ckpt: ckpt_now,
        });
        if mutated {
            // Publish recovery mutations (a completed checkpoint, a removed
            // roll, a truncated tail, an upgrade) to already-open readers.
            guard.bump_epoch(handle.last_lsn);
        }
        guard.cache_epoch_now();
        drop(guard);
        db.lease = Some(lease);
        db.wal = Some(Arc::new(Mutex::new(handle)));
        Ok((db, info))
    }

    /// Set the WAL size that triggers an automatic checkpoint after a
    /// committed write (`None` disables it; default
    /// [`DEFAULT_AUTO_CHECKPOINT_BYTES`]). No-op for in-memory databases.
    pub fn set_auto_checkpoint_bytes(&self, bytes: Option<u64>) {
        if let Some(wal) = self.durable_handle() {
            wal.lock()
                .unwrap_or_else(|p| p.into_inner())
                .set_auto_checkpoint_bytes(bytes);
        }
    }

    /// Acquire the write lease (`None` in memory). Callers must acquire it
    /// *before* any store lock (invariant I6); the in-process gate makes
    /// nested acquisitions by the same thread safe.
    pub(crate) fn acquire_lease(&self) -> Result<Option<LeaseGuard>> {
        match &self.lease {
            Some(lease) => Ok(Some(lease.acquire()?)),
            None => Ok(None),
        }
    }

    /// Refresh the in-memory store from disk if anything was published (or
    /// replaced), taking a short lease. No-op when fresh; used by read paths.
    pub(crate) fn refresh_if_stale(&mut self) -> Result<()> {
        let Some(lease) = self.lease.clone() else {
            return Ok(());
        };
        #[cfg(test)]
        lease.prechecks.fetch_add(1, Ordering::Relaxed);
        if !lease.stale() {
            #[cfg(test)]
            lease.fastpath_skips.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        #[cfg(test)]
        lease.refresh_passes.fetch_add(1, Ordering::Relaxed);
        let guard = lease.acquire()?;
        self.refresh_under_lease(&guard).map(|_| ())
    }

    /// Refresh under an already-held lease. Idempotent: a second caller
    /// after the first refresh is a no-op. Returns whether a refresh pass
    /// actually ran (callers that bound a plan before taking the lease must
    /// rebind when it did). A failure poisons the handle (hazard H6/H9):
    /// committed data is never silently dropped and the partially advanced
    /// store is never observable.
    pub(crate) fn refresh_under_lease(&mut self, guard: &LeaseGuard) -> Result<bool> {
        // Recovery/upgrade/replay below mutate files; a lease whose lock
        // file was deleted or replaced must not run them (see [`Lease`]).
        // Returned before any work: the store is left exactly as it was.
        guard.verify_lock_identity()?;
        let Some(wal_arc) = self.wal.clone() else {
            return Ok(false);
        };
        let mut wal = wal_arc
            .lock()
            .map_err(|_| PinkDbError::Execution("internal lock error (poisoned)".to_string()))?;
        if wal.poisoned.is_some() {
            // Fail-stop is already in effect; the error surfaces through
            // `ensure_wal_healthy`.
            return Ok(false);
        }
        if !guard.epoch_stale() && !wal.needs_scan()? {
            return Ok(false);
        }
        match self.refresh_inner(&mut wal, guard) {
            Ok(()) => {
                guard.cache_epoch_now();
                Ok(true)
            }
            Err(e) => {
                // H6/H9: the handle fail-stops with the full context.
                let msg = format!("refresh failed: {e}");
                wal.poison(msg.clone());
                Err(PinkDbError::Execution(msg))
            }
        }
    }

    /// The heavy refresh: recovery, legacy upgrade, identity compare,
    /// full-or-incremental reload, replay (design 4.4/4.5).
    fn refresh_inner(&mut self, wal: &mut Wal, guard: &LeaseGuard) -> Result<()> {
        let key = self.encryption_key.clone();
        let wal_path = wal.path.clone();
        let ckpt = wal.checkpoint_path.clone();
        let tmp = tmp_path(&ckpt);
        let local_session = self.current_db.clone();
        let mut mutated = false;

        // 1. Recovery state machine (idempotent under the lease): finish an
        //    interrupted checkpoint, remove provably folded rolls, recreate a
        //    missing live WAL. Only run it when a crash artifact exists or
        //    the WAL header is missing/torn — the steady-state append path
        //    must not read the whole WAL (probe authenticates records).
        let rolls = scan_rolls(&wal_path)?;
        let mut info = RecoveryInfo::default();
        if tmp.exists() || !rolls.is_empty() || wal_header_invalid(&wal_path)? {
            let probe = probe_wal(&wal_path, key.as_ref())?;
            recover_checkpoint(
                &ckpt,
                &wal_path,
                &tmp,
                &rolls,
                probe.as_ref(),
                &mut info,
                key.as_ref(),
            )?;
            if info.checkpoints_completed > 0 || info.stale_rolls_removed > 0 {
                mutated = true;
            }
        }

        // 2. Legacy formats are upgraded under the lease (plaintext v2 → v3;
        //    container v1 → v2 + publish block). The upgrade replaces the
        //    file incarnation, so a full rebuild follows.
        if wal_legacy_format(&wal_path)? {
            let data = std::fs::read(&wal_path)?;
            let header = parse_wal_header(&data)
                .map_err(|e| PinkDbError::Execution(header_error(&wal_path, &e)))?;
            upgrade_legacy_wal(&wal_path, &data, &header, key.as_ref())?;
            mutated = true;
        }

        // 3. Identity compare: a replaced WAL file, a new WAL incarnation
        //    (`wal_id`), or a new checkpoint means no incremental
        //    continuity — reload the checkpoint and replay from scratch.
        let disk = read_wal_identity(&wal_path)?;
        let ckpt_now = ckpt_meta(&ckpt);
        let full = ckpt_now != wal.applied.ckpt
            || disk.dev_ino != wal.applied.wal_dev_ino
            || disk.generation != wal.applied.generation
            || disk.wal_id != wal.applied.wal_id;

        let (records, end) = if full {
            let other = self.load_checkpoint_for_refresh(&key, &ckpt)?;
            self.adopt(other);
            wal.reopen_under_lease(&key)?
        } else {
            match wal.scan_increment()? {
                Some(x) => x,
                None => {
                    // Defensive: bytes below `applied_end` were rewritten
                    // (only possible outside the protocol) — rebuild.
                    let other = self.load_checkpoint_for_refresh(&key, &ckpt)?;
                    self.adopt(other);
                    let x = wal.reopen_under_lease(&key)?;
                    mutated = true;
                    x
                }
            }
        };
        // A rebuild re-derives the whole journal from the replayed frames;
        // an incremental scan appends to it (frames applied earlier keep
        // their entries: an open optimistic base may still need them).
        if full {
            wal.reset_journal();
        }
        if wal.truncated_last() > 0 || wal.upgraded_last() {
            mutated = true;
        }

        // 4. Replay the new frames into the live store (hazards H1–H10).
        if !records.is_empty() {
            self.replay_frames(wal, &records, &local_session)?;
            for rec in &records {
                wal.journal_push(rec.lsn, rec.sets.clone());
            }
        } else if self.database_exists(&local_session) {
            // No new frames: the local session is still valid (a full
            // rebuild adopted the checkpoint's default session).
            self.set_current_db(&local_session);
        } else {
            let fallback = self.fallback_db();
            self.set_current_db(&fallback);
        }

        // 5. Bookkeeping: the applied position and the epoch cache.
        let (wal_dev_ino, ckpt_now) = (file_identity(&wal_path), ckpt_meta(&ckpt));
        let total = wal.applied.applied_records + records.len() as u64;
        let last = if records.is_empty() {
            wal.applied.applied_lsn
        } else {
            wal.last_lsn
        };
        wal.mark_applied(Applied {
            wal_id: wal.wal_id,
            generation: wal.generation,
            wal_dev_ino,
            records_start: wal.records_start,
            applied_end: end,
            applied_lsn: last,
            applied_records: total,
            ckpt: ckpt_now,
        });
        if let Some(c) = wal.cipher.as_mut() {
            // A reopened encrypted WAL must not reuse the nonces of its
            // records: position the counter after the on-disk record count.
            let next = u32::try_from(total)
                .map_err(|_| {
                    PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
                })?
                .checked_add(1)
                .ok_or_else(|| {
                    PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
                })?;
            c.set_next_counter(next);
        }
        if mutated {
            guard.bump_epoch(wal.last_lsn);
        }
        Ok(())
    }

    /// Discard every in-memory change not in the WAL and reload the committed
    /// state (the undo step of a conflicted optimistic autocommit statement:
    /// its effect never reached the WAL, so it must never be observable). The
    /// caller holds the lease; for a shared store it also holds the write
    /// guard, so no local reader can observe the intermediate state.
    pub(crate) fn rebuild_under_lease(&mut self, guard: &LeaseGuard) -> Result<()> {
        let Some(wal_arc) = self.wal.clone() else {
            return Ok(());
        };
        {
            let mut wal = wal_arc.lock().map_err(|_| {
                PinkDbError::Execution("internal lock error (poisoned)".to_string())
            })?;
            // Invalidate the applied position: the identity compare in the
            // refresh then forces a full rebuild (reload the checkpoint and
            // replay the whole live WAL).
            wal.mark_applied(Applied::default());
        }
        self.refresh_under_lease(guard)?;
        Ok(())
    }

    /// A failure *after* an optimistic execution but before its frame was
    /// appended: the statement's in-memory effect cannot be undone without
    /// the lease, so the handle fail-stops (same policy as an append
    /// failure) — the uncommitted state is never observable and a reopen
    /// recovers exactly the durable prefix. Returns the error to report.
    pub(crate) fn poison_uncommitted(&mut self, cause: &PinkDbError) -> PinkDbError {
        let msg = format!(
            "an optimistic statement's changes were not committed and cannot be undone in \
             memory ({cause})"
        );
        if let Some(wal) = self.durable_handle() {
            let mut w = wal.lock().unwrap_or_else(|p| p.into_inner());
            w.poison(msg.clone());
        }
        PinkDbError::Execution(format!(
            "the database stopped after a write failure ({msg}); reopen it to recover the \
             durable state"
        ))
    }

    /// Capture an optimistic base at the current applied position: the WAL
    /// incarnation plus the applied LSN, registered so the frame journal is
    /// not pruned below it while the unit is open. `None` for in-memory
    /// stores (nothing to validate against) or a poisoned WAL (every
    /// statement fail-stops anyway).
    pub(crate) fn capture_base(&self) -> Result<Option<BaseToken>> {
        let Some(wal_arc) = self.wal.clone() else {
            return Ok(None);
        };
        let mut wal = wal_arc
            .lock()
            .map_err(|_| PinkDbError::Execution("internal lock error (poisoned)".to_string()))?;
        Ok(wal.register_base().map(|(seq, position)| BaseToken {
            wal: wal_arc.clone(),
            seq,
            position,
        }))
    }

    /// Test-only evidence: count one clean optimistic autocommit commit.
    /// Always defined (call sites stay unconditional) but a no-op outside
    /// test builds.
    #[inline]
    pub(crate) fn note_optimistic_success_for_test(&mut self) {
        #[cfg(test)]
        if let Some(wal) = self.durable_handle() {
            let mut w = wal.lock().unwrap_or_else(|p| p.into_inner());
            w.note_optimistic_outcome(false, false);
        }
    }

    /// Test-only evidence: count one optimistic conflict + pessimistic
    /// fallback (no-op outside test builds).
    #[inline]
    pub(crate) fn note_optimistic_fallback_for_test(&mut self) {
        #[cfg(test)]
        if let Some(wal) = self.durable_handle() {
            let mut w = wal.lock().unwrap_or_else(|p| p.into_inner());
            w.note_optimistic_outcome(true, true);
        }
    }

    /// Test-only: `(optimistic commits, conflicts, fallbacks)` on this
    /// handle's WAL (`None` in memory).
    #[cfg(test)]
    pub(crate) fn optimistic_counts_for_test(&self) -> Option<(u64, u64, u64)> {
        let wal = self.durable_handle()?;
        let w = wal.lock().unwrap_or_else(|p| p.into_inner());
        Some(w.optimistic_counts())
    }

    /// The optimistic commit check, run under the lease (and, for a shared
    /// store, the write guard): validate `ours` against every frame committed
    /// after `base` — the applied journal first, then the unapplied tail,
    /// which is scanned (decoded) without executing. When clean, the tail is
    /// replayed into this store and journaled so the view is current.
    /// Returns `None` when the unit may commit; `Some(conflict)` when it must
    /// abort, in which case nothing was applied and the store is left exactly
    /// as it was.
    ///
    /// Frames are only ever executed after their touched set proved disjoint
    /// from `ours`, so replaying them can never observe — or collide with —
    /// the store's uncommitted local state (the H4 hazard the frame-carried
    /// sets exist to avoid).
    pub(crate) fn optimistic_commit_check(
        &mut self,
        _guard: &LeaseGuard,
        base: &BasePosition,
        ours: &FrameSets,
    ) -> Result<Option<CommitConflict>> {
        // The lock-identity re-check lives at the mutation boundary
        // (`append`/`flush`/`refresh`/`checkpoint`); this pass only reads
        // files and applies already-validated frames in memory.
        #[cfg(debug_assertions)]
        {
            let mut sorted = ours.clone();
            sorted.canonicalize();
            debug_assert!(sorted == *ours, "ours must be canonicalized by the caller");
        }
        let Some(wal_arc) = self.wal.clone() else {
            return Ok(None);
        };
        let mut wal = wal_arc
            .lock()
            .map_err(|_| PinkDbError::Execution("internal lock error (poisoned)".to_string()))?;
        if let Some(msg) = &wal.poisoned {
            return Err(PinkDbError::Execution(format!(
                "WAL is unusable after an earlier write failure ({msg})"
            )));
        }
        // 1. Incarnation. The cached handle is only refreshed at statement
        //    boundaries, so trust nothing about it: re-read the live file
        //    under the lease (one open + header read + fstat). A checkpoint,
        //    recovery, upgrade, replacement, or a roll-away since the base
        //    invalidates it — the base's snapshot is still internally
        //    consistent, but it cannot be validated against frames that no
        //    longer exist, so this is a retryable conflict (the next
        //    statement's refresh reloads).
        //    Fast path (one stat): the file is the one this handle last
        //    applied and its length is the applied position — nothing on
        //    disk changed at all, so the header cannot have. Anything else
        //    (a foreign append, a replacement, a truncation) pays the header
        //    read + the tail scan below.
        let meta_now = file_meta(&wal.path);
        let unchanged = meta_now.is_some_and(|m| {
            m.dev_ino == wal.applied.wal_dev_ino && m.len == wal.applied.applied_end
        });
        if !unchanged {
            let disk = match read_wal_identity(&wal.path) {
                Ok(d) => d,
                Err(_) => {
                    return Ok(Some(CommitConflict {
                        object: "the WAL generation".to_string(),
                        detail: "the live WAL is unreadable (recovered by another process?)"
                            .to_string(),
                        lsn: None,
                    }));
                }
            };
            if disk.generation != base.generation
                || disk.wal_id != base.wal_id
                || disk.dev_ino != wal.applied.wal_dev_ino
            {
                return Ok(Some(CommitConflict {
                    object: "the WAL generation".to_string(),
                    detail: "another process checkpointed, recovered, or upgraded the database"
                        .to_string(),
                    lsn: None,
                }));
            }
        }
        // 1b. The checkpoint file itself: a `.save` snapshot of the target
        //     replaces `.pink` without rolling the WAL, so the incarnation
        //     check above cannot see it — but a frame appended after it
        //     would replay on top of a different database.
        if ckpt_meta(&wal.checkpoint_path) != base.ckpt {
            return Ok(Some(CommitConflict {
                object: "the checkpoint file".to_string(),
                detail: "another process replaced the checkpoint while this unit was open"
                    .to_string(),
                lsn: None,
            }));
        }
        // 2. Frames already applied since the base (refreshes and local
        //    commits recorded their sets in the journal).
        if let Some((object, lsn)) = wal.check_journal(base, ours) {
            return Ok(Some(CommitConflict {
                object,
                detail: "changed concurrently".to_string(),
                lsn,
            }));
        }
        // 3. The unapplied tail (empty in the common single-writer case,
        //    where the stat above proved the length unchanged): scan (decode
        //    + framing only), check every frame's set, and only then apply.
        let tail_is_empty = meta_now.is_some_and(|m| m.len == wal.applied.applied_end);
        let (records, end) = if tail_is_empty {
            (Vec::new(), wal.applied.applied_end)
        } else {
            match wal.scan_increment()? {
                Some(x) => x,
                None => {
                    return Ok(Some(CommitConflict {
                        object: "the WAL tail".to_string(),
                        detail: "the WAL was rewritten under the transaction (retry)".to_string(),
                        lsn: None,
                    }));
                }
            }
        };
        for rec in &records {
            if let Some((object, lsn)) =
                Wal::frame_conflicts(rec.sets.as_ref(), rec.lsn, base, ours)
            {
                return Ok(Some(CommitConflict {
                    object,
                    detail: "changed concurrently".to_string(),
                    lsn,
                }));
            }
        }
        if !records.is_empty() {
            let local_session = self.current_db.clone();
            self.replay_frames(&mut wal, &records, &local_session)?;
            for rec in &records {
                wal.journal_push(rec.lsn, rec.sets.clone());
            }
            let total = wal.applied.applied_records + records.len() as u64;
            let (wal_dev_ino, ckpt_now) =
                (file_identity(&wal.path), ckpt_meta(&wal.checkpoint_path));
            let applied = Applied {
                wal_id: wal.wal_id,
                generation: wal.generation,
                wal_dev_ino,
                records_start: wal.records_start,
                applied_end: end,
                applied_lsn: wal.last_lsn,
                applied_records: total,
                ckpt: ckpt_now,
            };
            wal.mark_applied(applied);
            if let Some(c) = wal.cipher.as_mut() {
                // An encrypted refresh must not reuse nonces: position the
                // counter after the on-disk record count.
                let next = u32::try_from(total)
                    .map_err(|_| {
                        PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
                    })?
                    .checked_add(1)
                    .ok_or_else(|| {
                        PinkDbError::Execution("encrypted WAL: nonce counter overflow".to_string())
                    })?;
                c.set_next_counter(next);
            }
        }
        Ok(None)
    }

    /// Load the checkpoint `.pink` (plaintext or keyed) for a refresh's full
    /// rebuild; `PinkDb::new()` when there is no checkpoint yet.
    fn load_checkpoint_for_refresh(
        &self,
        key: &Option<EncryptionKey>,
        ckpt: &Path,
    ) -> Result<PinkDb> {
        if !ckpt.exists() {
            return Ok(PinkDb::new());
        }
        let path = ckpt.to_str().ok_or_else(|| {
            PinkDbError::Execution(format!(
                "cannot refresh {}: path is not valid UTF-8",
                ckpt.display()
            ))
        })?;
        match key {
            Some(k) => PinkDb::open_with_key(path, k),
            None => PinkDb::open(path),
        }
    }

    /// Replay recovered frames into the LIVE store: WAL detached so nothing
    /// re-enters logging/publishing/auto-checkpoint (H1), replay in the WAL
    /// lineage session with the local session restored afterwards (H2/H3),
    /// extension loads resolved against the store's extension policy (H5),
    /// and the deterministic hard-error policy (H6). `local_session` is the
    /// session the store had before the refresh (a full rebuild must
    /// preserve it).
    fn replay_frames(
        &mut self,
        wal: &mut Wal,
        records: &[Record],
        local_session: &str,
    ) -> Result<()> {
        // Two callers: the refresh (never inside an embedded transaction) and
        // the optimistic commit check, where an open transaction's changes
        // are already in the store — safe there because validation proved
        // every replayed frame touches none of the transaction's objects.
        // H1: detach the handle for the whole replay.
        let stashed_wal = self.wal.take();
        // H2/H3: bare records replay in the lineage session; a vanished
        // lineage database falls back (the record then cannot name it either
        // — it was appended bare precisely because the database vanished).
        let start = match wal.session_db.clone() {
            Some(s) if self.database_exists(&s) => s,
            _ => self.fallback_db(),
        };
        self.set_current_db(&start);
        // H5: a replayed `LOAD EXTENSION` is never auto-loaded. The record
        // may name an extension this process already has mapped (the
        // registry is process-local and never persisted, so a registry
        // outlives the records that loaded it): such a record is satisfied
        // by the loaded library, without a second `dlopen`. Anything else
        // needs the operator's explicit opt-in (`allow_extension_replay`)
        // *and* an extension root that contains the library — otherwise the
        // replay fails with an actionable error (see
        // `PinkDb::resolve_replay_extension`).
        let previously_loaded: HashSet<String> =
            self.loaded_extension_paths().into_iter().collect();
        self.replaying = true;
        let mut result = Ok(());
        for rec in records {
            if let Err(e) = replay_record(self, rec) {
                result = Err(e);
                break;
            }
        }
        self.replaying = false;
        if result.is_ok() {
            // Extensions these frames loaded are covered by the current WAL
            // generation: a later record touching one of them must not emit
            // a redundant `LOAD EXTENSION` prefix. Only the frames just
            // replayed count — an extension loaded by an earlier generation
            // (and kept mapped across a checkpoint) is deliberately not
            // seeded, because a fresh replay of the current generation would
            // not load it.
            let newly_loaded: Vec<String> = self
                .loaded_extension_paths()
                .into_iter()
                .filter(|p| !previously_loaded.contains(p))
                .collect();
            wal.seed_tracked_extensions(newly_loaded);
            // The replay session after the last record is the new lineage.
            wal.set_session_db(self.current_db());
        }
        // H2c: never let another process's `USE` jump this process's local
        // session; if it vanished under the refresh, fall back.
        if self.database_exists(local_session) {
            self.set_current_db(local_session);
        } else {
            let fallback = self.fallback_db();
            self.set_current_db(&fallback);
        }
        self.wal = stashed_wal;
        result
    }

    /// Checkpoint when the WAL has outgrown the auto-checkpoint threshold
    /// (outside transactions only) and this process contributed at least
    /// half of the live WAL (design 6.5). Called after a write is durable,
    /// so a checkpoint failure must not fail that statement: it is reported
    /// on stderr, and a failure that poisons the WAL stops the database
    /// (fail-stop) exactly like a failed append. The caller passes the lease
    /// it already holds (if any); acquiring it here would invert the lock
    /// order.
    pub(crate) fn maybe_auto_checkpoint(&mut self, guard: Option<&LeaseGuard>) {
        if self.in_transaction() {
            return;
        }
        let Some(wal) = self.durable_handle() else {
            return;
        };
        let due = wal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .wants_checkpoint();
        if due && let Err(e) = self.checkpoint_with_guard(guard) {
            eprintln!("pinkdb: automatic checkpoint failed: {e}");
        }
    }

    /// Checkpoint: atomically persist the current state to the bound `.pink`
    /// path and roll the WAL. Only durable databases have a bound path; a
    /// transaction must not be open (its changes are not committed). Takes
    /// the lease itself; callers that already hold it use
    /// [`PinkDb::checkpoint_with_guard`].
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.in_transaction() {
            return Err(PinkDbError::Execution(
                "cannot checkpoint inside a transaction".to_string(),
            ));
        }
        let guard = self.acquire_lease()?;
        self.checkpoint_with_guard(guard.as_ref())
    }

    /// Checkpoint under a lease the caller already holds (or, for the
    /// embedded path, one acquired by [`PinkDb::checkpoint`]). Refreshes
    /// first (design 6.1): without it a checkpoint would publish a state
    /// lacking another process's committed frames and then roll those frames
    /// into a roll that recovery deletes — destroying commits.
    pub(crate) fn checkpoint_with_guard(&mut self, held: Option<&LeaseGuard>) -> Result<()> {
        if self.in_transaction() {
            return Err(PinkDbError::Execution(
                "cannot checkpoint inside a transaction".to_string(),
            ));
        }
        let wal = match self.durable_handle() {
            Some(wal) => wal,
            None => {
                return Err(PinkDbError::Execution(
                    "database is not durable (no WAL); use save() for a plain snapshot".to_string(),
                ));
            }
        };
        let own_guard;
        let guard = match held {
            Some(g) => g,
            None => {
                own_guard = self.acquire_lease()?;
                match &own_guard {
                    Some(g) => g,
                    None => {
                        return Err(PinkDbError::Execution(
                            "database is not durable".to_string(),
                        ));
                    }
                }
            }
        };
        // Refresh-first: the persisted state must include every committed
        // frame of every process.
        self.refresh_under_lease(guard)?;
        let mut guard_wal = wal
            .lock()
            .map_err(|_| PinkDbError::Execution("internal lock error (poisoned)".to_string()))?;
        // The live WAL's at-rest mode (and key) is fixed at open. A
        // checkpoint must never publish a `.pink` in a different mode or
        // under a different key than the WAL generation it rolls into — the
        // pair would be unopenable. Refuse instead of writing a mixed pair.
        // Without the `encryption` feature a keyed store refuses instead of
        // silently writing a plaintext checkpoint.
        #[cfg(feature = "encryption")]
        {
            let key = self.encryption_key.clone();
            if key.is_some() != guard_wal.is_encrypted() {
                return Err(PinkDbError::Execution(
                    "cannot change encryption mode on a live store; reopen it instead".to_string(),
                ));
            }
            if let Some(k) = &key
                && !guard_wal.key_matches(k)
            {
                return Err(PinkDbError::Execution(
                    "cannot change the encryption key on a live store; reopen it instead"
                        .to_string(),
                ));
            }
            match key {
                Some(k) => guard_wal.checkpoint(guard, |tmp| self.save_sync_with_key(tmp, &k))?,
                None => guard_wal.checkpoint(guard, |tmp| self.save_sync(tmp))?,
            }
        }
        #[cfg(not(feature = "encryption"))]
        {
            if self.encryption_key.is_some() {
                return Err(crate::crypto::unavailable());
            }
            guard_wal.checkpoint(guard, |tmp| self.save_sync(tmp))?;
        }
        // A fresh WAL generation replays from the checkpoint's session
        // default (the checkpoint does not store a session): reset the
        // tracked replay session so later appends `USE`-prefix correctly.
        let fallback = self.fallback_db();
        guard_wal.set_session_db(&fallback);
        // Index definitions and their maps ride in the published `.pink`
        // itself (flags bit 3, wave 79), atomically with the data, so the
        // fresh WAL generation starts empty like every other catalog object.
        Ok(())
    }

    /// The `.pink` path this database is durable to, if any.
    pub fn durable_path(&self) -> Option<PathBuf> {
        let wal = self.durable_handle()?;
        let guard = wal.lock().ok()?;
        Some(guard.checkpoint_path().to_path_buf())
    }

    /// fsync the WAL (relevant with [`SyncPolicy::Batch`]); no-op in memory.
    /// Takes the lease: with `Batch` a flush is the durability point other
    /// processes wait for.
    pub fn flush_wal(&mut self) -> Result<()> {
        let guard = self.acquire_lease()?;
        self.flush_wal_holding_lease()?;
        drop(guard);
        Ok(())
    }

    /// [`PinkDb::flush_wal`] with the lease already held by the caller.
    pub(crate) fn flush_wal_holding_lease(&mut self) -> Result<()> {
        if let Some(wal) = self.durable_handle() {
            let mut guard_wal = wal.lock().map_err(|_| {
                PinkDbError::Execution("internal lock error (poisoned)".to_string())
            })?;
            guard_wal.flush()?;
            guard_wal.publish();
        }
        Ok(())
    }

    /// Change the busy timeout of the durable lease (no-op in memory). The
    /// timeout bounds how long any operation waits for another process's
    /// lease before reporting the database busy.
    pub fn set_busy_timeout(&self, timeout: Duration) {
        if let Some(lease) = &self.lease {
            lease.set_busy_timeout(timeout);
        }
    }

    /// True when `path` refers to the same `.pink` file this database is
    /// bound to, compared by filesystem identity rather than by string:
    /// `./x.pink` vs `x.pink`, relative vs absolute, symlinks, and case-only
    /// differences on case-insensitive filesystems all compare equal. False
    /// for in-memory databases.
    pub fn is_bound_to(&self, path: &str) -> bool {
        match self.durable_path() {
            Some(bound) => same_file(&bound, Path::new(path)),
            None => false,
        }
    }

    /// Guards every plain snapshot (`save`/`save_with_key`): never serialize an
    /// open transaction (a mid-`BEGIN` snapshot resurrects rolled-back rows),
    /// and never overwrite a target whose sidecar WAL still holds committed
    /// records (the target's next durable open would replay them on top of the
    /// snapshot). Writing over the *bound* path is a checkpoint, not a
    /// snapshot; see [`PinkDb::checkpoint`].
    pub(crate) fn guard_snapshot_target(&self, path: &str) -> Result<()> {
        if self.in_transaction() {
            return Err(PinkDbError::Execution(
                "cannot save inside a transaction; COMMIT or ROLLBACK first".to_string(),
            ));
        }
        let target_wal = wal_path(Path::new(path));
        if target_wal.exists()
            && probe_wal(&target_wal, self.encryption_key.as_ref())?
                .map(|p| p.has_records)
                .unwrap_or(false)
        {
            return Err(PinkDbError::Execution(format!(
                "refusing to save over {path}: {} holds committed WAL records; \
                 open that database durably and checkpoint it instead",
                target_wal.display()
            )));
        }
        Ok(())
    }

    /// A plain snapshot with a durability guard: refuse to write over a
    /// target whose sidecar WAL still holds committed records. The target's
    /// next durable open would replay those records on top of this snapshot —
    /// duplicating or bricking that database. Saving to the *bound* path is a
    /// checkpoint, not a snapshot; see [`PinkDb::checkpoint`].
    ///
    /// Both the target `.pink` and its sidecar WAL must be in this store's
    /// at-rest mode: overwriting an encrypted target with plaintext (or the
    /// reverse) would silently flip the mode of the database that pays for
    /// it. A keyed store writes an encrypted snapshot.
    pub fn save_snapshot(&self, path: &str) -> Result<()> {
        let target = Path::new(path);
        check_artifact_mode(target, self.encryption_key.as_ref())?;
        self.guard_snapshot_target(path)?;
        // A live lease on the target means a process is writing or
        // checkpointing it: refuse instead of racing. The lock is held for
        // the snapshot itself, so nothing can slip in between the check and
        // the publish (dropping the fd releases the flock).
        let lock_path = PathBuf::from(format!("{}.lock", target.display()));
        let target_lease = match File::open(&lock_path) {
            Ok(f) => match f.try_lock() {
                Ok(()) => {
                    // The lock only guards the *current* file at this name:
                    // a deletion/replacement between open and lock would
                    // guard a stray inode while another process holds the
                    // replacement (wave 45n lock-identity check).
                    let same = f
                        .metadata()
                        .ok()
                        .map(|m| Some(identity_of(&m)) == file_identity(&lock_path))
                        .unwrap_or(false);
                    if !same {
                        return Err(PinkDbError::Execution(format!(
                            "refusing to save over {path}: its lock file {} was deleted or \
                             replaced while it was being checked; retry after the current \
                             database closes",
                            lock_path.display()
                        )));
                    }
                    Some(f)
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(PinkDbError::Execution(format!(
                        "refusing to save over {path}: the database is currently in use by \
                         another handle or process; retry after it finishes"
                    )));
                }
                // Locking unsupported: nothing to coordinate with.
                Err(std::fs::TryLockError::Error(_)) => None,
            },
            // No lock file: the target was never opened durably.
            Err(_) => None,
        };
        let result = match &self.encryption_key {
            Some(key) => self.save_with_key(path, key),
            None => self.save(path),
        };
        // Publish the replacement: a checkpoint written in place (same
        // inode) is invisible to other processes' identity checks, so bump
        // the target's lock-file epoch to make them re-check (their refresh
        // then compares the checkpoint meta and rebuilds). Best-effort — the
        // epoch is a hint; a failure only delays other processes' refresh.
        if result.is_ok()
            && let Some(f) = &target_lease
        {
            let seq = read_slot_max(f, LOCK_SLOT_A, LOCK_SLOT_DOMAIN)
                .unwrap_or(0)
                .saturating_add(1);
            if write_slot(f, LOCK_SLOT_A, seq, 0, LOCK_SLOT_DOMAIN).is_err() {
                warn_slot_once("lock-file epoch slot");
            }
        }
        drop(target_lease);
        result
    }
}

/// Filesystem identity of two `.pink` paths. `canonicalize` resolves
/// symlinks and, on case-insensitive filesystems, the on-disk spelling. The
/// sidecar WALs are the fallback when a `.pink` does not exist yet (a durable
/// database always has a WAL), and canonical parent + file name is the last
/// resort for paths that exist nowhere.
fn same_file(a: &Path, b: &Path) -> bool {
    if let (Ok(ca), Ok(cb)) = (std::fs::canonicalize(a), std::fs::canonicalize(b))
        && ca == cb
    {
        return true;
    }
    let (wa, wb) = (wal_path(a), wal_path(b));
    if let (Ok(ca), Ok(cb)) = (std::fs::canonicalize(&wa), std::fs::canonicalize(&wb))
        && ca == cb
    {
        return true;
    }
    match (normalized_path(a), normalized_path(b)) {
        (Some(na), Some(nb)) => na == nb,
        _ => false,
    }
}

fn normalized_path(p: &Path) -> Option<PathBuf> {
    let dir = std::fs::canonicalize(parent_dir(p)).ok()?;
    Some(dir.join(p.file_name()?))
}

/// Apply one recovered record: every statement must replay cleanly. A
/// CRC-valid record that fails to parse/bind/execute is a hard error.
fn replay_record(db: &mut PinkDb, rec: &Record) -> Result<()> {
    for sql in &rec.stmts {
        db.execute(sql).map_err(|e| {
            PinkDbError::Execution(format!("WAL replay failed at lsn {}: {e}", rec.lsn))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryResult;
    use crate::types::Value;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Per-test scratch directory: every file the test creates under it is
    /// removed when the guard drops — including on a panic — so WAL tests
    /// never leak `pinkdb_wal_*` files into the system temp dir. Derefs to
    /// the checkpoint path, so existing call sites read unchanged.
    struct TempCkpt {
        dir: PathBuf,
        ckpt: PathBuf,
    }

    impl TempCkpt {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "pinkdb_wal_{tag}_{}_{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("create test scratch dir");
            let ckpt = dir.join("db.pink");
            Self { dir, ckpt }
        }
    }

    impl std::ops::Deref for TempCkpt {
        type Target = Path;
        fn deref(&self) -> &Path {
            &self.ckpt
        }
    }

    impl AsRef<Path> for TempCkpt {
        fn as_ref(&self) -> &Path {
            &self.ckpt
        }
    }

    impl Drop for TempCkpt {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Unique per-test checkpoint path inside its own scratch directory.
    fn tmp_ckpt(tag: &str) -> TempCkpt {
        TempCkpt::new(tag)
    }

    fn cleanup(ckpt: &Path) {
        let wal = wal_path(ckpt);
        let _ = std::fs::remove_file(ckpt);
        let _ = std::fs::remove_file(&wal);
        let _ = std::fs::remove_file(tmp_path(ckpt));
        for r in scan_rolls(&wal).unwrap_or_default() {
            let _ = std::fs::remove_file(&r.path);
        }
        let _ = std::fs::remove_file(PathBuf::from(format!("{}.roll", wal.display())));
    }

    /// The legacy (v2/v3) payload layout, for hand-built fixtures.
    fn encode_payload_legacy_for_test(stmts: &[String]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(stmts.len() as u32).to_le_bytes());
        for s in stmts {
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        out
    }

    fn records_of(path: &Path) -> Vec<Record> {
        let data = std::fs::read(path).unwrap();
        let (recs, _) = parse_records(&data, RECORDS_START, None, 0, 0, true).unwrap();
        recs
    }

    /// M3: two independent opens (two leases, two WAL handles) in one
    /// process see each other's commits through the epoch-gated refresh.
    #[test]
    fn two_opens_refresh_each_other() {
        let p = tmp_ckpt("twoopens");
        let mut a = durable(&p);
        let mut b = durable(&p); // before the table exists
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        assert!(ints(&mut b, "SELECT 1;").len() == 1); // refresh past the create
        for n in 1..=5 {
            a.execute(&format!("INSERT INTO t VALUES ({n});")).unwrap();
            assert_eq!(
                ints(&mut b, &format!("SELECT a FROM t WHERE a = {n};")),
                vec![n],
                "b must see row {n}"
            );
        }
        for n in 6..=10 {
            b.execute(&format!("INSERT INTO t VALUES ({n});")).unwrap();
            assert_eq!(
                ints(&mut a, &format!("SELECT a FROM t WHERE a = {n};")),
                vec![n],
                "a must see row {n}"
            );
        }
        drop(a);
        drop(b);
        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            (1..=10).collect::<Vec<_>>()
        );
        cleanup(&p);
    }

    #[test]
    fn checkpoint_statement_and_auto_checkpoint_bound_the_wal() {
        let ckpt = tmp_ckpt("ckpt_stmt");
        let db = crate::Database::open(ckpt.to_str().unwrap()).unwrap();
        let mut c = db.connect();
        // `checkpoint` stays an ordinary identifier.
        c.execute("CREATE TABLE checkpoint (checkpoint INTEGER);")
            .unwrap();
        c.execute("INSERT INTO checkpoint VALUES (1), (2);")
            .unwrap();
        assert!(!records_of(&wal_path(&ckpt)).is_empty());
        let out = c.execute("checkpoint;").unwrap();
        assert_eq!(out, vec![QueryResult::Checkpointed]);
        assert!(
            records_of(&wal_path(&ckpt)).is_empty(),
            "CHECKPOINT rolls the WAL"
        );
        // Refused inside a transaction (uncommitted state must not persist).
        c.execute("BEGIN;").unwrap();
        let err = c.execute("CHECKPOINT;").unwrap_err().to_string();
        assert!(err.contains("not allowed inside a transaction"), "{err}");
        c.execute("ROLLBACK;").unwrap();
        // Auto-checkpoint: with a tiny threshold every commit checkpoints.
        db.flush_wal().unwrap();
        crate::Database::set_auto_checkpoint_bytes(&db, Some(1));
        c.execute("INSERT INTO checkpoint VALUES (3);").unwrap();
        assert!(
            records_of(&wal_path(&ckpt)).is_empty(),
            "auto checkpoint rolled the WAL"
        );
        c.execute("BEGIN; INSERT INTO checkpoint VALUES (4); COMMIT;")
            .unwrap();
        assert!(records_of(&wal_path(&ckpt)).is_empty());
        drop(c);
        drop(db);
        let mut back = durable(&ckpt);
        assert_eq!(
            ints(&mut back, "SELECT SUM(checkpoint) FROM checkpoint;"),
            vec![10]
        );
        // In-memory: CHECKPOINT is a harmless no-op.
        let mut mem = PinkDb::new();
        assert_eq!(
            mem.execute("CHECKPOINT").unwrap(),
            vec![QueryResult::Checkpointed]
        );
    }

    /// T6: a checkpoint publishes the current format (v5) and a durable
    /// reopen replays later WAL records on top of it.
    #[test]
    fn checkpoint_publishes_v5_and_replays_later_records() {
        let ckpt = tmp_ckpt("v4_ckpt");
        {
            let db = crate::Database::open(ckpt.to_str().unwrap()).unwrap();
            let mut c = db.connect();
            c.execute("CREATE TABLE t (a INTEGER, v VARCHAR);").unwrap();
            c.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two');")
                .unwrap();
            c.execute("checkpoint;").unwrap();
            let bytes = std::fs::read(&ckpt).unwrap();
            assert!(bytes.starts_with(b"PINKDB05"), "checkpoint publishes v5");
            // A commit after the checkpoint: the WAL record must survive.
            c.execute("INSERT INTO t VALUES (3, 'three');").unwrap();
        }
        let mut back = durable(&ckpt);
        assert_eq!(ints(&mut back, "SELECT SUM(a) FROM t;"), vec![6]);
        assert_eq!(ints(&mut back, "SELECT COUNT(*) FROM t;"), vec![3]);
    }

    /// Wave 45m: a durable database is no longer a single-handle resource.
    /// A second open succeeds, both handles see each other's commits on
    /// their next statement (refresh under the lease), clones share the
    /// lease, and the last clone releases the flock.
    #[test]
    fn concurrent_opens_share_the_database() {
        let ckpt = tmp_ckpt("lock");
        let mut first = durable(&ckpt);
        first
            .execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        // The second open recovers/replays under the per-operation lease and
        // releases it when open completes.
        let mut second = durable(&ckpt);
        assert_eq!(ints(&mut second, "SELECT a FROM t ORDER BY a;"), vec![1]);
        // Commits by one handle become visible to the other on its next
        // statement.
        second.execute("INSERT INTO t VALUES (2);").unwrap();
        assert_eq!(ints(&mut first, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        first.execute("INSERT INTO t VALUES (3);").unwrap();
        assert_eq!(
            ints(&mut second, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3]
        );
        // Clones (transaction snapshots, Database handles) share the lease.
        let shared = crate::Database::from_store(first.clone());
        drop(first);
        drop(second);
        // A third open may write while the clone still lives.
        let mut third = durable(&ckpt);
        third.execute("INSERT INTO t VALUES (4);").unwrap();
        drop(third);
        let mut c = shared.connect();
        assert_eq!(
            ints_conn(&mut c, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3, 4]
        );
        // The last clone releases the lease.
        drop(c);
        drop(shared);
        let mut again = durable(&ckpt);
        assert_eq!(
            ints(&mut again, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn a_failed_wal_write_stops_reads_writes_and_saves() {
        let ckpt = tmp_ckpt("failstop");
        let mut db = durable(&ckpt);
        db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        let shared = crate::Database::from_store(db.clone());
        db.durable_handle()
            .unwrap()
            .lock()
            .unwrap()
            .poison_for_test("disk full");
        // Embedded and multi-writer reads alike are refused: the failed
        // statement's in-memory effect must never be observable.
        let err = db.execute("SELECT * FROM t;").unwrap_err().to_string();
        assert!(err.contains("stopped after a WAL write failure"), "{err}");
        let err = shared
            .connect()
            .execute("SELECT 1;")
            .unwrap_err()
            .to_string();
        assert!(err.contains("stopped after a WAL write failure"), "{err}");
        let snap = ckpt.with_extension("snap.pink");
        assert!(db.save(snap.to_str().unwrap()).is_err());
        assert!(!snap.exists());
        drop(db);
        drop(shared);
        // Reopening recovers the durable prefix.
        let mut db = durable(&ckpt);
        assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
    }

    #[test]
    fn save_refuses_inside_a_transaction() {
        let p = tmp_ckpt("savetxn");
        let out = p.with_extension("snap.pink");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("BEGIN; INSERT INTO t VALUES (1);").unwrap();
        // A mid-transaction snapshot would resurrect the rolled-back row.
        let err = db.save(out.to_str().unwrap()).unwrap_err().to_string();
        assert!(err.contains("inside a transaction"), "{err}");
        assert!(!out.exists(), "a refused save writes nothing");
        db.execute("ROLLBACK;").unwrap();
        cleanup(&p);
    }

    #[test]
    fn save_refuses_over_a_target_whose_wal_has_records() {
        let p = tmp_ckpt("savewal");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        // The bound WAL holds the committed row: saving the bound path would
        // make the next durable open replay it on top of the snapshot.
        let err = db.save(p.to_str().unwrap()).unwrap_err().to_string();
        assert!(err.contains("committed WAL records"), "{err}");
        // The target was not touched, so the durable row still replays.
        drop(db);
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        drop(back);
        cleanup(&p);
    }

    #[test]
    fn save_is_atomic_and_leaves_no_temp_file() {
        let p = tmp_ckpt("saveatomic");
        let out = p.with_extension("out.pink");
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.save(out.to_str().unwrap()).unwrap();
        assert!(out.exists());
        let leftovers: Vec<String> = std::fs::read_dir(out.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "temp files left: {leftovers:?}");
        // Overwriting replaces the bytes wholesale.
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        db.save(out.to_str().unwrap()).unwrap();
        let mut back = PinkDb::open(out.to_str().unwrap()).unwrap();
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }

    fn durable(p: &Path) -> PinkDb {
        PinkDb::open_durable(p.to_str().unwrap()).unwrap()
    }

    fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i as i64,
                    Value::Bigint(i) => *i,
                    other => panic!("expected an integer, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn ints_conn(c: &mut crate::Connection, sql: &str) -> Vec<i64> {
        match c.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i as i64,
                    Value::Bigint(i) => *i,
                    other => panic!("expected an integer, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn wal_len(p: &Path) -> u64 {
        std::fs::metadata(wal_path(p)).unwrap().len()
    }

    /// Write a `.pink` checkpoint whose table `t` holds `rows`.
    fn write_pink(path: &Path, rows: &[i64]) {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        for r in rows {
            db.execute(&format!("INSERT INTO t VALUES ({r});")).unwrap();
        }
        db.save(path.to_str().unwrap()).unwrap();
    }

    /// M-5: the checkpoint's `.tmp` staging file (exactly what
    /// `Wal::checkpoint` writes before the roll/rename) is created 0600, not
    /// umask-derived.
    #[cfg(unix)]
    #[test]
    fn checkpoint_staging_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_ckpt_tmp_mode_{}_{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let ckpt = dir.join("db.pink");
        let db = PinkDb::new();
        let tmp = tmp_path(&ckpt);
        db.save_sync(&tmp).unwrap();
        let mode = std::fs::metadata(&tmp).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o600,
            "checkpoint staging {} is {mode:o}",
            tmp.display()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write a WAL file with the given generation and (lsn, statements)
    /// records, CRCs computed for real.
    fn write_wal_lsns(path: &Path, generation: u64, records: &[(u64, Vec<String>)]) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&Wal::header_fresh(generation)).unwrap();
        for (lsn, stmts) in records {
            let payload = encode_payload(stmts, None).unwrap();
            f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&lsn.to_le_bytes()).unwrap();
            f.write_all(&crc32(&payload).to_le_bytes()).unwrap();
            f.write_all(&payload).unwrap();
        }
        f.sync_all().unwrap();
    }

    fn raw(records: &[Vec<String>]) -> Vec<(u64, Vec<String>)> {
        records
            .iter()
            .enumerate()
            .map(|(i, s)| ((i as u64) + 1, s.clone()))
            .collect()
    }

    fn write_raw_wal(path: &Path, records: &[Vec<String>]) {
        write_wal_lsns(path, 1, &raw(records));
    }

    fn write_raw_wal_payload(path: &Path, payload: &[u8]) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&Wal::header_fresh(1)).unwrap();
        f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&1u64.to_le_bytes()).unwrap();
        f.write_all(&crc32(payload).to_le_bytes()).unwrap();
        f.write_all(payload).unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn crc32_matches_the_ieee_check_value() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
    }

    #[test]
    fn autocommit_records_roundtrip_with_monotonic_lsn() {
        let p = tmp_ckpt("autocommit");
        let (mut db, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info, RecoveryInfo::default());
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'y');").unwrap();
        // Read-only statements are never logged.
        db.execute("SELECT * FROM t;").unwrap();

        let data = std::fs::read(wal_path(&p)).unwrap();
        // The v3 header: magic, version 3, generation 1, a fresh wal_id, a
        // valid header CRC, and a published commit slot carrying lsn 3.
        assert_eq!(&data[..8], WAL_MAGIC);
        assert_eq!(
            u32::from_le_bytes(data[8..12].try_into().unwrap()),
            WAL_VERSION
        );
        assert_eq!(
            u64::from_le_bytes(data[12..20].try_into().unwrap()),
            1,
            "generation"
        );
        assert_ne!(&data[20..36], [0u8; 16], "wal_id is fresh, never zero");
        assert_eq!(
            u32::from_le_bytes(data[36..40].try_into().unwrap()),
            crc32(&data[..36]),
            "header CRC covers magic..wal_id"
        );
        let published = slot_max_in(&data, WAL_SLOT_A as usize, WAL_SLOT_DOMAIN);
        assert_eq!(published, 3, "one commit slot per appended record");
        let (recs, good_end) = parse_records(&data, RECORDS_START, None, 0, 0, true).unwrap();
        assert_eq!(good_end, data.len());
        assert_eq!(recs.len(), 3);
        assert_eq!(
            recs.iter().map(|r| r.lsn).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            recs[1].stmts,
            vec!["INSERT INTO t VALUES (1, 'x')".to_string()]
        );
        // CRC field verifies against the payload of every record.
        let mut pos = RECORDS_START;
        for rec in &recs {
            let payload_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(data[pos + 12..pos + 16].try_into().unwrap());
            let payload = &data[pos + 16..pos + 16 + payload_len];
            assert_eq!(crc32(payload), crc, "record {rec:?}");
            pos += 16 + payload_len;
        }
        drop(db);

        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 3);
        assert_eq!(info.replayed_statements, 3);
        assert_eq!(info.truncated_bytes, 0);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        // The recovered WAL continues the LSN sequence.
        back.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(records_of(&wal_path(&p)).last().unwrap().lsn, 4);
        cleanup(&p);
    }

    #[test]
    fn transaction_logs_one_record_at_commit_and_nothing_on_rollback() {
        let p = tmp_ckpt("txn");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        assert_eq!(records_of(&wal_path(&p)).len(), 1);

        // ROLLBACK logs nothing.
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (10);").unwrap();
        db.execute("INSERT INTO t VALUES (11);").unwrap();
        assert_eq!(records_of(&wal_path(&p)).len(), 1, "logged before COMMIT");
        db.execute("ROLLBACK;").unwrap();
        assert_eq!(records_of(&wal_path(&p)).len(), 1);

        // COMMIT logs every mutating statement as one record.
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("SELECT * FROM t;").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        db.execute("COMMIT;").unwrap();
        let recs = records_of(&wal_path(&p));
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1].stmts.len(), 2);
        assert_eq!(recs[1].lsn, 2);
        drop(db);

        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }

    #[test]
    fn multi_statement_string_logs_one_record_per_commit_unit() {
        let p = tmp_ckpt("multistmt");
        let mut db = durable(&p);
        // Autocommit statements inside one `execute` string: one record each.
        db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        assert_eq!(records_of(&wal_path(&p)).len(), 2);
        // BEGIN..COMMIT inside one string: the whole transaction is one record.
        db.execute("BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); COMMIT;")
            .unwrap();
        let recs = records_of(&wal_path(&p));
        assert_eq!(recs.len(), 3, "the two transaction inserts are one record");
        assert_eq!(
            recs[2].stmts,
            vec![
                "INSERT INTO t VALUES (2)".to_string(),
                "INSERT INTO t VALUES (3)".to_string(),
            ]
        );
        drop(db);

        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3]
        );
        cleanup(&p);
    }

    #[test]
    fn wave58_ctas_and_insert_select_replay_and_reads_skip_the_wal() {
        let p = tmp_ckpt("wave58dml");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE src (a INTEGER, b VARCHAR);")
                .unwrap();
            db.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y');")
                .unwrap();
            db.execute("CREATE TABLE copy AS SELECT a, b FROM src;")
                .unwrap();
            db.execute("CREATE TABLE ins (a INTEGER, b VARCHAR);")
                .unwrap();
            db.execute("INSERT INTO ins (a, b) SELECT a, b FROM src WHERE a = 2;")
                .unwrap();
            // Read-only completeness statements are never WAL-logged.
            db.execute("EXPLAIN SELECT * FROM src;").unwrap();
            db.execute("SHOW TABLES;").unwrap();
            db.execute("DESCRIBE src;").unwrap();
            db.execute("SELECT a FROM src INTERSECT SELECT a FROM copy;")
                .unwrap();
            // An IF NOT EXISTS no-op still logs its statement (it replays as
            // a no-op) and must not corrupt anything.
            db.execute("CREATE TABLE IF NOT EXISTS copy (a INTEGER);")
                .unwrap();
            db.execute("DROP TABLE IF EXISTS never_existed;").unwrap();
        }
        // No checkpoint: the whole state comes from replaying the WAL.
        let (mut back, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(
            ints(&mut back, "SELECT a FROM copy ORDER BY a;"),
            vec![1, 2]
        );
        assert_eq!(ints(&mut back, "SELECT a FROM ins ORDER BY a;"), vec![2]);
        // A transaction containing CTAS + INSERT SELECT replays as one record.
        {
            let mut db = durable(&p);
            db.execute("BEGIN;").unwrap();
            db.execute("CREATE TABLE tx AS SELECT a FROM src;").unwrap();
            db.execute("INSERT INTO ins (a) SELECT a FROM src;")
                .unwrap();
            db.execute("COMMIT;").unwrap();
        }
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert!(info.replayed_records >= 1);
        assert_eq!(ints(&mut back, "SELECT a FROM tx ORDER BY a;"), vec![1, 2]);
        assert_eq!(
            ints(&mut back, "SELECT a FROM ins ORDER BY a;"),
            vec![1, 2, 2]
        );
        cleanup(&p);
    }

    #[test]
    fn replay_reproduces_update_delete_and_drop_state() {
        let p = tmp_ckpt("dmlreplay");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z');")
                .unwrap();
            db.execute("BEGIN;").unwrap();
            db.execute("UPDATE t SET a = a * 10;").unwrap();
            db.execute("DELETE FROM t WHERE a = 20;").unwrap();
            db.execute("COMMIT;").unwrap();
            // Catalog churn replays too: create + drop cancel out.
            db.execute("CREATE TABLE u (a INTEGER); DROP TABLE u;")
                .unwrap();
        }
        // No checkpoint: the entire state comes from replaying the WAL.
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 5);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![10, 30]);
        assert!(back.execute("SELECT * FROM u;").is_err(), "u was dropped");
        // Checkpointing the replayed state round-trips exactly, no replay.
        back.checkpoint().unwrap();
        drop(back);
        let (mut again, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 0);
        assert_eq!(
            ints(&mut again, "SELECT a FROM t ORDER BY a;"),
            vec![10, 30]
        );
        cleanup(&p);
    }

    #[test]
    fn partial_transaction_never_applies() {
        let p = tmp_ckpt("partial");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("BEGIN;").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            db.execute("INSERT INTO t VALUES (2);").unwrap();
            // No COMMIT: simulated crash by dropping the handle.
        }
        let mut back = durable(&p);
        assert!(ints(&mut back, "SELECT a FROM t;").is_empty());
        assert_eq!(records_of(&wal_path(&p)).len(), 1, "only CREATE TABLE");
        cleanup(&p);
    }

    #[test]
    fn corrupt_tail_stops_replay_and_truncates() {
        // Bit flip in the last record.
        let p = tmp_ckpt("bitflip");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        let after_two = wal_len(&p);
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        drop(db);

        let mut data = std::fs::read(wal_path(&p)).unwrap();
        assert!(data.len() as u64 > after_two + 20);
        let len5 = data.len();
        data[len5 - 1] ^= 0xFF; // inside the last statement text
        std::fs::write(wal_path(&p), &data).unwrap();

        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 2, "stops at the last good record");
        assert_eq!(info.truncated_bytes, (len5 as u64) - after_two);
        assert_eq!(wal_len(&p), after_two, "file truncated to last good offset");
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
        // Appends after recovery land right after the last good record.
        back.execute("INSERT INTO t VALUES (3);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![1, 3]);
        cleanup(&p);

        // Truncated (short) record: same recovery.
        let p = tmp_ckpt("trunc");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        let after_two = wal_len(&p);
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        drop(db);
        let data = std::fs::read(wal_path(&p)).unwrap();
        std::fs::write(wal_path(&p), &data[..data.len() - 5]).unwrap();
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 2);
        assert_eq!(wal_len(&p), after_two);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
        cleanup(&p);
    }

    #[test]
    fn non_monotonic_lsn_is_treated_as_a_corrupt_tail() {
        let p = tmp_ckpt("lsn");
        // A regression (5 then 3) must stop the intact prefix, not be
        // replayed as-is: the next append would otherwise reuse an LSN.
        write_wal_lsns(
            &wal_path(&p),
            1,
            &[
                (5, vec!["CREATE TABLE t (a INTEGER);".to_string()]),
                (3, vec!["INSERT INTO t VALUES (1);".to_string()]),
                (4, vec!["INSERT INTO t VALUES (2);".to_string()]),
            ],
        );
        let (mut db, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 1);
        assert!(info.truncated_bytes > 0);
        assert_eq!(records_of(&wal_path(&p)).len(), 1);
        // The next append continues past the intact prefix (max lsn + 1).
        db.execute("INSERT INTO t VALUES (7);").unwrap();
        assert_eq!(
            records_of(&wal_path(&p))
                .iter()
                .map(|r| r.lsn)
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![7]);
        cleanup(&p);
    }

    #[test]
    fn crc_valid_record_with_bad_sql_is_a_hard_error() {
        let p = tmp_ckpt("hardsql");
        let wal = wal_path(&p);
        // A syntactically fine statement that cannot bind/execute.
        write_raw_wal(&wal, &[vec!["INSERT INTO missing VALUES (1);".to_string()]]);
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("WAL replay failed at lsn 1"), "{msg}");
        assert!(msg.contains("not found"), "{msg}");

        // Mid-file: intact records on either side of the bad one must not let
        // recovery skip or truncate around a committed record — replay stops
        // at the CRC-valid bad record and hard-errors (never a silent drop).
        let p = tmp_ckpt("hardmid");
        let wal = wal_path(&p);
        write_raw_wal(
            &wal,
            &[
                vec!["CREATE TABLE t (a INTEGER);".to_string()],
                vec!["INSERT INTO missing VALUES (1);".to_string()],
                vec!["INSERT INTO t VALUES (1);".to_string()],
            ],
        );
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("WAL replay failed at lsn 2"), "{msg}");
        assert!(msg.contains("not found"), "{msg}");

        // A CRC-valid record whose payload framing is broken.
        let p = tmp_ckpt("hardframe");
        let wal = wal_path(&p);
        let text = b"SELECT 1";
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u32.to_le_bytes()); // claims two statements
        payload.extend_from_slice(&(text.len() as u32).to_le_bytes());
        payload.extend_from_slice(text);
        write_raw_wal_payload(&wal, &payload);
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("corrupt WAL record"), "{err}");
        cleanup(&p);
    }

    #[test]
    fn plaintext_crc_mismatch_mid_file_is_a_hard_error() {
        let p = tmp_ckpt("plaincrcmid");
        let wal = wal_path(&p);
        write_raw_wal(
            &wal,
            &[
                vec!["CREATE TABLE t (a INTEGER);".to_string()],
                vec!["INSERT INTO t VALUES (1);".to_string()],
                vec!["INSERT INTO t VALUES (2);".to_string()],
            ],
        );
        // Flip a payload byte of the FIRST record while records follow: this is
        // mid-file corruption, so replay hard-errors instead of silently
        // truncating the committed records after it.
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[HEADER_LEN + RECORD_HEADER_LEN] ^= 0xff;
        std::fs::write(&wal, &bytes).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("corrupt WAL record"), "{msg}");
        assert!(msg.contains("mid-file"), "{msg}");

        // The same flip on the LAST record is a torn-tail CRC mismatch and still
        // truncates cleanly: the intact prefix replays (table t, no rows).
        let p2 = tmp_ckpt("plaincrctail");
        let wal2 = wal_path(&p2);
        write_raw_wal(
            &wal2,
            &[
                vec!["CREATE TABLE t (a INTEGER);".to_string()],
                vec!["INSERT INTO t VALUES (1);".to_string()],
            ],
        );
        let mut bytes = std::fs::read(&wal2).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&wal2, &bytes).unwrap();
        let mut db = PinkDb::open_durable(p2.to_str().unwrap()).unwrap();
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            Vec::<i64>::new()
        );

        cleanup(&p2);
        cleanup(&p);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn an_unsupported_container_version_is_never_torn() {
        // A short file whose container header names a future version must be a
        // hard error, not a torn creation (its records, if any, are preserved
        // for a newer build).
        let mut data = crate::crypto::ENC_MAGIC.to_vec();
        data.extend_from_slice(&99u32.to_le_bytes());
        data.resize(32, 0);
        let err = parse_wal_header(&data).unwrap_err();
        assert!(matches!(err, HeaderError::ContainerVersion(99)), "{err:?}");
        assert!(!header_torn(data.len(), &err), "must not be torn");
        assert!(header_error(Path::new("w"), &err).contains("99"));
    }

    #[test]
    fn wal_header_validation() {
        let p = tmp_ckpt("badmagic");
        // Invalid magic on a file longer than one header (it could hold
        // records): hard error.
        std::fs::write(wal_path(&p), vec![b'X'; HEADER_LEN + 40]).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("bad magic"), "{err}");

        // Invalid magic but no longer than one header: a torn creation,
        // recreated (this was the pre-v3 rule at 20 bytes; one header is 96).
        std::fs::write(wal_path(&p), b"NOTAWAL1\0\0\0\0extrabytes").unwrap();
        let db = PinkDb::open_durable(p.to_str().unwrap()).unwrap();
        assert_eq!(wal_len(&p), HEADER_LEN as u64);
        drop(db);

        // The pre-generation format (version 1) with records: hard error.
        let mut old = Vec::new();
        old.extend_from_slice(WAL_MAGIC);
        old.extend_from_slice(&1u32.to_le_bytes());
        old.extend_from_slice(b"payload-bytes");
        std::fs::write(wal_path(&p), &old).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().contains("version 1 is not supported"),
            "{err}"
        );

        // A future version is refused cleanly through the same BadVersion
        // path an older build would use to refuse our version 4 (that
        // direction cannot be executed from this tree).
        let mut future = Vec::new();
        future.extend_from_slice(WAL_MAGIC);
        future.extend_from_slice(&5u32.to_le_bytes());
        future.extend_from_slice(b"payload-bytes");
        std::fs::write(wal_path(&p), &future).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().contains("version 5 is not supported"),
            "{err}"
        );

        // A v3 header whose CRC does not verify, on a record-bearing file:
        // hard error (a corrupted header could name the wrong generation).
        let mut bytes = Wal::header_fresh(7).to_vec();
        bytes[20] ^= 0xFF; // inside wal_id (covered by the header CRC)
        bytes.extend_from_slice(b"garbage-bytes-here");
        std::fs::write(wal_path(&p), &bytes).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"), "{err}");

        // Nonzero reserved header bytes are a format error, not a torn write.
        let mut bytes = Wal::header_fresh(7).to_vec();
        bytes[88] = 1;
        bytes.extend_from_slice(b"garbage-bytes-here");
        std::fs::write(wal_path(&p), &bytes).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("reserved bytes"), "{err}");

        // Exactly one header of garbage: a torn header, recreated.
        std::fs::write(wal_path(&p), vec![0xABu8; HEADER_LEN]).unwrap();
        let db = PinkDb::open_durable(p.to_str().unwrap()).unwrap();
        assert!(db.is_durable());
        assert_eq!(wal_len(&p), HEADER_LEN as u64);
        drop(db);

        // A short prefix is recreated too.
        std::fs::write(wal_path(&p), b"PIN").unwrap();
        let db = PinkDb::open_durable(p.to_str().unwrap()).unwrap();
        assert!(db.is_durable());
        assert_eq!(wal_len(&p), HEADER_LEN as u64);
        drop(db);

        // Garbage after a valid header: intact prefix empty, tail truncated.
        let mut bytes = Wal::header_fresh(7).to_vec();
        bytes.extend_from_slice(b"garbage-bytes-here");
        std::fs::write(wal_path(&p), &bytes).unwrap();
        let (db, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.truncated_bytes, 18);
        assert_eq!(wal_len(&p), HEADER_LEN as u64);
        drop(db);
        cleanup(&p);
    }

    /// Wave 45m: a v2 WAL (20-byte header, no `wal_id`) is accepted and
    /// upgraded in place to v3 under the lease: records are byte-preserved,
    /// the header becomes v3 with a fresh `wal_id` and slot A seeded with
    /// the last intact LSN.
    #[test]
    fn legacy_v2_wal_upgrades_in_place() {
        let p = tmp_ckpt("v2upgrade");
        write_pink(&p, &[0]);
        // A hand-built v2 WAL: magic + version 2 + gen 1, then two records.
        let wal = wal_path(&p);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WAL_MAGIC);
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes());
        let recs = raw(&[
            vec!["INSERT INTO t VALUES (1);".to_string()],
            vec!["INSERT INTO t VALUES (2);".to_string()],
        ]);
        let record_bytes_start = bytes.len();
        for (lsn, stmts) in &recs {
            let payload = encode_payload_legacy_for_test(stmts);
            bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&lsn.to_le_bytes());
            bytes.extend_from_slice(&crc32(&payload).to_le_bytes());
            bytes.extend_from_slice(&payload);
        }
        std::fs::write(&wal, &bytes).unwrap();
        let record_bytes = bytes[record_bytes_start..].to_vec();

        let (mut db, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 2);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);

        // The file is now v4: 96-byte header, the records re-encoded in the
        // v4 payload layout (same statements, same LSNs, same order).
        let data = std::fs::read(&wal).unwrap();
        assert_eq!(u32::from_le_bytes(data[8..12].try_into().unwrap()), 4);
        assert_eq!(
            u32::from_le_bytes(data[36..40].try_into().unwrap()),
            crc32(&data[..36])
        );
        assert_ne!(&data[20..36], [0u8; 16], "fresh wal_id");
        assert_eq!(
            slot_max_in(&data, WAL_SLOT_A as usize, WAL_SLOT_DOMAIN),
            1,
            "upgrade seeds slot A"
        );
        let (recs, end) =
            parse_records(&data, RECORDS_START, None, 0, 0, true).expect("v4 records decode");
        let lsns: Vec<u64> = recs.iter().map(|r| r.lsn).collect();
        assert_eq!(lsns, vec![1, 2]);
        assert!(recs.iter().all(|r| r.sets.is_none()), "legacy sets unknown");
        assert_eq!(end, data.len(), "no trailing bytes after the upgrade");
        assert!(!record_bytes.is_empty());

        // A leftover `.upgrade` temp (a crash mid-upgrade) is ignored and
        // harmless (design row R1).
        std::fs::write(upgrade_path(&wal, 1), b"stale").unwrap();
        drop(db);
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 2, "idempotent: no re-append");
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            vec![0, 1, 2]
        );
        drop(back);
        cleanup(&p);
    }

    /// The publication slots: both a torn slot and a slot pair with a bad
    /// CRC are ignored (max over valid slots); a corrupted slot never
    /// validates.
    #[test]
    fn torn_commit_slots_are_ignored() {
        let p = tmp_ckpt("tornslots");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        drop(db);
        let wal = wal_path(&p);
        let mut data = std::fs::read(&wal).unwrap();
        // Three records: A holds seq 3 (value 3), B holds seq 2. Corrupting
        // A's CRC leaves the reader with seq 2.
        data[WAL_SLOT_A as usize + 16] ^= 0xFF;
        let h = parse_wal_header(&data).unwrap();
        assert_eq!(h.published_seq, 2);
        // Corrupt both: no valid slot at all, and the file still opens (the
        // slots are hints; the frames are authoritative).
        data[WAL_SLOT_B as usize + 16] ^= 0xFF;
        std::fs::write(&wal, &data).unwrap();
        let h = parse_wal_header(&data).unwrap();
        assert_eq!(h.published_seq, 0);
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        // The next append publishes into a fresh sequence (A at seq 1).
        back.execute("INSERT INTO t VALUES (3);").unwrap();
        drop(back);
        let data = std::fs::read(&wal).unwrap();
        assert_eq!(slot_max_in(&data, WAL_SLOT_A as usize, WAL_SLOT_DOMAIN), 1);
        cleanup(&p);
    }

    /// The epoch region in the lock file: bumps alternate slots, a torn slot
    /// is ignored by the max, and the reader's stale() comparison works.
    #[test]
    fn lock_epoch_slots_tolerate_a_torn_write() {
        let p = tmp_ckpt("epochslots");
        let db = durable(&p);
        let lease = db.lease_handle().unwrap();
        assert_eq!(lease.epoch_now(), Some(0), "a fresh lock file has no slots");
        {
            let g = lease.acquire().unwrap();
            g.bump_epoch(1);
            g.bump_epoch(2);
            g.bump_epoch(3);
        }
        assert_eq!(lease.epoch_now(), Some(3));
        let lock_path = format!("{}.lock", p.display());
        // Bumps alternate slots: A holds seq 3 (value 3), B holds seq 2.
        // Corrupting B leaves the max at seq 3.
        let mut raw = std::fs::read(&lock_path).unwrap();
        raw[24 + 16] ^= 0xFF;
        std::fs::write(&lock_path, &raw).unwrap();
        assert_eq!(lease.epoch_now(), Some(3));
        // Corrupt both slots: no valid epoch, and the bump sequence restarts
        // (the pair is a hint; a lost update costs a redundant refresh).
        raw[16] ^= 0xFF;
        std::fs::write(&lock_path, &raw).unwrap();
        assert_eq!(lease.epoch_now(), Some(0));
        lease.cache_epoch_now();
        assert!(!lease.stale(), "cache now holds 0");
        {
            let g = lease.acquire().unwrap();
            g.bump_epoch(9);
        }
        assert_eq!(lease.epoch_now(), Some(1), "seq restarts after total loss");
        // The publisher's own cache follows its bump (a self-bump is never
        // "stale for us"), and a foreign bump still compares unequal.
        assert!(!lease.stale());
        assert_eq!(lease.epoch_cache_value(), 1);
        lease.cache_epoch_now();
        assert!(!lease.stale());
        drop(db);
        cleanup(&p);
    }

    /// Wave 45m M1 (wave 45o semantics): the busy timeout is what a second
    /// process sees while another holds the lease (holding the flock from an
    /// independent fd in the test process is exactly what `flock` sees from
    /// another process). The report names both causes. Because the statement
    /// executed optimistically before the lease could be taken, the handle
    /// fail-stops (the uncommitted effect must never be observable) and a
    /// reopen recovers exactly the durable prefix.
    #[test]
    fn lease_reports_busy_after_the_timeout() {
        let p = tmp_ckpt("busy");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let foreign = {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(format!("{}.lock", p.display()))
                .unwrap();
            f.try_lock().unwrap();
            f
        };
        db.set_busy_timeout(Duration::from_millis(30));
        let err = db.execute("INSERT INTO t VALUES (1);").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("is busy"), "{msg}");
        assert!(msg.contains("older pinkdb build"), "{msg}");
        assert!(msg.contains("stopped after a write failure"), "{msg}");
        // The refused statement's effect is not durable and no longer
        // observable: the handle fail-stops until reopened.
        let read_err = db.execute("SELECT a FROM t;").unwrap_err().to_string();
        assert!(
            read_err.contains("stopped after a WAL write failure"),
            "{read_err}"
        );
        drop(db);
        // After the foreign holder exits, a fresh handle sees exactly the
        // durable prefix (no row) and writes normally.
        drop(foreign);
        let mut back = durable(&p);
        assert!(ints(&mut back, "SELECT a FROM t;").is_empty());
        back.execute("INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        drop(back);
        cleanup(&p);
    }

    /// M1: `UnsafeSingleProcess` restores today's semantics when the
    /// filesystem cannot lock; `Required` (the default) refuses to proceed.
    /// The `Unsupported` arm is injected through a test-only switch because
    /// no local filesystem here lacks `flock`.
    #[test]
    fn unsupported_locking_fails_closed_unless_opted_out() {
        let p = tmp_ckpt("unsupported");
        let opts = |mode| DurableOpenOptions {
            lock_mode: mode,
            ..DurableOpenOptions::default()
        };
        FORCE_UNSUPPORTED.with(|f| f.set(true));
        let err = PinkDb::open_durable_with_options(p.to_str().unwrap(), opts(LockMode::Required))
            .unwrap_err()
            .to_string();
        assert!(err.contains("file locking is not supported"), "{err}");
        // The escape hatch opens and writes without coordination.
        let (mut db, _) = PinkDb::open_durable_with_options(
            p.to_str().unwrap(),
            opts(LockMode::UnsafeSingleProcess),
        )
        .unwrap();
        db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        drop(db);
        FORCE_UNSUPPORTED.with(|f| f.set(false));
        // The default mode still opens the database normally afterwards.
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    #[test]
    fn oversized_record_payload_is_refused_at_the_u32_guard() {
        assert_eq!(record_len_guard(u32::MAX as usize).unwrap(), u32::MAX);
        #[cfg(target_pointer_width = "64")]
        {
            let err = record_len_guard(u32::MAX as usize + 1).unwrap_err();
            assert!(err.to_string().contains("exceeds the u32"), "{err}");
        }
        // Real encodings stay far under the limit.
        let payload = encode_payload(&["SELECT 1".to_string()], None).unwrap();
        assert!(payload.len() < 100);
    }

    #[test]
    fn pre_generation_roll_name_is_refused() {
        let p = tmp_ckpt("legacyroll");
        let legacy = PathBuf::from(format!("{}.roll", wal_path(&p).display()));
        std::fs::write(&legacy, b"not a generation-named roll").unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().contains("pre-generation roll name"),
            "{err}"
        );
        cleanup(&p);
    }

    /// A roll must self-identify: the generation in its name and in its
    /// header agree, or recovery refuses to touch it.
    #[test]
    fn roll_name_must_match_its_header_generation() {
        let p = tmp_ckpt("rollmismatch");
        write_pink(&p, &[0]);
        write_wal_lsns(
            &roll_path(&wal_path(&p), 1),
            2, // header says 2, name says 1
            &raw(&[vec!["INSERT INTO t VALUES (1);".to_string()]]),
        );
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("header says 2"), "{err}");
        cleanup(&p);
    }

    #[test]
    fn checkpoint_preserves_state_truncates_wal_and_skips_replay() {
        let p = tmp_ckpt("ckpt");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        db.execute("COMMIT;").unwrap();
        // Three commit units: CREATE and INSERT were autocommit records; the
        // transaction's INSERT logs as exactly one record at COMMIT (BEGIN/
        // COMMIT themselves are never records).
        let recs = records_of(&wal_path(&p));
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[2].lsn, 3);
        assert_eq!(recs[2].stmts, vec!["INSERT INTO t VALUES (2)".to_string()]);

        db.checkpoint().unwrap();
        assert!(p.exists(), "checkpoint file written");
        assert_eq!(
            wal_len(&p),
            HEADER_LEN as u64,
            "WAL rolled to a fresh header"
        );
        assert!(records_of(&wal_path(&p)).is_empty());
        assert!(!tmp_path(&p).exists() && !roll_path(&wal_path(&p), 1).exists());
        drop(db);

        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 0, "checkpoint needs no replay");
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        // Post-checkpoint commits land in the fresh WAL generation.
        back.execute("INSERT INTO t VALUES (3);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(
            ints(&mut again, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3]
        );
        cleanup(&p);
    }

    /// H2: a refresh replays another process's `USE` in the WAL lineage
    /// session but must never let it hijack this process's local session.
    #[test]
    fn refresh_does_not_hijack_the_local_session() {
        let p = tmp_ckpt("sessionhijack");
        let mut a = durable(&p);
        a.execute(
            "CREATE DATABASE db1; \
             CREATE TABLE main.t (a INTEGER); \
             CREATE TABLE db1.t (a INTEGER);",
        )
        .unwrap();
        let mut b = durable(&p);
        b.execute("INSERT INTO main.t VALUES (10);").unwrap();
        // a logs a USE + an insert in db1; b refreshes past both.
        a.execute("USE db1; INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(a.current_db(), "db1");
        assert_eq!(ints(&mut b, "SELECT a FROM db1.t;"), vec![1]);
        assert_eq!(
            b.current_db(),
            "main",
            "another process's USE must not move the local session"
        );
        assert_eq!(ints(&mut b, "SELECT a FROM t;"), vec![10]);
        // The writer's session is untouched either.
        assert_eq!(a.current_db(), "db1");
        assert_eq!(ints(&mut a, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    /// H5: replay is not gated by the front-end `LOAD EXTENSION` switch. A
    /// reader that disabled SQL `LOAD EXTENSION` still *attempts* the load a
    /// replayed record needs — the failure is the loader's (a missing file),
    /// never the front-end's "disabled" refusal — and its flag is restored
    /// afterwards.
    #[test]
    fn refresh_replay_bypasses_the_sql_load_extension_gate() {
        let p = tmp_ckpt("h5gate");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut b = durable(&p);
        b.set_allow_sql_load_extension(false);
        // A record that needs an extension the file does not provide.
        let wal = wal_path(&p);
        let mut data = std::fs::read(&wal).unwrap();
        let sql = "LOAD EXTENSION '/nonexistent/pinkdb-h5-test.dylib';";
        let payload = encode_payload(&[sql.to_string()], None).unwrap();
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&crc32(&payload).to_le_bytes());
        data.extend_from_slice(&payload);
        std::fs::write(&wal, &data).unwrap();
        // Publish the change from the *other* handle's lease (a crashed
        // publisher's epoch bump, simulated): b's cache stays behind, so its
        // next statement scans and replays the manual frame.
        a.lease_handle().unwrap().bump_epoch(2);
        let err = b.execute("SELECT 1;").unwrap_err().to_string();
        assert!(err.contains("refresh failed"), "{err}");
        assert!(
            !err.contains("disabled on this connection"),
            "replay must bypass the front-end gate: {err}"
        );
        assert!(
            err.to_lowercase().contains("dylib") || err.contains("load"),
            "{err}"
        );
        drop(b);
        drop(a);
        cleanup(&p);
    }

    /// H4/H9/H6: a replayed `CREATE TABLE` colliding with this process's
    /// session-local view is a hard error that poisons the handle — committed
    /// data is never silently dropped and the partially advanced store is
    /// never observable.
    #[test]
    fn refresh_replay_error_poisons_the_handle() {
        let p = tmp_ckpt("replaypoison");
        let mut a = durable(&p);
        a.execute("CREATE TABLE base (x INTEGER);").unwrap();
        let mut b = durable(&p);
        // b's session-local view is named like the table `a` will create.
        b.execute("CREATE VIEW t AS SELECT 1 AS x;").unwrap();
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        // b's next statement refreshes; the replayed CREATE TABLE collides.
        let err = b.execute("SELECT 1;").unwrap_err().to_string();
        assert!(err.contains("refresh failed"), "{err}");
        // Every later statement fail-stops (the store may hold a partial
        // replay; it must not be observed).
        let err = b.execute("SELECT 1;").unwrap_err().to_string();
        assert!(err.contains("stopped after a WAL write failure"), "{err}");
        drop(b);
        // A fresh open recovers the durable prefix.
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT COUNT(*) FROM t;"), vec![0]);
        cleanup(&p);
    }

    /// H10: a full rebuild (another process's checkpoint) preserves this
    /// process's session-local views and session; it is the reloaded catalog
    /// plus the local, unpersisted facets.
    #[test]
    fn full_rebuild_preserves_local_views_and_session() {
        let p = tmp_ckpt("rebuildlocals");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        let mut b = durable(&p);
        b.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        b.execute("INSERT INTO t VALUES (5);").unwrap();
        assert_eq!(ints(&mut b, "SELECT COUNT(*) FROM v;"), vec![2]);
        // a checkpoints (its state has rows 1 and 5): b's next statement does
        // a full rebuild and must keep its view.
        a.execute("INSERT INTO t VALUES (2); checkpoint;").unwrap();
        assert_eq!(
            ints(&mut b, "SELECT COUNT(*) FROM v;"),
            vec![3],
            "the local view must survive the rebuild and see the reloaded rows"
        );
        assert_eq!(b.current_db(), "main");
        // The replayed state is complete on both sides.
        assert_eq!(ints(&mut b, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 5]);
        assert_eq!(ints(&mut a, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 5]);
        cleanup(&p);
    }

    /// W2 self-healing: a crashed publisher left durable-but-unpublished
    /// frames (an appended record with no epoch bump). Already-open readers
    /// lag until later lease activity (the documented residual gap), but the
    /// next writer re-checks the file under the lease (`needs_scan` compares
    /// the length) and applies them before appending.
    #[test]
    fn unpublished_frames_are_recovered_by_the_next_lease_activity() {
        let p = tmp_ckpt("w2heal");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut b = durable(&p);
        assert!(ints(&mut b, "SELECT a FROM t;").is_empty());
        // A crashed publisher: append and fsync a record, never publish.
        let wal = wal_path(&p);
        let mut data = std::fs::read(&wal).unwrap();
        let payload = encode_payload(&["INSERT INTO t VALUES (7);".to_string()], None).unwrap();
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&crc32(&payload).to_le_bytes());
        data.extend_from_slice(&payload);
        std::fs::write(&wal, &data).unwrap();
        // The reader trusting its epoch still serves the old view.
        assert!(ints(&mut b, "SELECT a FROM t;").is_empty());
        // The next write scans under the lease and applies the frame before
        // appending its own.
        b.execute("INSERT INTO t VALUES (8);").unwrap();
        assert_eq!(ints(&mut b, "SELECT a FROM t ORDER BY a;"), vec![7, 8]);
        drop(a);
        drop(b);
        // A fresh open sees both, exactly once.
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![7, 8]);
        cleanup(&p);
    }

    /// Design 6.5: the auto-checkpoint additionally requires this process to
    /// own at least half of the live WAL, so N writers do not round-robin
    /// the whole store. A process that reopens an existing WAL owns nothing
    /// of it: it appends until its share is half, then the next commit folds.
    #[test]
    fn auto_checkpoint_requires_a_meaningful_share() {
        let p = tmp_ckpt("ownshare");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        for i in 0..50 {
            a.execute(&format!("INSERT INTO t VALUES ({i});")).unwrap();
        }
        let seeded = wal_len(&p);
        drop(a);
        let mut b = durable(&p);
        b.set_auto_checkpoint_bytes(Some(1));
        // b owns nothing of the seeded bytes: the first appends must not
        // trigger a checkpoint.
        b.execute("INSERT INTO t VALUES (1000);").unwrap();
        assert!(wal_len(&p) > seeded, "no checkpoint triggered");
        // Keep appending until b owns at least half of the live WAL; one of
        // those commits then folds it (the WAL returns to a bare header).
        let mut triggered = false;
        let mut inserted = 1i64; // the probe insert above
        for i in 0..200 {
            b.execute(&format!("INSERT INTO t VALUES ({});", 1000 + i))
                .unwrap();
            inserted += 1;
            if wal_len(&p) == HEADER_LEN as u64 {
                triggered = true;
                break;
            }
        }
        assert!(triggered, "attribution never reached half the WAL");
        drop(b);
        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT COUNT(*) FROM t;"),
            vec![50 + inserted],
            "all rows survived the attributed checkpoint"
        );
        cleanup(&p);
    }

    /// A snapshot over a target whose lease is currently held (another
    /// process is writing or checkpointing it) is refused; once released it
    /// succeeds.
    #[test]
    fn save_snapshot_refuses_a_lease_held_target() {
        let a = tmp_ckpt("snaplease_a");
        let b = tmp_ckpt("snaplease_b");
        let _idle = durable(&b);
        let foreign = {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(format!("{}.lock", b.display()))
                .unwrap();
            f.try_lock().unwrap();
            f
        };
        let mut aa = durable(&a);
        aa.execute("CREATE TABLE t (a INTEGER);").unwrap();
        std::fs::write(b.to_str().unwrap(), b"stale").unwrap();
        let err = aa.save_snapshot(b.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("in use"), "{err}");
        drop(foreign);
        aa.save_snapshot(b.to_str().unwrap()).unwrap();
        let mut back = durable(&b);
        assert_eq!(ints(&mut back, "SELECT COUNT(*) FROM t;"), vec![0]);
        drop(back);
        drop(aa);
        cleanup(&a);
        cleanup(&b);
    }

    /// The epoch fast path: an unchanged publication counter skips the
    /// refresh entirely; a foreign bump makes the next statement refresh.
    #[test]
    fn epoch_fast_path_skips_unchanged_publications() {
        let p = tmp_ckpt("fastpath");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut b = durable(&p);
        assert_eq!(ints(&mut b, "SELECT COUNT(*) FROM t;"), vec![0]);
        let lb = b.lease_handle().unwrap();
        assert!(!lb.stale(), "a refreshed store is at the current epoch");
        assert_eq!(lb.epoch_now(), Some(lb.epoch_cache_value()));
        // The next read does not change anything.
        assert!(!lb.stale());
        // A commit elsewhere makes the epoch unequal.
        a.execute("INSERT INTO t VALUES (1);").unwrap();
        assert!(lb.stale());
        assert_eq!(ints(&mut b, "SELECT a FROM t;"), vec![1]);
        assert!(!lb.stale());
        drop(a);
        drop(b);
        cleanup(&p);
    }

    /// C1 regression: a crash after the WAL roll but before the publish must
    /// lose nothing — recovery has to make the recovered state durable before
    /// it removes `.tmp`/`.roll`, and a second clean open must see the rows.
    #[test]
    fn interrupted_checkpoint_is_published_before_rolls_are_removed() {
        let p = tmp_ckpt("ckptcrash1");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            // Crash after step 1+2 of the checkpoint protocol: full state in
            // `.tmp`, old WAL moved to `.roll` of generation 1, a fresh
            // header-only WAL of generation 2. `.pink` never existed.
            db.save_sync(&tmp_path(&p)).unwrap();
            std::fs::rename(wal_path(&p), roll_path(&wal_path(&p), 1)).unwrap();
            Wal::create_file(&wal_path(&p), 2, None).unwrap();
        }
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.checkpoints_completed, 1, "recovery must publish .tmp");
        assert_eq!(
            info.replayed_records, 0,
            "the roll is folded by the publish"
        );
        assert_eq!(info.stale_rolls_removed, 1);
        assert!(
            p.exists(),
            "the recovered checkpoint must be a durable .pink"
        );
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        drop(back);

        // The second clean open (no crash in between) must not lose the rows
        // that the old recovery kept only in memory.
        let (mut again, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 0);
        assert_eq!(info.checkpoints_completed, 0);
        assert_eq!(ints(&mut again, "SELECT a FROM t;"), vec![1]);
        assert!(!tmp_path(&p).exists());
        assert!(scan_rolls(&wal_path(&p)).unwrap().is_empty());
        drop(again);
        cleanup(&p);
    }

    /// The mirror window: publish completed, roll removal did not. Recovery
    /// must delete the (folded) roll and never replay it again.
    #[test]
    fn published_checkpoint_with_leftover_roll_is_harmless() {
        let p = tmp_ckpt("ckptcrash2");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            // Crash after the publish rename but before the roll was removed.
            db.save_sync(&tmp_path(&p)).unwrap();
            std::fs::rename(tmp_path(&p), &p).unwrap();
            std::fs::rename(wal_path(&p), roll_path(&wal_path(&p), 1)).unwrap();
            Wal::create_file(&wal_path(&p), 2, None).unwrap();
        }
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 0, "stale roll must not be replayed");
        assert_eq!(info.checkpoints_completed, 0);
        assert_eq!(info.stale_rolls_removed, 1);
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        assert!(scan_rolls(&wal_path(&p)).unwrap().is_empty());
        drop(back);
        let mut again = durable(&p);
        assert_eq!(ints(&mut again, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    /// The reviewer's C1 sub-case: an earlier recovery crashed between
    /// removing `.tmp` and `.roll`, leaving `{no tmp, stale roll}`. The roll
    /// must be recognized as folded (generation < live generation) — never
    /// replayed onto a checkpoint that already contains it.
    #[test]
    fn crash_between_recovery_removes_reconverges() {
        let p = tmp_ckpt("recmid");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            // Interrupted checkpoint image ...
            db.save_sync(&tmp_path(&p)).unwrap();
            std::fs::rename(wal_path(&p), roll_path(&wal_path(&p), 1)).unwrap();
            Wal::create_file(&wal_path(&p), 2, None).unwrap();
        }
        // ... and recovery crashing right after its publish, before the roll
        // unlink (i.e. the exact intermediate state of the fix).
        std::fs::rename(tmp_path(&p), &p).unwrap();
        let (mut db, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.checkpoints_completed, 0);
        assert_eq!(info.stale_rolls_removed, 1);
        assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
        drop(db);

        // The mirror image: `.tmp` discarded (live WAL authoritative) but the
        // stale roll survived; recovery must reach the same rows again.
        let p2 = tmp_ckpt("recmid2");
        {
            let mut db = durable(&p2);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            db.save_sync(&tmp_path(&p2)).unwrap();
            db.execute("INSERT INTO t VALUES (2);").unwrap(); // live WAL: row 2
            std::fs::rename(wal_path(&p2), roll_path(&wal_path(&p2), 1)).unwrap();
            Wal::create_file(&wal_path(&p2), 2, None).unwrap();
        }
        let (mut db, info) =
            PinkDb::open_durable_with(p2.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 0);
        assert_eq!(info.stale_rolls_removed, 1);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1]);
        drop(db);
        cleanup(&p);
        cleanup(&p2);
    }

    /// C2 regression: a stale roll (whose record is already folded into the
    /// checkpoint) next to a fresh `.tmp` must not be replayed again — the
    /// live WAL is authoritative and the stale roll is discarded by
    /// generation, not applied.
    #[test]
    fn stale_roll_with_fresh_tmp_does_not_duplicate() {
        let p = tmp_ckpt("staleroll");
        // The checkpoint already contains row 1 (folded from the stale roll).
        write_pink(&p, &[0, 1]);
        // A stale roll of generation 1 re-appearing (failed unlink earlier).
        write_wal_lsns(
            &roll_path(&wal_path(&p), 1),
            1,
            &raw(&[vec!["INSERT INTO t VALUES (1);".to_string()]]),
        );
        // The live WAL (generation 2) holds row 2.
        write_wal_lsns(
            &wal_path(&p),
            2,
            &raw(&[vec!["INSERT INTO t VALUES (2);".to_string()]]),
        );
        // A checkpoint that crashed during step 1 (its `.tmp` is not trusted
        // while the live WAL still has records).
        write_pink(&tmp_path(&p), &[0, 1, 2]);

        let (mut db, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(
            info.checkpoints_completed, 0,
            ".tmp is not provably complete"
        );
        assert_eq!(info.replayed_records, 1, "only the live WAL record");
        assert_eq!(info.stale_rolls_removed, 1);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);
        drop(db);
        // Idempotent: no re-application, no duplicate.
        let (mut again, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(
            ints(&mut again, "SELECT a FROM t ORDER BY a;"),
            vec![0, 1, 2]
        );
        cleanup(&p);
    }

    /// Every on-disk combination of `{tmp, roll, wal, pink}` from an
    /// interrupted checkpoint: exact expected state, no loss, no duplicates,
    /// and idempotent recovery (three opens in a row).
    #[test]
    fn interrupted_checkpoint_state_matrix() {
        enum Expect {
            Rows(Vec<i64>),
            Error(&'static str),
        }
        struct Image {
            name: &'static str,
            pink: Option<Vec<i64>>,
            tmp: Option<Vec<i64>>,
            rolls: Vec<(u64, Vec<Vec<String>>)>,
            /// Live WAL generation + records (`None` with `torn_wal` false
            /// means the file is absent).
            wal: Option<(u64, Vec<Vec<String>>)>,
            torn_wal: bool,
            expect: Expect,
        }

        let cr = vec!["CREATE TABLE t (a INTEGER);".to_string()];
        let i0 = vec!["INSERT INTO t VALUES (0);".to_string()];
        let i1 = vec!["INSERT INTO t VALUES (1);".to_string()];
        let i2 = vec!["INSERT INTO t VALUES (2);".to_string()];
        use Expect::{Error, Rows};

        let cases = vec![
            Image {
                name: "empty",
                pink: None,
                tmp: None,
                rolls: vec![],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![]),
            },
            Image {
                name: "pink_only",
                pink: Some(vec![0]),
                tmp: None,
                rolls: vec![],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![0]),
            },
            Image {
                name: "wal_only",
                pink: None,
                tmp: None,
                rolls: vec![],
                wal: Some((1, vec![cr.clone(), i0.clone()])),
                torn_wal: false,
                expect: Rows(vec![0]),
            },
            Image {
                name: "pink_wal",
                pink: Some(vec![0]),
                tmp: None,
                rolls: vec![],
                wal: Some((1, vec![i1.clone()])),
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_only",
                pink: None,
                tmp: Some(vec![0]),
                rolls: vec![],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![]),
            },
            Image {
                name: "tmp_pink",
                pink: Some(vec![0]),
                tmp: Some(vec![0]),
                rolls: vec![],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![0]),
            },
            Image {
                name: "tmp_wal_records",
                pink: None,
                tmp: Some(vec![0, 1]),
                rolls: vec![],
                wal: Some((1, vec![cr.clone(), i0.clone(), i1.clone()])),
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_pink_wal_records",
                pink: Some(vec![0]),
                tmp: Some(vec![0, 1]),
                rolls: vec![],
                wal: Some((1, vec![i1.clone()])),
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "roll_without_checkpoint",
                pink: None,
                tmp: None,
                rolls: vec![(1, vec![cr.clone(), i0.clone()])],
                wal: Some((2, vec![])),
                torn_wal: false,
                expect: Error("checkpoint is missing"),
            },
            Image {
                name: "roll_no_live_no_pink",
                pink: None,
                tmp: None,
                rolls: vec![(1, vec![cr.clone(), i0.clone()])],
                wal: None,
                torn_wal: false,
                expect: Error("checkpoint is missing"),
            },
            Image {
                name: "roll_pink_no_live",
                pink: Some(vec![0]),
                tmp: None,
                rolls: vec![(1, vec![cr.clone(), i0.clone()])],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![0]),
            },
            Image {
                name: "roll_pink_live_empty",
                pink: Some(vec![0]),
                tmp: None,
                rolls: vec![(1, vec![cr.clone(), i0.clone()])],
                wal: Some((2, vec![])),
                torn_wal: false,
                expect: Rows(vec![0]),
            },
            Image {
                name: "roll_stale_live_records",
                pink: Some(vec![0, 1]),
                tmp: None,
                rolls: vec![(1, vec![i1.clone()])],
                wal: Some((2, vec![i2.clone()])),
                torn_wal: false,
                expect: Rows(vec![0, 1, 2]),
            },
            Image {
                name: "roll_not_older",
                pink: Some(vec![0]),
                tmp: None,
                rolls: vec![(2, vec![i1.clone()])],
                wal: Some((2, vec![])),
                torn_wal: false,
                expect: Error("does not precede"),
            },
            Image {
                name: "tmp_roll_no_pink",
                pink: None,
                tmp: Some(vec![0, 1]),
                rolls: vec![(1, vec![cr.clone(), i0.clone(), i1.clone()])],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_roll_live_no_pink",
                pink: None,
                tmp: Some(vec![0, 1]),
                rolls: vec![(1, vec![cr.clone(), i0.clone(), i1.clone()])],
                wal: Some((2, vec![])),
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_roll_pink",
                pink: Some(vec![0]),
                tmp: Some(vec![0, 1]),
                rolls: vec![(1, vec![i1.clone()])],
                wal: Some((2, vec![])),
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_roll_pink_no_live",
                pink: Some(vec![0]),
                tmp: Some(vec![0, 1]),
                rolls: vec![(1, vec![i1.clone()])],
                wal: None,
                torn_wal: false,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_stale_roll_live_records",
                pink: Some(vec![0, 1]),
                tmp: Some(vec![0, 1, 2]),
                rolls: vec![(1, vec![i1.clone()])],
                wal: Some((2, vec![i2.clone()])),
                torn_wal: false,
                expect: Rows(vec![0, 1, 2]),
            },
            Image {
                name: "tmp_roll_torn_live",
                pink: None,
                tmp: Some(vec![0, 1]),
                rolls: vec![(1, vec![cr.clone(), i0.clone(), i1.clone()])],
                wal: None,
                torn_wal: true,
                expect: Rows(vec![0, 1]),
            },
            Image {
                name: "tmp_torn_live_no_roll",
                pink: None,
                tmp: Some(vec![0]),
                rolls: vec![],
                wal: None,
                torn_wal: true,
                expect: Rows(vec![]),
            },
            Image {
                name: "torn_live",
                pink: None,
                tmp: None,
                rolls: vec![],
                wal: None,
                torn_wal: true,
                expect: Rows(vec![]),
            },
            Image {
                name: "torn_live_roll_pink",
                pink: Some(vec![0]),
                tmp: None,
                rolls: vec![(1, vec![cr.clone(), i0.clone()])],
                wal: None,
                torn_wal: true,
                expect: Rows(vec![0]),
            },
            Image {
                name: "tmp_pink_torn_live",
                pink: Some(vec![0]),
                tmp: Some(vec![0]),
                rolls: vec![],
                wal: None,
                torn_wal: true,
                expect: Rows(vec![0]),
            },
            Image {
                name: "tmp_roll_stale_high_live",
                pink: Some(vec![0, 1]),
                tmp: Some(vec![0, 1, 2]),
                rolls: vec![(1, vec![i1.clone()])],
                wal: Some((3, vec![i2.clone()])),
                torn_wal: false,
                expect: Rows(vec![0, 1, 2]),
            },
            Image {
                name: "tmp_roll_not_older",
                pink: Some(vec![0]),
                tmp: Some(vec![0, 1]),
                rolls: vec![(2, vec![i1.clone()])],
                wal: Some((2, vec![])),
                torn_wal: false,
                expect: Error("does not precede"),
            },
            Image {
                name: "roll_generation_equal_live",
                pink: Some(vec![0, 1]),
                tmp: None,
                rolls: vec![(1, vec![i1.clone()])],
                wal: Some((1, vec![i2.clone()])),
                torn_wal: false,
                expect: Error("does not precede"),
            },
        ];

        for img in &cases {
            let p = tmp_ckpt(img.name);
            let wal = wal_path(&p);
            if let Some(rows) = &img.pink {
                write_pink(&p, rows);
            }
            if let Some(rows) = &img.tmp {
                write_pink(&tmp_path(&p), rows);
            }
            for (generation, recs) in &img.rolls {
                write_wal_lsns(&roll_path(&wal, *generation), *generation, &raw(recs));
            }
            if let Some((generation, recs)) = &img.wal {
                write_wal_lsns(&wal, *generation, &raw(recs));
            } else if img.torn_wal {
                std::fs::write(&wal, b"PIN").unwrap();
            }

            match &img.expect {
                Expect::Rows(want) => {
                    // Open #1: recovery.
                    let (mut db, _) =
                        PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
                            .unwrap_or_else(|e| panic!("{}: first open failed: {e}", img.name));
                    assert_eq!(matrix_rows(&mut db), *want, "{}: first open", img.name);
                    let live_has = img
                        .wal
                        .as_ref()
                        .map(|(_, r)| !r.is_empty())
                        .unwrap_or(false);
                    let published = img.tmp.is_some() && !live_has && !img.rolls.is_empty();
                    assert_eq!(
                        p.exists(),
                        img.pink.is_some() || published,
                        "{}: checkpoint presence after recovery",
                        img.name
                    );
                    drop(db);

                    // Open #2 and #3: idempotent, same rows.
                    for open in 2..=3 {
                        let (mut again, info) =
                            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
                                .unwrap_or_else(|e| {
                                    panic!("{}: open #{open} failed: {e}", img.name)
                                });
                        assert_eq!(
                            matrix_rows(&mut again),
                            *want,
                            "{}: open #{open} rows changed (not idempotent)",
                            img.name
                        );
                        assert_eq!(
                            info.truncated_bytes, 0,
                            "{}: open #{open} must not truncate a recovered WAL",
                            img.name
                        );
                    }
                    assert!(!tmp_path(&p).exists(), "{}: .tmp left behind", img.name);
                    assert!(
                        scan_rolls(&wal).unwrap().is_empty(),
                        "{}: rolls left behind",
                        img.name
                    );
                }
                Expect::Error(needle) => {
                    match PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync) {
                        Ok(_) => panic!("{}: open should have failed", img.name),
                        Err(e) => assert!(
                            e.to_string().contains(needle),
                            "{}: expected {needle:?} in {e}",
                            img.name
                        ),
                    }
                }
            }
            cleanup(&p);
        }
    }

    /// Rows of table `t`, or empty when the database has no tables at all
    /// (the matrix includes states where nothing was ever committed).
    fn matrix_rows(db: &mut PinkDb) -> Vec<i64> {
        match db.execute("SELECT a FROM t ORDER BY a;") {
            Ok(mut out) => match out.pop().unwrap() {
                QueryResult::Selected { rows, .. } => rows
                    .into_iter()
                    .map(|r| match &r[0] {
                        Value::Integer(i) => *i as i64,
                        Value::Bigint(i) => *i,
                        other => panic!("expected an integer, got {other:?}"),
                    })
                    .collect(),
                other => panic!("expected rows, got {other:?}"),
            },
            Err(e) => {
                if db.list_tables().is_empty() {
                    Vec::new()
                } else {
                    panic!("select failed: {e}")
                }
            }
        }
    }

    #[test]
    fn checkpoint_needs_durability_and_a_settled_transaction() {
        let p = tmp_ckpt("ckptguard");
        let mut mem = PinkDb::new();
        assert!(mem.checkpoint().is_err(), "in-memory has no bound path");
        mem.flush_wal().unwrap(); // no-op
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        assert!(
            db.checkpoint().is_err(),
            "open transaction refuses checkpoint"
        );
        db.execute("COMMIT;").unwrap();
        db.checkpoint().unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    /// C1/A: a connection whose session database was dropped by another
    /// connection must not log a `USE` prefix for it. The fully qualified
    /// write is self-contained, so the record replays (before the fix the
    /// record began `USE "db1"` and every reopen hard-errored).
    #[test]
    fn dropped_session_database_does_not_brick_replay() {
        let p = tmp_ckpt("dropsession");
        let (db, _) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        let db = crate::Database::from_store(db);
        let mut a = db.connect();
        a.execute("CREATE DATABASE db1;").unwrap();
        a.execute("CREATE TABLE main.t (x INTEGER);").unwrap();
        a.execute("USE db1;").unwrap();
        let mut b = db.connect();
        b.execute("DROP DATABASE db1;").unwrap();
        // a's session is still db1 (it was dropped by b): the append must
        // drop the dead prefix, not poison the store.
        a.execute("INSERT INTO main.t VALUES (1);").unwrap();
        let recs = records_of(&wal_path(&p));
        let last = recs.last().expect("the insert was logged");
        assert!(
            !last
                .stmts
                .iter()
                .any(|s| s.to_ascii_uppercase().contains("USE")),
            "no dead USE prefix may be logged: {last:?}"
        );
        drop(a);
        drop(b);
        drop(db);

        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT x FROM main.t ORDER BY x;"), vec![1]);
        // The recovered session tracking is still usable: another append
        // replays too.
        back.execute("INSERT INTO main.t VALUES (2);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(
            ints(&mut again, "SELECT x FROM main.t ORDER BY x;"),
            vec![1, 2]
        );
        cleanup(&p);
    }

    /// The same append decision, single-threaded: a session database that is
    /// already gone when a fully qualified write appends (the autocommit
    /// race's end state) must produce a replayable record.
    #[test]
    fn append_with_a_vanished_session_database_is_prefixless() {
        let p = tmp_ckpt("deadprefix");
        let mut db = durable(&p);
        db.execute("CREATE TABLE main.t (a INTEGER);").unwrap();
        db.execute("CREATE DATABASE db1;").unwrap();
        db.execute("DROP DATABASE db1;").unwrap();
        db.set_current_db("db1");
        db.execute("INSERT INTO main.t VALUES (1);").unwrap();
        let recs = records_of(&wal_path(&p));
        assert!(
            recs.iter().all(|r| !r
                .stmts
                .iter()
                .any(|s| s.to_ascii_uppercase().contains("USE"))),
            "record for a vanished session must not carry a USE prefix: {recs:?}"
        );
        drop(db);

        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM main.t ORDER BY a;"), vec![1]);
        cleanup(&p);
    }

    /// C1/B: a transaction that created a table in a database dropped before
    /// COMMIT must fail the commit as a conflict — never log `CREATE TABLE
    /// db1.t` (which can never replay) or silently drop the table.
    #[test]
    fn dropped_database_before_commit_conflicts() {
        let p = tmp_ckpt("droptxn");
        let (db, _) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        let db = crate::Database::from_store(db);
        let mut a = db.connect();
        a.execute("CREATE DATABASE db1;").unwrap();
        let mut b = db.connect();
        a.execute("BEGIN;").unwrap();
        a.execute("CREATE TABLE db1.t (x INTEGER);").unwrap();
        b.execute("DROP DATABASE db1;").unwrap();
        let err = a.execute("COMMIT;").unwrap_err();
        assert!(err.to_string().contains("conflict"), "{err}");
        assert!(
            err.to_string().contains("db1"),
            "the conflict must name the vanished database: {err}"
        );
        drop(a);
        drop(b);
        drop(db);

        // The aborted commit logged nothing: the store reopens cleanly.
        let mut back = durable(&p);
        assert!(back.execute("SELECT * FROM db1.t;").is_err());
        back.execute("CREATE TABLE main.t (a INTEGER);").unwrap();
        back.execute("INSERT INTO main.t VALUES (1);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(ints(&mut again, "SELECT * FROM main.t;"), vec![1]);
        cleanup(&p);
    }

    /// C1/C: `BEGIN; USE db1;` then another connection drops db1: COMMIT must
    /// conflict (the record's `USE db1` could never replay).
    #[test]
    fn dropped_use_target_before_commit_conflicts() {
        let p = tmp_ckpt("dropuse");
        let (db, _) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        let db = crate::Database::from_store(db);
        let mut a = db.connect();
        a.execute("CREATE DATABASE db1;").unwrap();
        let mut b = db.connect();
        a.execute("BEGIN;").unwrap();
        a.execute("USE db1;").unwrap();
        b.execute("DROP DATABASE db1;").unwrap();
        let err = a.execute("COMMIT;").unwrap_err();
        assert!(err.to_string().contains("conflict"), "{err}");
        drop(a);
        drop(b);
        drop(db);

        let mut back = durable(&p);
        back.execute("CREATE TABLE t (a INTEGER);").unwrap();
        back.execute("INSERT INTO t VALUES (1);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(ints(&mut again, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    /// A durable store without `main`: drop it, checkpoint, append, reopen —
    /// the session falls back to the remaining database and both the
    /// checkpoint and the post-checkpoint record replay there.
    #[test]
    fn durable_flow_without_main_reopens_and_appends() {
        let p = tmp_ckpt("nomain");
        {
            let mut db = durable(&p);
            db.execute("CREATE DATABASE db1;").unwrap();
            db.execute("CREATE TABLE db1.t (a INTEGER);").unwrap();
            db.execute("INSERT INTO db1.t VALUES (1);").unwrap();
            db.execute("DROP DATABASE main;").unwrap();
            db.checkpoint().unwrap();
            // The checkpoint has no `main`; the session fell back to db1 and
            // this append must replay in db1.
            db.execute("INSERT INTO db1.t VALUES (2);").unwrap();
        }
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(back.list_databases(), vec!["db1".to_string()]);
        assert_eq!(back.current_db(), "db1");
        assert_eq!(info.replayed_records, 1, "only the post-checkpoint insert");
        assert_eq!(
            ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"),
            vec![1, 2]
        );
        // Appends after the reopen keep replaying in the no-main store.
        back.execute("INSERT INTO db1.t VALUES (3);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(
            ints(&mut again, "SELECT a FROM db1.t ORDER BY a;"),
            vec![1, 2, 3]
        );
        cleanup(&p);
    }

    #[test]
    fn two_connections_commit_in_order_and_both_survive_reopen() {
        let p = tmp_ckpt("multi");
        let (db, _) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        let db = crate::Database::from_store(db);
        db.connect().execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut a = db.connect();
        let mut b = db.connect();
        // b snapshots t, then a autocommits into t while b is open: b's
        // COMMIT must fail the version check (by design — no silent lost
        // updates). The intended usage is to retry the transaction.
        b.execute("BEGIN;").unwrap();
        b.execute("INSERT INTO t VALUES (2);").unwrap();
        a.execute("INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(
            ints_conn(&mut db.connect(), "SELECT COUNT(*) FROM t;"),
            vec![1]
        );
        assert!(
            b.execute("COMMIT;").is_err(),
            "stale transaction must conflict"
        );
        // Retry against the new state; this commit takes.
        b.execute("BEGIN;").unwrap();
        b.execute("INSERT INTO t VALUES (2);").unwrap();
        b.execute("COMMIT;").unwrap();
        drop(a);
        drop(b);
        drop(db);

        // Both connections' committed rows survive reopen: a's autocommit
        // record and b's retried transaction record are replayed in order.
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }

    #[test]
    fn write_conflict_abort_is_not_logged() {
        let p = tmp_ckpt("conflict");
        let (db, _) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        let db = crate::Database::from_store(db);
        db.connect().execute("CREATE TABLE t (a INTEGER);").unwrap();
        // Winner commits first; the loser aborts on the version check.
        let mut winner = db.connect();
        winner.execute("BEGIN;").unwrap();
        winner.execute("INSERT INTO t VALUES (20);").unwrap();
        let mut loser = db.connect();
        loser.execute("BEGIN;").unwrap();
        loser.execute("INSERT INTO t VALUES (10);").unwrap();
        winner.execute("COMMIT;").unwrap();
        assert!(loser.execute("COMMIT;").is_err());
        assert_eq!(
            records_of(&wal_path(&p)).len(),
            2,
            "create + the winning transaction only"
        );
        drop(winner);
        drop(loser);
        drop(db);

        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![20]);
        cleanup(&p);
    }

    #[test]
    fn batch_policy_commits_and_flushes() {
        let p = tmp_ckpt("batch");
        let (mut db, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Batch).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.flush_wal().unwrap();
        db.execute("BEGIN;").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        db.execute("COMMIT;").unwrap();
        drop(db);
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        back.checkpoint().unwrap();
        cleanup(&p);
    }

    #[test]
    fn plain_open_ignores_the_wal_and_memory_stays_clean() {
        let p = tmp_ckpt("plain");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            db.checkpoint().unwrap();
            db.execute("INSERT INTO t VALUES (2);").unwrap(); // WAL only
        }
        let mut plain = PinkDb::open(p.to_str().unwrap()).unwrap();
        assert_eq!(ints(&mut plain, "SELECT a FROM t;"), vec![1]);
        let mut durable_db = durable(&p);
        assert_eq!(ints(&mut durable_db, "SELECT a FROM t;"), vec![1, 2]);
        cleanup(&p);

        // An in-memory database never creates sidecar files.
        let q = tmp_ckpt("memoryonly");
        let mut mem = PinkDb::new();
        assert!(!mem.is_durable());
        mem.execute("CREATE TABLE m (a INTEGER);").unwrap();
        mem.save(q.to_str().unwrap()).unwrap();
        assert!(!wal_path(&q).exists());
        let mut loaded = PinkDb::open(q.to_str().unwrap()).unwrap();
        assert_eq!(ints(&mut loaded, "SELECT a FROM m;").len(), 0);
        cleanup(&q);
    }

    #[test]
    fn durable_flag_is_shared_across_clones() {
        let p = tmp_ckpt("clones");
        let db = durable(&p);
        let clone = db.clone();
        assert!(clone.is_durable());
        drop(clone);
        drop(db);
        cleanup(&p);
    }

    /// Durable commits refuse statements whose bound plan reads a
    /// session-local view: views are never persisted, so the logged SQL could
    /// never be replayed (recovery would hard-error "table not found").
    #[test]
    fn durable_dml_cannot_depend_on_a_session_view() {
        let p = tmp_ckpt("viewdep");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        // Read-only use of the view is fine: it is never logged.
        assert_eq!(ints(&mut db, "SELECT a FROM v;"), vec![1]);
        for sql in [
            "UPDATE t SET a = 2 WHERE a IN (SELECT a FROM v);",
            "DELETE FROM t WHERE a IN (SELECT a FROM v);",
            "INSERT INTO t VALUES ((SELECT max(a) FROM v));",
        ] {
            let err = db.execute(sql).unwrap_err();
            assert!(
                err.to_string().contains("session-local view \"v\""),
                "{sql}: {err}"
            );
        }
        // Refused statements never reach memory or the WAL.
        let recs = records_of(&wal_path(&p));
        assert_eq!(recs.len(), 2, "only CREATE TABLE + INSERT are logged");
        assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
        // Same inside a durable transaction (nothing is buffered for COMMIT).
        db.execute("BEGIN;").unwrap();
        assert!(
            db.execute("UPDATE t SET a = 3 WHERE a IN (SELECT a FROM v);")
                .is_err()
        );
        db.execute("ROLLBACK;").unwrap();

        // In-memory databases keep the old behavior.
        let mut mem = PinkDb::new();
        mem.execute("CREATE TABLE t (a INTEGER);").unwrap();
        mem.execute("INSERT INTO t VALUES (1);").unwrap();
        mem.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        mem.execute("UPDATE t SET a = 2 WHERE a IN (SELECT a FROM v);")
            .unwrap();
        assert_eq!(ints(&mut mem, "SELECT a FROM t;"), vec![2]);
        cleanup(&p);
    }

    #[test]
    fn use_and_create_database_replay_reproduces_state_exactly() {
        let p = tmp_ckpt("usedb");
        {
            let mut db = durable(&p);
            db.execute(
                "CREATE DATABASE db1; \
                 CREATE TABLE main.t (a INTEGER); \
                 CREATE TABLE db1.t (a INTEGER);",
            )
            .unwrap();
            // A mixed sequence: USE switches the replay session; every
            // statement is logged and must land in the right database.
            db.execute(
                "USE db1; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); \
                 USE main; INSERT INTO t VALUES (10);",
            )
            .unwrap();
            db.execute("USE db1; INSERT INTO t VALUES (3);").unwrap();
            // A transaction whose statements switch databases.
            db.execute("BEGIN; USE main; INSERT INTO t VALUES (20); USE db1; INSERT INTO t VALUES (4); COMMIT;")
                .unwrap();
            // Create + drop a database in the middle of the log.
            db.execute("CREATE DATABASE db2; CREATE TABLE db2.t (a INTEGER); INSERT INTO db2.t VALUES (99);")
                .unwrap();
            db.execute("DROP DATABASE db2;").unwrap();
            db.execute("USE main;").unwrap();
        }
        // The USE statements are in the WAL text (replay relies on them).
        let records = records_of(&wal_path(&p));
        assert!(
            records
                .iter()
                .any(|r| r.stmts.iter().any(|s| s.starts_with("USE "))),
            "USE must be logged: {records:?}"
        );
        assert!(
            records
                .iter()
                .any(|r| r.stmts.iter().any(|s| s.contains("CREATE DATABASE"))),
            "CREATE DATABASE must be logged"
        );

        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert!(info.replayed_records > 0);
        assert_eq!(
            ints(&mut back, "SELECT a FROM main.t ORDER BY a;"),
            vec![10, 20]
        );
        assert_eq!(
            ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"),
            vec![1, 2, 3, 4]
        );
        assert!(!back.database_exists("db2"), "db2 was dropped");
        assert_eq!(back.current_db(), "main");
        // Appends after recovery continue in the recovered session.
        back.execute("INSERT INTO main.t VALUES (11);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(
            ints(&mut again, "SELECT a FROM main.t ORDER BY a;"),
            vec![10, 11, 20]
        );
        assert_eq!(
            ints(&mut again, "SELECT a FROM db1.t ORDER BY a;"),
            vec![1, 2, 3, 4]
        );
    }

    /// A checkpoint publishes state and rolls the WAL: replay of the fresh
    /// generation starts at the checkpoint's session default (the session is
    /// not persisted), so appends after a checkpoint must `USE`-prefix again.
    #[test]
    fn checkpoint_resets_the_replay_session_to_the_checkpoint_default() {
        let p = tmp_ckpt("ckptsession");
        let mut db = durable(&p);
        db.execute("CREATE DATABASE db1; CREATE TABLE db1.t (a INTEGER);")
            .unwrap();
        db.execute("USE db1;").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.checkpoint().unwrap();
        // The session is still db1 in memory, but the fresh WAL replays from
        // main: this insert must land in db1 after a reopen.
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        drop(db);
        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"),
            vec![1, 2]
        );
        // Replay ends where the log left the session: the post-checkpoint
        // insert was `USE`-prefixed, so the reopened session is db1.
        assert_eq!(back.current_db(), "db1");
    }

    /// Databases created with quoted identifiers (spaces, keywords) must
    /// survive a checkpoint + `USE`-prefixed replay: the prefix quotes the
    /// name, so `USE "My Db"` parses on replay.
    #[test]
    fn quoted_database_names_replay_across_a_checkpoint() {
        let p = tmp_ckpt("quoteddb");
        let mut db = durable(&p);
        db.execute("CREATE DATABASE \"My Db\";").unwrap();
        db.execute("CREATE TABLE \"My Db\".t (a INTEGER);").unwrap();
        db.execute("USE \"My Db\";").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        drop(db);
        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT a FROM \"My Db\".t ORDER BY a;"),
            vec![1, 2]
        );
    }

    /// Two connections on different session databases share one WAL: every
    /// record is self-contained (a `USE` prefix is emitted when the replay
    /// session differs), so replay reproduces both databases exactly.
    #[test]
    fn multi_connection_sessions_replay_each_record_in_its_database() {
        let p = tmp_ckpt("multidb");
        {
            let shared = crate::Database::open(p.to_str().unwrap()).unwrap();
            let mut a = shared.connect();
            let mut b = shared.connect();
            a.execute("CREATE DATABASE db1; CREATE TABLE main.t (a INTEGER);")
                .unwrap();
            a.execute("CREATE TABLE db1.t (a INTEGER);").unwrap();
            a.execute("USE db1;").unwrap();
            a.execute("INSERT INTO t VALUES (1);").unwrap();
            // b never issued USE: its statements belong to main.
            b.execute("INSERT INTO t VALUES (2);").unwrap();
            a.execute("INSERT INTO t VALUES (3);").unwrap();
            b.execute("USE db1; INSERT INTO t VALUES (4);").unwrap();
            b.execute("USE main; INSERT INTO t VALUES (5);").unwrap();
            assert_eq!(a.current_db(), "db1");
            assert_eq!(b.current_db(), "main");
        }
        let (mut back, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(
            ints(&mut back, "SELECT a FROM main.t ORDER BY a;"),
            vec![2, 5]
        );
        assert_eq!(
            ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"),
            vec![1, 3, 4]
        );
        assert_eq!(back.current_db(), "main");
    }

    #[test]
    fn save_snapshot_refuses_a_target_with_a_live_wal() {
        let a = tmp_ckpt("snapa");
        let b = tmp_ckpt("snapb");
        // B has committed rows that only exist in its WAL.
        {
            let mut bb = durable(&b);
            bb.execute("CREATE TABLE other (x INTEGER);").unwrap();
            bb.execute("INSERT INTO other VALUES (7);").unwrap();
        }
        assert!(wal_path(&b).exists());

        let mut aa = durable(&a);
        aa.execute("CREATE TABLE mine (x INTEGER);").unwrap();
        // Publish the bound checkpoint so path identity has a real file to
        // resolve (a durable database may have only a WAL before this).
        aa.checkpoint().unwrap();
        let err = aa.save_snapshot(b.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("committed WAL records"), "{err}");
        // B's checkpoint file is untouched and its WAL still replays.
        let mut back = durable(&b);
        assert_eq!(ints(&mut back, "SELECT x FROM other;"), vec![7]);
        drop(back);
        drop(aa);

        // Filesystem identity, not string equality: `./`, absolute paths and
        // case-only variants all resolve to the bound database.
        let aa = durable(&a);
        assert!(aa.is_bound_to(a.to_str().unwrap()));
        let dotted = format!(
            "{}/./{}",
            a.parent().unwrap().display(),
            a.file_name().unwrap().to_str().unwrap()
        );
        assert!(aa.is_bound_to(&dotted), "{dotted}");
        let abs = std::fs::canonicalize(&a).unwrap();
        assert!(aa.is_bound_to(abs.to_str().unwrap()));
        if case_insensitive(a.parent().unwrap()) {
            let upper = a
                .parent()
                .unwrap()
                .join(a.file_name().unwrap().to_str().unwrap().to_uppercase());
            assert!(
                aa.is_bound_to(upper.to_str().unwrap()),
                "case-only variant must be the same file"
            );
        }
        assert!(!aa.is_bound_to(b.to_str().unwrap()));
        drop(aa);

        // A sibling WAL with no committed records is harmless: the snapshot
        // is allowed (its next open replays nothing).
        let c = tmp_ckpt("snapc");
        {
            let _c = durable(&c);
        }
        let aa = durable(&a);
        aa.save_snapshot(c.to_str().unwrap()).unwrap();
        drop(aa);
        let mut back = durable(&c);
        assert!(back.table("mine").is_ok(), "snapshot carried the table");
        assert_eq!(ints(&mut back, "SELECT x FROM mine;").len(), 0);
        drop(back);
        cleanup(&a);
        cleanup(&b);
        cleanup(&c);
    }

    /// Does `dir` treat `Aa` and `AA` as the same entry? Determines whether
    /// case-only path identity can be exercised (APFS/NTFS yes, ext4 no).
    fn case_insensitive(dir: &Path) -> bool {
        let probe = dir.join(format!("pinkdb_case_probe_{}", std::process::id()));
        let upper = dir.join(format!("PINKDB_CASE_PROBE_{}", std::process::id()));
        if std::fs::write(&probe, b"x").is_err() {
            return false;
        }
        let same = upper.exists();
        let _ = std::fs::remove_file(&probe);
        same
    }

    /// A keyed store in a crypto-free build must refuse to write plaintext,
    /// on both save paths.
    #[cfg(not(feature = "encryption"))]
    #[test]
    fn keyed_writes_without_the_feature_are_refused() {
        let p = tmp_ckpt("keyedwrite");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.set_encryption_key(Some(EncryptionKey::from_passphrase("sesame")));
        let err = db.checkpoint().unwrap_err();
        assert!(err.to_string().contains("not compiled in"), "{err}");
        assert!(!p.exists(), "no plaintext checkpoint may appear");
        let snap = p.parent().unwrap().join("snap.pink");
        let err = db.save_snapshot(snap.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("not compiled in"), "{err}");
        assert!(!snap.exists(), "no plaintext snapshot may appear");
        cleanup(&p);
    }

    /// m7 regression: a crafted roll name at the top of the `u64` range must
    /// produce a clean error, never an overflow panic, when recovery has to
    /// continue the generation sequence.
    #[test]
    fn max_roll_generation_overflow_is_a_clean_error() {
        let p = tmp_ckpt("genoverflow");
        write_pink(&p, &[0]);
        // A roll whose name and header agree on u64::MAX.
        write_wal_lsns(
            &roll_path(&wal_path(&p), u64::MAX),
            u64::MAX,
            &raw(&[vec!["INSERT INTO t VALUES (1);".to_string()]]),
        );
        // The live WAL is absent, so recovery must recreate it *after* the
        // roll and cannot.
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("highest representable"), "{err}");

        // The helper itself refuses the wrap (it backs both call sites).
        let roll = Roll {
            generation: u64::MAX,
            path: PathBuf::from("x.pink.wal.18446744073709551615.roll"),
        };
        let err = next_gen_after_rolls(std::slice::from_ref(&roll)).unwrap_err();
        assert!(err.to_string().contains("highest representable"), "{err}");
        assert_eq!(next_gen_after_rolls(&[]).unwrap(), 1);
        let roll = Roll {
            generation: 7,
            path: PathBuf::from("x"),
        };
        assert_eq!(
            next_gen_after_rolls(std::slice::from_ref(&roll)).unwrap(),
            8
        );
        cleanup(&p);
    }

    /// m5: a snapshot-only encrypted `.pink` (no sidecar WAL) opened through
    /// the durable path in a crypto-free build must name the missing feature
    /// with the standard wording, not the recovery-mode one.
    #[cfg(not(feature = "encryption"))]
    #[test]
    fn durable_open_of_a_snapshot_only_encrypted_pink_names_the_feature() {
        let p = tmp_ckpt("encsnapshotoff");
        let mut bytes = crypto::ENC_MAGIC.to_vec();
        bytes.extend_from_slice(&[0u8; 64]);
        std::fs::write(&p, &bytes).unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string()
                .contains("file is encrypted; build with the encryption feature"),
            "{err}"
        );
        cleanup(&p);
    }

    // ---------- encrypted WAL (feature `encryption`) ----------

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;

        fn key() -> EncryptionKey {
            EncryptionKey::from_raw([0x33u8; 32])
        }

        fn durable_keyed(p: &Path) -> PinkDb {
            PinkDb::open_durable_with_key(p.to_str().unwrap(), &key()).unwrap()
        }

        #[test]
        fn records_are_encrypted_and_replay_exactly_once() {
            let p = tmp_ckpt("encrecords");
            let mut db = durable_keyed(&p);
            db.execute("CREATE TABLE t (a INTEGER, v VARCHAR);")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'marker-plaintext');")
                .unwrap();
            let wal = std::fs::read(wal_path(&p)).unwrap();
            assert!(wal.starts_with(crypto::ENC_MAGIC));
            assert!(!wal.windows(8).any(|w| w == b"marker-p"));
            drop(db);

            let mut back = durable_keyed(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
            // The next append continues the ordinal nonce counter and replays.
            back.execute("INSERT INTO t VALUES (2, 'x');").unwrap();
            drop(back);
            let mut again = durable_keyed(&p);
            assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
            cleanup(&p);
        }

        /// Wave 45m: two keyed opens refresh each other, including across a
        /// checkpoint (the full-rebuild path with an encrypted WAL: the
        /// counter must resync past the records of the new incarnation).
        #[test]
        fn keyed_two_opens_refresh_across_a_checkpoint() {
            let p = tmp_ckpt("enctwoopens");
            let mut a = durable_keyed(&p);
            a.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let mut b = durable_keyed(&p);
            a.execute("INSERT INTO t VALUES (1);").unwrap();
            assert_eq!(ints(&mut b, "SELECT a FROM t;"), vec![1]);
            // A checkpoint by `a` replaces both files; `b`'s next statement
            // reloads checkpoint + fresh WAL.
            a.execute("INSERT INTO t VALUES (2); checkpoint;").unwrap();
            assert_eq!(ints(&mut b, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
            // `b` appends after the reload: the counter must continue past
            // the records of the new generation (no nonce reuse), and `a`
            // must see it after its own refresh.
            b.execute("INSERT INTO t VALUES (3);").unwrap();
            assert_eq!(ints(&mut a, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
            drop(a);
            drop(b);
            let mut back = durable_keyed(&p);
            assert_eq!(
                ints(&mut back, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3]
            );
            cleanup(&p);
        }

        #[test]
        fn wrong_key_fails_before_anything_replays() {
            let p = tmp_ckpt("encwrongkey");
            let mut db = durable_keyed(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            drop(db);
            let err = PinkDb::open_durable_with_key(
                p.to_str().unwrap(),
                &EncryptionKey::from_raw([0x44u8; 32]),
            )
            .unwrap_err();
            assert!(err.to_string().contains("wrong key"), "{err}");
            cleanup(&p);
        }

        /// L-5: the static publish-block identity is authenticated. Flipping
        /// `wal_id` must be rejected on recovery, not silently accepted as a
        /// different incarnation.
        #[test]
        fn tampered_publish_block_identity_is_rejected_on_recovery() {
            let p = tmp_ckpt("encpubmeta");
            {
                let mut db = durable_keyed(&p);
                db.execute("CREATE TABLE t (a INTEGER);").unwrap();
                db.execute("INSERT INTO t VALUES (1);").unwrap();
            }
            let wal = std::fs::read(wal_path(&p)).unwrap();
            let payload_start = crypto::parse_container_header(&wal)
                .expect("container header")
                .payload_start;
            let mut tampered = wal.clone();
            tampered[payload_start] ^= 0x01; // wal_id[0]
            std::fs::write(wal_path(&p), &tampered).unwrap();
            let err = PinkDb::open_durable_with_key(p.to_str().unwrap(), &key())
                .expect_err("a tampered publish-block identity must be refused")
                .to_string();
            assert!(err.contains("publish block authentication failed"), "{err}");
            cleanup(&p);
        }

        #[test]
        fn encrypted_torn_header_is_recreated_encrypted() {
            let p = tmp_ckpt("enctorn");
            std::fs::write(wal_path(&p), b"PINKENC1\x01\x00\x00\x00").unwrap();
            let (db, info) = PinkDb::open_durable_with_key_and_policy(
                p.to_str().unwrap(),
                SyncPolicy::Sync,
                &key(),
            )
            .unwrap();
            let wal = std::fs::read(wal_path(&p)).unwrap();
            assert!(wal.starts_with(crypto::ENC_MAGIC), "recreated encrypted");
            // Raw-key v4 container header (88 bytes) + the authenticated
            // 144-byte publish block.
            assert_eq!(
                wal.len(),
                88 + ENC_PUBLISH_BLOCK_LEN,
                "recreated as container v4"
            );
            assert_eq!(info.truncated_bytes, 0);
            drop(db);
            cleanup(&p);
        }

        /// Wave-22 C1 on an encrypted database: the crash image after the WAL
        /// roll but before the publish must be completed by recovery, the
        /// recovered checkpoint must be durable, and a second open must see
        /// the rows.
        #[test]
        fn encrypted_interrupted_checkpoint_is_published_before_rolls_are_removed() {
            let p = tmp_ckpt("encckptcrash");
            {
                let mut db = durable_keyed(&p);
                db.execute("CREATE TABLE t (a INTEGER);").unwrap();
                db.execute("INSERT INTO t VALUES (1);").unwrap();
                db.save_sync_with_key(&tmp_path(&p), &key()).unwrap();
                std::fs::rename(wal_path(&p), roll_path(&wal_path(&p), 1)).unwrap();
                let cipher = RecordCipher::create(&key(), 2).unwrap();
                Wal::create_file(&wal_path(&p), 2, Some(cipher)).unwrap();
            }
            let (mut back, info) = PinkDb::open_durable_with_key_and_policy(
                p.to_str().unwrap(),
                SyncPolicy::Sync,
                &key(),
            )
            .unwrap();
            assert_eq!(info.checkpoints_completed, 1, "recovery must publish .tmp");
            assert_eq!(info.replayed_records, 0);
            assert_eq!(info.stale_rolls_removed, 1);
            assert!(std::fs::read(&p).unwrap().starts_with(crypto::ENC_MAGIC));
            assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
            drop(back);

            // The second clean open must not lose the recovered rows, and the
            // live WAL (gen 2) must still be encrypted.
            let (mut again, info) = PinkDb::open_durable_with_key_and_policy(
                p.to_str().unwrap(),
                SyncPolicy::Sync,
                &key(),
            )
            .unwrap();
            assert_eq!(info.replayed_records, 0);
            assert_eq!(ints(&mut again, "SELECT a FROM t;"), vec![1]);
            assert!(
                std::fs::read(wal_path(&p))
                    .unwrap()
                    .starts_with(crypto::ENC_MAGIC)
            );
            assert!(!tmp_path(&p).exists());
            assert!(scan_rolls(&wal_path(&p)).unwrap().is_empty());
            drop(again);
            cleanup(&p);
        }

        /// A checkpoint rolls into a fresh encrypted generation whose records
        /// still replay (forked cipher: same key, fresh nonce prefix).
        #[test]
        fn encrypted_checkpoint_rolls_into_a_fresh_encrypted_generation() {
            let p = tmp_ckpt("encroll");
            let mut db = durable_keyed(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            db.checkpoint().unwrap();
            let wal = std::fs::read(wal_path(&p)).unwrap();
            assert!(wal.starts_with(crypto::ENC_MAGIC));
            assert_eq!(
                wal.len(),
                88 + ENC_PUBLISH_BLOCK_LEN,
                "fresh generation is header + authenticated block"
            );
            db.execute("INSERT INTO t VALUES (2);").unwrap();
            drop(db);
            let mut back = durable_keyed(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
            cleanup(&p);
        }

        /// A truncated encrypted WAL is rewritten atomically with a fresh
        /// nonce prefix; a leftover `.rewrite` temp must never confuse roll
        /// scanning or recovery.
        #[test]
        fn encrypted_truncation_rewrites_and_ignores_rewrite_leftovers() {
            let p = tmp_ckpt("encrewrite");
            let mut db = durable_keyed(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            db.execute("INSERT INTO t VALUES (2);").unwrap();
            drop(db);
            let before = std::fs::read(wal_path(&p)).unwrap();
            std::fs::write(wal_path(&p), &before[..before.len() - 5]).unwrap();
            // A stale rewrite temp from a hypothetical crash: harmless.
            std::fs::write(rewrite_path(&wal_path(&p)), b"stale").unwrap();
            let (mut back, info) = PinkDb::open_durable_with_key_and_policy(
                p.to_str().unwrap(),
                SyncPolicy::Sync,
                &key(),
            )
            .unwrap();
            assert!(info.truncated_bytes > 0);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
            let after = std::fs::read(wal_path(&p)).unwrap();
            assert!(after.starts_with(crypto::ENC_MAGIC));
            assert_ne!(&after[22..30], &before[22..30], "fresh nonce prefix");
            back.execute("INSERT INTO t VALUES (3);").unwrap();
            drop(back);
            let mut again = durable_keyed(&p);
            assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![1, 3]);
            cleanup(&p);
        }

        /// A plaintext `.tmp` meeting a keyed open is refused before anything
        /// is published or deleted.
        #[test]
        fn plaintext_tmp_meeting_a_keyed_open_is_refused() {
            let p = tmp_ckpt("encmode");
            write_pink(&tmp_path(&p), &[1]);
            let err = PinkDb::open_durable_with_key(p.to_str().unwrap(), &key()).unwrap_err();
            assert!(err.to_string().contains("not encrypted"), "{err}");
            assert!(tmp_path(&p).exists(), "nothing may be removed");
            cleanup(&p);
        }

        /// m5 (feature on): a snapshot-only encrypted `.pink` (no WAL) opened
        /// without a key fails with the standard wording, not the recovery one.
        #[test]
        fn keyless_open_of_a_snapshot_only_encrypted_pink() {
            let p = tmp_ckpt("encsnapshot");
            let mut db = PinkDb::new();
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.save_with_key(p.to_str().unwrap(), &key()).unwrap();
            let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("file is encrypted"), "{msg}");
            assert!(!msg.contains("cannot recover"), "{msg}");
            cleanup(&p);
        }

        /// M1 regression: switching the store's key mode (or its key) after a
        /// durable open must be refused at the checkpoint — a `.pink` in one
        /// mode beside a fresh WAL generation in the other is unopenable.
        #[test]
        fn changing_the_key_mode_or_key_on_a_live_store_is_refused() {
            // Plaintext open -> key set: refused, nothing is published.
            let p = tmp_ckpt("modeswitch1");
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            let wal_before = std::fs::read(wal_path(&p)).unwrap();
            db.set_encryption_key(Some(key()));
            let err = db.checkpoint().unwrap_err();
            assert!(
                err.to_string()
                    .contains("cannot change encryption mode on a live store"),
                "{err}"
            );
            assert!(!p.exists(), "no .pink may be published");
            assert!(!tmp_path(&p).exists() && !roll_path(&wal_path(&p), 1).exists());
            assert_eq!(
                std::fs::read(wal_path(&p)).unwrap(),
                wal_before,
                "the live WAL is untouched"
            );
            // Appends keep working in the WAL's own mode.
            db.execute("INSERT INTO t VALUES (2);").unwrap();
            drop(db);
            let mut back = durable(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);

            // Keyed open -> key cleared: refused too.
            let p = tmp_ckpt("modeswitch2");
            let mut db = durable_keyed(&p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1);").unwrap();
            db.set_encryption_key(None);
            let err = db.checkpoint().unwrap_err();
            assert!(
                err.to_string()
                    .contains("cannot change encryption mode on a live store"),
                "{err}"
            );
            assert!(!p.exists());
            // Same mode but a different key: also refused (the pair would be
            // sealed under two different keys).
            db.set_encryption_key(Some(EncryptionKey::from_raw([0x55u8; 32])));
            let err = db.checkpoint().unwrap_err();
            assert!(
                err.to_string()
                    .contains("cannot change the encryption key on a live store"),
                "{err}"
            );
            assert!(!p.exists());
            // Restoring the live key lets the checkpoint through, encrypted.
            db.set_encryption_key(Some(key()));
            db.checkpoint().unwrap();
            assert!(std::fs::read(&p).unwrap().starts_with(crypto::ENC_MAGIC));
            drop(db);
            let mut back = durable_keyed(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
            drop(back);
            cleanup(&p);
        }

        /// M3 regression: `save_snapshot` refuses a target whose `.pink`
        /// itself (not only its sidecar WAL) is in the other at-rest mode.
        #[test]
        fn snapshot_refuses_a_mode_mismatched_target_pink() {
            // Plaintext store over an encrypted target `.pink`.
            let p = tmp_ckpt("snapmode1");
            let mut plain = durable(&p);
            plain.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let target = p.parent().unwrap().join("enc-target.pink");
            let mut enc = PinkDb::new();
            enc.execute("CREATE TABLE u (b INTEGER);").unwrap();
            enc.execute("INSERT INTO u VALUES (7);").unwrap();
            enc.save_with_key(target.to_str().unwrap(), &key()).unwrap();
            let before = std::fs::read(&target).unwrap();
            let err = plain.save_snapshot(target.to_str().unwrap()).unwrap_err();
            assert!(err.to_string().contains("file is encrypted"), "{err}");
            assert_eq!(
                std::fs::read(&target).unwrap(),
                before,
                "the encrypted target is untouched"
            );
            drop(plain);

            // Keyed store over a plaintext target `.pink`.
            let p2 = tmp_ckpt("snapmode2");
            let mut mem = PinkDb::new();
            mem.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let pt = p2.parent().unwrap().join("plain-target.pink");
            mem.save(pt.to_str().unwrap()).unwrap();
            let before = std::fs::read(&pt).unwrap();
            let mut keyed = durable_keyed(&p2);
            keyed.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let err = keyed.save_snapshot(pt.to_str().unwrap()).unwrap_err();
            assert!(err.to_string().contains("not encrypted"), "{err}");
            assert_eq!(std::fs::read(&pt).unwrap(), before);
            drop(keyed);
            cleanup(&p);
            cleanup(&p2);
        }

        /// One wave-45n fuzz open with a key: must not panic; an open that
        /// succeeds sees exactly `1..=m` rows (a committed prefix), a second
        /// keyed open is idempotent, and a refused open is a clean error path.
        fn fuzz_open_prefix_keyed(p: &Path, lo: i64, hi: i64) {
            match PinkDb::open_durable_with_key(p.to_str().unwrap(), &key()) {
                Ok(mut db) => {
                    let rows = ints(&mut db, "SELECT a FROM t ORDER BY a;");
                    assert_eq!(
                        rows,
                        (1..=rows.len() as i64).collect::<Vec<_>>(),
                        "a fresh keyed open must recover a prefix of committed rows"
                    );
                    assert!(
                        rows.len() as i64 >= lo && rows.len() as i64 <= hi,
                        "recovered {} rows, expected [{lo}, {hi}]",
                        rows.len()
                    );
                    let mut again = durable_keyed(p);
                    assert_eq!(
                        ints(&mut again, "SELECT a FROM t ORDER BY a;"),
                        rows,
                        "a second keyed open must be idempotent"
                    );
                }
                Err(e) => {
                    let msg = e.to_string();
                    assert!(!msg.is_empty(), "a refused keyed open needs a message");
                    // The refused state must still be a clean error path.
                    let _ = PinkDb::open_durable_with_key(p.to_str().unwrap(), &key());
                }
            }
        }

        /// Wave 45n fuzz: the encrypted WAL (container header, `wal_id`
        /// publish block with its CRC and slots, AEAD records) truncated at
        /// every offset and flipped at every byte, through a keyed open: no
        /// panic, never a wrong value; a successful open recovers a committed
        /// prefix, a refused one errors cleanly without touching the
        /// checkpoint. The checkpoint (rows 1..=3) is intact throughout, so
        /// a successful open always has the first three rows.
        #[test]
        fn fuzz_encrypted_wal_truncation_and_byte_flips() {
            let p = tmp_ckpt("encfuzz");
            {
                let mut db = durable_keyed(&p);
                db.execute("CREATE TABLE t (a INTEGER);").unwrap();
                for i in 1..=3 {
                    db.execute(&format!("INSERT INTO t VALUES ({i});")).unwrap();
                }
                db.checkpoint().unwrap();
                db.execute("INSERT INTO t VALUES (4);").unwrap();
            }
            let wal = wal_path(&p);
            let pristine = std::fs::read(&wal).unwrap();
            assert!(pristine.starts_with(crypto::ENC_MAGIC));
            // Truncation at every offset (the file is bounded: one small
            // record in the fresh generation).
            for len in 0..=pristine.len() {
                std::fs::write(&wal, &pristine[..len]).unwrap();
                fuzz_open_prefix_keyed(&p, 3, 4);
            }
            // One-bit and byte flips at every offset: header, publish block
            // (wal_id/CRC/slots), record framing, ciphertext, tag.
            for off in 0..pristine.len() {
                for mask in [0x01u8, 0xFFu8] {
                    let mut corrupt = pristine.clone();
                    corrupt[off] ^= mask;
                    std::fs::write(&wal, &corrupt).unwrap();
                    fuzz_open_prefix_keyed(&p, 3, 4);
                }
            }
            // Restore and confirm every committed row is still there.
            std::fs::write(&wal, &pristine).unwrap();
            let mut back = durable_keyed(&p);
            assert_eq!(
                ints(&mut back, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4]
            );
            // The publish block is a hint: corrupting only its authenticated
            // slots must not block or misread anything (the static meta is
            // untouched). The block starts at the container's `payload_start`;
            // the slots are the last 88 bytes.
            let payload_start = crypto::parse_container_header(&pristine)
                .expect("container header")
                .payload_start;
            let mut slot_corrupt = pristine.clone();
            for b in slot_corrupt
                .iter_mut()
                .skip(payload_start + ENC_SLOT_A as usize)
                .take(2 * ENC_PUBLISH_SLOT_LEN)
            {
                *b ^= 0xA5;
            }
            std::fs::write(&wal, &slot_corrupt).unwrap();
            let mut hinted = durable_keyed(&p);
            assert_eq!(
                ints(&mut hinted, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4]
            );

            // A flip of the container version word (4 → 1) must not take the
            // legacy-upgrade path and silently drop the records that follow
            // the publish block: the key-check AAD binds the version, so the
            // keyed open is refused and the file is left untouched.
            std::fs::write(&wal, &pristine).unwrap();
            let mut v1 = pristine.clone();
            assert_eq!(u32::from_le_bytes(v1[8..12].try_into().unwrap()), 4);
            v1[8] = 1; // version 1: legacy container, records at payload_start
            std::fs::write(&wal, &v1).unwrap();
            let err = PinkDb::open_durable_with_key(p.to_str().unwrap(), &key()).unwrap_err();
            assert!(!err.to_string().is_empty(), "a version flip needs an error");
            assert_eq!(
                std::fs::read(&wal).unwrap(),
                v1,
                "the refused open must not rewrite the WAL (no silent drop)"
            );
            std::fs::write(&wal, &pristine).unwrap();
            let mut back = durable_keyed(&p);
            assert_eq!(
                ints(&mut back, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4]
            );
            cleanup(&p);
        }
    }

    // ---------- wave 45n: lock identity, fuzz, H5 replay ----------

    mod wave45n {
        use super::*;

        fn lock_path(p: &Path) -> PathBuf {
            PathBuf::from(format!("{}.lock", p.display()))
        }

        /// Replace the lock file the way a foreign process would: unlink the
        /// name and recreate it (a fresh inode). The running handle's fd keeps
        /// the old inode.
        fn replace_lock_file(p: &Path) {
            let lp = lock_path(p);
            std::fs::remove_file(&lp).unwrap();
            std::fs::write(&lp, b"").unwrap();
        }

        /// Wave 45n: a lock file replaced while a handle is idle must fail
        /// the next mutation closed — before any byte is written — and a
        /// fresh reopen must recover the exact durable prefix. Pure reads
        /// keep serving the last consistent view (statement snapshots; the
        /// replacement is detected at the next lease-taking operation).
        #[test]
        fn lock_file_replacement_fails_closed_before_any_mutation() {
            let p = tmp_ckpt("lockrep");
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
                .unwrap();
            let wal = wal_path(&p);
            let wal_before = std::fs::read(&wal).unwrap();
            replace_lock_file(&p);
            // Every lease-taking operation (write, refresh, checkpoint)
            // refuses with the same clear error.
            for sql in [
                "INSERT INTO t VALUES (2);",
                "INSERT INTO t VALUES (3);",
                "checkpoint;",
            ] {
                let err = db.execute(sql).unwrap_err().to_string();
                assert!(err.contains("deleted or replaced"), "{sql}: {err}");
            }
            // The first refused statement already fail-stopped the handle
            // (its optimistic effect could not be committed or undone), so
            // reads refuse too: the uncommitted state is never observable.
            let read_err = db.execute("SELECT a FROM t ORDER BY a;").unwrap_err();
            assert!(
                read_err
                    .to_string()
                    .contains("stopped after a WAL write failure"),
                "{read_err}"
            );
            assert_eq!(
                std::fs::read(&wal).unwrap(),
                wal_before,
                "the refused statements must not have written WAL bytes"
            );
            // The refused statements must not be observable after reopen.
            drop(db);
            let mut back = durable(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
        }

        /// Wave 45n/45o: replacement while a transaction is open (the
        /// transaction itself holds no lease now, but its COMMIT does):
        /// the COMMIT fails closed, the uncommitted row never becomes
        /// durable, and a fresh reopen is clean.
        #[test]
        fn lock_file_replacement_during_a_held_transaction_fails_closed() {
            let p = tmp_ckpt("lockreptxn");
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
                .unwrap();
            db.execute("BEGIN; INSERT INTO t VALUES (2);").unwrap();
            replace_lock_file(&p);
            let err = db.execute("COMMIT;").unwrap_err().to_string();
            assert!(err.contains("deleted or replaced"), "{err}");
            drop(db); // drops the still-open transaction (a lease failure keeps it)
            let mut back = durable(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
        }

        /// Wave 45n: a waiter blocked on a lease whose lock file is replaced
        /// reports the replacement immediately (the path-identity check runs
        /// on every wait iteration) instead of burning the whole busy
        /// timeout on a stray inode. Wave 45o: the waiter executes
        /// optimistically first, so on the refusal the handle fail-stops; the
        /// optimistic effect is never durable nor observable.
        #[test]
        fn lock_file_replacement_while_waiting_fails_closed_not_busy() {
            let p = tmp_ckpt("lockrepwait");
            let mut a = durable(&p);
            a.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let b = durable(&p);
            b.set_busy_timeout(Duration::from_millis(2_000));
            // A foreign fd (another "process") holds the flock: the waiter's
            // 0→1 lease transition must wait.
            let foreign = {
                let f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(format!("{}.lock", p.display()))
                    .unwrap();
                f.try_lock().unwrap();
                f
            };
            let p2 = p.ckpt.clone();
            let waiter = std::thread::spawn(move || {
                let mut b = b;
                b.execute("INSERT INTO t VALUES (1);")
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            });
            std::thread::sleep(Duration::from_millis(80));
            replace_lock_file(&p);
            let err = waiter
                .join()
                .expect("waiter thread")
                .expect_err("the replaced-lock write must fail");
            assert!(err.contains("deleted or replaced"), "{err}");
            drop(foreign);
            drop(a);
            let mut back = durable(&p2);
            assert!(ints(&mut back, "SELECT a FROM t ORDER BY a;").is_empty());
        }

        /// Wave 45n fuzz finding: flipping the v3 version word to 2 (one bit)
        /// used to take the v2-upgrade path and could silently drop every
        /// record. The corrected-header CRC now refuses it as header
        /// corruption, and the refused open must not rewrite the file.
        #[test]
        fn version_flip_v3_to_v2_is_refused_without_touching_the_wal() {
            let p = tmp_ckpt("verflip");
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);")
                .unwrap();
            drop(db);
            let wal = wal_path(&p);
            let good = std::fs::read(&wal).unwrap();
            assert_eq!(
                u32::from_le_bytes(good[8..12].try_into().unwrap()),
                WAL_VERSION
            );
            let mut corrupt = good.clone();
            corrupt[8] = WAL_VERSION_V2 as u8; // 3 -> 2: a single-bit flip
            std::fs::write(&wal, &corrupt).unwrap();
            let err = PinkDb::open_durable(p.to_str().unwrap())
                .unwrap_err()
                .to_string();
            assert!(err.contains("checksum") || err.contains("corrupt"), "{err}");
            assert_eq!(
                std::fs::read(&wal).unwrap(),
                corrupt,
                "the refused open must not upgrade/rewrite the file"
            );
            // Repair the byte: every committed record is still there.
            std::fs::write(&wal, &good).unwrap();
            let mut back = durable(&p);
            assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        }

        /// Rows of `t ORDER BY a`, or an empty vec when `t` does not exist.
        fn rows_or_missing(db: &mut PinkDb) -> Vec<i64> {
            match db.execute("SELECT a FROM t ORDER BY a;") {
                Ok(mut out) => match out.pop().unwrap() {
                    QueryResult::Selected { rows, .. } => rows
                        .into_iter()
                        .map(|r| match &r[0] {
                            Value::Integer(i) => *i as i64,
                            Value::Bigint(i) => *i,
                            other => panic!("expected integer, got {other:?}"),
                        })
                        .collect(),
                    other => panic!("expected rows, got {other:?}"),
                },
                Err(e) if e.to_string().contains("not found") => Vec::new(),
                Err(e) => panic!("unexpected query error: {e}"),
            }
        }

        /// One fuzz open: must not panic; when it succeeds the rows are
        /// exactly `1..=m` for an `m` in `[lo, hi]` (a committed prefix,
        /// never a wrong or duplicated value), a second fresh open is
        /// idempotent, and a failed open still leaves a clean error path.
        fn fuzz_open_prefix(p: &Path, lo: i64, hi: i64) {
            match PinkDb::open_durable(p.to_str().unwrap()) {
                Ok(mut db) => {
                    let rows = rows_or_missing(&mut db);
                    assert_eq!(
                        rows,
                        (1..=rows.len() as i64).collect::<Vec<_>>(),
                        "a fresh open must recover a prefix of committed rows"
                    );
                    assert!(
                        rows.len() as i64 >= lo && rows.len() as i64 <= hi,
                        "recovered {} rows, expected [{lo}, {hi}]",
                        rows.len()
                    );
                    let mut again = durable(p);
                    assert_eq!(
                        rows_or_missing(&mut again),
                        rows,
                        "a second fresh open must be idempotent"
                    );
                }
                Err(e) => {
                    let msg = e.to_string();
                    assert!(!msg.is_empty(), "a refused open needs a message");
                    // The refused state must still be a clean error path, not
                    // a panic or a hang.
                    let _ = PinkDb::open_durable(p.to_str().unwrap());
                }
            }
        }

        /// Build rows `1..=n`; when `ckpt_at` is set, checkpoint after that
        /// row, so `.pink` holds `1..=ckpt_at` and the live WAL the rest.
        fn build_db(p: &Path, n: i64, ckpt_at: Option<i64>) {
            let mut db = durable(p);
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            for i in 1..=n {
                db.execute(&format!("INSERT INTO t VALUES ({i});")).unwrap();
                if Some(i) == ckpt_at {
                    db.checkpoint().unwrap();
                }
            }
        }

        /// Truncate the live WAL at every length and reopen: no panic, and
        /// every successful open sees exactly the committed prefix (the
        /// checkpoint plus the records that survived).
        #[test]
        fn fuzz_wal_truncation_at_every_offset() {
            let p = tmp_ckpt("fuzztrunc");
            build_db(&p, 5, Some(2));
            let wal = wal_path(&p);
            let pristine = std::fs::read(&wal).unwrap();
            for len in 0..=pristine.len() {
                std::fs::write(&wal, &pristine[..len]).unwrap();
                fuzz_open_prefix(&p, 2, 5);
            }
            // Restore and confirm the whole corpus is intact afterwards.
            std::fs::write(&wal, &pristine).unwrap();
            let mut db = durable(&p);
            assert_eq!(
                ints(&mut db, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4, 5]
            );
        }

        /// Flip one byte at every WAL offset (bit 0 and all bits): header,
        /// commit slots, records. No panic, no wrong values; header damage is
        /// a clean refusal.
        #[test]
        fn fuzz_wal_byte_flips_never_yield_wrong_values() {
            let p = tmp_ckpt("fuzzflip");
            build_db(&p, 5, Some(2));
            let wal = wal_path(&p);
            let pristine = std::fs::read(&wal).unwrap();
            for off in 0..pristine.len() {
                for mask in [0x01u8, 0xFFu8] {
                    let mut corrupt = pristine.clone();
                    corrupt[off] ^= mask;
                    std::fs::write(&wal, &corrupt).unwrap();
                    fuzz_open_prefix(&p, 2, 5);
                }
            }
            std::fs::write(&wal, &pristine).unwrap();
            let mut db = durable(&p);
            assert_eq!(
                ints(&mut db, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4, 5]
            );
        }

        /// The 48-byte lock-file epoch region and the WAL commit slots are
        /// coordination hints: corrupting them must never change recovered
        /// data or block appends/reads.
        #[test]
        fn fuzz_epoch_region_and_commit_slots_are_hints_only() {
            let p = tmp_ckpt("fuzzhints");
            build_db(&p, 4, Some(2));
            let lock = lock_path(&p);
            // Corrupt the epoch region while the database is open (same
            // inode: no replacement), then read/write through the handle.
            {
                let mut f = OpenOptions::new().write(true).open(&lock).unwrap();
                f.write_all(&[0xFFu8; 96]).unwrap();
                f.sync_all().unwrap();
            }
            let mut db = durable(&p);
            assert_eq!(
                ints(&mut db, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4]
            );
            db.execute("INSERT INTO t VALUES (5);").unwrap();
            // Every single-byte corruption of the 48-byte epoch region (both
            // slots), while the database stays open: the slots are hints, so
            // recovered data never changes.
            let pristine_lock = std::fs::read(&lock).unwrap();
            for off in 0..48 {
                let mut corrupt = pristine_lock.clone();
                corrupt[off] ^= 0xFF;
                std::fs::write(&lock, &corrupt).unwrap();
                let mut back = durable(&p);
                assert_eq!(
                    ints(&mut back, "SELECT a FROM t ORDER BY a;"),
                    vec![1, 2, 3, 4, 5],
                    "epoch byte {off} corrupted: data must not change"
                );
            }
            std::fs::write(&lock, &pristine_lock).unwrap();
            // Truncating the lock file in place (same inode: not a
            // replacement) loses both slots: the epoch reads as absent, which
            // is "changed" — the next operation refreshes and the next bump
            // recreates slot A. Data is unaffected.
            {
                let f = OpenOptions::new().write(true).open(&lock).unwrap();
                f.set_len(0).unwrap();
                f.sync_all().unwrap();
            }
            db.execute("INSERT INTO t VALUES (50);").unwrap();
            assert_eq!(
                ints(&mut db, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4, 5, 50]
            );
            drop(db);
            // Corrupt the WAL commit slots (40..88) and reopen: the records
            // are authoritative, the slots are ignored when invalid.
            let wal = wal_path(&p);
            let mut data = std::fs::read(&wal).unwrap();
            for b in &mut data[WAL_SLOT_A as usize..WAL_SLOT_B as usize + SLOT_LEN] {
                *b ^= 0xA5;
            }
            std::fs::write(&wal, &data).unwrap();
            let mut back = durable(&p);
            assert_eq!(
                ints(&mut back, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4, 5, 50]
            );
            back.execute("INSERT INTO t VALUES (6);").unwrap();
            drop(back);
            let mut again = durable(&p);
            assert_eq!(
                ints(&mut again, "SELECT a FROM t ORDER BY a;"),
                vec![1, 2, 3, 4, 5, 6, 50]
            );
        }

        /// A hand-built genuine v2 WAL (magic + version 2 + gen + records).
        fn write_v2_wal(path: &Path, generation: u64, records: &[(u64, &str)]) {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"PINKWAL1");
            bytes.extend_from_slice(&2u32.to_le_bytes());
            bytes.extend_from_slice(&generation.to_le_bytes());
            for (lsn, sql) in records {
                let mut payload = Vec::new();
                payload.extend_from_slice(&1u32.to_le_bytes());
                payload.extend_from_slice(&(sql.len() as u32).to_le_bytes());
                payload.extend_from_slice(sql.as_bytes());
                bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&lsn.to_le_bytes());
                bytes.extend_from_slice(&crc32(&payload).to_le_bytes());
                bytes.extend_from_slice(&payload);
            }
            std::fs::write(path, &bytes).unwrap();
        }

        /// The v2→v3 upgrade's temp file: a stale temp (garbage, or a
        /// complete pre-rename copy) must never corrupt or block the upgrade,
        /// and a crash before the rename simply re-runs it.
        #[test]
        fn fuzz_v2_upgrade_temp_is_ignored_and_crash_reruns() {
            let p = tmp_ckpt("v2fuzz");
            write_pink(&p, &[0]);
            let wal = wal_path(&p);
            let upgrade = upgrade_path(&wal, 1);

            // Stale garbage temp next to an intact v2 WAL: upgraded anyway.
            write_v2_wal(
                &wal,
                1,
                &[
                    (1, "INSERT INTO t VALUES (1);"),
                    (2, "INSERT INTO t VALUES (2);"),
                ],
            );
            std::fs::write(&upgrade, b"not a wal at all").unwrap();
            {
                let mut db = durable(&p);
                assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);
            }
            let upgraded = std::fs::read(&wal).unwrap();
            assert_eq!(
                u32::from_le_bytes(upgraded[8..12].try_into().unwrap()),
                4,
                "the upgrade published a v4 header"
            );
            assert!(!upgrade.exists(), "the temp was renamed over the WAL");

            // The stale temp truncated at every offset (and re-copied each
            // round, because the successful upgrade renames it away): the
            // upgrade always re-runs from the intact v2 bytes.
            for len in 0..=upgraded.len() {
                write_v2_wal(
                    &wal,
                    1,
                    &[
                        (1, "INSERT INTO t VALUES (1);"),
                        (2, "INSERT INTO t VALUES (2);"),
                    ],
                );
                std::fs::write(&upgrade, &upgraded[..len]).unwrap();
                let mut db = durable(&p);
                assert_eq!(
                    ints(&mut db, "SELECT a FROM t ORDER BY a;"),
                    vec![0, 1, 2],
                    "a stale temp truncated at {len} must not affect the upgrade"
                );
            }

            // Crash right before the rename: the original v2 WAL is intact,
            // the temp holds the complete v3 image. Reopening re-runs the
            // upgrade from the v2 bytes (the temp is ignored, then replaced).
            write_v2_wal(
                &wal,
                1,
                &[
                    (1, "INSERT INTO t VALUES (1);"),
                    (2, "INSERT INTO t VALUES (2);"),
                ],
            );
            std::fs::write(&upgrade, &upgraded).unwrap();
            {
                let mut db = durable(&p);
                assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);
            }
            // Crash after the rename but before anything else: the WAL is v3
            // and a stale temp remains; opening normally ignores it.
            std::fs::write(&upgrade, b"stale leftover").unwrap();
            {
                let mut db = durable(&p);
                assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);
            }
        }

        /// Checkpoint `.tmp` windows: a `.tmp` next to an intact live WAL is
        /// discarded; a `.tmp` with the roll already created is published;
        /// truncated/flipped `.tmp` bytes either recover the full state or
        /// fail cleanly — never panic, never report wrong values.
        #[test]
        fn fuzz_checkpoint_tmp_windows() {
            let p = tmp_ckpt("tmpfuzz");
            build_db(&p, 3, Some(3)); // checkpoint holds 1..3; live WAL generation 2, empty
            let ckpt_bytes = std::fs::read(&p).unwrap();
            let tmp = tmp_path(&p);
            let wal = wal_path(&p);
            let roll = roll_path(&wal, 2);
            let roll_bytes = std::fs::read(&wal).unwrap();

            // State A: live WAL exists, garbage `.tmp` -> discarded.
            std::fs::write(&tmp, b"partial checkpoint").unwrap();
            {
                let mut db = durable(&p);
                assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
            }
            assert!(!tmp.exists(), "state A discards the .tmp");

            // State B: `.tmp` is the complete pre-rename checkpoint, the live
            // WAL is absent, its rolled generation survives, no `.pink`.
            // Recovery must publish `.tmp` (the roll proves it complete).
            let reset_state_b = |tmp_bytes: &[u8]| {
                let _ = std::fs::remove_file(&p);
                let _ = std::fs::remove_file(&wal);
                let _ = std::fs::remove_file(&roll);
                let parent = p.parent().unwrap();
                for e in std::fs::read_dir(parent).unwrap().flatten() {
                    let name = e.file_name().to_string_lossy().to_string();
                    if name.ends_with(".roll") {
                        std::fs::remove_file(e.path()).unwrap();
                    }
                }
                std::fs::write(&tmp, tmp_bytes).unwrap();
                std::fs::write(&roll, &roll_bytes).unwrap();
            };

            // The uncorrupted state recovers the full state.
            reset_state_b(&ckpt_bytes);
            {
                let mut db = durable(&p);
                assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
            }
            assert!(!tmp.exists(), "state B publishes the .tmp");
            assert!(!roll.exists(), "state B removes the roll");

            // Fuzz every truncation of `.tmp` (it is a small file), then some
            // byte flips: full recovery or a clean refusal, never a panic,
            // never wrong values.
            for len in 0..=ckpt_bytes.len() {
                reset_state_b(&ckpt_bytes[..len]);
                match PinkDb::open_durable(p.to_str().unwrap()) {
                    Ok(mut db) => {
                        assert_eq!(
                            rows_or_missing(&mut db),
                            vec![1, 2, 3],
                            "a published .tmp must hold the full state"
                        );
                    }
                    Err(e) => assert!(
                        !e.to_string().is_empty(),
                        "a refused corrupt .tmp needs a message"
                    ),
                }
            }
            for off in 0..ckpt_bytes.len() {
                let mut corrupt = ckpt_bytes.clone();
                corrupt[off] ^= 0x5A;
                reset_state_b(&corrupt);
                match PinkDb::open_durable(p.to_str().unwrap()) {
                    Ok(mut db) => {
                        assert_eq!(
                            rows_or_missing(&mut db),
                            vec![1, 2, 3],
                            "a published .tmp must hold the full state"
                        );
                    }
                    Err(e) => assert!(!e.to_string().is_empty()),
                }
            }

            // State C: `.tmp` present but nothing proves it complete (no
            // roll, no live WAL) -> discarded, an empty store opens cleanly.
            let _ = std::fs::remove_file(&p);
            let _ = std::fs::remove_file(&roll);
            let _ = std::fs::remove_file(&wal);
            std::fs::write(&tmp, &ckpt_bytes).unwrap();
            {
                let mut db = durable(&p);
                assert!(
                    rows_or_missing(&mut db).is_empty(),
                    "an unprovable .tmp must not resurrect rows"
                );
            }
        }

        /// Wave 45n gap (d): epoch fast-path evidence. A reader whose cached
        /// epoch is unchanged serves statements with one pread each and runs
        /// no refresh pass (no lease, no WAL read); one publication by
        /// another handle costs exactly one refresh pass, then the fast path
        /// resumes. White-box counters live on the handle's lease
        /// (`Lease::fast_path_counts`); limits: they count this handle's own
        /// `refresh_if_stale` calls, the pread itself is instrumented next to
        /// (not inside) `File::read_at`, and `needs_scan` may still run its
        /// two `stat`s under an *already acquired* lease.
        #[test]
        fn epoch_fast_path_skips_refresh_for_an_idle_reader() {
            let p = tmp_ckpt("epochfast");
            let mut a = durable(&p);
            a.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let mut b = durable(&p);
            // Warm b's view and its cached epoch.
            assert!(ints(&mut b, "SELECT a FROM t;").is_empty());
            let lease = b.lease_handle().unwrap();
            let (pre0, skip0, pass0) = lease.fast_path_counts();
            for _ in 0..25 {
                assert!(ints(&mut b, "SELECT a FROM t;").is_empty());
            }
            let (pre1, skip1, pass1) = lease.fast_path_counts();
            assert_eq!(pre1 - pre0, 25, "one precheck per statement");
            assert_eq!(skip1 - skip0, 25, "every statement took the fast path");
            assert_eq!(pass1, pass0, "an unchanged epoch runs no refresh pass");

            a.execute("INSERT INTO t VALUES (7);").unwrap();
            assert_eq!(ints(&mut b, "SELECT a FROM t;"), vec![7]);
            let (pre2, skip2, pass2) = lease.fast_path_counts();
            assert_eq!(pass2, pass0 + 1, "a publication costs one refresh pass");
            assert_eq!(skip2, skip1, "the changed epoch did not take the skip path");
            assert_eq!(pre2 - pre1, 1);
            for _ in 0..5 {
                assert_eq!(ints(&mut b, "SELECT a FROM t;"), vec![7]);
            }
            let (_, skip3, pass3) = lease.fast_path_counts();
            assert_eq!(pass3, pass2, "back to the fast path after the refresh");
            assert_eq!(skip3 - skip2, 5);
            drop(lease);
            cleanup(&p);
        }
    }
    // ---------- wave 45o: optimistic multi-writer commits ----------

    /// Build a frame's touched set for a raw injected record.
    fn sets_of(tables: &[(&str, &str)], dbs: &[&str]) -> FrameSets {
        let mut s = FrameSets::default();
        for (db, t) in tables {
            s.add_table(db, t);
        }
        for d in dbs {
            s.add_database(d);
        }
        s.canonicalize();
        s
    }

    /// The headline scenario: two overlapping transactions on disjoint tables
    /// both commit — neither holds the lease while it runs, so neither waits.
    #[test]
    fn optimistic_disjoint_transactions_both_commit() {
        let p = tmp_ckpt("optdisjoint");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER); CREATE TABLE u (a INTEGER);")
            .unwrap();
        let mut b = durable(&p);
        a.execute("BEGIN; INSERT INTO t VALUES (1);").unwrap();
        b.execute("BEGIN; INSERT INTO u VALUES (2);").unwrap();
        // Both are open at the same time; committing either must not wait on
        // the other.
        a.execute("COMMIT;").unwrap();
        b.execute("COMMIT;").unwrap();
        assert_eq!(ints(&mut a, "SELECT a FROM t ORDER BY a;"), vec![1]);
        assert_eq!(ints(&mut a, "SELECT a FROM u ORDER BY a;"), vec![2]);
        drop(a);
        drop(b);
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        assert_eq!(ints(&mut back, "SELECT a FROM u;"), vec![2]);
        cleanup(&p);
    }

    /// A same-table conflict names the table and the frame's LSN, aborts the
    /// transaction, and a retry (re-run on the refreshed state) commits.
    #[test]
    fn optimistic_same_table_conflict_then_retry_succeeds() {
        let p = tmp_ckpt("optsame");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut b = durable(&p);
        a.execute("BEGIN; INSERT INTO t VALUES (1);").unwrap();
        b.execute("BEGIN; INSERT INTO t VALUES (2);").unwrap();
        a.execute("COMMIT;").unwrap();
        let err = b.execute("COMMIT;").unwrap_err().to_string();
        assert!(err.contains("write conflict"), "{err}");
        assert!(err.contains("table \"main.t\""), "{err}");
        assert!(err.contains("lsn"), "{err}");
        // The aborted transaction logged nothing; a retry commits.
        b.execute("BEGIN; INSERT INTO t VALUES (2);").unwrap();
        b.execute("COMMIT;").unwrap();
        assert_eq!(ints(&mut b, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        drop(a);
        drop(b);
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }

    /// Read-set validation: a write computed from a read of `t` conflicts
    /// when another writer commits `t` after the transaction's base — the
    /// WAL record's replay would otherwise disagree with what the session
    /// saw.
    #[test]
    fn optimistic_read_set_conflict_prevents_a_stale_write() {
        let p = tmp_ckpt("optread");
        let mut a = durable(&p);
        a.execute(
            "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); CREATE TABLE u (a INTEGER);",
        )
        .unwrap();
        let mut b = durable(&p);
        a.execute("BEGIN; UPDATE u SET a = (SELECT MAX(a) FROM t);")
            .unwrap();
        b.execute("INSERT INTO t VALUES (50);").unwrap();
        let err = a.execute("COMMIT;").unwrap_err().to_string();
        assert!(err.contains("write conflict"), "{err}");
        assert!(err.contains("main.t"), "{err}");
        // The aborted write never applied.
        assert_eq!(ints(&mut a, "SELECT a FROM u;"), Vec::<i64>::new());
        // Read-only transactions never conflict on reads.
        a.execute("BEGIN; SELECT COUNT(*) FROM t;").unwrap();
        b.execute("INSERT INTO t VALUES (51);").unwrap();
        a.execute("COMMIT;").unwrap();
        drop(a);
        drop(b);
        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            vec![1, 50, 51]
        );
        cleanup(&p);
    }

    /// Write skew: T1 reads t and writes x, T2 reads x and writes t. Plain
    /// snapshot isolation would let both commit and break `x.v = t.a`;
    /// table-granular read-set validation aborts the second committer.
    #[test]
    fn optimistic_write_skew_is_prevented() {
        let p = tmp_ckpt("optskew");
        let mut a = durable(&p);
        a.execute(
            "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1); \
             CREATE TABLE x (v INTEGER); INSERT INTO x VALUES (1);",
        )
        .unwrap();
        let mut b = durable(&p);
        a.execute("BEGIN; UPDATE x SET v = 2 WHERE (SELECT MAX(a) FROM t) = 1;")
            .unwrap();
        b.execute("BEGIN; UPDATE t SET a = 2 WHERE (SELECT MAX(v) FROM x) = 1;")
            .unwrap();
        a.execute("COMMIT;").unwrap();
        let err = b.execute("COMMIT;").unwrap_err().to_string();
        assert!(err.contains("write conflict"), "{err}");
        assert!(
            err.contains("main.x") || err.contains("main.t"),
            "the skew must be reported on one of the crossed tables: {err}"
        );
        drop(a);
        drop(b);
        let mut back = durable(&p);
        // x.v = 2 and t.a = 1: the skew invariant (x.v = t.a) held.
        assert_eq!(ints(&mut back, "SELECT v FROM x;"), vec![2]);
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    /// An optimistic autocommit statement that conflicts is undone and
    /// re-executed under the lease (the pessimistic fallback): exactly one
    /// effect lands, and the evidence counters show the fallback ran. The
    /// conflicting frame is injected while the statement waits for the lease,
    /// so the interleaving is deterministic.
    #[test]
    fn optimistic_autocommit_falls_back_and_applies_exactly_once() {
        let p = tmp_ckpt("optfallback");
        let mut b = durable(&p);
        b.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        // A second open (its own lease) so b can be blocked on the flock.
        let a = durable(&p);
        b.set_busy_timeout(Duration::from_secs(5));
        // A foreign fd holds the flock while b's statement runs.
        let foreign = {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(format!("{}.lock", p.display()))
                .unwrap();
            f.try_lock().unwrap();
            f
        };
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        // b's WAL handle, shared with the thread: the main thread injects a
        // foreign frame through it while b waits for the flock.
        let b_wal = b.durable_handle().expect("durable");
        let handle = std::thread::spawn(move || {
            // The statement executes optimistically (in memory) and then
            // blocks on the foreign flock for up to the busy timeout.
            let r = b
                .execute("INSERT INTO t VALUES (2);")
                .map(|_| ())
                .map_err(|e| e.to_string());
            tx.send(format!("{r:?}")).ok();
            b
        });
        // Give the statement time to execute and reach the lease wait, then
        // inject a conflicting frame through *b's own* WAL handle (a second
        // writer's frame arriving mid-statement).
        std::thread::sleep(Duration::from_millis(250));
        {
            let mut guard = b_wal.lock().unwrap();
            guard
                .append(
                    &["INSERT INTO t VALUES (9);".to_string()],
                    Some(&sets_of(&[("main", "t")], &[])),
                )
                .unwrap();
        }
        drop(foreign);
        let r = rx.recv().expect("the statement finished");
        assert!(!r.contains("Err"), "the fallback must succeed: {r}");
        let mut b = handle.join().expect("writer thread");
        // Every row exactly once.
        assert_eq!(ints(&mut b, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 9]);
        let (attempts, conflicts, fallbacks) =
            b.optimistic_counts_for_test().expect("durable counts");
        assert!(attempts >= 1, "{attempts}");
        assert_eq!(conflicts, 1, "one injected conflict was detected");
        assert_eq!(fallbacks, 1, "the fallback ran exactly once");
        drop(a);
        drop(b);
        let mut back = durable(&p);
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 9]
        );
        cleanup(&p);
    }

    /// Frames carry their exact touched set: reads and writes of the
    /// statement, database-level `CREATE/DROP DATABASE`, and an empty set for
    /// `USE`.
    #[test]
    fn frames_carry_exact_touched_sets() {
        let p = tmp_ckpt("optsets");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("CREATE TABLE v (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        // Reads count too: the insert's scalar subquery reads v.
        db.execute("INSERT INTO t VALUES ((SELECT MAX(a) FROM v));")
            .unwrap();
        db.execute("CREATE DATABASE db1;").unwrap();
        db.execute("DROP DATABASE db1;").unwrap();
        db.execute("USE main;").unwrap();
        // Every lookup re-reads the frames: later statements in this test
        // (and the wave-44 `CALL`s below) append records after the first read.
        let sets = |stmts: &str| -> FrameSets {
            let recs = records_of(&wal_path(&p));
            let rec = recs
                .iter()
                .find(|r| r.stmts.iter().any(|s| s.contains(stmts)))
                .unwrap_or_else(|| panic!("no record with {stmts:?}: {recs:?}"));
            rec.sets
                .clone()
                .expect("live frames always carry known sets")
        };
        assert_eq!(
            sets("CREATE TABLE t"),
            sets_of(&[("main", "t")], &[]),
            "DDL write set"
        );
        assert_eq!(
            sets("INSERT INTO t VALUES (1)"),
            sets_of(&[("main", "t")], &[]),
            "insert write set"
        );
        assert_eq!(
            sets("SELECT MAX(a) FROM v"),
            sets_of(&[("main", "t"), ("main", "v")], &[]),
            "subquery reads are part of the frame's set"
        );
        assert_eq!(
            sets("CREATE DATABASE db1"),
            sets_of(&[], &["db1"]),
            "database-level set"
        );
        assert_eq!(sets("DROP DATABASE db1"), sets_of(&[], &["db1"]));
        assert_eq!(sets("USE main"), sets_of(&[], &[]), "USE touches nothing");
        // Wave 44: a `CALL`'s frame carries the tables its body actually
        // touched — the set depends on the data (loop conditions, `IF`
        // branches), so the executor records it and the logging path reads it
        // back (`PinkDb::proc_frame`).
        db.execute(
            "CREATE PROCEDURE touch(v INTEGER) AS $$ BEGIN \
               DECLARE n INTEGER DEFAULT 0; \
               WHILE n < 2 DO \
                 INSERT INTO t VALUES (v + n); \
                 SET n = n + 1; \
               END WHILE; \
               SELECT a FROM v; \
             END $$;",
        )
        .unwrap();
        db.execute("CALL touch(10);").unwrap();
        assert_eq!(
            sets("CALL touch(10)"),
            sets_of(&[("main", "t"), ("main", "v")], &[]),
            "a call's writes and reads are the frame's set"
        );
        // A call that reads only (no writes) still reports its reads, and one
        // that touches nothing reports an empty set.
        db.execute("CREATE PROCEDURE reader() AS $$ BEGIN SELECT a FROM v; END $$;")
            .unwrap();
        db.execute("CALL reader();").unwrap();
        assert_eq!(sets("CALL reader()"), sets_of(&[("main", "v")], &[]));
        db.execute("CREATE PROCEDURE idle() AS $$ BEGIN DECLARE x INTEGER; END $$;")
            .unwrap();
        db.execute("CALL idle();").unwrap();
        assert_eq!(sets("CALL idle()"), sets_of(&[], &[]));
        // The frames replay: a reopen lands on the same data. The `WHILE`
        // loop in `touch` ran twice, so the recorded `CALL` reproduced both
        // inserts (and `reader`'s SELECT left no trace).
        drop(db);
        let mut back = durable(&p);
        // (The earlier `INSERT … (SELECT MAX(a) FROM v)` wrote one NULL row.)
        assert_eq!(
            ints(&mut back, "SELECT a FROM t WHERE a IS NOT NULL ORDER BY a;"),
            vec![1, 10, 11]
        );
        assert!(back.procedure_in("main", "touch").is_ok());
        assert!(back.procedure_in("main", "idle").is_ok());
        assert_eq!(
            ints(&mut back, "SELECT COUNT(*) FROM t;"),
            vec![4],
            "the replayed CALLs, the hand-written inserts and the NULL row"
        );
        cleanup(&p);
    }

    /// A checkpoint by another process invalidates an open transaction's
    /// base: a retryable conflict, never a silent append onto a new
    /// generation.
    #[test]
    fn optimistic_commit_conflicts_on_incarnation_change() {
        let p = tmp_ckpt("optckpt");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut b = durable(&p);
        a.execute("BEGIN; INSERT INTO t VALUES (1);").unwrap();
        b.execute("CHECKPOINT;").unwrap();
        let err = a.execute("COMMIT;").unwrap_err().to_string();
        assert!(err.contains("write conflict"), "{err}");
        assert!(err.contains("WAL generation"), "{err}");
        // The retry runs against the new incarnation and commits.
        a.execute("BEGIN; INSERT INTO t VALUES (1);").unwrap();
        a.execute("COMMIT;").unwrap();
        drop(a);
        drop(b);
        let mut back = durable(&p);
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        cleanup(&p);
    }

    /// `save_snapshot` (`SAVE ... AS`, `.save <other>`) replaces the
    /// checkpoint without rolling the WAL: a base captured before it must
    /// conflict, or a frame appended after it would replay onto a different
    /// database.
    #[test]
    fn optimistic_commit_conflicts_on_checkpoint_replacement() {
        let p = tmp_ckpt("optsnap");
        let mut a = durable(&p);
        a.execute("CREATE TABLE t (a INTEGER); CHECKPOINT;")
            .unwrap();
        a.execute("BEGIN; INSERT INTO t VALUES (1);").unwrap();
        // Replace the target's `.pink` with an empty store's snapshot (the
        // WAL is empty, so the live-WAL guard lets it through).
        let other = PinkDb::new();
        other.save(p.to_str().unwrap()).unwrap();
        let err = a.execute("COMMIT;").unwrap_err().to_string();
        assert!(err.contains("write conflict"), "{err}");
        assert!(err.contains("checkpoint file"), "{err}");
        // The aborted transaction left the snapshot in place and the WAL
        // empty: a fresh open sees the snapshot's empty store.
        drop(a);
        let back = PinkDb::open(p.to_str().unwrap()).unwrap();
        assert!(back.list_tables().is_empty());
        cleanup(&p);
    }

    /// A durable transaction whose whole effect is `CREATE TABLE` + rows is
    /// exactly-once across the optimistic commit and a reopen (frames carry
    /// the sets; replay is unchanged).
    #[test]
    fn optimistic_transaction_replays_exactly_once_after_reopen() {
        let p = tmp_ckpt("optreplay");
        let mut a = durable(&p);
        a.execute("BEGIN; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2), (3); COMMIT;")
            .unwrap();
        drop(a);
        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.replayed_records, 1, "one transaction record");
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3]
        );
        drop(back);
        cleanup(&p);
    }

    /// The journal is pruned to the oldest open base and emptied when nothing
    /// is open: a long-running process does not accumulate sets forever.
    #[test]
    fn journal_is_pruned_when_no_base_is_open() {
        let p = tmp_ckpt("optjournal");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        for i in 0..5 {
            db.execute(&format!("INSERT INTO t VALUES ({i});")).unwrap();
        }
        let wal = db.durable_handle().unwrap();
        let entries = wal.lock().unwrap().journal_for_test();
        assert!(
            entries.is_empty(),
            "no open base: the journal is empty, got {entries:?}"
        );
        assert_eq!(wal.lock().unwrap().open_bases_for_test(), 0);
        cleanup(&p);
    }

    /// Index DDL is logged as canonical, fully-qualified, identifier-quoted
    /// text and its frame's touched set covers the indexed table. A
    /// checkpoint folds the definitions into `.pink` and leaves the fresh
    /// WAL generation empty (no re-log record).
    #[test]
    fn index_ddl_frames_are_canonical_and_checkpoint_does_not_relog() {
        let p = tmp_ckpt("idxddl");
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER, \"b b\" INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        db.execute("CREATE INDEX \"i x\" ON t(\"b b\");").unwrap();
        let rec = records_of(&wal_path(&p))
            .into_iter()
            .find(|r| r.stmts.iter().any(|s| s.contains("CREATE INDEX")))
            .expect("create-index record");
        assert_eq!(rec.stmts, vec!["CREATE INDEX \"i x\" ON main.t(\"b b\")"]);
        assert_eq!(
            rec.sets.as_ref().expect("known touched set").table_pairs(),
            &[("main".to_string(), "t".to_string())]
        );

        db.execute("DROP INDEX \"i x\";").unwrap();
        let rec = records_of(&wal_path(&p))
            .into_iter()
            .find(|r| r.stmts.iter().any(|s| s.contains("DROP INDEX")))
            .expect("drop-index record");
        assert_eq!(rec.stmts, vec!["DROP INDEX main.\"i x\""]);
        assert_eq!(
            rec.sets.as_ref().expect("known touched set").table_pairs(),
            &[("main".to_string(), "t".to_string())]
        );

        // A checkpoint publishes the definitions (and maps) inside `.pink`
        // (flags bit 3), so the fresh WAL generation carries no index
        // record at all, and repeated checkpoints keep it that way.
        db.execute("CREATE INDEX z ON t(a);").unwrap();
        db.execute("CREATE DATABASE db1;").unwrap();
        db.execute("CREATE TABLE db1.t (a INTEGER);").unwrap();
        db.execute("CREATE INDEX a ON db1.t(a);").unwrap();
        db.checkpoint().unwrap();
        assert!(records_of(&wal_path(&p)).is_empty(), "no index re-log");
        let flags = u32::from_le_bytes(std::fs::read(&p).unwrap()[8..12].try_into().unwrap());
        assert_eq!(flags & 0x8, 0x8, "the checkpoint carries the index section");
        db.checkpoint().unwrap();
        assert!(records_of(&wal_path(&p)).is_empty());
        drop(db);
        let db = durable(&p);
        assert_eq!(db.table_in("main", "t").unwrap().indexes.len(), 1);
        assert_eq!(db.table_in("db1", "t").unwrap().indexes.len(), 1);
        cleanup(&p);
    }

    /// A torn index record at the WAL tail is discarded by recovery: it must
    /// neither fabricate an index nor surface a replay error.
    #[test]
    fn torn_index_record_tail_does_not_fabricate_an_index() {
        let p = tmp_ckpt("idxtorn");
        write_pink(&p, &[1, 2]);
        let wal = wal_path(&p);
        write_wal_lsns(&wal, 1, &[(1, vec!["CREATE INDEX i ON t(a)".to_string()])]);
        let len = std::fs::metadata(&wal).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&wal)
            .unwrap()
            .set_len(len - 7)
            .unwrap();

        let mut db = durable(&p);
        assert!(
            !db.execute("EXPLAIN SELECT a FROM t WHERE a = 1;")
                .unwrap()
                .iter()
                .any(|r| format!("{r:?}").contains("IndexScan")),
            "a torn record must not fabricate an index"
        );
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }

    /// Recovery keeps an intact index record before the torn tail and applies
    /// no part of the truncated statement.
    #[test]
    fn recovery_keeps_an_intact_index_record_before_a_torn_tail() {
        let p = tmp_ckpt("idxtornkeep");
        write_pink(&p, &[1, 2]);
        let wal = wal_path(&p);
        write_wal_lsns(
            &wal,
            1,
            &[
                (1, vec!["CREATE INDEX i ON t(a)".to_string()]),
                (2, vec!["INSERT INTO t VALUES (3)".to_string()]),
            ],
        );
        let len = std::fs::metadata(&wal).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&wal)
            .unwrap()
            .set_len(len - 5)
            .unwrap();

        let mut db = durable(&p);
        let plan = db
            .execute("EXPLAIN SELECT a FROM t WHERE a = 1;")
            .unwrap()
            .into_iter()
            .map(|r| format!("{r:?}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("IndexScan"),
            "intact index record lost: {plan}"
        );
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }

    /// The `Connection`/`Database` front-end shares the same logging path:
    /// index DDL inside a transaction and a SQL `CHECKPOINT` must survive a
    /// reopen (through the checkpoint's `.pink` index section). `Connection` does not run the
    /// optimizer, so the assertions use the durable `PinkDb` handle.
    #[test]
    fn connection_index_ddl_survives_reopen_and_checkpoint() {
        let p = tmp_ckpt("idxconn");
        {
            let store = crate::Database::open(p.to_str().unwrap()).unwrap();
            let mut c = store.connect();
            c.execute("CREATE TABLE t (a INTEGER);").unwrap();
            c.execute("INSERT INTO t VALUES (1), (2);").unwrap();
            c.execute("BEGIN; CREATE INDEX i ON t(a); COMMIT;").unwrap();
            c.execute("CHECKPOINT;").unwrap();
            assert_eq!(ints_conn(&mut c, "SELECT a FROM t WHERE a = 2;"), vec![2]);
        }
        {
            let mut db = durable(&p);
            assert_eq!(db.table_in("main", "t").unwrap().indexes.len(), 1);
            let plan = db
                .execute("EXPLAIN SELECT a FROM t WHERE a = 2;")
                .unwrap()
                .into_iter()
                .map(|r| format!("{r:?}"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("IndexScan"),
                "Connection path lost the index across CHECKPOINT: {plan}"
            );
        }
        {
            let store = crate::Database::open(p.to_str().unwrap()).unwrap();
            let mut c = store.connect();
            c.execute("DROP INDEX i;").unwrap();
        }
        let mut db = durable(&p);
        assert_eq!(db.table_in("main", "t").unwrap().indexes.len(), 0);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
        cleanup(&p);
    }
}
