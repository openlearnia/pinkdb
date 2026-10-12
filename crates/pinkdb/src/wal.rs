//! Write-ahead log: durable commits without changing the `.pink` format.
//!
//! For `<path>.pink` the log is a sidecar `<path>.pink.wal`:
//!
//! ```text
//! header:  magic "PINKWAL1" (8 bytes) + format version u32 (=2)
//!          + generation u64
//! record:  payload_len u32 | lsn u64 | crc32(payload) u32 | payload
//! payload: stmt_count u32 | per statement: len u32 + UTF-8 SQL text
//! ```
//!
//! One record = one committed transaction unit: an autocommit statement logs
//! individually; `BEGIN … COMMIT` logs all of the transaction's mutating
//! statements as one record at `COMMIT` (a `ROLLBACK`, or a write-conflict
//! abort, logs nothing). Replay is logical: the recorded SQL is re-parsed and
//! re-executed, which is deterministic in v1 (no random()/now()). Because
//! replay re-executes SQL text, a durable commit is refused when its bound
//! plan depends on a session-local view (views are never persisted, so replay
//! could not rebuild it).
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
//! <path>.pink.wal          live WAL, header names its generation `gen`
//! <path>.pink.wal.<gen>.roll   a rolled-away generation (name == header gen)
//! ```
//!
//! The protocol (every step ordered so recovery can tell crash windows apart):
//!
//! 1. write `.pink.tmp` (full state) and fsync the file *and its directory*;
//! 2. rename the live WAL to `.wal.<gen>.roll` (the name carries the
//!    generation, so a roll can never overwrite another roll) and create a
//!    fresh header-only WAL with generation `gen + 1`, fsynced;
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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result};

pub const WAL_MAGIC: &[u8; 8] = b"PINKWAL1";
/// On-disk format version. Version 2 adds the generation to the header.
pub const WAL_VERSION: u32 = 2;
const HEADER_LEN: usize = 20;
const RECORD_HEADER_LEN: usize = 16;
const ROLL_SUFFIX: &str = ".roll";

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
}

/// An open WAL file positioned for appends (shared by every clone of the
/// owning store, so `txn.work` snapshots log to the same file).
#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
    checkpoint_path: PathBuf,
    policy: SyncPolicy,
    /// Generation of the live WAL; rolls carry the drained generation.
    gen: u64,
    last_lsn: u64,
    /// Set after a write/sync failure: appends are refused so a retry can
    /// never append a duplicate record after a partially durable write.
    poisoned: Option<String>,
    /// The replay session's database after the records appended so far
    /// (`None` = unknown, so the next append prefixes unconditionally).
    /// Multi-connection appends emit a `USE` prefix whenever their session
    /// differs, so every record replays in the database it was executed in.
    session_db: Option<String>,
}

/// One rolled-away WAL generation found on disk.
#[derive(Debug)]
struct Roll {
    gen: u64,
    path: PathBuf,
}

/// What a recovery pass sees in the live WAL before replaying it.
#[derive(Debug)]
struct WalProbe {
    /// Generation from a valid header (`None` for a torn/absent file).
    gen: Option<u64>,
    /// Whether the file holds at least one intact record.
    has_records: bool,
}

#[derive(Debug)]
struct Header {
    gen: u64,
}

#[derive(Debug)]
enum HeaderError {
    TooShort,
    BadMagic,
    BadVersion(u32),
}

impl Wal {
    fn header_for(gen: u64) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[..8].copy_from_slice(WAL_MAGIC);
        h[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
        h[12..].copy_from_slice(&gen.to_le_bytes());
        h
    }

    /// Create (or recreate) the WAL file at `path` with just a header.
    fn create_file(path: &Path, gen: u64) -> Result<File> {
        // No `append(true)` here: std rejects append+truncate on Unix, and a
        // fresh file needs only sequential writes from offset 0.
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        f.write_all(&Self::header_for(gen))?;
        f.sync_all()?;
        Ok(f)
    }

    /// Path of the `.pink` checkpoint this WAL belongs to.
    pub fn checkpoint_path(&self) -> &Path {
        &self.checkpoint_path
    }

    /// Open the WAL at `path` (creating it when absent) and parse every intact
    /// record. The file is truncated at the last intact record, so the handle
    /// returned appends right after it. `recreate_gen` is the generation used
    /// when the file is missing or its header is torn; recovery computes it
    /// from the surviving roll files so the sequence never regresses.
    /// Returns the records, the bytes discarded from the tail, and the
    /// positioned handle.
    pub(crate) fn open(
        path: &Path,
        checkpoint_path: PathBuf,
        policy: SyncPolicy,
        recreate_gen: u64,
    ) -> Result<(Wal, Vec<Record>, u64)> {
        let plain = |file: File, gen: u64, last_lsn: u64| Wal {
            file,
            path: path.to_path_buf(),
            checkpoint_path: checkpoint_path.clone(),
            policy,
            gen,
            last_lsn,
            poisoned: None,
            session_db: None,
        };
        if !path.exists() {
            return Ok((
                plain(Self::create_file(path, recreate_gen)?, recreate_gen, 0),
                Vec::new(),
                0,
            ));
        }
        let data = std::fs::read(path)?;
        let header = match parse_header(&data) {
            Ok(h) => h,
            Err(e) => {
                if data.len() <= HEADER_LEN {
                    // Torn header (crash while creating): start over.
                    return Ok((
                        plain(Self::create_file(path, recreate_gen)?, recreate_gen, 0),
                        Vec::new(),
                        data.len() as u64,
                    ));
                }
                return Err(PinkDbError::Execution(header_error(path, &e)));
            }
        };
        let (records, good_end) =
            parse_records(&data, HEADER_LEN).map_err(PinkDbError::Execution)?;
        let file = OpenOptions::new().read(true).append(true).open(path)?;
        let truncated = (data.len() - good_end) as u64;
        if truncated > 0 {
            file.set_len(good_end as u64)?;
            file.sync_all()?;
        }
        // Records are validated strictly increasing, so max == last; `max`
        // keeps the next append monotonic even if that ever changes.
        let last_lsn = records.iter().map(|r| r.lsn).max().unwrap_or(0);
        Ok((plain(file, header.gen, last_lsn), records, truncated))
    }

    /// Append one committed unit of statements as a single record.
    pub(crate) fn append(&mut self, stmts: &[String]) -> Result<()> {
        if let Some(msg) = &self.poisoned {
            return Err(PinkDbError::Execution(format!(
                "WAL is unusable after an earlier write failure ({msg})"
            )));
        }
        let payload = encode_payload(stmts)?;
        let len = record_len_guard(payload.len())?;
        let lsn = self.last_lsn + 1;
        let mut rec = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(&lsn.to_le_bytes());
        rec.extend_from_slice(&crc32(&payload).to_le_bytes());
        rec.extend_from_slice(&payload);
        let result = self.file.write_all(&rec).and_then(|()| match self.policy {
            SyncPolicy::Sync => self.file.sync_all(),
            SyncPolicy::Batch => Ok(()),
        });
        match result {
            Ok(()) => {
                self.last_lsn = lsn;
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
        self.file.sync_all()?;
        Ok(())
    }

    /// Record the replay-session database after the records appended so far.
    /// Called after recovery (the replay leaves the session wherever the last
    /// record did) and by the embedded path (whose session is the store's).
    pub(crate) fn set_session_db(&mut self, db: &str) {
        self.session_db = Some(db.to_string());
    }

    /// Append one committed unit executed in session database `db_before`
    /// (when it still exists), leaving the replay session at `db_after`. When
    /// the WAL's replay session differs from `db_before`, a `USE db_before`
    /// statement is prepended so the record replays against the same database
    /// (multi-connection stores: `USE` is per-connection but the WAL is
    /// shared).
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
    ) -> Result<()> {
        match db_before {
            Some(before) => {
                if self
                    .session_db
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(before))
                {
                    self.append(stmts)?;
                } else {
                    // Quote the name: databases may hold spaces/keywords when
                    // created with a quoted identifier (`CREATE DATABASE
                    // "My Db"`).
                    let mut rec = Vec::with_capacity(stmts.len() + 1);
                    rec.push(format!("USE \"{}\"", before.replace('"', "\"\"")));
                    rec.extend_from_slice(stmts);
                    self.append(&rec)?;
                }
                self.session_db = Some(db_after.to_string());
            }
            None => {
                self.append(stmts)?;
                self.session_db = None;
            }
        }
        Ok(())
    }

    /// Crash-safe checkpoint: publish the state `save_tmp` writes to `tmp` as
    /// the new checkpoint and roll the WAL generation (see module docs).
    pub(crate) fn checkpoint(&mut self, save_tmp: impl FnOnce(&Path) -> Result<()>) -> Result<()> {
        if self.poisoned.is_some() {
            return Err(PinkDbError::Execution(
                "cannot checkpoint: the WAL failed earlier in this session".to_string(),
            ));
        }
        let tmp = tmp_path(&self.checkpoint_path);
        let roll = roll_path(&self.path, self.gen);
        // A roll of the live generation already existing proves a protocol
        // violation (a live generation is rolled exactly once); overwriting
        // it could destroy records, so refuse.
        if roll.exists() {
            let msg = format!("a roll file for generation {} already exists", self.gen);
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
        //    gen + 1.
        std::fs::rename(&self.path, &roll)?;
        let fresh_gen = self.gen.checked_add(1).ok_or_else(|| {
            PinkDbError::Execution("WAL generation counter overflow".to_string())
        })?;
        match Self::create_file(&self.path, fresh_gen) {
            Ok(fresh) => {
                self.file = fresh;
                self.gen = fresh_gen;
                self.last_lsn = 0;
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
        // 4. The old generation is now folded into the published `.pink`. A
        //    failed unlink (or a crash before it) is harmless: the next open
        //    proves foldedness from the roll name + live generation and
        //    removes it again.
        let _ = std::fs::remove_file(&roll);
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

fn encode_payload(stmts: &[String]) -> Result<Vec<u8>> {
    let count = u32::try_from(stmts.len()).map_err(|_| {
        PinkDbError::Execution("transaction has too many statements for one WAL record".to_string())
    })?;
    let mut total = 4usize;
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
    for s in stmts {
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    debug_assert_eq!(out.len(), total);
    Ok(out)
}

/// Bounds-checked payload decode. `Err` means a CRC-valid record is malformed
/// (hard error on replay, never a silent drop).
fn decode_payload(payload: &[u8]) -> std::result::Result<Vec<String>, String> {
    let mut pos = 0usize;
    let u32_at = |pos: &mut usize| -> std::result::Result<u32, String> {
        if *pos + 4 > payload.len() {
            return Err("truncated payload".to_string());
        }
        let v = u32::from_le_bytes(payload[*pos..*pos + 4].try_into().expect("4 bytes"));
        *pos += 4;
        Ok(v)
    };
    let count = u32_at(&mut pos)? as usize;
    if count > payload.len() / 4 {
        return Err(format!("implausible statement count {count}"));
    }
    let mut stmts = Vec::with_capacity(count);
    for _ in 0..count {
        let n = u32_at(&mut pos)? as usize;
        if pos + n > payload.len() {
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

/// Parse records from `start` onward. A short read, a CRC mismatch, or an LSN
/// that does not continue the strictly increasing sequence stops the scan
/// (torn tail / framing corruption); a malformed CRC-valid payload is a hard
/// error. Returns the records and the offset just past the last intact
/// record.
fn parse_records(
    data: &[u8],
    start: usize,
) -> std::result::Result<(Vec<Record>, usize), String> {
    let mut pos = start;
    let mut prev_lsn = 0u64;
    let mut out = Vec::new();
    while let Some(payload_start) = pos.checked_add(RECORD_HEADER_LEN) {
        if payload_start > data.len() {
            break;
        }
        let payload_len = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let lsn = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().expect("8 bytes"));
        let crc = u32::from_le_bytes(data[pos + 12..pos + 16].try_into().expect("4 bytes"));
        // The LSN is not covered by the payload CRC; a record that regresses
        // it is corrupt framing and must not extend the intact prefix (the
        // next append would otherwise reuse an LSN).
        if lsn <= prev_lsn {
            break;
        }
        let Some(end) = payload_start.checked_add(payload_len) else {
            break;
        };
        if end > data.len() {
            break;
        }
        let payload = &data[payload_start..end];
        if crc32(payload) != crc {
            break;
        }
        let stmts = decode_payload(payload)
            .map_err(|e| format!("corrupt WAL record at offset {pos} (lsn {lsn}): {e}"))?;
        out.push(Record { lsn, stmts });
        prev_lsn = lsn;
        pos = end;
    }
    Ok((out, pos))
}

// ---------- file naming + header parsing ----------

fn wal_path(checkpoint: &Path) -> PathBuf {
    let mut s = checkpoint.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

fn tmp_path(checkpoint: &Path) -> PathBuf {
    let mut s = checkpoint.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// `<wal>.<gen>.roll`: the generation in the name is what makes stale rolls
/// distinguishable from a roll of the live generation.
fn roll_path(wal: &Path, gen: u64) -> PathBuf {
    let mut s = wal.as_os_str().to_os_string();
    s.push(format!(".{gen}{ROLL_SUFFIX}"));
    PathBuf::from(s)
}

fn parse_header(data: &[u8]) -> std::result::Result<Header, HeaderError> {
    if data.len() < HEADER_LEN {
        return Err(HeaderError::TooShort);
    }
    if &data[..8] != WAL_MAGIC {
        return Err(HeaderError::BadMagic);
    }
    let version = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
    if version != WAL_VERSION {
        return Err(HeaderError::BadVersion(version));
    }
    Ok(Header {
        gen: u64::from_le_bytes(data[12..20].try_into().expect("8 bytes")),
    })
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
    }
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
fn sync_parent_dir(path: &Path) {
    if let Ok(f) = File::open(parent_dir(path)) {
        let _ = f.sync_all();
    }
}

/// The live WAL's header state, before anything is replayed: `None` when the
/// file is absent; a torn header reports no generation and no records (the
/// file is recreated); invalid magic/version on a file that carries payload
/// bytes is a hard error.
fn probe_wal(path: &Path) -> Result<Option<WalProbe>> {
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read(path)?;
    match parse_header(&data) {
        Ok(h) => {
            let (records, _) = parse_records(&data, HEADER_LEN).map_err(PinkDbError::Execution)?;
            Ok(Some(WalProbe {
                gen: Some(h.gen),
                has_records: !records.is_empty(),
            }))
        }
        Err(e) => {
            if data.len() <= HEADER_LEN {
                // A torn header (crash while creating the WAL): recreate it.
                Ok(Some(WalProbe {
                    gen: None,
                    has_records: false,
                }))
            } else {
                Err(PinkDbError::Execution(header_error(path, &e)))
            }
        }
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
        if let Some(mid) = name.strip_prefix(&prefix).and_then(|r| r.strip_suffix(ROLL_SUFFIX)) {
            if let Ok(gen) = mid.parse::<u64>() {
                rolls.push(Roll {
                    gen,
                    path: entry.path(),
                });
            }
        }
    }
    rolls.sort_by_key(|r| r.gen);
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
/// them or proves the generation older than the live WAL.
fn check_roll_header(roll: &Roll) -> Result<()> {
    let mut f = match File::open(&roll.path) {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };
    let mut buf = [0u8; HEADER_LEN];
    if f.read_exact(&mut buf).is_err() {
        return Ok(());
    }
    match parse_header(&buf) {
        Ok(h) if h.gen == roll.gen => Ok(()),
        Ok(h) => Err(PinkDbError::Execution(format!(
            "cannot recover: roll file {} is named generation {} but its header says {}; \
             refusing to delete it",
            roll.path.display(),
            roll.gen,
            h.gen
        ))),
        Err(_) => Ok(()),
    }
}

fn inconsistent_roll(path: &Path, gen: u64, live_gen: u64) -> PinkDbError {
    PinkDbError::Execution(format!(
        "cannot recover: rolled WAL generation {gen} ({}) does not precede the live WAL \
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

/// Recreate the live WAL when it is missing or has a torn header, continuing
/// the generation sequence past every roll so future rolls stay unique.
fn ensure_live_wal(wal: &Path, live_gen: Option<u64>, rolls: &[Roll]) -> Result<()> {
    if live_gen.is_some() {
        return Ok(());
    }
    let gen = rolls.iter().map(|r| r.gen).max().map_or(1, |g| g + 1);
    Wal::create_file(wal, gen).map(|_| ())
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
) -> Result<()> {
    let live_gen = probe.and_then(|p| p.gen);
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
                    if r.gen >= lgen {
                        return Err(inconsistent_roll(&r.path, r.gen, lgen));
                    }
                }
            }
            info.stale_rolls_removed += remove_rolls(rolls);
        }
        return ensure_live_wal(wal, live_gen, rolls);
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
            if r.gen >= lgen {
                return Err(inconsistent_roll(&r.path, r.gen, lgen));
            }
        }
        let _ = std::fs::remove_file(tmp);
        info.stale_rolls_removed += remove_rolls(rolls);
        return ensure_live_wal(wal, live_gen, rolls);
    }

    if rolls.is_empty() {
        // Crash during step 1 (or a redundant `.tmp`): `.tmp` is not provably
        // complete, so it must never be published. The live WAL is the
        // authority; skipping the removal repeats the same decision.
        let _ = std::fs::remove_file(tmp);
        return ensure_live_wal(wal, live_gen, rolls);
    }

    // `.tmp` + a roll: the roll rename happens only after `.tmp` was fully
    // written and fsynced, so `.tmp` is complete and contains the roll's
    // records. Publish it *first* — this is the crash window where an older
    // recovery replayed the roll into memory, deleted `.tmp`/`.roll`, and
    // lost everything on the next open.
    let max_roll = rolls.iter().map(|r| r.gen).max().expect("rolls is non-empty");
    if let Some(lgen) = live_gen {
        if max_roll >= lgen {
            let path = rolls
                .iter()
                .find(|r| r.gen == max_roll)
                .map(|r| r.path.as_path())
                .expect("max comes from the list");
            return Err(inconsistent_roll(path, max_roll, lgen));
        }
    }
    if let Err(e) = std::fs::rename(tmp, ckpt) {
        return Err(PinkDbError::Io(e));
    }
    sync_parent_dir(ckpt);
    info.checkpoints_completed += 1;
    info.stale_rolls_removed += remove_rolls(rolls);
    ensure_live_wal(wal, live_gen, rolls)
}

// ---------- durable open / checkpoint on PinkDb ----------

impl PinkDb {
    /// Open `path` for durable commits with the default [`SyncPolicy::Sync`]:
    /// load the checkpoint `.pink` (if present), recover an interrupted
    /// checkpoint, replay `<path>.pink.wal` (replay + torn-tail truncation),
    /// then log every commit to that WAL.
    /// A missing checkpoint *and* WAL starts a fresh durable database.
    pub fn open_durable(path: &str) -> Result<PinkDb> {
        Ok(Self::open_durable_with(path, SyncPolicy::Sync)?.0)
    }

    /// [`PinkDb::open_durable`] with an explicit sync policy; also returns
    /// what recovery replayed (for callers like the REPL that report it).
    pub fn open_durable_with(
        path: &str,
        policy: SyncPolicy,
    ) -> Result<(PinkDb, RecoveryInfo)> {
        let ckpt = PathBuf::from(path);
        let wal = wal_path(&ckpt);
        let tmp = tmp_path(&ckpt);
        let mut info = RecoveryInfo::default();

        // 1. Finish an interrupted checkpoint before loading anything.
        let rolls = scan_rolls(&wal)?;
        let probe = probe_wal(&wal)?;
        recover_checkpoint(&ckpt, &wal, &tmp, &rolls, probe.as_ref(), &mut info)?;

        // 2. Checkpoint, if one exists, then replay the live WAL on top.
        let mut db = if ckpt.exists() { PinkDb::open(path)? } else { PinkDb::new() };
        // When the live WAL is missing or torn, continue the generation
        // sequence past every roll (recovery has already recreated the file
        // with this generation when that was needed).
        let recreate_gen = probe
            .as_ref()
            .and_then(|p| p.gen)
            .unwrap_or_else(|| rolls.iter().map(|r| r.gen).max().map_or(1, |g| g + 1));
        let (handle, records, truncated) =
            Wal::open(&wal, ckpt.clone(), policy, recreate_gen)?;
        for rec in &records {
            replay_record(&mut db, rec)?;
        }
        info.replayed_records += records.len() as u64;
        info.replayed_statements += records.iter().map(|r| r.stmts.len() as u64).sum::<u64>();
        info.truncated_bytes = truncated;
        let mut handle = handle;
        // Replay leaves the session wherever the last record did (a logged
        // `USE` or the start at `main`): later appends must know that.
        handle.set_session_db(db.current_db());
        db.wal = Some(std::sync::Arc::new(std::sync::Mutex::new(handle)));
        Ok((db, info))
    }

    /// Checkpoint: atomically persist the current state to the bound `.pink`
    /// path and roll the WAL. Only durable databases have a bound path; a
    /// transaction must not be open (its changes are not committed).
    pub fn checkpoint(&mut self) -> Result<()> {
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
                ))
            }
        };
        let mut guard = wal.lock().map_err(|_| {
            PinkDbError::Execution("internal lock error (poisoned)".to_string())
        })?;
        guard.checkpoint(|tmp| self.save_sync(tmp))?;
        // A fresh WAL generation replays from the checkpoint's session
        // default (the checkpoint does not store a session): reset the
        // tracked replay session so later appends `USE`-prefix correctly.
        let fallback = self.fallback_db();
        guard.set_session_db(&fallback);
        Ok(())
    }

    /// The `.pink` path this database is durable to, if any.
    pub fn durable_path(&self) -> Option<PathBuf> {
        let wal = self.durable_handle()?;
        let guard = wal.lock().ok()?;
        Some(guard.checkpoint_path().to_path_buf())
    }

    /// fsync the WAL (relevant with [`SyncPolicy::Batch`]); no-op in memory.
    pub fn flush_wal(&mut self) -> Result<()> {
        if let Some(wal) = self.durable_handle() {
            let mut guard = wal.lock().map_err(|_| {
                PinkDbError::Execution("internal lock error (poisoned)".to_string())
            })?;
            guard.flush()?;
        }
        Ok(())
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

    /// A plain snapshot with a durability guard: refuse to write over a
    /// target whose sidecar WAL still holds committed records. The target's
    /// next durable open would replay those records on top of this snapshot —
    /// duplicating or bricking that database. Saving to the *bound* path is a
    /// checkpoint, not a snapshot; see [`PinkDb::checkpoint`].
    pub fn save_snapshot(&self, path: &str) -> Result<()> {
        let target_wal = wal_path(Path::new(path));
        if target_wal.exists()
            && probe_wal(&target_wal)?.map(|p| p.has_records).unwrap_or(false)
        {
            return Err(PinkDbError::Execution(format!(
                "refusing to save over {path}: {} holds committed WAL records; \
                 open that database durably and checkpoint it instead",
                target_wal.display()
            )));
        }
        self.save(path)
    }
}

/// Filesystem identity of two `.pink` paths. `canonicalize` resolves
/// symlinks and, on case-insensitive filesystems, the on-disk spelling. The
/// sidecar WALs are the fallback when a `.pink` does not exist yet (a durable
/// database always has a WAL), and canonical parent + file name is the last
/// resort for paths that exist nowhere.
fn same_file(a: &Path, b: &Path) -> bool {
    if let (Ok(ca), Ok(cb)) = (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        if ca == cb {
            return true;
        }
    }
    let (wa, wb) = (wal_path(a), wal_path(b));
    if let (Ok(ca), Ok(cb)) = (std::fs::canonicalize(&wa), std::fs::canonicalize(&wb)) {
        if ca == cb {
            return true;
        }
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
    use crate::types::Value;
    use crate::QueryResult;
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

    fn records_of(path: &Path) -> Vec<Record> {
        let data = std::fs::read(path).unwrap();
        let (recs, _) = parse_records(&data, HEADER_LEN).unwrap();
        recs
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

    /// Write a WAL file with the given generation and (lsn, statements)
    /// records, CRCs computed for real.
    fn write_wal_lsns(path: &Path, gen: u64, records: &[(u64, Vec<String>)]) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&Wal::header_for(gen)).unwrap();
        for (lsn, stmts) in records {
            let payload = encode_payload(stmts).unwrap();
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
        f.write_all(&Wal::header_for(1)).unwrap();
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
        let (mut db, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(info, RecoveryInfo::default());
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'y');").unwrap();
        // Read-only statements are never logged.
        db.execute("SELECT * FROM t;").unwrap();

        let data = std::fs::read(wal_path(&p)).unwrap();
        assert_eq!(&data[..HEADER_LEN], &Wal::header_for(1));
        let (recs, good_end) = parse_records(&data, HEADER_LEN).unwrap();
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
        let mut pos = HEADER_LEN;
        for rec in &recs {
            let payload_len =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(data[pos + 12..pos + 16].try_into().unwrap());
            let payload = &data[pos + 16..pos + 16 + payload_len];
            assert_eq!(crc32(payload), crc, "record {rec:?}");
            pos += 16 + payload_len;
        }
        drop(db);

        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
        db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);").unwrap();
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
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
        cleanup(&p);
    }

    #[test]
    fn replay_reproduces_update_delete_and_drop_state() {
        let p = tmp_ckpt("dmlreplay");
        {
            let mut db = durable(&p);
            db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
            db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z');").unwrap();
            db.execute("BEGIN;").unwrap();
            db.execute("UPDATE t SET a = a * 10;").unwrap();
            db.execute("DELETE FROM t WHERE a = 20;").unwrap();
            db.execute("COMMIT;").unwrap();
            // Catalog churn replays too: create + drop cancel out.
            db.execute("CREATE TABLE u (a INTEGER); DROP TABLE u;").unwrap();
        }
        // No checkpoint: the entire state comes from replaying the WAL.
        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(info.replayed_records, 5);
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![10, 30]);
        assert!(back.execute("SELECT * FROM u;").is_err(), "u was dropped");
        // Checkpointing the replayed state round-trips exactly, no replay.
        back.checkpoint().unwrap();
        drop(back);
        let (mut again, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(info.replayed_records, 0);
        assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![10, 30]);
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

        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(info.replayed_records, 2, "stops at the last good record");
        assert_eq!(info.truncated_bytes, (len5 as u64) - after_two);
        assert_eq!(wal_len(&p), after_two, "file truncated to last good offset");
        assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1]);
        // Appends after recovery land right after the last good record.
        back.execute("INSERT INTO t VALUES (3);").unwrap();
        drop(back);
        let mut again = durable(&p);
        assert_eq!(
            ints(&mut again, "SELECT a FROM t ORDER BY a;"),
            vec![1, 3]
        );
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
        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
        let (mut db, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
    fn wal_header_validation() {
        let p = tmp_ckpt("badmagic");
        // Invalid magic on a file with payload bytes: hard error.
        std::fs::write(wal_path(&p), b"NOTAWAL1\0\0\0\0extrabytes").unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("bad magic"), "{err}");

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
        let mut bytes = Wal::header_for(7).to_vec();
        bytes.extend_from_slice(b"garbage-bytes-here");
        std::fs::write(wal_path(&p), &bytes).unwrap();
        let (db, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert_eq!(info.truncated_bytes, 18);
        assert_eq!(wal_len(&p), HEADER_LEN as u64);
        drop(db);
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
        let payload = encode_payload(&["SELECT 1".to_string()]).unwrap();
        assert!(payload.len() < 100);
    }

    #[test]
    fn pre_generation_roll_name_is_refused() {
        let p = tmp_ckpt("legacyroll");
        let legacy = PathBuf::from(format!("{}.roll", wal_path(&p).display()));
        std::fs::write(&legacy, b"not a gen-named roll").unwrap();
        let err = PinkDb::open_durable(p.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("pre-generation roll name"), "{err}");
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
        assert_eq!(wal_len(&p), HEADER_LEN as u64, "WAL rolled to a fresh header");
        assert!(records_of(&wal_path(&p)).is_empty());
        assert!(!tmp_path(&p).exists() && !roll_path(&wal_path(&p), 1).exists());
        drop(db);

        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
            Wal::create_file(&wal_path(&p), 2).unwrap();
        }
        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(info.checkpoints_completed, 1, "recovery must publish .tmp");
        assert_eq!(info.replayed_records, 0, "the roll is folded by the publish");
        assert_eq!(info.stale_rolls_removed, 1);
        assert!(p.exists(), "the recovered checkpoint must be a durable .pink");
        assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
        drop(back);

        // The second clean open (no crash in between) must not lose the rows
        // that the old recovery kept only in memory.
        let (mut again, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
            Wal::create_file(&wal_path(&p), 2).unwrap();
        }
        let (mut back, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
            Wal::create_file(&wal_path(&p), 2).unwrap();
        }
        // ... and recovery crashing right after its publish, before the roll
        // unlink (i.e. the exact intermediate state of the fix).
        std::fs::rename(tmp_path(&p), &p).unwrap();
        let (mut db, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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
            Wal::create_file(&wal_path(&p2), 2).unwrap();
        }
        let (mut db, info) = PinkDb::open_durable_with(p2.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
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

        let (mut db, info) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(info.checkpoints_completed, 0, ".tmp is not provably complete");
        assert_eq!(info.replayed_records, 1, "only the live WAL record");
        assert_eq!(info.stale_rolls_removed, 1);
        assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);
        drop(db);
        // Idempotent: no re-application, no duplicate.
        let (mut again, _) = PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync)
            .unwrap();
        assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![0, 1, 2]);
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
            Image { name: "empty", pink: None, tmp: None, rolls: vec![], wal: None, torn_wal: false, expect: Rows(vec![]) },
            Image { name: "pink_only", pink: Some(vec![0]), tmp: None, rolls: vec![], wal: None, torn_wal: false, expect: Rows(vec![0]) },
            Image { name: "wal_only", pink: None, tmp: None, rolls: vec![], wal: Some((1, vec![cr.clone(), i0.clone()])), torn_wal: false, expect: Rows(vec![0]) },
            Image { name: "pink_wal", pink: Some(vec![0]), tmp: None, rolls: vec![], wal: Some((1, vec![i1.clone()])), torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_only", pink: None, tmp: Some(vec![0]), rolls: vec![], wal: None, torn_wal: false, expect: Rows(vec![]) },
            Image { name: "tmp_pink", pink: Some(vec![0]), tmp: Some(vec![0]), rolls: vec![], wal: None, torn_wal: false, expect: Rows(vec![0]) },
            Image { name: "tmp_wal_records", pink: None, tmp: Some(vec![0, 1]), rolls: vec![], wal: Some((1, vec![cr.clone(), i0.clone(), i1.clone()])), torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_pink_wal_records", pink: Some(vec![0]), tmp: Some(vec![0, 1]), rolls: vec![], wal: Some((1, vec![i1.clone()])), torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "roll_without_checkpoint", pink: None, tmp: None, rolls: vec![(1, vec![cr.clone(), i0.clone()])], wal: Some((2, vec![])), torn_wal: false, expect: Error("checkpoint is missing") },
            Image { name: "roll_no_live_no_pink", pink: None, tmp: None, rolls: vec![(1, vec![cr.clone(), i0.clone()])], wal: None, torn_wal: false, expect: Error("checkpoint is missing") },
            Image { name: "roll_pink_no_live", pink: Some(vec![0]), tmp: None, rolls: vec![(1, vec![cr.clone(), i0.clone()])], wal: None, torn_wal: false, expect: Rows(vec![0]) },
            Image { name: "roll_pink_live_empty", pink: Some(vec![0]), tmp: None, rolls: vec![(1, vec![cr.clone(), i0.clone()])], wal: Some((2, vec![])), torn_wal: false, expect: Rows(vec![0]) },
            Image { name: "roll_stale_live_records", pink: Some(vec![0, 1]), tmp: None, rolls: vec![(1, vec![i1.clone()])], wal: Some((2, vec![i2.clone()])), torn_wal: false, expect: Rows(vec![0, 1, 2]) },
            Image { name: "roll_not_older", pink: Some(vec![0]), tmp: None, rolls: vec![(2, vec![i1.clone()])], wal: Some((2, vec![])), torn_wal: false, expect: Error("does not precede") },
            Image { name: "tmp_roll_no_pink", pink: None, tmp: Some(vec![0, 1]), rolls: vec![(1, vec![cr.clone(), i0.clone(), i1.clone()])], wal: None, torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_roll_live_no_pink", pink: None, tmp: Some(vec![0, 1]), rolls: vec![(1, vec![cr.clone(), i0.clone(), i1.clone()])], wal: Some((2, vec![])), torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_roll_pink", pink: Some(vec![0]), tmp: Some(vec![0, 1]), rolls: vec![(1, vec![i1.clone()])], wal: Some((2, vec![])), torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_roll_pink_no_live", pink: Some(vec![0]), tmp: Some(vec![0, 1]), rolls: vec![(1, vec![i1.clone()])], wal: None, torn_wal: false, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_stale_roll_live_records", pink: Some(vec![0, 1]), tmp: Some(vec![0, 1, 2]), rolls: vec![(1, vec![i1.clone()])], wal: Some((2, vec![i2.clone()])), torn_wal: false, expect: Rows(vec![0, 1, 2]) },
            Image { name: "tmp_roll_torn_live", pink: None, tmp: Some(vec![0, 1]), rolls: vec![(1, vec![cr.clone(), i0.clone(), i1.clone()])], wal: None, torn_wal: true, expect: Rows(vec![0, 1]) },
            Image { name: "tmp_torn_live_no_roll", pink: None, tmp: Some(vec![0]), rolls: vec![], wal: None, torn_wal: true, expect: Rows(vec![]) },
            Image { name: "torn_live", pink: None, tmp: None, rolls: vec![], wal: None, torn_wal: true, expect: Rows(vec![]) },
            Image { name: "torn_live_roll_pink", pink: Some(vec![0]), tmp: None, rolls: vec![(1, vec![cr.clone(), i0.clone()])], wal: None, torn_wal: true, expect: Rows(vec![0]) },
            Image { name: "tmp_pink_torn_live", pink: Some(vec![0]), tmp: Some(vec![0]), rolls: vec![], wal: None, torn_wal: true, expect: Rows(vec![0]) },
            Image { name: "tmp_roll_stale_high_live", pink: Some(vec![0, 1]), tmp: Some(vec![0, 1, 2]), rolls: vec![(1, vec![i1.clone()])], wal: Some((3, vec![i2.clone()])), torn_wal: false, expect: Rows(vec![0, 1, 2]) },
            Image { name: "tmp_roll_not_older", pink: Some(vec![0]), tmp: Some(vec![0, 1]), rolls: vec![(2, vec![i1.clone()])], wal: Some((2, vec![])), torn_wal: false, expect: Error("does not precede") },
            Image { name: "roll_generation_equal_live", pink: Some(vec![0, 1]), tmp: None, rolls: vec![(1, vec![i1.clone()])], wal: Some((1, vec![i2.clone()])), torn_wal: false, expect: Error("does not precede") },
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
            for (gen, recs) in &img.rolls {
                write_wal_lsns(&roll_path(&wal, *gen), *gen, &raw(recs));
            }
            if let Some((gen, recs)) = &img.wal {
                write_wal_lsns(&wal, *gen, &raw(recs));
            } else if img.torn_wal {
                std::fs::write(&wal, b"PIN").unwrap();
            }

            match &img.expect {
                Expect::Rows(want) => {
                    // Open #1: recovery.
                    let (mut db, _) = PinkDb::open_durable_with(
                        p.to_str().unwrap(),
                        SyncPolicy::Sync,
                    )
                    .unwrap_or_else(|e| panic!("{}: first open failed: {e}", img.name));
                    assert_eq!(matrix_rows(&mut db), *want, "{}: first open", img.name);
                    let live_has = img.wal.as_ref().map(|(_, r)| !r.is_empty()).unwrap_or(false);
                    let published =
                        img.tmp.is_some() && !live_has && !img.rolls.is_empty();
                    assert_eq!(
                        p.exists(),
                        img.pink.is_some() || published,
                        "{}: checkpoint presence after recovery",
                        img.name
                    );
                    drop(db);

                    // Open #2 and #3: idempotent, same rows.
                    for open in 2..=3 {
                        let (mut again, info) = PinkDb::open_durable_with(
                            p.to_str().unwrap(),
                            SyncPolicy::Sync,
                        )
                        .unwrap_or_else(|e| panic!("{}: open #{open} failed: {e}", img.name));
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
        assert!(db.checkpoint().is_err(), "open transaction refuses checkpoint");
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
        let (db, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
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
            recs.iter()
                .all(|r| !r.stmts.iter().any(|s| s.to_ascii_uppercase().contains("USE"))),
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
        let (db, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
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
        let (db, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
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
        assert_eq!(ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"), vec![1, 2]);
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
        let (db, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
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
        assert_eq!(ints_conn(&mut db.connect(), "SELECT COUNT(*) FROM t;"), vec![1]);
        assert!(b.execute("COMMIT;").is_err(), "stale transaction must conflict");
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
        assert_eq!(
            ints(&mut back, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2]
        );
        cleanup(&p);
    }

    #[test]
    fn write_conflict_abort_is_not_logged() {
        let p = tmp_ckpt("conflict");
        let (db, _) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
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
        assert!(db
            .execute("UPDATE t SET a = 3 WHERE a IN (SELECT a FROM v);")
            .is_err());
        db.execute("ROLLBACK;").unwrap();

        // In-memory databases keep the old behavior.
        let mut mem = PinkDb::new();
        mem.execute("CREATE TABLE t (a INTEGER);").unwrap();
        mem.execute("INSERT INTO t VALUES (1);").unwrap();
        mem.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        mem.execute("UPDATE t SET a = 2 WHERE a IN (SELECT a FROM v);").unwrap();
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
            records.iter().any(|r| r.stmts.iter().any(|s| s.starts_with("USE "))),
            "USE must be logged: {records:?}"
        );
        assert!(
            records.iter().any(|r| r.stmts.iter().any(|s| s.contains("CREATE DATABASE"))),
            "CREATE DATABASE must be logged"
        );

        let (mut back, info) =
            PinkDb::open_durable_with(p.to_str().unwrap(), SyncPolicy::Sync).unwrap();
        assert!(info.replayed_records > 0);
        assert_eq!(ints(&mut back, "SELECT a FROM main.t ORDER BY a;"), vec![10, 20]);
        assert_eq!(ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"), vec![1, 2, 3, 4]);
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
    fn checkpoint_resets_the_replay_session_to_the_checkpoint_default() {        let p = tmp_ckpt("ckptsession");
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
        assert_eq!(ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"), vec![1, 2]);
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
    fn multi_connection_sessions_replay_each_record_in_its_database() {        let p = tmp_ckpt("multidb");
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
        assert_eq!(ints(&mut back, "SELECT a FROM main.t ORDER BY a;"), vec![2, 5]);
        assert_eq!(ints(&mut back, "SELECT a FROM db1.t ORDER BY a;"), vec![1, 3, 4]);
        assert_eq!(back.current_db(), "main");
    }

    #[test]
    fn save_snapshot_refuses_a_target_with_a_live_wal() {        let a = tmp_ckpt("snapa");
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
}
