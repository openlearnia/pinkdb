//! `pink+s3://` catalog durability: a cached pinkdb image plus immutable
//! portable WAL records in object storage.
//!
//! A handle reads the catalog as of its last load or publish, like the other
//! backends: commits arbitrate through a conditional create of the next log
//! object, and the loser reloads and retries, so staleness costs a retry
//! rather than correctness. Refreshing is therefore limited to the point where
//! our own base may have moved, instead of listing on every statement.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pinkdb::{EncryptionKey, PinkDb};

use super::store::SqlMetadataStore;
use crate::metadata::pink;
use crate::s3::{PutNew, S3Storage};
use crate::storage::Storage;
use crate::{LakeError, Result};

const CHECKPOINT_INTERVAL: u64 = 100;
const SEQUENCE_WIDTH: usize = 20;

#[derive(Debug)]
pub(crate) struct RemoteCatalog {
    storage: Arc<S3Storage>,
    label: PathBuf,
    key: Option<EncryptionKey>,
    applied: AtomicU64,
    checkpoint: AtomicU64,
    /// Whether a LIST may show a newer base than `applied`. Every statement
    /// would otherwise pay a LIST, so reads only look again after this handle
    /// published, and commits arbitrate through `put_new` in between.
    dirty: AtomicBool,
    work_dir: PathBuf,
    interval: u64,
}

impl Drop for RemoteCatalog {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.work_dir);
    }
}

pub(crate) fn open(
    url: &str,
    bucket: &str,
    prefix: &str,
    create: bool,
    key: Option<EncryptionKey>,
) -> Result<SqlMetadataStore> {
    let storage = Arc::new(S3Storage::from_url(bucket, prefix)?);
    let label = PathBuf::from(crate::redact_location(url));
    let (work_dir, _) = crate::storage::private_temp_dir().map_err(|source| LakeError::Io {
        context: format!("creating a private cache for {url}"),
        source,
    })?;
    let remote = Arc::new(RemoteCatalog {
        storage,
        label,
        key,
        applied: AtomicU64::new(0),
        checkpoint: AtomicU64::new(0),
        dirty: AtomicBool::new(false),
        work_dir,
        interval: checkpoint_interval(),
    });
    let (db, found) = remote.load()?;
    if !create && !found {
        return Err(LakeError::NotALake {
            path: remote.label.clone(),
        });
    }
    let db = remote.materialize(db)?;
    pink::open_db(url, db, Some(remote), create)
}

impl RemoteCatalog {
    fn load(&self) -> Result<(PinkDb, bool)> {
        let checkpoint = self.listed_checkpoint()?;
        let mut db = match checkpoint {
            Some(sequence) => {
                let bytes = self.checkpoint_bytes(sequence)?;
                self.open_checkpoint(&bytes)?
            }
            None => PinkDb::new(),
        };
        let base = checkpoint.unwrap_or(0);
        let logs = self.logs_after(base)?;
        let found = checkpoint.is_some() || !logs.is_empty();
        let records = logs
            .iter()
            .map(|sequence| self.read_log(*sequence).map(|record| (*sequence, record)))
            .collect::<Result<Vec<_>>>()?;
        db.replay_wal_records(&records)
            .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
        let applied = logs.last().copied().unwrap_or(base);
        self.applied.store(applied, Ordering::Relaxed);
        self.checkpoint.store(base, Ordering::Relaxed);
        Ok((db, found))
    }

    fn materialize(&self, db: PinkDb) -> Result<PinkDb> {
        let sequence = self.applied.load(Ordering::Relaxed);
        let path = self
            .work_dir
            .join(format!("catalog-{sequence}-{}.pink", crate::fresh_id()));
        let path_text = path.to_string_lossy();
        match &self.key {
            Some(key) => db.save_with_key(&path_text, key),
            None => db.save(&path_text),
        }
        .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
        let opened = match &self.key {
            Some(key) => pinkdb::PinkDb::open_durable_with_key(&path_text, key),
            None => pinkdb::PinkDb::open_durable(&path_text),
        }
        .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
        opened.set_auto_checkpoint_bytes(None);
        Ok(opened)
    }

    pub(crate) fn refresh(&self, db: &mut PinkDb) -> Result<()> {
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return Ok(());
        }
        let refreshed = self.refresh_base(db);
        if refreshed.is_err() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        refreshed
    }

    fn refresh_base(&self, db: &mut PinkDb) -> Result<()> {
        let applied = self.applied.load(Ordering::Relaxed);
        let logs = match self.logs_after(applied) {
            Ok(logs) => logs,
            // `expire` uploads a checkpoint at the newest applied sequence and
            // then deletes the logs it folded. A gap right above our base is
            // that fold, not corruption: reload from the newest checkpoint.
            // `load` checks contiguity above the newest checkpoint, so real
            // corruption in the live range still fails.
            Err(LakeError::Corrupt { .. }) => {
                self.rebuild(db)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if logs.is_empty() {
            return Ok(());
        }
        let records = logs
            .iter()
            .map(|sequence| self.read_log(*sequence).map(|record| (*sequence, record)))
            .collect::<Result<Vec<_>>>()?;
        db.replay_wal_records(&records)
            .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
        self.applied
            .store(*logs.last().expect("non-empty"), Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn publish(&self, db: &mut PinkDb) -> Result<()> {
        let records = db
            .drain_wal_records()
            .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
        if records.is_empty() {
            return Ok(());
        }
        // Our base may be stale now, and the next read should look again.
        self.dirty.store(true, Ordering::Relaxed);
        // `expire` checkpoints and prunes from another handle. If it ran while
        // this transaction was being built, `log/<applied + 1>` is already
        // below the newest checkpoint: writing it would fork a sequence every
        // later open ignores. Rebase and let the caller retry instead.
        if let Some(newest) = self.listed_checkpoint()?
            && newest > self.applied.load(Ordering::Relaxed)
        {
            self.rebuild(db)?;
            return Err(LakeError::Conflict {
                path: self.label.clone(),
                message: format!(
                    "a checkpoint at sequence {newest} superseded this catalog handle; \
                     retry the transaction"
                ),
            });
        }
        for record in records {
            let sequence = self
                .applied
                .load(Ordering::Relaxed)
                .checked_add(1)
                .ok_or_else(|| LakeError::Internal {
                    message: "the S3 catalog sequence overflowed".to_string(),
                })?;
            let bytes = match &self.key {
                Some(key) => PinkDb::seal_wal_record(&record, key, sequence),
                None => Ok(record),
            }
            .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
            let path = log_path(sequence);
            match self.storage.put_new(&path, &bytes) {
                Ok(PutNew::Written) => {
                    self.applied.store(sequence, Ordering::Relaxed);
                }
                Ok(PutNew::Exists) => {
                    self.rebuild(db)?;
                    return Err(LakeError::Conflict {
                        path: self.label.clone(),
                        message: format!(
                            "another writer published catalog log {sequence}; retry the transaction"
                        ),
                    });
                }
                Ok(PutNew::ConditionalUnsupported) => {
                    self.rebuild(db)?;
                    return Err(LakeError::Io {
                        context: format!("writing {}", self.label.display()),
                        source: std::io::Error::other(
                            "the S3 endpoint does not support conditional writes; \
                             pink+s3:// catalogs are read-only on this endpoint",
                        ),
                    });
                }
                Err(error) => {
                    let _ = self.rebuild(db);
                    return Err(error);
                }
            }
        }
        self.maybe_checkpoint(db)
    }

    fn rebuild(&self, db: &mut PinkDb) -> Result<()> {
        let (loaded, _) = self.load()?;
        *db = self.materialize(loaded)?;
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn maybe_checkpoint(&self, db: &PinkDb) -> Result<()> {
        let sequence = self.applied.load(Ordering::Relaxed);
        let previous = self.checkpoint.load(Ordering::Relaxed);
        if sequence == 0 || sequence.saturating_sub(previous) < self.interval {
            return Ok(());
        }
        self.checkpoint_now(db)
    }

    /// The newest checkpoint named by a LIST, which is the truth. `catalog/head`
    /// is written as an operator hint but never read: it may be stale, missing,
    /// or name an upload that never landed, and the log listing is needed in
    /// any case, so reading it would only cost a round trip.
    fn listed_checkpoint(&self) -> Result<Option<u64>> {
        Ok(self
            .storage
            .list("catalog/checkpoint")?
            .into_iter()
            .filter_map(|name| name.strip_suffix(".pink").and_then(parse_sequence))
            .max())
    }

    /// Upload a checkpoint at the applied sequence (maintenance path only;
    /// never called on commit beyond the interval check in `publish`).
    pub(crate) fn checkpoint_now(&self, db: &PinkDb) -> Result<()> {
        let sequence = self.applied.load(Ordering::Relaxed);
        if sequence == 0 || self.checkpoint.load(Ordering::Relaxed) >= sequence {
            return Ok(());
        }
        // A handle that fell behind another writer must not upload an image at
        // its own older sequence and then move `head` backwards; the writer
        // that is ahead checkpoints its own base when it expires.
        if self
            .listed_checkpoint()?
            .is_some_and(|newest| newest > sequence)
        {
            return Ok(());
        }
        let bytes = match &self.key {
            Some(key) => db.to_pink_bytes_with_key(key),
            None => db.to_pink_bytes(),
        }
        .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))?;
        match self.storage.put_new(&checkpoint_path(sequence), &bytes)? {
            PutNew::Written | PutNew::Exists => {}
            PutNew::ConditionalUnsupported => {
                return Err(LakeError::Io {
                    context: format!("checkpointing {}", self.label.display()),
                    source: std::io::Error::other(
                        "the S3 endpoint does not support conditional writes",
                    ),
                });
            }
        }
        self.store_cached_checkpoint(sequence, &bytes)?;
        self.storage
            .put("catalog/head", sequence.to_string().as_bytes())?;
        self.checkpoint.store(sequence, Ordering::Relaxed);
        Ok(())
    }

    /// Delete log objects folded into the newest checkpoint and superseded
    /// checkpoint images. Called only from snapshot expiration.
    pub(crate) fn prune(&self) -> Result<()> {
        let Some(newest) = self.listed_checkpoint()? else {
            return Ok(());
        };
        for name in self.storage.list("catalog/checkpoint")? {
            let superseded = name
                .strip_suffix(".pink")
                .and_then(parse_sequence)
                .filter(|sequence| *sequence < newest);
            if let Some(sequence) = superseded {
                self.storage.delete(&format!("catalog/checkpoint/{name}"))?;
                // Keep the local cache bounded too; it is a pure cache.
                let _ = std::fs::remove_file(self.cache_path(sequence));
            }
        }
        for name in self.storage.list("catalog/log")? {
            if parse_sequence(&name).is_some_and(|sequence| sequence <= newest) {
                self.storage.delete(&format!("catalog/log/{name}"))?;
            }
        }
        Ok(())
    }

    fn logs_after(&self, after: u64) -> Result<Vec<u64>> {
        let mut logs: Vec<u64> = self
            .storage
            .list("catalog/log")?
            .into_iter()
            .filter_map(|name| parse_sequence(&name))
            .filter(|sequence| *sequence > after)
            .collect();
        logs.sort_unstable();
        let mut expected = after.saturating_add(1);
        for sequence in &logs {
            if *sequence != expected {
                return Err(LakeError::Corrupt {
                    path: self.label.clone(),
                    message: format!(
                        "catalog log sequence is not contiguous: expected {expected}, found {sequence}"
                    ),
                });
            }
            expected = expected.saturating_add(1);
        }
        Ok(logs)
    }

    fn read_log(&self, sequence: u64) -> Result<Vec<u8>> {
        let bytes = self.storage.read(
            &log_path(sequence),
            &format!("reading S3 catalog log {sequence}"),
        )?;
        match &self.key {
            Some(key) => PinkDb::open_wal_record(&bytes, key, sequence)
                .map_err(|error| pink::map_error(&self.label.to_string_lossy(), error)),
            None if bytes.starts_with(pinkdb::crypto::ENC_MAGIC) => Err(LakeError::Encrypted {
                path: self.label.clone(),
                message: "the S3 catalog log is encrypted; open it with the lake's key".to_string(),
            }),
            None => Ok(bytes),
        }
    }

    fn open_checkpoint(&self, bytes: &[u8]) -> Result<PinkDb> {
        let opened = match &self.key {
            Some(key) => PinkDb::from_pink_bytes_with_key(bytes, key),
            None if bytes.starts_with(pinkdb::crypto::ENC_MAGIC) => {
                return Err(LakeError::Encrypted {
                    path: self.label.clone(),
                    message: "the S3 catalog checkpoint is encrypted; open it with the lake's key"
                        .to_string(),
                });
            }
            None => PinkDb::from_pink_bytes(bytes),
        };
        opened.map_err(|error| pink::map_error(&self.label.to_string_lossy(), error))
    }

    fn checkpoint_bytes(&self, sequence: u64) -> Result<Vec<u8>> {
        let cache = self.cache_path(sequence);
        match std::fs::read(&cache) {
            Ok(bytes) => return Ok(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(LakeError::Io {
                    context: format!("reading the catalog checkpoint cache {}", cache.display()),
                    source,
                });
            }
        }
        let bytes = self.storage.read(
            &checkpoint_path(sequence),
            &format!("reading S3 catalog checkpoint {sequence}"),
        )?;
        self.store_cached_checkpoint(sequence, &bytes)?;
        Ok(bytes)
    }

    fn cache_path(&self, sequence: u64) -> PathBuf {
        let mut hash = DefaultHasher::new();
        self.label.hash(&mut hash);
        std::env::var("AWS_ENDPOINT_URL_S3").ok().hash(&mut hash);
        std::env::temp_dir()
            .join("pinkdb-catalog-s3-cache")
            .join(format!("{:016x}", hash.finish()))
            .join(format!("{sequence:0SEQUENCE_WIDTH$}.pink"))
    }

    fn store_cached_checkpoint(&self, sequence: u64, bytes: &[u8]) -> Result<()> {
        let path = self.cache_path(sequence);
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        std::fs::create_dir_all(parent).map_err(|source| LakeError::Io {
            context: format!("creating the catalog checkpoint cache {}", parent.display()),
            source,
        })?;
        let tmp = parent.join(format!(".tmp-{}-{}", std::process::id(), crate::fresh_id()));
        crate::storage::write_private(&tmp, bytes).map_err(|source| LakeError::Io {
            context: format!("writing the catalog checkpoint cache {}", tmp.display()),
            source,
        })?;
        match std::fs::rename(&tmp, &path) {
            Ok(()) => Ok(()),
            Err(_error) if path.exists() => {
                let _ = std::fs::remove_file(tmp);
                Ok(())
            }
            Err(source) => {
                let _ = std::fs::remove_file(tmp);
                Err(LakeError::Io {
                    context: format!("publishing the catalog checkpoint cache {}", path.display()),
                    source,
                })
            }
        }
    }
}

fn checkpoint_interval() -> u64 {
    std::env::var("PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(CHECKPOINT_INTERVAL)
}

fn parse_sequence(name: &str) -> Option<u64> {
    (name.len() == SEQUENCE_WIDTH && name.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| name.parse().ok())
        .flatten()
}

fn log_path(sequence: u64) -> String {
    format!("catalog/log/{sequence:0SEQUENCE_WIDTH$}")
}

fn checkpoint_path(sequence: u64) -> String {
    format!("catalog/checkpoint/{sequence:0SEQUENCE_WIDTH$}.pink")
}
