//! Local-directory storage primitives.
//!
//! Every filesystem operation of a lake goes through [`LocalStorage`]: reads,
//! directory creation, listing, and atomic publication. A write goes to a
//! *unique* temp file beside its target (`<name>.<uuid>.tmp`, created with
//! `create_new(true)` so a planted symlink or old temp file is never followed
//! or truncated), is fsynced, and is then renamed into place — or hard-linked
//! into place, for files that must never be replaced. The parent directory is
//! fsynced afterwards, best-effort; the crate docs are precise about what
//! that does and does not guarantee on APFS.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use crate::{fresh_id, LakeError, Result};

/// The catalog file name inside the lake root.
pub(crate) const CATALOG_FILE: &str = "catalog.json";

/// Directory-and-file I/O rooted at one lake directory.
#[derive(Debug)]
pub(crate) struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub(crate) fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
        }
    }

    /// Absolute path of a lake-relative path.
    pub(crate) fn abs(&self, rel: &str) -> PathBuf {
        if rel.is_empty() {
            self.root.clone()
        } else {
            self.root.join(rel)
        }
    }

    pub(crate) fn exists(&self, rel: &str) -> bool {
        self.abs(rel).exists()
    }

    pub(crate) fn read(&self, rel: &str) -> Result<Vec<u8>> {
        let path = self.abs(rel);
        fs::read(&path).map_err(|e| LakeError::Io {
            context: format!("reading {}", path.display()),
            source: e,
        })
    }

    pub(crate) fn file_len(&self, rel: &str) -> Result<u64> {
        let path = self.abs(rel);
        fs::metadata(&path).map(|m| m.len()).map_err(|e| LakeError::Io {
            context: format!("stat {}", path.display()),
            source: e,
        })
    }

    pub(crate) fn create_dir(&self, rel: &str) -> Result<()> {
        let path = self.abs(rel);
        fs::create_dir_all(&path).map_err(|e| LakeError::Io {
            context: format!("creating directory {}", path.display()),
            source: e,
        })
    }

    /// Write `bytes` to `rel` atomically, replacing any previous file:
    /// a unique `<name>.<uuid>.tmp` sibling, fsync, rename over `rel`, then
    /// a best-effort parent-directory fsync. A crash before the rename
    /// leaves only an unreferenced `.tmp` file. Only the catalog uses this
    /// (the snapshot path refuses to replace an existing version).
    pub(crate) fn write_atomic(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let path = self.abs(rel);
        self.ensure_parent(&path)?;
        let (tmp, mut file) = create_unique_tmp(&path).map_err(|e| LakeError::Io {
            context: format!("creating a temp file next to {}", path.display()),
            source: e,
        })?;
        let written = write_and_sync(&mut file, bytes);
        drop(file);
        if let Err(e) = written {
            let _ = fs::remove_file(&tmp);
            return Err(LakeError::Io {
                context: format!("writing {}", tmp.display()),
                source: e,
            });
        }
        fs::rename(&tmp, &path).map_err(|e| LakeError::Io {
            context: format!("publishing {}", path.display()),
            source: e,
        })?;
        self.sync_parent(&path);
        Ok(())
    }

    /// Publish `bytes` under `rel` **only if `rel` does not exist yet**:
    /// write a unique fsynced `.tmp`, then hard-link it to `rel` (an atomic
    /// create-if-absent), then remove the temp name. Returns `Ok(false)` when
    /// `rel` already existed — nothing was published and the temp file is
    /// gone. This is how snapshots (and the create-time v1 pair) are
    /// published: a published version file is never replaced, and two
    /// concurrent writers cannot both claim the same version.
    pub(crate) fn publish_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        let path = self.abs(rel);
        self.ensure_parent(&path)?;
        let (tmp, mut file) = create_unique_tmp(&path).map_err(|e| LakeError::Io {
            context: format!("creating a temp file next to {}", path.display()),
            source: e,
        })?;
        let written = write_and_sync(&mut file, bytes);
        drop(file);
        if let Err(e) = written {
            let _ = fs::remove_file(&tmp);
            return Err(LakeError::Io {
                context: format!("writing {}", tmp.display()),
                source: e,
            });
        }
        let linked = fs::hard_link(&tmp, &path);
        let _ = fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                self.sync_parent(&path);
                Ok(true)
            }
            // `link` refuses to follow a symlink at `path`, so `AlreadyExists`
            // covers both a regular file and a planted symlink there.
            Err(e)
                if e.kind() == ErrorKind::AlreadyExists
                    || fs::symlink_metadata(&path).is_ok() =>
            {
                Ok(false)
            }
            Err(e) => Err(LakeError::Io {
                context: format!("publishing {}", path.display()),
                source: e,
            }),
        }
    }

    /// Create `rel` as a unique temp file for a writer that fills it in place
    /// (the engine's `PinkDb::save`): `create_new(true)` so a pre-existing
    /// symlink or temp file at that name is never followed or truncated.
    /// The caller passes a fresh-id name; a collision (or a planted file at
    /// exactly that 128-bit-random name) fails closed.
    pub(crate) fn claim_tmp(&self, rel: &str) -> Result<()> {
        let path = self.abs(rel);
        self.ensure_parent(&path)?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map(|_| ())
            .map_err(|e| LakeError::Io {
                context: format!("claiming temp file {}", path.display()),
                source: e,
            })
    }

    /// Publish a file another writer produced at `rel_tmp` (the engine's
    /// `PinkDb::save` writes plain bytes): fsync it, rename it over
    /// `rel_final`, then a best-effort parent-directory fsync. Both are
    /// lake-relative and share a directory.
    pub(crate) fn publish_tmp(&self, rel_tmp: &str, rel_final: &str) -> Result<()> {
        let tmp = self.abs(rel_tmp);
        let final_path = self.abs(rel_final);
        let synced = OpenOptions::new()
            .write(true)
            .open(&tmp)
            .and_then(|f| f.sync_all());
        if let Err(e) = synced {
            let _ = fs::remove_file(&tmp);
            return Err(LakeError::Io {
                context: format!("fsyncing {}", tmp.display()),
                source: e,
            });
        }
        fs::rename(&tmp, &final_path).map_err(|e| LakeError::Io {
            context: format!("publishing {}", final_path.display()),
            source: e,
        })?;
        self.sync_parent(&final_path);
        Ok(())
    }

    /// File names directly inside `rel` (sorted; a missing directory reads as
    /// empty). Subdirectories and non-UTF-8 names are skipped.
    pub(crate) fn list(&self, rel: &str) -> Result<Vec<String>> {
        let path = self.abs(rel);
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(LakeError::Io {
                    context: format!("listing {}", path.display()),
                    source: e,
                })
            }
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| LakeError::Io {
                context: format!("listing {}", path.display()),
                source: e,
            })?;
            if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                if let Some(name) = entry.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| LakeError::Io {
                context: format!("creating directory {}", parent.display()),
                source: e,
            })?;
        }
        Ok(())
    }

    /// Best-effort `fsync` of the directory holding `path`, so a rename
    /// survives a *process* crash. Errors are ignored: the rename has already
    /// happened, and on APFS a directory fsync does not flush the drive's own
    /// write cache anyway (see the crate docs on durability).
    fn sync_parent(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
}

/// Write `bytes` and fsync the file (the durability half of a publication).
fn write_and_sync(file: &mut File, bytes: &[u8]) -> std::io::Result<()> {
    file.write_all(bytes)?;
    file.sync_all()
}

/// `<name>.<uuid>.tmp` next to `path` (same directory, so renames and hard
/// links stay atomic). The id makes the name unique per write, so concurrent
/// writers never share a temp path.
fn unique_tmp(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(format!(".{}.tmp", fresh_id()));
    path.with_file_name(name)
}

/// Create a fresh unique temp sibling of `path` with `create_new(true)`.
/// A collision with an existing (possibly planted) name is retried with a
/// new id; anything else is an I/O error.
fn create_unique_tmp(path: &Path) -> std::io::Result<(PathBuf, File)> {
    for _ in 0..16 {
        let tmp = unique_tmp(path);
        match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => return Ok((tmp, file)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::new(
        ErrorKind::AlreadyExists,
        "could not create a unique temp file",
    ))
}
