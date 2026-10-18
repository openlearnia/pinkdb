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
//!
//! The implementation of the crate-internal [`Storage`] trait: this is the
//! original wave-34/35 directory behavior, unchanged (same files, same
//! atomicity, same errors) — the trait is what wave 36 adds for S3.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use super::storage::{Head, LocalCopy, ObjectInfo, Storage, VersionTag};
use crate::{LakeError, Result, fresh_id};

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
        if let Some(parent) = path.parent()
            && let Ok(dir) = File::open(parent)
        {
            let _ = dir.sync_all();
        }
    }

    /// Write `bytes` to `rel` atomically, replacing any previous file:
    /// a unique `<name>.<uuid>.tmp` sibling, fsync, rename over `rel`, then
    /// a best-effort parent-directory fsync. A crash before the rename
    /// leaves only an unreferenced `.tmp` file. Used by the catalog and by
    /// v2 Vortex data files; the snapshot path refuses to replace an
    /// existing version.
    fn write_atomic(&self, rel: &str, bytes: &[u8]) -> Result<()> {
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
    fn publish_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
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
                if e.kind() == ErrorKind::AlreadyExists || fs::symlink_metadata(&path).is_ok() =>
            {
                Ok(false)
            }
            Err(e) => Err(LakeError::Io {
                context: format!("publishing {}", path.display()),
                source: e,
            }),
        }
    }
}

/// The local file's identity: byte length + modification time (nanoseconds
/// since the Unix epoch; a clock before the epoch or an unsupported
/// `modified()` reads as 0). This is the local stand-in for an S3 `ETag`:
/// `put_if_match` re-stats the file and refuses when it changed while a save
/// was in flight. A concurrent writer that moves `catalog.json` forward is
/// already rejected by the version re-check before this point; the tag closes
/// the tiny check-then-replace window (S3 closes it exactly with `If-Match`).
fn local_tag(path: &Path) -> Result<VersionTag> {
    let meta = fs::metadata(path).map_err(|e| LakeError::Io {
        context: format!("reading metadata of {}", path.display()),
        source: e,
    })?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Ok(VersionTag(format!("file:{}:{}", meta.len(), mtime)))
}

impl Storage for LocalStorage {
    fn abs(&self, rel: &str) -> PathBuf {
        if rel.is_empty() {
            self.root.clone()
        } else {
            self.root.join(rel)
        }
    }

    fn head(&self, rel: &str) -> Result<Head> {
        let path = self.abs(rel);
        // `fs::metadata` follows symlinks, exactly as `Path::is_file` did.
        match fs::metadata(&path) {
            Ok(meta) if meta.is_file() => Ok(Head::File),
            Ok(_) => Ok(Head::NotAFile),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Head::Missing),
            Err(e) => Err(LakeError::Io {
                context: format!("reading metadata of {}", path.display()),
                source: e,
            }),
        }
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        let path = self.abs(rel);
        match fs::metadata(&path) {
            Ok(meta) if meta.is_file() => {
                let modified_micros = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_micros().min(i64::MAX as u128) as i64);
                Ok(Some(ObjectInfo {
                    size: meta.len(),
                    modified_micros,
                }))
            }
            Ok(_) => Ok(None),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(LakeError::Io {
                context: format!("reading metadata of {}", path.display()),
                source: e,
            }),
        }
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        let path = self.abs(rel);
        fs::read(&path).map_err(|e| LakeError::Io {
            context: context.to_string(),
            source: e,
        })
    }

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        let bytes = self.read(rel, context)?;
        let tag = local_tag(&self.abs(rel))?;
        Ok((bytes, tag))
    }

    fn create_dir(&self, rel: &str) -> Result<()> {
        let path = self.abs(rel);
        fs::create_dir_all(&path).map_err(|e| LakeError::Io {
            context: format!("creating directory {}", path.display()),
            source: e,
        })
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        self.write_atomic(rel, bytes)
    }

    /// Stream into a unique temp sibling through a 1 MiB buffer, fsync,
    /// rename into place, fsync the parent — [`LocalStorage::write_atomic`]
    /// without ever holding the whole file in memory.
    fn put_streamed(
        &self,
        rel: &str,
        fill: &mut dyn FnMut(&mut (dyn Write + Send)) -> Result<()>,
    ) -> Result<u64> {
        let path = self.abs(rel);
        self.ensure_parent(&path)?;
        let (tmp, file) = create_unique_tmp(&path).map_err(|e| LakeError::Io {
            context: format!("creating a temp file next to {}", path.display()),
            source: e,
        })?;
        let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
        let filled = fill(&mut out);
        let synced = filled.and_then(|()| {
            let file = out.into_inner().map_err(|e| LakeError::Io {
                context: format!("writing {}", tmp.display()),
                source: e.into_error(),
            })?;
            file.sync_all()
                .and_then(|()| file.metadata().map(|m| m.len()))
                .map_err(|e| LakeError::Io {
                    context: format!("writing {}", tmp.display()),
                    source: e,
                })
        });
        let len = match synced {
            Ok(len) => len,
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                return Err(e);
            }
        };
        fs::rename(&tmp, &path).map_err(|e| LakeError::Io {
            context: format!("publishing {}", path.display()),
            source: e,
        })?;
        self.sync_parent(&path);
        Ok(len)
    }

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        self.publish_new(rel, bytes)
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        let current = local_tag(&self.abs(rel))?;
        if &current != expected {
            return Ok(false);
        }
        self.write_atomic(rel, bytes)?;
        Ok(true)
    }

    /// File names directly inside `rel` (sorted; a missing directory reads as
    /// empty). Subdirectories and non-UTF-8 names are skipped.
    fn list(&self, rel: &str) -> Result<Vec<String>> {
        let path = self.abs(rel);
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(LakeError::Io {
                    context: format!("listing {}", path.display()),
                    source: e,
                });
            }
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| LakeError::Io {
                context: format!("listing {}", path.display()),
                source: e,
            })?;
            if entry.file_type().map(|t| t.is_file()).unwrap_or(false)
                && let Some(name) = entry.file_name().to_str()
            {
                out.push(name.to_string());
            }
        }
        out.sort();
        Ok(out)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        let path = self.abs(rel);
        fs::remove_file(&path).map_err(|e| LakeError::Io {
            context: format!("deleting {}", path.display()),
            source: e,
        })
    }

    /// Every file under `rel` (any depth), as `/`-joined paths relative to
    /// `rel`, sorted; symlinked directories are not followed (a symlink to a
    /// file counts as a file, exactly as `list` treats it).
    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        let path = self.abs(rel);
        let mut out = Vec::new();
        let mut stack: Vec<(PathBuf, String)> = vec![(path.clone(), String::new())];
        while let Some((dir, prefix)) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(LakeError::Io {
                        context: format!("listing {}", dir.display()),
                        source: e,
                    });
                }
            };
            for entry in entries {
                let entry = entry.map_err(|e| LakeError::Io {
                    context: format!("listing {}", dir.display()),
                    source: e,
                })?;
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let child = entry.path();
                let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
                let joined = if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{name}")
                };
                if is_dir {
                    stack.push((child, joined));
                } else if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    out.push(joined);
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn local_copy(&self, rel: &str, _context: &str) -> Result<LocalCopy> {
        Ok(LocalCopy::Path(self.abs(rel)))
    }

    fn local_path(&self, rel: &str) -> Option<PathBuf> {
        Some(self.abs(rel))
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
