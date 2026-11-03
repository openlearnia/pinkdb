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

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use super::storage::{Head, LocalCopy, ObjectInfo, Storage};
use crate::{LakeError, Result, fresh_id};

/// How many data files [`LocalStorage`] keeps open at once.
const OPEN_HANDLES: usize = 64;

/// Directory-and-file I/O rooted at one lake directory.
#[derive(Debug)]
pub(crate) struct LocalStorage {
    root: PathBuf,
    /// Open data files by resolved path. A lake data file is immutable — a
    /// change writes a new file — so a handle stays valid as long as it is
    /// cached; the map is bounded and cleared when full.
    open: Mutex<HashMap<PathBuf, Arc<File>>>,
    /// How many files this storage opened (a test observes handle reuse).
    #[cfg(test)]
    opens: std::sync::atomic::AtomicU64,
}

impl LocalStorage {
    pub(crate) fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            open: Mutex::new(HashMap::new()),
            #[cfg(test)]
            opens: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The open handle for `path`, opening it on first use.
    fn handle(&self, path: &Path, context: &str) -> Result<Arc<File>> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(file) = open.get(path) {
            return Ok(Arc::clone(file));
        }
        let file = Arc::new(File::open(path).map_err(|e| LakeError::Io {
            context: context.to_string(),
            source: e,
        })?);
        if open.len() >= OPEN_HANDLES {
            open.clear();
        }
        open.insert(path.to_path_buf(), Arc::clone(&file));
        #[cfg(test)]
        self.opens
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(file)
    }

    /// Drop a cached handle so the next read opens the file again.
    fn forget(&self, path: &Path) {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(path);
    }

    /// How many files this storage opened (a test observes handle reuse).
    #[cfg(test)]
    fn opens(&self) -> u64 {
        self.opens.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Resolve `rel` below the lake root, rejecting anything that `join`
    /// would let escape: an absolute path (which replaces the root) or a
    /// `..`/prefix/current-directory component. Empty stays the root, which
    /// is the trait's documented `abs("")` spelling.
    fn checked_path(&self, rel: &str) -> Result<PathBuf> {
        if rel.is_empty() {
            return Ok(self.root.clone());
        }
        let plain = Path::new(rel)
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
        if !plain {
            return Err(LakeError::Corrupt {
                path: PathBuf::from(rel),
                message: format!(
                    "lake path {rel:?} must stay below the lake root (no absolute paths, \
                     `..`, `.` or drive prefixes)"
                ),
            });
        }
        Ok(self.root.join(rel))
    }

    fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            create_dir_all_private(parent).map_err(|e| LakeError::Io {
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
    /// data files; the snapshot path refuses to replace an existing
    /// version.
    fn write_atomic(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let path = self.checked_path(rel)?;
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
}

/// Best-effort narrow one lake-owned file to 0600 and a directory to 0700.
fn tighten(path: &Path, is_dir: bool) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if is_dir { 0o700 } else { 0o600 };
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
    }
    #[cfg(not(unix))]
    {
        let _ = (path, is_dir);
    }
}

/// Narrow a local catalog database and its WAL/lock siblings to 0600.
pub(crate) fn tighten_catalog(path: &Path) {
    tighten(path, false);
    for suffix in [".wal", ".lock"] {
        let sibling = PathBuf::from(format!("{}{suffix}", path.display()));
        if sibling.exists() {
            tighten(&sibling, false);
        }
    }
}

impl Storage for LocalStorage {
    /// Display-only resolution: `abs` cannot return an error, so a hostile
    /// path resolves to a fixed name *below* the root rather than escaping it
    /// (`Path::join` would replace the root for an absolute path). Every I/O
    /// method uses [`LocalStorage::checked_path`] and returns the real error.
    fn abs(&self, rel: &str) -> PathBuf {
        self.checked_path(rel)
            .unwrap_or_else(|_| self.root.join(".pinkdb-invalid-rel"))
    }

    fn head(&self, rel: &str) -> Result<Head> {
        let path = self.checked_path(rel)?;
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
        let path = self.checked_path(rel)?;
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
        let path = self.checked_path(rel)?;
        fs::read(&path).map_err(|e| LakeError::Io {
            context: context.to_string(),
            source: e,
        })
    }

    fn read_range(&self, rel: &str, offset: u64, len: u64, context: &str) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let path = self.checked_path(rel)?;
        let mut bytes = vec![
            0u8;
            usize::try_from(len).map_err(|_| LakeError::Io {
                context: context.to_string(),
                source: std::io::Error::other(format!(
                    "byte range length {len} does not fit this platform"
                )),
            })?
        ];
        let failed = |e| LakeError::Io {
            context: format!(
                "{} (byte range {offset}..{} of {})",
                context,
                offset.saturating_add(len),
                path.display()
            ),
            source: e,
        };
        let file = self.handle(&path, context)?;
        match read_at(&file, &mut bytes, offset) {
            Ok(()) => Ok(bytes),
            // A cached handle whose read failed (the file was replaced under
            // it, or the filesystem dropped it) is dropped and reopened once.
            Err(_) => {
                self.forget(&path);
                let file = self.handle(&path, context)?;
                read_at(&file, &mut bytes, offset).map_err(failed)?;
                Ok(bytes)
            }
        }
    }

    fn ranged_reads(&self) -> bool {
        true
    }

    fn create_dir(&self, rel: &str) -> Result<()> {
        let path = self.checked_path(rel)?;
        create_dir_all_private(&path).map_err(|e| LakeError::Io {
            context: format!("creating directory {}", path.display()),
            source: e,
        })
    }

    fn tighten_existing(&self) -> Result<()> {
        let root = self.root.join("data");
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            tighten(&dir, true);
            for entry in entries.flatten() {
                let path = entry.path();
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    stack.push(path);
                } else {
                    tighten(&path, false);
                }
            }
        }
        Ok(())
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
        let path = self.checked_path(rel)?;
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

    /// File names directly inside `rel` (sorted; a missing directory reads as
    /// empty). Subdirectories and non-UTF-8 names are skipped.
    fn list(&self, rel: &str) -> Result<Vec<String>> {
        let path = self.checked_path(rel)?;
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
        let path = self.checked_path(rel)?;
        fs::remove_file(&path).map_err(|e| LakeError::Io {
            context: format!("deleting {}", path.display()),
            source: e,
        })
    }

    /// Every file under `rel` (any depth), as `/`-joined paths relative to
    /// `rel`, sorted; symlinked directories are not followed (a symlink to a
    /// file counts as a file, exactly as `list` treats it).
    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        let path = self.checked_path(rel)?;
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
        Ok(LocalCopy::Path(self.checked_path(rel)?))
    }

    fn local_path(&self, rel: &str) -> Option<PathBuf> {
        // A hostile relative path has no local path; callers fall back to the
        // generic read path, which rejects it with a clean error.
        self.checked_path(rel).ok()
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
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        // M-5: lake artifacts (manifests, data files, commits) are owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&tmp) {
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

/// Read `bytes` at `offset` without moving the file's position, so one cached
/// handle serves concurrent readers.
#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(bytes, offset)
}

#[cfg(not(unix))]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = file;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)
}

/// Create `path` and missing parents with mode 0700. Existing directories are
/// left exactly as their owner made them.
fn create_dir_all_private(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    let mut missing = Vec::new();
    let mut cursor = path;
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(std::io::Error::new(
                        ErrorKind::AlreadyExists,
                        format!("{} is not a directory", cursor.display()),
                    ));
                }
                break;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                missing.push(cursor.to_path_buf());
                match cursor.parent() {
                    Some(parent) if !parent.as_os_str().is_empty() => cursor = parent,
                    _ => break,
                }
            }
            Err(e) => return Err(e),
        }
    }
    for dir in missing.into_iter().rev() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_path_rejects_escapes_and_accepts_data_paths() {
        let root = PathBuf::from("/tmp/pinkdb-lake-root");
        let storage = LocalStorage::new(&root);

        for hostile in ["../escape", "/etc/x", "data/../../escape"] {
            let err = storage.checked_path(hostile).unwrap_err();
            assert!(
                err.to_string().contains("must stay below the lake root"),
                "{hostile}: {err}"
            );
            // The infallible display path never leaves the root either.
            assert!(storage.abs(hostile).starts_with(&root), "{hostile}");
            assert_ne!(storage.abs(hostile), root.join(hostile));
        }

        assert_eq!(storage.checked_path("").unwrap(), root);
        assert_eq!(
            storage.checked_path("data/main.t/abc.pink").unwrap(),
            root.join("data/main.t/abc.pink")
        );
        assert_eq!(
            storage.abs("data/main.t/abc.pink"),
            root.join("data/main.t/abc.pink")
        );
    }

    #[test]
    fn range_reads_reuse_one_open_handle() {
        let root = std::env::temp_dir().join(format!("pinkdb-handles-{}", fresh_id()));
        fs::create_dir_all(&root).unwrap();
        let storage = LocalStorage::new(&root);
        let bytes: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        storage.put("data/main.t/a.pink", &bytes).unwrap();
        for (start, len) in [(0, 8u64), (1, 7), (100, 32), (4088, 8)] {
            assert_eq!(
                storage
                    .read_range("data/main.t/a.pink", start, len, "test")
                    .unwrap(),
                bytes[start as usize..(start + len) as usize]
            );
            assert_eq!(
                storage.opens(),
                1,
                "range read {start} opened the file again"
            );
        }
        storage.put("data/main.t/b.pink", b"other").unwrap();
        assert_eq!(
            storage
                .read_range("data/main.t/b.pink", 0, 5, "test")
                .unwrap(),
            b"other"
        );
        assert_eq!(storage.opens(), 2, "a second file gets its own handle");
        fs::remove_dir_all(&root).unwrap();
    }

    /// A missing file still reports exactly what a whole-file read reports;
    /// nothing is cached for it.
    #[test]
    fn a_missing_file_still_errors_with_the_io_context() {
        let root = std::env::temp_dir().join(format!("pinkdb-missing-{}", fresh_id()));
        fs::create_dir_all(&root).unwrap();
        let storage = LocalStorage::new(&root);
        let context = "reading a data file";
        let ranged = storage
            .read_range("data/main.t/gone.pink", 0, 4, context)
            .unwrap_err()
            .to_string();
        let whole = storage
            .read("data/main.t/gone.pink", context)
            .unwrap_err()
            .to_string();
        assert_eq!(ranged, whole, "a range read reports what a whole read does");
        assert!(
            ranged.starts_with("lake I/O error (reading a data file): "),
            "{ranged}"
        );
        assert_eq!(storage.opens(), 0, "a failed open cached a handle");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn storage_io_refuses_hostile_paths_without_touching_the_parent() {
        let root = std::env::temp_dir().join(format!("pinkdb-local-{}", fresh_id()));
        fs::create_dir_all(&root).unwrap();
        let storage = LocalStorage::new(&root);
        let outside = root
            .parent()
            .unwrap()
            .join(format!("escape-{}", fresh_id()));

        assert!(storage.read("../escape", "test").is_err());
        assert!(storage.head("/etc/x").is_err());
        assert!(storage.put("../escape", b"must not be written").is_err());
        assert!(storage.create_dir("..").is_err());
        assert!(!outside.exists(), "a hostile path escaped the lake root");

        // A normal lake-relative path still works.
        storage.put("data/main.t/ok.pink", b"ok").unwrap();
        assert_eq!(storage.read("data/main.t/ok.pink", "test").unwrap(), b"ok");

        fs::remove_dir_all(&root).unwrap();
    }
}
