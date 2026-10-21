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
use std::io::{ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
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
    /// v2 Vortex data files; the snapshot path refuses to replace an
    /// existing version.
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

    /// Publish `bytes` under `rel` **only if `rel` does not exist yet**:
    /// write a unique fsynced `.tmp`, then hard-link it to `rel` (an atomic
    /// create-if-absent), then remove the temp name. Returns `Ok(false)` when
    /// `rel` already existed — nothing was published and the temp file is
    /// gone. This is how snapshots (and the create-time v1 pair) are
    /// published: a published version file is never replaced, and two
    /// concurrent writers cannot both claim the same version.
    fn publish_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
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

/// The local file's identity: `(dev, ino)` where available, else zeroes —
/// the same primitive the engine's WAL uses. Combined with byte length and
/// modification time, this is the local stand-in for an S3 `ETag`:
/// `put_if_match` re-stats the file and refuses when it changed while a save
/// was in flight. `(length, mtime)` alone is not an identity: a same-length
/// replacement inside one timestamp tick (HFS+, FAT, some NFS mounts) would
/// share a tag. The device/inode pair distinguishes the inode incarnation.
#[cfg(unix)]
fn identity_of(meta: &fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (meta.dev(), meta.ino())
}

#[cfg(not(unix))]
fn identity_of(_meta: &fs::Metadata) -> (u64, u64) {
    (0, 0)
}

fn tag_of(meta: &fs::Metadata) -> VersionTag {
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let (dev, ino) = identity_of(meta);
    VersionTag(format!("file:{}:{}:{}:{}", meta.len(), mtime, dev, ino))
}

fn local_tag(path: &Path) -> Result<VersionTag> {
    let meta = fs::metadata(path).map_err(|e| LakeError::Io {
        context: format!("reading metadata of {}", path.display()),
        source: e,
    })?;
    Ok(tag_of(&meta))
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

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        // M-12: one `open`, then `fstat` and `read` on that *same* fd. Two
        // syscalls on the path could pair old bytes with a replacement's tag
        // (or the reverse); the conditional catalog replace would then
        // clobber a concurrent writer.
        let path = self.checked_path(rel)?;
        let mut file = File::open(&path).map_err(|e| LakeError::Io {
            context: context.to_string(),
            source: e,
        })?;
        let meta = file.metadata().map_err(|e| LakeError::Io {
            context: format!("reading metadata of {}", path.display()),
            source: e,
        })?;
        let tag = tag_of(&meta);
        #[cfg(test)]
        run_read_hook(&path);
        let mut bytes = Vec::with_capacity(meta.len().min(64 << 20) as usize);
        file.read_to_end(&mut bytes).map_err(|e| LakeError::Io {
            context: context.to_string(),
            source: e,
        })?;
        Ok((bytes, tag))
    }

    fn create_dir(&self, rel: &str) -> Result<()> {
        let path = self.checked_path(rel)?;
        create_dir_all_private(&path).map_err(|e| LakeError::Io {
            context: format!("creating directory {}", path.display()),
            source: e,
        })
    }

    fn tighten_existing(&self) -> Result<()> {
        // M-5 migration: a lake created before the 0600 policy left its
        // catalog, manifests, commits and data files world-readable. Narrow
        // every engine-owned artifact here; a permission error is ignored
        // (the lake stays usable) and a user-supplied shared parent is never
        // touched because only files inside the lake root are visited.
        for rel in [CATALOG_FILE, crate::LAKE_FILE, crate::HIGHEST_FILE] {
            tighten_file(&self.checked_path(rel)?);
        }
        for dir in ["snapshots", "commits"] {
            if let Ok(entries) = fs::read_dir(self.checked_path(dir)?) {
                for entry in entries.flatten() {
                    tighten_file(&entry.path());
                }
            }
        }
        if let Ok(files) = self.list_recursive("data") {
            for rel in files {
                if let Ok(path) = self.checked_path(&format!("data/{rel}")) {
                    tighten_file(&path);
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

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        self.publish_new(rel, bytes)
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        let current = local_tag(&self.checked_path(rel)?)?;
        if &current != expected {
            return Ok(false);
        }
        self.write_atomic(rel, bytes)?;
        Ok(true)
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

/// Test-only seam: run `hook` after `fstat` and before the read in
/// [`LocalStorage::read_tagged`], so a test can deterministically replace the
/// path while the caller holds the old fd (M-12).
#[cfg(test)]
type ReadHook = Box<dyn Fn(&Path) + Send>;

#[cfg(test)]
static READ_HOOK: std::sync::Mutex<Option<ReadHook>> = std::sync::Mutex::new(None);

#[cfg(test)]
fn run_read_hook(path: &Path) {
    let hook = READ_HOOK.lock().unwrap_or_else(|p| p.into_inner()).take();
    if let Some(hook) = hook {
        hook(path);
    }
}

/// Best-effort narrow one lake-owned file to 0600.
fn tighten_file(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Ok(meta) = fs::symlink_metadata(path) else {
            return;
        };
        if !meta.is_file() {
            return;
        }
        if meta.permissions().mode() & 0o077 != 0 {
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
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
    /// M-12: the tag includes `(dev, ino)`, so a same-length replacement with
    /// a preserved mtime is still distinguishable.
    #[cfg(unix)]
    #[test]
    fn tags_distinguish_same_length_same_mtime_replacements() {
        use std::fs::FileTimes;

        let root = std::env::temp_dir().join(format!("pinkdb-local-tag-{}", fresh_id()));
        fs::create_dir_all(&root).unwrap();
        let storage = LocalStorage::new(&root);
        storage.put("data/t/a.pink", b"AAAA").unwrap();
        let (old_bytes, old_tag) = storage.read_tagged("data/t/a.pink", "test").unwrap();
        assert_eq!(old_bytes, b"AAAA");
        let old_mtime = fs::metadata(root.join("data/t/a.pink"))
            .unwrap()
            .modified()
            .unwrap();

        // Same length, same mtime, new inode (the atomic replace).
        storage.put("data/t/a.pink", b"BBBB").unwrap();
        let file = fs::OpenOptions::new()
            .write(true)
            .open(root.join("data/t/a.pink"))
            .unwrap();
        file.set_times(FileTimes::new().set_modified(old_mtime))
            .unwrap();
        drop(file);
        let (new_bytes, new_tag) = storage.read_tagged("data/t/a.pink", "test").unwrap();
        assert_eq!(new_bytes, b"BBBB");
        assert_ne!(
            old_tag.0, new_tag.0,
            "a same-length/same-mtime replacement must not share a tag"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// M-12: replacing the path after `fstat` cannot make the returned
    /// `(bytes, tag)` pair inconsistent — both come from the open fd.
    #[test]
    fn read_tagged_is_consistent_when_the_path_is_replaced_mid_read() {
        let root = std::env::temp_dir().join(format!("pinkdb-local-race-{}", fresh_id()));
        fs::create_dir_all(&root).unwrap();
        let storage = std::sync::Arc::new(LocalStorage::new(&root));
        storage.put("catalog.json", b"old-catalog").unwrap();
        let (_, old_tag) = storage.read_tagged("catalog.json", "test").unwrap();

        let replacer = std::sync::Arc::clone(&storage);
        *READ_HOOK.lock().unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move |_path| {
            replacer.put("catalog.json", b"new-catalog").unwrap();
        }));
        let (bytes, tag) = storage.read_tagged("catalog.json", "test").unwrap();
        assert_eq!(bytes, b"old-catalog", "the read must use the opened fd");
        assert_eq!(tag.0, old_tag.0, "the tag must describe the same fd");
        assert_eq!(
            fs::read(root.join("catalog.json")).unwrap(),
            b"new-catalog",
            "the replacement really happened"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// M-12: a stale tag is rejected by the conditional replace, so a
    /// concurrent writer's catalog is never clobbered.
    #[test]
    fn stale_tag_is_rejected_by_conditional_replace() {
        let root = std::env::temp_dir().join(format!("pinkdb-local-stale-{}", fresh_id()));
        fs::create_dir_all(&root).unwrap();
        let storage = LocalStorage::new(&root);
        storage.put("catalog.json", b"first").unwrap();
        let (_, stale) = storage.read_tagged("catalog.json", "test").unwrap();
        storage.put("catalog.json", b"second").unwrap();
        assert!(
            !storage
                .put_if_match("catalog.json", b"third", &stale)
                .unwrap()
        );
        assert_eq!(
            fs::read(root.join("catalog.json")).unwrap(),
            b"second",
            "a stale conditional replace must not write"
        );
        let _ = fs::remove_dir_all(&root);
    }
}
