//! The internal storage abstraction behind a lake.
//!
//! A lake needs eight operations and nothing more: read one object, read with
//! a version tag, create-if-absent, replace, list the files under a prefix,
//! list everything under a prefix recursively, stat one object, and delete.
//! [`LocalStorage`](crate::local::LocalStorage) is the original directory
//! implementation (wave 34/35, unchanged files/atomicity/errors); the
//! feature-gated S3 backend implements the same operations over the S3 REST
//! API (AWS S3, Cloudflare R2, MinIO, …).
//!
//! # Positioned reads and transport counters (wave 98b)
//!
//! [`Storage::read_range`] reads one byte window: a positioned file read
//! locally, an S3 `Range` GET over object storage (same retry policy). The
//! encryption decorator refuses ranges of a sealed object
//! ([`Storage::ranged_reads`] is `false` there) because a `PNKLAKE1`
//! container has no plaintext offsets; callers fall back to a whole read.
//! [`Storage::stats`] reports requests, bytes and network wait so the lake can
//! show exactly what an attach or query read. [`Storage::read_optional`] is
//! one GET on object storage (absence is a 404), while the default keeps the
//! local HEAD-then-read behavior (including the "not a regular file"
//! refusal).
//!
//! Single-writer safety lives in the metadata store's head check inside its
//! transaction (see `metadata`); the storage layer only has to make each
//! object write atomic ([`Storage::put`]).

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::crypto::{self, Crypto};
use crate::{LakeError, Result};

/// What [`Storage::head`] found at a lake-relative path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Head {
    /// Nothing is there.
    Missing,
    /// A regular file (local) or an object (S3).
    File,
    /// Something exists but is not a regular file: a local directory, fifo,
    /// device, … Object storage never reports this (a `dir/` key is still an
    /// object).
    NotAFile,
}

/// What [`Storage::stat`] found: one stored object's size and last-modified
/// time.
///
/// `size` is the **stored** object's byte length, so for an encrypted lake it
/// is the ciphertext size (the container framing adds a plaintext header, a
/// key-check block and per-1-MiB-frame tags; see the `crypto` module). The
/// modification time is the backend's own (`mtime` locally, the object's
/// `Last-Modified` over S3), in microseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObjectInfo {
    pub(crate) size: u64,
    pub(crate) modified_micros: Option<i64>,
}

/// Transport totals of one storage backend, for diagnostics (wave 98b):
/// requests issued, response bytes received, and wall time spent waiting on
/// the network. Local storage reports zeros.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StorageStats {
    pub(crate) requests: u64,
    pub(crate) bytes: u64,
    pub(crate) wait_nanos: u64,
}

/// A local path to one lake object.
///
/// Local storage hands out the file itself; object storage downloads the
/// object into a unique temp file (engine `.pink` files must be opened by
/// path, and the v1 loader is the only user of this). The temp copy is
/// removed when the value is dropped.
#[derive(Debug)]
pub(crate) enum LocalCopy {
    /// The storage's own file; nothing to clean up.
    Path(PathBuf),
    /// A staging temp copy (decrypted plaintext or a downloaded object) inside
    /// a private directory, removed on drop (only constructed by object
    /// storage, so it is dead code in a default build).
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    Temp {
        /// The staged file the caller reads.
        file: PathBuf,
        /// The private (0700) directory holding it, removed on drop.
        dir: PathBuf,
    },
}

impl LocalCopy {
    pub(crate) fn path(&self) -> &Path {
        match self {
            LocalCopy::Path(path) => path,
            LocalCopy::Temp { file, .. } => file,
        }
    }
}

impl Drop for LocalCopy {
    fn drop(&mut self) {
        if let LocalCopy::Temp { dir, .. } = self {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Create a private (0700, owner-only) staging directory and return it with a
/// file path inside it. Decrypted/downloaded plaintext lives here only for as
/// long as the engine loads it; other local users cannot read it and the
/// directory is removed when the [`LocalCopy`] drops.
pub(crate) fn private_temp_dir() -> std::io::Result<(PathBuf, PathBuf)> {
    sweep_once();
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-lake-dec-{}-{}",
        std::process::id(),
        crate::fresh_id()
    ));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&dir)?;
    let file = dir.join("copy.pink");
    Ok((dir, file))
}

/// Write `bytes` to `file` with owner-only (0600) permissions, never following
/// a pre-existing path (`create_new`).
pub(crate) fn write_private(file: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut f = options.open(file)?;
    std::io::Write::write_all(&mut f, bytes)
}

/// Sweep [`sweep_stale_temp_dirs`] once per process (the first staging call).
fn sweep_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(sweep_stale_temp_dirs);
}

/// Best-effort: remove staging directories left behind by a crashed process.
/// A live directory is removed on [`LocalCopy`] drop and is short-lived, so
/// only directories older than an hour are swept.
fn sweep_stale_temp_dirs() {
    sweep_dir(&std::env::temp_dir(), std::time::Duration::from_secs(3600));
}

/// Remove `pinkdb-lake-dec-*` directories under `base` older than `min_age`.
fn sweep_dir(base: &Path, min_age: std::time::Duration) {
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    let Some(cutoff) = std::time::SystemTime::now().checked_sub(min_age) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with("pinkdb-lake-dec-")
        {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .map(|t| t < cutoff)
            .unwrap_or(false);
        if stale {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// One backend of lake objects: a local directory or an S3 bucket+prefix.
///
/// All paths are lake-relative (`data/<table>/<uuid>.pink`); [`Storage::abs`]
/// turns one into the backend's absolute form for error messages.
///
/// `Send + Sync` because one handle is shared (`Arc`) with an attached-lake
/// handle the engine may read through from more than one thread.
pub(crate) trait Storage: fmt::Debug + Send + Sync {
    /// The absolute form of a lake-relative path: a filesystem path locally,
    /// an `s3://bucket/prefix/rel` URL for S3. `abs("")` is the lake root.
    fn abs(&self, rel: &str) -> PathBuf;

    /// What is at `rel` (missing / a regular file / something else).
    fn head(&self, rel: &str) -> Result<Head>;

    /// One object's size and last-modified time: `None` when nothing is
    /// there. Used by lake maintenance (the `cleanup_old_files` /
    /// `delete_orphaned_files` age gate and the `.lake files` / `.lake
    /// table_info` listings). A directory (or any non-file) reads as `None`.
    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>>;

    /// Read one object. `context` names the failed operation in the same
    /// style as the local backend (`reading <abs>`, `loading data file "…"`).
    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>>;

    /// Read one object, or `None` when it does not exist. Object storage
    /// answers this with one GET (a 404 is the absence); the default checks
    /// `head` first, preserving the local backend's "not a regular file"
    /// refusal.
    fn read_optional(&self, rel: &str, context: &str) -> Result<Option<Vec<u8>>> {
        match self.head(rel)? {
            Head::Missing => Ok(None),
            Head::NotAFile => Err(LakeError::Corrupt {
                path: self.abs(rel),
                message: format!("{} is not a regular file", self.abs(rel).display()),
            }),
            Head::File => Ok(Some(self.read(rel, context)?)),
        }
    }

    /// Read a byte window of one object: `offset..offset+len`. The default
    /// falls back to a whole read (and slices); the local and S3 backends
    /// implement positioned reads. Callers gate on [`Storage::ranged_reads`]
    /// first, so a backend that cannot do better never pays for the fallback
    /// twice.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    fn read_range(&self, rel: &str, offset: u64, len: u64, context: &str) -> Result<Vec<u8>> {
        let bytes = self.read(rel, context)?;
        slice_range(&bytes, offset, len, &self.abs(rel))
    }

    /// Whether [`Storage::read_range`] is a real positioned read (no whole
    /// object behind it). Object storage and local plaintext files answer
    /// `true`; encrypted sessions answer `false` (a sealed container must be
    /// unsealed whole before any plaintext range exists).
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    fn ranged_reads(&self) -> bool {
        false
    }

    /// The backend's transport totals (wave 98b diagnostics). Local storage
    /// and wrappers report zeros unless they issue network requests.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    fn stats(&self) -> StorageStats {
        StorageStats::default()
    }

    /// Create the directory `rel` (and parents) — a no-op for object
    /// storage, which has no directories.
    #[allow(dead_code)]
    fn create_dir(&self, rel: &str) -> Result<()>;

    /// Atomically replace one object: locally a unique fsynced temp file
    /// renamed into place; over S3 a single `PUT` (S3 object writes are
    /// atomic — a reader sees the old or the new bytes, never a mix).
    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()>;

    /// [`Storage::put`] for content produced by `fill` into a sink, with the
    /// same atomicity; returns the bytes written. The default buffers the
    /// whole object (object storage needs it for one PUT); the local backend
    /// streams it straight into its fsynced temp file.
    #[allow(dead_code)]
    fn put_streamed(
        &self,
        rel: &str,
        fill: &mut dyn FnMut(&mut (dyn std::io::Write + Send)) -> Result<()>,
    ) -> Result<u64> {
        let mut bytes: Vec<u8> = Vec::new();
        fill(&mut bytes)?;
        self.put(rel, &bytes)?;
        Ok(bytes.len() as u64)
    }

    /// File names directly under `rel`, sorted. A missing location reads as
    /// empty; subdirectories are not listed.
    #[allow(dead_code)]
    fn list(&self, rel: &str) -> Result<Vec<String>>;

    /// Every *file* under `rel`, recursively, as paths **relative to
    /// `rel`** (`"<subdir>/<name>"`, `/`-separated and sorted). A missing
    /// location reads as empty. Unlike [`Storage::list`] (the manifest-facing
    /// direct-children listing) this is the object inventory maintenance
    /// scans for unreferenced data objects (`.lake files --all`,
    /// `cleanup_old_files`, `delete_orphaned_files`); it returns names only,
    /// never directories.
    fn list_recursive(&self, rel: &str) -> Result<Vec<String>>;

    /// Delete one object. Published versions are never mutated (every
    /// retained version stays loadable), so only maintenance touches this:
    /// `cleanup_old_files` / `delete_orphaned_files` remove `data/` objects no
    /// retained version references (snapshot expiration only deletes catalog
    /// rows). A missing object is a backend-specific answer (S3 DELETE
    /// is idempotent; locally it is a `NotFound` I/O error the maintenance
    /// code treats as already-deleted).
    fn delete(&self, rel: &str) -> Result<()>;

    /// A local path for one object (see [`LocalCopy`]). Only the v1 `.pink`
    /// loader uses this: the engine opens files by path.
    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy>;

    /// The object's real filesystem path, when the backend is local: a
    /// projected read opens a data file with positioned reads through this.
    /// Object storage answers `None` (no ranged reads in this build), and the
    /// caller falls back to reading the whole object.
    #[allow(dead_code)]
    fn local_path(&self, _rel: &str) -> Option<PathBuf> {
        None
    }

    /// Whether anything exists at `rel` (a directory counts, mirroring
    /// `Path::exists`).
    fn exists(&self, rel: &str) -> Result<bool> {
        Ok(!matches!(self.head(rel)?, Head::Missing))
    }

    /// Whether this storage seals every object at rest (per-file-key lake).
    fn encrypted(&self) -> bool {
        false
    }

    /// The random 256-bit key one just-written data/delete file was sealed
    /// with.
    #[allow(dead_code)]
    fn object_key(&self, _rel: &str) -> Option<Vec<u8>> {
        None
    }

    /// Best-effort narrow pre-existing artifacts to 0600 (M-5). The local
    /// backend overrides this; object storage has no file modes.
    #[allow(dead_code)]
    fn tighten_existing(&self) -> Result<()> {
        Ok(())
    }
}

/// One object's byte window from a whole buffer (the fallback for a backend
/// without positioned reads). A window past the end is a clean error naming
/// the object, never a short slice.
fn slice_range(bytes: &[u8], offset: u64, len: u64, path: &Path) -> Result<Vec<u8>> {
    let start = usize::try_from(offset).map_err(|_| LakeError::Corrupt {
        path: path.to_path_buf(),
        message: format!("byte range offset {offset} does not fit this platform"),
    })?;
    let end = offset
        .checked_add(len)
        .and_then(|end| usize::try_from(end).ok())
        .ok_or_else(|| LakeError::Corrupt {
            path: path.to_path_buf(),
            message: format!("byte range {offset}..+{len} overflows this platform"),
        })?;
    if end > bytes.len() {
        return Err(LakeError::Corrupt {
            path: path.to_path_buf(),
            message: format!(
                "byte range {offset}..{end} is past the {}-byte object",
                bytes.len()
            ),
        });
    }
    Ok(bytes[start..end].to_vec())
}

/// The lake-relative-path → container domain/binding mapping every encrypted
/// object is sealed and opened under (see the `crypto` module docs).
fn crypto_domain(rel: &str) -> (crypto::Domain, &str) {
    if rel.ends_with(".pinkdel") || rel.ends_with("-delete.parquet") {
        (crypto::Domain::DeleteFile, rel)
    } else {
        (crypto::Domain::DataFile, rel)
    }
}

/// Keys of files this process just wrote, keyed by lake-relative path.
/// A keyed SQL lake's commit writes the key into the file's catalog row; the
/// path (a fresh UUID per file) is unique, so one map for the process is
/// enough. A file written by another process gets its key from the catalog on
/// read (the resolver below), never from here.
static WRITTEN_KEYS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, [u8; 32]>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// The per-file key a data/delete file was just sealed with.
pub(crate) fn written_file_key(rel: &str) -> Option<Vec<u8>> {
    WRITTEN_KEYS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(rel)
        .copied()
        .map(|key| key.to_vec())
}

/// Per-file-key encryption for a keyed SQL catalog (wave 103 T2).
///
/// DuckLake protects each data/delete file with its own random 256-bit key and
/// keeps the key in that file's catalog row; the metadata rows stay plaintext
/// (the catalog database's own protection is the operator's job — for a
/// `pink:` catalog the engine's `PINKENC1` at-rest encryption). This decorator
/// wraps only the *data* half of a split lake: it seals every written object in
/// a `PNKLAKE1` container with KDF id 0 (a raw key, no passphrase derivation),
/// records the key for the commit, and unseals on read with the key resolved
/// from the catalog. A keyless session never sees these objects (the catalog's
/// `encrypted` row refuses it before a data read).
/// The key resolver and cache of a keyed SQL lake (see [`FileKeyStorage`]).
type KeyResolver = Arc<dyn Fn(&str) -> Option<[u8; 32]> + Send + Sync>;
type KeyCache = std::sync::Mutex<std::collections::HashMap<String, [u8; 32]>>;

pub(crate) struct FileKeyStorage {
    inner: Arc<dyn Storage>,
    /// Looks a file's key up in the catalog after a reopen. `None` while a
    /// just-created lake has no catalog row yet.
    resolver: KeyResolver,
    cache: KeyCache,
}

impl fmt::Debug for FileKeyStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileKeyStorage")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl FileKeyStorage {
    pub(crate) fn new(inner: Arc<dyn Storage>, resolver: KeyResolver) -> Self {
        Self {
            inner,
            resolver,
            cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn key_for(&self, rel: &str) -> Option<[u8; 32]> {
        if let Some(key) = self
            .cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(rel)
            .copied()
        {
            return Some(key);
        }
        let key = (self.resolver)(rel)?;
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(rel.to_string(), key);
        Some(key)
    }

    fn seal(&self, rel: &str, plain: &[u8]) -> Result<Vec<u8>> {
        let mut key = [0u8; 32];
        getrandom::getrandom(&mut key).map_err(|e| LakeError::Io {
            context: format!("generating the per-file encryption key for {rel}"),
            source: std::io::Error::other(e.to_string()),
        })?;
        let crypto = Crypto::new(crypto::EncryptionKey::from_raw(key))?;
        let (domain, binding) = crypto_domain(rel);
        let sealed = crypto.seal(&self.inner.abs(rel), domain, binding, plain)?;
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(rel.to_string(), key);
        WRITTEN_KEYS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(rel.to_string(), key);
        Ok(sealed)
    }

    fn open(&self, rel: &str, stored: &[u8]) -> Result<Vec<u8>> {
        if !crypto::is_container(stored) {
            return Err(crypto::not_encrypted_error(&self.inner.abs(rel)));
        }
        let key = self
            .key_for(rel)
            .ok_or_else(|| crypto::encrypted_error(&self.inner.abs(rel)))?;
        let crypto = Crypto::new(crypto::EncryptionKey::from_raw(key))?;
        let (domain, binding) = crypto_domain(rel);
        crypto.open(&self.inner.abs(rel), domain, binding, stored)
    }
}

impl Storage for FileKeyStorage {
    fn abs(&self, rel: &str) -> PathBuf {
        self.inner.abs(rel)
    }

    fn head(&self, rel: &str) -> Result<Head> {
        self.inner.head(rel)
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        self.inner.stat(rel)
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        let stored = self.inner.read(rel, context)?;
        self.open(rel, &stored)
    }

    fn read_optional(&self, rel: &str, context: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.read_optional(rel, context)? {
            Some(stored) => self.open(rel, &stored).map(Some),
            None => Ok(None),
        }
    }

    fn read_range(&self, rel: &str, _offset: u64, _len: u64, context: &str) -> Result<Vec<u8>> {
        Err(LakeError::Internal {
            message: format!(
                "the encrypted data file {rel} has no plaintext ranges ({context}); read it \
                 whole so the container authenticates"
            ),
        })
    }

    fn ranged_reads(&self) -> bool {
        false
    }

    fn create_dir(&self, rel: &str) -> Result<()> {
        self.inner.create_dir(rel)
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let sealed = self.seal(rel, bytes)?;
        self.inner.put(rel, &sealed)
    }

    fn put_streamed(
        &self,
        rel: &str,
        fill: &mut dyn FnMut(&mut (dyn std::io::Write + Send)) -> Result<()>,
    ) -> Result<u64> {
        let mut plain = Vec::new();
        fill(&mut plain)?;
        let sealed = self.seal(rel, &plain)?;
        self.inner.put(rel, &sealed)?;
        Ok(sealed.len() as u64)
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        self.inner.list(rel)
    }

    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        self.inner.list_recursive(rel)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(rel);
        self.inner.delete(rel)
    }

    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy> {
        let bytes = self.read(rel, context)?;
        let (dir, path) = private_temp_dir().map_err(|e| LakeError::Io {
            context: format!("creating a private temp dir for the decrypted copy of {rel}"),
            source: e,
        })?;
        if let Err(e) = write_private(&path, &bytes) {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(LakeError::Io {
                context: format!("writing the decrypted temp copy {}", path.display()),
                source: e,
            });
        }
        Ok(LocalCopy::Temp { file: path, dir })
    }

    fn local_path(&self, _rel: &str) -> Option<PathBuf> {
        None
    }

    fn exists(&self, rel: &str) -> Result<bool> {
        self.inner.exists(rel)
    }

    fn encrypted(&self) -> bool {
        true
    }

    fn object_key(&self, rel: &str) -> Option<Vec<u8>> {
        self.key_for(rel).map(|key| key.to_vec())
    }

    #[allow(dead_code)]
    fn tighten_existing(&self) -> Result<()> {
        self.inner.tighten_existing()
    }
}

/// A parsed lake location: a local directory path, an `s3://` URL, or a SQL
/// catalog URL (`pink:<path>` / `sqlite:<path>` / `postgres://…`; wave 96a and
/// wave 103). A catalog location names only the metadata store — the data
/// location is recorded in the catalog itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Location {
    Local(PathBuf),
    S3 {
        bucket: String,
        prefix: String,
    },
    /// A pinkdb catalog database whose checkpoint and WAL records live in S3.
    CatalogS3 {
        bucket: String,
        prefix: String,
    },
    /// A durable pinkdb `.pink` catalog (`pink:<path>`).
    Pink(PathBuf),
    Sqlite(PathBuf),
    Postgres(String),
}

impl Location {
    /// Parse a lake location string: `s3://bucket[/prefix…]` is object
    /// storage, `pink:<path>` is a durable pinkdb catalog, `sqlite:<path>` is
    /// a SQLite catalog, `postgres://…` / `postgresql://…` is a Postgres
    /// catalog URL and `postgres:<conninfo>` its libpq key=value form, anything
    /// else is a local path (a directory literally named `s3://…` is not a
    /// thing). A bare path keeps its current meaning: the object-store JSON
    /// directory layout.
    pub(crate) fn parse(location: &str) -> Result<Self> {
        if let Some(rest) = location.strip_prefix("pink+s3://") {
            return Self::parse_catalog_s3(location, rest);
        }
        if let Some(rest) = location.strip_prefix("pink:") {
            return Self::parse_pink(location, rest);
        }
        if let Some(rest) = location.strip_prefix("sqlite:") {
            return Self::parse_sqlite(location, rest);
        }
        if location.starts_with("postgres:") || location.starts_with("postgresql://") {
            crate::catalog_store::url::PostgresUrl::parse(location).map_err(|message| {
                LakeError::BadLocation {
                    location: crate::catalog_store::url::redact_location(location),
                    message,
                }
            })?;
            return Ok(Location::Postgres(location.to_string()));
        }
        match location.strip_prefix("s3://") {
            Some(rest) => Self::parse_s3(location, rest),
            None => Ok(Location::Local(PathBuf::from(location))),
        }
    }

    fn parse_pink(original: &str, rest: &str) -> Result<Self> {
        // A lake catalog must persist: an in-memory database would vanish with
        // the handle. pinkdb's `:memory:` is not a `.pink` file location.
        if rest.is_empty() || rest == ":memory:" {
            return Err(LakeError::BadLocation {
                location: original.to_string(),
                message: "a lake catalog must persist; `pink::memory:` is not a lake location \
                          (use `pink:<path>`)"
                    .to_string(),
            });
        }
        Ok(Location::Pink(PathBuf::from(rest)))
    }

    fn parse_sqlite(original: &str, rest: &str) -> Result<Self> {
        // A lake catalog must persist: an in-memory database would vanish with
        // the handle. Refused here, before any backend is constructed.
        if rest.is_empty() || rest == ":memory:" {
            return Err(LakeError::BadLocation {
                location: original.to_string(),
                message: "a lake catalog must persist; `sqlite::memory:` is not a lake location \
                          (use `sqlite:<path>`)"
                    .to_string(),
            });
        }
        Ok(Location::Sqlite(PathBuf::from(rest)))
    }

    fn parse_s3(original: &str, rest: &str) -> Result<Self> {
        let (bucket, prefix) = Self::parse_s3_parts(original, rest)?;
        Ok(Location::S3 { bucket, prefix })
    }

    fn parse_catalog_s3(original: &str, rest: &str) -> Result<Self> {
        let (bucket, prefix) = Self::parse_s3_parts(original, rest)?;
        Ok(Location::CatalogS3 { bucket, prefix })
    }

    fn parse_s3_parts(original: &str, rest: &str) -> Result<(String, String)> {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix.trim_end_matches('/')),
            None => (rest, ""),
        };
        validate_bucket_name(bucket).map_err(|message| LakeError::BadLocation {
            location: original.to_string(),
            message,
        })?;
        validate_prefix(prefix, original)?;
        Ok((bucket.to_string(), prefix.to_string()))
    }
}

/// The S3 bucket-name shape this client accepts: 3–63 bytes of lowercase
/// ASCII letters, digits, `.` and `-` (the DNS-compatible object-storage
/// rule; shared by [`Location::parse`] and the `s3` feature's `S3Config`, so
/// a URL and an explicit config reject exactly the same names).
pub(crate) fn validate_bucket_name(bucket: &str) -> std::result::Result<(), String> {
    let characters_ok = bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.');
    if (3..=63).contains(&bucket.len()) && characters_ok {
        Ok(())
    } else {
        Err(
            "the bucket name must be 3–63 characters of lowercase ASCII letters, digits, \
             `.` and `-` (expected s3://bucket/prefix)"
                .to_string(),
        )
    }
}

/// A key prefix: no leading `/` (trailing slashes are normalized away).
pub(crate) fn validate_prefix(prefix: &str, original: &str) -> Result<()> {
    if prefix.starts_with('/') {
        return Err(LakeError::BadLocation {
            location: original.to_string(),
            message: "the prefix must not start with `/`".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paths_pass_through() {
        assert_eq!(
            Location::parse("mylake").unwrap(),
            Location::Local(PathBuf::from("mylake"))
        );
        assert_eq!(
            Location::parse("/tmp/mylake").unwrap(),
            Location::Local(PathBuf::from("/tmp/mylake"))
        );
        // `s3:` without the full scheme is a local path.
        assert_eq!(
            Location::parse("s3:not-a-url").unwrap(),
            Location::Local(PathBuf::from("s3:not-a-url"))
        );
    }

    #[test]
    fn s3_urls_parse_bucket_and_prefix() {
        let cases = [
            ("s3://bucket", "bucket", ""),
            ("s3://bucket/", "bucket", ""),
            ("s3://bucket//", "bucket", ""),
            ("s3://bucket/prefix", "bucket", "prefix"),
            ("s3://bucket/a/b/c", "bucket", "a/b/c"),
            ("s3://bucket/a/b/c/", "bucket", "a/b/c"),
            ("s3://my.bucket-1/x", "my.bucket-1", "x"),
        ];
        for (url, bucket, prefix) in cases {
            assert_eq!(
                Location::parse(url).unwrap(),
                Location::S3 {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string()
                },
                "{url}"
            );
        }
    }

    #[test]
    fn catalog_s3_urls_are_distinct_from_data_locations() {
        assert_eq!(
            Location::parse("pink+s3://bucket/lake").unwrap(),
            Location::CatalogS3 {
                bucket: "bucket".to_string(),
                prefix: "lake".to_string(),
            }
        );
        assert_ne!(
            Location::parse("pink+s3://bucket/lake").unwrap(),
            Location::parse("s3://bucket/lake").unwrap()
        );
    }

    #[test]
    fn bad_s3_urls_are_clear_errors() {
        let long_bucket = "b".repeat(64);
        let too_long = format!("s3://{long_bucket}/x");
        for bad in [
            "s3://",
            "s3:///prefix",
            "s3://bucket//x",
            "s3://bad_bucket/x",
            "s3://b ck/x",
            // Uppercase is not a valid S3 bucket name (shared with S3Config).
            "s3://UPPER/x",
            // Shorter than 3 bytes.
            "s3://ab/x",
        ] {
            let err = Location::parse(bad).unwrap_err();
            assert!(
                matches!(err, LakeError::BadLocation { .. }),
                "{bad}: {err:?}"
            );
            assert!(err.to_string().contains("invalid lake location"), "{err}");
        }
        let err = Location::parse(&too_long).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    }

    #[test]
    fn staging_copies_are_owner_only_and_removed_on_drop() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, file) = private_temp_dir().unwrap();
        write_private(&file, b"top-secret plaintext").unwrap();
        let dir_mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "temp dir mode {dir_mode:o}");
        let file_mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "temp file mode {file_mode:o}");
        {
            let copy = LocalCopy::Temp {
                file: file.clone(),
                dir: dir.clone(),
            };
            assert_eq!(copy.path(), file.as_path());
            assert!(file.exists());
        }
        assert!(!dir.exists(), "the private temp dir must go on drop");
    }

    /// A staging directory left by a crashed process (older than an hour) is
    /// swept; a fresh one is left alone.
    #[cfg(unix)]
    #[test]
    fn stale_staging_dirs_are_swept_fresh_ones_kept() {
        // A private base dir keeps this independent of the other tests in this
        // binary (which stage under the shared system temp dir).
        let base = std::env::temp_dir().join(format!(
            "pinkdb-sweep-test-{}-{}",
            std::process::id(),
            crate::fresh_id()
        ));
        let stale = base.join("pinkdb-lake-dec-1-stale");
        let fresh = base.join("pinkdb-lake-dec-2-fresh");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("copy.pink"), b"leftover plaintext").unwrap();
        std::fs::create_dir_all(&fresh).unwrap();

        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
        std::fs::File::open(&stale)
            .unwrap()
            .set_modified(old)
            .unwrap();

        sweep_dir(&base, std::time::Duration::from_secs(3600));
        assert!(!stale.exists(), "a stale staging dir must be swept");
        assert!(fresh.exists(), "a fresh staging dir must be kept");
        let _ = std::fs::remove_dir_all(&base);
    }
}
