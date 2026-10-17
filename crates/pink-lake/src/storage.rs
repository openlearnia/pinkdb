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
//! Two operations carry the single-writer contract:
//!
//! - [`Storage::put_new`] publishes a snapshot manifest **only if it does not
//!   exist yet** (`If-None-Match: *` over S3; a hard link locally), so two
//!   writers racing for one version cannot both win.
//! - [`Storage::put_if_match`] replaces `catalog.json` **only if it still
//!   carries the version tag read moments earlier** (`If-Match: <etag>` over
//!   S3, HTTP 412 → conflict; a file-identity re-check locally). The lake
//!   additionally re-parses the catalog right before this call, so a stale
//!   handle fails with the familiar "another writer published version N"
//!   error even on backends that cannot enforce conditional writes.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::crypto::{self, Crypto};
use crate::local::CATALOG_FILE;
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

/// An opaque storage version tag.
///
/// For S3 this is the object's `ETag` **including its quotes** (sent back
/// verbatim as `If-Match: <etag>`); for local files it is the file's identity
/// (byte length + modification time in nanoseconds). A tag is captured by
/// [`Storage::read_tagged`] when the lake reads a manifest and is required to
/// still match when the same manifest is replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionTag(pub(crate) String);

impl VersionTag {
    /// The tag as it goes on the wire (an S3 `If-Match` value).
    #[cfg(feature = "s3")]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
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
    /// A downloaded temp copy, removed on drop (only constructed by object
    /// storage, so it is dead code in a default build).
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    Temp(PathBuf),
}

impl LocalCopy {
    pub(crate) fn path(&self) -> &Path {
        match self {
            LocalCopy::Path(path) | LocalCopy::Temp(path) => path,
        }
    }
}

impl Drop for LocalCopy {
    fn drop(&mut self) {
        if let LocalCopy::Temp(path) = self {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// One backend of lake objects: a local directory or an S3 bucket+prefix.
///
/// All paths are lake-relative (`catalog.json`, `snapshots/<v>.json`,
/// `data/<table>/<uuid>.vortex`); [`Storage::abs`] turns one into the
/// backend's absolute form for error messages.
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

    /// [`Storage::read`] plus the version tag for a later conditional
    /// replace. The tag must come from the same read the caller reasons
    /// about.
    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)>;

    /// Create the directory `rel` (and parents) — a no-op for object
    /// storage, which has no directories.
    fn create_dir(&self, rel: &str) -> Result<()>;

    /// Atomically replace one object: locally a unique fsynced temp file
    /// renamed into place; over S3 a single `PUT` (S3 object writes are
    /// atomic — a reader sees the old or the new bytes, never a mix).
    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()>;

    /// [`Storage::put`] for content produced by `fill` into a sink, with the
    /// same atomicity; returns the bytes written. The default buffers the
    /// whole object (object storage needs it for one PUT); the local backend
    /// streams it straight into its fsynced temp file. Used by the
    /// `vortex-format` writer; a lean build's v1 writer puts whole buffers.
    #[cfg_attr(not(feature = "vortex-format"), allow(dead_code))]
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

    /// Publish `bytes` **only if `rel` does not exist yet**: `Ok(false)` when
    /// it already existed (locally a hard link, so an existing file or
    /// planted symlink is never replaced; over S3 `If-None-Match: *` →
    /// HTTP 412).
    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool>;

    /// Replace `rel` **only if it still carries `expected`**: `Ok(false)`
    /// when the tag changed (over S3 `If-Match: <etag>`; locally the file
    /// identity is re-checked). A concurrent writer's change is never
    /// silently clobbered.
    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool>;

    /// File names directly under `rel`, sorted. A missing location reads as
    /// empty; subdirectories are not listed.
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
    /// `expire_snapshots` removes *expired* `snapshots/<v>.json` and their
    /// `commits/<v>.json`, and `cleanup_old_files` /
    /// `delete_orphaned_files` remove `data/` objects no retained version
    /// references. A missing object is a backend-specific answer (S3 DELETE
    /// is idempotent; locally it is a `NotFound` I/O error the maintenance
    /// code treats as already-deleted).
    fn delete(&self, rel: &str) -> Result<()>;

    /// A local path for one object (see [`LocalCopy`]). Only the v1 `.pink`
    /// loader uses this: the engine opens files by path.
    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy>;

    /// The object's real filesystem path, when the backend is local: the
    /// footer-only attach opens a data file with positioned reads through
    /// this. Object storage answers `None` (no ranged reads in this build),
    /// and the caller falls back to reading the whole object. Used by the
    /// `vortex-format` (attach/Vortex) read paths only.
    #[cfg_attr(not(feature = "vortex-format"), allow(dead_code))]
    fn local_path(&self, _rel: &str) -> Option<PathBuf> {
        None
    }

    /// Whether anything exists at `rel` (a directory counts, mirroring
    /// `Path::exists`).
    fn exists(&self, rel: &str) -> Result<bool> {
        Ok(!matches!(self.head(rel)?, Head::Missing))
    }

    /// Restore `snapshots/<version>.json` to the authoritative
    /// `catalog.json` bytes after a degraded (check-then-put) race left a
    /// *published* snapshot replaced: catalog and snapshot are byte-identical
    /// by construction, so the catalog bytes are the repair. `bytes` is the
    /// catalog manifest the caller just read. The local backend and
    /// conditional-write endpoints have nothing to repair and do nothing.
    fn reconcile_snapshot(&self, _version: u64, _bytes: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Whether this backend is currently using the explicit degraded
    /// check-then-put fallback (only possible over S3, and only after the
    /// endpoint rejected conditional writes in opt-in mode). The lake then
    /// verifies the catalog replace and repairs the snapshot pair; local
    /// storage and conditional-write endpoints answer `false`.
    fn uses_degraded_fallback(&self) -> bool {
        false
    }

    /// Whether this storage seals every object at rest (an encrypted lake
    /// session). The default is `false`; the encryption decorator overrides
    /// it. Used for reporting (`.lake attach`) and for the attach layer's
    /// whole-object-read accounting — never to change what is read.
    fn encrypted(&self) -> bool {
        false
    }

    /// Seal one version's manifest **once**, so the exact ciphertext bytes
    /// can be published as both `snapshots/<version>.json` and
    /// `catalog.json` — the byte-identity invariant every published version
    /// keeps. A plaintext backend (or a keyless session) returns the bytes
    /// unchanged. The decorator's write methods recognize the container magic
    /// and store already-sealed bytes as-is, so this is the one seam where
    /// "seal once, publish twice" is expressed.
    fn seal_manifest(&self, plain: &[u8]) -> Result<Vec<u8>> {
        Ok(plain.to_vec())
    }

    /// [`Storage::read`] plus the object's **stored** bytes as the backend
    /// holds them (raw ciphertext for an encrypted session). Only the create
    /// adoption path uses it: when a concurrent create won the
    /// `snapshots/1.json` link, the catalog must be published from the
    /// snapshot's *exact stored bytes*, not from a fresh encryption (which
    /// would mint a different container and break the byte-identity
    /// invariant). The plaintext copy is what the caller validates.
    fn read_with_stored(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let bytes = self.read(rel, context)?;
        Ok((bytes.clone(), bytes))
    }
}

/// The lake-relative-path → container domain/binding mapping every encrypted
/// object is sealed and opened under (see the `crypto` module docs).
fn crypto_domain(rel: &str) -> (crypto::Domain, &str) {
    if rel == CATALOG_FILE || rel.starts_with("snapshots/") {
        // Manifests are bound to nothing path-like: catalog.json and
        // snapshots/<v>.json are the same bytes by construction.
        (crypto::Domain::Manifest, "")
    } else if rel.starts_with("commits/") {
        (crypto::Domain::Commit, rel)
    } else if rel.ends_with(".pinkdel") {
        (crypto::Domain::DeleteFile, rel)
    } else {
        (crypto::Domain::DataFile, rel)
    }
}

/// The at-rest encryption decorator: the single choke point every lake object
/// passes through, so commits, deletes, time travel, attach and the retry
/// paths all get encryption without knowing it.
///
/// - **Keyed session** (`crypto.is_some()`): every object the lake writes
///   (that is not already a `PNKLAKE1` container — the once-per-version
///   manifest `seal_manifest` mints) is sealed on the way to the backend, and
///   every object it reads is authenticated and unsealed first. A keyed read
///   of a plaintext object fails closed (`NotEncrypted`): a key is never
///   silently ignored.
/// - **Keyless session** (`crypto.is_none()`): reads and writes pass through
///   unchanged — *except* that a read of a `PNKLAKE1` container fails closed
///   with [`LakeError::Encrypted`] (or the "rebuild with the encryption
///   feature" hint in a build without it), never a JSON parse error.
///
/// Branch/conditional semantics are untouched: `put_new` and `put_if_match`
/// evaluate their conditions on the *sealed* bytes exactly as they would on
/// plaintext (the ciphertext is the object). `read_tagged` returns the
/// backend's version tag for the stored object — the caller's later
/// conditional replace still reasons about the object it actually read.
#[derive(Debug)]
pub(crate) struct CryptoStorage {
    inner: Arc<dyn Storage>,
    crypto: Option<Arc<Crypto>>,
}

impl CryptoStorage {
    /// Wrap `inner`. `key = None` is a plaintext session (the decorator still
    /// recognizes containers to fail closed); `Some(key)` is a keyed session
    /// — and in a build without the `encryption` feature this is the clear
    /// "not compiled in" error, before any storage is touched.
    pub(crate) fn new(inner: Arc<dyn Storage>, key: Option<crypto::EncryptionKey>) -> Result<Self> {
        let crypto = match key {
            Some(key) => Some(Arc::new(Crypto::new(key)?)),
            None => None,
        };
        Ok(Self { inner, crypto })
    }

    /// The plaintext of one stored object: unsealed (and authenticated) for a
    /// keyed session, as-is for a keyless one. Both directions fail closed —
    /// see the type docs.
    fn plain(&self, rel: &str, stored: Vec<u8>) -> Result<Vec<u8>> {
        let path = self.inner.abs(rel);
        match &self.crypto {
            Some(crypto) => {
                if !crypto::is_container(&stored) {
                    return Err(crypto::not_encrypted_error(&path));
                }
                let (domain, binding) = crypto_domain(rel);
                crypto.open(&path, domain, binding, &stored)
            }
            None => {
                if crypto::is_container(&stored) {
                    return Err(crypto::encrypted_error(&path));
                }
                Ok(stored)
            }
        }
    }

    /// The stored bytes of one object the lake is about to write: sealed for
    /// a keyed session — unless the bytes already are a `PNKLAKE1` container
    /// (the once-per-version manifest, or the snapshot bytes a create adopted
    /// for the catalog), which are published as-is so the shared bytes stay
    /// byte-identical.
    fn sealed(&self, rel: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        match &self.crypto {
            None => Ok(bytes.to_vec()),
            Some(crypto) => {
                if crypto::is_container(bytes) {
                    return Ok(bytes.to_vec());
                }
                let path = self.inner.abs(rel);
                let (domain, binding) = crypto_domain(rel);
                crypto.seal(&path, domain, binding, bytes)
            }
        }
    }
}

impl Storage for CryptoStorage {
    fn abs(&self, rel: &str) -> PathBuf {
        self.inner.abs(rel)
    }

    fn head(&self, rel: &str) -> Result<Head> {
        self.inner.head(rel)
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        // The stored object's facts (ciphertext size for an encrypted
        // session): maintenance reasons about *objects*, never plaintext.
        self.inner.stat(rel)
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        let stored = self.inner.read(rel, context)?;
        self.plain(rel, stored)
    }

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        let (stored, tag) = self.inner.read_tagged(rel, context)?;
        Ok((self.plain(rel, stored)?, tag))
    }

    fn read_with_stored(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let stored = self.inner.read(rel, context)?;
        let plain = self.plain(rel, stored.clone())?;
        Ok((plain, stored))
    }

    fn create_dir(&self, rel: &str) -> Result<()> {
        self.inner.create_dir(rel)
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put(rel, &self.sealed(rel, bytes)?)
    }

    fn put_streamed(
        &self,
        rel: &str,
        fill: &mut dyn FnMut(&mut (dyn std::io::Write + Send)) -> Result<()>,
    ) -> Result<u64> {
        match &self.crypto {
            None => self.inner.put_streamed(rel, fill),
            Some(crypto) => {
                let path = self.inner.abs(rel);
                let (domain, binding) = crypto_domain(rel);
                self.inner.put_streamed(rel, &mut |sink| {
                    let mut sealed = crypto.sealing_writer(&path, domain, binding, sink)?;
                    fill(&mut sealed)?;
                    sealed.finish()
                })
            }
        }
    }

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        self.inner.put_new(rel, &self.sealed(rel, bytes)?)
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        self.inner
            .put_if_match(rel, &self.sealed(rel, bytes)?, expected)
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        self.inner.list(rel)
    }

    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        self.inner.list_recursive(rel)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        self.inner.delete(rel)
    }

    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy> {
        match &self.crypto {
            None => {
                let copy = self.inner.local_copy(rel, context)?;
                // Fail closed before the engine opens a ciphertext file as if
                // it were a `.pink` one: the first 8 bytes are the magic.
                if let Some(head) = read_magic(copy.path()) {
                    if &head == crypto::ENC_MAGIC {
                        return Err(crypto::encrypted_error(&self.inner.abs(rel)));
                    }
                }
                Ok(copy)
            }
            Some(_) => {
                // The engine opens `.pink` files by path, so unseal to a
                // unique temp copy the caller drops when done; the plaintext
                // exists on disk only for as long as the engine takes to load
                // it. (Stage 1: whole-file. The engine's loader holds the
                // whole store in memory anyway, so no streaming is lost.)
                let bytes = self.read(rel, context)?;
                let path =
                    std::env::temp_dir().join(format!("pink-lake-dec-{}.pink", crate::fresh_id()));
                let written = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .and_then(|mut file| std::io::Write::write_all(&mut file, &bytes));
                if let Err(e) = written {
                    let _ = std::fs::remove_file(&path);
                    return Err(LakeError::Io {
                        context: format!("writing the decrypted temp copy {}", path.display()),
                        source: e,
                    });
                }
                Ok(LocalCopy::Temp(path))
            }
        }
    }

    fn local_path(&self, rel: &str) -> Option<PathBuf> {
        match &self.crypto {
            // Encrypted objects are containers: a positioned footer read of
            // the ciphertext is meaningless. `None` makes the Vortex attach
            // paths read the whole object through `read` (unsealed first) —
            // the documented stage-1 cost; footer-range reads over
            // decrypted segments are the follow-up.
            Some(_) => None,
            None => self.inner.local_path(rel),
        }
    }

    fn reconcile_snapshot(&self, version: u64, bytes: &[u8]) -> Result<()> {
        if !self.inner.uses_degraded_fallback() {
            return self.inner.reconcile_snapshot(version, bytes);
        }
        match &self.crypto {
            None => self.inner.reconcile_snapshot(version, bytes),
            Some(_) => {
                // The repair must republish the catalog's *stored* bytes (the
                // caller's `bytes` are the decrypted copy; sealing them again
                // would mint a different container and break
                // catalog == snapshot).
                let context = format!("reading {}", self.inner.abs(CATALOG_FILE).display());
                let stored = self.inner.read(CATALOG_FILE, &context)?;
                let stored = if crypto::is_container(&stored) {
                    stored
                } else {
                    bytes.to_vec()
                };
                self.inner.reconcile_snapshot(version, &stored)
            }
        }
    }

    fn uses_degraded_fallback(&self) -> bool {
        self.inner.uses_degraded_fallback()
    }

    fn encrypted(&self) -> bool {
        self.crypto.is_some()
    }

    fn seal_manifest(&self, plain: &[u8]) -> Result<Vec<u8>> {
        match &self.crypto {
            None => Ok(plain.to_vec()),
            Some(crypto) => {
                let path = self.inner.abs(CATALOG_FILE);
                crypto.seal(&path, crypto::Domain::Manifest, "", plain)
            }
        }
    }
}

/// The first 8 bytes of a file, when it has that many.
fn read_magic(path: &Path) -> Option<[u8; 8]> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).ok()?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic).ok()?;
    Some(magic)
}

/// A parsed lake location: a local directory path or an `s3://` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Location {
    Local(PathBuf),
    S3 { bucket: String, prefix: String },
}

impl Location {
    /// Parse a lake location string: `s3://bucket[/prefix…]` is object
    /// storage; anything else is a local path (a directory literally named
    /// `s3://…` is not a thing).
    pub(crate) fn parse(location: &str) -> Result<Self> {
        match location.strip_prefix("s3://") {
            Some(rest) => Self::parse_s3(location, rest),
            None => Ok(Location::Local(PathBuf::from(location))),
        }
    }

    fn parse_s3(original: &str, rest: &str) -> Result<Self> {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix.trim_end_matches('/')),
            None => (rest, ""),
        };
        validate_bucket_name(bucket).map_err(|message| LakeError::BadLocation {
            location: original.to_string(),
            message,
        })?;
        validate_prefix(prefix, original)?;
        Ok(Location::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        })
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

    /// A keyless session never hands out ciphertext: a `PNKLAKE1` object is
    /// the "open it with a key" error (or the rebuild hint in a build without
    /// the `encryption` feature), never raw bytes.
    #[test]
    fn keyless_reads_of_containers_fail_closed() {
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_crypto_storage_{}_{}",
            std::process::id(),
            crate::fresh_id()
        ));
        std::fs::create_dir_all(dir.join("data/t")).unwrap();
        std::fs::write(dir.join("catalog.json"), b"PNKLAKE1 not for you").unwrap();
        let inner: Arc<dyn Storage> = Arc::new(crate::local::LocalStorage::new(&dir));
        let storage = CryptoStorage::new(inner, None).unwrap();

        let err = storage
            .read(CATALOG_FILE, "reading catalog.json")
            .unwrap_err();
        assert!(matches!(err, LakeError::Encrypted { .. }), "{err:?}");
        assert!(err.to_string().contains("the lake is encrypted"), "{err}");
        #[cfg(not(feature = "encryption"))]
        assert!(
            err.to_string().contains("encryption"),
            "a feature-off build must name the missing feature: {err}"
        );

        // A keyless `seal_manifest` is the identity (plaintext lakes keep
        // byte-identical manifests) and `local_copy` refuses the container
        // before the engine could open it as a `.pink` file.
        assert_eq!(storage.seal_manifest(b"{\"v\":1}").unwrap(), b"{\"v\":1}");
        let err = storage
            .local_copy(CATALOG_FILE, "loading catalog.json")
            .unwrap_err();
        assert!(matches!(err, LakeError::Encrypted { .. }), "{err:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A keyed session seals and unseals every object, and the "seal once per
    /// version" contract: `seal_manifest` output published to both objects is
    /// byte-identical (the decorator stores already-sealed bytes as-is).
    #[cfg(feature = "encryption")]
    #[test]
    fn keyed_storage_seals_once_and_reads_back_through_every_path() {
        use crate::crypto::EncryptionKey;

        let dir = std::env::temp_dir().join(format!(
            "pink_lake_crypto_keyed_{}_{}",
            std::process::id(),
            crate::fresh_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let inner: Arc<dyn Storage> = Arc::new(crate::local::LocalStorage::new(&dir));
        let storage = CryptoStorage::new(
            inner,
            Some(EncryptionKey::from_passphrase("storage-level test key")),
        )
        .unwrap();
        assert!(storage.encrypted());

        // The manifest pair: sealed once, published twice, byte-identical.
        let plain = b"{\n  \"format_version\": 4\n}\n";
        let sealed = storage.seal_manifest(plain).unwrap();
        assert!(sealed.starts_with(b"PNKLAKE1"));
        assert!(storage.put_new("snapshots/1.json", &sealed).unwrap());
        assert!(storage.put_new(CATALOG_FILE, &sealed).unwrap());
        assert_eq!(
            std::fs::read(dir.join(CATALOG_FILE)).unwrap(),
            std::fs::read(dir.join("snapshots/1.json")).unwrap()
        );
        assert_ne!(std::fs::read(dir.join(CATALOG_FILE)).unwrap(), plain);
        assert_eq!(storage.read(CATALOG_FILE, "reading").unwrap(), plain);
        let (plain_read, stored) = storage.read_with_stored(CATALOG_FILE, "reading").unwrap();
        assert_eq!(plain_read, plain);
        assert_eq!(stored, sealed);

        // Plaintext writes are sealed on the way through (data files, delete
        // files, commit deltas), and reads unseal them.
        storage.put("data/t/x.pink", b"data bytes").unwrap();
        let raw = std::fs::read(dir.join("data/t/x.pink")).unwrap();
        assert!(raw.starts_with(b"PNKLAKE1"), "the data file must be sealed");
        assert_eq!(
            storage.read("data/t/x.pink", "reading").unwrap(),
            b"data bytes"
        );
        // A container sealed for another path does not open here (binding).
        std::fs::create_dir_all(dir.join("data/u")).unwrap();
        std::fs::write(dir.join("data/u/y.pink"), &raw).unwrap();
        let err = storage.read("data/u/y.pink", "reading").unwrap_err();
        assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
        // A keyed read of a plaintext object is refused, never guessed at.
        std::fs::write(dir.join("data/plain.pink"), b"not sealed").unwrap();
        let err = storage.read("data/plain.pink", "reading").unwrap_err();
        assert!(matches!(err, LakeError::NotEncrypted { .. }), "{err:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
