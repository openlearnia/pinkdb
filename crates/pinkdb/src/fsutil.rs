//! Owner-only, symlink-safe filesystem primitives (M-3 / M-5).
//!
//! Durable engine artifacts (WAL, checkpoint, lock file, `.tmp` staging) and
//! `COPY … TO` destinations are created through this module so they never
//! inherit the process umask:
//!
//! - files are created with mode **0600** and directories with **0700** on
//!   Unix (a no-op on other platforms, where the calls still compile);
//! - a `COPY` destination is written to a unique `create_new` temp sibling in
//!   the same directory and renamed into place, so a planted symlink is never
//!   followed or truncated and a crash leaves the previous target intact;
//! - [`tighten`] best-effort narrows an existing artifact that predates this
//!   policy (0644 → 0600), but never fails an open on a permission error.
//!
//! The destination policy lives in [`CopyToPolicy`]: an embedded store is
//! unrestricted (the historical behavior), the network server refuses by
//! default, and the opt-in form confines every write to one root directory.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::{PinkDbError, Result};

/// Where SQL `COPY … TO '<path>'` may write.
///
/// The embedded/trusted API defaults to [`CopyToPolicy::Unrestricted`] — the
/// historical behavior, still written atomically with mode 0600. A network
/// server installs [`CopyToPolicy::Disabled`] at bind time, and the
/// `--allow-copy-to --copy-root DIR` opt-in installs
/// [`CopyToPolicy::Confined`]. `RootRequired` is the safe refusal when an
/// operator opted in but named no root.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CopyToPolicy {
    /// Embedded/trusted default: any destination path (safe-write semantics
    /// still apply).
    #[default]
    Unrestricted,
    /// Refuse every `COPY … TO` (the network server default).
    Disabled,
    /// The operator opted in but no destination root is configured: refuse.
    RootRequired,
    /// Confine every destination to this directory (the server opt-in).
    Confined(PathBuf),
}

impl CopyToPolicy {
    /// Whether this policy permits a write at all.
    pub fn permits_writes(&self) -> bool {
        matches!(self, Self::Unrestricted | Self::Confined(_))
    }
}

/// One process-wide unique temp-name counter (paired with the pid).
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Open (creating or truncating) a file with mode 0600 on Unix.
pub(crate) fn create_private(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Open a read/write file, creating it if absent, with mode 0600.
pub(crate) fn open_rw_private(path: &Path, truncate: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Open (creating, never truncating an existing file) a file with mode 0600.
pub(crate) fn create_new_private(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Best-effort tighten an existing file to 0600 when group/other bits are set.
///
/// Only the owner-read/write bits are kept; a permission error (a file the
/// process does not own, a read-only filesystem) is ignored so an open never
/// fails because of the hardening pass.
pub(crate) fn tighten(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Ok(meta) = std::fs::symlink_metadata(path) else {
            return;
        };
        if meta.file_type().is_symlink() || !meta.is_file() {
            return;
        }
        let mode = meta.permissions().mode() & 0o7777;
        if mode & 0o077 != 0 {
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// Write one `COPY … TO` destination atomically and privately.
///
/// The final path must not be a symlink; the parent must already exist. Bytes
/// go to `.<name>.<pid>.<seq>.tmp` in the same directory (`create_new`, mode
/// 0600), the writer callback fills it, the file is flushed and fsynced, and
/// the temp is renamed over the target (atomic replace, matching COPY's
/// historical truncate semantics). A failure removes the temp and leaves the
/// previous target intact.
pub fn write_atomic_private(
    path: &Path,
    write: impl FnOnce(&mut File) -> Result<()>,
) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(PinkDbError::Execution(format!(
                "cannot write \"{}\": the destination is a symlink (refused)",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(PinkDbError::Execution(format!(
                "cannot inspect \"{}\": {e}",
                path.display()
            )));
        }
    }
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let name = match path.file_name() {
        Some(name) if !name.is_empty() => name.to_os_string(),
        _ => {
            return Err(PinkDbError::Execution(format!(
                "cannot write \"{}\": a file name is required",
                path.display()
            )));
        }
    };
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(&name);
    tmp_name.push(format!(".{}.{seq}.tmp", std::process::id()));
    let tmp = parent.join(tmp_name);
    let mut file = create_new_private(&tmp).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot create a temp file next to \"{}\": {e}",
            path.display()
        ))
    })?;
    let written = (|| -> Result<()> {
        write(&mut file)?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    })();
    drop(file);
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(PinkDbError::Execution(format!(
            "cannot publish \"{}\": {e}",
            path.display()
        )));
    }
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Resolve a `COPY … TO` destination under `policy` and reject paths that
/// escape it.
///
/// Rules (all policies):
/// - an empty path, or one without a normal final component, is refused;
/// - a `..` component anywhere is refused;
/// - the parent must exist, be a directory, and contain no symlinked
///   component between the policy root (when confined) and the target;
/// - the final target must not be a symlink.
///
/// With [`CopyToPolicy::Confined`], the canonical parent must be the root or
/// below it. `Unrestricted` still canonicalizes the parent (so the writer
/// uses the real directory) but imposes no root.
pub(crate) fn resolve_copy_destination(policy: &CopyToPolicy, raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if raw.trim().is_empty() {
        return Err(PinkDbError::Execution(
            "COPY TO needs a destination path".to_string(),
        ));
    }
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(PinkDbError::Execution(format!(
            "cannot write \"{raw}\": a `..` path component is refused"
        )));
    }
    let name = match path.file_name() {
        Some(name) if !name.is_empty() && name != "." => name.to_os_string(),
        _ => {
            return Err(PinkDbError::Execution(format!(
                "cannot write \"{raw}\": a file name is required"
            )));
        }
    };
    let raw_parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let parent = raw_parent.canonicalize().map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot write \"{raw}\": the destination directory \"{}\" is not accessible ({e})",
            raw_parent.display()
        ))
    })?;
    let meta = std::fs::metadata(&parent).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot write \"{raw}\": cannot inspect \"{}\": {e}",
            parent.display()
        ))
    })?;
    if !meta.is_dir() {
        return Err(PinkDbError::Execution(format!(
            "cannot write \"{raw}\": \"{}\" is not a directory",
            parent.display()
        )));
    }
    // Reject a symlinked parent chain (a canonicalized path would otherwise
    // hide the redirect). Confined paths are checked from the root down; an
    // unrestricted path is checked from the filesystem root down.
    let (check_from, _check_to) = match policy {
        CopyToPolicy::Confined(root) => {
            let root = root.canonicalize().map_err(|e| {
                PinkDbError::Execution(format!(
                    "cannot write \"{raw}\": the configured COPY root is not accessible ({e})"
                ))
            })?;
            if !parent.starts_with(&root) {
                return Err(PinkDbError::Execution(format!(
                    "cannot write \"{raw}\": the destination is outside the configured COPY \
                     root \"{}\"",
                    root.display()
                )));
            }
            (root, parent.clone())
        }
        _ => (PathBuf::from("/"), parent.clone()),
    };
    if let Ok(relative) = parent.strip_prefix(&check_from) {
        let mut cursor = check_from.clone();
        for component in relative.components() {
            cursor.push(component);
            if let Ok(meta) = std::fs::symlink_metadata(&cursor)
                && meta.file_type().is_symlink()
            {
                return Err(PinkDbError::Execution(format!(
                    "cannot write \"{raw}\": \"{}\" is a symlinked directory (refused)",
                    cursor.display()
                )));
            }
        }
    }
    let target = parent.join(name);
    if let Ok(meta) = std::fs::symlink_metadata(&target)
        && meta.file_type().is_symlink()
    {
        return Err(PinkDbError::Execution(format!(
            "cannot write \"{raw}\": the destination is a symlink (refused)"
        )));
    }
    Ok(target)
}
