//! Optional load-time integrity for native extension libraries.
//!
//! A native extension is code the host `dlopen`s. [`super::resolve_path`]
//! already pins *which file* is opened (no bare names, no `..`, no symlinks,
//! canonical path), but it says nothing about the file's *contents*: a
//! writable working directory, a shared volume, or a directory owned by
//! another user is enough to swap the library between deployments.
//!
//! Embedders that cannot make every extension directory root-owned and
//! read-only can hold a SHA-256 allowlist instead ([`set_pins`]): once it is
//! configured, a load of a path that is not in it is refused
//! ([`ExtError::Unpinned`]) and a load whose bytes differ from the pinned hash
//! is refused ([`ExtError::Integrity`]). The check is process-global, so it
//! also covers the WAL replay path and any host that loads through the engine.
//!
//! # Residual risk
//!
//! The hash is computed from the file at `load`, and `dlopen` takes a *path*,
//! so a core cannot hand it an already-verified file descriptor (Linux, macOS
//! and Windows all lack an fd-based `dlopen`). This module narrows the window
//! as far as the platform allows: the file is hashed through an open handle,
//! then its identity (device, inode, size, mtime) is re-checked immediately
//! before `dlopen`, so a swap between the two is detected unless it lands in
//! the last few instructions. What remains is a race with a writer that can
//! replace a file in a directory the host trusts — keep extension directories
//! non-writable by anyone who is not the server user, and treat the allowlist
//! as defence in depth, not as a sandbox.

use std::collections::BTreeMap;
use std::fs::{File, Metadata};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

use sha2::{Digest, Sha256};

use crate::ExtError;

/// SHA-256 of a file's contents, streamed (libraries are small, but a pinned
/// path could be any size and the loader should not have to trust it).
pub(crate) fn hash_file(path: &Path) -> Result<[u8; 32], ExtError> {
    let mut file = File::open(path).map_err(|e| ExtError::Load {
        path: path.to_string_lossy().into_owned(),
        message: format!("cannot open the file to hash it: {e}"),
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| ExtError::Load {
            path: path.to_string_lossy().into_owned(),
            message: format!("cannot read the file to hash it: {e}"),
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

/// The lowercase hex SHA-256 of `path`: the value a `--extension-pin` entry
/// pins (also spelled `sha256:<hex>`).
pub fn sha256_file(path: &Path) -> Result<String, ExtError> {
    hash_file(path).map(|bits| hex(&bits))
}

/// Lowercase hex, the spelling a pinned hash is printed and compared in.
pub(crate) fn hex(bits: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bits {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Parse a hex SHA-256 as written in `--extension-pin PATH=SHA256`.
///
/// Accepts 64 hex digits, case-insensitively, with an optional `sha256:`
/// prefix (the spelling `sha256sum` output and the install manifests use).
pub fn parse_sha256(text: &str) -> Result<[u8; 32], ExtError> {
    let text = text.trim();
    let digits = text
        .strip_prefix("sha256:")
        .or_else(|| text.strip_prefix("SHA256:"))
        .unwrap_or(text);
    if digits.len() != 64 {
        return Err(ExtError::BadPin {
            pin: text.to_string(),
            message: format!("expected 64 hex digits, got {} characters", digits.len()),
        });
    }
    let mut out = [0u8; 32];
    for (i, pair) in digits.as_bytes().chunks(2).enumerate() {
        let hi = hex_digit(pair[0]);
        let lo = hex_digit(pair[1]);
        let (Some(hi), Some(lo)) = (hi, lo) else {
            return Err(ExtError::BadPin {
                pin: text.to_string(),
                message: "a SHA-256 is hex digits only".to_string(),
            });
        };
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// What the loader remembers about the file it hashed, so it can tell
/// whether `dlopen` is about to open that same file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl Identity {
    fn of(metadata: &Metadata) -> Self {
        Identity {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
        }
    }
}

/// The result of checking a pinned path: the identity to re-check just
/// before `dlopen`. `None` means no allowlist is configured.
#[derive(Debug)]
pub(crate) struct PinGuard {
    path: PathBuf,
    identity: Identity,
}

impl PinGuard {
    /// Re-stat the path and fail if it no longer names the file that was
    /// hashed. Call this immediately before `dlopen`.
    pub(crate) fn recheck(&self) -> Result<(), ExtError> {
        let display = self.path.to_string_lossy().into_owned();
        let metadata = std::fs::metadata(&self.path).map_err(|e| ExtError::Changed {
            path: display.clone(),
            message: format!("cannot re-stat it: {e}"),
        })?;
        if Identity::of(&metadata) != self.identity {
            return Err(ExtError::Changed {
                path: display,
                message: "the file was replaced between the hash check and the load".to_string(),
            });
        }
        Ok(())
    }
}

type Pins = BTreeMap<PathBuf, [u8; 32]>;

static PINS: OnceLock<RwLock<Option<Pins>>> = OnceLock::new();

fn pins() -> &'static RwLock<Option<Pins>> {
    PINS.get_or_init(|| RwLock::new(None))
}

/// Install the process-wide allowlist from `PATH=SHA256` entries.
///
/// Each path is resolved exactly like a load ([`super::resolve_path`]: it must
/// exist, be a regular file, contain a directory and no `..`, and not be a
/// symlink), then hashed and stored by canonical path. An empty iterator
/// *enables* the allowlist with no entries, which refuses every load — that is
/// the strictest configuration, not "off"; use [`clear_allowlist`] for off.
///
/// Errors are returned without installing anything (the allowlist is
/// all-or-nothing), so a typo cannot leave a host with a partial policy.
pub fn set_pins<I, S>(entries: I) -> Result<(), ExtError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut table = Pins::new();
    for entry in entries {
        let entry = entry.as_ref();
        let (path, hash) = entry.split_once('=').ok_or_else(|| ExtError::BadPin {
            pin: entry.to_string(),
            message: "expected PATH=SHA256".to_string(),
        })?;
        let hash = parse_sha256(hash)?;
        let canonical = crate::resolve_path(path)?;
        if let Some(existing) = table.insert(canonical.clone(), hash)
            && existing != hash
        {
            return Err(ExtError::BadPin {
                pin: entry.to_string(),
                message: format!(
                    "{} is already pinned with a different hash",
                    canonical.display()
                ),
            });
        }
    }
    *pins().write().unwrap_or_else(|p| p.into_inner()) = Some(table);
    Ok(())
}

/// Remove the allowlist: loads are no longer content-checked.
pub fn clear_allowlist() {
    *pins().write().unwrap_or_else(|p| p.into_inner()) = None;
}

/// Is a content allowlist configured? (`false` after [`clear_allowlist`], and
/// on a host that never set one.)
pub fn allowlist_is_active() -> bool {
    pins().read().unwrap_or_else(|p| p.into_inner()).is_some()
}

/// The pinned paths, sorted (for diagnostics and tests).
pub fn pinned_paths() -> Vec<PathBuf> {
    pins()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .map(|table| table.keys().cloned().collect())
        .unwrap_or_default()
}

/// Check a resolved path against the allowlist and remember the file's
/// identity for the pre-`dlopen` re-check.
pub(crate) fn verify(canonical: &Path, display: &str) -> Result<Option<PinGuard>, ExtError> {
    let guard = pins().read().unwrap_or_else(|p| p.into_inner());
    let Some(table) = guard.as_ref() else {
        return Ok(None);
    };
    let path = display.to_string();
    let Some(expected) = table.get(canonical) else {
        return Err(ExtError::Unpinned { path });
    };
    let actual = hash_file(canonical)?;
    if actual != *expected {
        return Err(ExtError::Integrity {
            path,
            expected: hex(expected),
            actual: hex(&actual),
        });
    }
    // Hash and identity from the same open would be ideal; hashing through
    // `File::open` and then re-stating narrows the window to the gap between
    // the two syscalls, and `recheck` closes it further right before
    // `dlopen`.
    let metadata = std::fs::metadata(canonical).map_err(|e| ExtError::Changed {
        path: display.to_string(),
        message: format!("cannot stat it: {e}"),
    })?;
    Ok(Some(PinGuard {
        path: canonical.to_path_buf(),
        identity: Identity::of(&metadata),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hex_hashes_and_rejects_malformed_ones() {
        let hash =
            parse_sha256("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                .unwrap();
        assert_eq!(
            hex(&hash),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            parse_sha256("E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855")
                .unwrap(),
            hash
        );
        for bad in [
            "",
            "e3b0c4",
            "sha256:",
            "g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8550",
        ] {
            let error = parse_sha256(bad).unwrap_err();
            assert!(error.to_string().contains("invalid pin"), "{error}");
        }
    }

    #[test]
    fn pin_entries_must_be_path_equals_hash() {
        let error = set_pins(["no-separator"]).unwrap_err();
        assert!(
            error.to_string().contains("expected PATH=SHA256"),
            "{error}"
        );
    }
}
