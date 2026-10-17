//! Positional delete files (the v4 incremental format's merge-on-read
//! deletes).
//!
//! A delete file holds one sorted, deduplicated list of 0-based row positions
//! within one data file — exactly DuckLake's `file_path`/`pos` delete-file
//! pairing (`files/<schema>/<table>/ducklake-<uuid>-delete.parquet`), encoded
//! in this lake's own compact binary container instead of Parquet:
//!
//! ```text
//! magic   "PINKDL1\n" (8 bytes)
//! count   u64 little-endian
//! pos[0]  u64 little-endian (ascending, no duplicates)
//! ...
//! ```
//!
//! The manifest's [`DeleteRef`](crate::catalog::DeleteRef) already binds a
//! delete file to its data file and row count, so the file itself needs no
//! identity fields; readers validate that the count matches the manifest and
//! every position is inside the data file. A reader applies the positions to
//! the *decoded* data file: rows are dropped by position, and everything else
//! (chunking, values, NaN payloads) is untouched. `UPDATE` is delete + append;
//! writers never rewrite an existing data file.

use crate::{LakeError, Result};

/// Container magic (8 bytes, with a newline terminator like the engine's own
/// magics).
pub(crate) const DELETE_MAGIC: &[u8; 8] = b"PINKDL1\n";

/// Encode a sorted, deduplicated position list. Crate-internal: callers have
/// validated order/uniqueness (or sort themselves via [`normalize`]).
pub(crate) fn encode(positions: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(DELETE_MAGIC.len() + 8 + positions.len() * 8);
    out.extend_from_slice(DELETE_MAGIC);
    out.extend_from_slice(&(positions.len() as u64).to_le_bytes());
    for &p in positions {
        out.extend_from_slice(&p.to_le_bytes());
    }
    out
}

/// Decode and structurally validate one delete file: exact length, sorted,
/// deduplicated positions. A file that fails any of these (a truncated,
/// padded, misordered, or duplicated list) is [`LakeError::Corrupt`] — a
/// silently misapplied delete is exactly the failure this format must never
/// have.
pub(crate) fn decode(bytes: &[u8], path: &std::path::Path) -> Result<Vec<u64>> {
    let corrupt = |message: String| LakeError::Corrupt {
        path: path.to_path_buf(),
        message,
    };
    if bytes.len() < DELETE_MAGIC.len() + 8 {
        return Err(corrupt(format!(
            "delete file is {} bytes, shorter than its {} -byte header",
            bytes.len(),
            DELETE_MAGIC.len() + 8
        )));
    }
    if &bytes[..DELETE_MAGIC.len()] != DELETE_MAGIC {
        return Err(corrupt(
            "delete file does not start with the PINKDL1 magic".to_string(),
        ));
    }
    let count = u64::from_le_bytes(
        bytes[DELETE_MAGIC.len()..DELETE_MAGIC.len() + 8]
            .try_into()
            .expect("8 bytes"),
    ) as usize;
    let expected = DELETE_MAGIC.len() + 8 + count * 8;
    if bytes.len() != expected {
        return Err(corrupt(format!(
            "delete file declares {count} positions ({expected} bytes), but is {} bytes",
            bytes.len()
        )));
    }
    let mut positions = Vec::with_capacity(count);
    for i in 0..count {
        let at = DELETE_MAGIC.len() + 8 + i * 8;
        positions.push(u64::from_le_bytes(
            bytes[at..at + 8].try_into().expect("8 bytes"),
        ));
    }
    if positions.windows(2).any(|w| w[0] >= w[1]) {
        return Err(corrupt(
            "delete file positions must be strictly ascending (sorted, deduplicated)".to_string(),
        ));
    }
    Ok(positions)
}

/// Sort + deduplicate a caller-supplied position list (writer-side; the
/// encoder requires the canonical form, and a repeated position would corrupt
/// the logical row count).
pub(crate) fn normalize(mut positions: Vec<u64>) -> Vec<u64> {
    positions.sort_unstable();
    positions.dedup();
    positions
}

/// Positions must be inside the data file they belong to.
pub(crate) fn validate_bounds(
    positions: &[u64],
    file_rows: u64,
    file: &str,
    path: &std::path::Path,
) -> Result<()> {
    if let Some(&bad) = positions.iter().find(|&&p| p >= file_rows) {
        return Err(LakeError::Corrupt {
            path: path.to_path_buf(),
            message: format!(
                "delete positions for \"{file}\" go up to {bad}, but the file has {file_rows} rows"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn roundtrip_is_exact() {
        for positions in [vec![], vec![0], vec![0, 2, 7], vec![u64::MAX - 1]] {
            let bytes = encode(&positions);
            let decoded = decode(&bytes, Path::new("x.pinkdel")).unwrap();
            assert_eq!(decoded, positions);
        }
    }

    #[test]
    fn malformed_files_are_corrupt_never_silent() {
        let path = Path::new("x.pinkdel");
        // Too short.
        assert!(decode(b"PINKDL1\n", path).is_err());
        // Bad magic.
        let mut bytes = encode(&[1, 2]);
        bytes[0] = b'X';
        assert!(decode(&bytes, path).is_err());
        // Trailing bytes.
        let mut bytes = encode(&[1, 2]);
        bytes.push(0);
        assert!(decode(&bytes, path).is_err());
        // Truncated list.
        let bytes = encode(&[1, 2, 3]);
        assert!(decode(&bytes[..bytes.len() - 1], path).is_err());
        // Not sorted.
        let bytes = {
            let mut b = Vec::new();
            b.extend_from_slice(DELETE_MAGIC);
            b.extend_from_slice(&2u64.to_le_bytes());
            b.extend_from_slice(&5u64.to_le_bytes());
            b.extend_from_slice(&1u64.to_le_bytes());
            b
        };
        assert!(decode(&bytes, path).is_err());
        // Duplicates.
        let bytes = {
            let mut b = Vec::new();
            b.extend_from_slice(DELETE_MAGIC);
            b.extend_from_slice(&2u64.to_le_bytes());
            b.extend_from_slice(&5u64.to_le_bytes());
            b.extend_from_slice(&5u64.to_le_bytes());
            b
        };
        assert!(decode(&bytes, path).is_err());
    }

    #[test]
    fn bounds_and_normalization() {
        assert!(validate_bounds(&[0, 4], 5, "f", Path::new("d")).is_ok());
        assert!(validate_bounds(&[5], 5, "f", Path::new("d")).is_err());
        assert_eq!(normalize(vec![3, 1, 3, 2]), vec![1, 2, 3]);
    }
}
