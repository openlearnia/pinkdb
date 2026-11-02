//! The engine-side hook for vector indexes (wave 107b).
//!
//! The graph itself lives in a linked extension (`pinkdb-vss`); the engine
//! only knows the operations it needs: build through a provider, keep a node
//! set in step with table mutations, search top-k, compact, serialize, and
//! report stats. A `CREATE INDEX … USING <provider>` statement resolves a
//! registered provider by name, so no HNSW type appears in the core.
//!
//! The index is a plain box stored on the table (cloned with the table, so a
//! transaction snapshot is independent) and serialized in the `.pink` footer.
//! The extension registers through [`crate::package::ExtensionPackage`], the
//! same surface host scalars and host statements use.

use std::fmt::Debug;

use crate::types::Result;

/// A graph node count, for `EXPLAIN`/tests/`CALL`-level introspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VectorIndexStats {
    pub nodes: usize,
    pub live: usize,
    pub tombstones: usize,
    pub max_level: u8,
}

/// How a physical row deletion moved the rows the index still knows.
///
/// `delete_from_chunk` removes rows inside one chunk and may drop the chunk;
/// the index must tombstone the removed rows and renumber every later row.
/// DELETE walks chunks in reverse, so no earlier chunk changes underneath.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShift {
    /// Global row id of the chunk's first row, before the deletion.
    pub chunk_start: u64,
    /// The chunk's row count before the deletion.
    pub chunk_len: u64,
    /// Removed offsets inside the chunk, sorted ascending.
    pub deleted: Vec<usize>,
    /// True when every row of the chunk was removed (the chunk is dropped).
    pub chunk_dropped: bool,
}

/// An opaque vector index, implemented by a provider extension.
pub trait VectorIndex: Send + Sync + Debug {
    /// Deep copy, for the engine's clone-on-write transaction snapshots.
    fn clone_box(&self) -> Box<dyn VectorIndex>;
    fn dim(&self) -> usize;
    /// Add one row. An existing row is replaced (delete + insert).
    fn insert(&mut self, row: u64, vector: &[f32]) -> Result<()>;
    /// Add many rows, in the given order. The default is a loop over
    /// [`Self::insert`]; a provider may override it with a bulk build so
    /// long as the result depends only on the rows and their order (wave
    /// 107d: the HNSW build is parallel yet thread-count independent).
    fn insert_batch(&mut self, rows: &[(u64, &[f32])]) -> Result<()> {
        for (row, vector) in rows {
            self.insert(*row, vector)?;
        }
        Ok(())
    }
    /// Tombstone one row; it stays in the graph for connectivity.
    fn delete(&mut self, row: u64);
    /// Apply a physical row-number shift after a DELETE.
    fn remap_after_delete(&mut self, shift: &DeleteShift);
    /// Approximate top-k, sorted by distance then row. `ef_search` overrides
    /// the index setting when present.
    fn search(&self, query: &[f32], k: usize, ef_search: Option<usize>) -> Result<Vec<(f32, u64)>>;
    /// Rebuild the graph without tombstones.
    fn compact(&mut self);
    fn serialize(&self) -> Result<Vec<u8>>;
    fn stats(&self) -> VectorIndexStats;
}

impl Clone for Box<dyn VectorIndex> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// One index-provider extension.
/// A deserialized graph waiting to be attached to its table slot.
pub(crate) type HydratedVectorIndex = (
    crate::storage::SchemaPath,
    String,
    usize,
    Box<dyn VectorIndex>,
);

pub trait VectorIndexProvider: Send + Sync + Debug {
    /// The name after `USING` (case-insensitive), e.g. `hnsw`.
    fn name(&self) -> &'static str;
    /// Build an empty index for `dim`-dimensional f32 vectors. Options are
    /// the `WITH (...)` pairs; the provider validates and defaults them.
    fn create(&self, dim: usize, options: &[(String, String)]) -> Result<Box<dyn VectorIndex>>;
    /// Restore a graph from its serialized form (same provider, same store).
    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn VectorIndex>>;
}

/// Borrow one ARRAY row and widen it to f32 (the index's representation).
/// Returns `None` for a NULL row, a NULL element, a non-ARRAY column or a
/// length that does not match `len`.
pub(crate) fn row_vector(
    vector: &crate::vector::Vector,
    row: usize,
    len: usize,
) -> Option<Vec<f32>> {
    let (elem, values, valid) = vector.array_row(row)?;
    let start = row.checked_mul(len)?;
    if let Some(mask) = valid {
        for i in 0..len {
            let k = start + i;
            if (mask[k / 8] >> (k % 8)) & 1 == 0 {
                return None;
            }
        }
    }
    match (elem, values) {
        (crate::types::ArrayElem::Float, crate::array::ArrayElems::Floats32(v)) => {
            Some(v.get(start..start + len)?.to_vec())
        }
        (crate::types::ArrayElem::Double, crate::array::ArrayElems::Floats(v)) => v
            .get(start..start + len)
            .map(|s| s.iter().map(|x| *x as f32).collect()),
        _ => None,
    }
}
