//! Selection vectors: the rows a predicate kept from one [`DataChunk`], as
//! ascending indices.
//!
//! DuckDB's `SelectionVector` in a leaner form (see
//! `src/include/duckdb/common/types/selection_vector.hpp`): a filter no
//! longer materializes a filtered copy of every column, it hands its consumer
//! the surviving row indices and the consumer reads *through* them. `u32`
//! suffices because a selection never spans chunks, and ascending order lets a
//! conjunction narrow one in place.

use crate::vector::DataChunk;

/// Ascending row indices into a chunk, never more than `CHUNK_SIZE`.
pub(crate) type Selection = Vec<u32>;

/// A chunk together with the rows a filter kept from it.
pub(crate) struct Selected {
    pub chunk: DataChunk,
    pub sel: Selection,
}

/// A selection holding at most `1/MATERIALIZE_DIVISOR` of its chunk is
/// cheaper to *materialize once* than to read through: every column read pays
/// an index load, while a compacted chunk is read sequentially. Measured on
/// the two extremes of the TPC-H set — Q6 keeps 1.8 % of each chunk for one
/// aggregate argument (compacting wins), Q1 keeps 98 % for eight (the
/// indirection wins).
const MATERIALIZE_DIVISOR: usize = 8;

/// True when `kept` surviving rows out of `rows` are sparse enough that
/// materializing them beats reading through the selection.
pub(crate) fn worth_materializing(kept: usize, rows: usize) -> bool {
    kept * MATERIALIZE_DIVISOR <= rows
}

/// Every row of a `rows`-row chunk: the start of a narrowing chain.
pub(crate) fn all_rows(rows: usize) -> Selection {
    (0..rows as u32).collect()
}
