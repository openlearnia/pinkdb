//! Buffered table appender: typed columns in, one `INSERT` per flush.
//!
//! The buffer holds `Vector`s typed like the target table, so appending a row
//! or another table's chunk copies values, never SQL text. A flush hands the
//! materialized chunks to the INSERT executor ([`LogicalPlan::Chunks`] as an
//! `INSERT INTO … SELECT` source), which gives it every guarantee a normal
//! insert has: coercion, defaults for omitted columns, PRIMARY KEY/UNIQUE
//! keys, NOT NULL where the engine enforces it, triggers, index maintenance,
//! the write lease and commit validation. One flush is one statement: inside
//! a transaction it joins the open transaction, in autocommit it is one
//! commit unit.
//!
//! A durable store still logs the flush: the WAL record carries the
//! materialized rows as a binary chunk (payload v7), so an autocommit flush
//! renders no SQL text. Inside an open transaction the commit record is
//! assembled from statement text, so that case renders one multi-row
//! `INSERT … VALUES`. An in-memory store (and a temp-table flush) renders
//! nothing.
//!
//! The appender borrows no connection: every call that may flush takes
//! `&mut Connection` explicitly, so a caller can query the same connection
//! between appends. Unflushed rows are invisible to those queries. Reaching
//! the auto-flush row count commits the buffer at that boundary; an explicit
//! [`Appender::flush`] commits whatever is buffered.

use std::sync::Arc;

use crate::plan::LogicalPlan;
use crate::storage::{DataTable, SchemaPath};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector};
use crate::{Connection, PinkDbError, Result, Value};

/// Rows buffered before an automatic flush when the caller does not set a
/// count: 100 chunks, DuckDB's appender flush size. Every flush is a commit
/// with its own WAL sync; on a durable store, appending TPC-H SF1 lineitem
/// ran 1.36× (row-wise) and 1.53× (chunk-wise) faster than with 4,096 rows
/// per flush (interleaved, 3 rounds).
pub const DEFAULT_AUTO_FLUSH_ROWS: usize = 100 * CHUNK_SIZE;

/// A row/column appender for one table. See the module docs for semantics.
pub struct Appender {
    ns: SchemaPath,
    table: String,
    /// Column names and types resolved when the appender was created.
    schema: Vec<(String, crate::types::DataType)>,
    /// Buffered rows, each chunk at most [`CHUNK_SIZE`]; the last chunk is
    /// the one row appends fill.
    chunks: Vec<DataChunk>,
    rows: usize,
    auto_flush: usize,
}

impl Appender {
    pub(crate) fn new(
        ns: SchemaPath,
        table: String,
        schema: Vec<(String, crate::types::DataType)>,
    ) -> Self {
        Self {
            ns,
            table,
            schema,
            chunks: Vec::new(),
            rows: 0,
            auto_flush: DEFAULT_AUTO_FLUSH_ROWS,
        }
    }

    /// Number of columns every row must carry.
    pub fn column_count(&self) -> usize {
        self.schema.len()
    }

    /// Rows buffered but not yet flushed.
    pub fn pending_rows(&self) -> usize {
        self.rows
    }

    /// Rows buffered before an automatic flush. Zero selects `1`; auto-flush
    /// is never disabled, so a caller that wants explicit batches sets a
    /// count above its batch size.
    pub fn set_auto_flush(&mut self, rows: usize) {
        self.auto_flush = rows.max(1);
    }

    fn empty_columns(&self) -> Vec<Arc<Vector>> {
        self.schema
            .iter()
            .map(|(_, dtype)| Arc::new(Vector::with_capacity(*dtype, CHUNK_SIZE)))
            .collect()
    }

    /// Buffer one full row, flushing when the auto-flush count is reached.
    pub fn append_row(&mut self, conn: &mut Connection, values: Vec<Value>) -> Result<()> {
        if values.len() != self.schema.len() {
            return Err(PinkDbError::Type(format!(
                "appender row has {} values but table {} has {} columns",
                values.len(),
                self.table,
                self.schema.len()
            )));
        }
        // Coerce first: a rejected value must not leave half a row in the
        // column buffers.
        let mut row = Vec::with_capacity(values.len());
        for (value, (_, dtype)) in values.into_iter().zip(&self.schema) {
            row.push(DataTable::coerce(value, *dtype)?);
        }
        if self
            .chunks
            .last()
            .is_none_or(|chunk| chunk.num_rows >= CHUNK_SIZE)
        {
            let columns = self.empty_columns();
            self.chunks.push(DataChunk::projected(columns, 0));
        }
        let chunk = self.chunks.last_mut().expect("append row: tail chunk");
        for (column, value) in chunk.columns.iter_mut().zip(row) {
            Arc::make_mut(column).push_value(value)?;
        }
        chunk.num_rows += 1;
        self.rows += 1;
        if self.rows >= self.auto_flush {
            self.flush(conn)?;
        }
        Ok(())
    }

    /// Buffer a whole chunk column-wise. Every column must have the same
    /// length and match the table width; nothing is buffered on a width
    /// mismatch.
    pub fn append_chunk(&mut self, conn: &mut Connection, columns: Vec<Vector>) -> Result<()> {
        let columns: Vec<Arc<Vector>> = columns.into_iter().map(Arc::new).collect();
        self.append_shared_chunk(conn, &columns)
    }

    /// [`Appender::append_chunk`] for a fetched chunk's shared columns: a
    /// matching-dtype column is adopted without copying when the buffer is
    /// empty, otherwise its values are copied into the buffer's vectors.
    pub fn append_shared_chunk(
        &mut self,
        conn: &mut Connection,
        columns: &[Arc<Vector>],
    ) -> Result<()> {
        if columns.len() != self.schema.len() {
            return Err(PinkDbError::Type(format!(
                "appender chunk has {} columns but table {} has {} columns",
                columns.len(),
                self.table,
                self.schema.len()
            )));
        }
        let rows = columns.first().map(|column| column.len()).unwrap_or(0);
        if let Some(bad) = columns.iter().find(|column| column.len() != rows) {
            return Err(PinkDbError::Type(format!(
                "appender chunk columns have different lengths: expected {rows}, got {}",
                bad.len()
            )));
        }
        if rows == 0 {
            return Ok(());
        }
        let dtypes: Vec<_> = self.schema.iter().map(|(_, dtype)| *dtype).collect();
        if self.rows == 0 && rows <= CHUNK_SIZE {
            // Adopt a whole fetched chunk: columns already typed like the
            // target are shared, the rest coerced once.
            let mut built = Vec::with_capacity(columns.len());
            for (source, dtype) in columns.iter().zip(&dtypes) {
                if source.dtype() == *dtype {
                    built.push(Arc::clone(source));
                } else {
                    let mut column = Vector::with_capacity(*dtype, rows);
                    for row in 0..rows {
                        column.push_value(DataTable::coerce(source.get(row), *dtype)?)?;
                    }
                    built.push(Arc::new(column));
                }
            }
            self.chunks.clear();
            self.chunks.push(DataChunk::projected(built, rows));
            self.rows = rows;
        } else {
            let same_types = columns
                .iter()
                .zip(&dtypes)
                .all(|(source, dtype)| source.dtype() == *dtype);
            let mut start = 0;
            while start < rows {
                if self
                    .chunks
                    .last()
                    .is_none_or(|chunk| chunk.num_rows >= CHUNK_SIZE)
                {
                    let empty = self.empty_columns();
                    self.chunks.push(DataChunk::projected(empty, 0));
                }
                let chunk = self.chunks.last_mut().expect("append chunk: tail chunk");
                let take = (CHUNK_SIZE - chunk.num_rows).min(rows - start);
                if same_types {
                    for (column, source) in chunk.columns.iter_mut().zip(columns) {
                        Arc::make_mut(column).extend_from(source, start, take);
                    }
                } else {
                    for row in start..start + take {
                        // Coerce the whole row before pushing any of it.
                        let mut values = Vec::with_capacity(dtypes.len());
                        for (source, dtype) in columns.iter().zip(&dtypes) {
                            values.push(DataTable::coerce(source.get(row), *dtype)?);
                        }
                        for (column, value) in chunk.columns.iter_mut().zip(values) {
                            Arc::make_mut(column).push_value(value)?;
                        }
                    }
                }
                chunk.num_rows += take;
                self.rows += take;
                start += take;
            }
        }
        if self.rows >= self.auto_flush {
            self.flush(conn)?;
        }
        Ok(())
    }

    /// Commit the buffered rows as one statement. A failure rejects the whole
    /// batch (the statement is atomic), and the buffer is cleared.
    pub fn flush(&mut self, conn: &mut Connection) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let chunks = std::mem::take(&mut self.chunks);
        self.rows = 0;
        self.flush_chunks(conn, chunks)
    }

    fn flush_chunks(&self, conn: &mut Connection, chunks: Vec<DataChunk>) -> Result<()> {
        // The appender was created for one table version; an ALTER since then
        // must not silently drop or NULL columns.
        let width = conn.appender_width(&self.ns, &self.table)?;
        if width != self.schema.len() {
            return Err(PinkDbError::Type(format!(
                "appender was created for table {} with {} columns but it now has {width}",
                self.table,
                self.schema.len()
            )));
        }
        let plan = LogicalPlan::InsertSelect {
            ns: Some(self.ns.clone()),
            table: self.table.clone(),
            columns: None,
            defaults: vec![None; self.schema.len()],
            query: Box::new(LogicalPlan::Chunks { chunks }),
            conflict: None,
            returning: None,
        };
        let wal_text = if conn.needs_wal_text_for(&plan) {
            Some(render_insert(&self.ns, &self.table, &plan)?)
        } else {
            None
        };
        conn.execute_plan(plan, wal_text.as_deref()).map(|_| ())
    }

    /// Flush and consume the appender.
    pub fn close(mut self, conn: &mut Connection) -> Result<()> {
        self.flush(conn)
    }
}

/// Render the WAL's logical SQL for a flush: one multi-row `INSERT`. Only a
/// durable, persistent flush inside an open transaction calls this; an
/// autocommit flush logs the binary chunk instead.
fn render_insert(ns: &SchemaPath, table: &str, plan: &LogicalPlan) -> Result<String> {
    let LogicalPlan::InsertSelect { query, .. } = plan else {
        unreachable!("an appender flush is an INSERT … SELECT");
    };
    let LogicalPlan::Chunks { chunks } = query.as_ref() else {
        unreachable!("an appender flush sources materialized chunks");
    };
    let mut sql = String::new();
    sql.push_str("INSERT INTO ");
    sql.push_str(&ns.to_string());
    sql.push('.');
    sql.push_str(&crate::mv::quote_ident(table));
    sql.push_str(" VALUES ");
    let mut first = true;
    for chunk in chunks {
        for row in 0..chunk.num_rows {
            if !first {
                sql.push_str(", ");
            }
            first = false;
            sql.push('(');
            for (column, vector) in chunk.columns.iter().enumerate() {
                if column > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&crate::prepared::value_literal(&vector.get(row))?);
            }
            sql.push(')');
        }
    }
    Ok(sql)
}
