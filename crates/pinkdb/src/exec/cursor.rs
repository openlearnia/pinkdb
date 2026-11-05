//! Streaming result cursor: a SELECT's chunks pulled one at a time instead of
//! materialized as a whole result.
//!
//! A cursor starts on the caller's thread, under the view the caller already
//! holds, and buffers chunks until the plan ends or the buffer passes
//! [`INLINE_BYTES`]. Most results end there: a blocking plan (sort,
//! aggregate, join, window) has its whole output at the end of evaluation
//! anyway, and a point query or a small scan costs what it always did — no
//! snapshot, no thread.
//!
//! A larger streaming result hands its running pipeline, as it is, to a
//! producer thread: the thread gets a snapshot of the store taken at that
//! moment (a clone of the [`PinkDb`]: tables hold their chunks behind `Arc`s,
//! so the clone shares the data) and the statement's evaluation context, and
//! continues the same evaluation. Nothing is re-run, so every row comes from
//! one evaluation of one view, and a slow consumer never holds a lock that
//! blocks writers. The producer hands chunks out through a bounded channel:
//! a full channel blocks it, which is the backpressure. A table function's
//! push scan runs on a thread of its own the same way.
//!
//! Dropping a cursor closes the channel: the producer's next hand-off fails,
//! it stops, and the drop joins it.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::JoinHandle;

use super::eval as evaluate;
use super::{Cx, ExecBudget, QueryResult, Stream, chunk_approx_bytes};
use crate::plan::LogicalPlan;
use crate::storage::{PinkDb, Session};
use crate::types::{DataType, PinkDbError, Result};
use crate::vector::DataChunk;

/// Batches a producer may run ahead of its consumer (a table function's
/// push scan uses it for single chunks).
pub(crate) const CURSOR_DEPTH: usize = 4;

/// A producer hands its chunks over in batches of up to this many estimated
/// bytes (`check_chunk`'s per-cell estimate, a few times the payload for
/// fixed-width columns) or [`BATCH_CHUNKS`] chunks: each channel hand-off
/// parks and wakes a thread, so one per 2048-row chunk made a narrow
/// streamed scan several times slower than the materialized path (C API
/// fetch of one SF1 `lineitem` column, 3.9x). In flight: at most
/// [`CURSOR_DEPTH`] batches plus the one being filled.
const BATCH_BYTES: usize = 8 << 20;
const BATCH_CHUNKS: usize = 1024;

/// Result bytes a streaming plan buffers inline before its pipeline moves to
/// a producer thread over a snapshot.
pub(crate) const INLINE_BYTES: usize = 16 << 20;

/// The error a producer sees when its consumer went away; it only ends the
/// producer and is never reported.
pub(crate) const CURSOR_CLOSED: &str = "the result cursor was closed";

/// One statement's result, pulled chunk by chunk. See the module docs.
pub struct QueryCursor {
    names: Vec<String>,
    types: Vec<DataType>,
    /// Chunks produced inline (or a row-shaped result converted once).
    buffered: VecDeque<DataChunk>,
    /// The rest of a large streaming result.
    producer: Option<Producer>,
    /// A statement that produced no row set.
    result: Option<QueryResult>,
    rows: u64,
}

struct Producer {
    rx: Option<Receiver<Result<Vec<DataChunk>>>>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for Producer {
    fn drop(&mut self) {
        // Closing the channel first unblocks a producer waiting to hand off a
        // chunk; its send then fails and it returns.
        self.rx.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl std::fmt::Debug for QueryCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCursor")
            .field("names", &self.names)
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

impl QueryCursor {
    /// Run a read-only, row-producing `plan` over `db` (see the module
    /// docs). Errors from building the plan, from a blocking plan's
    /// evaluation, or from the inline phase surface here, like a materialized
    /// query's.
    pub(crate) fn start(
        db: &PinkDb,
        plan: LogicalPlan,
        budget: Arc<ExecBudget>,
        session: Session,
    ) -> Result<QueryCursor> {
        // The plan sits at a fixed heap address for the whole statement: the
        // pipeline borrows it, and the scan projections and the subquery memo
        // are keyed by its nodes' addresses.
        let plan = Arc::new(plan);
        let cx = Cx::with_budget(db, budget)
            .with_session(session)
            .with_projections(&plan);
        let mut buffered = VecDeque::new();
        let Some(mut stream) = Stream::build(&cx, &plan)? else {
            // A blocking plan has its whole output at the end of evaluation.
            let out = evaluate(&cx, &plan)?;
            for chunk in &out.chunks {
                check_chunk(&cx, chunk)?;
            }
            buffered.extend(out.chunks);
            return Ok(QueryCursor::buffered(out.names, out.types, buffered));
        };
        let (names, types) = (stream.names.clone(), stream.types.clone());
        let mut bytes = 0usize;
        while let Some(chunk) = stream.next(&cx)? {
            bytes += check_chunk(&cx, &chunk)?;
            buffered.push_back(chunk);
            if bytes < INLINE_BYTES {
                continue;
            }
            // Past the inline bound: the rest of this same pipeline continues
            // on a producer thread, over a snapshot of exactly the view it has
            // read so far (the caller still holds it, so nothing changed).
            let snapshot = db.cursor_snapshot();
            let parts = cx.detach();
            // SAFETY: the stream borrows only from `*plan` (`Stream::build`
            // and `next` keep references to plan nodes, nothing else), which
            // lives in the `Arc`'s heap allocation: it never moves and is
            // never mutated. `Handoff` owns that `Arc` and drops the stream
            // before it, so the references never outlive their target.
            let stream = unsafe { std::mem::transmute::<Stream<'_>, Stream<'static>>(stream) };
            let handoff = Handoff {
                stream,
                _plan: plan,
            };
            let mut cursor = QueryCursor::buffered(names, types, buffered);
            cursor.producer = Some(Producer::spawn(snapshot, parts, handoff)?);
            return Ok(cursor);
        }
        Ok(QueryCursor::buffered(names, types, buffered))
    }

    fn buffered(
        names: Vec<String>,
        types: Vec<DataType>,
        buffered: VecDeque<DataChunk>,
    ) -> QueryCursor {
        QueryCursor {
            names,
            types,
            buffered,
            producer: None,
            result: None,
            rows: 0,
        }
    }

    /// A cursor over a statement that already ran: a SELECT that exists only
    /// as rows hands them out as chunks; any other outcome has no rows and
    /// keeps its [`QueryResult`] for [`Self::result`].
    pub fn from_result(result: QueryResult) -> Result<QueryCursor> {
        match result {
            QueryResult::Selected { names, types, rows } => {
                let schema: Vec<(String, DataType)> =
                    names.iter().cloned().zip(types.iter().copied()).collect();
                let out = super::output_from_rows(&schema, &rows)?;
                Ok(QueryCursor {
                    names,
                    types,
                    buffered: out.chunks.into_iter().collect(),
                    producer: None,
                    result: None,
                    rows: 0,
                })
            }
            other => Ok(QueryCursor {
                names: Vec::new(),
                types: Vec::new(),
                buffered: VecDeque::new(),
                producer: None,
                result: Some(other),
                rows: 0,
            }),
        }
    }

    /// The result's column names (empty for a statement without rows).
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The result's column types.
    pub fn types(&self) -> &[DataType] {
        &self.types
    }

    /// Whether this statement produces a row set.
    pub fn has_rows(&self) -> bool {
        self.result.is_none()
    }

    /// The outcome of a statement that produced no row set (`CREATE`,
    /// `INSERT`, `USE`, …), once; `None` for a row set.
    pub fn result(&mut self) -> Option<QueryResult> {
        self.result.take()
    }

    /// Rows handed out so far.
    pub fn rows_fetched(&self) -> u64 {
        self.rows
    }

    /// The next chunk of the row set, or `None` at its end. An error ends the
    /// cursor: later calls return `None`.
    pub fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        let next = match self.buffered.pop_front() {
            Some(chunk) => Ok(Some(chunk)),
            None => self.recv(),
        };
        if let Ok(Some(chunk)) = &next {
            self.rows += chunk.num_rows() as u64;
        }
        next
    }

    /// The producer's next chunk; its end or error ends the cursor.
    fn recv(&mut self) -> Result<Option<DataChunk>> {
        let next = match self
            .producer
            .as_ref()
            .and_then(|p| p.rx.as_ref())
            .map(|rx| rx.recv())
        {
            Some(Ok(Ok(batch))) => {
                // A producer never sends an empty batch.
                let mut batch = batch.into_iter();
                let first = batch.next();
                self.buffered.extend(batch);
                return Ok(first);
            }
            Some(Ok(Err(e))) => Err(e),
            // The producer finished (its sender dropped), or none ran.
            Some(Err(_)) | None => {
                // It has sent everything; all it has left is to drop its
                // pipeline and snapshot, which the caller need not wait for
                // (that drop was a sixth of a narrow streamed scan's time).
                // A cursor dropped mid-stream still joins its producer.
                if let Some(mut producer) = self.producer.take() {
                    producer.handle.take();
                }
                Ok(None)
            }
        };
        self.producer = None;
        next
    }

    /// Drain the rest of the row set into one materialized result.
    pub fn into_query_result(mut self) -> Result<QueryResult> {
        if let Some(result) = self.result() {
            return Ok(result);
        }
        let mut rows = Vec::new();
        while let Some(chunk) = self.next_chunk()? {
            for i in 0..chunk.num_rows() {
                rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
            }
        }
        Ok(QueryResult::Selected {
            names: std::mem::take(&mut self.names),
            types: std::mem::take(&mut self.types),
            rows,
        })
    }
}

/// What every handed-out chunk is checked against: the deadline and
/// cancellation, decoded columns, and the result caps. Returns the chunk's
/// estimated bytes.
fn check_chunk(cx: &Cx, chunk: &DataChunk) -> Result<usize> {
    cx.check_deadline()?;
    // A zero-length column would be an undecoded projection placeholder,
    // never a result value (the same check the materializing path runs).
    for (i, column) in chunk.columns.iter().enumerate() {
        if column.len() != chunk.num_rows() {
            return Err(PinkDbError::Execution(format!(
                "internal error: result column {i} was never decoded \
                 (a scan projection missed a reference)"
            )));
        }
    }
    let bytes = chunk_approx_bytes(chunk);
    cx.add_result(chunk.num_rows(), bytes)?;
    Ok(bytes)
}

/// A running pipeline moving to the producer thread. Field order matters:
/// the stream borrows the plan, so it is declared (and dropped) first.
struct Handoff {
    stream: Stream<'static>,
    /// Never read: it only keeps the plan the stream borrows alive.
    _plan: Arc<LogicalPlan>,
}

impl Producer {
    fn spawn(snapshot: PinkDb, parts: super::CxParts, handoff: Handoff) -> Result<Producer> {
        let (tx, rx) = sync_channel(CURSOR_DEPTH);
        let handle = std::thread::Builder::new()
            .name("pinkdb-cursor".to_string())
            .spawn(move || produce(snapshot, parts, handoff, tx))
            .map_err(PinkDbError::Io)?;
        Ok(Producer {
            rx: Some(rx),
            handle: Some(handle),
        })
    }
}

/// The producer thread: continue the handed-off pipeline over the snapshot
/// and hand its chunks out.
fn produce(
    snapshot: PinkDb,
    parts: super::CxParts,
    mut handoff: Handoff,
    tx: SyncSender<Result<Vec<DataChunk>>>,
) {
    let cx = parts.attach(&snapshot);
    let send = |batch: Vec<DataChunk>| {
        tx.send(Ok(batch))
            .map_err(|_| PinkDbError::Execution(CURSOR_CLOSED.to_string()))
    };
    let mut run = || -> Result<()> {
        let mut batch = Vec::new();
        let mut bytes = 0;
        while let Some(chunk) = handoff.stream.next(&cx)? {
            bytes += check_chunk(&cx, &chunk)?;
            batch.push(chunk);
            if bytes >= BATCH_BYTES || batch.len() >= BATCH_CHUNKS {
                send(std::mem::take(&mut batch))?;
                bytes = 0;
            }
        }
        if !batch.is_empty() {
            send(batch)?;
        }
        Ok(())
    };
    if let Err(e) = run() {
        // A closed channel means the consumer is gone: nothing to report.
        let _ = tx.send(Err(e));
    }
    // The stream (and any scan thread it owns) goes before the plan it
    // borrows; `Handoff`'s field order guarantees it.
    drop(handoff);
}

/// Whether a read-only `plan` produces a row set the cursor can stream (the
/// read path answers `USE`, the setting statements and `COPY` itself).
pub(crate) fn streams_rows(plan: &LogicalPlan) -> bool {
    crate::plan::is_read_only(plan)
        && !matches!(
            plan,
            LogicalPlan::Use { .. }
                | LogicalPlan::SetVariable { .. }
                | LogicalPlan::SetSetting { .. }
                | LogicalPlan::ResetSetting { .. }
                | LogicalPlan::CopyTo { .. }
        )
}
