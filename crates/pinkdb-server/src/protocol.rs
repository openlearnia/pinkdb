//! Hand-rolled binary protocol for the pinkdb network server (no serde).
//!
//! # Framing
//!
//! Every message travels as one frame:
//!
//! ```text
//! +-----------------+-----------+--------------------------+
//! | length (u32 LE) | type (u8) | body (length - 1 bytes)  |
//! +-----------------+-----------+--------------------------+
//! ```
//!
//! `length` counts the type byte plus the body. A frame larger than
//! [`MAX_FRAME_SIZE`] (64 MiB) is rejected before anything is allocated, and
//! every inner length (string, column, row, value) is validated against the
//! bytes remaining in the frame — a malformed or truncated peer can never
//! make the decoder read past the frame or allocate on an unvalidated length.
//!
//! # Messages
//!
//! Requests: [`Request::Hello`], [`Request::Execute`], [`Request::Ping`].
//!
//! Responses: [`Response::HelloOk`], [`Response::Pong`], and, per executed
//! statement, one result group — [`Response::ResultSet`] followed by zero or
//! more [`Response::Rows`] frames and exactly one [`Response::Done`] frame.
//! A successful `Execute` batch ends with exactly one
//! [`Response::BatchDone`] frame carrying the statement count; the client
//! reads result groups until it, so neither side parses SQL just to count
//! statements. A failing statement ends the batch with one
//! [`Response::Error`] frame and no `BatchDone` (the same all-or-nothing
//! contract as `Connection::execute`: earlier statements' results are not
//! sent because the engine returns them only on success).
//!
//! [`PROTOCOL_VERSION`] is the wire version; a mismatched `Hello` is rejected
//! by the server with a clear `Error` frame.

use std::io::{self, Read, Write};

use pinkdb::{DataType, Value};

/// Protocol version spoken by this crate. Bump on any wire change.
///
/// v2: `Execute` batches end with an explicit [`Response::BatchDone`]
/// terminator (the client no longer parses SQL locally to count statements).
/// v3: [`DoneKind::LoadedExtension`] (tag 15) reports `LOAD EXTENSION`.
/// v4: [`DoneKind::Checkpointed`] (tag 16) reports `CHECKPOINT`.
/// v5: [`Request::Prepare`] / [`Request::ExecutePrepared`] and
/// [`Response::Prepared`] carry server-side prepared statements with `?`
/// parameters.
/// v5: `DECIMAL`/`INTERVAL` value and dtype tags (DECIMAL's precision/scale
/// ride after its dtype tag).
/// v7: [`DoneKind::AlteredTable`] (tag 17) reports `ALTER TABLE`.
pub const PROTOCOL_VERSION: u32 = 7;

/// Hard maximum frame size (64 MiB).
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Rows per `Rows` frame when a result set is streamed (upper bound; the
/// server may emit fewer rows per frame to keep frames well under
/// [`MAX_FRAME_SIZE`]).
pub const ROWS_PER_FRAME: usize = 2048;

/// Wire tags for the message types.
pub mod msg_type {
    pub const HELLO: u8 = 1;
    pub const EXECUTE: u8 = 2;
    pub const PING: u8 = 3;
    /// v5: parse one statement with `?` placeholders into a connection-local
    /// prepared statement.
    pub const PREPARE: u8 = 4;
    /// v5: execute a prepared statement with one value per placeholder.
    pub const EXECUTE_PREPARED: u8 = 5;

    pub const HELLO_OK: u8 = 10;
    pub const PONG: u8 = 11;
    pub const RESULT_SET: u8 = 12;
    pub const ROWS: u8 = 13;
    pub const DONE: u8 = 14;
    pub const ERROR: u8 = 15;
    /// v2: end of one `Execute` batch; body is the statement count.
    pub const BATCH_DONE: u8 = 16;
    /// v5: reply to `Prepare`; body is the handle and the parameter count.
    pub const PREPARED: u8 = 17;
}

/// Value tag bytes (one per [`Value`] variant).
mod value_tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const I32: u8 = 2;
    pub const I64: u8 = 3;
    pub const F64: u8 = 4;
    pub const VARCHAR: u8 = 5;
    pub const DATE: u8 = 6;
    pub const TIMESTAMP: u8 = 7;
    /// v6: `DECIMAL(i128 unscaled | u8 scale)` and
    /// `INTERVAL(months i32 | days i32 | micros i64)`.
    pub const DECIMAL: u8 = 8;
    pub const INTERVAL: u8 = 9;
}

/// DataType tag bytes (one per [`DataType`] variant).
mod dtype_tag {
    pub const BOOLEAN: u8 = 0;
    pub const INTEGER: u8 = 1;
    pub const BIGINT: u8 = 2;
    pub const DOUBLE: u8 = 3;
    pub const VARCHAR: u8 = 4;
    pub const DATE: u8 = 5;
    pub const TIMESTAMP: u8 = 6;
    /// v6: DECIMAL writes precision and scale bytes after the tag; INTERVAL
    /// carries no parameters.
    pub const DECIMAL: u8 = 7;
    pub const INTERVAL: u8 = 8;
}

// ---------- errors ----------

/// Every failure the protocol layer, server, or client can report.
#[derive(Debug)]
pub enum ProtocolError {
    /// Transport failure (connect, read, write, EOF).
    Io(io::Error),
    /// A frame's declared length exceeds [`MAX_FRAME_SIZE`].
    FrameTooLarge { size: usize, max: usize },
    /// A declared inner length exceeds the bytes remaining in the frame.
    Truncated(&'static str),
    /// A structurally invalid frame (bad tag, bad UTF-8, trailing bytes, ...).
    Malformed(String),
    /// The peer speaks a different protocol version.
    VersionMismatch { server: u32, client: u32 },
    /// The server reported a SQL/execution error.
    Server(String),
    /// A well-formed message of the wrong kind for the current state.
    Unexpected(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::Io(e) => write!(f, "IO error: {e}"),
            ProtocolError::FrameTooLarge { size, max } => write!(
                f,
                "frame of {size} bytes exceeds the {max}-byte protocol maximum"
            ),
            ProtocolError::Truncated(what) => write!(f, "truncated frame: {what}"),
            ProtocolError::Malformed(msg) => write!(f, "malformed frame: {msg}"),
            ProtocolError::VersionMismatch { server, client } => write!(
                f,
                "protocol version mismatch: server speaks v{server}, client speaks v{client}"
            ),
            // The server's message is the engine's own error text; keep it
            // verbatim so remote output matches the embedded REPL.
            ProtocolError::Server(msg) => write!(f, "{msg}"),
            ProtocolError::Unexpected(msg) => write!(f, "unexpected message: {msg}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProtocolError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        ProtocolError::Io(e)
    }
}

// ---------- messages ----------

/// One client request.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// First message on a connection; carries the client's protocol version.
    Hello { protocol_version: u32 },
    /// Run every statement in `sql`, in order.
    Execute { sql: String },
    /// Liveness probe.
    Ping,
    /// v5: parse one statement with `?` placeholders (exactly one statement)
    /// into a connection-local prepared statement.
    Prepare { sql: String },
    /// v5: execute a prepared statement with one value per `?` (textual
    /// order). The reply is one result group plus a `BatchDone`, exactly like
    /// a one-statement `Execute`.
    ExecutePrepared { handle: u64, values: Vec<Value> },
}

impl Request {
    /// The wire tag of this message.
    pub fn msg_type(&self) -> u8 {
        match self {
            Request::Hello { .. } => msg_type::HELLO,
            Request::Execute { .. } => msg_type::EXECUTE,
            Request::Ping => msg_type::PING,
            Request::Prepare { .. } => msg_type::PREPARE,
            Request::ExecutePrepared { .. } => msg_type::EXECUTE_PREPARED,
        }
    }

    /// The message kind's name, for error messages.
    pub fn kind(&self) -> &'static str {
        match self {
            Request::Hello { .. } => "Hello",
            Request::Execute { .. } => "Execute",
            Request::Ping => "Ping",
            Request::Prepare { .. } => "Prepare",
            Request::ExecutePrepared { .. } => "ExecutePrepared",
        }
    }

    /// Encode the body (everything after the type byte).
    pub fn encode_body(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = Vec::new();
        match self {
            Request::Hello { protocol_version } => put_u32(&mut buf, *protocol_version),
            Request::Execute { sql } => put_str(&mut buf, sql)?,
            Request::Ping => {}
            Request::Prepare { sql } => put_str(&mut buf, sql)?,
            Request::ExecutePrepared { handle, values } => {
                put_u64(&mut buf, *handle);
                put_u32(&mut buf, count_u32(values.len(), "ExecutePrepared values")?);
                for value in values {
                    put_value(&mut buf, value)?;
                }
            }
        }
        Ok(buf)
    }

    /// Decode a body previously produced by [`Request::encode_body`].
    pub fn decode_body(tag: u8, body: &[u8]) -> Result<Request, ProtocolError> {
        let mut r = Reader::new(body);
        let request = match tag {
            msg_type::HELLO => Request::Hello {
                protocol_version: r.u32("Hello.protocol_version")?,
            },
            msg_type::EXECUTE => Request::Execute {
                sql: r.string("Execute.sql")?,
            },
            msg_type::PING => Request::Ping,
            msg_type::PREPARE => Request::Prepare {
                sql: r.string("Prepare.sql")?,
            },
            msg_type::EXECUTE_PREPARED => {
                let handle = r.u64("ExecutePrepared.handle")?;
                let count = r.u32("ExecutePrepared value count")? as usize;
                // Every value costs at least one tag byte.
                if count as u64 > r.remaining() as u64 {
                    return Err(ProtocolError::Truncated("ExecutePrepared values"));
                }
                let mut values = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    values.push(r.value("ExecutePrepared value")?);
                }
                Request::ExecutePrepared { handle, values }
            }
            other => {
                return Err(ProtocolError::Malformed(format!(
                    "unknown request message type {other}"
                )))
            }
        };
        r.finish("request")?;
        Ok(request)
    }
}

/// One server response.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// Handshake reply; the server's version and the agreed protocol version.
    HelloOk {
        server_version: String,
        protocol_version: u32,
    },
    /// Reply to [`Request::Ping`].
    Pong,
    /// A row-producing statement's column names and types; followed by
    /// `Rows` frames and one `Done`.
    ResultSet {
        names: Vec<String>,
        types: Vec<DataType>,
    },
    /// A batch of rows of the current result set. The frame carries the
    /// result set's column count (each row must have exactly that many
    /// values; a missing value is a truncation and trailing bytes are
    /// rejected, so the wire form stays self-validating).
    Rows { rows: Vec<Vec<Value>> },
    /// The end of one statement's result.
    Done {
        kind: DoneKind,
        count: u64,
        name: String,
    },
    /// v2: the end of one `Execute` batch; `statements` is the number of
    /// result groups that preceded it. The client reads until this frame, so
    /// the statement count never needs local parsing.
    BatchDone { statements: u32 },
    /// v5: reply to [`Request::Prepare`]; `handle` names the server-side
    /// prepared statement (connection-local) and `parameter_count` is its
    /// number of `?` placeholders.
    Prepared { handle: u64, parameter_count: u32 },
    /// The statement (or handshake) failed; the connection stays usable.
    Error { message: String },
}

/// What a statement did (the wire form of pinkdb's `QueryResult` variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DoneKind {
    /// `SELECT`: the preceding `ResultSet`/`Rows` frames carry the rows.
    Selected = 0,
    Inserted = 1,
    Updated = 2,
    Deleted = 3,
    /// `CREATE TABLE`.
    Created = 4,
    Began = 5,
    Committed = 6,
    RolledBack = 7,
    CreatedView = 8,
    DroppedTable = 9,
    DroppedView = 10,
    CreatedDatabase = 11,
    DroppedDatabase = 12,
    /// `USE <db>`; `name` is the database.
    Used = 13,
    /// Reserved for statement outcomes a future protocol version may add;
    /// this version's server never emits it and the client rejects it.
    Other = 14,
    /// `LOAD EXTENSION '<path>'` (v3); `name` is the extension's declared
    /// name.
    LoadedExtension = 15,
    /// `CHECKPOINT` (v4).
    Checkpointed = 16,
    /// `ALTER TABLE` (v7); `name` is the table's name after the statement (a
    /// `RENAME TO` reports the new one).
    AlteredTable = 17,
}

impl DoneKind {
    fn from_tag(tag: u8) -> Result<DoneKind, ProtocolError> {
        Ok(match tag {
            0 => DoneKind::Selected,
            1 => DoneKind::Inserted,
            2 => DoneKind::Updated,
            3 => DoneKind::Deleted,
            4 => DoneKind::Created,
            5 => DoneKind::Began,
            6 => DoneKind::Committed,
            7 => DoneKind::RolledBack,
            8 => DoneKind::CreatedView,
            9 => DoneKind::DroppedTable,
            10 => DoneKind::DroppedView,
            11 => DoneKind::CreatedDatabase,
            12 => DoneKind::DroppedDatabase,
            13 => DoneKind::Used,
            14 => DoneKind::Other,
            15 => DoneKind::LoadedExtension,
            16 => DoneKind::Checkpointed,
            17 => DoneKind::AlteredTable,
            other => {
                return Err(ProtocolError::Malformed(format!(
                    "unknown Done kind {other}"
                )))
            }
        })
    }
}

impl Response {
    /// The wire tag of this message.
    pub fn msg_type(&self) -> u8 {
        match self {
            Response::HelloOk { .. } => msg_type::HELLO_OK,
            Response::Pong => msg_type::PONG,
            Response::ResultSet { .. } => msg_type::RESULT_SET,
            Response::Rows { .. } => msg_type::ROWS,
            Response::Done { .. } => msg_type::DONE,
            Response::BatchDone { .. } => msg_type::BATCH_DONE,
            Response::Prepared { .. } => msg_type::PREPARED,
            Response::Error { .. } => msg_type::ERROR,
        }
    }

    /// The message kind's name, for error messages.
    pub fn kind(&self) -> &'static str {
        match self {
            Response::HelloOk { .. } => "HelloOk",
            Response::Pong => "Pong",
            Response::ResultSet { .. } => "ResultSet",
            Response::Rows { .. } => "Rows",
            Response::Done { .. } => "Done",
            Response::BatchDone { .. } => "BatchDone",
            Response::Prepared { .. } => "Prepared",
            Response::Error { .. } => "Error",
        }
    }

    /// Encode the body (everything after the type byte).
    pub fn encode_body(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = Vec::new();
        match self {
            Response::HelloOk {
                server_version,
                protocol_version,
            } => {
                put_str(&mut buf, server_version)?;
                put_u32(&mut buf, *protocol_version);
            }
            Response::Pong => {}
            Response::ResultSet { names, types } => {
                if names.len() != types.len() {
                    return Err(ProtocolError::Malformed(format!(
                        "ResultSet has {} names but {} types",
                        names.len(),
                        types.len()
                    )));
                }
                put_u32(&mut buf, count_u32(names.len(), "ResultSet columns")?);
                for (name, dtype) in names.iter().zip(types) {
                    put_str(&mut buf, name)?;
                    put_u8(&mut buf, dtype_byte(*dtype));
                    // v6: DECIMAL's parameters ride after the tag.
                    if let DataType::Decimal { precision, scale } = dtype {
                        put_u8(&mut buf, *precision);
                        put_u8(&mut buf, *scale);
                    }
                }
            }
            Response::Rows { rows } => {
                let columns = rows.first().map_or(0, Vec::len);
                for row in rows {
                    if row.len() != columns {
                        return Err(ProtocolError::Malformed(format!(
                            "Rows frame is ragged: row of {} values among rows of {columns}",
                            row.len()
                        )));
                    }
                }
                put_u32(&mut buf, count_u32(rows.len(), "Rows count")?);
                put_u32(&mut buf, count_u32(columns, "Rows columns")?);
                for row in rows {
                    for value in row {
                        put_value(&mut buf, value)?;
                    }
                }
            }
            Response::Done { kind, count, name } => {
                put_u8(&mut buf, *kind as u8);
                put_u64(&mut buf, *count);
                put_str(&mut buf, name)?;
            }
            Response::BatchDone { statements } => put_u32(&mut buf, *statements),
            Response::Prepared {
                handle,
                parameter_count,
            } => {
                put_u64(&mut buf, *handle);
                put_u32(&mut buf, *parameter_count);
            }
            Response::Error { message } => put_str(&mut buf, message)?,
        }
        Ok(buf)
    }

    /// Decode a body previously produced by [`Response::encode_body`].
    pub fn decode_body(tag: u8, body: &[u8]) -> Result<Response, ProtocolError> {
        let mut r = Reader::new(body);
        let response = match tag {
            msg_type::HELLO_OK => Response::HelloOk {
                server_version: r.string("HelloOk.server_version")?,
                protocol_version: r.u32("HelloOk.protocol_version")?,
            },
            msg_type::PONG => Response::Pong,
            msg_type::RESULT_SET => {
                let count = r.u32("ResultSet column count")? as usize;
                // Each column costs at least a 4-byte name length plus a
                // 1-byte dtype tag; reject an impossible count up front.
                if count as u64 * 5 > r.remaining() as u64 {
                    return Err(ProtocolError::Truncated("ResultSet columns"));
                }
                let mut names = Vec::with_capacity(count.min(4096));
                let mut types = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    names.push(r.string("ResultSet column name")?);
                    types.push(r.dtype("ResultSet column type")?);
                }
                Response::ResultSet { names, types }
            }
            msg_type::ROWS => {
                let count = r.u32("Rows count")? as usize;
                let columns = r.u32("Rows columns")? as usize;
                // Every cell costs at least one tag byte; a zero-column row
                // (pinkdb's one-row `SingleRow` shape) is representable at
                // most once, so `max(columns, 1)` bounds the claim.
                let row_cost = columns.max(1) as u64;
                if count as u64 * row_cost > r.remaining() as u64 + 1 {
                    return Err(ProtocolError::Truncated("Rows values"));
                }
                let mut rows = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    let mut row = Vec::with_capacity(columns.min(1024));
                    for _ in 0..columns {
                        row.push(r.value("Rows value")?);
                    }
                    rows.push(row);
                }
                Response::Rows { rows }
            }
            msg_type::DONE => Response::Done {
                kind: DoneKind::from_tag(r.u8("Done.kind")?)?,
                count: r.u64("Done.count")?,
                name: r.string("Done.name")?,
            },
            msg_type::BATCH_DONE => Response::BatchDone {
                statements: r.u32("BatchDone.statements")?,
            },
            msg_type::PREPARED => Response::Prepared {
                handle: r.u64("Prepared.handle")?,
                parameter_count: r.u32("Prepared.parameter_count")?,
            },
            msg_type::ERROR => Response::Error {
                message: r.string("Error.message")?,
            },
            other => {
                return Err(ProtocolError::Malformed(format!(
                    "unknown response message type {other}"
                )))
            }
        };
        r.finish("response")?;
        Ok(response)
    }
}

// ---------- frame layer ----------

/// Write one frame: `u32 LE length | u8 type | body`.
pub fn write_frame<W: Write>(w: &mut W, msg_type: u8, body: &[u8]) -> Result<(), ProtocolError> {
    let len = body.len() + 1;
    if len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            size: len,
            max: MAX_FRAME_SIZE,
        });
    }
    let mut frame = Vec::with_capacity(4 + len);
    frame.extend_from_slice(&(len as u32).to_le_bytes());
    frame.push(msg_type);
    frame.extend_from_slice(body);
    w.write_all(&frame)?;
    Ok(())
}

/// Bytes read per step while receiving a frame body. The body buffer grows
/// with the bytes that actually arrived, so a peer that announces a large
/// frame and then stalls pins at most one step, not the announced size.
const FRAME_READ_STEP: usize = 64 * 1024;

/// Read one frame; returns `(type, body)`. The length prefix is validated
/// against [`MAX_FRAME_SIZE`], and the body buffer grows with received bytes
/// (never pre-sized from the untrusted prefix).
pub fn read_frame<R: Read>(r: &mut R) -> Result<(u8, Vec<u8>), ProtocolError> {
    let mut prefix = [0u8; 4];
    r.read_exact(&mut prefix)?;
    let len = u32::from_le_bytes(prefix) as usize;
    if len == 0 {
        return Err(ProtocolError::Malformed(
            "empty frame (missing message type)".to_string(),
        ));
    }
    if len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            size: len,
            max: MAX_FRAME_SIZE,
        });
    }
    let mut tag = [0u8; 1];
    read_exact(r, &mut tag, "frame type byte")?;
    let want = len - 1;
    let mut body = Vec::with_capacity(want.min(FRAME_READ_STEP));
    while body.len() < want {
        let start = body.len();
        let step = (want - start).min(FRAME_READ_STEP);
        body.resize(start + step, 0);
        read_exact(r, &mut body[start..], "frame body")?;
    }
    Ok((tag[0], body))
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8], what: &'static str) -> Result<(), ProtocolError> {
    match r.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Err(ProtocolError::Truncated(what)),
        Err(e) => Err(ProtocolError::Io(e)),
    }
}

/// Write one [`Request`] frame.
pub fn write_request<W: Write>(w: &mut W, request: &Request) -> Result<(), ProtocolError> {
    write_frame(w, request.msg_type(), &request.encode_body()?)
}

/// Read one [`Request`] frame.
pub fn read_request<R: Read>(r: &mut R) -> Result<Request, ProtocolError> {
    let (tag, body) = read_frame(r)?;
    Request::decode_body(tag, &body)
}

/// Write one [`Response`] frame.
pub fn write_response<W: Write>(w: &mut W, response: &Response) -> Result<(), ProtocolError> {
    write_frame(w, response.msg_type(), &response.encode_body()?)
}

/// Read one [`Response`] frame.
pub fn read_response<R: Read>(r: &mut R) -> Result<Response, ProtocolError> {
    let (tag, body) = read_frame(r)?;
    Response::decode_body(tag, &body)
}

/// Encode a complete [`Request`] frame (length prefix included) into bytes.
pub fn encode_request(request: &Request) -> Result<Vec<u8>, ProtocolError> {
    let mut buf = Vec::new();
    write_request(&mut buf, request)?;
    Ok(buf)
}

/// Decode one complete [`Request`] frame from bytes.
pub fn decode_request(frame: &[u8]) -> Result<Request, ProtocolError> {
    read_request(&mut &frame[..])
}

/// Encode a complete [`Response`] frame (length prefix included) into bytes.
pub fn encode_response(response: &Response) -> Result<Vec<u8>, ProtocolError> {
    let mut buf = Vec::new();
    write_response(&mut buf, response)?;
    Ok(buf)
}

/// Decode one complete [`Response`] frame from bytes.
pub fn decode_response(frame: &[u8]) -> Result<Response, ProtocolError> {
    read_response(&mut &frame[..])
}

// ---------- primitive codecs ----------

fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_str(buf: &mut Vec<u8>, s: &str) -> Result<(), ProtocolError> {
    let len = u32::try_from(s.len()).map_err(|_| {
        ProtocolError::Malformed(format!("string of {} bytes exceeds u32", s.len()))
    })?;
    put_u32(buf, len);
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

fn count_u32(n: usize, what: &str) -> Result<u32, ProtocolError> {
    u32::try_from(n).map_err(|_| ProtocolError::Malformed(format!("{what} exceeds u32")))
}

fn put_value(buf: &mut Vec<u8>, value: &Value) -> Result<(), ProtocolError> {
    match value {
        Value::Null => put_u8(buf, value_tag::NULL),
        Value::Boolean(b) => {
            put_u8(buf, value_tag::BOOL);
            put_u8(buf, u8::from(*b));
        }
        Value::Integer(i) => {
            put_u8(buf, value_tag::I32);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Bigint(i) => {
            put_u8(buf, value_tag::I64);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Double(x) => {
            put_u8(buf, value_tag::F64);
            buf.extend_from_slice(&x.to_bits().to_le_bytes());
        }
        Value::Varchar(s) => {
            put_u8(buf, value_tag::VARCHAR);
            put_str(buf, s)?;
        }
        Value::Date(d) => {
            put_u8(buf, value_tag::DATE);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(us) => {
            put_u8(buf, value_tag::TIMESTAMP);
            buf.extend_from_slice(&us.to_le_bytes());
        }
        Value::Decimal(x, scale) => {
            put_u8(buf, value_tag::DECIMAL);
            buf.extend_from_slice(&x.to_le_bytes());
            put_u8(buf, *scale);
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            put_u8(buf, value_tag::INTERVAL);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&micros.to_le_bytes());
        }
    }
    Ok(())
}

fn dtype_byte(dtype: DataType) -> u8 {
    match dtype {
        DataType::Boolean => dtype_tag::BOOLEAN,
        DataType::Integer => dtype_tag::INTEGER,
        DataType::Bigint => dtype_tag::BIGINT,
        DataType::Double => dtype_tag::DOUBLE,
        DataType::Varchar => dtype_tag::VARCHAR,
        DataType::Date => dtype_tag::DATE,
        DataType::Timestamp => dtype_tag::TIMESTAMP,
        DataType::Decimal { .. } => dtype_tag::DECIMAL,
        DataType::Interval => dtype_tag::INTERVAL,
    }
}

/// Cursor over one frame body. Every read is checked against the remaining
/// bytes, so a malformed length can never read past the frame.
struct Reader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(body: &'a [u8]) -> Self {
        Reader { body, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.body.len() - self.pos
    }

    fn take(&mut self, n: usize, what: &'static str) -> Result<&'a [u8], ProtocolError> {
        if self.remaining() < n {
            return Err(ProtocolError::Truncated(what));
        }
        let bytes = &self.body[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    fn u8(&mut self, what: &'static str) -> Result<u8, ProtocolError> {
        Ok(self.take(1, what)?[0])
    }

    fn u32(&mut self, what: &'static str) -> Result<u32, ProtocolError> {
        let bytes = self.take(4, what)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self, what: &'static str) -> Result<u64, ProtocolError> {
        let bytes = self.take(8, what)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn string(&mut self, what: &'static str) -> Result<String, ProtocolError> {
        let len = self.u32(what)? as usize;
        let bytes = self.take(len, what)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| ProtocolError::Malformed(format!("{what} is not valid UTF-8")))
    }

    fn value(&mut self, what: &'static str) -> Result<Value, ProtocolError> {
        let tag = self.u8(what)?;
        Ok(match tag {
            value_tag::NULL => Value::Null,
            value_tag::BOOL => match self.u8(what)? {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                other => {
                    return Err(ProtocolError::Malformed(format!(
                        "BOOL payload must be 0 or 1, got {other}"
                    )))
                }
            },
            value_tag::I32 => {
                let b = self.take(4, what)?;
                Value::Integer(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            }
            value_tag::I64 => {
                let b = self.take(8, what)?;
                Value::Bigint(i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            }
            value_tag::F64 => {
                let b = self.take(8, what)?;
                Value::Double(f64::from_bits(u64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])))
            }
            value_tag::VARCHAR => Value::Varchar(self.string(what)?),
            value_tag::DATE => {
                let b = self.take(4, what)?;
                Value::Date(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            }
            value_tag::TIMESTAMP => {
                let b = self.take(8, what)?;
                Value::Timestamp(i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            }
            value_tag::DECIMAL => {
                let b = self.take(16, what)?;
                let mut x = [0u8; 16];
                x.copy_from_slice(b);
                let scale = self.u8(what)?;
                Value::Decimal(i128::from_le_bytes(x), scale)
            }
            value_tag::INTERVAL => {
                let b = self.take(16, what)?;
                Value::Interval {
                    months: i32::from_le_bytes([b[0], b[1], b[2], b[3]]),
                    days: i32::from_le_bytes([b[4], b[5], b[6], b[7]]),
                    micros: i64::from_le_bytes([
                        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
                    ]),
                }
            }
            other => {
                return Err(ProtocolError::Malformed(format!(
                    "unknown value tag {other}"
                )))
            }
        })
    }

    fn dtype(&mut self, what: &'static str) -> Result<DataType, ProtocolError> {
        let tag = self.u8(what)?;
        Ok(match tag {
            dtype_tag::BOOLEAN => DataType::Boolean,
            dtype_tag::INTEGER => DataType::Integer,
            dtype_tag::BIGINT => DataType::Bigint,
            dtype_tag::DOUBLE => DataType::Double,
            dtype_tag::VARCHAR => DataType::Varchar,
            dtype_tag::DATE => DataType::Date,
            dtype_tag::TIMESTAMP => DataType::Timestamp,
            dtype_tag::DECIMAL => {
                let precision = self.u8(what)?;
                let scale = self.u8(what)?;
                pinkdb::types::decimal_type(precision, scale).ok_or_else(|| {
                    ProtocolError::Malformed(format!(
                        "bad DECIMAL precision/scale {precision},{scale}"
                    ))
                })?
            }
            dtype_tag::INTERVAL => DataType::Interval,
            other => {
                return Err(ProtocolError::Malformed(format!(
                    "unknown data type tag {other}"
                )))
            }
        })
    }

    /// Reject trailing bytes: a frame must be consumed exactly.
    fn finish(&self, what: &str) -> Result<(), ProtocolError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(ProtocolError::Malformed(format!(
                "{what} frame has {} trailing bytes",
                self.remaining()
            )))
        }
    }
}
