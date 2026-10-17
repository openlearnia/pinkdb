//! Blocking client for the pinkdb server protocol.
//!
//! [`Client::execute`] speaks the framed protocol and reconstructs pinkdb's
//! public [`QueryResult`]/[`Value`]/[`DataType`] values, so callers (e.g. the
//! REPL) render remote results with the exact same code as local ones.
//!
//! ```no_run
//! use pinkdb_server::Client;
//!
//! let mut client = Client::connect("127.0.0.1:5433")?;
//! for result in client.execute("SELECT 1;")? {
//!     println!("{result:?}");
//! }
//! # Ok::<(), pinkdb_server::ClientError>(())
//! ```

use std::io::BufReader;
use std::net::{TcpStream, ToSocketAddrs};

use pinkdb::{DataType, QueryResult, Value};

use crate::protocol::{self, DoneKind, ProtocolError, Request, Response, PROTOCOL_VERSION};

/// Everything [`Client`] can fail with.
#[derive(Debug)]
pub enum ClientError {
    /// Wire/transport/handshake failure.
    Protocol(ProtocolError),
    /// The connection is unusable after an earlier decode or transport error:
    /// no further frames may be read from it. Reconnect.
    Broken(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "{e}"),
            ClientError::Broken(msg) => write!(f, "client connection is broken: {msg}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Protocol(e) => Some(e),
            ClientError::Broken(_) => None,
        }
    }
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        ClientError::Protocol(e)
    }
}

/// Client result type.
pub type Result<T> = std::result::Result<T, ClientError>;

/// One result set being assembled from frames: names, types, rows so far.
type PendingResult = Option<(Vec<String>, Vec<DataType>, Vec<Vec<Value>>)>;

/// A blocking client connection. One client = one server-side session.
#[derive(Debug)]
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    server_version: String,
    protocol_version: u32,
    /// Set after a decode/transport failure: the stream is out of sync, so
    /// further calls must fail fast instead of reading garbage frames.
    broken: bool,
}

impl Client {
    /// Connect and perform the `Hello` handshake, checking the protocol
    /// version the server agrees on.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Client> {
        let stream = TcpStream::connect(addr).map_err(ProtocolError::Io)?;
        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("pinkdb: cannot set TCP_NODELAY on the client socket: {e}");
        }
        let writer = stream.try_clone().map_err(ProtocolError::Io)?;
        let mut client = Client {
            reader: BufReader::new(stream),
            writer,
            server_version: String::new(),
            protocol_version: PROTOCOL_VERSION,
            broken: false,
        };
        protocol::write_request(
            &mut client.writer,
            &Request::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        )?;
        match protocol::read_response(&mut client.reader)? {
            Response::HelloOk {
                server_version,
                protocol_version,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(ProtocolError::VersionMismatch {
                        server: protocol_version,
                        client: PROTOCOL_VERSION,
                    }
                    .into());
                }
                client.server_version = server_version;
                client.protocol_version = protocol_version;
                Ok(client)
            }
            Response::Error { message } => Err(ProtocolError::Server(message).into()),
            other => Err(ProtocolError::Unexpected(format!(
                "expected HelloOk, got {}",
                other.kind()
            ))
            .into()),
        }
    }

    /// The server's version string (from the handshake).
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// The protocol version agreed in the handshake.
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Liveness probe.
    pub fn ping(&mut self) -> Result<()> {
        self.check_usable()?;
        let result = (|| {
            protocol::write_request(&mut self.writer, &Request::Ping)?;
            match protocol::read_response(&mut self.reader)? {
                Response::Pong => Ok(()),
                Response::Error { message } => Err(ProtocolError::Server(message).into()),
                other => Err(ProtocolError::Unexpected(format!(
                    "expected Pong, got {}",
                    other.kind()
                ))
                .into()),
            }
        })();
        self.finish(result)
    }

    /// Run every statement in `sql` on the server, returning one result each.
    ///
    /// The server sends one result group per statement and then a
    /// `BatchDone { statements }` terminator; this reads groups until that
    /// frame, so neither side needs to parse SQL just to count statements.
    /// A failing statement stops the batch and comes back as a single
    /// [`ClientError::Protocol`] `Server` error — matching the embedded
    /// engine's all-or-nothing `execute` (earlier statements' results are
    /// never sent).
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        self.check_usable()?;
        let result = self.execute_inner(sql);
        self.finish(result)
    }

    fn execute_inner(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        protocol::write_request(
            &mut self.writer,
            &Request::Execute {
                sql: sql.to_string(),
            },
        )?;
        self.read_batch()
    }

    /// v5: parse one statement with `?` placeholders into a connection-local
    /// prepared statement; returns `(handle, parameter_count)`.
    pub fn prepare(&mut self, sql: &str) -> Result<(u64, usize)> {
        self.check_usable()?;
        let result = (|| {
            protocol::write_request(
                &mut self.writer,
                &Request::Prepare {
                    sql: sql.to_string(),
                },
            )?;
            match protocol::read_response(&mut self.reader)? {
                Response::Prepared {
                    handle,
                    parameter_count,
                } => Ok((handle, parameter_count as usize)),
                Response::Error { message } => Err(ProtocolError::Server(message).into()),
                other => Err(ProtocolError::Unexpected(format!(
                    "expected Prepared, got {}",
                    other.kind()
                ))
                .into()),
            }
        })();
        self.finish(result)
    }

    /// v5: execute a connection-local prepared statement with one value per
    /// `?` (textual order). Framed exactly like a one-statement
    /// [`Client::execute`]: one result group, then `BatchDone`.
    pub fn execute_prepared(&mut self, handle: u64, values: &[Value]) -> Result<Vec<QueryResult>> {
        self.check_usable()?;
        let result = self.execute_prepared_inner(handle, values);
        self.finish(result)
    }

    fn execute_prepared_inner(
        &mut self,
        handle: u64,
        values: &[Value],
    ) -> Result<Vec<QueryResult>> {
        protocol::write_request(
            &mut self.writer,
            &Request::ExecutePrepared {
                handle,
                values: values.to_vec(),
            },
        )?;
        self.read_batch()
    }

    /// Read result groups until the `BatchDone` terminator; shared by
    /// `Execute` and `ExecutePrepared`.
    fn read_batch(&mut self) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();
        let mut current: PendingResult = None;
        loop {
            match protocol::read_response(&mut self.reader)? {
                Response::ResultSet { names, types } => {
                    if names.len() != types.len() {
                        return Err(ProtocolError::Malformed(format!(
                            "ResultSet has {} names but {} types",
                            names.len(),
                            types.len()
                        ))
                        .into());
                    }
                    if current.is_some() {
                        return Err(ProtocolError::Malformed(
                            "nested ResultSet frames".to_string(),
                        )
                        .into());
                    }
                    current = Some((names, types, Vec::new()));
                }
                Response::Rows { rows } => {
                    let Some((names, _, accumulated)) = current.as_mut() else {
                        return Err(ProtocolError::Malformed(
                            "Rows frame before any ResultSet".to_string(),
                        )
                        .into());
                    };
                    for row in &rows {
                        if row.len() != names.len() {
                            return Err(ProtocolError::Malformed(format!(
                                "row has {} values but the result set has {} columns",
                                row.len(),
                                names.len()
                            ))
                            .into());
                        }
                    }
                    accumulated.extend(rows);
                }
                Response::Done { kind, count, name } => {
                    results.push(build_result(kind, count, name, current.take())?);
                }
                Response::BatchDone { statements } => {
                    if current.is_some() {
                        return Err(ProtocolError::Malformed(
                            "BatchDone before the pending result set's Done".to_string(),
                        )
                        .into());
                    }
                    if results.len() != statements as usize {
                        return Err(ProtocolError::Malformed(format!(
                            "BatchDone reports {statements} statement(s) but {} result group(s) arrived",
                            results.len()
                        ))
                        .into());
                    }
                    return Ok(results);
                }
                Response::Error { message } => {
                    return Err(ProtocolError::Server(message).into());
                }
                other => {
                    return Err(ProtocolError::Unexpected(format!(
                        "expected a result frame, got {}",
                        other.kind()
                    ))
                    .into());
                }
            }
        }
    }

    /// Fail fast when the stream is known to be out of sync.
    fn check_usable(&self) -> Result<()> {
        if self.broken {
            return Err(ClientError::Broken(
                "an earlier protocol or transport error desynchronized the stream; reconnect"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Mark the client broken on fatal protocol failures, then pass the
    /// result through. A server-reported SQL error is not fatal: the
    /// connection stays usable, exactly like the embedded engine.
    fn finish<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(ClientError::Protocol(e)) = &result {
            if !matches!(e, ProtocolError::Server(_)) {
                self.broken = true;
            }
        }
        result
    }
}

fn build_result(
    kind: DoneKind,
    count: u64,
    name: String,
    current: PendingResult,
) -> Result<QueryResult> {
    let to_count = |n: u64| -> Result<usize> {
        usize::try_from(n)
            .map_err(|_| ProtocolError::Malformed(format!("count {n} exceeds usize")).into())
    };
    if kind == DoneKind::Selected {
        let (names, types, rows) = current.ok_or_else(|| {
            ProtocolError::Malformed("Done(Selected) without a ResultSet".to_string())
        })?;
        return Ok(QueryResult::Selected { names, types, rows });
    }
    // A non-row result must not carry a half-built result set.
    if current.is_some() {
        return Err(
            ProtocolError::Malformed(format!("Done({kind:?}) after a ResultSet frame")).into(),
        );
    }
    Ok(match kind {
        DoneKind::Inserted => QueryResult::Inserted {
            count: to_count(count)?,
        },
        DoneKind::Updated => QueryResult::Updated {
            count: to_count(count)?,
        },
        DoneKind::Deleted => QueryResult::Deleted {
            count: to_count(count)?,
        },
        DoneKind::Created => QueryResult::Created,
        DoneKind::Began => QueryResult::Began,
        DoneKind::Committed => QueryResult::Committed,
        DoneKind::RolledBack => QueryResult::RolledBack,
        DoneKind::CreatedView => QueryResult::CreatedView { name },
        DoneKind::DroppedTable => QueryResult::DroppedTable { name },
        DoneKind::DroppedView => QueryResult::DroppedView { name },
        DoneKind::CreatedDatabase => QueryResult::CreatedDatabase { name },
        DoneKind::DroppedDatabase => QueryResult::DroppedDatabase { name },
        DoneKind::Used => QueryResult::Used { db: name },
        DoneKind::LoadedExtension => QueryResult::LoadedExtension { name },
        DoneKind::Checkpointed => QueryResult::Checkpointed,
        DoneKind::AlteredTable => QueryResult::AlteredTable { name },
        DoneKind::Other => {
            return Err(ProtocolError::Malformed(
                "server sent the reserved Done kind Other".to_string(),
            )
            .into())
        }
        DoneKind::Selected => unreachable!("Selected returned above"),
    })
}
