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

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

use pinkdb::{DataType, QueryResult, Value};

use crate::protocol::{self, DoneKind, PROTOCOL_VERSION, ProtocolError, Request, Response};

/// Everything [`Client`] can fail with.
#[derive(Debug)]
pub enum ClientError {
    /// Wire/transport/handshake failure.
    Protocol(ProtocolError),
    /// The connection is unusable after an earlier decode or transport error:
    /// no further frames may be read from it. Reconnect.
    Broken(String),
    /// TLS could not be set up (bad certificate file, bad server name, ...)
    /// or the TLS handshake failed before the pinkdb handshake.
    #[cfg(feature = "tls")]
    Tls(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Protocol(e) => write!(f, "{e}"),
            ClientError::Broken(msg) => write!(f, "client connection is broken: {msg}"),
            #[cfg(feature = "tls")]
            ClientError::Tls(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Protocol(e) => Some(e),
            ClientError::Broken(_) => None,
            #[cfg(feature = "tls")]
            ClientError::Tls(_) => None,
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

/// The byte stream under a client connection: plain TCP, or TLS over TCP
/// (crate feature `tls`).
///
/// The pinkdb protocol runs over one owned stream that is both readable and
/// writable; the client reads through its `BufReader` and writes through the
/// same stream (`BufReader::get_mut`), which is exactly what rustls'
/// `StreamOwned` requires.
#[derive(Debug)]
enum Transport {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(stream) => stream.read(buf),
            #[cfg(feature = "tls")]
            Transport::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(stream) => stream.write(buf),
            #[cfg(feature = "tls")]
            Transport::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Transport::Plain(stream) => stream.flush(),
            #[cfg(feature = "tls")]
            Transport::Tls(stream) => stream.flush(),
        }
    }
}

/// A blocking client connection. One client = one server-side session.
#[derive(Debug)]
pub struct Client {
    reader: BufReader<Transport>,
    server_version: String,
    protocol_version: u32,
    /// Set after a decode/transport failure: the stream is out of sync, so
    /// further calls must fail fast instead of reading garbage frames.
    broken: bool,
}

/// The most rows one result batch may materialize on the client. Generous for
/// legitimate results (the client materializes anyway), bounded so a hostile
/// server cannot force unbounded growth.
const MAX_BATCH_ROWS: usize = 10_000_000;

impl Client {
    /// Connect and perform the `Hello` handshake, checking the protocol
    /// version the server agrees on.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Client> {
        let stream = TcpStream::connect(addr).map_err(ProtocolError::Io)?;
        configure_client_socket(&stream);
        Self::handshake(Transport::Plain(stream))
    }

    /// Connect over TLS and perform the `Hello` handshake (crate feature
    /// `tls`).
    ///
    /// `server_name` is what the server certificate is verified against: a
    /// DNS name (`db.example.com`) or an IP-address literal (`127.0.0.1`).
    /// `trust` selects the trust anchor, or the loudly unsafe
    /// [`crate::ClientTrust::Insecure`] escape hatch for local tests.
    ///
    /// A TLS client pointed at a *plaintext* server cannot tell the stall
    /// apart from a slow peer: the ClientHello reads as a plausible frame
    /// length, so the failure surfaces only when the server's idle timeout
    /// (or `PINKDB_CLIENT_TIMEOUT_MS` on this side) expires — always an
    /// error, never a served connection.
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        addr: impl ToSocketAddrs,
        server_name: &str,
        trust: crate::tls::ClientTrust,
    ) -> Result<Client> {
        let config = crate::tls::client_config(trust)
            .map_err(|e| ClientError::Tls(format!("TLS configuration failed: {e}")))?;
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| ClientError::Tls(format!("invalid TLS server name: {e}")))?;
        let stream = TcpStream::connect(addr).map_err(ProtocolError::Io)?;
        configure_client_socket(&stream);
        let session = rustls::ClientConnection::new(config, server_name)
            .map_err(|e| ClientError::Tls(format!("cannot start the TLS session: {e}")))?;
        // The TLS handshake runs lazily on the first read/write below; a
        // certificate or protocol failure surfaces as the IO error of the
        // `Hello` write or response read.
        Self::handshake(Transport::Tls(Box::new(rustls::StreamOwned::new(
            session, stream,
        ))))
    }

    /// Configure the transport, then speak the `Hello` handshake.
    fn handshake(transport: Transport) -> Result<Client> {
        let mut client = Client {
            reader: BufReader::new(transport),
            server_version: String::new(),
            protocol_version: PROTOCOL_VERSION,
            broken: false,
        };
        protocol::write_request(
            client.reader.get_mut(),
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
            protocol::write_request(self.reader.get_mut(), &Request::Ping)?;
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
            self.reader.get_mut(),
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
                self.reader.get_mut(),
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
            self.reader.get_mut(),
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
                    // A hostile/compromised server could stream unbounded rows;
                    // the client materializes them, so cap the batch.
                    if accumulated.len() > MAX_BATCH_ROWS {
                        return Err(ProtocolError::Malformed(format!(
                            "result batch exceeds {MAX_BATCH_ROWS} rows; refusing to \
                             materialize it"
                        ))
                        .into());
                    }
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
        if let Err(ClientError::Protocol(e)) = &result
            && !matches!(e, ProtocolError::Server(_))
        {
            self.broken = true;
        }
        result
    }
}

/// Socket options every client connection gets: `TCP_NODELAY`, and a bounded
/// read/write timeout (override with `PINKDB_CLIENT_TIMEOUT_MS`; 0 disables)
/// so a stalling server cannot hang the client forever. With TLS, the same
/// socket timeouts bound the handshake and every encrypted read/write.
fn configure_client_socket(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        eprintln!("pinkdb: cannot set TCP_NODELAY on the client socket: {e}");
    }
    let timeout_ms = std::env::var("PINKDB_CLIENT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300_000);
    if timeout_ms > 0 {
        let timeout = Some(std::time::Duration::from_millis(timeout_ms));
        if let Err(e) = stream.set_read_timeout(timeout) {
            eprintln!("pinkdb: cannot set the client read timeout: {e}");
        }
        if let Err(e) = stream.set_write_timeout(timeout) {
            eprintln!("pinkdb: cannot set the client write timeout: {e}");
        }
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
    if kind == DoneKind::Called {
        // `CALL` (v8, wave 44): the call's own result set (when it has one)
        // arrives as the pending `ResultSet`/`Rows` group; everything else
        // about the call rides the `Done` payload.
        let result = current
            .map(|(names, types, rows)| Box::new(QueryResult::Selected { names, types, rows }));
        return Ok(QueryResult::Called {
            name,
            statements: to_count(count)?,
            result,
        });
    }
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
            .into());
        }
        DoneKind::Selected => unreachable!("Selected returned above"),
        DoneKind::Called => unreachable!("Called returned above"),
    })
}
