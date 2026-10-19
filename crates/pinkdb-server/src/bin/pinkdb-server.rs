//! `pinkdb-server` — run a pinkdb database over TCP.
//!
//! ```text
//! pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]
//!               [--max-connections N] [--idle-timeout SECS]
//!               [--transport sync|async] [--allow-load-extension]
//!               [--tls-cert PATH --tls-key PATH]        (built with `tls`)
//! ```
//!
//! Without `--file` the database is in-memory; with it, the server opens the
//! durable checkpoint + WAL path (wave 22): rows acked to a client survive a
//! server crash and are replayed on the next start. No authentication and no
//! TLS in this wave — bind localhost.
//!
//! `--transport sync` (the default) is the blocking thread-per-connection
//! server. `--transport async` is the tokio transport from the optional
//! `async-server` feature: same protocol, same engine (which stays
//! synchronous — see the crate docs' "Async carve-out" section), same
//! robustness knobs; the flag needs a build with that feature.
//! server crash and are replayed on the next start.
//!
//! `--tls-cert` + `--tls-key` (both, or neither) serve the same protocol over
//! rustls-encrypted TCP; they are only accepted by a binary built with the
//! `tls` feature. TLS encrypts the transport — there is still **no
//! authentication**: every peer that completes a handshake (and, by default,
//! that is every peer) gets the full SQL surface. Bind localhost.

use std::process::ExitCode;

use pinkdb::Database;
use std::time::Duration;

use pinkdb_server::{DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS, Server};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5433;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Transport {
    Sync,
    /// The tokio transport, present only in `async-server` builds (the
    /// parser refuses `--transport async` otherwise).
    #[cfg(feature = "async-server")]
    Async,
}

struct Args {
    host: String,
    port: u16,
    file: Option<String>,
    max_connections: usize,
    idle_timeout_secs: u64,
    transport: Transport,
    allow_load_extension: bool,
    /// `--tls-cert PATH`: must be paired with `--tls-key`.
    tls_cert: Option<String>,
    /// `--tls-key PATH`: must be paired with `--tls-cert`.
    tls_key: Option<String>,
}

fn usage() -> &'static str {
    "Usage: pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]\n\
     \x20                    [--max-connections N] [--idle-timeout SECS]\n\
     \x20                    [--transport sync|async] [--allow-load-extension]\n\
     \x20                    [--tls-cert PATH.pem --tls-key PATH.pem]\n\
     \n\
     \x20 --host HOST            bind address (default 127.0.0.1)\n\
     \x20 --port PORT            TCP port; 0 picks an ephemeral port (default 5433)\n\
     \x20 --file PATH            durable database (checkpoint + WAL); in-memory when omitted\n\
     \x20 --max-connections N    concurrent clients served (default 128)\n\
     \x20 --idle-timeout SECS    disconnect clients idle this long; 0 = never (default 300)\n\
     \x20 --transport MODE       sync = blocking thread-per-connection (default);\n\
     \x20                        async = tokio I/O, same protocol/engine (needs a\n\
     \x20                        build with the `async-server` feature)\n\
     \x20 --allow-load-extension let clients run LOAD EXTENSION (loads native code on\n\
     \x20                        the server host; off by default)\n\
     \x20 --tls-cert PATH        serve TLS with this PEM certificate chain\n\
     \x20 --tls-key PATH         PEM private key matching --tls-cert (PKCS#8/#1/SEC1);\n\
     \x20                        both or neither, and only in a build with `tls`\n\
     \n\
     At-rest encryption is not wired into the server: PINKDB_KEY is ignored and\n\
     --file must be a plaintext database (an encrypted one is refused at startup).\n\
     \n\
     Security: no authentication (TLS only encrypts the transport; it does not\n\
     identify clients). Keep the bind address on localhost or a trusted network;\n\
     anyone who can reach the port has full SQL access."
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        host: DEFAULT_HOST.to_string(),
        port: DEFAULT_PORT,
        file: None,
        max_connections: DEFAULT_MAX_CONNECTIONS,
        idle_timeout_secs: DEFAULT_IDLE_TIMEOUT.as_secs(),
        transport: Transport::Sync,
        allow_load_extension: false,
        tls_cert: None,
        tls_key: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
            None => (arg, None),
        };
        let mut value = |name: &str| -> Result<String, String> {
            match &inline {
                Some(v) => Ok(v.clone()),
                None => it.next().ok_or_else(|| format!("{name} requires a value")),
            }
        };
        match flag.as_str() {
            "-h" | "--help" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            "--host" => args.host = value("--host")?,
            "--port" => {
                let raw = value("--port")?;
                args.port = raw
                    .parse()
                    .map_err(|_| format!("invalid --port value {raw:?}"))?;
            }
            "--file" => args.file = Some(value("--file")?),
            "--max-connections" => {
                let raw = value("--max-connections")?;
                args.max_connections = raw
                    .parse()
                    .ok()
                    .filter(|n: &usize| *n > 0)
                    .ok_or_else(|| format!("invalid --max-connections value {raw:?}"))?;
            }
            "--idle-timeout" => {
                let raw = value("--idle-timeout")?;
                args.idle_timeout_secs = raw
                    .parse()
                    .map_err(|_| format!("invalid --idle-timeout value {raw:?}"))?;
            }
            "--transport" => {
                let raw = value("--transport")?;
                match raw.as_str() {
                    "sync" => args.transport = Transport::Sync,
                    #[cfg(feature = "async-server")]
                    "async" => args.transport = Transport::Async,
                    #[cfg(not(feature = "async-server"))]
                    "async" => {
                        return Err(
                            "--transport async requires a build with the `async-server` \
                             feature (this build has the blocking transport only)"
                                .to_string(),
                        );
                    }
                    other => return Err(format!("invalid --transport value {other:?}")),
                }
            }
            "--allow-load-extension" if inline.is_none() => args.allow_load_extension = true,
            "--tls-cert" => args.tls_cert = Some(value("--tls-cert")?),
            "--tls-key" => args.tls_key = Some(value("--tls-key")?),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    // Pairing is validated before anything binds, so a half-configured TLS
    // listener never starts: both flags or neither.
    if args.tls_cert.is_some() != args.tls_key.is_some() {
        return Err(
            "--tls-cert and --tls-key must be given together (both or neither)".to_string(),
        );
    }
    Ok(args)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("Error: {msg}");
            eprintln!("{}", usage());
            return ExitCode::from(2);
        }
    };

    // TLS was asked for: refuse precisely when this binary cannot provide it.
    if args.tls_cert.is_some() && !cfg!(feature = "tls") {
        eprintln!(
            "Error: TLS was requested, but this pinkdb-server was built without TLS support."
        );
        eprintln!("       rebuild with `cargo build -p pinkdb-server --features tls`.");
        return ExitCode::from(2);
    }

    let db = match &args.file {
        Some(path) => match Database::open(path) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error: cannot open durable database {path:?}: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => Database::new(),
    };

    let mode = match &args.file {
        Some(path) => format!("durable: {path}"),
        None => "in-memory".to_string(),
    };

    match args.transport {
        Transport::Sync => {
            let bound = match (&args.tls_cert, &args.tls_key) {
                (Some(cert), Some(key)) => {
                    #[cfg(feature = "tls")]
                    {
                        Server::bind_tls_with_max_connections(
                            (args.host.as_str(), args.port),
                            db,
                            args.max_connections,
                            cert,
                            key,
                        )
                    }
                    #[cfg(not(feature = "tls"))]
                    {
                        let _ = (cert, key);
                        unreachable!("TLS flags are validated in parse_args")
                    }
                }
                _ => Server::bind_with_max_connections(
                    (args.host.as_str(), args.port),
                    db,
                    args.max_connections,
                ),
            };
            let server = match bound {
                Ok(server) => server
                    .idle_timeout(Some(Duration::from_secs(args.idle_timeout_secs)))
                    .allow_load_extension(args.allow_load_extension),
                Err(e) => {
                    eprintln!("Error: cannot bind {}:{}: {e}", args.host, args.port);
                    return ExitCode::FAILURE;
                }
            };
            announce(
                server.local_addr(),
                &mode,
                if args.tls_cert.is_some() {
                    "TLS"
                } else {
                    "plaintext"
                },
            );
            match server.run() {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("Error: accept loop stopped: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        #[cfg(feature = "async-server")]
        Transport::Async => {
            let server = match pinkdb_server::AsyncServer::bind_with_max_connections(
                (args.host.as_str(), args.port),
                db,
                args.max_connections,
            ) {
                Ok(server) => server
                    .idle_timeout(Some(Duration::from_secs(args.idle_timeout_secs)))
                    .allow_load_extension(args.allow_load_extension),
                Err(e) => {
                    eprintln!("Error: cannot bind {}:{}: {e}", args.host, args.port);
                    return ExitCode::FAILURE;
                }
            };
            announce(server.local_addr(), &mode, "async");
            match server.run_blocking() {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("Error: async accept loop stopped: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// The listening line is the readiness signal (tests wait for it).
fn announce(addr: std::net::SocketAddr, mode: &str, transport: &str) {
    println!(
        "pinkdb-server v{} listening on {} ({mode}, transport: {transport})",
        env!("CARGO_PKG_VERSION"),
        addr
    );
    if transport == "TLS" {
        eprintln!(
            "pinkdb-server: TLS encrypts the transport only — there is no authentication; \
             keep the bind address trusted"
        );
    } else {
        eprintln!(
            "pinkdb-server: no authentication, no TLS on this transport — keep the bind \
             address trusted"
        );
    }
}
