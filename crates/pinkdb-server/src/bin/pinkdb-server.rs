//! `pinkdb-server` — run a pinkdb database over TCP.
//!
//! ```text
//! pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]
//!               [--max-connections N] [--idle-timeout SECS]
//!               [--statement-timeout-ms N] [--max-result-rows N]
//!               [--max-result-bytes N] [--max-concurrent-logins N]
//!               [--transport sync|async] [--allow-load-extension]
//!               [--extension-root PATH]
//!               [--tls-cert PATH --tls-key PATH]        (built with `tls`)
//! ```
//!
//! Without `--file` the database is in-memory; with it, the server opens the
//! durable checkpoint + WAL path (wave 22): rows acked to a client survive a
//! server crash and are replayed on the next start. Authentication is opt-in
//! per store: while the store has no users every connection is trusted, and
//! once a user exists every statement request must log in first. Bind
//! localhost until the store has users.
//!
//! `--transport sync` (the default) is the blocking thread-per-connection
//! server. `--transport async` is the tokio transport from the optional
//! `async-server` feature: same protocol, same engine (which stays
//! synchronous — see the crate docs' "Async carve-out" section), same
//! robustness knobs; the flag needs a build with that feature.
//!
//! `--tls-cert` + `--tls-key` (both, or neither) serve the same protocol over
//! rustls-encrypted TCP; they are only accepted by a binary built with the
//! `tls` feature **and** only by the default `sync` transport. The async
//! transport has no TLS path, so `--transport async` combined with either TLS
//! flag is a usage error (exit 2) instead of a silent plaintext downgrade.
//! TLS encrypts the transport and identifies the server; it does **not**
//! identify the client — login is what gates statements once the store has
//! users. Bind localhost until then.

use std::process::ExitCode;

use pinkdb::{Database, DurableOpenOptions};
use std::time::Duration;

use pinkdb_server::{
    DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONCURRENT_LOGINS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS, DEFAULT_STATEMENT_TIMEOUT_MS, Server,
};

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
    /// Per-statement engine deadline (milliseconds); always positive.
    statement_timeout_ms: u64,
    /// Total materialized-row cap per statement; always positive.
    max_result_rows: usize,
    /// Total materialized-byte cap per statement; always positive.
    max_result_bytes: usize,
    /// Global cap on concurrent Argon2id login verifications.
    max_concurrent_logins: usize,
    transport: Transport,
    allow_load_extension: bool,
    /// `--extension-root PATH`: the directory every extension load is
    /// confined to.
    extension_root: Option<String>,
    /// `--tls-cert PATH`: must be paired with `--tls-key`.
    tls_cert: Option<String>,
    /// `--tls-key PATH`: must be paired with `--tls-cert`.
    tls_key: Option<String>,
}

fn usage() -> &'static str {
    "Usage: pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]\n\
     \x20                    [--max-connections N] [--idle-timeout SECS]\n\
     \x20                    [--statement-timeout-ms N] [--max-result-rows N]\n\
     \x20                    [--max-result-bytes N] [--max-concurrent-logins N]\n\
     \x20                    [--transport sync|async] [--allow-load-extension]\n\
     \x20                    [--extension-root PATH]\n\
     \x20                    [--tls-cert PATH.pem --tls-key PATH.pem]\n\
     \n\
     \x20 --host HOST            bind address (default 127.0.0.1)\n\
     \x20 --port PORT            TCP port; 0 picks an ephemeral port (default 5433)\n\
     \x20 --file PATH            durable database (checkpoint + WAL); in-memory when omitted\n\
     \x20 --max-connections N    concurrent clients served (default 128)\n\
     \x20 --idle-timeout SECS    disconnect clients idle this long; 0 = never (default 300)\n\
     \x20 --statement-timeout-ms N  per-statement execution deadline in ms (default 30000);\n\
     \x20                        checked at chunk boundaries; a timed-out statement\n\
     \x20                        returns an error and never holds the write lock\n\
     \x20 --max-result-rows N   max rows one statement may materialize, join output\n\
     \x20                        included (default 10000000)\n\
     \x20 --max-result-bytes N  max estimated bytes one statement may materialize\n\
     \x20                        (default 268435456 = 256 MiB)\n\
     \x20 --max-concurrent-logins N  concurrent Argon2id verifications (default 4);\n\
     \x20                        excess logins get 'too many concurrent logins'\n\
     \x20 --transport MODE       sync = blocking thread-per-connection (default);\n\
     \x20                        the only transport with TLS support\n\
     \x20                        async = tokio I/O, same protocol/engine (needs a\n\
     \x20                        build with the `async-server` feature); the async transport\n\
     \x20                        has no TLS, so --tls-cert/--tls-key are refused at startup\n\
     \x20 --allow-load-extension let clients run LOAD EXTENSION (loads native code on\n\
     \x20                        the server host; off by default)\n\
     \x20 --extension-root PATH  confine every extension load to this directory (a path\n\
     \x20                        outside it is refused; a bare library name, a `..`\n\
     \x20                        component and a symlinked library are always refused)\n\
     \x20 --tls-cert PATH        serve TLS with this PEM certificate chain (sync only)\n\
     \x20 --tls-key PATH         PEM private key matching --tls-cert (PKCS#8/#1/SEC1);\n\
     \x20                        both or neither, only in a build with `tls`, and\n\
     \x20                        only with the sync transport\n\
     \n\
     At-rest encryption is not wired into the server: PINKDB_KEY is ignored and\n\
     --file must be a plaintext database (an encrypted one is refused at startup).\n\
     \n\
     Security: authentication is opt-in per store — while the store has no\n\
     users every connection is trusted and can run any SQL; once a user\n\
     exists, every statement request requires Login first. TLS only encrypts\n\
     the transport (it identifies the server, not clients). Keep the bind\n\
     address on localhost or a trusted network until the store has users."
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        host: DEFAULT_HOST.to_string(),
        port: DEFAULT_PORT,
        file: None,
        max_connections: DEFAULT_MAX_CONNECTIONS,
        idle_timeout_secs: DEFAULT_IDLE_TIMEOUT.as_secs(),
        statement_timeout_ms: DEFAULT_STATEMENT_TIMEOUT_MS,
        max_result_rows: DEFAULT_MAX_RESULT_ROWS,
        max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        max_concurrent_logins: DEFAULT_MAX_CONCURRENT_LOGINS,
        transport: Transport::Sync,
        allow_load_extension: false,
        extension_root: None,
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
            "--statement-timeout-ms" => {
                let raw = value("--statement-timeout-ms")?;
                args.statement_timeout_ms = raw
                    .parse()
                    .ok()
                    .filter(|n: &u64| *n > 0)
                    .ok_or_else(|| format!("invalid --statement-timeout-ms value {raw:?}"))?;
            }
            "--max-result-rows" => {
                let raw = value("--max-result-rows")?;
                args.max_result_rows = raw
                    .parse()
                    .ok()
                    .filter(|n: &usize| *n > 0)
                    .ok_or_else(|| format!("invalid --max-result-rows value {raw:?}"))?;
            }
            "--max-result-bytes" => {
                let raw = value("--max-result-bytes")?;
                args.max_result_bytes = raw
                    .parse()
                    .ok()
                    .filter(|n: &usize| *n > 0)
                    .ok_or_else(|| format!("invalid --max-result-bytes value {raw:?}"))?;
            }
            "--max-concurrent-logins" => {
                let raw = value("--max-concurrent-logins")?;
                args.max_concurrent_logins = raw
                    .parse()
                    .ok()
                    .filter(|n: &usize| *n > 0)
                    .ok_or_else(|| format!("invalid --max-concurrent-logins value {raw:?}"))?;
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
            "--extension-root" => args.extension_root = Some(value("--extension-root")?),
            "--tls-cert" => args.tls_cert = Some(value("--tls-cert")?),
            "--tls-key" => args.tls_key = Some(value("--tls-key")?),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    // H-2: the async transport has no TLS path. Refuse the flags outright
    // rather than accepting and ignoring them (which would serve plaintext on
    // a port the operator asked to encrypt). This check precedes the pairing
    // check so either flag alone gets the transport-specific message; the
    // sync path keeps its existing both-or-neither error unchanged.
    #[cfg(feature = "async-server")]
    if args.transport == Transport::Async && (args.tls_cert.is_some() || args.tls_key.is_some()) {
        return Err(
            "--transport async does not support TLS; use the default sync transport with \
             --tls-cert/--tls-key, or remove the TLS flags."
                .to_string(),
        );
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

    // The extension root: every extension load is confined to it.
    let extension_root: Option<std::path::PathBuf> = match &args.extension_root {
        Some(root) => {
            let root = std::path::PathBuf::from(root);
            if !root.is_dir() {
                eprintln!(
                    "Error: --extension-root {:?} is not a directory",
                    root.display()
                );
                return ExitCode::from(2);
            }
            Some(root)
        }
        None => None,
    };

    let db = match &args.file {
        Some(path) => {
            // The durable open carries the extension policy: a WAL that
            // records an extension is only replayed into a load when the
            // operator opted in *and* the library is inside the extension
            // root. Without the opt-in the open fails with an actionable
            // error instead of loading a path the file names.
            let opts = DurableOpenOptions {
                allow_extension_replay: args.allow_load_extension,
                extension_root: extension_root.clone(),
                ..DurableOpenOptions::default()
            };
            match Database::open_with_options(path, opts) {
                Ok(db) => db,
                Err(e) => {
                    eprintln!("Error: cannot open durable database {path:?}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        None => {
            let db = Database::new();
            db.set_extension_root(extension_root.clone());
            db
        }
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
                    .statement_timeout(Duration::from_millis(args.statement_timeout_ms))
                    .max_result_rows(args.max_result_rows)
                    .max_result_bytes(args.max_result_bytes)
                    .max_concurrent_logins(args.max_concurrent_logins)
                    .allow_load_extension(args.allow_load_extension)
                    .extension_root(extension_root.clone()),
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
                    .statement_timeout(Duration::from_millis(args.statement_timeout_ms))
                    .max_result_rows(args.max_result_rows)
                    .max_result_bytes(args.max_result_bytes)
                    .max_concurrent_logins(args.max_concurrent_logins)
                    .allow_load_extension(args.allow_load_extension)
                    .extension_root(extension_root.clone()),
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
            "pinkdb-server: TLS encrypts the transport only — it identifies the server, not \
             the client; authentication is required once the store has users"
        );
    } else {
        eprintln!(
            "pinkdb-server: authentication is required once the store has users; while it \
             has none, every connection is trusted and this port is plaintext — keep the \
             bind address trusted"
        );
    }
}
