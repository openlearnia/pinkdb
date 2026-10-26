//! `pinkdb-server` — run a pinkdb database over TCP.
//!
//! ```text
//! pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]
//!               [--max-connections N] [--idle-timeout SECS]
//!               [--statement-timeout-ms N] [--max-result-rows N]
//!               [--max-result-bytes N] [--max-concurrent-logins N]
//!               [--transport sync|async] [--allow-load-extension]
//!               [--extension-root PATH]
//!               [--allow-copy-to --copy-root PATH]
//!               [--mcp-extension PATH [--mcp-port PORT] [--mcp-user NAME]]
//!               [--tls-cert PATH --tls-key PATH]        (built with `tls`)
//!               [--enable-ui --ui-host HOST --ui-port PORT]  (built with `ui`)
//! pinkdb-server --health-check [HOST:PORT]
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
//!
//! `--health-check [HOST:PORT]` is a one-shot liveness probe for container
//! orchestrators: connect, speak the protocol `Ping`, print a line, and exit
//! 0 (healthy) or 1 (unhealthy) within a short timeout. It needs no shell,
//! curl or extra tools, so it works in a minimal image. Without an explicit
//! address it reads `PINKDB_HEALTHCHECK_ADDR`, then `PINKDB_HOST`/
//! `PINKDB_PORT`, then `127.0.0.1:5433`; `PINKDB_HEALTHCHECK_TIMEOUT_MS`
//! bounds the probe (default 2000 ms). When `PINKDB_TLS_CERT` is set (the
//! container serves TLS), the probe checks that the port accepts a TCP
//! connection instead: a TLS server refuses the plaintext `Ping`, and a
//! localhost probe cannot generally verify a self-signed certificate.

use std::process::ExitCode;

use pinkdb::{Database, DurableOpenOptions};
use std::time::Duration;

#[cfg(feature = "catalog")]
use pinkdb_server::CatalogStore;
use pinkdb_server::{
    Client, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONCURRENT_LOGINS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS, DEFAULT_STATEMENT_TIMEOUT_MS, Server,
};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5433;
/// The bridge's own listener; separate from the wire protocol's port.
const DEFAULT_MCP_PORT: u16 = 7801;
const DEFAULT_HEALTH_CHECK_TIMEOUT_MS: u64 = 2_000;

/// The console's default bind address: loopback only.
#[cfg(feature = "ui")]
const DEFAULT_UI_HOST: &str = "127.0.0.1";
/// The console's default port.
#[cfg(feature = "ui")]
const DEFAULT_UI_PORT: u16 = 8080;
/// The console's runtime threads.
#[cfg(feature = "ui")]
const DEFAULT_UI_WORKERS: usize = 4;
/// The console's concurrent connections.
#[cfg(feature = "ui")]
const DEFAULT_UI_MAX_CONNECTIONS: usize = 64;
/// The console's row-preview page-size bound.
#[cfg(feature = "ui")]
const DEFAULT_UI_MAX_PAGE_SIZE: usize = 500;

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
    /// `--catalog CATALOG`: keep the store in a pink-lake lake (wave 96b) —
    /// the engine metadata in the catalog database, rows in `--data`.
    /// Mutually exclusive with `--file`.
    catalog: Option<String>,
    /// `--data DIR|s3://BUCKET/PREFIX`: the lake's data location; required
    /// with `--catalog`.
    data: Option<String>,
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
    /// `--extension-pin PATH=SHA256` (repeatable): require every library a
    /// client loads to be one of these paths with exactly this hash.
    extension_pins: Vec<String>,
    /// `--allow-copy-to`: let clients run COPY … TO (off by default; ADMIN is
    /// still required in an authenticated session).
    /// Wave 105: the MCP bridge extension (`libpinkdb_mcp.*`). Its presence
    /// starts a loopback HTTP listener in front of the same store.
    mcp_extension: Option<String>,
    mcp_port: u16,
    mcp_user: Option<String>,
    allow_copy_to: bool,
    /// `--copy-root PATH`: the directory every COPY … TO destination is
    /// confined to. Opting in without it refuses every COPY.
    copy_root: Option<String>,
    /// `--tls-cert PATH`: must be paired with `--tls-key`.
    tls_cert: Option<String>,
    /// `--tls-key PATH`: must be paired with `--tls-cert`.
    tls_key: Option<String>,
    /// Wave 78: serve the read-only warehouse browsing console.
    #[cfg(feature = "ui")]
    enable_ui: bool,
    /// `--ui-host HOST`: the console's bind address (loopback by default).
    #[cfg(feature = "ui")]
    ui_host: String,
    /// `--ui-port PORT`: the console's TCP port.
    #[cfg(feature = "ui")]
    ui_port: u16,
    /// `--ui-auth basic`: require HTTP Basic authentication against the
    /// store's own users.
    #[cfg(feature = "ui")]
    ui_auth_basic: bool,
    /// `--ui-token TOKEN`: require this bearer token.
    #[cfg(feature = "ui")]
    ui_token: Option<String>,
    /// `--ui-workers N`: the console's runtime threads.
    #[cfg(feature = "ui")]
    ui_workers: usize,
    /// `--ui-max-connections N`: the console's concurrent connections.
    #[cfg(feature = "ui")]
    ui_max_connections: usize,
    /// `--ui-max-page-size N`: the console's page-size bound.
    #[cfg(feature = "ui")]
    ui_max_page_size: usize,
}

fn usage() -> &'static str {
    "Usage: pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]\n\
     \x20       pinkdb-server [--host HOST] [--port PORT] --catalog CATALOG --data DIR\n\
     \x20                    [--max-connections N] [--idle-timeout SECS]\n\
     \x20                    [--statement-timeout-ms N] [--max-result-rows N]\n\
     \x20                    [--max-result-bytes N] [--max-concurrent-logins N]\n\
     \x20                    [--transport sync|async] [--allow-load-extension]\n\
     \x20                    [--extension-root PATH [--extension-pin PATH=SHA256]]\n\
     \x20                    [--allow-copy-to --copy-root PATH]\n\
     \x20                    [--tls-cert PATH.pem --tls-key PATH.pem]\n\
     \x20                    [--mcp-extension PATH [--mcp-port PORT] [--mcp-user NAME]]\n\
     \x20       pinkdb-server --health-check [HOST:PORT]\n\
     \n\
     \x20 --host HOST            bind address (default 127.0.0.1)\n\
     \x20 --port PORT            TCP port; 0 picks an ephemeral port (default 5433)\n\
     \x20 --file PATH            durable database (checkpoint + WAL); in-memory when omitted\n\
     \x20 --catalog CATALOG      keep the store in a pink-lake lake (wave 96b): engine\n\
     \x20                        metadata in the catalog database, rows in --data\n\
     \x20                        (pink:PATH, sqlite:PATH, or a postgres: URL; the\n\
     \x20                        backends are build features)\n\
     \x20 --data DIR|s3://…      the lake's data location; required with --catalog\n\
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
     \x20 --extension-pin PATH=SHA256  pin a library's contents: with any pin given,\n\
     \x20                        a load of an unpinned path or of a file whose SHA-256\n\
     \x20                        differs from its pin is refused (repeatable)\n\
     \x20 --allow-copy-to        let clients run COPY TO (writes files on the server\n\
     \x20                        host; off by default; ADMIN is also required)\n\
     \x20 --copy-root PATH       confine every COPY TO destination to this directory\n\
     \x20                        (required with --allow-copy-to; `..`, absolute\n\
     \x20                        escapes and symlinked parents/targets are refused)\n\
     \x20 --tls-cert PATH        serve TLS with this PEM certificate chain (sync only)\n\
     \x20 --tls-key PATH         PEM private key matching --tls-cert (PKCS#8/#1/SEC1);\n\
     \x20                        both or neither, only in a build with `tls`, and\n\
     \x20                        only with the sync transport\n\
     \x20 --mcp-extension PATH  serve the MCP bridge (wave 105) for this store on\n\
     \x20                        127.0.0.1:7801: agents call the tools, the bridge runs\n\
     \x20                        every statement under this server's login and grants,\n\
     \x20                        read-only (the model brings the reasoning; no LLM\n\
     \x20                        code runs here). Needs libpinkdb_mcp.<ext>.\n\
     \x20 --mcp-port PORT        the bridge's loopback port, 0 to let the OS pick
\
     \x20                        (default 7801)\n\
     \x20 --mcp-user NAME        the store user the bridge logs in as; with users\n\
     \x20                        defined this is required, and PINKDB_MCP_PASSWORD\n\
     \x20                        holds its password (an unauthenticated bridge would\n\
     \x20                        bypass the store's authentication)\n\
     \x20 --enable-ui           serve the read-only warehouse browsing console (wave 78);\n\
     \x20                        needs a build with the `ui` feature; off by default\n\
     \x20 --ui-host HOST        the console's bind address (default 127.0.0.1; a\n\
     \x20                        non-loopback address is refused: the console has no\n\
     \x20                        TLS of its own)\n\
     \x20 --ui-port PORT        the console's TCP port (default 8080; 0 = ephemeral)\n\
     \x20 --ui-auth basic       require HTTP Basic authentication against the store's\n\
     \x20                        own users (the engine verifies the password, and the\n\
     \x20                        console's statements run under that user's grants)\n\
     \x20 --ui-token TOKEN      require this bearer token on every API request\n\
     \x20                        (constant-time comparison; independent of the catalog)\n\
     \x20 --ui-workers N        the console's runtime threads (default 4)\n\
     \x20 --ui-max-connections N  the console's concurrent connections (default 64)\n\
     \x20 --ui-max-page-size N  the console's row-preview page-size bound (default 500)\n\
     \n\
     \x20 --health-check [HOST:PORT]  one-shot liveness probe: connect, send Ping, and\n\
     \x20                        exit 0 (healthy) or 1 (unhealthy). Without an address it\n\
     \x20                        reads PINKDB_HEALTHCHECK_ADDR, then PINKDB_HOST/\n\
     \x20                        PINKDB_PORT, then 127.0.0.1:5433;\n\
     \x20                        PINKDB_HEALTHCHECK_TIMEOUT_MS bounds it (default 2000)\n\
     \n\
     At-rest encryption is not wired into the server: PINKDB_KEY is ignored and\n\
     --file must be a plaintext database (an encrypted one is refused at startup).\n\
     \n\
     Security: authentication is opt-in per store — while the store has no\n\
     users every connection is trusted and can run any SQL; once a user\n\
     exists, every statement request requires Login first. TLS only encrypts\n\
     the transport (it identifies the server, not clients). Keep the bind\n\
     address on localhost or a trusted network until the store has users.\n\
     \n\
     The warehouse console (--enable-ui) is read-only: it has no DML and no\n\
     DDL endpoint. It binds 127.0.0.1 by default and refuses any other\n\
     address (it has no TLS of its own). A store with users is never served\n\
     unauthenticated: start it with --ui-auth basic or --ui-token. Serving a\n\
     store without users prints a loud warning."
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        host: DEFAULT_HOST.to_string(),
        port: DEFAULT_PORT,
        file: None,
        catalog: None,
        data: None,
        max_connections: DEFAULT_MAX_CONNECTIONS,
        idle_timeout_secs: DEFAULT_IDLE_TIMEOUT.as_secs(),
        statement_timeout_ms: DEFAULT_STATEMENT_TIMEOUT_MS,
        max_result_rows: DEFAULT_MAX_RESULT_ROWS,
        max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        max_concurrent_logins: DEFAULT_MAX_CONCURRENT_LOGINS,
        transport: Transport::Sync,
        allow_load_extension: false,
        extension_root: None,
        extension_pins: Vec::new(),
        mcp_extension: None,
        mcp_port: DEFAULT_MCP_PORT,
        mcp_user: None,
        allow_copy_to: false,
        copy_root: None,
        tls_cert: None,
        tls_key: None,
        #[cfg(feature = "ui")]
        enable_ui: false,
        #[cfg(feature = "ui")]
        ui_host: DEFAULT_UI_HOST.to_string(),
        #[cfg(feature = "ui")]
        ui_port: DEFAULT_UI_PORT,
        #[cfg(feature = "ui")]
        ui_auth_basic: false,
        #[cfg(feature = "ui")]
        ui_token: None,
        #[cfg(feature = "ui")]
        ui_workers: DEFAULT_UI_WORKERS,
        #[cfg(feature = "ui")]
        ui_max_connections: DEFAULT_UI_MAX_CONNECTIONS,
        #[cfg(feature = "ui")]
        ui_max_page_size: DEFAULT_UI_MAX_PAGE_SIZE,
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
            "--catalog" => args.catalog = Some(value("--catalog")?),
            "--data" => args.data = Some(value("--data")?),
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
            "--extension-pin" => args.extension_pins.push(value("--extension-pin")?),
            "--allow-copy-to" if inline.is_none() => args.allow_copy_to = true,
            "--copy-root" => args.copy_root = Some(value("--copy-root")?),
            "--mcp-extension" => args.mcp_extension = Some(value("--mcp-extension")?),
            "--mcp-port" => {
                let raw = value("--mcp-port")?;
                args.mcp_port = raw
                    .parse()
                    .map_err(|_| format!("invalid --mcp-port value {raw:?}"))?;
            }
            "--mcp-user" => args.mcp_user = Some(value("--mcp-user")?),
            "--tls-cert" => args.tls_cert = Some(value("--tls-cert")?),
            "--tls-key" => args.tls_key = Some(value("--tls-key")?),
            "--enable-ui" => {
                #[cfg(feature = "ui")]
                {
                    args.enable_ui = true;
                }
                #[cfg(not(feature = "ui"))]
                {
                    return Err(ui_feature_missing());
                }
            }
            "--ui-host" => {
                let raw = value("--ui-host")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_host = raw;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-port" => {
                let raw = value("--ui-port")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_port = raw
                        .parse()
                        .map_err(|_| format!("invalid --ui-port value {raw:?}"))?;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-auth" => {
                let raw = value("--ui-auth")?;
                #[cfg(feature = "ui")]
                {
                    if raw != "basic" {
                        return Err(format!(
                            "invalid --ui-auth value {raw:?}: the only mode is `basic` \
                             (HTTP Basic against the store's users)"
                        ));
                    }
                    args.ui_auth_basic = true;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-token" => {
                let raw = value("--ui-token")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_token = Some(raw);
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-workers" => {
                let raw = value("--ui-workers")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_workers = raw
                        .parse()
                        .ok()
                        .filter(|n: &usize| *n > 0)
                        .ok_or_else(|| format!("invalid --ui-workers value {raw:?}"))?;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-max-connections" => {
                let raw = value("--ui-max-connections")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_max_connections = raw
                        .parse()
                        .ok()
                        .filter(|n: &usize| *n > 0)
                        .ok_or_else(|| format!("invalid --ui-max-connections value {raw:?}"))?;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-max-page-size" => {
                let raw = value("--ui-max-page-size")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_max_page_size = raw
                        .parse()
                        .ok()
                        .filter(|n: &usize| *n > 0)
                        .ok_or_else(|| format!("invalid --ui-max-page-size value {raw:?}"))?;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
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
    // The console's two credential modes are mutually exclusive: the
    // operator asks for one, not two.
    #[cfg(feature = "ui")]
    if args.ui_auth_basic && args.ui_token.is_some() {
        return Err(
            "--ui-auth basic and --ui-token are mutually exclusive: pick one credential for \
             the console"
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
    // M-3: opting in without a root would leave the destination policy at
    // "root required" (every COPY refused). Refuse the unusable startup
    // instead of silently accepting a flag that can never take effect.
    if args.allow_copy_to && args.copy_root.is_none() {
        return Err(
            "--allow-copy-to requires --copy-root PATH (COPY TO is confined to a directory; \
             there is no unrestricted server mode)"
                .to_string(),
        );
    }
    // Wave 96b: `--catalog` and `--file` are two durable stores, and `--data`
    // is the catalog mode's data location. Refuse the ambiguous combinations
    // before anything opens.
    if args.catalog.is_some() && args.file.is_some() {
        return Err(
            "--catalog and --file are mutually exclusive: a --catalog store keeps \
                    its metadata in the lake catalog, a --file store in the file"
                .to_string(),
        );
    }
    if args.catalog.is_some() && args.data.is_none() {
        return Err(
            "--catalog requires --data DIR|s3://BUCKET/PREFIX (the lake's data \
                    location)"
                .to_string(),
        );
    }
    if args.catalog.is_none() && args.data.is_some() {
        return Err(
            "--data needs --catalog CATALOG (it names a --catalog store's data \
                    location)"
                .to_string(),
        );
    }
    Ok(args)
}

/// The message for a build without the console.
#[cfg(not(feature = "ui"))]
fn ui_feature_missing() -> String {
    "--enable-ui and the --ui-* flags need a build with the `ui` feature (this build has \
     no warehouse console); rebuild with `cargo build -p pinkdb-server --features ui`."
        .to_string()
}

/// The health-check target resolution shared by the binary and its unit
/// tests: an explicit flag value, then the container-oriented environment,
/// then the server's default address.
fn health_check_addr(host: &str, port: &str) -> String {
    let host = match host {
        "" | "0.0.0.0" => "127.0.0.1",
        "::" | "[::]" => "::1",
        other => other,
    };
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// `Some(None)` for `--health-check` (use the environment), `Some(Some(addr))`
/// for `--health-check HOST:PORT` or `--health-check=HOST:PORT`, and `None`
/// when the mode was not requested. `--help` wins over the probe so the
/// operator always gets the usage text.
fn parse_health_check_arg(args: &[String]) -> Option<Option<String>> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        return None;
    }
    for (index, arg) in args.iter().enumerate() {
        if arg == "--health-check" {
            let addr = args
                .get(index + 1)
                .filter(|value| !value.starts_with('-'))
                .cloned();
            return Some(addr);
        }
        if let Some(addr) = arg.strip_prefix("--health-check=") {
            let addr = (!addr.is_empty()).then(|| addr.to_string());
            return Some(addr);
        }
    }
    None
}

fn health_check_target(explicit: Option<&str>) -> String {
    if let Some(addr) = explicit {
        return addr.to_string();
    }
    if let Ok(addr) = std::env::var("PINKDB_HEALTHCHECK_ADDR") {
        let addr = addr.trim();
        if !addr.is_empty() {
            return addr.to_string();
        }
    }
    let host = std::env::var("PINKDB_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let port = std::env::var("PINKDB_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
    health_check_addr(&host, &port)
}

/// Connect, speak the protocol `Ping`, and map the outcome to the process
/// exit code. The probe is deliberately tiny: it is the image's `HEALTHCHECK`
/// and must work without a shell or any extra binary.
fn run_health_check(target: &str) -> ExitCode {
    let timeout_ms = std::env::var("PINKDB_HEALTHCHECK_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_HEALTH_CHECK_TIMEOUT_MS);
    let timeout = Duration::from_millis(timeout_ms);
    // A TLS server refuses the plaintext protocol, and a localhost probe
    // cannot generally verify a container's (often self-signed) certificate,
    // so with TLS configured the probe checks that the port accepts TCP
    // connections instead of speaking `Ping`.
    if std::env::var("PINKDB_TLS_CERT").is_ok_and(|cert| !cert.trim().is_empty()) {
        return run_tcp_health_check(target, timeout);
    }
    match Client::connect_timeout(target, timeout).and_then(|mut client| client.ping()) {
        Ok(()) => {
            println!("pinkdb-server: healthy ({target})");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("pinkdb-server: unhealthy ({target}): {e}");
            ExitCode::FAILURE
        }
    }
}

/// The TLS-server variant of the probe: the port accepts a TCP connection
/// within `timeout`.
fn run_tcp_health_check(target: &str, timeout: Duration) -> ExitCode {
    use std::net::{TcpStream, ToSocketAddrs};
    let addrs = match target.to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(e) => {
            eprintln!("pinkdb-server: unhealthy ({target}): {e}");
            return ExitCode::FAILURE;
        }
    };
    for addr in addrs {
        if TcpStream::connect_timeout(&addr, timeout).is_ok() {
            println!("pinkdb-server: healthy ({target}, TLS server: TCP accept)");
            return ExitCode::SUCCESS;
        }
    }
    eprintln!("pinkdb-server: unhealthy ({target}): no TCP connection (TLS server)");
    ExitCode::FAILURE
}

/// `--mcp-extension PATH`: hand the MCP bridge this server's store over a
/// loopback HTTP listener.
///
/// The bridge is loaded into the shared store like any other extension and
/// served on one connection: every statement it runs goes through the
/// connection's login, so the store's roles and grants apply exactly as they
/// do to a protocol client. With users defined the bridge needs `--mcp-user`
/// and `PINKDB_MCP_PASSWORD` — an unauthenticated MCP session on a store with
/// users would be a privilege bypass. The policy is read-only; the bridge's
/// `allow-writes` is a CLI flag, not a server one.
fn start_mcp(db: Database, path: &str, port: u16, user: Option<&str>) -> Result<(), String> {
    db.load_extension(path)
        .map_err(|e| format!("cannot load the MCP bridge {path:?}: {e}"))?;
    let ext = db
        .dynamic_extension("mcp")
        .map_err(|e| format!("the MCP bridge did not register: {e}"))?;
    let mut conn = db.connect();
    if db.has_users() {
        let user = user.ok_or_else(|| {
            "--mcp-extension on a store with users needs --mcp-user NAME and \
             PINKDB_MCP_PASSWORD"
                .to_string()
        })?;
        let password = std::env::var("PINKDB_MCP_PASSWORD").map_err(|_| {
            "PINKDB_MCP_PASSWORD must hold the --mcp-user password on a store with users"
                .to_string()
        })?;
        conn.login(user, &password)
            .map_err(|e| format!("MCP login as {user:?} failed: {e}"))?;
        conn.require_login(true);
    }
    let transport = format!("http:127.0.0.1:{port}");
    eprintln!(
        "pinkdb mcp: bridging this store ({}, read-only)",
        match user {
            Some(user) if db.has_users() => format!("as {user}"),
            _ => "trusted".to_string(),
        }
    );
    // Read-only, and no `allow_files`: a `Connection` has no store handle, so
    // the policy cannot inspect the plan. The login's RBAC gate is what
    // restricts file scans here — `rbac` M-4 makes them administrator-only.
    let policy = pinkdb::serve::ServePolicy::default();
    let mut host = pinkdb::serve::SessionHost::new(&mut conn, &policy);
    pinkdb::serve::serve_with(&ext, &transport, "", &mut host)
        .map_err(|e| format!("the MCP bridge stopped: {e}"))
}

fn main() -> ExitCode {
    // The probe is a mode of its own: it must run before any database or
    // listener setup (a health check inside the server container is a second
    // process and must not open the store).
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if let Some(explicit) = parse_health_check_arg(&argv) {
        let target = health_check_target(explicit.as_deref());
        return run_health_check(&target);
    }

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

    // Content pins: installed before the first store opens, because a WAL
    // replay can load a library through the same loader. A bad pin stops the
    // server; the operator asked for a policy and should not get a silent
    // half of it.
    if !args.extension_pins.is_empty()
        && let Err(error) = pinkdb_ext_loader::set_pins(&args.extension_pins)
    {
        eprintln!("Error: {error}");
        return ExitCode::from(2);
    }

    // COPY TO destination root (M-3): validated before anything binds.
    let copy_root: Option<std::path::PathBuf> = match &args.copy_root {
        Some(root) => {
            let root = std::path::PathBuf::from(root);
            if !root.is_dir() {
                eprintln!("Error: --copy-root {:?} is not a directory", root.display());
                return ExitCode::from(2);
            }
            Some(root)
        }
        None => None,
    };

    // A durable open replays the WAL, which may write to a checkpointed
    // vector graph; the owning package must be registered before that.
    #[cfg(feature = "vss")]
    if let Err(e) = pinkdb_vss::ensure_registered() {
        eprintln!("warning: the vss extension could not be registered: {e}");
    }

    // Kept alive for the whole process: dropping it stops the lease heartbeat
    // and releases the catalog lease.
    #[cfg(feature = "catalog")]
    let mut catalog_store: Option<CatalogStore> = None;
    let db = match (&args.file, &args.catalog) {
        (Some(path), _) => {
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
        #[cfg(not(feature = "catalog"))]
        (None, Some(_)) => {
            eprintln!(
                "Error: --catalog needs a pinkdb-server built with the `catalog` feature \
                 (cargo build -p pinkdb-server --features catalog, plus catalog-sqlite, \
                 catalog-postgres or lake-s3 for those backends)"
            );
            return ExitCode::FAILURE;
        }
        #[cfg(feature = "catalog")]
        (None, Some(catalog)) => {
            let data = args.data.as_deref().unwrap_or_default();
            let owner = format!("{}:{} pid {}", args.host, args.port, std::process::id());
            match CatalogStore::open(catalog, data, &owner) {
                Ok(store) => {
                    let db = store.database();
                    db.set_extension_root(extension_root.clone());
                    catalog_store = Some(store);
                    db
                }
                Err(e) => {
                    eprintln!("Error: cannot open the lake catalog {catalog:?}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        (None, None) => {
            let db = Database::new();
            db.set_extension_root(extension_root.clone());
            db
        }
    };

    // Built-in linked extensions, published before any store exists and
    // loaded into the store this server serves. A durable store whose log
    // already recorded the load keeps it (that is what the replay guard is
    // for), so the load is skipped rather than reported as a duplicate.
    #[cfg(feature = "vss")]
    {
        if !db.is_extension_loaded(pinkdb_vss::EXTENSION_NAME)
            && let Err(e) = db.load_extension(pinkdb_vss::EXTENSION_NAME)
        {
            eprintln!("warning: the vss extension could not be loaded: {e}");
        }
    }
    #[cfg(feature = "fts")]
    {
        if let Err(e) = pinkdb_fts::ensure_registered() {
            eprintln!("warning: the fts extension could not be registered: {e}");
        }
        if !db.is_extension_loaded(pinkdb_fts::EXTENSION_NAME)
            && let Err(e) = db.load_extension(pinkdb_fts::EXTENSION_NAME)
        {
            eprintln!("warning: the fts extension could not be loaded: {e}");
        }
    }
    #[cfg(feature = "embeddings")]
    {
        if let Err(e) = pinkdb_embeddings::ensure_registered() {
            eprintln!("warning: the embeddings extension could not be registered: {e}");
        }
        if !db.is_extension_loaded(pinkdb_embeddings::EXTENSION_NAME)
            && let Err(e) = db.load_extension(pinkdb_embeddings::EXTENSION_NAME)
        {
            eprintln!("warning: the embeddings extension could not be loaded: {e}");
        }
    }

    let mode = match (&args.file, &args.catalog) {
        (Some(path), _) => format!("durable: {path}"),
        #[cfg(feature = "catalog")]
        (None, Some(catalog)) => format!(
            "catalog: {catalog} (data {}, lake version {})",
            args.data.as_deref().unwrap_or_default(),
            catalog_store.as_ref().map_or(0, CatalogStore::version)
        ),
        #[cfg(not(feature = "catalog"))]
        (None, Some(_)) => unreachable!("--catalog is refused above without the feature"),
        (None, None) => "in-memory".to_string(),
    };

    // The warehouse browsing console (wave 78): opt-in, read-only, loopback
    // by default. A configuration the exposure policy refuses stops the
    // server: the operator asked for the console, so running without it
    // would be a silent downgrade.
    #[cfg(feature = "ui")]
    let _ui_handle = if args.enable_ui {
        match start_ui(&args, &db) {
            Ok(handle) => Some(handle),
            Err(msg) => {
                eprintln!("Error: {msg}");
                return ExitCode::from(2);
            }
        }
    } else {
        None
    };

    // The bridge (wave 105) starts before the server binds: it shares the
    // same store and its own loopback listener, so a failure to load or log
    // in stops the process instead of leaving a half-configured server.
    if let Some(path) = args.mcp_extension.clone() {
        let bridge_db = db.clone();
        let user = args.mcp_user.clone();
        let port = args.mcp_port;
        let spawned = std::thread::Builder::new()
            .name("pinkdb-mcp".to_string())
            .spawn(move || {
                if let Err(message) = start_mcp(bridge_db, &path, port, user.as_deref()) {
                    eprintln!("Error: {message}");
                    std::process::exit(1);
                }
            });
        if let Err(e) = spawned {
            eprintln!("Error: cannot start the MCP bridge thread: {e}");
            return ExitCode::FAILURE;
        }
    }

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
                    .extension_root(extension_root.clone())
                    .allow_copy_to(args.allow_copy_to)
                    .copy_root(copy_root.clone()),
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
                    .extension_root(extension_root.clone())
                    .allow_copy_to(args.allow_copy_to)
                    .copy_root(copy_root.clone()),
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

/// Start the read-only warehouse browsing console (wave 78).
#[cfg(feature = "ui")]
fn start_ui(args: &Args, db: &Database) -> Result<pinkdb_server::ui::UiHandle, String> {
    use pinkdb_server::ui::{UiAuth, UiOptions, UiServer};
    let auth = match (&args.ui_token, args.ui_auth_basic) {
        (Some(token), _) => UiAuth::Token(token.clone()),
        (None, true) => UiAuth::Basic,
        (None, false) => UiAuth::None,
    };
    let opts = UiOptions {
        workers: args.ui_workers,
        max_connections: args.ui_max_connections,
        max_page_size: args.ui_max_page_size,
        auth,
        ..UiOptions::default()
    };
    let server = UiServer::bind((args.ui_host.as_str(), args.ui_port), db.clone(), opts)
        .map_err(|e| format!("cannot start the warehouse console: {e}"))?;
    let addr = server.local_addr();
    let handle = server
        .spawn()
        .map_err(|e| format!("cannot start the warehouse console: {e}"))?;
    println!("pinkdb-server: warehouse console on http://{addr} (read-only)");
    Ok(handle)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn health_check_flag_accepts_an_optional_address() {
        assert_eq!(
            parse_health_check_arg(&args(&["--health-check"])),
            Some(None)
        );
        assert_eq!(
            parse_health_check_arg(&args(&["--health-check", "127.0.0.1:6000"])),
            Some(Some("127.0.0.1:6000".to_string()))
        );
        assert_eq!(
            parse_health_check_arg(&args(&["--health-check=db:5433"])),
            Some(Some("db:5433".to_string()))
        );
        // A following flag is not an address.
        assert_eq!(
            parse_health_check_arg(&args(&["--health-check", "--port", "6000"])),
            Some(None)
        );
        assert_eq!(parse_health_check_arg(&args(&["--port", "6000"])), None);
    }

    #[test]
    fn help_wins_over_the_health_check_mode() {
        assert_eq!(
            parse_health_check_arg(&args(&["--health-check", "--help"])),
            None
        );
    }

    #[test]
    fn health_check_addr_maps_wildcard_binds_to_loopback() {
        assert_eq!(health_check_addr("0.0.0.0", "5433"), "127.0.0.1:5433");
        assert_eq!(health_check_addr("", "6000"), "127.0.0.1:6000");
        assert_eq!(health_check_addr("::", "5433"), "[::1]:5433");
        assert_eq!(health_check_addr("[::1]", "5433"), "[::1]:5433");
        assert_eq!(health_check_addr("::1", "5433"), "[::1]:5433");
        assert_eq!(health_check_addr("db.internal", "5433"), "db.internal:5433");
    }
}
