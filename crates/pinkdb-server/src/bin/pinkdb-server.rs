//! `pinkdb-server` — run a pinkdb database over TCP.
//!
//! ```text
//! pinkdb-server [--host HOST] [--port PORT] [--data-dir DIR | --in-memory]
//!               [--lake-root DIR]...
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
//! The server owns a data directory (wave 124; `--data-dir`, default
//! `./pinkdb-data`, created on first start): `pinkdb.pink` holds the local
//! databases, the users and grants and the list of lake databases (durable
//! checkpoint + WAL: rows acked to a client survive a crash and replay on the
//! next start), `state.pink` the console's and the agent's own state,
//! `lakes/` the lakes `CREATE DATABASE … (TYPE LAKE)` creates, and `tmp/`
//! upload staging. `--in-memory` keeps everything in memory instead (nothing
//! is saved). SIGINT/SIGTERM stop accepting, checkpoint both stores and
//! release the lakes' leases. Authentication is opt-in
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pinkdb_server::{
    Client, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONCURRENT_LOGINS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS, DEFAULT_STATEMENT_TIMEOUT_MS, Server,
};
use pinkdb_server::{DEFAULT_DATA_DIR, DataDir};

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
    /// `--data-dir DIR` (wave 124): the server's data directory; default
    /// `./pinkdb-data`.
    data_dir: Option<String>,
    /// `--in-memory`: keep everything in memory (nothing is saved).
    in_memory: bool,
    /// `--lake-root DIR` (repeatable): another directory local lakes may
    /// live in, besides `<data-dir>/lakes`.
    lake_roots: Vec<String>,
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
    /// `--ui-auth none`: serve a store without users with no sign-in (the
    /// default is sign-in with the store's users, wave 121d).
    #[cfg(feature = "ui")]
    ui_auth_none: bool,
    /// `--ui-tls-cert PATH` / `--ui-tls-key PATH`: the console's own HTTPS.
    #[cfg(feature = "ui")]
    ui_tls_cert: Option<String>,
    #[cfg(feature = "ui")]
    ui_tls_key: Option<String>,
    /// `--ui-workers N`: the console's runtime threads.
    #[cfg(feature = "ui")]
    ui_workers: usize,
    /// `--ui-max-connections N`: the console's concurrent connections.
    #[cfg(feature = "ui")]
    ui_max_connections: usize,
    /// `--ui-max-page-size N`: the console's page-size bound.
    #[cfg(feature = "ui")]
    ui_max_page_size: usize,
    /// `--ui-read-only`: worksheets run only queries, whatever the grants.
    #[cfg(feature = "ui")]
    ui_read_only: bool,
    /// `--ui-allowed-host NAME` (repeatable): extra `Host` names.
    #[cfg(feature = "ui")]
    ui_allowed_hosts: Vec<String>,
    /// `--agent-allow-writes`: the console's agent may write (under the
    /// user's grants).
    #[cfg(feature = "ui")]
    agent_allow_writes: bool,
    /// `--ui-max-upload-mb N`: the largest file an upload may send.
    #[cfg(feature = "ui")]
    ui_max_upload_mb: usize,
    /// `--agent-extension PATH`: load the data agent for the console's ask box.
    agent_extension: Option<String>,
    /// `--memory-extension PATH`: load the agent memory.
    memory_extension: Option<String>,
    /// `--memory-distill-every SECS`: drain the memory queue on an interval.
    memory_distill_every: Option<u64>,
}

fn usage() -> &'static str {
    "Usage: pinkdb-server [--host HOST] [--port PORT] [--data-dir DIR | --in-memory]\n\
     \x20                    [--lake-root DIR]...\n\
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
     \x20 --data-dir DIR         the server's data directory (default ./pinkdb-data,\n\
     \x20                        created on first start): pinkdb.pink (databases, users,\n\
     \x20                        grants, the lake databases' locations; checkpoint +\n\
     \x20                        WAL), state.pink (console/agent state), lakes/ (lakes\n\
     \x20                        CREATE DATABASE … (TYPE LAKE) makes) and tmp/\n\
     \x20 --in-memory            keep everything in memory: nothing is saved\n\
     \x20 --lake-root DIR        another directory local lakes may live in (repeatable);\n\
     \x20                        lakes are databases: CREATE DATABASE n (TYPE LAKE\n\
     \x20                        [, CATALOG '…', DATA '…']), ATTACH 'lake:…' AS n\n\
     \x20                        [(READ_ONLY)], DETACH n (ADMIN only); a local lake\n\
     \x20                        path must be inside <data-dir>/lakes or a --lake-root,\n\
     \x20                        remote catalogs are postgres:// (credentials from\n\
     \x20                        PGPASSWORD, never in the URL) and data s3:// (AWS_*)\n\
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
     \x20 --enable-ui           serve the warehouse console: catalog browser, SQL\n\
     \x20                        worksheets and the ask box (waves 78, 121);\n\
     \x20                        needs a build with the `ui` feature; off by default\n\
     \x20 --ui-host HOST        the console's bind address (default 127.0.0.1; a\n\
     \x20                        non-loopback address needs --ui-tls-cert/--ui-tls-key)\n\
     \x20 --ui-port PORT        the console's TCP port (default 8080; 0 = ephemeral)\n\
     \x20 --ui-auth MODE        `password` (the default): sign in with the store's users\n\
     \x20                        (their grants apply), with an optional TOTP second\n\
     \x20                        factor and personal access tokens for scripts; a store\n\
     \x20                        without users starts in first-run setup (the setup code\n\
     \x20                        is printed at start). `none`: no sign-in, for a store\n\
     \x20                        without users on loopback only\n\
     \x20 --ui-tls-cert PATH    serve the console over HTTPS with this PEM certificate\n\
     \x20                        chain (with --ui-tls-key); allows a non-loopback\n\
     \x20                        --ui-host\n\
     \x20 --ui-tls-key PATH     the certificate's PEM private key\n\
     \x20 --ui-workers N        the console's runtime threads (default 4)\n\
     \x20 --ui-max-connections N  the console's concurrent connections (default 64)\n\
     \x20 --ui-max-page-size N  the console's row-preview page-size bound (default 500)\n\
     \x20 --ui-read-only        worksheets run only queries (SELECT, EXPLAIN, DESCRIBE,\n\
     \x20                        SHOW, USE) for every user, whatever their grants\n\
     \x20 --ui-allowed-host NAME  another Host name the console answers to (a reverse\n\
     \x20                        proxy's); loopback names and the bind address always are\n\
     \x20 --ui-max-upload-mb N  the largest file the console's Import may upload (default 64)\n\
     \x20 --agent-extension PATH  load the data agent (libpinkdb_agent) for the console's\n\
     \x20                        ask box; the provider and its key come from this\n\
     \x20                        process's environment and never reach the browser\n\
     \x20 --agent-allow-writes  let the console's agent write (under the user's grants;\n\
     \x20                        never with --ui-read-only)\n\
     \x20 --memory-extension PATH  load the agent memory (libpinkdb_memory)\n\
     \x20 --memory-distill-every SECS  drain the memory distillation queue on this\n\
     \x20                        interval, as each owner (off by default)\n\
     \n\
     \x20 --health-check [HOST:PORT]  one-shot liveness probe: connect, send Ping, and\n\
     \x20                        exit 0 (healthy) or 1 (unhealthy). Without an address it\n\
     \x20                        reads PINKDB_HEALTHCHECK_ADDR, then PINKDB_HOST/\n\
     \x20                        PINKDB_PORT, then 127.0.0.1:5433;\n\
     \x20                        PINKDB_HEALTHCHECK_TIMEOUT_MS bounds it (default 2000)\n\
     \n\
     At-rest encryption is not wired into the server: PINKDB_KEY is ignored and\n\
     the data directory's stores are plaintext (an encrypted one is refused).\n\
     \n\
     Security: authentication is opt-in per store — while the store has no\n\
     users every connection is trusted and can run any SQL; once a user\n\
     exists, every statement request requires Login first. TLS only encrypts\n\
     the transport (it identifies the server, not clients). Keep the bind\n\
     address on localhost or a trusted network until the store has users.\n\
     \n\
     The warehouse console (--enable-ui) runs SQL worksheets as the signed-in\n\
     user (their grants apply; --ui-read-only limits everyone to queries). It\n\
     asks for sign-in by default; a store without users starts in first-run\n\
     setup, guarded by a code printed at start. It binds 127.0.0.1 unless it\n\
     has a certificate (--ui-tls-cert/--ui-tls-key). --ui-auth none serves a\n\
     store without users with no sign-in, on loopback only, with a loud\n\
     warning."
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        host: DEFAULT_HOST.to_string(),
        port: DEFAULT_PORT,
        data_dir: None,
        in_memory: false,
        lake_roots: Vec::new(),
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
        ui_auth_none: false,
        #[cfg(feature = "ui")]
        ui_tls_cert: None,
        #[cfg(feature = "ui")]
        ui_tls_key: None,
        #[cfg(feature = "ui")]
        ui_workers: DEFAULT_UI_WORKERS,
        #[cfg(feature = "ui")]
        ui_max_connections: DEFAULT_UI_MAX_CONNECTIONS,
        #[cfg(feature = "ui")]
        ui_max_page_size: DEFAULT_UI_MAX_PAGE_SIZE,
        #[cfg(feature = "ui")]
        ui_read_only: false,
        #[cfg(feature = "ui")]
        ui_allowed_hosts: Vec::new(),
        #[cfg(feature = "ui")]
        agent_allow_writes: false,
        #[cfg(feature = "ui")]
        ui_max_upload_mb: 64,
        agent_extension: None,
        memory_extension: None,
        memory_distill_every: None,
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
            "--data-dir" => args.data_dir = Some(value("--data-dir")?),
            "--in-memory" => args.in_memory = true,
            "--lake-root" => args.lake_roots.push(value("--lake-root")?),
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
                    args.ui_auth_none = match raw.as_str() {
                        "password" => false,
                        "none" => true,
                        _ => {
                            return Err(format!(
                                "invalid --ui-auth value {raw:?}: `password` (the default: \
                                 sign in with the store's users) or `none` (a store without \
                                 users, on loopback)"
                            ));
                        }
                    };
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--ui-token" => {
                return Err(
                    "--ui-token is gone (wave 121d): the console asks for sign-in by default, \
                     and scripts use personal access tokens (created on the console's Account \
                     page, sent as `Authorization: Bearer pdbt_…`)"
                        .to_string(),
                );
            }
            "--ui-tls-cert" | "--ui-tls-key" => {
                let which = flag.clone();
                let raw = value(&which)?;
                #[cfg(feature = "ui")]
                {
                    if which == "--ui-tls-cert" {
                        args.ui_tls_cert = Some(raw);
                    } else {
                        args.ui_tls_key = Some(raw);
                    }
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
            "--ui-read-only" => {
                #[cfg(feature = "ui")]
                {
                    args.ui_read_only = true;
                }
                #[cfg(not(feature = "ui"))]
                {
                    return Err(ui_feature_missing());
                }
            }
            "--ui-allowed-host" => {
                let raw = value("--ui-allowed-host")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_allowed_hosts.push(raw);
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--agent-allow-writes" => {
                #[cfg(feature = "ui")]
                {
                    args.agent_allow_writes = true;
                }
                #[cfg(not(feature = "ui"))]
                {
                    return Err(ui_feature_missing());
                }
            }
            "--ui-max-upload-mb" => {
                let raw = value("--ui-max-upload-mb")?;
                #[cfg(feature = "ui")]
                {
                    args.ui_max_upload_mb = raw
                        .parse()
                        .ok()
                        .filter(|n: &usize| *n > 0)
                        .ok_or_else(|| format!("invalid --ui-max-upload-mb value {raw:?}"))?;
                }
                #[cfg(not(feature = "ui"))]
                {
                    let _ = raw;
                    return Err(ui_feature_missing());
                }
            }
            "--agent-extension" => args.agent_extension = Some(value("--agent-extension")?),
            "--memory-extension" => args.memory_extension = Some(value("--memory-extension")?),
            "--memory-distill-every" => {
                let raw = value("--memory-distill-every")?;
                args.memory_distill_every = Some(
                    raw.parse()
                        .ok()
                        .filter(|n: &u64| *n > 0)
                        .ok_or_else(|| format!("invalid --memory-distill-every value {raw:?}"))?,
                );
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
    // The console's certificate and key come together.
    #[cfg(feature = "ui")]
    if args.ui_tls_cert.is_some() != args.ui_tls_key.is_some() {
        return Err(
            "--ui-tls-cert and --ui-tls-key must be given together (both or neither)".to_string(),
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
    // Wave 124: a server keeps its data in one directory, or in memory.
    if args.in_memory && args.data_dir.is_some() {
        return Err(
            "--in-memory and --data-dir are mutually exclusive: an in-memory server saves \
             nothing"
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

    // The import readers (wave 121b) register process-wide before any store
    // opens, as the CLI does: a durable open may replay a statement that read
    // a file.
    #[cfg(feature = "parquet")]
    pinkdb_parquet::ensure_registered();
    #[cfg(feature = "httpfs")]
    pinkdb_httpfs::ensure_registered();

    // A durable open replays the WAL, which may write to a checkpointed
    // vector graph; the owning package must be registered before that.
    #[cfg(feature = "vss")]
    if let Err(e) = pinkdb_vss::ensure_registered() {
        eprintln!("warning: the vss extension could not be registered: {e}");
    }

    // Wave 124: lake databases open through pink-lake. The opener registers
    // before the store opens: the checkpoint names lake databases and the
    // WAL may replay an ATTACH.
    pink_lake::database::register();

    // The data directory: created on first start, unless --in-memory.
    let data_dir = if args.in_memory {
        None
    } else {
        let path = args.data_dir.as_deref().unwrap_or(DEFAULT_DATA_DIR);
        match DataDir::prepare(path) {
            Ok(dir) => Some(dir),
            Err(e) => {
                eprintln!("Error: cannot use the data directory {path:?}: {e}");
                return ExitCode::FAILURE;
            }
        }
    };
    let mut lake_roots: Vec<std::path::PathBuf> = Vec::new();
    for root in &args.lake_roots {
        match std::path::Path::new(root).canonicalize() {
            Ok(root) if root.is_dir() => lake_roots.push(root),
            _ => {
                eprintln!("Error: --lake-root {root:?} is not a directory");
                return ExitCode::from(2);
            }
        }
    }

    let db = match &data_dir {
        Some(dir) => {
            // The durable open carries the extension policy: a WAL that
            // records an extension is only replayed into a load when the
            // operator opted in *and* the library is inside the extension
            // root. Without the opt-in the open fails with an actionable
            // error instead of loading a path the file names.
            let opts = DurableOpenOptions {
                allow_extension_replay: args.allow_load_extension,
                extension_root: extension_root.clone(),
                lake_dir: Some(dir.lakes()),
                ..DurableOpenOptions::default()
            };
            let path = dir.store().to_string_lossy().into_owned();
            match Database::open_with_options(&path, opts) {
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
    db.set_lake_policy(Some(pinkdb_server::lake_policy(
        data_dir.as_ref().map(DataDir::lakes),
        lake_roots.clone(),
    )));
    for info in db.database_infos() {
        if info.status != "ok" {
            eprintln!(
                "warning: lake database {:?} is {} (its queries fail until it is reachable \
                 again; DETACH forgets it)",
                info.name, info.status
            );
        }
    }

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

    let mode = match &data_dir {
        Some(dir) => format!("durable: {}", dir.root().display()),
        None => "in-memory: nothing is saved".to_string(),
    };

    // The agent and its memory (wave 121): loaded into the served store for
    // the console's ask box and the distillation worker. A library that does
    // not load stops the server: the operator asked for it.
    for (flag, path) in [
        ("--agent-extension", &args.agent_extension),
        ("--memory-extension", &args.memory_extension),
    ] {
        if let Some(path) = path
            && let Err(e) = db.load_extension(path)
        {
            eprintln!("Error: {flag} {path:?} could not be loaded: {e}");
            return ExitCode::FAILURE;
        }
    }
    // The state store (wave 121e): the console's, the agent's and memory's
    // own bookkeeping, apart from the served store, where no statement can
    // list, read or drop it. Wave 124: always `<data-dir>/state.pink` (in
    // memory for an in-memory server).
    let state = match &data_dir {
        Some(dir) => {
            let path = dir.state().to_string_lossy().into_owned();
            match Database::open(&path) {
                Ok(state) => state,
                Err(e) => {
                    eprintln!("Error: cannot open the state store {path:?}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        None => Database::new(),
    };
    // Memory's vector search calls `embed()` on its own channel, which runs
    // on the state store.
    #[cfg(feature = "embeddings")]
    if !state.is_extension_loaded(pinkdb_embeddings::EXTENSION_NAME)
        && let Err(e) = state.load_extension(pinkdb_embeddings::EXTENSION_NAME)
    {
        eprintln!(
            "warning: the embeddings extension could not be loaded into the state store: {e}"
        );
    }
    if let Err(e) = db.attach_state_store(state.clone()) {
        eprintln!("Error: cannot attach the state store: {e}");
        return ExitCode::FAILURE;
    }
    let _memory_worker = match args.memory_distill_every {
        Some(secs) => {
            if args.memory_extension.is_none() {
                eprintln!("Error: --memory-distill-every needs --memory-extension");
                return ExitCode::from(2);
            }
            match pinkdb_server::memory_worker::MemoryWorker::spawn(
                db.clone(),
                Duration::from_secs(secs),
            ) {
                Ok(worker) => Some(worker),
                Err(e) => {
                    eprintln!("Error: cannot start the memory worker: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        None => None,
    };

    // The warehouse console (waves 78, 121): opt-in, loopback by default. A configuration the exposure policy refuses stops the
    // server: the operator asked for the console, so running without it
    // would be a silent downgrade.
    #[cfg(feature = "ui")]
    let _ui_handle = if args.enable_ui {
        let storage =
            pinkdb_server::ui::storage_info(data_dir.as_ref(), &lake_roots, import_formats());
        match start_ui(&args, &db, storage, data_dir.as_ref().map(DataDir::tmp)) {
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

    // Graceful shutdown (wave 124): SIGINT/SIGTERM stop the accept loop; the
    // stores are checkpointed and the lakes' leases released below.
    install_signal_handlers();
    let served = db.clone();
    let exit = match args.transport {
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
            stop_on_signal(server.shutdown_flag());
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
            stop_on_signal(server.shutdown_flag());
            match server.run_blocking() {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("Error: async accept loop stopped: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    };
    shut_down(&served, &state, data_dir.is_some());
    exit
}

/// Set by SIGINT/SIGTERM (wave 124).
static SIGNALED: AtomicBool = AtomicBool::new(false);

/// Route SIGINT and SIGTERM to [`SIGNALED`] instead of killing the process.
#[cfg(unix)]
fn install_signal_handlers() {
    extern "C" fn on_signal(_: libc::c_int) {
        SIGNALED.store(true, Ordering::SeqCst);
    }
    let handler = on_signal as extern "C" fn(libc::c_int) as *const () as libc::sighandler_t;
    // SAFETY: the handler only stores to a static atomic, which is
    // async-signal-safe; `signal` installs it for the two signals.
    unsafe {
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }
}

#[cfg(not(unix))]
fn install_signal_handlers() {}

/// Stop the accept loop behind `flag` once a signal arrived.
fn stop_on_signal(flag: Arc<AtomicBool>) {
    let _ = std::thread::Builder::new()
        .name("pinkdb-signals".to_string())
        .spawn(move || {
            while !SIGNALED.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(100));
            }
            flag.store(true, Ordering::SeqCst);
        });
}

/// After the accept loop stopped: checkpoint both stores (each waits for the
/// statements running on it) and release the lake databases' leases.
fn shut_down(db: &Database, state: &Database, durable: bool) {
    eprintln!("pinkdb-server: shutting down");
    if durable {
        for (what, store) in [("store", db), ("state store", state)] {
            if let Err(e) = store.checkpoint() {
                eprintln!("warning: cannot checkpoint the {what}: {e}");
            }
        }
    }
    db.close_lake_databases();
    eprintln!("pinkdb-server: stopped");
}

/// What an import can read in this build (the Storage panel and the Import
/// dialog show it): `upload:` formats the console parses itself, `url:`
/// formats a statement reads from a server path, `https://` or `s3://`.
#[cfg(feature = "ui")]
fn import_formats() -> Vec<String> {
    let mut formats = vec!["upload:csv".to_string()];
    if cfg!(feature = "parquet") {
        formats.push("upload:parquet".to_string());
    }
    if cfg!(feature = "httpfs") {
        formats.push("url:csv".to_string());
    }
    if cfg!(feature = "parquet") {
        formats.push("url:parquet".to_string());
    }
    formats
}

/// Start the warehouse console (waves 78 and 121).
#[cfg(feature = "ui")]
fn start_ui(
    args: &Args,
    db: &Database,
    storage: pinkdb_server::ui::StorageInfo,
    scratch_dir: Option<std::path::PathBuf>,
) -> Result<pinkdb_server::ui::UiHandle, String> {
    use pinkdb_server::ui::{UiAuth, UiOptions, UiServer, UiTls};
    let auth = if args.ui_auth_none {
        UiAuth::None
    } else {
        UiAuth::Password
    };
    let tls = match (&args.ui_tls_cert, &args.ui_tls_key) {
        (Some(cert), Some(key)) => Some(UiTls {
            cert_pem: std::fs::read(cert)
                .map_err(|e| format!("cannot read --ui-tls-cert {cert}: {e}"))?,
            key_pem: std::fs::read(key)
                .map_err(|e| format!("cannot read --ui-tls-key {key}: {e}"))?,
        }),
        _ => None,
    };
    let opts = UiOptions {
        workers: args.ui_workers,
        max_connections: args.ui_max_connections,
        max_page_size: args.ui_max_page_size,
        auth,
        read_only: args.ui_read_only,
        allowed_hosts: args.ui_allowed_hosts.clone(),
        agent_allow_writes: args.agent_allow_writes,
        max_upload_bytes: args.ui_max_upload_mb.saturating_mul(1024 * 1024),
        scratch_dir,
        storage,
        tls,
        ..UiOptions::default()
    };
    let server = UiServer::bind((args.ui_host.as_str(), args.ui_port), db.clone(), opts)
        .map_err(|e| format!("cannot start the warehouse console: {e}"))?;
    let url = server.url();
    let handle = server
        .spawn()
        .map_err(|e| format!("cannot start the warehouse console: {e}"))?;
    println!(
        "pinkdb-server: warehouse console on {url} ({})",
        if args.ui_read_only {
            "read-only worksheets"
        } else {
            "worksheets"
        }
    );
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
