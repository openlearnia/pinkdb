//! `pinkdb-server` — run a pinkdb database over TCP.
//!
//! ```text
//! pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]
//! ```
//!
//! Without `--file` the database is in-memory; with it, the server opens the
//! durable checkpoint + WAL path (wave 22): rows acked to a client survive a
//! server crash and are replayed on the next start. No authentication and no
//! TLS in this wave — bind localhost.

use std::process::ExitCode;

use pinkdb::Database;
use pinkdb_server::Server;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5433;

struct Args {
    host: String,
    port: u16,
    file: Option<String>,
}

fn usage() -> &'static str {
    "Usage: pinkdb-server [--host HOST] [--port PORT] [--file PATH.pink]\n\
     \n\
     \x20 --host HOST   bind address (default 127.0.0.1)\n\
     \x20 --port PORT   TCP port; 0 picks an ephemeral port (default 5433)\n\
     \x20 --file PATH   durable database (checkpoint + WAL); in-memory when omitted\n\
     \n\
     At-rest encryption is not wired into the server: PINKDB_KEY is ignored and\n\
     --file must be a plaintext database (an encrypted one is refused at startup).\n\
     \n\
     Security: no authentication, no TLS. Keep the bind address on localhost\n\
     or a trusted network; anyone who can reach the port has full SQL access."
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args { host: DEFAULT_HOST.to_string(), port: DEFAULT_PORT, file: None };
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
                args.port =
                    raw.parse().map_err(|_| format!("invalid --port value {raw:?}"))?;
            }
            "--file" => args.file = Some(value("--file")?),
            other => return Err(format!("unknown argument {other:?}")),
        }
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

    let server = match Server::bind((args.host.as_str(), args.port), db) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Error: cannot bind {}:{}: {e}", args.host, args.port);
            return ExitCode::FAILURE;
        }
    };

    let mode = match &args.file {
        Some(path) => format!("durable: {path}"),
        None => "in-memory".to_string(),
    };
    // The listening line is the readiness signal (tests wait for it).
    println!(
        "pinkdb-server v{} listening on {} ({mode})",
        env!("CARGO_PKG_VERSION"),
        server.local_addr()
    );
    eprintln!("pinkdb-server: no authentication, no TLS — keep the bind address trusted");

    match server.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: accept loop stopped: {e}");
            ExitCode::FAILURE
        }
    }
}
