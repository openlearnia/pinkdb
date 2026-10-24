//! The transport the host asked for: stdio or localhost HTTP.
//!
//! The descriptor comes from `pinkdb_ext_serve` — `stdio`, or
//! `http:HOST:PORT`. Only loopback HTTP is accepted; refusing here, before
//! the socket exists, keeps a misconfigured host from exposing a store to the
//! network even for a moment.

use std::io::{BufRead, Write};

use crate::http;
use crate::protocol::{Host, Server};

/// Serve `server` over `transport` until the client disconnects.
pub fn serve<H: Host>(server: &mut Server<H>, transport: &str) -> Result<(), String> {
    if transport == "stdio" {
        serve_stdio(server)
    } else {
        http::serve(server, transport)
    }
}

/// One JSON-RPC message per line on stdin, one response per line on stdout.
/// Diagnostics go to stderr only: stdout carries the protocol and nothing
/// else.
fn serve_stdio<H: Host>(server: &mut Server<H>) -> Result<(), String> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line.map_err(|error| format!("cannot read stdin: {error}"))?;
        let Some(response) = server.handle(&line) else {
            continue;
        };
        writeln!(stdout, "{response}").map_err(|error| format!("cannot write stdout: {error}"))?;
        stdout
            .flush()
            .map_err(|error| format!("cannot write stdout: {error}"))?;
    }
    Ok(())
}
