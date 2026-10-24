//! The localhost-only HTTP transport.
//!
//! This is MCP's streamable HTTP in its simplest legal shape: one `POST` per
//! JSON-RPC message, answered with `application/json` (the spec allows a JSON
//! body instead of an SSE stream), `202` for a notification, `405` for `GET`
//! (no server-initiated stream). An HTTP client needs no async runtime and no
//! TLS: the listener is bound to a loopback address and a non-loopback bind is
//! refused before the socket exists.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};

use crate::protocol::{Host, Server};

/// A request body larger than this is refused: JSON-RPC messages are small,
/// and the host's statement budget is what bounds the work.
const MAX_BODY: usize = 4 * 1024 * 1024;

/// The address `description` names, refused unless it is loopback.
pub fn bind_address(description: &str) -> Result<String, String> {
    let Some(rest) = description.strip_prefix("http:") else {
        return Err(format!(
            "unsupported transport \"{description}\": expected \"stdio\" or \"http:HOST:PORT\""
        ));
    };
    let Some((host, port)) = rest.rsplit_once(':') else {
        return Err(format!(
            "invalid http transport \"{description}\": expected http:HOST:PORT"
        ));
    };
    let port: u16 = port
        .parse()
        .map_err(|_| format!("invalid port in \"{description}\""))?;
    if !is_loopback(host) {
        return Err(format!(
            "refusing to bind \"{host}\": the MCP HTTP transport is localhost only (use \
             127.0.0.1 or ::1)"
        ));
    }
    Ok(if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

/// Whether `host` is this machine: `localhost`, `127.0.0.0/8` or `::1`. A
/// hostname other than `localhost` is refused rather than resolved — a name
/// could point anywhere.
pub fn is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>()
        .map(|address| address.is_loopback())
        .unwrap_or(false)
}

/// Serve every incoming connection until the listener fails.
pub fn serve<H: Host>(server: &mut Server<H>, description: &str) -> Result<(), String> {
    let address = bind_address(description)?;
    let listener = TcpListener::bind(&address)
        .map_err(|error| format!("cannot listen on {address}: {error}"))?;
    // Stderr only: stdout is the protocol on the stdio transport and this
    // line tells a test (or an operator) which port `:0` picked.
    eprintln!("pinkdb mcp: listening on http://{address}");
    for connection in listener.incoming() {
        let stream = match connection {
            Ok(stream) => stream,
            Err(error) => return Err(format!("cannot accept a connection: {error}")),
        };
        if let Err(error) = handle(server, stream) {
            eprintln!("pinkdb mcp: request failed: {error}");
        }
    }
    Ok(())
}

/// One request per connection (every response says `Connection: close`).
fn handle<H: Host>(server: &mut Server<H>, mut stream: TcpStream) -> Result<(), String> {
    let mut reader = BufReader::new(stream.try_clone().map_err(|e| e.to_string())?);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .map_err(|e| format!("cannot read the request line: {e}"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();

    let mut headers: HashMap<String, String> = HashMap::new();
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("cannot read a header: {e}"))?;
        if read == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    if method != "POST" {
        return respond(
            &mut stream,
            405,
            "Method Not Allowed",
            "only POST carries a JSON-RPC message",
            "Allow: POST\r\n",
        );
    }
    if target != "/" && target != "/mcp" {
        return respond(
            &mut stream,
            404,
            "Not Found",
            "the MCP endpoint is /mcp",
            "",
        );
    }
    let length: usize = match headers.get("content-length") {
        Some(value) => value
            .parse()
            .map_err(|_| "invalid Content-Length".to_string())?,
        None => {
            return respond(
                &mut stream,
                411,
                "Length Required",
                "a Content-Length is required",
                "",
            );
        }
    };
    if length > MAX_BODY {
        return respond(
            &mut stream,
            413,
            "Payload Too Large",
            "request body too large",
            "",
        );
    }
    let mut body = vec![0u8; length];
    reader
        .read_exact(&mut body)
        .map_err(|e| format!("cannot read the body: {e}"))?;
    let body = String::from_utf8(body).map_err(|_| "the body is not UTF-8".to_string())?;
    match server.handle(&body) {
        Some(response) => respond(&mut stream, 200, "OK", &response, ""),
        // A notification has no response; 202 is the server's acknowledgement.
        None => respond(&mut stream, 202, "Accepted", "", ""),
    }
}

fn respond(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
    extra_headers: &str,
) -> Result<(), String> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|e| format!("cannot write the response: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("cannot write the response: {e}"))
}
