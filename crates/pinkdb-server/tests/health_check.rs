//! `pinkdb-server --health-check` drives the real binary: it must speak the
//! protocol `Ping` (not just open a TCP socket), exit 0/1, honour an explicit
//! address, fall back to the container environment, and fail fast against a
//! peer that accepts and never answers.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Start the real server on an ephemeral port and read its readiness line.
fn spawn_server() -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(["--host", "127.0.0.1", "--port", "0"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pinkdb-server");
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("readiness line");
    let addr = line
        .split_once("listening on ")
        .unwrap_or_else(|| panic!("readiness line missing 'listening on': {line:?}"))
        .1
        .split_whitespace()
        .next()
        .expect("address")
        .to_string();
    (child, addr)
}

fn stop(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn health_check(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(args)
        .env_remove("PINKDB_HEALTHCHECK_ADDR")
        .env_remove("PINKDB_HEALTHCHECK_TIMEOUT_MS")
        .env_remove("PINKDB_TLS_CERT")
        .output()
        .expect("run pinkdb-server --health-check")
}

#[test]
fn health_check_exits_zero_for_a_live_server_and_one_for_a_dead_address() {
    let (child, addr) = spawn_server();

    let healthy = health_check(&["--health-check", &addr]);
    assert!(
        healthy.status.success(),
        "live server was unhealthy: {}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    let stdout = String::from_utf8_lossy(&healthy.stdout);
    assert!(stdout.contains("healthy"), "{stdout}");

    // Bind then drop a port so it is (almost certainly) closed: connect fails
    // immediately, not after the probe timeout.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind dead port");
    let dead = listener.local_addr().unwrap().to_string();
    drop(listener);
    let unhealthy = health_check(&["--health-check", &dead]);
    assert_eq!(unhealthy.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&unhealthy.stderr);
    assert!(stderr.contains("unhealthy"), "{stderr}");

    stop(child);
}

#[test]
fn health_check_without_an_address_uses_the_container_environment() {
    let (child, addr) = spawn_server();
    let (host, port) = addr.rsplit_once(':').expect("host:port");

    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .arg("--health-check")
        .env("PINKDB_HOST", host)
        .env("PINKDB_PORT", port)
        .env_remove("PINKDB_HEALTHCHECK_ADDR")
        .env_remove("PINKDB_TLS_CERT")
        .output()
        .expect("run pinkdb-server --health-check");
    assert!(
        out.status.success(),
        "environment-derived target failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains(&addr), "{stdout}");

    stop(child);
}

#[test]
fn health_check_fails_fast_against_a_silent_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent peer");
    let addr = listener.local_addr().unwrap().to_string();
    let peer = std::thread::spawn(move || {
        // Accept the probe, then never write a Pong.
        if let Ok((_stream, _peer)) = listener.accept() {
            std::thread::sleep(Duration::from_millis(1_000));
        }
    });

    let start = Instant::now();
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(["--health-check", &addr])
        .env("PINKDB_HEALTHCHECK_TIMEOUT_MS", "200")
        .env_remove("PINKDB_HEALTHCHECK_ADDR")
        .env_remove("PINKDB_TLS_CERT")
        .output()
        .expect("run pinkdb-server --health-check");
    let elapsed = start.elapsed();

    assert_eq!(out.status.code(), Some(1));
    assert!(
        elapsed < Duration::from_secs(3),
        "the probe waited {elapsed:?} despite a 200 ms timeout"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unhealthy"), "{stderr}");

    let _ = peer.join();
}

/// A container serving TLS (`PINKDB_TLS_CERT` set) refuses the plaintext
/// `Ping`, so the probe checks that the port accepts a TCP connection: an
/// accepting peer is healthy, a closed port is not.
#[test]
fn health_check_with_tls_configured_checks_tcp_accept() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind accepting peer");
    let addr = listener.local_addr().unwrap().to_string();
    let peer = std::thread::spawn(move || {
        let _ = listener.accept();
    });
    let probe = |target: &str| {
        Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
            .args(["--health-check", target])
            .env("PINKDB_TLS_CERT", "/certs/server.pem")
            .env("PINKDB_HEALTHCHECK_TIMEOUT_MS", "500")
            .env_remove("PINKDB_HEALTHCHECK_ADDR")
            .output()
            .expect("run pinkdb-server --health-check")
    };
    let healthy = probe(&addr);
    assert!(
        healthy.status.success(),
        "an accepting TLS port must be healthy: {}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    assert!(
        String::from_utf8_lossy(&healthy.stdout).contains("TCP accept"),
        "{}",
        String::from_utf8_lossy(&healthy.stdout)
    );
    let _ = peer.join();

    let closed = TcpListener::bind("127.0.0.1:0").expect("bind closed port");
    let dead = closed.local_addr().unwrap().to_string();
    drop(closed);
    assert_eq!(probe(&dead).status.code(), Some(1));
}
