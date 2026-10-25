//! Wave 105, end to end: `pinkdb --mcp <file>` is a real MCP server.
//!
//! The test spawns the CLI with the bridge shared library, speaks JSON-RPC on
//! its stdin/stdout, and answers a TPC-H-style question from a generated
//! dataset. Build the bridge first (`cargo build -p pinkdb-mcp`); the test
//! asks `PINKDB_MCP_LIB` or the workspace target directory for it.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};

const TOOLS: [&str; 8] = [
    "query",
    "describe",
    "tables",
    "databases",
    "explain",
    "lake_tables",
    "lake_versions",
    "lake_changes",
];

fn cli() -> &'static str {
    env!("CARGO_BIN_EXE_pinkdb")
}

/// The bridge cdylib, built by `cargo build -p pinkdb-mcp`.
/// The bridge library, resolved (and built if missing) **once per test
/// binary**. Tests run in parallel: when several each ran `cargo build -p
/// pinkdb-mcp`, the concurrent builds re-linked the cdylib under each
/// other, so a path one test had just found vanished before its child could
/// `dlopen` it ("No such file or directory"). The lock makes one thread
/// build while the others wait for its answer.
fn bridge_library() -> PathBuf {
    static BRIDGE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BRIDGE.get_or_init(resolve_bridge_library).clone()
}

fn resolve_bridge_library() -> PathBuf {
    let name = if cfg!(target_os = "macos") {
        "libpinkdb_mcp.dylib"
    } else if cfg!(target_os = "windows") {
        "pinkdb_mcp.dll"
    } else {
        "libpinkdb_mcp.so"
    };
    let mut candidates = Vec::new();
    if let Ok(path) = std::env::var("PINKDB_MCP_LIB") {
        candidates.push(PathBuf::from(path));
    }
    // Next to the test's own binary is `target/<profile>`; the workspace root
    // holds both profiles.
    let exe = Path::new(cli());
    if let Some(dir) = exe.parent() {
        candidates.push(dir.join(name));
        if let Some(target) = dir.parent() {
            candidates.extend([
                target.join("debug").join(name),
                target.join("release").join(name),
            ]);
        }
    }
    for path in &candidates {
        if path.is_file() {
            return path.clone();
        }
    }
    // CI runs feature jobs that build only the CLI, so a missing bridge is
    // built here rather than left to fail the test.
    let built = Command::new(env!("CARGO"))
        .args(["build", "-q", "-p", "pinkdb-mcp"])
        .status()
        .is_ok_and(|status| status.success());
    if built && let Some(dir) = Path::new(cli()).parent() {
        for path in [dir.join(name), dir.join("deps").join(name)] {
            if path.is_file() {
                return path;
            }
        }
    }
    panic!(
        "the MCP bridge is not built: {} (run `cargo build -p pinkdb-mcp`, or set PINKDB_MCP_LIB)",
        candidates
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

/// A fresh durable store per test, holding the TPC-H-shaped fixture.
struct Fixture {
    dir: PathBuf,
    store: PathBuf,
}

impl Fixture {
    fn new(tag: &str, script: &str) -> Fixture {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-mcp-{}-{tag}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the store directory");
        let store = dir.join("store.pink");
        let path = store.display().to_string();
        let script = format!(".open {path}\n{script}\n.save {path}\n");
        let mut child = Command::new(cli())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the CLI");
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(script.as_bytes())
            .expect("feed the fixture");
        let output = child.wait_with_output().expect("seed the store");
        assert!(
            output.status.success(),
            "seeding failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Fixture { dir, store }
    }

    fn client(&self) -> Client {
        self.client_with(&[])
    }

    /// A client for a server started with extra flags (`--mcp-allow-files`).
    fn client_with(&self, extra: &[&str]) -> Client {
        let mut child = Command::new(cli())
            .arg("--mcp")
            .arg(&self.store)
            .arg("--mcp-extension")
            .arg(bridge_library())
            .args(extra)
            // The cost hint is the CLI's own threshold; one row makes the
            // fixture big enough to see it.
            .env("PINKDB_AGENT_HINT_ROWS", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn pinkdb --mcp");
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
        Client {
            child,
            stdin,
            stdout,
            next_id: 0,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

const FIXTURE: &str = "\
CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR);
CREATE TABLE lineitem (customer_id INTEGER, quantity INTEGER, price INTEGER);
INSERT INTO customers VALUES (1, 'Acme'), (2, 'Bravo'), (3, 'Cedar'), (4, 'Delta'), (5, 'Echo');
INSERT INTO lineitem VALUES (1, 3, 100), (1, 2, 50), (2, 5, 10), (3, 2, 30), (4, 4, 5), (5, 1, 1000);
";

/// A minimal MCP client: one JSON-RPC message per line, in both directions.
struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Client {
    fn send(&mut self, message: Value) {
        writeln!(self.stdin, "{message}").expect("write to the server");
        self.stdin.flush().expect("flush");
    }

    fn notify(&mut self, method: &str) {
        self.send(json!({"jsonrpc": "2.0", "method": method}));
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read from the server");
            if read == 0 {
                // Show why the server went away: its exit status and stderr.
                let status = self.child.wait().ok();
                let mut stderr = String::new();
                if let Some(mut pipe) = self.child.stderr.take() {
                    let _ = std::io::Read::read_to_string(&mut pipe, &mut stderr);
                }
                panic!(
                    "the server closed its stdout (id {id} unanswered); exit {status:?}; stderr:\n{stderr}"
                );
            }
            let line = line.trim();
            if !line.starts_with('{') {
                continue;
            }
            let value: Value = serde_json::from_str(line).expect("a JSON-RPC message");
            if value["id"] == json!(id) {
                return value;
            }
        }
    }

    fn initialize(&mut self) -> Value {
        let reply = self.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "pinkdb e2e", "version": "0"},
            }),
        );
        self.notify("notifications/initialized");
        reply
    }

    /// `tools/call`, returning `(isError, text)`.
    fn tool(&mut self, name: &str, arguments: Value) -> (bool, String) {
        let reply = self.request("tools/call", json!({"name": name, "arguments": arguments}));
        assert!(reply.get("error").is_none(), "{name}: {reply}");
        let result = &reply["result"];
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        (result["isError"] == json!(true), text)
    }

    fn query(&mut self, sql: &str) -> (bool, String) {
        self.tool("query", json!({"sql": sql}))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn initializes_and_lists_every_tool_with_a_schema() {
    let fixture = Fixture::new("list", FIXTURE);
    let mut client = fixture.client();
    let init = client.initialize();
    let result = &init["result"];
    assert_eq!(result["serverInfo"]["name"], json!("pinkdb"));
    assert!(result["capabilities"]["tools"].is_object(), "{init}");
    assert!(init["result"]["instructions"].as_str().is_some(), "{init}");
    let ping = client.request("ping", json!({}));
    assert!(ping.get("result").is_some(), "{ping}");
    let list = client.request("tools/list", json!({}));
    let tools = list["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, TOOLS);
    for tool in tools {
        let name = tool["name"].as_str().unwrap();
        assert!(
            tool["description"]
                .as_str()
                .is_some_and(|text| !text.is_empty()),
            "{name} needs a description"
        );
        let schema = &tool["inputSchema"];
        assert_eq!(schema["type"], json!("object"), "{name}: {schema}");
        assert!(schema["properties"].is_object(), "{name}: {schema}");
        assert_eq!(schema["additionalProperties"], json!(false), "{name}");
    }
}

#[test]
fn answers_a_tpch_style_question() {
    let fixture = Fixture::new("tpch", FIXTURE);
    let mut client = fixture.client();
    client.initialize();
    let (is_error, text) = client.query(
        "SELECT c.name, SUM(l.quantity * l.price) AS revenue \
         FROM lineitem l JOIN customers c ON c.id = l.customer_id \
         GROUP BY c.name ORDER BY revenue DESC, c.name LIMIT 3",
    );
    assert!(!is_error, "{text}");
    assert!(text.contains("revenue:BIGINT"), "{text}");
    let echo = text.find("| Echo |").expect("Echo is the top customer");
    let acme = text.find("| Acme |").expect("Acme is second");
    let cedar = text.find("| Cedar |").expect("Cedar is third");
    assert!(echo < acme && acme < cedar, "revenue order:\n{text}");
    assert!(text.contains("| Echo | 1000 |"), "{text}");
    assert!(text.contains("| Acme | 400 |"), "{text}");
    assert!(
        !text.contains("Bravo"),
        "LIMIT 3 must drop the rest:\n{text}"
    );
}

#[test]
fn reads_the_catalog() {
    let fixture = Fixture::new("catalog", FIXTURE);
    let mut client = fixture.client();
    client.initialize();
    let (is_error, text) = client.tool("databases", json!({}));
    assert!(!is_error, "{text}");
    assert!(text.contains("main"), "{text}");
    let (is_error, text) = client.tool("tables", json!({}));
    assert!(!is_error, "{text}");
    assert!(
        text.contains("customers") && text.contains("lineitem"),
        "{text}"
    );
    let (is_error, text) = client.tool("describe", json!({"table": "lineitem"}));
    assert!(!is_error, "{text}");
    assert!(text.contains("quantity"), "{text}");
    let (is_error, text) = client.tool(
        "explain",
        json!({"sql": "SELECT c.name FROM customers c JOIN lineitem l ON l.customer_id = c.id"}),
    );
    assert!(!is_error, "{text}");
    assert!(text.to_lowercase().contains("join"), "{text}");
    // Wave 99's scan estimate rides along, under the same env threshold.
    assert!(text.contains("Estimated"), "{text}");
    assert!(text.contains("rows scanned"), "{text}");
}

#[test]
fn read_only_refuses_writes_and_errors_are_json() {
    let fixture = Fixture::new("readonly", FIXTURE);
    let mut client = fixture.client();
    client.initialize();
    for sql in [
        "INSERT INTO customers VALUES (6, 'Fox')",
        "UPDATE customers SET name = 'Fox' WHERE id = 1",
        "DELETE FROM customers",
        "CREATE TABLE nope (x INTEGER)",
        "DROP TABLE customers",
        "CREATE DATABASE nope",
        "CREATE USER nope PASSWORD 'x'",
        "COPY customers TO 'customers.csv'",
        "LOAD EXTENSION 'elsewhere.dylib'",
        "CHECKPOINT",
    ] {
        let (is_error, text) = client.query(sql);
        assert!(is_error, "{sql} must be refused: {text}");
        assert!(text.contains("read-only"), "{sql}: {text}");
        assert!(text.starts_with("{\"error\""), "{sql}: {text}");
    }
    // Lake maintenance arrives as a dot command: the default build has no
    // lake hook, and a build with `pinklake` refuses the write commands in
    // `lake::lake_markdown` (tested there).
    let (is_error, text) = client.query(".lake commit /tmp/nope");
    assert!(is_error, "{text}");

    // The refusals left the store alone.
    let (is_error, text) = client.query("SELECT count(*) AS n FROM customers");
    assert!(!is_error, "{text}");
    assert!(text.contains("| 5 |"), "{text}");

    let (is_error, text) = client.query("SELECT * FROM no_such_table");
    assert!(is_error, "{text}");
    assert!(text.contains("\"kind\":\"catalog\""), "{text}");

    let (is_error, text) = client.tool("describe", json!({"table": "no_such_table"}));
    assert!(is_error, "{text}");
    assert!(text.contains("not found"), "{text}");
}

/// Wave 108a: full-text search is plain SQL, so the read-only `query` tool
/// may rank with `match_bm25`; building or dropping an index writes and is
/// refused. Only built when the CLI ships the `fts` feature.
#[cfg(feature = "fts")]
#[test]
fn full_text_search_reads_but_index_creation_is_refused() {
    let fixture = Fixture::new(
        "fts",
        "CREATE TABLE docs (id INTEGER, body VARCHAR);\n\
         INSERT INTO docs VALUES (1, 'relational databases rank rows'), \
                                 (2, 'a bread recipe needs flour');\n\
         CALL create_fts_index('docs', 'id', 'body');\n",
    );
    let mut client = fixture.client();
    client.initialize();
    let (is_error, text) = client.query(
        "SELECT id, match_bm25('docs', id, 'relational database') AS score \
         FROM docs ORDER BY score DESC",
    );
    assert!(!is_error, "{text}");
    assert!(text.contains("| 1 |"), "{text}");
    for sql in [
        "CALL create_fts_index('docs', 'id', 'body', overwrite := 1)",
        "CALL drop_fts_index('docs')",
    ] {
        let (is_error, text) = client.query(sql);
        assert!(is_error, "{sql} must be refused: {text}");
        assert!(text.contains("read-only"), "{sql}: {text}");
    }
    // The refused calls left the index in place.
    let (is_error, text) = client.query("SELECT count(*) AS n FROM docs");
    assert!(!is_error, "{text}");
    assert!(text.contains("| 2 |"), "{text}");
}

/// Reading files and URLs is its own switch: `allow-writes` does not imply
/// it, and the default session must not open anything a prompt injection
/// names. The default build registers no file source, so with the switch the
/// statement fails in the catalog instead — what matters here is that the
/// host's policy is what changes.
#[test]
fn external_files_need_allow_files() {
    let fixture = Fixture::new("files", FIXTURE);
    let csv = fixture.dir.join("rows.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let path = csv.display().to_string();
    let sqls = [
        format!("SELECT * FROM read_csv('{path}')"),
        format!("SELECT * FROM read_parquet('{path}')"),
        format!("SELECT * FROM '{path}'"),
        format!("WITH c AS (SELECT * FROM read_csv('{path}')) SELECT * FROM c"),
        format!("SELECT * FROM (SELECT * FROM read_csv('{path}')) AS s"),
        format!("SELECT 1 UNION ALL SELECT * FROM read_csv('{path}')"),
        format!("EXPLAIN SELECT * FROM read_csv('{path}')"),
    ];

    let mut client = fixture.client();
    client.initialize();
    for sql in &sqls {
        let (is_error, text) = client.query(sql);
        assert!(is_error, "{sql} must be refused: {text}");
        assert!(text.contains("\"kind\":\"permission\""), "{sql}: {text}");
        assert!(text.contains("allow-files"), "{sql}: {text}");
    }

    let mut client = fixture.client_with(&["--mcp-allow-files"]);
    client.initialize();
    for sql in &sqls {
        let (_, text) = client.query(sql);
        assert!(
            !text.contains("allow-files"),
            "{sql} was still refused: {text}"
        );
    }
    // The switch does not change what a table query does.
    let (is_error, text) = client.query("SELECT count(*) AS n FROM customers");
    assert!(!is_error, "{text}");
    assert!(text.contains("| 5 |"), "{text}");
}

#[test]
fn a_wide_result_is_truncated_with_a_notice() {
    let script = format!(
        "{FIXTURE}CREATE TABLE wide (x INTEGER);\nINSERT INTO wide VALUES {};\n",
        (1..=150)
            .map(|n| format!("({n})"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let fixture = Fixture::new("wide", &script);
    let mut client = fixture.client();
    client.initialize();
    let (is_error, text) = client.query("SELECT x FROM wide ORDER BY x");
    assert!(!is_error, "{text}");
    assert!(text.contains("more rows"), "{text}");
    assert!(text.contains("150 total"), "{text}");
}

/// The lake readers go through the CLI's `.lake` commands, which only a build
/// with `pinklake` has. The store is snapshotted into a catalog first.
#[cfg(feature = "pinklake")]
#[test]
fn lake_tools_read_a_catalog() {
    let fixture = Fixture::new("lake", FIXTURE);
    let catalog = fixture.dir.join("catalog");
    let output = Command::new(cli())
        .args(["-c", &format!(".open {}", fixture.store.display())])
        .args(["-c", &format!(".lake save {}", catalog.display())])
        .output()
        .expect("save a lake");
    assert!(
        output.status.success(),
        "saving the lake failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut client = fixture.client();
    client.initialize();
    let path = catalog.display().to_string();
    let (is_error, text) = client.tool("lake_tables", json!({"catalog": path}));
    assert!(!is_error, "{text}");
    assert!(
        text.contains("customers") && text.contains("lineitem"),
        "{text}"
    );

    let (is_error, text) = client.tool("lake_versions", json!({"catalog": path}));
    assert!(!is_error, "{text}");
    assert!(text.contains("| 1 |"), "{text}");

    // `to` defaults to the current version; asking for the version that
    // created a table alone is a pink-lake error, not an empty change set.
    let (is_error, text) = client.tool(
        "lake_changes",
        json!({"catalog": path, "table": "customers", "from": 1}),
    );
    assert!(!is_error, "{text}");
    assert!(text.contains("change:VARCHAR"), "{text}");
    assert!(text.contains("insert") && text.contains("Acme"), "{text}");
}

/// The streamable-HTTP transport carries the whole protocol, not one call:
/// initialize, tools/list and a tools/call against a loopback session, each
/// answered by the extension over one POST.
#[test]
fn http_transport_round_trips_the_protocol() {
    let fixture = Fixture::new("http", FIXTURE);
    let (_session, port) = serve_http(&fixture.store, "allow-files");

    let reply = http_post(
        port,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                           "clientInfo": {"name": "pinkdb e2e", "version": "0"}}}),
    );
    assert_eq!(reply["jsonrpc"], json!("2.0"), "{reply}");
    assert_eq!(reply["id"], json!(1), "{reply}");
    assert_eq!(reply["result"]["serverInfo"]["name"], json!("pinkdb"));
    assert!(
        reply["result"]["capabilities"]["tools"].is_object(),
        "{reply}"
    );

    let reply = http_post(
        port,
        &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    );
    assert_eq!(reply["id"], json!(2), "{reply}");
    let tools = reply["result"]["tools"].as_array().expect("a tool list");
    assert_eq!(tools.len(), TOOLS.len(), "{reply}");
    for tool in tools {
        assert!(
            tool["inputSchema"]["type"] == json!("object"),
            "{}: {tool}",
            tool["name"]
        );
    }

    let (is_error, text) = http_tool(
        port,
        3,
        "query",
        json!({"sql": "SELECT c.name, SUM(l.quantity * l.price) AS revenue \
                       FROM lineitem l JOIN customers c ON c.id = l.customer_id \
                       GROUP BY c.name ORDER BY revenue DESC, c.name LIMIT 3"}),
    );
    assert!(!is_error, "{text}");
    assert!(text.contains("| Echo | 1000 |"), "{text}");
    assert!(text.contains("| Acme | 400 |"), "{text}");

    // `.mcp serve … allow-files` reaches the host too: the file policy no
    // longer refuses, and this build registers no file source, so the answer
    // is a catalog error rather than a permission one.
    let (_, text) = http_tool(
        port,
        4,
        "query",
        json!({"sql": "SELECT * FROM read_csv('/tmp/x.csv')"}),
    );
    assert!(!text.contains("allow-files"), "{text}");
}

/// The same statements succeed once the session is started with
/// `allow-writes`: the HTTP transport serves the write-enabled session.
#[test]
fn allow_writes_lets_the_same_statements_through() {
    let fixture = Fixture::new("writes", FIXTURE);
    let (_session, port) = serve_http(&fixture.store, "allow-writes");
    http_initialize(port);

    for (id, sql, expected) in [
        (2, "INSERT INTO customers VALUES (6, 'Fox')", "Inserted"),
        (
            3,
            "UPDATE customers SET name = 'Foxx' WHERE id = 6",
            "Updated",
        ),
        (4, "DELETE FROM customers WHERE id = 6", "Deleted"),
        (5, "CREATE TABLE scratch (a INTEGER)", "Created"),
        (6, "DROP TABLE scratch", "Dropped table scratch"),
        (7, "CREATE DATABASE scratch_db", "Created database"),
        (8, "CHECKPOINT", "Checkpointed"),
    ] {
        let (is_error, text) = http_tool(port, id, "query", json!({"sql": sql}));
        assert!(!is_error, "{sql}: {text}");
        assert!(text.contains(expected), "{sql}: {text}");
    }

    // `COPY … TO` is a write the policy allows; the default build registers
    // no file format, so only the refusal text is asserted against.
    let (is_error, text) = http_tool(
        port,
        9,
        "query",
        json!({"sql": "COPY customers TO 'customers.csv'"}),
    );
    assert!(!text.contains("read-only"), "{text}");
    assert!(!is_error || text.starts_with("{\"error\""), "{text}");

    // `LOAD EXTENSION` is allowed too, and fails here only because the bridge
    // is already loaded.
    let (_, text) = http_tool(
        port,
        10,
        "query",
        json!({"sql": format!("LOAD EXTENSION '{}'", bridge_path().display())}),
    );
    assert!(!text.contains("read-only"), "{text}");

    let (is_error, text) = http_tool(
        port,
        11,
        "query",
        json!({"sql": "SELECT count(*) AS n FROM customers"}),
    );
    assert!(!is_error, "{text}");
    assert!(text.contains("| 5 |"), "{text}");
}

fn bridge_path() -> PathBuf {
    bridge_library()
}

/// One JSON-RPC message over the bridge's streamable HTTP transport.
/// Kills the spawned session on drop, so a failed assertion does not leak it.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start a session that serves `store` over HTTP under `policy`. The port is
/// 0 so the kernel picks one: a port reserved in the test could be taken by a
/// parallel test between the check and the child's bind, and the client would
/// then reach the wrong server. The guard keeps the session alive.
fn serve_http(store: &Path, policy: &str) -> (ChildGuard, u16) {
    let child = Command::new(cli())
        .args(["--allow-load-extension"])
        .args(["-c", &format!(".open {}", store.display())])
        .args([
            "-c",
            &format!("LOAD EXTENSION '{}'", bridge_path().display()),
        ])
        .args(["-c", &format!(".mcp serve http 127.0.0.1:0 {policy}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn an HTTP session");
    let mut guard = ChildGuard(child);
    let port = listening_port(&mut guard.0);
    (guard, port)
}

/// The port the session bound, read from its `listening on http://…` line.
/// Stderr keeps being drained on the reading thread afterwards, so the
/// session never blocks on a full pipe.
fn listening_port(child: &mut Child) -> u16 {
    let stderr = child.stderr.take().expect("the session's stderr");
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut seen = String::new();
        let mut port = None;
        for line in reader.by_ref().lines() {
            let Ok(line) = line else { break };
            if let Some((_, rest)) = line.split_once("listening on http://")
                && let Some((_, digits)) = rest.rsplit_once(':')
            {
                // Another thread sharing stderr can interleave into this line;
                // the port still ends at the first non-digit.
                let digits: String = digits
                    .trim()
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect();
                port = digits.parse().ok();
                if port.is_some() {
                    break;
                }
            }
            seen.push_str(&line);
            seen.push('\n');
        }
        let _ = sender.send((port, seen));
        let mut rest = String::new();
        let _ = reader.read_to_string(&mut rest);
    });
    match receiver.recv_timeout(Duration::from_secs(30)) {
        Ok((Some(port), _)) => port,
        Ok((None, seen)) => panic!("the session never announced its port: {seen}"),
        Err(error) => panic!("the session never announced its port: {error}"),
    }
}

fn http_post(port: u16, message: &Value) -> Value {
    let body = message.to_string();
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write");
    stream.flush().expect("flush");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    let payload = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    serde_json::from_str(payload.trim()).expect("a JSON-RPC reply")
}

fn http_initialize(port: u16) {
    let reply = http_post(
        port,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                           "clientInfo": {"name": "pinkdb e2e", "version": "0"}}}),
    );
    assert_eq!(reply["result"]["serverInfo"]["name"], json!("pinkdb"));
}

fn http_tool(port: u16, id: u64, name: &str, arguments: Value) -> (bool, String) {
    let reply = http_post(
        port,
        &json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
                "params": {"name": name, "arguments": arguments}}),
    );
    let result = &reply["result"];
    let text = result["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (result["isError"] == json!(true), text)
}
