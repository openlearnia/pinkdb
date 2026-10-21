//! Wave 48: RBAC over the wire — login, the authentication gate, and
//! per-statement enforcement, driven through the real [`Client`] and raw
//! frames against **both** transports (the blocking server always, the
//! `async-server` feature's tokio transport when it is built).

use std::sync::Arc;
use std::time::Duration;

use pinkdb::{
    DataType, Database, FileFormat, QueryResult, TableFunctionCall, TableScanOutput, TableSource,
};
use pinkdb_server::protocol::{PROTOCOL_VERSION, Request, Response, read_response, write_request};
use pinkdb_server::{Client, Server, ServerHandle};

#[cfg(feature = "async-server")]
use pinkdb_server::{AsyncServer, AsyncServerHandle};

/// Which transport a test drives: every assertion must hold for both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flavor {
    Blocking,
    #[cfg(feature = "async-server")]
    Async,
}

/// Every transport this build serves: the blocking server always, the tokio
/// one when the `async-server` feature is on.
fn flavors() -> Vec<Flavor> {
    #[cfg(feature = "async-server")]
    {
        vec![Flavor::Blocking, Flavor::Async]
    }
    #[cfg(not(feature = "async-server"))]
    {
        vec![Flavor::Blocking]
    }
}

/// The running server's handle (either transport). The field is never read —
/// the handle is held so the server keeps running for the test, and its
/// `Drop` shuts the accept loop down.
enum Handle {
    #[allow(dead_code)]
    Blocking(ServerHandle),
    #[cfg(feature = "async-server")]
    #[allow(dead_code)]
    Async(AsyncServerHandle),
}

fn start(flavor: Flavor, db: Database) -> (Handle, String) {
    start_configured(flavor, db, false, None)
}

/// Start a server with an explicit COPY policy (`allow_copy`/`copy_root`).
fn start_configured(
    flavor: Flavor,
    db: Database,
    allow_copy: bool,
    copy_root: Option<std::path::PathBuf>,
) -> (Handle, String) {
    match flavor {
        Flavor::Blocking => {
            let server = Server::bind("127.0.0.1:0", db)
                .expect("bind ephemeral server")
                .allow_copy_to(allow_copy)
                .copy_root(copy_root);
            let addr = server.local_addr().to_string();
            let handle = server.spawn().expect("spawn server");
            (Handle::Blocking(handle), addr)
        }
        #[cfg(feature = "async-server")]
        Flavor::Async => {
            let server = AsyncServer::bind("127.0.0.1:0", db)
                .expect("bind ephemeral async server")
                .allow_copy_to(allow_copy)
                .copy_root(copy_root);
            let addr = server.local_addr().to_string();
            let handle = server.spawn().expect("spawn async server");
            (Handle::Async(handle), addr)
        }
    }
}

/// A `COPY … TO` sink for the server tests (the shipped server registers no
/// format, so the tests register a deterministic one).
#[derive(Debug)]
struct TestSink;

impl FileFormat for TestSink {
    fn name(&self) -> &str {
        "testfmt"
    }
    fn extensions(&self) -> &[&str] {
        &["testfmt"]
    }
    fn write(&self, path: &std::path::Path, table: &pinkdb::DataTable) -> pinkdb::Result<()> {
        use std::io::Write as _;
        pinkdb::write_atomic_private(path, |file| {
            file.write_all(format!("rows={}\n", table.num_rows()).as_bytes())?;
            Ok(())
        })
    }
}

/// A table source that claims `read_parquet` / `FROM 'file'` and returns an
/// empty result, so the privilege path is exercised without a parquet build.
#[derive(Debug)]
struct DummySource;

impl TableSource for DummySource {
    fn name(&self) -> &str {
        "rbac-dummy"
    }
    fn bind(&self, call: &TableFunctionCall) -> pinkdb::Result<Option<Vec<(String, DataType)>>> {
        let claimed = matches!(call, TableFunctionCall::Path(_))
            || call.function_name().as_deref() == Some("read_parquet");
        Ok(claimed.then(|| vec![("a".to_string(), DataType::Bigint)]))
    }
    fn scan(
        &self,
        _call: &TableFunctionCall,
        _projection: Option<&[usize]>,
        _filters: &[pinkdb::PrunePredicate],
    ) -> pinkdb::Result<TableScanOutput> {
        Ok(TableScanOutput {
            chunks: Vec::new(),
            ..TableScanOutput::default()
        })
    }
}

fn ensure_test_globals() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        pinkdb::filefmt::register_global_file_format(Arc::new(TestSink));
        pinkdb::table_source::register_global_table_source(Arc::new(DummySource));
    });
}

/// Start a server over a fresh store; the store handle is returned so tests
/// can create users through the (trusted) embedded API.
fn start_fresh(flavor: Flavor) -> (Handle, String, Database) {
    let db = Database::new();
    let (handle, addr) = start(flavor, db.clone());
    (handle, addr, db)
}

/// A trusted embedded connection to the served store (the bootstrap path).
fn embedded(db: &Database) -> pinkdb::Connection {
    db.connect()
}

fn err_of(r: Result<Vec<QueryResult>, pinkdb_server::ClientError>) -> String {
    r.expect_err("statement should have failed").to_string()
}

#[test]
fn login_succeeds_wrong_password_and_unknown_user_are_indistinguishable() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        let mut trusted = embedded(&db);
        trusted
            .execute("CREATE USER Alice PASSWORD 's3cret';")
            .unwrap();

        let mut client = Client::connect(&addr).unwrap();
        // The store's spelling comes back.
        assert_eq!(client.login("alice", "s3cret").unwrap(), "Alice");
        let _ = flavor;

        // A wrong password and an unknown user are one and the same error.
        let mut a = Client::connect(&addr).unwrap();
        let wrong = a.login("alice", "nope").unwrap_err().to_string();
        assert!(wrong.contains("authentication failed"), "{wrong}");
        let unknown = a.login("nobody", "nope").unwrap_err().to_string();
        assert_eq!(wrong, unknown);
        // The connection survives a failed login and can retry.
        assert_eq!(a.login("alice", "s3cret").unwrap(), "Alice");
    }
}

#[test]
fn authentication_is_required_once_any_user_exists() {
    for flavor in flavors() {
        let (_handle, addr, _db) = start_fresh(flavor);
        // Empty catalog: connections are trusted (the documented bootstrap).
        let mut client = Client::connect(&addr).unwrap();
        assert!(client.execute("CREATE TABLE t (i INTEGER);").unwrap().len() == 1);
        client
            .execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
        // The very next statement of the *same* connection is refused: the
        // gate is evaluated per statement, not per connection.
        let refused = err_of(client.execute("SELECT * FROM t;"));
        assert!(refused.contains("authentication required"), "{refused}");
        assert!(refused.contains("Login"), "{refused}");
        // A brand-new connection is refused too, for every statement form.
        let mut fresh = Client::connect(&addr).unwrap();
        assert!(err_of(fresh.execute("SELECT 1;")).contains("authentication required"));
        assert!(
            err_of(fresh.prepare("SELECT 1;").map(|_| Vec::new()))
                .contains("authentication required")
        );
        // Ping never needs authentication (a liveness probe is not SQL).
        fresh.ping().unwrap();
        // A successful login opens the same connection.
        assert_eq!(fresh.login("alice", "alice-secret").unwrap(), "alice");
        assert!(fresh.execute("SELECT 1;").is_ok());
    }
}

#[test]
fn an_unauthenticated_batch_cannot_create_and_promote_an_admin() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        let mut client = Client::connect(&addr).unwrap();
        // H-1: one unauthenticated batch that bootstraps a user and promotes
        // it. The first statement is the documented bootstrap (allowed while
        // the catalog is empty); the gate re-evaluates before the GRANT and
        // refuses it.
        let e =
            err_of(client.execute("CREATE USER evil PASSWORD 'evil-secret'; GRANT ADMIN TO evil;"));
        assert!(e.contains("authentication required"), "{flavor:?}: {e}");
        // The user exists (the first statement committed), but is not admin:
        // the GRANT never ran.
        assert!(db.has_users());
        // Logging in as the created user confirms it has no privileges.
        client.login("evil", "evil-secret").unwrap();
        let e = err_of(client.execute("CREATE USER mallory PASSWORD 'x';"));
        assert!(e.contains("needs the ADMIN privilege"), "{flavor:?}: {e}");
        let mut probe = Client::connect(&addr).unwrap();
        assert!(probe.login("mallory", "x").is_err());
    }
}

#[test]
fn a_batch_is_refused_before_its_first_statement_when_users_exist() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        embedded(&db)
            .execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
        let mut client = Client::connect(&addr).unwrap();
        // Every statement request is refused while the connection has no
        // principal — including a batch whose first statement would otherwise
        // run before the gate noticed the catalog.
        let e = err_of(client.execute("CREATE TABLE pwned (i INTEGER); SELECT 1;"));
        assert!(e.contains("authentication required"), "{flavor:?}: {e}");
        let mut trusted = embedded(&db);
        let e = trusted
            .execute("SELECT * FROM pwned;")
            .unwrap_err()
            .to_string();
        assert!(e.contains("not found"), "{flavor:?}: {e}");
    }
}

#[test]
fn an_authenticated_session_is_not_promoted_when_the_last_user_is_dropped() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        embedded(&db)
            .execute(
                "CREATE USER root PASSWORD 'root-secret';\
                 GRANT ADMIN TO root;\
                 CREATE USER alice PASSWORD 'alice-secret';",
            )
            .unwrap();
        let mut alice = Client::connect(&addr).unwrap();
        alice.login("alice", "alice-secret").unwrap();
        let e = err_of(alice.execute("CREATE TABLE alice_table (a INTEGER);"));
        assert!(e.contains("permission denied"), "{flavor:?}: {e}");

        // An authenticated admin empties the catalog (M-1).
        let mut root = Client::connect(&addr).unwrap();
        root.login("root", "root-secret").unwrap();
        root.execute("DROP USER alice; DROP USER root;").unwrap();
        assert!(!db.has_users());

        // Alice authenticated earlier: she stays checked, and an empty
        // catalog means no grants, not full access.
        let e = err_of(alice.execute("CREATE TABLE alice_table (a INTEGER);"));
        assert!(
            e.contains("permission denied"),
            "{flavor:?}: empty catalog promoted a live session: {e}"
        );
        let e = err_of(alice.execute("CREATE USER mallory PASSWORD 'x';"));
        assert!(e.contains("needs the ADMIN privilege"), "{flavor:?}: {e}");
        let mut probe = Client::connect(&addr).unwrap();
        assert!(probe.login("mallory", "x").is_err());
    }
}

#[test]
fn a_role_granted_admin_is_honored_over_the_wire() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        embedded(&db)
            .execute(
                "CREATE USER alice PASSWORD 'alice-secret';\
                 CREATE ROLE ops;\
                 GRANT ADMIN TO ops;\
                 GRANT ops TO alice;",
            )
            .unwrap();
        let mut alice = Client::connect(&addr).unwrap();
        alice.login("alice", "alice-secret").unwrap();
        // ADMIN-held-through-a-role satisfies the admin-only statements.
        alice
            .execute("CREATE USER bob PASSWORD 'bob-secret';")
            .unwrap_or_else(|e| panic!("{flavor:?}: {e}"));
        alice.execute("CREATE DATABASE other;").unwrap();
        let mut bob = Client::connect(&addr).unwrap();
        bob.login("bob", "bob-secret").unwrap();
        let e = err_of(bob.execute("CREATE DATABASE nope;"));
        assert!(e.contains("needs the ADMIN privilege"), "{flavor:?}: {e}");
    }
}

#[test]
fn prepared_execution_is_gated_when_the_catalog_changes() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        let mut client = Client::connect(&addr).unwrap();
        // Parsing is allowed while the catalog is empty.
        let (handle, _) = client.prepare("CREATE USER pwned PASSWORD 'x';").unwrap();
        // The first user appears after the prepare but before the execute.
        embedded(&db)
            .execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
        let e = err_of(client.execute_prepared(handle, &[]));
        assert!(e.contains("authentication required"), "{flavor:?}: {e}");
        let mut probe = Client::connect(&addr).unwrap();
        assert!(probe.login("pwned", "x").is_err());
        // After login the same prepared statement executes.
        embedded(&db).execute("GRANT ADMIN TO alice;").unwrap();
        client.login("alice", "alice-secret").unwrap();
        client.execute_prepared(handle, &[]).unwrap();
        let mut probe = Client::connect(&addr).unwrap();
        assert_eq!(probe.login("pwned", "x").unwrap(), "pwned");
    }
}

#[test]
fn privileges_are_enforced_per_statement_through_the_server() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        let mut trusted = embedded(&db);
        trusted
            .execute(
                "CREATE TABLE t (i INTEGER);\
                 INSERT INTO t VALUES (1), (2);\
                 CREATE USER alice PASSWORD 'alice-secret';\
                 CREATE ROLE analyst;\
                 GRANT analyst TO alice;",
            )
            .unwrap();

        let mut alice = Client::connect(&addr).unwrap();
        alice.login("alice", "alice-secret").unwrap();
        // No grants: reads and writes are refused with the engine's message.
        let e = err_of(alice.execute("SELECT * FROM t;"));
        assert!(e.contains("permission denied"), "{flavor:?}: {e}");
        assert!(e.contains("GRANT SELECT ON TABLE main.t TO alice"), "{e}");
        let e = err_of(alice.execute("INSERT INTO t VALUES (3);"));
        assert!(e.contains("INSERT privilege"), "{e}");
        let e = err_of(alice.execute("CREATE TABLE other (a INTEGER);"));
        assert!(e.contains("CREATE privilege on database \"main\""), "{e}");
        // Metadata stays available.
        assert!(alice.execute("SHOW TABLES;").is_ok());
        assert!(alice.execute("DESCRIBE t;").is_ok());

        // A grant to the *role* the user is a member of opens the read.
        trusted
            .execute("GRANT SELECT ON TABLE t TO analyst;")
            .unwrap();
        assert!(alice.execute("SELECT * FROM t;").is_ok());
        let rows = match alice.execute("SELECT count(*) FROM t;").unwrap().pop() {
            Some(QueryResult::Selected { rows, .. }) => rows,
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(rows.len(), 1);
        // The write stays refused (SELECT does not imply INSERT).
        assert!(err_of(alice.execute("INSERT INTO t VALUES (3);")).contains("permission denied"));

        // Revoking stops the read immediately.
        trusted
            .execute("REVOKE SELECT ON TABLE t FROM analyst;")
            .unwrap();
        assert!(err_of(alice.execute("SELECT * FROM t;")).contains("permission denied"));

        // ADMIN on the *user* passes every check, including user DDL.
        trusted.execute("GRANT ADMIN TO alice;").unwrap();
        alice.execute("SELECT * FROM t;").unwrap();
        alice
            .execute("CREATE TABLE admin_table (a INTEGER);")
            .unwrap();
        alice
            .execute("CREATE USER bob PASSWORD 'bob-secret';")
            .unwrap();
        // The new user is subject to the checks from its first statement.
        let mut bob = Client::connect(&addr).unwrap();
        bob.login("bob", "bob-secret").unwrap();
        assert!(err_of(bob.execute("SELECT * FROM t;")).contains("permission denied"));
    }
}

#[test]
fn rbac_results_round_trip_over_the_wire() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        // The bootstrap is the trusted embedded path: create the first user
        // and make it an admin.
        embedded(&db)
            .execute(
                "CREATE USER admin PASSWORD 'admin-secret';\
                 GRANT ADMIN TO admin;",
            )
            .unwrap();
        let mut client = Client::connect(&addr).unwrap();
        client.login("admin", "admin-secret").unwrap();
        let results = client
            .execute(
                "CREATE USER alice PASSWORD_HASH \
                 '$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA\
$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';\
                 CREATE ROLE analyst;\
                 CREATE TABLE t (i INTEGER);\
                 ALTER USER alice PASSWORD_HASH \
                 '$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAA\
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';",
            )
            .unwrap_or_else(|e| panic!("{flavor:?}: {e}"));
        assert_eq!(
            results,
            vec![
                QueryResult::CreatedUser {
                    name: "alice".to_string()
                },
                QueryResult::CreatedRole {
                    name: "analyst".to_string()
                },
                QueryResult::Created,
                QueryResult::AlteredUser {
                    name: "alice".to_string()
                },
            ],
            "{flavor:?}"
        );
        let results = client
            .execute(
                "GRANT analyst TO alice;\
                 GRANT SELECT, INSERT ON TABLE t TO analyst;\
                 GRANT ADMIN TO alice;\
                 REVOKE ADMIN FROM alice;\
                 DROP ROLE analyst;\
                 DROP USER alice;",
            )
            .unwrap();
        assert_eq!(
            results,
            vec![
                QueryResult::Granted {
                    description: "ROLE analyst TO alice".to_string()
                },
                QueryResult::Granted {
                    description: "SELECT, INSERT ON TABLE main.t TO analyst".to_string()
                },
                QueryResult::Granted {
                    description: "ADMIN TO alice".to_string()
                },
                QueryResult::Revoked {
                    description: "ADMIN TO alice".to_string()
                },
                QueryResult::DroppedRole {
                    name: "analyst".to_string()
                },
                QueryResult::DroppedUser {
                    name: "alice".to_string()
                },
            ],
            "{flavor:?}"
        );
    }
}

#[test]
fn unauthenticated_connections_cannot_prepare_and_a_failed_login_keeps_them_usable() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        embedded(&db)
            .execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
        let mut client = Client::connect(&addr).unwrap();
        // Prepare is a parse service: refused before the login…
        let e = err_of(client.prepare("SELECT ?;").map(|_| Vec::new()));
        assert!(e.contains("authentication required"), "{flavor:?}: {e}");
        // … a failed login keeps the connection and changes nothing…
        let e = client.login("alice", "wrong").unwrap_err().to_string();
        assert!(e.contains("authentication failed"), "{e}");
        assert!(err_of(client.execute("SELECT 1;")).contains("authentication required"));
        // … and the successful one opens prepared statements too.
        client.login("alice", "alice-secret").unwrap();
        let (handle, parameters) = client.prepare("SELECT ? + 1;").unwrap();
        assert_eq!(parameters, 1);
        assert!(
            client
                .execute_prepared(handle, &[pinkdb::Value::Integer(1)])
                .is_ok()
        );
    }
}

/// The raw handshake path: `Login` frames are answered by `LoggedIn` (with
/// the store's spelling) or the same `Error` text, on either transport, and
/// the connection stays in sync afterwards.
#[test]
fn raw_login_frames_round_trip() {
    for flavor in flavors() {
        let (_handle, addr, db) = start_fresh(flavor);
        embedded(&db)
            .execute("CREATE USER Alice PASSWORD 's3cret';")
            .unwrap();
        let stream = std::net::TcpStream::connect(&addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut writer = stream.try_clone().unwrap();
        let mut reader = std::io::BufReader::new(stream);
        write_request(
            &mut writer,
            &Request::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        match read_response(&mut reader).unwrap() {
            Response::HelloOk { .. } => {}
            other => panic!("expected HelloOk, got {other:?}"),
        }
        write_request(
            &mut writer,
            &Request::Login {
                user: "alice".to_string(),
                password: "s3cret".to_string(),
            },
        )
        .unwrap();
        match read_response(&mut reader).unwrap() {
            Response::LoggedIn { user } => assert_eq!(user, "Alice", "{flavor:?}"),
            other => panic!("expected LoggedIn, got {other:?}"),
        }
        write_request(
            &mut writer,
            &Request::Login {
                user: "alice".to_string(),
                password: "wrong".to_string(),
            },
        )
        .unwrap();
        match read_response(&mut reader).unwrap() {
            Response::Error { message } => {
                assert!(message.contains("authentication failed"), "{message}")
            }
            other => panic!("expected Error, got {other:?}"),
        }
        // A failed login does not reset the adopted principal: the statement
        // below runs as Alice, so the error is a catalog error, not an auth
        // one — and the connection is still in sync (one response per
        // request).
        write_request(
            &mut writer,
            &Request::Execute {
                sql: "SELECT * FROM missing;".to_string(),
            },
        )
        .unwrap();
        match read_response(&mut reader).unwrap() {
            Response::Error { message } => assert!(message.contains("not found"), "{message}"),
            other => panic!("expected Error, got {other:?}"),
        }
        write_request(&mut writer, &Request::Ping).unwrap();
        assert_eq!(read_response(&mut reader).unwrap(), Response::Pong);
    }
}

// ---------------------------------------------------------------------------
// M-3: COPY … TO is ADMIN-only and server-side opt-in.
// ---------------------------------------------------------------------------

#[test]
fn copy_to_is_refused_by_default_even_for_admin() {
    for flavor in flavors() {
        ensure_test_globals();
        let db = Database::new();
        let (handle, addr) = start(flavor, db.clone());
        let root = std::env::temp_dir().join(format!(
            "pinkdb_rbac_copy_default_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("out.testfmt");

        let mut trusted = embedded(&db);
        trusted
            .execute(
                "CREATE TABLE t (a INTEGER);\
                 INSERT INTO t VALUES (1);\
                 CREATE USER admin PASSWORD 'admin-secret';\
                 GRANT ADMIN TO admin;",
            )
            .unwrap();
        let mut admin = Client::connect(&addr).unwrap();
        admin.login("admin", "admin-secret").unwrap();
        let e = err_of(admin.execute(&format!("COPY t TO '{}' (FORMAT testfmt);", file.display())));
        assert!(
            e.contains("COPY TO is disabled"),
            "{flavor:?}: an ADMIN must not bypass the server opt-in: {e}"
        );
        assert!(!file.exists(), "{flavor:?}: no file may be written");
        drop(handle);
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[test]
fn copy_to_opt_in_requires_admin_and_confines_the_destination() {
    for flavor in flavors() {
        ensure_test_globals();
        let db = Database::new();
        let root = std::env::temp_dir().join(format!(
            "pinkdb_rbac_copy_root_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let outside = root
            .parent()
            .unwrap()
            .join(format!("pinkdb_rbac_copy_outside_{}", std::process::id()));
        std::fs::create_dir_all(root.join("sub")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let (handle, addr) = start_configured(flavor, db.clone(), true, Some(root.clone()));

        let mut trusted = embedded(&db);
        trusted
            .execute(
                "CREATE TABLE t (a INTEGER);\
                 INSERT INTO t VALUES (1), (2);\
                 CREATE USER admin PASSWORD 'admin-secret';\
                 CREATE USER alice PASSWORD 'alice-secret';\
                 GRANT ADMIN TO admin;\
                 GRANT SELECT ON TABLE t TO alice;",
            )
            .unwrap();

        // A mere SELECT grant (and no ADMIN) is refused.
        let mut alice = Client::connect(&addr).unwrap();
        alice.login("alice", "alice-secret").unwrap();
        let e = err_of(alice.execute(&format!(
            "COPY t TO '{}' (FORMAT testfmt);",
            root.join("alice.testfmt").display()
        )));
        assert!(e.contains("needs the ADMIN privilege"), "{flavor:?}: {e}");

        let mut admin = Client::connect(&addr).unwrap();
        admin.login("admin", "admin-secret").unwrap();

        // Inside the root: allowed, atomic replace, owner-only mode.
        let file = root.join("sub").join("out.testfmt");
        let first = admin
            .execute(&format!("COPY t TO '{}' (FORMAT testfmt);", file.display()))
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "rows=2\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "{flavor:?}: COPY destination mode");
        }
        // A second COPY atomically replaces the target (truncate semantics).
        std::fs::write(&file, "old longer content").unwrap();
        admin
            .execute(&format!("COPY t TO '{}' (FORMAT testfmt);", file.display()))
            .unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "rows=2\n");

        // Outside the root, `..`, and an absolute escape are refused.
        for hostile in [
            outside.join("escape.testfmt"),
            root.join("..").join("escape.testfmt"),
            std::path::PathBuf::from("/tmp/pinkdb-rbac-escape.testfmt"),
        ] {
            let e = err_of(admin.execute(&format!(
                "COPY t TO '{}' (FORMAT testfmt);",
                hostile.display()
            )));
            assert!(
                e.contains("outside the configured COPY root") || e.contains("`..` path component"),
                "{flavor:?}: {hostile:?}: {e}"
            );
            assert!(!hostile.exists(), "{flavor:?}: {hostile:?} was written");
        }

        // A symlinked target and a symlinked parent are refused.
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let real = root.join("real.testfmt");
            std::fs::write(&real, b"keep").unwrap();
            let link = root.join("link.testfmt");
            symlink(&real, &link).unwrap();
            let e =
                err_of(admin.execute(&format!("COPY t TO '{}' (FORMAT testfmt);", link.display())));
            assert!(e.contains("symlink"), "{flavor:?}: {e}");
            assert_eq!(std::fs::read(&real).unwrap(), b"keep");

            let linked_dir = root.join("linked-dir");
            symlink(&outside, &linked_dir).unwrap();
            let e = err_of(admin.execute(&format!(
                "COPY t TO '{}' (FORMAT testfmt);",
                linked_dir.join("x.testfmt").display()
            )));
            assert!(
                e.contains("symlink") || e.contains("outside the configured COPY root"),
                "{flavor:?}: {e}"
            );
            assert!(!outside.join("x.testfmt").exists());
        }

        // Opting in without a root refuses every COPY with a clear message.
        let (handle2, addr2) = start_configured(flavor, db.clone(), true, None);
        let mut admin2 = Client::connect(&addr2).unwrap();
        admin2.login("admin", "admin-secret").unwrap();
        let e = err_of(admin2.execute(&format!(
            "COPY t TO '{}' (FORMAT testfmt);",
            root.join("no-root.testfmt").display()
        )));
        assert!(e.contains("requires a destination root"), "{flavor:?}: {e}");
        assert!(!root.join("no-root.testfmt").exists());

        drop(handle);
        drop(handle2);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }
}

// ---------------------------------------------------------------------------
// M-4: table-function scans require ADMIN in an authenticated session.
// ---------------------------------------------------------------------------

#[test]
fn table_functions_require_admin_in_every_statement_shape() {
    for flavor in flavors() {
        ensure_test_globals();
        let db = Database::new();
        let (handle, addr) = start(flavor, db.clone());
        // Before the first user exists every connection is trusted: the
        // unauthenticated network path keeps working exactly like the
        // embedded one.
        let mut anon = Client::connect(&addr).unwrap();
        anon.execute("SELECT * FROM read_parquet('/tmp/anon.parquet');")
            .unwrap_or_else(|e| panic!("{flavor:?}: {e}"));
        drop(anon);
        embedded(&db)
            .execute(
                "CREATE USER admin PASSWORD 'admin-secret';\
                 CREATE USER alice PASSWORD 'alice-secret';\
                 GRANT ADMIN TO admin;",
            )
            .unwrap();

        // Embedded/trusted behavior is unchanged: no principal, no check.
        let mut trusted = embedded(&db);
        assert!(
            trusted
                .execute("SELECT * FROM read_parquet('/tmp/trusted.parquet');")
                .is_ok(),
            "{flavor:?}: embedded/trusted table-function scans keep working"
        );

        // A zero-grant authenticated user is denied before execution.
        let mut alice = Client::connect(&addr).unwrap();
        alice.login("alice", "alice-secret").unwrap();
        for sql in [
            "SELECT * FROM read_parquet('/tmp/x.parquet');",
            "WITH c AS (SELECT * FROM read_parquet('/tmp/x.parquet')) SELECT * FROM c;",
            "SELECT * FROM read_parquet('/tmp/x.parquet') UNION ALL SELECT 1;",
            "SELECT (SELECT count(*) FROM read_parquet('/tmp/x.parquet'));",
            "SELECT (SELECT count(*) FROM read_parquet('/tmp/x.parquet')) IN (SELECT 1);",
            "EXPLAIN SELECT * FROM read_parquet('/tmp/x.parquet');",
        ] {
            let e = err_of(alice.execute(sql));
            assert!(
                e.contains("needs the ADMIN privilege"),
                "{flavor:?}: {sql}: {e}"
            );
        }

        // ADMIN may run the same statements.
        let mut admin = Client::connect(&addr).unwrap();
        admin.login("admin", "admin-secret").unwrap();
        for sql in [
            "SELECT * FROM read_parquet('/tmp/x.parquet');",
            "WITH c AS (SELECT * FROM read_parquet('/tmp/x.parquet')) SELECT * FROM c;",
            "SELECT * FROM read_parquet('/tmp/x.parquet') UNION ALL SELECT 1;",
            "EXPLAIN SELECT * FROM read_parquet('/tmp/x.parquet');",
        ] {
            admin
                .execute(sql)
                .unwrap_or_else(|e| panic!("{flavor:?}: {sql}: {e}"));
        }
        drop(handle);
    }
}
