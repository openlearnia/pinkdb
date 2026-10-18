//! Wave 48: RBAC over the wire — login, the authentication gate, and
//! per-statement enforcement, driven through the real [`Client`] and raw
//! frames against **both** transports (the blocking server always, the
//! `async-server` feature's tokio transport when it is built).

use std::time::Duration;

use pinkdb::{Database, QueryResult};
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
    match flavor {
        Flavor::Blocking => {
            let server = Server::bind("127.0.0.1:0", db).expect("bind ephemeral server");
            let addr = server.local_addr().to_string();
            let handle = server.spawn().expect("spawn server");
            (Handle::Blocking(handle), addr)
        }
        #[cfg(feature = "async-server")]
        Flavor::Async => {
            let server = AsyncServer::bind("127.0.0.1:0", db).expect("bind ephemeral async server");
            let addr = server.local_addr().to_string();
            let handle = server.spawn().expect("spawn async server");
            (Handle::Async(handle), addr)
        }
    }
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
