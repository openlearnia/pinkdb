//! DuckDB-compatible secrets and the `ducklake:` attach form (wave 132b).
//!
//! Covers the statement surface (`CREATE`/`DROP SECRET`, `duckdb_secrets()`,
//! `which_secret()`), the refusals that name the offending option, the
//! builtin-extension no-ops QuackLab's attach sequence runs first, and — with
//! the `encryption` feature — that persistent secret values survive only
//! sealed in the checkpoint and WAL.

#[cfg(feature = "encryption")]
use std::path::PathBuf;
#[cfg(feature = "encryption")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "encryption")]
use pinkdb::SyncPolicy;
use pinkdb::{PinkDb, QueryResult, Value};

#[cfg(feature = "encryption")]
static SEQ: AtomicU64 = AtomicU64::new(0);

fn rows_of(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn error(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

fn text(value: &Value) -> String {
    match value {
        Value::Varchar(s) => s.clone(),
        other => panic!("expected varchar, got {other:?}"),
    }
}

#[test]
fn secret_ddl_and_catalog_functions() {
    let mut db = PinkDb::new();
    db.execute("CREATE SECRET s (TYPE S3, KEY_ID 'kid', SECRET 'S3CRET', REGION 'us-east-1');")
        .unwrap();
    db.execute("CREATE PERSISTENT SECRET p (TYPE S3, SECRET 'PSECRET');")
        .unwrap();
    // Replace and IF NOT EXISTS are DuckDB's: the second value wins, the
    // third statement leaves the replacing secret alone.
    db.execute("CREATE OR REPLACE SECRET s (TYPE S3, SECRET 'second');")
        .unwrap();
    db.execute("CREATE SECRET IF NOT EXISTS s (TYPE S3, SECRET 'ignored');")
        .unwrap();

    let rows = rows_of(
        &mut db,
        "SELECT name, type, persistent, storage FROM duckdb_secrets() ORDER BY name;",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Varchar("p".into()),
                Value::Varchar("s3".into()),
                Value::Boolean(true),
                Value::Varchar("local_file".into()),
            ],
            vec![
                Value::Varchar("s".into()),
                Value::Varchar("s3".into()),
                Value::Boolean(false),
                Value::Varchar("memory".into()),
            ],
        ]
    );

    let shown = rows_of(
        &mut db,
        "SELECT secret_string FROM duckdb_secrets() WHERE name = 's';",
    );
    let shown = text(&shown[0][0]);
    assert!(shown.contains("secret=redacted"), "{shown}");
    assert!(!shown.contains("second"), "{shown}");

    assert_eq!(
        rows_of(
            &mut db,
            "SELECT name, persistent FROM which_secret('s3://bucket/key', 's3');"
        ),
        vec![vec![
            Value::Varchar("s".into()),
            Value::Varchar("TEMPORARY".into()),
        ]]
    );
    assert!(
        rows_of(
            &mut db,
            "SELECT * FROM which_secret('s3://bucket/key', 'postgres');"
        )
        .is_empty()
    );
}

#[test]
fn an_unnamed_r2_secret_derives_its_endpoint_and_default_name() {
    let mut db = PinkDb::new();
    db.execute("CREATE SECRET (TYPE R2, ACCOUNT_ID 'acct', KEY_ID 'k', SECRET 'v');")
        .unwrap();
    let rows = rows_of(&mut db, "SELECT name, secret_string FROM duckdb_secrets();");
    assert_eq!(text(&rows[0][0]), "__default_r2");
    let shown = text(&rows[0][1]);
    assert!(
        shown.contains("endpoint=acct.r2.cloudflarestorage.com"),
        "{shown}"
    );
    assert!(shown.contains("url_style=path"), "{shown}");
    assert!(shown.contains("scope=s3://"), "{shown}");
    assert!(!shown.contains("v;"), "{shown}");
}

#[test]
fn drops_name_the_scope_and_report_a_missing_secret() {
    let mut db = PinkDb::new();
    db.execute("CREATE SECRET s (TYPE S3, SECRET 'v');")
        .unwrap();
    db.execute("DROP SECRET s;").unwrap();
    assert!(rows_of(&mut db, "SELECT * FROM duckdb_secrets();").is_empty());
    assert!(error(&mut db, "DROP SECRET s;").contains("non-existent secret"));
    db.execute("DROP SECRET IF EXISTS s;").unwrap();

    db.execute("CREATE PERSISTENT SECRET p (TYPE S3, SECRET 'v');")
        .unwrap();
    assert!(error(&mut db, "DROP TEMPORARY SECRET p;").contains("non-existent secret"));
    assert_eq!(
        rows_of(&mut db, "SELECT count(*) FROM duckdb_secrets();")[0][0],
        Value::Bigint(1)
    );
    db.execute("DROP PERSISTENT SECRET p;").unwrap();
    assert!(rows_of(&mut db, "SELECT * FROM duckdb_secrets();").is_empty());
}

#[test]
fn every_refusal_names_the_offending_option() {
    let mut db = PinkDb::new();
    assert!(error(&mut db, "CREATE SECRET x (TYPE bogus);").contains("unknown secret type"));
    assert!(error(&mut db, "CREATE SECRET x (TYPE S3, TOKEN 't');").contains("no option"));
    assert!(
        error(
            &mut db,
            "CREATE SECRET x (TYPE S3, REGION 'a', REGION 'b');"
        )
        .contains("given twice")
    );
    assert!(error(&mut db, "CREATE SECRET x (TYPE S3, TYPE R2);").contains("TYPE given twice"));
    assert!(error(&mut db, "CREATE SECRET x (TYPE POSTGRES, PORT 'nope');").contains("PORT"));
    assert!(error(&mut db, "CREATE SECRET x (TYPE S3, URL_STYLE 'nope');").contains("URL_STYLE"));
    assert!(
        error(
            &mut db,
            "CREATE SECRET x (TYPE R2, ACCOUNT_ID 'a', ENDPOINT 'e');"
        )
        .contains("ENDPOINT")
    );
    db.execute("CREATE SECRET s (TYPE S3, SECRET 'v');")
        .unwrap();
    assert!(
        error(&mut db, "SELECT * FROM duckdb_secrets(redact := false);")
            .contains("unredacted secrets is disabled")
    );
    assert!(db.execute("SELECT * FROM which_secret('s3://x');").is_err());
}

#[test]
fn builtin_extensions_install_and_load_as_noops() {
    let mut db = PinkDb::new();
    for sql in [
        "INSTALL ducklake;",
        "LOAD ducklake;",
        "INSTALL postgres_scanner;",
        "LOAD postgres_scanner;",
        "INSTALL httpfs;",
        "LOAD httpfs;",
    ] {
        db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(db.execute("INSTALL no_such_extension_xyz;").is_err());
}

#[test]
fn ducklake_attach_forms_and_option_refusals() {
    let mut db = PinkDb::new();
    // A bare DuckDB database file is named, not misparsed as a catalog.
    assert!(
        error(&mut db, "ATTACH 'ducklake:/tmp/warehouse.ducklake' AS w;")
            .contains("DuckDB database file")
    );
    assert!(
        error(
            &mut db,
            "ATTACH 'ducklake:sqlite:/tmp/x.sqlite' AS w (SNAPSHOT_TIME '2024-01-01');"
        )
        .contains("SNAPSHOT_TIME")
    );
    assert!(
        error(
            &mut db,
            "ATTACH 'ducklake:sqlite:/tmp/x.sqlite' AS w (METADATA_SCHEMA 'lake');"
        )
        .contains("METADATA_SCHEMA")
    );
    assert!(
        error(
            &mut db,
            "ATTACH 'ducklake:sqlite:/tmp/x.sqlite' AS w (ENCRYPTED);"
        )
        .contains("ENCRYPTED")
    );
    assert!(
        error(&mut db, "ATTACH 'lake:/tmp/x' AS w (SNAPSHOT_TIME 'now');")
            .contains("SNAPSHOT_TIME")
    );
    assert!(
        error(
            &mut db,
            "ATTACH 'ducklake:sqlite:/tmp/x.sqlite' AS w (VERSION 3);"
        )
        .contains("READ_ONLY")
    );
    assert!(error(&mut db, "ATTACH 'not-a-lake' AS w;").contains("not a lake"));
}

/// A sealed store: neither the checkpoint nor the WAL may contain a secret
/// value in the clear, and the reopened store still knows every secret.
#[cfg(feature = "encryption")]
#[test]
fn persistent_secret_values_stay_sealed_on_disk() {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_secrets_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.pink");
    let wal = PathBuf::from(format!("{}.wal", path.display()));
    let key = pinkdb::EncryptionKey::from_raw([0x5A; 32]);

    let mut db = PinkDb::new();
    db.execute(
        "CREATE PERSISTENT SECRET r2_lake (TYPE S3, KEY_ID 'kid', SECRET 'S3CRET', \
         ENDPOINT 'acct.r2.cloudflarestorage.com', URL_STYLE 'path', REGION 'auto');",
    )
    .unwrap();
    db.save_with_key(path.to_str().unwrap(), &key).unwrap();

    let (mut db, _) =
        PinkDb::open_durable_with_key_and_policy(path.to_str().unwrap(), SyncPolicy::Sync, &key)
            .unwrap();
    // A secret the attach password path mints: the same record section carries
    // it, and replay must find it again.
    db.execute(
        "CREATE PERSISTENT SECRET pg (TYPE POSTGRES, HOST 'h', USER 'u', PASSWORD 'PGPASS123');",
    )
    .unwrap();
    db.execute("CHECKPOINT;").unwrap();
    db.execute("CREATE PERSISTENT SECRET later (TYPE S3, SECRET 'LATER');")
        .unwrap();

    for (file, bytes) in [
        ("checkpoint", std::fs::read(&path).unwrap()),
        ("WAL", std::fs::read(&wal).unwrap()),
    ] {
        for needle in ["S3CRET", "PGPASS123", "LATER"] {
            assert!(
                !bytes.windows(needle.len()).any(|w| w == needle.as_bytes()),
                "{needle} leaked into the {file}"
            );
        }
    }

    let (mut back, _) =
        PinkDb::open_durable_with_key_and_policy(path.to_str().unwrap(), SyncPolicy::Sync, &key)
            .unwrap();
    let names = rows_of(
        &mut back,
        "SELECT name FROM duckdb_secrets() ORDER BY name;",
    );
    let names: Vec<String> = names.iter().map(|r| text(&r[0])).collect();
    assert_eq!(names, ["later", "pg", "r2_lake"]);
    let all = format!(
        "{:?}",
        rows_of(&mut back, "SELECT * FROM duckdb_secrets();")
    );
    assert!(
        !all.contains("S3CRET") && !all.contains("PGPASS123"),
        "{all}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
