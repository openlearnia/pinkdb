//! Shared fixtures for the pink-lake integration tests.
//!
//! `fidelity_store` is the store behind the checked-in v1 lake fixture
//! (`tests/fixtures/lake_v1/`) and the v2 round-trip tests: every type,
//! NULLs, empty strings, embedded NULs, multibyte strings, `-0.0`, NaN
//! payloads (positive and negative, signaling and quiet), i32/i64/DATE/
//! TIMESTAMP extremes, a multi-chunk table, an empty table, multiple
//! databases, and dotted + unicode table names.

use pinkdb::{DataType, PinkDb, Value, CHUNK_SIZE};

/// The fidelity store described above.
pub fn fidelity_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE main.fid (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, \
         dt DATE, ts TIMESTAMP);",
    )
    .unwrap();
    {
        let t = db.table_mut("fid").unwrap();
        // Extremes, empty string, embedded NUL, multibyte.
        t.append_row(vec![
            Value::Boolean(true),
            Value::Integer(i32::MIN),
            Value::Bigint(i64::MIN),
            Value::Double(-0.0),
            Value::Varchar(String::new()),
            Value::Date(i32::MIN),
            Value::Timestamp(i64::MIN),
        ])
        .unwrap();
        t.append_row(vec![
            Value::Boolean(false),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MAX),
            Value::Double(f64::from_bits(0x7ff8_0000_dead_beef)),
            Value::Varchar("héllo wörld 🌍\0embedded".to_string()),
            Value::Date(i32::MAX),
            Value::Timestamp(i64::MAX),
        ])
        .unwrap();
        // All NULLs.
        t.append_row(vec![Value::Null; 7]).unwrap();
        // Additive zeros.
        t.append_row(vec![
            Value::Boolean(false),
            Value::Integer(0),
            Value::Bigint(0),
            Value::Double(0.0),
            Value::Varchar(String::new()),
            Value::Date(0),
            Value::Timestamp(0),
        ])
        .unwrap();
        // Negative NaN payload, signaling NaN, infinity, narrow table names.
        t.append_row(vec![
            Value::Boolean(true),
            Value::Integer(-1),
            Value::Bigint(-1),
            Value::Double(f64::from_bits(0xfff8_0000_0000_0001)),
            Value::Varchar(".. %2E / \\ weird".to_string()),
            Value::Date(19_000),
            Value::Timestamp(1_700_000_000_000_000),
        ])
        .unwrap();
        t.append_row(vec![
            Value::Boolean(false),
            Value::Integer(1),
            Value::Bigint(1),
            Value::Double(f64::INFINITY),
            Value::Varchar("".to_string()),
            Value::Date(-1),
            Value::Timestamp(-1),
        ])
        .unwrap();
    }

    // Multi-chunk table (CHUNK_SIZE + 100 rows).
    db.execute("CREATE TABLE big (a INTEGER, s VARCHAR);")
        .unwrap();
    {
        let t = db.table_mut("big").unwrap();
        for i in 0..(CHUNK_SIZE + 100) {
            t.append_row(vec![
                Value::Integer((i % 7) as i32 - 3),
                Value::Varchar(format!("region{}", i % 3)),
            ])
            .unwrap();
        }
    }

    // Empty table: schema must survive.
    db.execute("CREATE TABLE empty (x INTEGER, y VARCHAR);")
        .unwrap();

    // Second database with a dotted table name.
    db.execute("CREATE DATABASE analytics;").unwrap();
    db.execute("CREATE TABLE analytics.u (k BIGINT);").unwrap();
    db.execute("INSERT INTO analytics.u VALUES (7);").unwrap();
    db.execute("CREATE TABLE analytics.\"a.b\" (z DOUBLE);")
        .unwrap();
    db.execute("INSERT INTO analytics.\"a.b\" VALUES (1.5), (-0.0);")
        .unwrap();

    // Unicode table name (created through the API: the parser is ASCII).
    db.create_table_in("main", "héllo✓", vec![("s".to_string(), DataType::Varchar)])
        .unwrap();
    db.table_mut_in("main", "héllo✓")
        .unwrap()
        .append_row(vec![Value::Varchar("π≈3.14".to_string())])
        .unwrap();
    db
}
