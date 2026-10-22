//! DECIMAL storage in `PINKDB06` (wave 102): precision <= 18 is stored with
//! the BIGINT encodings (exact i64 unscaled values), precision > 18 keeps the
//! 16-byte path. Both must round-trip exactly, NULLs included.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn decimals_round_trip_exactly_at_the_i64_boundary_and_beyond() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE d (p18 DECIMAL(18,0), money DECIMAL(15,2), wide DECIMAL(38,6));
         INSERT INTO d VALUES
           (999999999999999999, 12345.67, 12345678901234567890123456789.123456),
           (-999999999999999999, -0.01, -12345678901234567890123456789.123456),
           (NULL, NULL, NULL),
           (0, 0.00, 0.000001);",
    )
    .unwrap();
    let before = rows(&mut db, "SELECT * FROM d;");
    let bytes = db.to_pink_bytes().unwrap();
    let mut back = PinkDb::from_pink_bytes(&bytes).unwrap();
    assert_eq!(rows(&mut back, "SELECT * FROM d;"), before);
}

#[test]
fn narrow_decimals_compress_like_integers() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE m (price DECIMAL(15,2));").unwrap();
    // 100,000 prices with a small range: a 16-byte-per-value layout would be
    // ~1.6 MB; the BIGINT encodings need a few bits per value.
    let mut sql = String::from("INSERT INTO m VALUES ");
    for i in 0..100_000u32 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}.{:02})", 900 + i % 1000, i % 100));
    }
    sql.push(';');
    db.execute(&sql).unwrap();
    let bytes = db.to_pink_bytes().unwrap();
    assert!(
        bytes.len() < 400_000,
        "DECIMAL(15,2) should not be stored as 16 raw bytes per value: {} bytes",
        bytes.len()
    );
    let mut back = PinkDb::from_pink_bytes(&bytes).unwrap();
    assert_eq!(
        rows(
            &mut back,
            "SELECT sum(price), min(price), max(price) FROM m;"
        ),
        rows(&mut db, "SELECT sum(price), min(price), max(price) FROM m;")
    );
}
