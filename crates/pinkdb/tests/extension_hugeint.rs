//! Binding of HUGEINT/UHUGEINT arguments to linked extension functions.
//!
//! The extension ABI has no overload set: an extension declares one function
//! per name. These tests pin the host's coercion rules instead: an exact
//! 128-bit parameter wins, a BIGINT parameter accepts a 128-bit argument only
//! when the value fits, and a DOUBLE parameter widens.

use pinkdb::{PinkDb, QueryResult, Value};
use pinkdb_ext_loader::linked::{LinkedExtension, LinkedFunction, register};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(out.len(), 1, "{sql} must be one statement");
    match out.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

fn load(db: &mut PinkDb, name: &str, functions: Vec<LinkedFunction>) {
    register(LinkedExtension::new(name, functions).unwrap()).unwrap();
    db.load_linked_extension(name).unwrap();
}

fn identity(name: &str, arg: ExtType, ret: ExtType) -> LinkedFunction {
    LinkedFunction::new(name, vec![arg], ret, |args: &[ExtArg<'_>]| {
        match args.first() {
            Some(ExtArg::Null(_)) => Ok(ExtValue::Null),
            Some(value) => Ok(match value {
                ExtArg::Bigint(v) => ExtValue::Bigint(*v),
                ExtArg::Hugeint(v) => ExtValue::Hugeint(*v),
                ExtArg::Uhugeint(v) => ExtValue::Uhugeint(*v),
                ExtArg::Double(v) => ExtValue::Double(*v),
                other => return Err(format!("unexpected argument {other:?}")),
            }),
            None => Err("missing argument".to_string()),
        }
    })
    .unwrap()
}

/// SUM(INTEGER) is a HUGEINT; a function that declares BIGINT keeps working.
#[test]
fn sum_of_integers_binds_to_a_bigint_parameter() {
    let mut db = PinkDb::new();
    load(
        &mut db,
        "wave_ext_hugeint_bigint",
        vec![identity(
            "wave_ext_sum_bigint",
            ExtType::Bigint,
            ExtType::Bigint,
        )],
    );
    db.execute("CREATE TABLE t(i INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT wave_ext_sum_bigint(SUM(i)) FROM t"),
        vec![vec![Value::Bigint(6)]]
    );
    // NULL still passes through.
    assert_eq!(
        rows(&mut db, "SELECT wave_ext_sum_bigint(NULL)"),
        vec![vec![Value::Null]]
    );
}

/// The same binding for a function that declares DOUBLE: the usual widening.
#[test]
fn sum_of_integers_widens_to_a_double_parameter() {
    let mut db = PinkDb::new();
    load(
        &mut db,
        "wave_ext_hugeint_double",
        vec![identity(
            "wave_ext_sum_double",
            ExtType::Double,
            ExtType::Double,
        )],
    );
    db.execute("CREATE TABLE t(i INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT wave_ext_sum_double(SUM(i)) FROM t"),
        vec![vec![Value::Double(6.0)]]
    );
}

/// An exact 128-bit declaration receives the value unchanged.
#[test]
fn hugeint_and_uhugeint_round_trip_through_sum() {
    let mut db = PinkDb::new();
    load(
        &mut db,
        "wave_ext_hugeint_exact",
        vec![
            identity("wave_ext_hugeint_id", ExtType::Hugeint, ExtType::Hugeint),
            identity("wave_ext_uhugeint_id", ExtType::Uhugeint, ExtType::Uhugeint),
        ],
    );
    db.execute("CREATE TABLE t(i INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT wave_ext_hugeint_id(SUM(i)) FROM t"),
        vec![vec![Value::Hugeint(6)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT wave_ext_uhugeint_id(CAST(18446744073709551615 AS UHUGEINT))"
        ),
        vec![vec![Value::Uhugeint(u64::MAX as u128)]]
    );
}

/// Narrowing to BIGINT is checked: a value outside its range is a clear error
/// naming the function and the parameter, never a wrap.
#[test]
fn out_of_range_narrowing_names_function_and_argument() {
    let mut db = PinkDb::new();
    load(
        &mut db,
        "wave_ext_hugeint_narrow",
        vec![identity(
            "wave_ext_narrow",
            ExtType::Bigint,
            ExtType::Bigint,
        )],
    );
    db.execute("CREATE TABLE t(i BIGINT)").unwrap();
    db.execute("INSERT INTO t VALUES (9223372036854775807), (1)")
        .unwrap();
    let err = db
        .execute("SELECT wave_ext_narrow(SUM(i)) FROM t")
        .unwrap_err()
        .to_string();
    assert!(err.contains("wave_ext_narrow"), "{err}");
    assert!(err.contains("argument 1"), "{err}");
    assert!(err.contains("BIGINT"), "{err}");
    assert!(err.contains("9223372036854775808"), "{err}");

    let err = db
        .execute("SELECT wave_ext_narrow(CAST(18446744073709551615 AS UHUGEINT))")
        .unwrap_err()
        .to_string();
    assert!(err.contains("wave_ext_narrow"), "{err}");
    assert!(err.contains("argument 1"), "{err}");
    assert!(err.contains("BIGINT"), "{err}");

    // In range, the same cast narrows successfully.
    assert_eq!(
        rows(&mut db, "SELECT wave_ext_narrow(CAST(5 AS HUGEINT))"),
        vec![vec![Value::Bigint(5)]]
    );
}
