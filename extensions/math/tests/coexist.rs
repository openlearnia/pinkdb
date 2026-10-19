//! Wave 78 regression: **three SDK-declared extensions coexist in one binary**.
//!
//! This test binary links two linked-only in-repo extensions
//! (`pinkdb-math` and `pinkdb-datetime`, both rlibs) plus a third extension
//! declared locally with the SDK. That is exactly the shape that failed on
//! Linux when `pinkdb_extension!` emitted the ABI v1 `#[no_mangle]` exports
//! unconditionally: both rlibs defined `pinkdb_extension_init` and
//! `pinkdb_extension_name`, and rust-lld refused with
//!
//! ```text
//! duplicate symbol: pinkdb_extension_init  (pinkdb_math and pinkdb_datetime)
//! duplicate symbol: pinkdb_extension_name  (pinkdb_math and pinkdb_datetime)
//! ```
//!
//! The macOS linker tolerates duplicate symbols, so this file is the
//! **link-level** guard: on Linux, a regression fails `cargo test -p
//! pinkdb-math` at link time (never reaching the assertions); everywhere it
//! also proves the three extensions register and answer SQL together.
//!
//! Linked-only extensions must not call `pinkdb_abi_exports!()` — only a
//! cdylib does (see the SDK README). `ext-sdk/rust/check-native-exports.sh`
//! checks the built rlibs for the symbols directly.

use pinkdb::{PinkDb, QueryResult, Value};

/// A third extension, declared exactly like a linked-only host extension.
mod fixture {
    use pinkdb_ext_sdk::pinkdb_extension;

    pinkdb_extension! {
        name = "wave78_coexist_fixture",
        functions = [
            /// A deterministic function so the coexistence check is trivial
            /// to read.
            fn sdk_coexist_value() -> i64 { 42 },
        ],
    }
}

#[test]
fn linked_only_extensions_link_register_and_answer_sql_together() {
    // Referencing both crates is what pulls their rlibs (and, before the fix,
    // their duplicate globals) into this binary. `ensure_registered` is
    // idempotent, so the calls are safe even if another test ran first.
    pinkdb_math::ensure_registered().expect("register pinkdb-math");
    pinkdb_datetime::ensure_registered().expect("register pinkdb-datetime");
    fixture::pinkdb_register_linked().expect("register the local fixture");

    // Each declaration's spec list is linked in and reachable.
    assert!(
        pinkdb_math::pinkdb_scalar_specs()
            .iter()
            .any(|spec| spec.name == "exp")
    );
    assert!(
        pinkdb_datetime::pinkdb_scalar_specs()
            .iter()
            .any(|spec| spec.name == "epoch_ms")
    );

    let mut db = PinkDb::new();
    for name in [
        pinkdb_math::EXTENSION_NAME,
        pinkdb_datetime::EXTENSION_NAME,
        "wave78_coexist_fixture",
    ] {
        assert_eq!(db.load_linked_extension(name).unwrap(), name);
    }

    // One function from each extension, in one store.
    assert_eq!(
        rows(
            &mut db,
            "SELECT exp(0), epoch_ms(TIMESTAMP '1970-01-01 00:00:01'), sdk_coexist_value();"
        ),
        vec![vec![
            Value::Double(1.0),
            Value::Bigint(1000),
            Value::Bigint(42),
        ]]
    );
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}
