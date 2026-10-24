//! Wave 88, plan rule 7: the shared format contract, run against the built-in
//! `.pink` provider and the Parquet provider in one binary.
//!
//! The contract body is the exact file
//! `crates/pink-lake/tests/format_contract.rs` includes, so the Parquet
//! provider cannot pass a weaker check.

#[path = "../../pink-lake/tests/common/format_contract.rs"]
mod format_contract;

use format_contract::{ContractOptions, run_contract};
use pinkdb::DataType;

#[test]
fn pink_meets_the_format_contract() {
    run_contract("pink", ContractOptions::default());
}

#[test]
fn parquet_meets_the_format_contract() {
    pink_lake_parquet::register().unwrap();
    // Parquet has no INTERVAL logical type in the writer
    // (`pinkdb_parquet::write` refuses it), so INTERVAL is the one allowed
    // refusal.
    run_contract(
        "parquet",
        ContractOptions {
            refused_types: vec![DataType::Interval],
        },
    );
}
