//! Wave 88, plan rule 7: the shared format contract, run against all three
//! providers in one binary.
//!
//! The dev-dependency enables pink-lake's `vortexlake` feature, so the
//! built-in Vortex provider and the Parquet provider are both available here.
//! The contract body is the exact file `crates/pink-lake/tests/format_contract.rs`
//! includes, so a provider cannot pass a weaker check.

#[path = "../../pink-lake/tests/common/format_contract.rs"]
mod format_contract;

use format_contract::{ContractOptions, run_contract};
use pinkdb::DataType;

#[test]
fn pink_meets_the_format_contract() {
    run_contract("pink", ContractOptions::default());
}

#[test]
fn vortex_meets_the_format_contract() {
    // The Vortex encoder refuses DECIMAL and INTERVAL at its entry (a wave-87
    // limitation, not a lossy write), so they are listed as allowed refusals
    // and left out of the round-trip corpus.
    run_contract(
        "vortex",
        ContractOptions {
            refused_types: vec![
                DataType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                DataType::Decimal {
                    precision: 18,
                    scale: 4,
                },
                DataType::Decimal {
                    precision: 38,
                    scale: 6,
                },
                DataType::Interval,
            ],
        },
    );
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
