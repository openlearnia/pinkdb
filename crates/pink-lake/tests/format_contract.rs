//! Wave 88, plan rule 7: the shared format contract, run against pink-lake's
//! built-in providers.
//!
//! The contract body lives in `tests/common/format_contract.rs` and is
//! included here through `#[path]` rather than declared in `common/mod.rs`:
//! the other test binaries (`lake.rs`, `pinklake.rs`) declare `mod common;`
//! and would otherwise compile — and warn about — this module without using
//! it. `crates/pink-lake-parquet/tests/format_contract.rs` includes the very
//! same file.

#[path = "common/format_contract.rs"]
mod format_contract;

use format_contract::{ContractOptions, run_contract};
use pinkdb::DataType;

#[test]
fn pink_meets_the_format_contract() {
    run_contract("pink", ContractOptions::default());
}

#[cfg(feature = "vortexlake")]
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
