//! Wave 88, plan rule 7: the shared format contract, run against pink-lake's
//! built-in providers.
//!
//! The contract body lives in `tests/common/format_contract.rs` and is
//! included here through `#[path]` rather than declared in `common/mod.rs`:
//! the other test binaries (`lake.rs`, `pinklake.rs`) declare `mod common;`
//! and would otherwise compile — and warn about — this module without using
//! it. `crates/pink-lake-parquet/tests/format_contract.rs` includes the very
//! same file.
#[path = "common/local_lake.rs"]
mod local_lake;

#[path = "common/format_contract.rs"]
mod format_contract;

use format_contract::{ContractOptions, run_contract};

#[test]
fn pink_meets_the_format_contract() {
    run_contract("pink", ContractOptions::default());
}
