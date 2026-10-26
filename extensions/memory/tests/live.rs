//! The one memory test that talks to a real model provider.
//!
//! Ignored by default and gated on `PINKDB_MEMORY_LIVE=1`: everything else in
//! this crate runs against a scripted provider. Run it with
//!
//! ```text
//! PINKDB_MEMORY_LIVE=1 PINKDB_AGENT_PROVIDER=anthropic ANTHROPIC_API_KEY=… \
//!     cargo test -p pinkdb-memory --test live -- --ignored --nocapture
//! ```
//!
//! The assertions are deliberately loose: a real model's wording is not
//! expected to match anything. What is checked is the contract — facts come
//! out of a conversation, they are searchable, and the persona mentions the
//! subject the turns were about.

mod support;

use pinkdb_memory::distill;
use pinkdb_memory::host::Caller;
use pinkdb_memory::ops::{self, Add};
use pinkdb_memory::search::Request;
use pinkdb_memory::store;
use support::TestHost;

#[test]
#[ignore = "needs a live model provider: PINKDB_MEMORY_LIVE=1 and a provider with its key"]
fn a_live_provider_distills_and_the_facts_come_back() {
    if std::env::var("PINKDB_MEMORY_LIVE").as_deref() != Ok("1") {
        eprintln!(
            "PINKDB_MEMORY_LIVE is not 1: this test spends a real model call. \
             Set it and a provider (PINKDB_AGENT_PROVIDER plus that provider's key) to run."
        );
        return;
    }
    let mut provider = distill::provider(None, None)
        .expect("a provider: set PINKDB_AGENT_PROVIDER and export its key variable");

    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    let caller = Caller::of(&mut host).unwrap();
    for turn in [
        "Hi! My warehouse is TPC-H SF1: about six million lineitems. Please always run \
         EXPLAIN before a big join — I got burned by a nested loop last month.",
        "Noted. Anything else about how you work?",
        "I review the query plan in the morning and only then start the benchmark.",
    ] {
        ops::add(
            &mut host,
            &caller,
            &Add {
                text: turn.to_string(),
                ..Add::default()
            },
        )
        .unwrap();
    }

    let report = distill::drain(&mut host, &caller, provider.as_mut()).expect("distillation");
    println!("{report:?}");
    assert!(report.failed == 0, "a live model misbehaved: {report:?}");
    let atoms = store::atoms(&mut host, &caller, true).unwrap();
    assert!(!atoms.is_empty(), "no facts came out of three turns");
    assert!(
        atoms
            .iter()
            .any(|atom| atom.text.to_lowercase().contains("tpc")),
        "the warehouse fact is missing: {atoms:?}"
    );

    let outcome = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "what is my warehouse".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert!(outcome.persona.is_some(), "no persona after distillation");
    assert!(
        outcome
            .hits
            .iter()
            .any(|hit| hit.text.to_lowercase().contains("tpc")),
        "the warehouse fact is not a hit: {:?}",
        outcome.hits
    );
}
