//! The live path: one call to the provider configured in the environment.
//!
//! Ignored by default, because it spends money and needs a key. Run it with
//! `PINKDB_EMBEDDINGS_LIVE=1` and a key in the environment:
//!
//! ```text
//! PINKDB_EMBEDDINGS_LIVE=1 PINKDB_EMBEDDINGS_API_KEY=... \
//!     cargo test -p pinkdb-embeddings --test live -- --ignored
//! ```

use pinkdb::{PinkDb, QueryResult, Value};

#[test]
#[ignore = "calls a real embeddings provider; needs PINKDB_EMBEDDINGS_LIVE=1 and a key"]
fn a_real_provider_answers() {
    if std::env::var("PINKDB_EMBEDDINGS_LIVE").as_deref() != Ok("1") {
        eprintln!("PINKDB_EMBEDDINGS_LIVE is not 1: not calling a provider");
        return;
    }
    let configured = [
        pinkdb_embeddings::KEY_VARIABLE,
        "PINKDB_EMBEDDINGS_BASE_URL",
        "PINKDB_EMBEDDINGS_MODEL",
    ]
    .iter()
    .all(|variable| {
        std::env::var(variable)
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    });
    if !configured {
        eprintln!("no provider, model or key in the environment: not calling a provider");
        return;
    }

    pinkdb_embeddings::ensure_registered().expect("register the package");
    let mut db = PinkDb::new();
    db.load_extension_package(pinkdb_embeddings::EXTENSION_NAME)
        .expect("load the package");

    let results = db
        .execute("SELECT embed('pinkdb'), embed('a different sentence')")
        .expect("call the provider");
    let QueryResult::Selected { rows, .. } = results.into_iter().next().unwrap() else {
        panic!("expected rows");
    };
    let vector = |cell: &Value| match cell {
        Value::Array(array) => array.floats32().expect("a FLOAT array").to_vec(),
        other => panic!("expected an array, got {other:?}"),
    };
    let first = vector(&rows[0][0]);
    let second = vector(&rows[0][1]);
    assert!(!first.is_empty());
    assert_eq!(first.len(), second.len());
    assert_ne!(first, second);
    assert!(first.iter().all(|value| value.is_finite()));
}
