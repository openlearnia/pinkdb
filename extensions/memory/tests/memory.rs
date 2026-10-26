//! Agent memory end to end: distillation with a scripted model, the ACLs, the
//! retrieval budgets, supersession and forget, the bridge's write switch, and
//! the key sentinel.
//!
//! Every test runs against a local store and a scripted provider; the one
//! test that talks to a network talks to a loopback embeddings listener this
//! process owns.

mod support;

use std::time::Duration;

use pinkdb::serve::ServePolicy;
use pinkdb_ext_loader::serve::StatementHost;
use pinkdb_memory::command;
use pinkdb_memory::distill::{self, MAX_ATTEMPTS};
use pinkdb_memory::host::Caller;
use pinkdb_memory::ops::{self, Add};
use pinkdb_memory::search::{Budget, Mode, Request, Source};
use pinkdb_memory::store::{self, Acl, Draft};
use support::{Mock, TestHost};

/// The three answers one full distillation needs: facts, a scenario over them,
/// a persona. `$IDS` is replaced with the atom ids from the prompt, so the
/// scenario cites the atoms the previous step actually wrote.
const L0_ANSWER: &str = r#"{"atoms":[
  {"text":"the user's warehouse is TPC-H SF1","confidence":0.9,"tag":"tpch","supersedes":""},
  {"text":"the user prefers EXPLAIN before running big joins","confidence":0.8,"tag":"style","supersedes":""}
]}"#;
const L2_ANSWER: &str = r#"{"scenarios":[{"title":"TPC-H benchmarking","summary":"The user benchmarks on TPC-H SF1 and explains a query before running it.","atoms":[$IDS],"tag":"tpch"}]}"#;
const L3_ANSWER: &str =
    r#"{"persona":"Data engineer on TPC-H SF1; prefers EXPLAIN before big joins."}"#;

/// One new fact, for the turn added after a batch already failed: the retry
/// must not re-summarize facts a scenario already covers.
const L0_RETRY: &str = r#"{"atoms":[{"text":"the user runs on a laptop","confidence":0.8,"tag":"env","supersedes":""}]}"#;
const L2_RETRY: &str = r#"{"scenarios":[{"title":"Work setup","summary":"The user runs on a laptop.","atoms":[$IDS],"tag":"env"}]}"#;

fn add(host: &mut TestHost, text: &str) -> String {
    let caller = who(host);
    ops::add(
        host,
        &caller,
        &Add {
            text: text.to_string(),
            ..Add::default()
        },
    )
    .unwrap()
}

fn who(host: &mut TestHost) -> Caller {
    Caller::of(host).unwrap()
}

fn atom(host: &mut TestHost, visibility: &str, acl: &str, text: &str) -> String {
    let caller = who(host);
    store::ensure_schema(host).unwrap();
    store::add_atom(
        host,
        &caller,
        &Draft {
            text: text.to_string(),
            tag: "test".to_string(),
            confidence: 0.8,
            visibility: visibility.to_string(),
            acl: Acl::parse(acl).unwrap(),
            supersedes: String::new(),
        },
        "",
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Distillation and retrieval
// ---------------------------------------------------------------------------

#[test]
fn a_conversation_becomes_atoms_scenarios_and_a_persona() {
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    add(
        &mut host,
        "my warehouse is TPC-H SF1, about 6 million lineitems",
    );
    add(&mut host, "please EXPLAIN before you run a big join");

    let mut mock = Mock::json(&[L0_ANSWER, L2_ANSWER, L3_ANSWER]);
    let caller = who(&mut host);
    let report = distill::drain(&mut host, &caller, &mut mock).unwrap();
    assert_eq!(report.failed, 0, "{:?}", report.steps);
    assert_eq!(report.atoms, 2, "{:?}", report.steps);
    assert_eq!(report.scenarios, 1, "{:?}", report.steps);
    assert!(report.persona, "{:?}", report.steps);
    assert_eq!(mock.calls(), 3);

    let caller = who(&mut host);
    let atoms = store::atoms(&mut host, &caller, true).unwrap();
    assert_eq!(atoms.len(), 2);
    assert!(atoms.iter().all(|atom| atom.status == "active"));
    assert!(atoms.iter().any(|atom| atom.text.contains("TPC-H SF1")));
    // The distiller read two turns and marked them, so a second drain is a
    // no-op rather than a duplicate.
    let turns = store::undistilled_turns(&mut host, &caller, 10).unwrap();
    assert!(turns.is_empty());

    let scenarios = store::scenarios(&mut host, &caller).unwrap();
    assert_eq!(scenarios.len(), 1);
    assert_eq!(scenarios[0].title, "TPC-H benchmarking");
    assert_eq!(scenarios[0].atom_ids.len(), 2);

    let persona = store::persona(&mut host, &caller).unwrap().unwrap();
    assert!(persona.contains("TPC-H SF1"), "{persona}");

    // Retrieval: the bootstrap first, then the facts that match.
    let outcome = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "how big is the warehouse".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert_eq!(outcome.persona.as_deref(), Some(persona.as_str()));
    assert_eq!(outcome.scenarios.len(), 1);
    assert_eq!(outcome.mode, Some(Mode::Bm25));
    assert!(outcome.note.contains("bm25 only"), "{}", outcome.note);
    let hit = outcome
        .hits
        .iter()
        .find(|hit| hit.text.contains("TPC-H SF1"))
        .expect("the warehouse fact is a hit");
    assert_eq!(hit.layer, "L1");
    assert_eq!(hit.source, Source::Bm25);
    assert!(hit.score > 0.0);
    assert!(!hit.id.is_empty());
    // Every hit came through the visibility filter, so it carries its class.
    assert_eq!(hit.visibility, "private");
}

#[test]
fn the_item_character_and_time_budgets_each_stop_retrieval() {
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    for index in 0..4 {
        atom(
            &mut host,
            "private",
            "",
            &format!("the warehouse fact number {index}"),
        );
    }
    let caller = who(&mut host);

    let items = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "warehouse".to_string(),
            max_items: 1,
            ..Request::default()
        },
    )
    .unwrap();
    assert_eq!(items.hits.len(), 1);
    assert_eq!(items.stopped, Some(Budget::Items));

    let chars = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "warehouse".to_string(),
            max_chars: 10,
            ..Request::default()
        },
    )
    .unwrap();
    assert!(chars.hits.is_empty(), "{:?}", chars.hits);
    assert_eq!(chars.stopped, Some(Budget::Characters));

    let time = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "warehouse".to_string(),
            timeout: Duration::ZERO,
            ..Request::default()
        },
    )
    .unwrap();
    assert!(time.hits.is_empty());
    assert_eq!(time.stopped, Some(Budget::Timeout));
}

#[test]
fn hybrid_retrieval_fuses_bm25_with_embeddings() {
    let _lock = support::env_lock();
    let mock = support::Embeddings::start();
    let _env = support::Env::embeddings(&mock.base_url());
    // The registry is process-wide: a second test registering the same package
    // is a no-op rather than a failure.
    let _ = pinkdb_embeddings::ensure_registered();

    let store = support::Store::memory();
    store.load(pinkdb_embeddings::EXTENSION_NAME);
    let mut host = TestHost::open(&store, Some("alice"));
    atom(&mut host, "private", "", "the warehouse is TPC-H SF1");
    atom(&mut host, "private", "", "the user prefers EXPLAIN");
    let caller = who(&mut host);

    let outcome = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "how big is the warehouse".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert_eq!(outcome.mode, Some(Mode::Hybrid), "{}", outcome.note);
    // The note names the vector source so a caller can tell the two modes
    // apart from the result alone.
    assert!(outcome.note.contains("4-dimension"), "{}", outcome.note);
    assert!(mock.count() > 0, "the query was embedded");

    // Hand-computed RRF: the warehouse atom is first in both lists, so it
    // scores 1/(60+0) twice; the other is second in both, 1/(60+1) twice.
    let warehouse = outcome
        .hits
        .iter()
        .find(|hit| hit.text.contains("warehouse"))
        .unwrap();
    assert_eq!(warehouse.source, Source::Both);
    assert!((warehouse.score - (1.0 / 60.0 + 1.0 / 60.0)).abs() < 1e-12);
    let explain = outcome
        .hits
        .iter()
        .find(|hit| hit.text.contains("EXPLAIN"))
        .unwrap();
    assert!((explain.score - (1.0 / 61.0 + 1.0 / 61.0)).abs() < 1e-12);
}

// ---------------------------------------------------------------------------
// Identity and visibility
// ---------------------------------------------------------------------------

#[test]
fn every_visibility_class_is_visible_to_exactly_the_right_principals() {
    let store = support::Store::memory();
    support::users(&store);
    store.sql("CREATE USER atlas PASSWORD 'memory-test-secret'");
    store.sql("CREATE USER carol PASSWORD 'memory-test-secret'");
    store.sql("GRANT t1 TO carol");

    let mut alice = TestHost::open(&store, Some("alice"));
    atom(&mut alice, "private", "", "alice's private fact");
    atom(&mut alice, "team", "", "the team's fact");
    atom(&mut alice, "restricted", "u:bob,r:t2", "bob may read this");
    let agent = atom(&mut alice, "agent", "", "atlas may read this");
    store::ensure_schema(&mut alice).unwrap();
    // `agent` rows are addressed by the agent's name, which the store keeps in
    // the agent column: point the row at atlas.
    alice.raw_internal(&format!(
        "UPDATE pinkdb_memory.atoms SET agent = 'atlas' WHERE id = {}",
        store::text(&agent)
    ));

    let texts = |host: &mut TestHost| -> Vec<String> {
        let caller = who(host);
        store::atoms(host, &caller, true)
            .unwrap()
            .into_iter()
            .map(|atom| atom.text)
            .collect()
    };

    // The owner sees everything she wrote.
    let mine = texts(&mut alice);
    assert_eq!(mine.len(), 4, "{mine:?}");

    // Bob is in t2: the row he is named on, and nothing else. The team fact
    // belongs to alice's t1, so a card-carrying member of t2 cannot read it.
    let mut bob = TestHost::open(&store, Some("bob"));
    let theirs = texts(&mut bob);
    assert_eq!(theirs, vec!["bob may read this".to_string()], "{theirs:?}");

    // Carol is in t1, so the team's fact is hers, and the ACL row is not.
    let mut carol = TestHost::open(&store, Some("carol"));
    let hers = texts(&mut carol);
    assert_eq!(hers, vec!["the team's fact".to_string()], "{hers:?}");

    // The agent identity reads only its own row, and cannot string-match its
    // way into the ACL: the fourth row names bob, not atlas.
    let mut atlas = TestHost::open(&store, Some("atlas"));
    let named = texts(&mut atlas);
    assert_eq!(named, vec!["atlas may read this".to_string()], "{named:?}");

    // ... and the same filter applies to search, not just to the table scan.
    let caller = who(&mut bob);
    let outcome = ops::search(
        &mut bob,
        &caller,
        &Request {
            query: "fact".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert!(!outcome.hits.iter().any(|hit| hit.text.contains("private")));
    assert!(
        !outcome
            .hits
            .iter()
            .any(|hit| hit.text.contains("team's fact"))
    );
}

#[test]
fn the_tables_are_not_readable_outside_the_extension() {
    let store = support::Store::memory();
    support::users(&store);
    let mut alice = TestHost::open(&store, Some("alice"));
    atom(&mut alice, "private", "", "alice's private fact");

    // A non-admin with no grant: the engine refuses before any row is read.
    // A plain session (no bridge policy in the way) answers with the grant
    // hint, so the tables really are ungranted and not just masked.
    let mut plain = store.connect();
    plain.login("bob", support::PASSWORD).unwrap();
    plain.require_login(true);
    let policy = ServePolicy::default();
    let mut plain = pinkdb::serve::SessionHost::new(&mut plain, &policy);
    let (code, refused) = plain.execute_statement("SELECT text FROM pinkdb_memory.atoms");
    assert_ne!(code, 0, "{refused}");
    assert!(refused.contains("permission denied"), "{refused}");
    assert!(
        refused.contains("GRANT SELECT ON TABLE pinkdb_memory.atoms"),
        "{refused}"
    );

    // Through the bridge the mask answers first, naming the tools instead.
    let mut bob = TestHost::open(&store, Some("bob"));
    let refused = bob.refused("SELECT text FROM pinkdb_memory.atoms");
    assert!(refused.contains("pinkdb_memory"), "{refused}");

    // Even asking the memory tools' own channel for another caller's rows
    // stays inside the filter: the internal channel is trusted, the store's
    // visibility is not.
    let caller = Caller::of(&mut bob).unwrap();
    let visible = store::atoms(&mut bob, &caller, true).unwrap();
    assert!(visible.is_empty());

    // An administrator may read the tables directly; that is documented.
    let trusted = store.sql("SELECT count(*) FROM pinkdb_memory.atoms");
    assert_eq!(trusted.len(), 1);
}

// ---------------------------------------------------------------------------
// Supersession, forget and the audit trail
// ---------------------------------------------------------------------------

#[test]
fn a_contradicting_fact_supersedes_and_forget_keeps_the_row() {
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    let old = atom(&mut host, "private", "", "the warehouse is TPC-H SF1");
    let caller = who(&mut host);

    let new = ops::update(&mut host, &caller, &old, "the warehouse is TPC-DS now").unwrap();
    assert_ne!(new, old);

    let superseded = store::atom(&mut host, &caller, &old).unwrap().unwrap();
    assert_eq!(superseded.status, "superseded");
    assert_eq!(superseded.superseded_by, new);
    let active = store::atoms(&mut host, &caller, true).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, new);

    // Search shows the correction, not the old text.
    let outcome = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "warehouse".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert!(outcome.hits.iter().any(|hit| hit.text.contains("TPC-DS")));
    assert!(!outcome.hits.iter().any(|hit| hit.text.contains("TPC-H")));

    // Forget hides it, and the row and its audit entries stay.
    ops::forget(&mut host, &caller, &new).unwrap();
    let forgotten = store::atom(&mut host, &caller, &new).unwrap().unwrap();
    assert_eq!(forgotten.status, "forgotten");
    let outcome = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "warehouse".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert!(outcome.hits.is_empty(), "{:?}", outcome.hits);

    let audit = store::audits(&mut host, &old).unwrap();
    assert!(
        audit.iter().any(|(_, _, action, _)| action == "supersede"),
        "{audit:?}"
    );
    let audit = store::audits(&mut host, &new).unwrap();
    assert!(
        audit.iter().any(|(_, _, action, _)| action == "forget"),
        "{audit:?}"
    );
    // Forgetting twice is an error, not a silent success.
    assert!(ops::forget(&mut host, &caller, &new).is_err());
}

// ---------------------------------------------------------------------------
// Distillation robustness
// ---------------------------------------------------------------------------

#[test]
fn a_malformed_answer_changes_nothing_and_is_retried_later() {
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    add(&mut host, "my warehouse is TPC-H SF1");

    // The first answer is prose, not the contract: it must be recorded and
    // retried within the same drain, and it must not half-apply.
    let mut mock = Mock::json(&[
        "I think the user likes databases.",
        L0_ANSWER,
        L2_ANSWER,
        L3_ANSWER,
    ]);
    let caller = who(&mut host);
    let report = distill::drain(&mut host, &caller, &mut mock).unwrap();
    assert_eq!(report.failed, 1, "{:?}", report.steps);
    assert!(
        report.steps.iter().any(|step| step.contains("JSON")),
        "{:?}",
        report.steps
    );
    assert_eq!(mock.calls(), 4);
    assert_eq!(store::atoms(&mut host, &caller, true).unwrap().len(), 2);
    assert_eq!(store::scenarios(&mut host, &caller).unwrap().len(), 1);
    // Nothing was left undistilled by the failed attempt, and the queue kept
    // the failure for an operator to read.
    assert!(
        store::undistilled_turns(&mut host, &caller, 10)
            .unwrap()
            .is_empty()
    );
    let row = host
        .rows("SELECT attempts, last_error FROM pinkdb_memory.distill_queue WHERE kind = 'turn'");
    assert_eq!(pinkdb_memory::host::cell_u64(&row[0], 0), 1);
    assert!(
        pinkdb_memory::host::cell_str(&row[0], 1).contains("JSON"),
        "{}",
        pinkdb_memory::host::cell_str(&row[0], 1)
    );
}

#[test]
fn a_provider_error_is_recorded_and_the_job_stops_after_max_attempts() {
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    add(&mut host, "my warehouse is TPC-H SF1");
    let caller = who(&mut host);

    // A 429 is not malformed output: it is recorded, then retried, so the
    // second answer applies in the same drain.
    let mut mock = Mock::new(vec![
        Err("provider answered 429 Too Many Requests".to_string()),
        Ok(L0_ANSWER.to_string()),
        Ok(L2_ANSWER.to_string()),
        Ok(L3_ANSWER.to_string()),
    ]);
    let report = distill::drain(&mut host, &caller, &mut mock).unwrap();
    assert_eq!(report.failed, 1, "{:?}", report.steps);
    assert_eq!(store::atoms(&mut host, &caller, true).unwrap().len(), 2);
    let row = host
        .rows("SELECT attempts, last_error FROM pinkdb_memory.distill_queue WHERE kind = 'turn'");
    assert_eq!(pinkdb_memory::host::cell_u64(&row[0], 0), 1);
    assert!(
        pinkdb_memory::host::cell_str(&row[0], 1).contains("429"),
        "{}",
        pinkdb_memory::host::cell_str(&row[0], 1)
    );

    // A batch that only answers 429 stops after MAX_ATTEMPTS instead of
    // spending model calls forever.
    let mut host = TestHost::open(&store, Some("alice"));
    add(&mut host, "another turn that will not distill");
    let caller = who(&mut host);
    let mut mock = Mock::new(vec![
        Err("provider answered 429 Too Many Requests".to_string()),
        Err("provider answered 429 Too Many Requests".to_string()),
        Err("provider answered 429 Too Many Requests".to_string()),
    ]);
    let report = distill::drain(&mut host, &caller, &mut mock).unwrap();
    assert_eq!(report.failed, MAX_ATTEMPTS as usize, "{:?}", report.steps);
    let queued =
        host.rows("SELECT state, attempts FROM pinkdb_memory.distill_queue WHERE state = 'failed'");
    assert_eq!(queued.len(), 1, "{queued:?}");
    assert_eq!(
        pinkdb_memory::host::cell_u64(&queued[0], 1),
        MAX_ATTEMPTS as u64
    );

    // Draining again is an operator's decision, and it retries from zero. All
    // the atoms of the first turn are already grouped, so this batch is the one
    // new fact and nothing else: the scenario step has nothing to summarize and
    // makes no call.
    let mut mock = Mock::json(&[L0_RETRY, L2_RETRY, L3_ANSWER]);
    let report = distill::drain(&mut host, &caller, &mut mock).unwrap();
    assert_eq!(report.failed, 0, "{:?}", report.steps);
    assert_eq!(store::atoms(&mut host, &caller, true).unwrap().len(), 3);
}

#[test]
fn without_a_provider_distill_is_one_clear_error_and_the_rest_works() {
    let _lock = support::env_lock();
    let mut env = support::Env::embeddings("http://127.0.0.1:9/embeddings");
    for name in [
        "PINKDB_AGENT_PROVIDER",
        "PINKDB_AGENT_MODEL",
        "PINKDB_AGENT_API_KEY",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
    ] {
        env.unset(name);
    }

    let message = distill::provider(None, None).err().expect("no provider");
    assert!(
        message.contains("PINKDB_AGENT_PROVIDER") || message.contains("provider"),
        "{message}"
    );

    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    add(&mut host, "my warehouse is TPC-H SF1");
    let caller = who(&mut host);
    atom(&mut host, "private", "", "the warehouse is TPC-H SF1");
    let outcome = ops::search(
        &mut host,
        &caller,
        &Request {
            query: "warehouse".to_string(),
            ..Request::default()
        },
    )
    .unwrap();
    assert!(!outcome.hits.is_empty());
}

#[test]
fn distill_every_n_turns_queues_without_calling_a_model() {
    let _lock = support::env_lock();
    let mut env = support::Env::default();
    let _env = env.set("PINKDB_MEMORY_DISTILL_EVERY", "2");
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    add(&mut host, "first turn");
    let caller = who(&mut host);
    assert!(store::pending(&mut host, &caller).unwrap().is_empty());
    add(&mut host, "second turn");
    let jobs = store::pending(&mut host, &caller).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].kind, "turn");
}

// ---------------------------------------------------------------------------
// The bridge: the write switch and the read-only tools
// ---------------------------------------------------------------------------

#[test]
fn the_memory_write_tools_need_the_switch_and_cannot_write_outside_memory() {
    use pinkdb_mcp_protocol::ToolSet;

    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));
    let text = serde_json::json!({"text": "the warehouse is TPC-H SF1"});
    let arguments = text.as_object().unwrap().clone();

    let mut read_only = pinkdb_memory::Memory {
        allow_memory: false,
    };
    let schemas = read_only.schemas();
    let names: Vec<&str> = schemas
        .iter()
        .filter_map(|schema| schema["name"].as_str())
        .collect();
    assert!(names.contains(&"memory_search"));
    assert!(names.contains(&"memory_persona"));
    assert!(!names.contains(&"memory_add"));
    let refused = read_only
        .call(&mut host, "memory_add", &arguments)
        .expect_err("the write tool is not allowed");
    assert!(refused.contains("allow-memory"), "{refused}");

    let mut writable = pinkdb_memory::Memory { allow_memory: true };
    let reply = writable.call(&mut host, "memory_add", &arguments).unwrap();
    assert_eq!(reply.code, 0, "{}", reply.text);
    assert!(reply.text.starts_with("added turn t"), "{}", reply.text);
    let search = serde_json::json!({"query": "warehouse"});
    let reply = writable
        .call(&mut host, "memory_search", search.as_object().unwrap())
        .unwrap();
    assert!(reply.text.contains("TPC-H SF1"), "{}", reply.text);

    // A write tool is a memory write, not a hole in the policy: the public
    // channel still cannot touch the memory tables, and a warehouse query
    // cannot either.
    let refused = host.refused("INSERT INTO pinkdb_memory.atoms (id) VALUES ('x')");
    assert!(
        refused.contains("does not read pinkdb_memory") || refused.contains("write"),
        "{refused}"
    );
}

// ---------------------------------------------------------------------------
// The command surface
// ---------------------------------------------------------------------------

#[test]
fn the_command_surface_adds_searches_and_reports() {
    let store = support::Store::memory();
    let mut host = TestHost::open(&store, Some("alice"));

    assert!(command::dispatch(&mut host, "").is_ok());
    assert!(host.out.last().unwrap().contains(".memory add"));

    assert!(command::dispatch(&mut host, "add the warehouse is TPC-H SF1").is_ok());
    assert!(host.out.last().unwrap().starts_with("added turn t"));

    assert!(command::dispatch(&mut host, "search --items 3 the warehouse").is_ok());
    let answer = host.out.last().unwrap().clone();
    assert!(answer.contains("mode: bm25"), "{answer}");

    assert!(command::dispatch(&mut host, "show").is_ok());
    assert!(host.out.last().unwrap().contains("turns: 1"));

    assert!(command::dispatch(&mut host, "persona").is_ok());
    assert!(host.out.last().unwrap().contains("no persona yet"));

    let unknown = command::dispatch(&mut host, "frobnicate").unwrap_err();
    assert!(unknown.contains("unknown memory verb"), "{unknown}");
}

// ---------------------------------------------------------------------------
// The key sentinel
// ---------------------------------------------------------------------------

#[test]
fn the_embeddings_key_never_reaches_the_store_or_its_files() {
    /// The key the mock provider is given; a store or a log that captured it
    /// is a leak this constant makes greppable.
    const SENTINEL: &str = "sk-memory-sentinel-9c31d7";

    let _lock = support::env_lock();
    let mock = support::Embeddings::start();
    let mut env = support::Env::embeddings(&mock.base_url());
    env.set("PINKDB_EMBEDDINGS_API_KEY", SENTINEL);
    let _ = pinkdb_embeddings::ensure_registered();

    let durable = support::durable("sentinel");
    durable.store.load(pinkdb_embeddings::EXTENSION_NAME);
    {
        let mut host = TestHost::open(&durable.store, Some("alice"));
        atom(&mut host, "private", "", "the warehouse is TPC-H SF1");
        let caller = who(&mut host);
        let outcome = ops::search(
            &mut host,
            &caller,
            &Request {
                query: "warehouse".to_string(),
                ..Request::default()
            },
        )
        .unwrap();
        assert_eq!(outcome.mode, Some(Mode::Hybrid));
    }
    assert!(mock.count() > 0);

    let tables = durable.store.sql("SELECT text FROM pinkdb_memory.atoms");
    let rendered = format!("{tables:?}");
    assert!(!rendered.contains(SENTINEL), "{rendered}");

    let mut files = 0;
    for entry in std::fs::read_dir(&durable.dir).unwrap() {
        let entry = entry.unwrap();
        files += 1;
        let bytes = std::fs::read(entry.path()).unwrap();
        assert!(
            !bytes
                .windows(SENTINEL.len())
                .any(|window| window == SENTINEL.as_bytes()),
            "{} holds the key",
            entry.path().display()
        );
    }
    assert!(files > 0, "the store wrote nothing to scan");
}
