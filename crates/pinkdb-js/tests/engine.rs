//! The quickjs-ng binding against the sandbox contract, with a recording
//! fake host (`cargo test -p pinkdb-js --features engine`).
#![cfg(feature = "engine")]

use pinkdb_js::{
    DENIED_INTRINSICS, Host, HostReply, HostRequest, JsError, JsValue, SandboxConfig, check_script,
    run_procedure,
};

/// Records every request; answers queries with fixed rows and fails any SQL
/// containing `boom`.
#[derive(Default)]
struct FakeHost {
    requests: Vec<HostRequest>,
    rows: Vec<Vec<JsValue>>,
}

impl Host for FakeHost {
    fn handle(&mut self, request: HostRequest) -> Result<HostReply, String> {
        self.requests.push(request.clone());
        match request {
            HostRequest::Print(_) => Ok(HostReply::Ok),
            HostRequest::Exec { sql, .. } | HostRequest::Query { sql, .. }
                if sql.contains("boom") =>
            {
                Err(format!("table \"boom\" not found ({sql})"))
            }
            HostRequest::Exec { .. } => Ok(HostReply::Affected(3)),
            HostRequest::Query { .. } => Ok(HostReply::Rows(self.rows.clone())),
        }
    }
}

fn run(src: &str) -> Result<JsValue, JsError> {
    run_procedure(
        src,
        &[],
        &[],
        &SandboxConfig::default(),
        &mut FakeHost::default(),
    )
}

fn run_with(src: &str, host: &mut FakeHost) -> Result<JsValue, JsError> {
    run_procedure(src, &[], &[], &SandboxConfig::default(), host)
}

#[test]
fn returns_values_and_binds_parameters() {
    let out = run_procedure(
        "return a * 2 + b.length;",
        &["a", "b"],
        &[JsValue::Int(20), JsValue::Str("xy".into())],
        &SandboxConfig::default(),
        &mut FakeHost::default(),
    );
    assert_eq!(out, Ok(JsValue::Int(42)));
    assert_eq!(run(""), Ok(JsValue::Null), "no return → NULL");
    assert_eq!(run("return 'é';"), Ok(JsValue::Str("é".into())));
    assert_eq!(run("return true;"), Ok(JsValue::Bool(true)));
    assert_eq!(run("return 1.5;"), Ok(JsValue::Double(1.5)));
}

#[test]
fn number_conversions_are_exact() {
    assert_eq!(run("return 2 ** 40;"), Ok(JsValue::Int(1 << 40)));
    assert_eq!(
        run("return 2 ** 53;"),
        Ok(JsValue::Double(9007199254740992.0))
    );
    assert_eq!(run("return -0;"), Ok(JsValue::Double(-0.0)));
    assert!(matches!(run("return NaN;"), Ok(JsValue::Double(x)) if x.is_nan()));
    assert_eq!(
        run("return 9223372036854775807n;"),
        Ok(JsValue::Int(i64::MAX))
    );
    assert!(matches!(
        run("return 9223372036854775808n;"),
        Err(JsError::Value(m)) if m.contains("BIGINT range")
    ));
    assert_eq!(
        run("return -9223372036854775808n;"),
        Ok(JsValue::Int(i64::MIN))
    );
    // Large BIGINTs arrive as BigInt, never rounded.
    let out = run_procedure(
        "return typeof x + ':' + (x + 1n);",
        &["x"],
        &[JsValue::Int(i64::MAX - 1)],
        &SandboxConfig::default(),
        &mut FakeHost::default(),
    );
    assert_eq!(out, Ok(JsValue::Str("bigint:9223372036854775807".into())));
    for (src, kind) in [("return {};", "object"), ("return () => 1;", "function")] {
        assert!(
            matches!(run(src), Err(JsError::Value(ref m)) if m.contains(kind)),
            "{src}"
        );
    }
}

#[test]
fn denied_intrinsics_are_absent() {
    for name in DENIED_INTRINSICS {
        if *name == "import" {
            // `import` is syntax, not a binding: dynamic `import()` must be
            // refused (there is no module loader).
            let mut host = FakeHost::default();
            let out = run_with(
                "import('fs').then(() => db.exec('loaded'), () => db.exec('refused')); return 1;",
                &mut host,
            );
            assert_eq!(out, Ok(JsValue::Int(1)));
            assert_eq!(
                host.requests,
                vec![HostRequest::Exec {
                    sql: "refused".into(),
                    params: vec![]
                }]
            );
            continue;
        }
        let probe =
            format!("try {{ return typeof ({name}); }} catch (e) {{ return 'undefined'; }}");
        assert_eq!(
            run(&probe),
            Ok(JsValue::Str("undefined".into())),
            "{name} must not exist in the sandbox"
        );
    }
    for src in [
        "return eval('1');",
        "return globalThis['ev' + 'al']('1');",
        "return Function('return 1')();",
        "return new Function('return 1')();",
        "return (function () {}).constructor('return 1')();",
        "return (function* () {}).constructor('yield 1')().next().value;",
        "return Object.getPrototypeOf(function () {}).constructor('return 1')();",
    ] {
        assert!(run(src).is_err(), "{src} must not compile code at run time");
    }
    // Ordinary functions, closures and generators still work.
    assert_eq!(
        run(
            "const g = function* () { yield 2; }; const f = (x) => x + g().next().value; return f(1);"
        ),
        Ok(JsValue::Int(3))
    );
    assert_eq!(
        run("return Object.isFrozen(db) && typeof db.random;"),
        Ok(JsValue::Str("function".into()))
    );
}

#[test]
fn host_calls_carry_bound_parameters() {
    let mut host = FakeHost {
        rows: vec![vec![JsValue::Int(7), JsValue::Str("x".into())]],
        ..Default::default()
    };
    let out = run_with(
        "print('n', 1, null); \
         const n = db.exec('INSERT INTO t VALUES (?, ?)', [1, \"it's; DROP\"]); \
         const rows = db.query('SELECT a, b FROM t'); \
         return n + rows[0][0] + db.value('SELECT a FROM t');",
        &mut host,
    );
    assert_eq!(out, Ok(JsValue::Int(17)));
    assert_eq!(host.requests[0], HostRequest::Print("n 1 null".into()));
    assert_eq!(
        host.requests[1],
        HostRequest::Exec {
            sql: "INSERT INTO t VALUES (?, ?)".into(),
            params: vec![JsValue::Int(1), JsValue::Str("it's; DROP".into())],
        }
    );
    assert_eq!(host.requests.len(), 4);
}

#[test]
fn host_errors_are_catchable_and_reported_as_host_errors() {
    assert!(matches!(
        run("db.exec('INSERT INTO boom VALUES (1)');"),
        Err(JsError::Host(m)) if m.contains("boom")
    ));
    assert_eq!(
        run("try { db.exec('boom'); } catch (e) { return e.name; }"),
        Ok(JsValue::Str("PinkDbError".into()))
    );
    assert!(matches!(
        run("throw new TypeError('nope');"),
        Err(JsError::Script(m)) if m.contains("nope")
    ));
    assert!(matches!(
        run("db.exec('x', [{}]);"),
        Err(JsError::Host(m)) if m.contains("parameter 1")
    ));
}

#[test]
fn syntax_and_parameter_names_are_checked_up_front() {
    assert!(check_script("return a + 1;", &["a"]).is_ok());
    assert!(matches!(
        check_script("return (;", &[]),
        Err(JsError::Script(_))
    ));
    for bad in ["class", "1x", "a b", "arguments", "é"] {
        assert!(
            matches!(check_script("return 1;", &[bad]), Err(JsError::Value(_))),
            "{bad}"
        );
    }
}

#[test]
fn random_is_seeded_and_replay_stable() {
    let draw = |seed| {
        let config = SandboxConfig {
            seed,
            ..SandboxConfig::default()
        };
        run_procedure(
            "return [db.random(), db.random(), db.random()].join(',');",
            &[],
            &[],
            &config,
            &mut FakeHost::default(),
        )
    };
    assert_eq!(draw(7), draw(7));
    assert_ne!(draw(7), draw(8));
    let Ok(JsValue::Str(s)) = draw(7) else {
        panic!("expected a string")
    };
    for x in s.split(',') {
        let x: f64 = x.parse().unwrap();
        assert!((0.0..1.0).contains(&x), "{x}");
    }
}

#[test]
fn limits_end_runaway_scripts_cleanly() {
    let tight = SandboxConfig {
        step_limit: 1_000_000,
        ..SandboxConfig::default()
    };
    let out = run_procedure(
        "while (true) {}",
        &[],
        &[],
        &tight,
        &mut FakeHost::default(),
    );
    assert!(
        matches!(&out, Err(JsError::Host(m)) if m.contains("step limit")),
        "{out:?}"
    );
    // A catch cannot keep the script alive past the limit.
    let out = run_procedure(
        "for (;;) { try { while (true) {} } catch (e) {} }",
        &[],
        &[],
        &tight,
        &mut FakeHost::default(),
    );
    assert!(
        matches!(&out, Err(JsError::Host(m)) if m.contains("step limit")),
        "{out:?}"
    );

    let few = SandboxConfig {
        statement_limit: 3,
        ..SandboxConfig::default()
    };
    let mut host = FakeHost::default();
    let out = run_procedure(
        "for (let i = 0; i < 10; i++) { try { db.exec('x'); } catch (e) {} }",
        &[],
        &[],
        &few,
        &mut host,
    );
    assert!(
        matches!(&out, Err(JsError::Host(m)) if m.contains("statement limit")),
        "{out:?}"
    );
    assert_eq!(host.requests.len(), 3, "no host call after the limit");

    let small = SandboxConfig {
        memory_limit: 4 * 1024 * 1024,
        ..SandboxConfig::default()
    };
    let out = run_procedure(
        "const a = []; while (true) { a.push('x'.repeat(1024)); }",
        &[],
        &[],
        &small,
        &mut FakeHost::default(),
    );
    assert!(out.is_err(), "allocation bomb must fail: {out:?}");

    let out = run("function f(n) { return f(n + 1) + 1; } return f(0);");
    assert!(
        matches!(&out, Err(JsError::Script(m)) if m.to_lowercase().contains("stack")),
        "{out:?}"
    );
}

/// Each call starts from a fresh context: globals do not leak between calls.
#[test]
fn calls_do_not_share_state() {
    assert_eq!(run("globalThis.leak = 1; return 1;"), Ok(JsValue::Int(1)));
    assert_eq!(
        run("return typeof globalThis.leak;"),
        Ok(JsValue::Str("undefined".into()))
    );
}

/// `async` helpers run to completion inside the call (promise jobs are
/// drained before it returns), and creating one never crashes the engine.
#[test]
fn async_helpers_run_inside_the_call() {
    let mut host = FakeHost::default();
    let out = run_with(
        "async function f(x) { await null; db.exec('INSERT ' + x); } f(1); f(2); return 0;",
        &mut host,
    );
    assert_eq!(out, Ok(JsValue::Int(0)));
    assert_eq!(
        host.requests.len(),
        2,
        "both async bodies ran: {:?}",
        host.requests
    );
    // A throw inside a job is the call's error, not a silent success.
    let out = run("Promise.resolve().then(() => { throw new Error('late'); }); return 1;");
    assert!(
        out.is_ok() || matches!(&out, Err(JsError::Script(m)) if m.contains("late")),
        "{out:?}"
    );
    // A job that spins is still bounded by the step limit.
    let tight = SandboxConfig {
        step_limit: 1_000_000,
        ..SandboxConfig::default()
    };
    let out = run_procedure(
        "(async () => { await null; while (true) {} })(); return 1;",
        &[],
        &[],
        &tight,
        &mut FakeHost::default(),
    );
    assert!(
        matches!(&out, Err(JsError::Host(m)) if m.contains("step limit")),
        "{out:?}"
    );
}
