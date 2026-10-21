//! pinkdb-js — the deterministic JavaScript sandbox for pinkdb procedures
//! (roadmap wave 44, `LANGUAGE JAVASCRIPT`).
//!
//! # What this crate is
//!
//! `pinkdb` stores a SQL procedure as a parsed body it can rebind statement by
//! statement; a **JavaScript** procedure is the same catalog object with an
//! opaque script body, and this crate owns everything about *running* that
//! body: an engine-agnostic host contract, the determinism policy the engine
//! must be configured with, and the conversion rules between JavaScript
//! values and pinkdb's [`pinkdb::Value`](https://docs.rs/pinkdb)-shaped
//! requests. `pinkdb` links it behind the optional `js` cargo feature, so the
//! default build stays lean and engine-free.
//!
//! # The engine (wave 81)
//!
//! With the `engine` cargo feature (turned on by `pinkdb`'s `js` feature)
//! the body runs in **quickjs-ng** through the vendored `rquickjs` crate —
//! see the `engine` module for the intrinsic selection, the limits and the
//! one contained `unsafe` block. Without it, [`engine_available`] returns
//! `Err` and [`run_procedure`] reports [`JsError::EngineUnavailable`]: a
//! clean, documented refusal. `pinkdb` checks [`engine_available`] before
//! storing a `LANGUAGE JAVASCRIPT` definition, so a database can never hold
//! a definition its build cannot run.
//!
//! The engine compiles for `wasm32-unknown-unknown` (rquickjs ships the
//! bindings and a libc shim) but is not yet exercised there; `js` is
//! supported on native targets only until a wasm test runner covers it.
//!
//! # The host contract
//!
//! A script sees exactly [`HOST_FUNCTIONS`] — nothing else:
//!
//! ```text
//! print(...args)                  → appends one line to the call's output
//! db.exec(sql [, params])         → affected-row count (DML/DDL)
//! db.query(sql [, params])        → array of rows (arrays of values)
//! db.value(sql [, params])        → the first column of the first row, or null
//! db.random()                     → a deterministic number in [0, 1)
//! ```
//!
//! The body is the body of a strict-mode function whose parameters are the
//! procedure's parameters (each must be a plain JavaScript identifier);
//! `return x` makes `x` the call's result.
//!
//! Every statement is sent to the host as a [`HostRequest`], which the
//! executor binds with parameters **as bound values** — exactly like the SQL
//! procedural language (never string-spliced SQL). The host also applies the
//! SQL language's replay rules: a statement that reads an attached lake, a
//! session-local view, or calls a loadable-extension function is refused
//! (see `pinkdb::procedure`).
//!
//! # The determinism policy
//!
//! A durable pinkdb store logs the committed `CALL` statement and replays it
//! by re-executing it (see `pinkdb::procedure`'s module docs). A script is
//! therefore only legal if it is a pure function of (script text, arguments,
//! database state). The sandbox enforces that by *absence*:
//!
//! * **no clock** — [`DENIED_INTRINSICS`] removes `Date`, `performance`,
//!   `Intl` date formatting and every other time source; a script that needs
//!   a timestamp must take it as a parameter or read it from a table.
//! * **no randomness** — `Math.random` is removed; a seeded generator
//!   (`db.random()`), seeded from the procedure definition at every call, is
//!   available instead, so two replays of one record draw the same stream.
//! * **no ambient I/O** — no `fetch`, `XMLHttpRequest`, `require`, `import`,
//!   `process`, `setTimeout`/`setInterval`, `Atomics`/`SharedArrayBuffer`, and
//!   no filesystem or network host function exists.
//! * **no run-time compilation** — `eval` and the `Function` constructor are
//!   not available (the engine's `Eval` intrinsic is not installed).
//! * **bounded execution** — [`SandboxConfig::statement_limit`] caps the
//!   number of host calls, [`SandboxConfig::step_limit`] the script's
//!   evaluation steps, and [`SandboxConfig::memory_limit`] /
//!   [`SandboxConfig::stack_limit`] its heap and native stack, so a runaway
//!   script fails the call cleanly instead of wedging the engine.
//!
//! Anything a script can observe is either an argument, a host reply, or the
//! script text: that is the whole guarantee the WAL replay strategy needs, and
//! it mirrors `pinkdb::procedure`'s rules for the SQL language.
//!
//! # Conversions
//!
//! Values cross the boundary as [`JsValue`] (never as JavaScript objects with
//! hidden state): `NULL`/`null`/`undefined` map to `NULL`, booleans to
//! `BOOLEAN`, integral numbers within `±(2^53 − 1)` to `BIGINT` (or `INTEGER`
//! when they fit — the host binds them exactly like literals), other numbers
//! (fractions, NaN, ±∞, `-0`) to `DOUBLE`, strings to `VARCHAR`, and a
//! `BigInt` in the `BIGINT` range to `BIGINT`. Going the other way, integers
//! beyond `±(2^53 − 1)` arrive as `BigInt`, so they are never rounded. A
//! value of any other JavaScript type is a [`JsError::Value`] naming its
//! position.

use std::fmt;

#[cfg(feature = "engine")]
mod engine;

/// The engine status of this build: `Ok(())` when [`run_procedure`] can
/// evaluate scripts.
///
/// Without the `engine` feature this is the documented refusal. `pinkdb`
/// calls it before storing a `LANGUAGE JAVASCRIPT` definition, so the refusal
/// happens at `CREATE` time, never at `CALL` time.
pub fn engine_available() -> Result<(), String> {
    #[cfg(feature = "engine")]
    {
        Ok(())
    }
    #[cfg(not(feature = "engine"))]
    {
        Err(
            "the JavaScript engine is not built into this pinkdb-js build (enable the \
             `engine` feature — pinkdb's `js` feature does — to bind quickjs-ng)"
                .to_string(),
        )
    }
}

/// Check a definition without running it: every parameter name must be a
/// plain JavaScript identifier and the body must compile as a function body.
/// `pinkdb` calls this at `CREATE PROCEDURE … LANGUAGE JAVASCRIPT`, so a
/// syntax error is reported when the procedure is defined, not when it is
/// first called.
pub fn check_script(source: &str, params: &[&str]) -> Result<(), JsError> {
    #[cfg(feature = "engine")]
    {
        engine::check_script(source, params)
    }
    #[cfg(not(feature = "engine"))]
    {
        let _ = (source, params);
        Err(JsError::EngineUnavailable(
            engine_available().expect_err("no engine in this build"),
        ))
    }
}

/// JavaScript intrinsics and globals the sandbox must remove (or never
/// install): every entry is a determinism hole for WAL replay. The engine
/// binding enforces this list; the tests pin that the policy still names the
/// clock, randomness and ambient I/O.
pub const DENIED_INTRINSICS: &[&str] = &[
    // Time: a clock makes two replays of one record differ.
    "Date",
    "Date.now",
    "performance",
    "Intl.DateTimeFormat",
    // Randomness (a seeded `db.random()` replaces it).
    "Math.random",
    "crypto",
    "getRandomValues",
    // Ambient I/O and host access.
    "fetch",
    "XMLHttpRequest",
    "WebSocket",
    "require",
    "import",
    "process",
    "Deno",
    "std",
    "Worker",
    "setTimeout",
    "setInterval",
    "setImmediate",
    "queueMicrotask",
    "Atomics",
    "SharedArrayBuffer",
    "FinalizationRegistry",
];

/// One host function a script may call, with its documented signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostFunctionSpec {
    pub name: &'static str,
    pub signature: &'static str,
    pub docs: &'static str,
}

/// The complete host surface: a script can call exactly these.
pub const HOST_FUNCTIONS: &[HostFunctionSpec] = &[
    HostFunctionSpec {
        name: "print",
        signature: "print(...args)",
        docs: "append one line to the call's output (joined by a single space)",
    },
    HostFunctionSpec {
        name: "db.exec",
        signature: "db.exec(sql, params?)",
        docs: "run a statement, return the affected-row count (parameters are bound values)",
    },
    HostFunctionSpec {
        name: "db.query",
        signature: "db.query(sql, params?)",
        docs: "run a query, return an array of rows (each row an array of values)",
    },
    HostFunctionSpec {
        name: "db.value",
        signature: "db.value(sql, params?)",
        docs: "return the first column of the first row of a query, or null",
    },
    HostFunctionSpec {
        name: "db.random",
        signature: "db.random()",
        docs: "a number in [0, 1) from a generator seeded by the definition (replay-stable)",
    },
];

/// Determinism knobs the host pins for one call. Both are part of the call's
/// record: the same record replays with the same limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxConfig {
    /// Seed of the deterministic generator behind `db.random()`.
    pub seed: u64,
    /// Upper bound on host calls (`db.exec`/`db.query`/`db.value`) per call.
    pub statement_limit: u64,
    /// Upper bound on evaluated script steps per call. The engine counts in
    /// quickjs-ng's interrupt budget (one check per 10,000 internal
    /// operations), so the limit is enforced in those units: deterministic
    /// for a given script and build, not a portable instruction count.
    pub step_limit: u64,
    /// Upper bound on the script's heap, in bytes.
    pub memory_limit: usize,
    /// Upper bound on the native stack the script may use, in bytes.
    pub stack_limit: usize,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            statement_limit: 10_000,
            step_limit: 50_000_000,
            memory_limit: 64 * 1024 * 1024,
            stack_limit: 256 * 1024,
        }
    }
}

/// A value crossing the sandbox boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum JsValue {
    Null,
    Bool(bool),
    /// An integral number that fits `i64`.
    Int(i64),
    Double(f64),
    Str(String),
}

impl fmt::Display for JsValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsValue::Null => write!(f, "null"),
            JsValue::Bool(b) => write!(f, "{b}"),
            JsValue::Int(i) => write!(f, "{i}"),
            JsValue::Double(x) => write!(f, "{x:?}"),
            JsValue::Str(s) => write!(f, "'{s}'"),
        }
    }
}

/// One request a script makes to the host.
#[derive(Debug, Clone, PartialEq)]
pub enum HostRequest {
    /// `print(...)`.
    Print(String),
    /// `db.exec(sql, params)`: run a mutating statement.
    Exec { sql: String, params: Vec<JsValue> },
    /// `db.query(sql, params)`: run a query.
    Query { sql: String, params: Vec<JsValue> },
}

/// The host's answer.
#[derive(Debug, Clone, PartialEq)]
pub enum HostReply {
    /// `print` acknowledged.
    Ok,
    /// `db.exec`'s affected-row count.
    Affected(u64),
    /// `db.query`'s rows (each row in column order).
    Rows(Vec<Vec<JsValue>>),
}

/// What the sandbox calls for every [`HostRequest`]. The executor implements
/// it over the ordinary bind/execute path (parameters bound as values, never
/// SQL text).
pub trait Host {
    fn handle(&mut self, request: HostRequest) -> Result<HostReply, String>;
}

/// A sandbox failure.
#[derive(Debug, Clone, PartialEq)]
pub enum JsError {
    /// No engine binding in this build (see [`engine_available`]).
    EngineUnavailable(String),
    /// The script failed to parse or threw.
    Script(String),
    /// A host call failed (a SQL error, a refusal, an exceeded limit).
    Host(String),
    /// A value could not cross the boundary (unsupported JavaScript type).
    Value(String),
}

impl fmt::Display for JsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsError::EngineUnavailable(m) => write!(f, "{m}"),
            JsError::Script(m) => write!(f, "JavaScript error: {m}"),
            JsError::Host(m) => write!(f, "host call failed: {m}"),
            JsError::Value(m) => write!(f, "cannot convert value: {m}"),
        }
    }
}

impl std::error::Error for JsError {}

/// Evaluate one JavaScript procedure body with `args` bound to the
/// parameters named `params`, sending every `print`/`db.*` call to `host`.
/// Returns the body's `return` value (`Null` when it returns nothing).
///
/// Without the `engine` feature every call reports
/// [`JsError::EngineUnavailable`] before the script is looked at.
pub fn run_procedure(
    source: &str,
    params: &[&str],
    args: &[JsValue],
    config: &SandboxConfig,
    host: &mut dyn Host,
) -> Result<JsValue, JsError> {
    #[cfg(feature = "engine")]
    {
        engine::run_procedure(source, params, args, config, host)
    }
    #[cfg(not(feature = "engine"))]
    {
        let _ = (source, params, args, config, host);
        Err(JsError::EngineUnavailable(
            engine_available().expect_err("this build has no engine"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "engine"))]
    struct NoHost;

    #[cfg(not(feature = "engine"))]
    impl Host for NoHost {
        fn handle(&mut self, _request: HostRequest) -> Result<HostReply, String> {
            panic!("the host must never be called when the engine is unavailable")
        }
    }

    /// The build-level refusal is clean, documented, and happens before any
    /// script or host interaction.
    #[cfg(not(feature = "engine"))]
    #[test]
    fn engine_unavailable_refusal_is_clean() {
        let err = engine_available().expect_err("no engine in this build");
        assert!(err.contains("not built"), "{err}");
        assert!(err.contains("`engine` feature"), "{err}");
        let err = run_procedure("1 + 1", &[], &[], &SandboxConfig::default(), &mut NoHost)
            .expect_err("no engine in this build");
        assert!(matches!(err, JsError::EngineUnavailable(_)), "{err:?}");
        assert!(err.to_string().contains("not built"), "{err}");
    }

    /// The determinism policy still names the clock, randomness and ambient
    /// I/O — the three holes WAL replay cannot survive.
    #[test]
    fn determinism_policy_names_the_holes() {
        for name in [
            "Date",
            "Date.now",
            "Math.random",
            "fetch",
            "require",
            "setTimeout",
        ] {
            assert!(DENIED_INTRINSICS.contains(&name), "{name} must stay denied");
        }
    }

    /// The host surface is exactly the documented five functions.
    #[test]
    fn host_surface_is_small_and_documented() {
        let names: Vec<&str> = HOST_FUNCTIONS.iter().map(|f| f.name).collect();
        assert_eq!(
            names,
            vec!["print", "db.exec", "db.query", "db.value", "db.random"]
        );
        assert!(HOST_FUNCTIONS.iter().all(|f| !f.docs.is_empty()));
    }
}
