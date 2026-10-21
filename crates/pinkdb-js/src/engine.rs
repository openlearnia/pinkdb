//! The quickjs-ng engine binding (wave 81), compiled with the `engine`
//! feature.
//!
//! Every call gets a fresh `Runtime` + `Context`, so nothing survives from
//! one call to the next. The context is built from an explicit intrinsic
//! list — no `Date`, `Performance`, `WeakRef`, `TypedArrays`
//! (`SharedArrayBuffer`/`Atomics`) or `Proxy` — and `Math.random` (part of
//! the base objects) is deleted before the script runs. The script sees
//! `print` and a frozen `db` object, nothing else.
//!
//! `Promise` is installed: quickjs-ng leaks (and asserts at teardown) when a
//! script creates an `async` function without it. Promise jobs are drained
//! before the call returns, under the same limits, so `async` helpers run to
//! completion inside the call — deterministically (the job queue is FIFO and
//! there is no timer).
//!
//! The `Eval` intrinsic *is* installed, because quickjs-ng cannot compile
//! anything — the host's own compile of the procedure body included —
//! without it. The body is compiled first; then [`LOCK_DOWN`] deletes
//! `eval` and replaces every code-compiling constructor (`Function`,
//! `GeneratorFunction`, `AsyncFunction`, `AsyncGeneratorFunction`) with a
//! throwing stub, all before any user code runs, so a script cannot compile
//! code at run time.
//!
//! # The one `unsafe` block (contained-FFI carve-out)
//!
//! The engine only accepts `'static` host closures, but the [`Host`] the
//! executor passes in borrows the database. [`HostSlot`] holds a
//! lifetime-erased pointer to it for exactly the duration of
//! [`run_procedure`]: the slot is filled after the runtime is built, emptied
//! by a drop guard before the runtime is dropped (panics included), and
//! taken out while a host call runs, so the pointer is never dereferenced
//! after `run_procedure` returns and never aliased.

use std::cell::{Cell, RefCell};
use std::ptr::NonNull;
use std::rc::Rc;

use rquickjs::context::intrinsic;
use rquickjs::function::{Opt, Rest};
use rquickjs::{
    Array, BigInt, CaughtError, Coerced, Context, Ctx, Exception, Function, Object, Runtime, Value,
};

use crate::{Host, HostReply, HostRequest, JsError, JsValue, SandboxConfig};

/// quickjs-ng calls the interrupt handler once per this many internal
/// operations (`JS_INTERRUPT_COUNTER_INIT`); `SandboxConfig::step_limit` is
/// enforced in units of it.
pub(crate) const INTERRUPT_BUDGET: u64 = 10_000;

/// JavaScript procedures may call JavaScript procedures (through `CALL` in
/// `db.exec`) at most this deep. Each level owns a runtime whose stack
/// budget is measured from its own entry point, so the bound keeps the
/// worst-case native stack use at `MAX_JS_NESTING × stack_limit`.
pub(crate) const MAX_JS_NESTING: usize = 4;

/// The largest integer a JavaScript number holds exactly (`2^53 − 1`).
const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

/// Every intrinsic beyond the base objects the sandbox installs.
type Intrinsics = (
    intrinsic::Eval,
    intrinsic::RegExpCompiler,
    intrinsic::RegExp,
    intrinsic::Json,
    intrinsic::MapSet,
    intrinsic::Promise,
);

/// Runs after the body is compiled and before it is called: removes
/// `queueMicrotask` (it comes with `Promise`) and every way to compile code
/// at run time (`eval`, and the constructors reachable
/// as `Function` or as `.constructor` of a function, generator, async
/// function or async generator — the latter two may not exist without the
/// `Promise` intrinsic, hence the `try`).
const LOCK_DOWN: &str = r#"(function () {
  "use strict";
  const deny = function () {
    throw new EvalError("run-time code compilation is disabled in pinkdb procedures");
  };
  const protos = [Function.prototype];
  for (const make of [
    () => Object.getPrototypeOf(function* () {}),
    () => Object.getPrototypeOf(async function () {}),
    () => Object.getPrototypeOf(async function* () {}),
  ]) {
    try { protos.push(make()); } catch (e) {}
  }
  for (const proto of protos) {
    Object.defineProperty(proto, "constructor", {
      value: deny, writable: false, enumerable: false, configurable: false,
    });
  }
  delete globalThis.eval;
  // Installed with `Promise`; denied by the contract (`DENIED_INTRINSICS`).
  delete globalThis.queueMicrotask;
  Object.defineProperty(globalThis, "Function", {
    value: deny, writable: false, enumerable: false, configurable: false,
  });
})();"#;

/// Error `name` the host's failures are thrown with, so the final error can
/// be reported as [`JsError::Host`] when the script does not catch it.
const HOST_ERROR_NAME: &str = "PinkDbError";

thread_local! {
    static NESTING: Cell<usize> = const { Cell::new(0) };
}

/// The lifetime-erased host pointer (see the module docs).
struct HostSlot {
    ptr: Cell<Option<NonNull<dyn Host + 'static>>>,
}

impl HostSlot {
    /// Run `f` with the host, or `None` when the slot is empty (after
    /// `run_procedure` started returning, or while another host call is
    /// already running — re-entry is refused rather than aliased).
    fn with<R>(&self, f: impl FnOnce(&mut dyn Host) -> R) -> Option<R> {
        let ptr = self.ptr.take()?;
        // SAFETY: the pointer was created from the `&mut dyn Host` that
        // `run_procedure` holds for its whole body, and `SlotGuard` clears
        // the slot before that borrow ends. Taking the pointer out of the
        // slot while `f` runs guarantees this is the only live reference.
        let out = f(unsafe { &mut *ptr.as_ptr() });
        self.ptr.set(Some(ptr));
        Some(out)
    }
}

/// Clears the slot when `run_procedure` exits by any path.
struct SlotGuard(Rc<HostSlot>);

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.0.ptr.set(None);
    }
}

/// Decrements the nesting counter when a call exits by any path.
struct NestingGuard;

impl Drop for NestingGuard {
    fn drop(&mut self) {
        NESTING.with(|n| n.set(n.get().saturating_sub(1)));
    }
}

/// Per-call bookkeeping shared with the host closures.
struct CallState {
    slot: Rc<HostSlot>,
    host_calls: Cell<u64>,
    statement_limit: u64,
    /// Set when a limit is hit: the interrupt handler then aborts the script
    /// at its next check, so a `catch` cannot keep a runaway call alive.
    fatal: RefCell<Option<String>>,
    random: Cell<u64>,
}

/// Check that `params` are usable JavaScript parameter names and that
/// `source` compiles as a function body. Runs nothing.
pub(crate) fn check_script(source: &str, params: &[&str]) -> Result<(), JsError> {
    check_params(params)?;
    let (rt, ctx) = new_runtime(&SandboxConfig::default())?;
    let out = ctx.with(|ctx| {
        compile(&ctx, source, params)
            .map(|_| ())
            .map_err(|e| script_error(&ctx, e))
    });
    drop(ctx);
    drop(rt);
    out
}

/// Evaluate one procedure body with `args` bound to `params`.
pub(crate) fn run_procedure(
    source: &str,
    params: &[&str],
    args: &[JsValue],
    config: &SandboxConfig,
    host: &mut dyn Host,
) -> Result<JsValue, JsError> {
    check_params(params)?;
    if params.len() != args.len() {
        return Err(JsError::Value(format!(
            "{} argument(s) for {} parameter(s)",
            args.len(),
            params.len()
        )));
    }
    let depth = NESTING.with(|n| n.get());
    if depth >= MAX_JS_NESTING {
        return Err(JsError::Host(format!(
            "JavaScript procedures nest at most {MAX_JS_NESTING} deep"
        )));
    }
    NESTING.with(|n| n.set(depth + 1));
    let _nesting = NestingGuard;

    let (rt, ctx) = new_runtime(config)?;
    let slot = Rc::new(HostSlot {
        ptr: Cell::new(None),
    });
    let _slot_guard = SlotGuard(Rc::clone(&slot));
    let ptr: NonNull<dyn Host + '_> = NonNull::from(host);
    // SAFETY: only erases the trait object's lifetime; see `HostSlot::with`
    // for why the pointer never outlives the borrow.
    let ptr: NonNull<dyn Host + 'static> = unsafe { std::mem::transmute(ptr) };
    slot.ptr.set(Some(ptr));

    let state = Rc::new(CallState {
        slot,
        host_calls: Cell::new(0),
        statement_limit: config.statement_limit,
        fatal: RefCell::new(None),
        random: Cell::new(config.seed),
    });
    let allowed_ticks = config.step_limit.div_ceil(INTERRUPT_BUDGET);
    {
        let state = Rc::clone(&state);
        let ticks = Cell::new(0u64);
        rt.set_interrupt_handler(Some(Box::new(move || {
            ticks.set(ticks.get() + 1);
            if ticks.get() > allowed_ticks {
                let mut fatal = state.fatal.borrow_mut();
                if fatal.is_none() {
                    *fatal = Some(format!(
                        "step limit exceeded (limit {} steps, enforced in units of \
                         {INTERRUPT_BUDGET})",
                        allowed_ticks * INTERRUPT_BUDGET
                    ));
                }
            }
            state.fatal.borrow().is_some()
        })));
    }

    let out = ctx.with(|ctx| -> Result<JsValue, JsError> {
        install_globals(&ctx, &state).map_err(|e| script_error(&ctx, e))?;
        let func = compile(&ctx, source, params).map_err(|e| script_error(&ctx, e))?;
        ctx.eval::<(), _>(LOCK_DOWN)
            .map_err(|e| script_error(&ctx, e))?;
        let mut js_args = Vec::with_capacity(args.len());
        for a in args {
            js_args.push(to_js(&ctx, a).map_err(|e| script_error(&ctx, e))?);
        }
        let ret = func.call::<_, Value>((Rest(js_args),));
        if let Some(why) = state.fatal.borrow().clone() {
            // Clear the pending (uncatchable) exception before the context
            // is dropped, then report the limit.
            if let Err(e) = ret {
                let _ = script_error(&ctx, e);
            }
            return Err(JsError::Host(why));
        }
        let ret = ret.map_err(|e| script_error(&ctx, e))?;
        from_js(&ret, "the return value")
    });
    // Drain promise jobs (async helpers), still under the interrupt handler
    // and the host's statement limit. Runs outside `ctx.with`: executing a
    // job takes the runtime lock itself.
    let out = out.and_then(|value| {
        while rt.is_job_pending() {
            if let Err(e) = rt.execute_pending_job() {
                let ctx = e.0;
                return Err(ctx.with(|ctx| {
                    let err = script_error(&ctx, rquickjs::Error::Exception);
                    match state.fatal.borrow().clone() {
                        Some(why) => JsError::Host(why),
                        None => err,
                    }
                }));
            }
        }
        match state.fatal.borrow().clone() {
            Some(why) => Err(JsError::Host(why)),
            None => Ok(value),
        }
    });
    rt.set_interrupt_handler(None);
    drop(ctx);
    drop(rt);
    out
}

fn new_runtime(config: &SandboxConfig) -> Result<(Runtime, Context), JsError> {
    let rt =
        Runtime::new().map_err(|e| JsError::Script(format!("cannot start the engine: {e}")))?;
    rt.set_memory_limit(config.memory_limit);
    rt.set_max_stack_size(config.stack_limit);
    let ctx = Context::custom::<Intrinsics>(&rt)
        .map_err(|e| JsError::Script(format!("cannot start the engine: {e}")))?;
    Ok((rt, ctx))
}

/// Compile the body as a strict-mode function of the declared parameters
/// (inside the sandboxed context; nothing runs until the caller calls it).
fn compile<'js>(ctx: &Ctx<'js>, source: &str, params: &[&str]) -> rquickjs::Result<Function<'js>> {
    let wrapped = format!(
        "(function ({}) {{ \"use strict\";\n{source}\n}})",
        params.join(", ")
    );
    ctx.eval::<Function<'js>, _>(wrapped)
}

/// `print`, the frozen `db` object, and the removal of `Math.random`.
fn install_globals<'js>(ctx: &Ctx<'js>, state: &Rc<CallState>) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    let math: Object = globals.get("Math")?;
    math.remove("random")?;

    let st = Rc::clone(state);
    globals.set(
        "print",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, args: Rest<Coerced<String>>| {
                let line = args
                    .0
                    .into_iter()
                    .map(|c| c.0)
                    .collect::<Vec<_>>()
                    .join(" ");
                host_call(&ctx, &st, HostRequest::Print(line)).map(|_| ())
            },
        )?,
    )?;

    let db = Object::new(ctx.clone())?;
    let st = Rc::clone(state);
    db.set(
        "exec",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, sql: String, params: Opt<Array<'js>>| {
                let params = params_of(&ctx, params.0)?;
                match host_call(&ctx, &st, HostRequest::Exec { sql, params })? {
                    HostReply::Affected(n) => Ok(Value::new_number(ctx, n as f64)),
                    other => Err(protocol_error(&ctx, "db.exec", &other)),
                }
            },
        )?,
    )?;
    let st = Rc::clone(state);
    db.set(
        "query",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, sql: String, params: Opt<Array<'js>>| {
                let params = params_of(&ctx, params.0)?;
                match host_call(&ctx, &st, HostRequest::Query { sql, params })? {
                    HostReply::Rows(rows) => rows_to_js(&ctx, &rows),
                    other => Err(protocol_error(&ctx, "db.query", &other)),
                }
            },
        )?,
    )?;
    let st = Rc::clone(state);
    db.set(
        "value",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, sql: String, params: Opt<Array<'js>>| {
                let params = params_of(&ctx, params.0)?;
                match host_call(&ctx, &st, HostRequest::Query { sql, params })? {
                    HostReply::Rows(rows) => match rows.first().and_then(|r| r.first()) {
                        Some(v) => to_js(&ctx, v),
                        None => Ok(Value::new_null(ctx)),
                    },
                    other => Err(protocol_error(&ctx, "db.value", &other)),
                }
            },
        )?,
    )?;
    let st = Rc::clone(state);
    db.set(
        "random",
        Function::new(ctx.clone(), move || {
            let (next, x) = splitmix64(st.random.get());
            st.random.set(next);
            // 53 random bits → [0, 1), like `Math.random`.
            (x >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
        })?,
    )?;
    let object: Object = globals.get("Object")?;
    let freeze: Function = object.get("freeze")?;
    let db: Object = freeze.call((db,))?;
    globals.set("db", db)?;
    Ok(())
}

/// One host call: the statement limit, the slot, and the host's error
/// thrown as a catchable `PinkDbError`.
fn host_call<'js>(
    ctx: &Ctx<'js>,
    state: &CallState,
    request: HostRequest,
) -> rquickjs::Result<HostReply> {
    if let Some(why) = state.fatal.borrow().clone() {
        return Err(throw_host(ctx, &why));
    }
    if !matches!(request, HostRequest::Print(_)) {
        let n = state.host_calls.get() + 1;
        state.host_calls.set(n);
        if n > state.statement_limit {
            let why = format!(
                "statement limit exceeded (limit {} host calls)",
                state.statement_limit
            );
            *state.fatal.borrow_mut() = Some(why.clone());
            return Err(throw_host(ctx, &why));
        }
    }
    match state.slot.with(|host| host.handle(request)) {
        Some(Ok(reply)) => Ok(reply),
        Some(Err(msg)) => Err(throw_host(ctx, &msg)),
        None => Err(throw_host(ctx, "the host is not available")),
    }
}

fn throw_host(ctx: &Ctx<'_>, msg: &str) -> rquickjs::Error {
    match Exception::from_message(ctx.clone(), msg) {
        Ok(e) => {
            let _ = e.set("name", HOST_ERROR_NAME);
            ctx.throw(e.into_value())
        }
        Err(e) => e,
    }
}

fn protocol_error(ctx: &Ctx<'_>, what: &str, reply: &HostReply) -> rquickjs::Error {
    throw_host(ctx, &format!("{what}: unexpected host reply {reply:?}"))
}

/// Map an engine error (usually a pending exception) to a [`JsError`].
fn script_error(ctx: &Ctx<'_>, e: rquickjs::Error) -> JsError {
    match CaughtError::from_error(ctx, e) {
        CaughtError::Exception(ex) => {
            let name: Option<String> = ex.get("name").ok();
            let message = ex.message().unwrap_or_default();
            if name.as_deref() == Some(HOST_ERROR_NAME) {
                JsError::Host(message)
            } else {
                JsError::Script(ex.to_string().trim_end().to_string())
            }
        }
        CaughtError::Value(v) => JsError::Script(format!(
            "uncaught {}",
            v.get::<Coerced<String>>()
                .map(|c| c.0)
                .unwrap_or_else(|_| v.type_name().to_string())
        )),
        CaughtError::Error(e) => JsError::Script(e.to_string()),
    }
}

fn params_of<'js>(ctx: &Ctx<'js>, params: Option<Array<'js>>) -> rquickjs::Result<Vec<JsValue>> {
    let Some(arr) = params else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter::<Value>().enumerate() {
        let v = v?;
        out.push(
            from_js(&v, &format!("parameter {}", i + 1))
                .map_err(|e| throw_host(ctx, &e.to_string()))?,
        );
    }
    Ok(out)
}

fn rows_to_js<'js>(ctx: &Ctx<'js>, rows: &[Vec<JsValue>]) -> rquickjs::Result<Value<'js>> {
    let out = Array::new(ctx.clone())?;
    for (i, row) in rows.iter().enumerate() {
        let r = Array::new(ctx.clone())?;
        for (j, v) in row.iter().enumerate() {
            r.set(j, to_js(ctx, v)?)?;
        }
        out.set(i, r)?;
    }
    Ok(out.into_value())
}

/// [`JsValue`] → JavaScript. Integers beyond `±(2^53 − 1)` become `BigInt`
/// (exact, and mixing them with numbers is a loud `TypeError` rather than a
/// silent rounding).
fn to_js<'js>(ctx: &Ctx<'js>, v: &JsValue) -> rquickjs::Result<Value<'js>> {
    Ok(match v {
        JsValue::Null => Value::new_null(ctx.clone()),
        JsValue::Bool(b) => Value::new_bool(ctx.clone(), *b),
        JsValue::Int(i) if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(i) => {
            match i32::try_from(*i) {
                Ok(small) => Value::new_int(ctx.clone(), small),
                Err(_) => Value::new_float(ctx.clone(), *i as f64),
            }
        }
        JsValue::Int(i) => BigInt::from_i64(ctx.clone(), *i)?.into_value(),
        JsValue::Double(x) => Value::new_float(ctx.clone(), *x),
        JsValue::Str(s) => rquickjs::String::from_str(ctx.clone(), s)?.into_value(),
    })
}

/// JavaScript → [`JsValue`] (`what` names the position in errors).
pub(crate) fn from_js(v: &Value<'_>, what: &str) -> Result<JsValue, JsError> {
    if v.is_undefined() || v.is_null() {
        return Ok(JsValue::Null);
    }
    if let Some(b) = v.as_bool() {
        return Ok(JsValue::Bool(b));
    }
    if let Some(i) = v.as_int() {
        return Ok(JsValue::Int(i64::from(i)));
    }
    if let Some(x) = v.as_float() {
        return Ok(number_to_value(x));
    }
    if let Some(s) = v.as_string() {
        return s
            .to_string()
            .map(JsValue::Str)
            .map_err(|e| JsError::Value(format!("{what}: {e}")));
    }
    if v.as_big_int().is_some() {
        // `BigInt::to_i64` wraps modulo 2^64; go through the exact decimal
        // text so an out-of-range value is an error, never a wrapped one.
        let text = v
            .get::<Coerced<String>>()
            .map_err(|e| JsError::Value(format!("{what}: {e}")))?
            .0;
        return text.parse::<i64>().map(JsValue::Int).map_err(|_| {
            JsError::Value(format!("{what}: BigInt {text} is outside the BIGINT range"))
        });
    }
    Err(JsError::Value(format!(
        "{what}: a JavaScript {} cannot cross into SQL (use null, a boolean, a number, a \
         string or a BigInt)",
        v.type_name()
    )))
}

/// Integral finite numbers in the safe range are integers (BIGINT/INTEGER);
/// everything else (fractions, NaN, ±∞, `-0`) stays a DOUBLE.
fn number_to_value(x: f64) -> JsValue {
    let safe = MAX_SAFE_INTEGER as f64;
    if x.is_finite() && x.fract() == 0.0 && x.abs() <= safe && !(x == 0.0 && x.is_sign_negative()) {
        JsValue::Int(x as i64)
    } else {
        JsValue::Double(x)
    }
}

fn splitmix64(state: u64) -> (u64, u64) {
    let next = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = next;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (next, z ^ (z >> 31))
}

/// JavaScript reserved words (ES2024, strict mode) plus the two names strict
/// mode forbids as parameters.
const RESERVED: &[&str] = &[
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "implements",
    "import",
    "in",
    "instanceof",
    "interface",
    "let",
    "new",
    "null",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "static",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "void",
    "while",
    "with",
    "yield",
    "eval",
    "arguments",
];

fn check_params(params: &[&str]) -> Result<(), JsError> {
    for p in params {
        let mut chars = p.chars();
        let ok = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        if !ok || RESERVED.contains(p) {
            return Err(JsError::Value(format!(
                "parameter name \"{p}\" is not a usable JavaScript identifier (ASCII letters, \
                 digits, `_` and `$`, not starting with a digit, not a reserved word)"
            )));
        }
    }
    Ok(())
}
