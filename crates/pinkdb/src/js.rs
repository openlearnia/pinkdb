//! The `js` cargo feature's bridge to the [`pinkdb_js`] sandbox crate.
//!
//! The engine, its limits and the JavaScript-side value rules live in
//! `pinkdb-js`; this module is the **host** a script talks to (wave 81):
//! every `db.exec`/`db.query`/`db.value` is parsed as exactly one statement,
//! its `?` placeholders are bound to the script's values (never spliced into
//! SQL text), and it runs through the same per-statement path as a SQL body
//! ([`crate::procedure::run_call_statement`]) — the same refusals, the same
//! replay checks, the same touched-set bookkeeping.

use pinkdb_js::{Host, HostReply, HostRequest, JsValue, SandboxConfig};

use crate::QueryResult;
use crate::procedure::ProcedureDef;
use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result, Value};

/// Can this build evaluate JavaScript procedure bodies?
pub(crate) fn engine_available() -> std::result::Result<(), String> {
    pinkdb_js::engine_available()
}

/// `CREATE PROCEDURE … LANGUAGE JAVASCRIPT`: the parameter names must be
/// JavaScript identifiers and the body must compile — reported when the
/// procedure is defined, not at its first call.
pub(crate) fn check_definition(name: &str, source: &str, params: &[String]) -> Result<()> {
    let params: Vec<&str> = params.iter().map(String::as_str).collect();
    pinkdb_js::check_script(source, &params)
        .map_err(|e| PinkDbError::Execution(format!("procedure \"{name}\": {e}")))
}

/// Run one call of a JavaScript procedure. Arguments are already coerced to
/// the declared types; atomicity and call depth belong to the caller
/// (`procedure::execute_call`).
pub(crate) fn call(
    db: &mut PinkDb,
    db_name: &str,
    def: &ProcedureDef,
    args: &[Value],
) -> Result<QueryResult> {
    let params: Vec<&str> = def.params.iter().map(|(n, _)| n.as_str()).collect();
    let js_args: Vec<JsValue> = args.iter().map(to_js).collect();
    let config = SandboxConfig {
        seed: seed_of(def),
        ..SandboxConfig::default()
    };
    let mut host = SqlHost {
        db,
        db_name,
        statements: 0,
        output: Vec::new(),
    };
    let ret = pinkdb_js::run_procedure(&def.source, &params, &js_args, &config, &mut host)
        .map_err(|e| PinkDbError::Execution(format!("procedure \"{}\": {e}", def.name)))?;
    let result = match ret {
        JsValue::Null => None,
        v => {
            let value = from_js(v);
            Some(Box::new(QueryResult::Selected {
                names: vec!["return".to_string()],
                types: vec![value.data_type().unwrap_or(crate::types::DataType::Varchar)],
                rows: vec![vec![value]],
            }))
        }
    };
    Ok(QueryResult::Called {
        name: def.name.clone(),
        statements: host.statements,
        result,
        output: host.output,
    })
}

/// The `db.random()` seed: a hash of the stored definition (name + body),
/// so every call — and every WAL replay of it — draws the same stream.
fn seed_of(def: &ProcedureDef) -> u64 {
    // FNV-1a, 64-bit: stable across builds and platforms.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in def
        .name
        .to_ascii_lowercase()
        .bytes()
        .chain([0u8])
        .chain(def.source.bytes())
    {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

struct SqlHost<'a> {
    db: &'a mut PinkDb,
    db_name: &'a str,
    statements: usize,
    output: Vec<String>,
}

impl Host for SqlHost<'_> {
    fn handle(&mut self, request: HostRequest) -> std::result::Result<HostReply, String> {
        match request {
            HostRequest::Print(line) => {
                self.output.push(line);
                Ok(HostReply::Ok)
            }
            HostRequest::Exec { sql, params } => match self.run("db.exec", &sql, params)? {
                QueryResult::Inserted { count }
                | QueryResult::Updated { count }
                | QueryResult::Deleted { count } => Ok(HostReply::Affected(count as u64)),
                _ => Ok(HostReply::Affected(0)),
            },
            HostRequest::Query { sql, params } => match self.run("db.query", &sql, params)? {
                QueryResult::Selected { rows, .. } => Ok(HostReply::Rows(
                    rows.iter().map(|r| r.iter().map(to_js).collect()).collect(),
                )),
                QueryResult::Called {
                    result: Some(result),
                    ..
                } if matches!(*result, QueryResult::Selected { .. }) => {
                    let QueryResult::Selected { rows, .. } = *result else {
                        unreachable!()
                    };
                    Ok(HostReply::Rows(
                        rows.iter().map(|r| r.iter().map(to_js).collect()).collect(),
                    ))
                }
                _ => Err(format!(
                    "db.query: \"{sql}\" returns no rows (use db.exec for statements)"
                )),
            },
        }
    }
}

impl SqlHost<'_> {
    /// Parse, check and run one statement with `params` bound to its `?`
    /// placeholders. A nested `CALL`'s printed lines join this call's output.
    fn run(
        &mut self,
        what: &str,
        sql: &str,
        params: Vec<JsValue>,
    ) -> std::result::Result<QueryResult, String> {
        let (mut stmts, offsets) =
            crate::sql::peg_parser::parse_prepared(sql).map_err(|e| format!("{what}: {e}"))?;
        if stmts.len() != 1 {
            return Err(format!(
                "{what} takes exactly one statement, got {}",
                stmts.len()
            ));
        }
        let stmt = stmts.pop().expect("one statement");
        if let Some(refused) = crate::procedure::refused_in_body(&stmt) {
            // The SQL body's `BEGIN` hint (blocks) does not apply here.
            let refused = if matches!(stmt, crate::sql::ast::Statement::Begin) {
                "BEGIN"
            } else {
                refused
            };
            return Err(format!(
                "{what}: {refused} is not allowed inside a procedure (a CALL is \
                 statement-atomic, so transaction control and store-level statements are \
                 refused)"
            ));
        }
        // `db.query`/`db.value` read: a statement that changes data is
        // refused *before* it runs, so a script that catches the error is
        // not left with a write it did not ask `db.exec` for. (`CALL` is
        // allowed: its returned value is the rows.)
        if what == "db.query" && !returns_rows(&stmt) {
            return Err(format!(
                "db.query: \"{sql}\" does not return rows (use db.exec for statements)"
            ));
        }
        crate::prepared::check_arity(offsets.len(), params.len())
            .map_err(|e| format!("{what}: {e}"))?;
        let values: Vec<Value> = params.into_iter().map(from_js).collect();
        let slots = crate::sql::binder::ParamSlots::new(values);
        self.statements += 1;
        let result =
            crate::procedure::run_call_statement(self.db, self.db_name, slots, &stmt, |msg| {
                PinkDbError::Execution(format!("{what}: {msg}"))
            })
            .map_err(|e| e.to_string())?;
        if let QueryResult::Called { output, .. } = &result {
            self.output.extend(output.iter().cloned());
        }
        Ok(result)
    }
}

/// Statements `db.query` may run: the read-only, row-returning shapes, plus
/// `CALL` (whose returned value is the rows).
fn returns_rows(stmt: &crate::sql::ast::Statement) -> bool {
    use crate::sql::ast::Statement as S;
    matches!(
        stmt,
        S::Select { .. }
            | S::ShowTables
            | S::ShowDatabases
            | S::ShowMaterializedViews
            | S::ShowMacros
            | S::Describe { .. }
            | S::DescribeMacro { .. }
            | S::Explain { .. }
            | S::Call { .. }
    )
}

/// SQL → JavaScript. Integers and booleans/strings/doubles map directly;
/// DATE, TIMESTAMP, DECIMAL and INTERVAL arrive as their canonical text
/// (exact, and what `CAST(x AS VARCHAR)` prints).
fn to_js(v: &Value) -> JsValue {
    match v {
        Value::Null => JsValue::Null,
        Value::Boolean(b) => JsValue::Bool(*b),
        Value::Integer(i) => JsValue::Int(i64::from(*i)),
        Value::Bigint(i) => JsValue::Int(*i),
        Value::Double(x) => JsValue::Double(*x),
        Value::Varchar(s) => JsValue::Str(s.clone()),
        other => JsValue::Str(other.to_string()),
    }
}

/// JavaScript → SQL, typed like a literal: integers that fit `INTEGER` are
/// `INTEGER`, larger ones `BIGINT`.
fn from_js(v: JsValue) -> Value {
    match v {
        JsValue::Null => Value::Null,
        JsValue::Bool(b) => Value::Boolean(b),
        JsValue::Int(i) => match i32::try_from(i) {
            Ok(small) => Value::Integer(small),
            Err(_) => Value::Bigint(i),
        },
        JsValue::Double(x) => Value::Double(x),
        JsValue::Str(s) => Value::Varchar(s),
    }
}
