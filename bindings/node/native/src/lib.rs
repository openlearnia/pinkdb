//! Node.js binding for pinkdb: a thin N-API addon over the C ABI in
//! `crates/pinkdb-ffi`.
//!
//! This crate only marshals. The public JavaScript API lives in `index.js`,
//! which owns the `PinkdbError` class and the row iterators; everything here
//! hands back plain JS values (numbers, strings, `BigInt`, `null`) so the
//! wrapper has nothing left to decode.
//!
//! Type mapping (documented in `bindings/node/README.md`):
//! BOOLEAN → boolean, INTEGER → number, BIGINT → BigInt, DOUBLE → number,
//! VARCHAR/DECIMAL/INTERVAL/HUGEINT/UHUGEINT → string, DATE/TIMESTAMP → string
//! in the engine's
//! canonical form (a JS `Date` cannot carry a µs timestamp or a calendar date
//! without a timezone assumption), DOUBLE-based ARRAY → `Float64Array`,
//! FLOAT-based ARRAY → `Float32Array`.

use std::ffi::{CStr, c_char};
use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};

use napi::bindgen_prelude::*;
use napi::{Env, JsUnknown, ValueType};
use napi_derive::napi;

use pinkdb_ffi as ffi;
use pinkdb_ffi::{
    PINKDB_TYPE_ARRAY, PINKDB_TYPE_ARRAY_FLOAT, PINKDB_TYPE_BIGINT, PINKDB_TYPE_BOOLEAN,
    PINKDB_TYPE_DATE,
    PINKDB_TYPE_DECIMAL, PINKDB_TYPE_DOUBLE, PINKDB_TYPE_HUGEINT, PINKDB_TYPE_INTEGER,
    PINKDB_TYPE_INTERVAL, PINKDB_TYPE_NULL, PINKDB_TYPE_TIMESTAMP, PINKDB_TYPE_UHUGEINT,
    PINKDB_TYPE_VARCHAR, PinkHandle,
    PinkPrepared, PinkResult, PinkValue, array_tag_len,
};

const KIND_NAMES: [&str; 27] = [
    "selected",
    "created",
    "inserted",
    "updated",
    "deleted",
    "began",
    "committed",
    "rolledback",
    "created_view",
    "dropped_table",
    "dropped_view",
    "created_database",
    "dropped_database",
    "used",
    "loaded_extension",
    "checkpointed",
    "altered_table",
    "created_user",
    "altered_user",
    "dropped_user",
    "created_role",
    "dropped_role",
    "granted",
    "revoked",
    "set_variable",
    "created_schema",
    "dropped_schema",
];

// Indices follow the C ABI's PINKDB_TYPE_* tags; 10 is VARIANT, 11 and 12 are
// the ARRAY kinds (named from their tag's upper bits before this table is
// consulted), 13 and 14 are the 128-bit integers.
const TYPE_NAMES: [&str; 15] = [
    "null",
    "boolean",
    "integer",
    "bigint",
    "double",
    "varchar",
    "date",
    "timestamp",
    "decimal",
    "interval",
    "variant",
    "double[]",
    "float[]",
    "hugeint",
    "uhugeint",
];

fn kind_name(kind: i32) -> &'static str {
    KIND_NAMES.get(kind as usize).copied().unwrap_or("unknown")
}

fn type_name(tag: i32) -> String {
    // An ARRAY packs its element count above the type byte: type 11 for
    // `double` elements, 12 for 4-byte `float` ones.
    if tag & 0xff == PINKDB_TYPE_ARRAY || tag & 0xff == PINKDB_TYPE_ARRAY_FLOAT {
        let elem = if tag & 0xff == PINKDB_TYPE_ARRAY_FLOAT {
            "float"
        } else {
            "double"
        };
        return format!("{elem}[{}]", array_tag_len(tag));
    }
    TYPE_NAMES
        .get(tag as usize)
        .copied()
        .unwrap_or("unknown")
        .to_owned()
}

/// Turn the C ABI's thread-local error into a napi error whose message is
/// `<class>\x01<message>`; `index.js` splits it back into a `PinkdbError`.
fn ffi_error() -> Error {
    // SAFETY: both pointers are thread-local NUL-terminated strings owned by
    // the ABI and valid until the next FFI call on this thread.
    let (class, message) = unsafe {
        let message = ffi::pinkdb_last_error();
        let class = ffi::pinkdb_last_error_class();
        (
            if class.is_null() {
                "internal".to_owned()
            } else {
                CStr::from_ptr(class).to_string_lossy().into_owned()
            },
            if message.is_null() {
                "pinkdb call failed".to_owned()
            } else {
                CStr::from_ptr(message).to_string_lossy().into_owned()
            },
        )
    };
    Error::new(Status::GenericFailure, format!("{class}\u{1}{message}"))
}

/// An error raised by this addon itself rather than by the ABI.
fn local_error(class: &str, message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, format!("{class}\u{1}{}", message.into()))
}

fn text_of(value: &PinkValue) -> Result<String> {
    if value.text_ptr.is_null() {
        return Ok(String::new());
    }
    // SAFETY: the ABI guarantees `text_len` readable bytes at `text_ptr`, owned
    // by the result until it is freed.
    let bytes = unsafe { slice::from_raw_parts(value.text_ptr as *const u8, value.text_len) };
    String::from_utf8(bytes.to_vec())
        .map_err(|_| local_error("internal", "non-UTF-8 text from the pinkdb ABI"))
}

fn value_to_js(env: &Env, value: &PinkValue) -> Result<JsUnknown> {
    if value.is_null != 0 {
        return Ok(env.get_null()?.into_unknown());
    }
    let js = match value.type_tag {
        PINKDB_TYPE_BOOLEAN => env.get_boolean(value.int64 != 0)?.into_unknown(),
        PINKDB_TYPE_INTEGER => env.create_int32(value.int64 as i32)?.into_unknown(),
        PINKDB_TYPE_BIGINT => {
            let bigint: napi::JsBigInt = env.create_bigint_from_i64(value.int64)?;
            bigint.into_unknown()?
        }
        PINKDB_TYPE_DOUBLE => env.create_double(value.double)?.into_unknown(),
        PINKDB_TYPE_VARCHAR
        | PINKDB_TYPE_DECIMAL
        | PINKDB_TYPE_INTERVAL
        | PINKDB_TYPE_HUGEINT
        | PINKDB_TYPE_UHUGEINT => {
            env.create_string_from_std(text_of(value)?)?.into_unknown()
        }
        PINKDB_TYPE_DATE => {
            let days = i32::try_from(value.int64)
                .map_err(|_| local_error("internal", "DATE out of range"))?;
            env.create_string_from_std(pinkdb::types::format_date(days))?
                .into_unknown()
        }
        PINKDB_TYPE_TIMESTAMP => env
            .create_string_from_std(pinkdb::types::format_timestamp(value.int64))?
            .into_unknown(),
        // The payload is the engine's contiguous element buffer, so one copy
        // into a `Float64Array` is the whole conversion.
        tag if tag & 0xff == PINKDB_TYPE_ARRAY => {
            let values: Vec<f64> = if value.text_len == 0 {
                Vec::new()
            } else {
                // SAFETY: the ABI guarantees `text_len` readable bytes at
                // `text_ptr`, owned by the result until it is freed.
                let bytes =
                    unsafe { slice::from_raw_parts(value.text_ptr as *const u8, value.text_len) };
                bytes
                    .chunks_exact(8)
                    .map(|c| f64::from_le_bytes(c.try_into().expect("8-byte chunk")))
                    .collect()
            };
            // `Float64Array` is the natural JS shape for a vector column; the
            // napi2 helper that turns one into a `JsUnknown` needs a raw value.
            let raw = unsafe {
                <Float64Array as napi::bindgen_prelude::ToNapiValue>::to_napi_value(
                    env.raw(),
                    Float64Array::new(values),
                )?
            };
            unsafe { <JsUnknown as napi::NapiValue>::from_raw_unchecked(env.raw(), raw) }
        }
        // The 4-byte counterpart, `Float32Array`.
        tag if tag & 0xff == PINKDB_TYPE_ARRAY_FLOAT => {
            let values: Vec<f32> = if value.text_len == 0 {
                Vec::new()
            } else {
                // SAFETY: the ABI guarantees `text_len` readable bytes at
                // `text_ptr`, owned by the result until it is freed.
                let bytes =
                    unsafe { slice::from_raw_parts(value.text_ptr as *const u8, value.text_len) };
                bytes
                    .chunks_exact(4)
                    .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                    .collect()
            };
            let raw = unsafe {
                <Float32Array as napi::bindgen_prelude::ToNapiValue>::to_napi_value(
                    env.raw(),
                    Float32Array::new(values),
                )?
            };
            unsafe { <JsUnknown as napi::NapiValue>::from_raw_unchecked(env.raw(), raw) }
        }
        other => return Err(local_error("internal", format!("unknown pinkdb type tag {other}"))),
    };
    Ok(js)
}

fn column_names(result: *const PinkResult, statement: usize) -> Result<Vec<String>> {
    let count = ffi::pinkdb_result_column_count(result, statement);
    let mut names = Vec::with_capacity(count);
    for column in 0..count {
        let name = ffi::pinkdb_result_column_name(result, statement, column);
        if name.is_null() {
            names.push(String::new());
        } else {
            // SAFETY: the ABI returns a NUL-terminated string owned by the result.
            names.push(unsafe { CStr::from_ptr(name) }.to_string_lossy().into_owned());
        }
    }
    Ok(names)
}

fn column_types(result: *const PinkResult, statement: usize) -> Vec<String> {
    let count = ffi::pinkdb_result_column_count(result, statement);
    (0..count)
        .map(|column| type_name(ffi::pinkdb_result_column_type(result, statement, column)))
        .collect()
}

fn rows_to_js(
    env: &Env,
    result: *const PinkResult,
    statement: usize,
    names: &[String],
) -> Result<Vec<JsUnknown>> {
    let rows = ffi::pinkdb_result_row_count(result, statement);
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut object = env.create_object()?;
        for (column, name) in names.iter().enumerate() {
            let value = ffi::pinkdb_result_value(result, statement, row, column);
            object.set_named_property(name, value_to_js(env, &value)?)?;
        }
        out.push(object.into_unknown());
    }
    Ok(out)
}

/// One statement result as `{kind, name, affected, columns, columnTypes,
/// rowCount, rows?}`; `rows` is present only for SELECT statements and only
/// when `include_rows` is set.
fn result_to_js(
    env: &Env,
    result: *const PinkResult,
    statement: usize,
    include_rows: bool,
) -> Result<JsUnknown> {
    let kind = ffi::pinkdb_result_kind(result, statement);
    let names = column_names(result, statement)?;
    let mut object = env.create_object()?;
    object.set_named_property("kind", kind_name(kind))?;
    object.set_named_property("columnTypes", column_types(result, statement))?;
    object.set_named_property(
        "rowCount",
        u32::try_from(ffi::pinkdb_result_row_count(result, statement)).unwrap_or(u32::MAX),
    )?;
    object.set_named_property(
        "affected",
        u32::try_from(ffi::pinkdb_result_affected(result, statement)).unwrap_or(u32::MAX),
    )?;
    let name = ffi::pinkdb_result_name(result, statement);
    if name.is_null() {
        object.set_named_property("name", env.get_null()?)?;
    } else {
        // SAFETY: NUL-terminated string owned by the result.
        let name = unsafe { CStr::from_ptr(name) }.to_string_lossy().into_owned();
        object.set_named_property("name", name)?;
    }
    if include_rows && kind == ffi::PINKDB_STMT_SELECTED {
        let rows = rows_to_js(env, result, statement, &names)?;
        object.set_named_property("rows", rows)?;
    }
    object.set_named_property("columns", names)?;
    Ok(object.into_unknown())
}

fn results_to_js(env: &Env, result: *mut PinkResult, include_rows: bool) -> Result<Vec<JsUnknown>> {
    let count = ffi::pinkdb_result_statement_count(result);
    let mut out = Vec::with_capacity(count);
    for statement in 0..count {
        out.push(result_to_js(env, result, statement, include_rows)?);
    }
    Ok(out)
}

/// A bound parameter, kept as owned Rust data until `to_ffi` builds the
/// `PinkValue`s that point into it. A BNUMBER is INTEGER when it fits in i32,
/// BIGINT when it is a safe integer, DOUBLE otherwise.
struct Param {
    tag: i32,
    int64: i64,
    double: f64,
    text: Option<String>,
}

fn param_from_js(value: &JsUnknown) -> Result<Param> {
    let param = match value.get_type()? {
        ValueType::Undefined | ValueType::Null => Param {
            tag: PINKDB_TYPE_NULL,
            int64: 0,
            double: 0.0,
            text: None,
        },
        ValueType::Boolean => Param {
            tag: PINKDB_TYPE_BOOLEAN,
            int64: i64::from(unsafe { value.cast::<napi::JsBoolean>() }.get_value()?),
            double: 0.0,
            text: None,
        },
        ValueType::Number => {
            let number = unsafe { value.cast::<napi::JsNumber>() }.get_double()?;
            if number.fract() == 0.0 && (i32::MIN as f64..=i32::MAX as f64).contains(&number) {
                Param {
                    tag: PINKDB_TYPE_INTEGER,
                    int64: number as i64,
                    double: 0.0,
                    text: None,
                }
            } else if number.fract() == 0.0 && (-(2f64.powi(63))..2f64.powi(63)).contains(&number)
            {
                Param {
                    tag: ffi::PINKDB_TYPE_BIGINT,
                    int64: number as i64,
                    double: 0.0,
                    text: None,
                }
            } else {
                Param {
                    tag: PINKDB_TYPE_DOUBLE,
                    int64: 0,
                    double: number,
                    text: None,
                }
            }
        }
        ValueType::BigInt => {
            let (number, lossless) = unsafe { value.cast::<napi::JsBigInt>() }.get_i64()?;
            if !lossless {
                return Err(local_error(
                    "invalid_argument",
                    "BIGINT parameter does not fit in 64 bits",
                ));
            }
            Param {
                tag: ffi::PINKDB_TYPE_BIGINT,
                int64: number,
                double: 0.0,
                text: None,
            }
        }
        ValueType::String => {
            let utf8 = unsafe { value.cast::<napi::JsString>() }.into_utf8()?;
            Param {
                tag: PINKDB_TYPE_VARCHAR,
                int64: 0,
                double: 0.0,
                text: Some(utf8.as_str()?.to_owned()),
            }
        }
        other => {
            return Err(local_error(
                "invalid_argument",
                format!(
                    "unsupported parameter type {other}; pass null, a boolean, a number, a BigInt or a string (DATE/TIMESTAMP as canonical strings)"
                ),
            ));
        }
    };
    Ok(param)
}

/// Owned parameters plus the `PinkValue`s that point into them. The two must
/// stay together: `ffi` borrows the strings, so a caller that let the `Params`
/// drop first would hand the ABI dangling text pointers.
struct Params {
    values: Vec<Param>,
}

impl Params {
    fn from_js(params: Option<Vec<JsUnknown>>) -> Result<Self> {
        let mut values = Vec::new();
        for value in params.unwrap_or_default() {
            values.push(param_from_js(&value)?);
        }
        Ok(Self { values })
    }

    fn ffi(&self) -> Vec<PinkValue> {
        self.values
            .iter()
            .map(|param| PinkValue {
                type_tag: param.tag,
                is_null: i32::from(param.tag == PINKDB_TYPE_NULL),
                int64: param.int64,
                double: param.double,
                text_ptr: param
                    .text
                    .as_ref()
                    .map_or(ptr::null(), |text| text.as_ptr() as *const c_char),
                text_len: param.text.as_ref().map_or(0, String::len),
            })
            .collect()
    }
}

/// The handle plus the closed flag, shared between a database and every
/// prepared statement made from it, so closing the database cannot leave a
/// prepared statement holding a dangling handle.
struct DbState {
    handle: Mutex<*mut PinkHandle>,
}

unsafe impl Send for DbState {}
unsafe impl Sync for DbState {}

impl DbState {
    fn handle(&self) -> Result<*mut PinkHandle> {
        let handle = *self.handle.lock().unwrap_or_else(|e| e.into_inner());
        if handle.is_null() {
            Err(local_error("invalid_argument", "database is closed"))
        } else {
            Ok(handle)
        }
    }
}

impl Drop for DbState {
    fn drop(&mut self) {
        let handle = *self.handle.get_mut().unwrap_or_else(|e| e.into_inner());
        if !handle.is_null() {
            ffi::pinkdb_close(handle);
        }
    }
}

#[napi]
pub fn abi_version() -> u32 {
    ffi::pinkdb_abi_version()
}

#[napi]
pub struct NativeDatabase {
    state: Arc<DbState>,
}

#[napi]
impl NativeDatabase {
    #[napi]
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<Vec<JsUnknown>> {
        let result = if params.is_some() {
            let prepared = self.prepare_native(&sql)?;
            let result = prepared.execute(params)?;
            result
        } else {
            let handle = self.state.handle()?;
            let result = ffi::pinkdb_execute(handle, sql.as_ptr() as *const c_char, sql.len());
            if result.is_null() {
                return Err(ffi_error());
            }
            result
        };
        // SAFETY: the result was just produced by the ABI and is freed here.
        let out = results_to_js(&env, result, true);
        ffi::pinkdb_result_free(result);
        out
    }

    #[napi]
    pub fn query(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<Vec<JsUnknown>> {
        let prepared = self.prepare_native(&sql)?;
        let result = prepared.execute(params)?;
        let names = column_names(result, 0)?;
        // SAFETY: single-statement prepared result, freed here.
        let out = rows_to_js(&env, result, 0, &names);
        ffi::pinkdb_result_free(result);
        out
    }

    #[napi]
    pub fn prepare(&self, sql: String) -> Result<NativePrepared> {
        self.prepare_native(&sql)
    }

    #[napi]
    pub fn close(&self) {
        let mut handle = self.state.handle.lock().unwrap_or_else(|e| e.into_inner());
        if !handle.is_null() {
            ffi::pinkdb_close(*handle);
            *handle = ptr::null_mut();
        }
    }
}

impl NativeDatabase {
    fn prepare_native(&self, sql: &str) -> Result<NativePrepared> {
        let handle = self.state.handle()?;
        let ptr = ffi::pinkdb_prepare(handle, sql.as_ptr() as *const c_char, sql.len());
        if ptr.is_null() {
            Err(ffi_error())
        } else {
            Ok(NativePrepared {
                state: Arc::clone(&self.state),
                ptr,
            })
        }
    }
}

#[napi]
pub fn open(path: Option<String>) -> Result<NativeDatabase> {
    let handle = match path.as_deref() {
        None | Some("") => ffi::pinkdb_open_memory(),
        Some(path) => match std::ffi::CString::new(path) {
            Ok(c_path) => ffi::pinkdb_open(c_path.as_ptr()),
            Err(_) => return Err(local_error("invalid_argument", "database path contains a NUL byte")),
        },
    };
    if handle.is_null() {
        return Err(ffi_error());
    }
    Ok(NativeDatabase {
        state: Arc::new(DbState {
            handle: Mutex::new(handle),
        }),
    })
}

#[napi]
pub struct NativePrepared {
    state: Arc<DbState>,
    ptr: *mut PinkPrepared,
}

unsafe impl Send for NativePrepared {}

#[napi]
impl NativePrepared {
    #[napi]
    pub fn parameter_count(&self) -> u32 {
        u32::try_from(ffi::pinkdb_prepared_parameter_count(self.ptr)).unwrap_or(u32::MAX)
    }

    #[napi]
    pub fn run(&self, env: Env, params: Option<Vec<JsUnknown>>) -> Result<JsUnknown> {
        let result = self.execute(params)?;
        // SAFETY: just produced by `execute`, freed here.
        let out = result_to_js(&env, result, 0, false);
        ffi::pinkdb_result_free(result);
        out
    }

    #[napi]
    pub fn all(&self, env: Env, params: Option<Vec<JsUnknown>>) -> Result<Vec<JsUnknown>> {
        let result = self.execute(params)?;
        let names = column_names(result, 0)?;
        // SAFETY: single-statement prepared result, freed here.
        let out = rows_to_js(&env, result, 0, &names);
        ffi::pinkdb_result_free(result);
        out
    }

    #[napi]
    pub fn iterate(&self, env: Env, params: Option<Vec<JsUnknown>>) -> Result<NativeRowStream> {
        let _ = env;
        let result = self.execute(params)?;
        NativeRowStream::new(result)
    }

    #[napi]
    pub fn close(&mut self) {
        if !self.ptr.is_null() {
            ffi::pinkdb_prepared_free(self.ptr);
            self.ptr = ptr::null_mut();
        }
    }
}

impl NativePrepared {
    fn execute(&self, params: Option<Vec<JsUnknown>>) -> Result<*mut PinkResult> {
        if self.ptr.is_null() {
            return Err(local_error("invalid_argument", "prepared statement is closed"));
        }
        let handle = self.state.handle()?;
        let params = Params::from_js(params)?;
        let values = params.ffi();
        let result = ffi::pinkdb_execute_prepared(handle, self.ptr, values.as_ptr(), values.len());
        if result.is_null() {
            Err(ffi_error())
        } else {
            Ok(result)
        }
    }
}

impl Drop for NativePrepared {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            ffi::pinkdb_prepared_free(self.ptr);
        }
    }
}

#[napi]
pub struct NativeRowStream {
    result: *mut PinkResult,
    names: Vec<String>,
    row: usize,
    total: usize,
}

unsafe impl Send for NativeRowStream {}

impl NativeRowStream {
    fn new(result: *mut PinkResult) -> Result<Self> {
        let names = column_names(result, 0)?;
        let total = ffi::pinkdb_result_row_count(result, 0);
        Ok(Self {
            result,
            names,
            row: 0,
            total,
        })
    }
}

#[napi]
impl NativeRowStream {
    #[napi]
    pub fn next(&mut self, env: Env) -> Result<Option<JsUnknown>> {
        if self.result.is_null() || self.row >= self.total {
            self.close();
            return Ok(None);
        }
        let mut object = env.create_object()?;
        for (column, name) in self.names.iter().enumerate() {
            let value = ffi::pinkdb_result_value(self.result, 0, self.row, column);
            object.set_named_property(name, value_to_js(&env, &value)?)?;
        }
        self.row += 1;
        Ok(Some(object.into_unknown()))
    }

    #[napi]
    pub fn close(&mut self) {
        if !self.result.is_null() {
            ffi::pinkdb_result_free(self.result);
            self.result = ptr::null_mut();
        }
    }
}

impl Drop for NativeRowStream {
    fn drop(&mut self) {
        self.close();
    }
}
