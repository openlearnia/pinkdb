//! The `embeddings` extension: `embed(text)` returns `text`'s embedding as
//! `FLOAT[N]`, from an OpenAI-compatible embeddings endpoint.
//!
//! The width `N` is `PINKDB_EMBEDDINGS_DIMENSIONS` (default
//! [`DEFAULT_DIMENSIONS`]), read when the package is registered: the binder
//! needs a static result type before any request exists. The same width is
//! sent as the request's `dimensions` parameter, and a response of another
//! length is an error naming the setting. A changed width needs a restart,
//! because a column built by `embed()` keeps the type it was bound with.
//!
//! `embed()` is a volatile, host-dependent function. It is a `DynamicScalar`
//! plan node, so the binder, the optimizer and `EXPLAIN` never evaluate it —
//! only a chunk that actually streams rows does — and it is cached by
//! (endpoint, model, width, text). It is deliberately *not* registered with
//! the engine's `volatile` ABI flag: that flag refuses the call in any
//! WAL-logged statement, and `UPDATE docs SET v = embed(body)` is the main
//! use. A statement the engine WAL-logs instead carries the call's result in
//! the record's replay memo (the `external_io` marker), so a crash replay
//! answers from the record with no key and no provider.
//!
//! A call is external I/O to a paid service, so the package marks it
//! `external_io`: the MCP/agent policy refuses it unless the server permits
//! external services (`allow-files`, the same switch as httpfs). The gate is
//! enforced at the call site, so a batch, a macro, a view, a trigger or a
//! `CALL`/`DO` body cannot reach the provider under the read-only default.
//!
//! The host evaluates a package host scalar one chunk at a time (wave 107c's
//! engine hook), so rows are deduplicated within a chunk and sent in batches:
//! a thousand rows with three hundred distinct texts cost three requests at
//! the default batch size of 128.

mod cache;
mod client;

use std::sync::Arc;

pub use client::{DEFAULT_BATCH_SIZE, DEFAULT_DIMENSIONS, KEY_VARIABLE};

use pinkdb::array::{ArrayElems, ArrayValue};
use pinkdb::package::{ExtensionPackage, HostParam, HostScalar};
use pinkdb::{ArrayElem, DataType, ExtType, PinkDbError, Result, Value, Vector};

/// Registry name: what `LOAD EXTENSION 'embeddings'` replays.
pub const EXTENSION_NAME: &str = "embeddings";

/// Build the package: the `embed(text)` host scalar with this process's
/// declared width.
pub fn package() -> ExtensionPackage {
    let dimensions = dimensions();
    ExtensionPackage::new(EXTENSION_NAME).with_host_scalar(
        HostScalar::batched(
            "embed",
            vec![HostParam::new("text", ExtType::Varchar)],
            ExtType::ArrayFloat { len: dimensions },
            move |_db, args| embed(args, dimensions),
        )
        .external_io(),
    )
}

/// Publish the package process-wide, before a store can load it.
pub fn register_package() -> Result<()> {
    pinkdb::register_extension_package(package())
}

/// Register the package if it is not registered yet; errors are the
/// registry's.
pub fn ensure_registered() -> Result<()> {
    register_package()
}

/// The width `embed()` declares: `PINKDB_EMBEDDINGS_DIMENSIONS`, else the
/// default. A malformed value is warned about here and rejected again on the
/// first call, so a typo cannot silently shrink the declared type.
fn dimensions() -> u32 {
    match client::dimensions_from_env() {
        Ok(dimensions) => dimensions,
        Err(error) => {
            eprintln!("warning: {error}; embed() declares FLOAT[{DEFAULT_DIMENSIONS}]");
            DEFAULT_DIMENSIONS
        }
    }
}

/// One chunk of rows: missing texts are null (no request), repeated texts are
/// embedded once, and the result is a `FLOAT[N]` array per row.
fn embed(args: &[Vector], dimensions: u32) -> Result<Vector> {
    let [text] = args else {
        return Err(PinkDbError::Execution(
            "embed() takes one argument".to_string(),
        ));
    };
    let config = client::Config::from_env().map_err(PinkDbError::Execution)?;
    if config.dimensions != dimensions {
        return Err(PinkDbError::Execution(format!(
            "PINKDB_EMBEDDINGS_DIMENSIONS is now {}, but embed() is bound as FLOAT[{dimensions}]; \
             restart pinkdb to change the width",
            config.dimensions
        )));
    }
    let mut inputs = Vec::with_capacity(text.len());
    let mut input_of_row = Vec::with_capacity(text.len());
    for row in 0..text.len() {
        match text.get(row) {
            Value::Null => input_of_row.push(None),
            Value::Varchar(value) => {
                input_of_row.push(Some(inputs.len()));
                inputs.push(value);
            }
            other => {
                return Err(PinkDbError::Execution(format!(
                    "embed() expects a VARCHAR argument, got {other:?}"
                )));
            }
        }
    }
    let vectors = client::Client::new(config)
        .embed(&inputs)
        .map_err(PinkDbError::Execution)?;
    let mut out = Vector::new_empty(DataType::Array {
        elem: ArrayElem::Float,
        len: dimensions,
    });
    for input in input_of_row {
        match input {
            None => out.push_null()?,
            Some(index) => {
                let array = ArrayValue::new(
                    ArrayElem::Float,
                    dimensions,
                    ArrayElems::Floats32(vectors[index].as_ref().to_vec()),
                    None,
                )?;
                out.push_valid(Value::Array(Arc::new(array)))?;
            }
        }
    }
    Ok(out)
}
