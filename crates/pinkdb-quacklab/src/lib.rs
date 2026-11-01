//! QuackLab's HTTP surface (wave 138): what grain's QuackLab service speaks,
//! over a pinkdb store.
//!
//! One `axum` router over a [`pinkdb::Database`], built like the console
//! (`pinkdb-server-ui`): [`QuacklabServer::bind`] owns the listener,
//! [`QuacklabServer::spawn`] runs the router on its own tokio runtime thread,
//! and [`QuacklabHandle`] shuts it down. `pinkdb-server --quacklab-bind`
//! wires one to the same store the binary protocol serves.
//!
//! The bodies are DuckDB's, byte for byte where pinkdb can produce them
//! (`docs/wave-138-plan.md`): the last statement's result in DuckDB's shapes,
//! Go's cell encoding (DECIMAL as `{"Width","Scale","Value"}`, DATE and
//! TIMESTAMP as RFC 3339, INTERVAL as a day/month/micro count, UUIDs as
//! canonical text), the row cap, and `{"error":{"code","message"}}` with
//! DuckDB's error prefix chosen by pinkdb's error kind.
//!
//! Statements run on a fresh `Connection` per request, positioned on the
//! warehouse database (`DUCKLAKE_ALIAS`, `USE "<alias>"`), and a transaction a
//! script leaves open is rolled back before the response.
//!
//! The exclusive write lease (`/write-lease/*`) freezes the warehouse for one
//! holder: [`Warehouse`] is what a lease closes and reopens, and 138c gives
//! the lake one that detaches and re-attaches its catalog. `GRAIN_DUCKDB_SANDBOX`
//! applies the one isolation policy pinkdb has (no `LOAD`/`INSTALL`).
//!
//! Two cells of DuckDB's table have no counterpart here: `TIME`, because
//! pinkdb has no `TIME` type (so no cell of it can reach the encoder), and
//! `LIST`/`STRUCT`, which pinkdb does not have either (a pinkdb `ARRAY` cell
//! prints as a JSON array, a `VARIANT` cell as its own JSON). A `DOUBLE` that
//! Go's `json.Marshal` refuses (NaN, ±Infinity) is a `500 encode_error`, as
//! it is in DuckDB's driver.

mod app;
mod encode;
mod errors;
mod lease;
mod sandbox;
mod server;

pub use app::{router, router_with_warehouse};
pub use lease::Warehouse;
pub use server::{
    DEFAULT_BIND, DEFAULT_ROW_LIMIT, DEFAULT_WAREHOUSE, MAX_ROW_LIMIT, QuacklabError,
    QuacklabHandle, QuacklabOptions, QuacklabServer, parse_row_limit,
};
