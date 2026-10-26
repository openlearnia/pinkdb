//! Vector similarity search (wave 107b): HNSW indexes over `FLOAT[N]` and
//! `DOUBLE[N]` columns.
//!
//! ```sql
//! CREATE INDEX items_vec ON items USING HNSW (embedding)
//!   WITH (metric = 'cosine', M = 16, ef_construction = 128);
//! SELECT id FROM items
//!   ORDER BY array_cosine_distance(embedding, [0.1, 0.2, …]) LIMIT 10;
//! CALL hnsw_compact_index('items', 'items_vec');
//! ```
//!
//! The distance functions themselves are engine core, as in DuckDB. This
//! crate is the index provider: the engine's optimizer recognizes the top-k
//! order and asks the registered provider, and the serialized graph travels
//! in the `.pink` footer, so reopening a store does not rebuild it. Persistence
//! is unconditional here (unlike DuckDB's `hnsw_enable_experimental_persistence`)
//! because the graph is written by the same atomic checkpoint as the rows and
//! is only trusted after its magic, dimensions and link ranges validate.
//!
//! Results are approximate when the index is used, exactly as in DuckDB's vss.
//! `SET VARIABLE hnsw_exact = true` forces brute force (`hnsw_ef_search`
//! changes the search width). `WHERE` filters are not pushed into the index,
//! so the optimizer leaves those queries on the exact path.

mod hnsw;

use std::sync::{Arc, OnceLock};

use hnsw::{Hnsw, Params};
use pinkdb::distance::Metric;
use pinkdb::exec::QueryResult;
use pinkdb::package::{ExtensionPackage, HostCtx, HostParam, HostStatement};
use pinkdb::vector_index::VectorIndexProvider;
use pinkdb::{ExtType, PinkDbError, Result, Value};

pub use hnsw::{Hnsw as HnswIndex, Params as HnswParams};

/// The package (and linked-registry) name: `LOAD EXTENSION 'vss'`.
pub const EXTENSION_NAME: &str = "vss";
/// The provider name `CREATE INDEX … USING HNSW` resolves.
pub const PROVIDER_NAME: &str = "hnsw";

static REGISTERED: OnceLock<()> = OnceLock::new();

/// The extension package: the HNSW provider plus the two maintenance calls.
pub fn package() -> ExtensionPackage {
    ExtensionPackage::new(EXTENSION_NAME)
        .with_vector_index_provider(Arc::new(HnswProvider))
        .with_host_statement(compact_statement())
        .with_host_statement(stats_statement())
}

/// Register the package process-wide, once. A durable reopen's
/// `LOAD EXTENSION 'vss'` replay resolves against this registry.
pub fn ensure_registered() -> Result<()> {
    if REGISTERED.get().is_some() {
        return Ok(());
    }
    pinkdb::register_extension_package(package())?;
    let _ = REGISTERED.set(());
    Ok(())
}

#[derive(Debug)]
struct HnswProvider;

impl VectorIndexProvider for HnswProvider {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn create(
        &self,
        dim: usize,
        options: &[(String, String)],
    ) -> Result<Box<dyn pinkdb::vector_index::VectorIndex>> {
        if dim == 0 {
            return Err(PinkDbError::Execution(
                "HNSW index needs a non-empty ARRAY column".to_string(),
            ));
        }
        let metric = match option(options, "metric") {
            None => Metric::L2Sq,
            Some(value) => Metric::parse(value).ok_or_else(|| {
                PinkDbError::Execution(format!(
                    "unknown HNSW metric \"{value}\": use 'l2sq', 'cosine' or 'ip'"
                ))
            })?,
        };
        let mut params = Params::default();
        if let Some(value) = option(options, "m") {
            params.m = positive(value, "M")?;
        }
        if let Some(value) = option(options, "m0") {
            params.m0 = positive(value, "M0")?;
        } else {
            params.m0 = params.m * 2;
        }
        if let Some(value) = option(options, "ef_construction") {
            params.ef_construction = positive(value, "ef_construction")?;
        }
        if let Some(value) = option(options, "ef_search") {
            params.ef_search = positive(value, "ef_search")?;
        }
        if params.m < 2 {
            return Err(PinkDbError::Execution(
                "HNSW M must be at least 2".to_string(),
            ));
        }
        if params.m0 < params.m || params.m0 > 8192 {
            return Err(PinkDbError::Execution(
                "HNSW M0 must be at least M and at most 8192".to_string(),
            ));
        }
        if params.ef_construction > 65_536 || params.ef_search > 65_536 {
            return Err(PinkDbError::Execution(
                "HNSW ef parameters must be at most 65536".to_string(),
            ));
        }
        for (key, _) in options {
            if !matches!(
                key.as_str(),
                "metric" | "m" | "m0" | "ef_construction" | "ef_search"
            ) {
                return Err(PinkDbError::Execution(format!(
                    "unknown HNSW option \"{key}\""
                )));
            }
        }
        Ok(Box::new(Hnsw::new(dim, metric, params)))
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn pinkdb::vector_index::VectorIndex>> {
        Ok(Box::new(Hnsw::from_serialized(bytes)?))
    }
}

fn option<'a>(options: &'a [(String, String)], key: &str) -> Option<&'a str> {
    options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.as_str())
}

fn positive(value: &str, key: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            PinkDbError::Execution(format!("HNSW option {key} must be a positive integer"))
        })
}

fn compact_statement() -> HostStatement {
    HostStatement::new(
        "hnsw_compact_index",
        vec![
            HostParam::new("input_table", ExtType::Varchar),
            HostParam::new("index_name", ExtType::Varchar),
        ],
        compact,
    )
}

fn compact(ctx: &mut HostCtx<'_>, args: &[Value]) -> Result<QueryResult> {
    let table = varchar(&args[0], "hnsw_compact_index")?;
    let index = varchar(&args[1], "hnsw_compact_index")?;
    let db = ctx.db_name().to_string();
    ctx.db_mut().compact_vector_index_in(&db, table, index)?;
    Ok(QueryResult::Called {
        name: "hnsw_compact_index".to_string(),
        statements: 1,
        result: None,
        output: vec![format!("compacted {index} on {table}")],
    })
}

fn stats_statement() -> HostStatement {
    HostStatement::new(
        "hnsw_index_stats",
        vec![
            HostParam::new("input_table", ExtType::Varchar),
            HostParam::new("index_name", ExtType::Varchar),
        ],
        stats,
    )
}

fn stats(ctx: &mut HostCtx<'_>, args: &[Value]) -> Result<QueryResult> {
    let table = varchar(&args[0], "hnsw_index_stats")?;
    let index = varchar(&args[1], "hnsw_index_stats")?;
    let s = ctx
        .db()
        .vector_index_stats_in(ctx.db_name(), table, index)?;
    Ok(QueryResult::Called {
        name: "hnsw_index_stats".to_string(),
        statements: 0,
        result: None,
        output: vec![
            format!("nodes: {}", s.nodes),
            format!("live: {}", s.live),
            format!("tombstones: {}", s.tombstones),
            format!("max_level: {}", s.max_level),
        ],
    })
}

fn varchar<'a>(value: &'a Value, what: &str) -> Result<&'a str> {
    match value {
        Value::Varchar(text) => Ok(text),
        other => Err(PinkDbError::Execution(format!(
            "{what}: expected VARCHAR, got {}",
            other
                .data_type()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        ))),
    }
}
