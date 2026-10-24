//! Opening a lake's catalog database (wave 103).
//!
//! A lake catalog is always a database: a durable `pink:<path>`, a
//! `sqlite:<path>`, or a `postgres://…` URL, all on the shared DuckLake v1.1
//! tables. This module is only the feature-gated constructor; the SQL lives
//! in [`crate::metadata`].

use crate::storage::Location;
use crate::{LakeError, Result};

pub(crate) mod url;

pub(crate) use crate::metadata::store::SqlMetadataStore;

/// Open the catalog's metadata store. The `.pink` backend is always built
/// (pinkdb is a dependency); SQLite and Postgres are feature-gated. A keyed
/// lake passes its key to the `.pink` backend (the engine's `PINKENC1`
/// at-rest encryption); SQLite/Postgres catalogs are protected by whoever
/// runs them.
///
/// `None` means "this build cannot serve this catalog location" (the caller
/// reports the missing feature).
pub(crate) fn open_metadata_store_keyed(
    catalog_url: &str,
    location: &Location,
    create: bool,
    key: Option<&crate::EncryptionKey>,
) -> Result<Option<SqlMetadataStore>> {
    match location {
        Location::Pink(path) => Ok(Some(crate::metadata::pink::open_keyed(
            catalog_url,
            path,
            create,
            key.and_then(|key| key.engine_key()),
        )?)),
        #[cfg(feature = "catalog-sqlite")]
        Location::Sqlite(path) => Ok(Some(crate::metadata::sqlite::open(
            catalog_url,
            path,
            create,
        )?)),
        #[cfg(not(feature = "catalog-sqlite"))]
        Location::Sqlite(_) => Ok(None),
        #[cfg(feature = "catalog-postgres")]
        Location::Postgres(_) => Ok(Some(crate::metadata::postgres::open(catalog_url, create)?)),
        #[cfg(not(feature = "catalog-postgres"))]
        Location::Postgres(_) => Ok(None),
        Location::Local(_) | Location::S3 { .. } => Err(LakeError::BadLocation {
            location: url::redact_location(catalog_url),
            message: "this is a data location, not a lake catalog".to_string(),
        }),
    }
}
