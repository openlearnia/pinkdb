//! Catalog migration between the object-store layout and a SQL catalog
//! (wave 96a).
//!
//! [`Lake::migrate_catalog`] copies every metadata object **byte-for-byte**
//! (an encrypted lake's sealed blobs stay sealed) into one target publish
//! scope; data files are never read, moved or rewritten. The three legal
//! directions are:
//!
//! - object store → SQL: the SQL catalog records the source location as its
//!   data location, because the data files stay exactly where they were;
//! - SQL → object store: the target must be the lake's own data location, so
//!   the metadata objects land next to the data they describe;
//! - SQL → SQL: the data location is copied (or overridden with `data` when
//!   the data was moved by hand).
//!
//! Each direction retires the source only **after** the target has been
//! copied and verified:
//!
//! - a SQL source gets `pink_lake_meta.moved_to`;
//! - an object-store source gets `moved.json` (target URL, password redacted)
//!   and then loses `catalog.json` last. Snapshots stay, so a hand recovery
//!   can rebuild the source if the target is lost.
//!
//! Opening a retired location reports [`LakeError::CatalogMoved`] instead of
//! `NotALake`. The object-store check happens only on the missing-catalog
//! error path, so ordinary opens pay nothing.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::catalog_store::{self, CatalogStore};
use crate::local::CATALOG_FILE;
use crate::storage::{Location, Storage};
use crate::{
    HIGHEST_FILE, LAKE_FILE, Lake, LakeError, MOVED_FILE, Result, is_lake_id,
    parse_manifest_binding, redact_location, time,
};

/// What [`Lake::migrate_catalog`] copied and whether the source was retired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateReport {
    /// Metadata objects copied (every `lake.json`/`catalog.json`/`highest.json`
    /// / snapshot / commit / replication object).
    pub objects: usize,
    /// Their total stored size (the sealed size for an encrypted lake).
    pub bytes: u64,
    /// Whether the source was retired (a `moved.json` marker or a
    /// `pink_lake_meta.moved_to` row was written).
    pub source_retired: bool,
}

/// The plaintext retirement marker left at an object-store source's root.
#[derive(Debug, Serialize, Deserialize)]
struct MovedMarker {
    moved_to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lake_id: Option<String>,
    moved_at: String,
}

/// The source side of a migration: either a SQL catalog store or the ordinary
/// object-store layout.
enum Source {
    Sql {
        store: Arc<dyn CatalogStore>,
        lake_id: Option<String>,
        data_location: Option<String>,
        objects: Vec<(String, Vec<u8>)>,
    },
    Object {
        storage: Arc<dyn Storage>,
        lake_id: Option<String>,
        objects: Vec<(String, Vec<u8>)>,
    },
}

impl Source {
    fn is_sql(&self) -> bool {
        matches!(self, Source::Sql { .. })
    }

    fn lake_id(&self) -> Option<&str> {
        match self {
            Source::Sql { lake_id, .. } | Source::Object { lake_id, .. } => lake_id.as_deref(),
        }
    }

    fn objects(&self) -> &[(String, Vec<u8>)] {
        match self {
            Source::Sql { objects, .. } | Source::Object { objects, .. } => objects,
        }
    }

    fn data_location(&self) -> Option<&str> {
        match self {
            Source::Sql { data_location, .. } => data_location.as_deref(),
            Source::Object { .. } => None,
        }
    }
}

/// The target side: a SQL catalog store or the ordinary object-store layout.
enum Target {
    Sql(Arc<dyn CatalogStore>),
    Object(Arc<dyn Storage>),
}

impl Target {
    fn storage(&self) -> &dyn Storage {
        match self {
            Target::Sql(store) => &**store,
            Target::Object(storage) => &**storage,
        }
    }

    fn as_catalog(&self) -> Option<&dyn CatalogStore> {
        match self {
            Target::Sql(store) => Some(&**store),
            Target::Object(_) => None,
        }
    }
}

impl Lake {
    /// Migrate the lake whose metadata lives at `from` to the catalog at `to`
    /// (wave 96a). Every metadata object is copied byte-for-byte in one target
    /// publish scope; data files are not touched. See the module docs for the
    /// three legal directions and the source-retirement rules.
    ///
    /// `data` overrides the target's recorded data location. Object store →
    /// SQL always records the source location (the data stays there), so an
    /// explicit `data` there must equal `from`. SQL → object store always
    /// writes to the lake's own data location, so `to` (and an explicit
    /// `data`) must equal it. SQL → SQL accepts an override for data that was
    /// moved by hand.
    pub fn migrate_catalog(from: &str, to: &str, data: Option<&str>) -> Result<MigrateReport> {
        let from_location = Location::parse(from)?;
        let to_location = Location::parse(to)?;
        let source = read_source(from, &from_location)?;
        if matches!(source, Source::Object { .. }) && !is_catalog_location(&to_location) {
            return Err(LakeError::BadLocation {
                location: redact_location(to),
                message: "a migration needs a SQL catalog target (`sqlite:<path>` or \
                          `postgres://…`) or, from a SQL source, the lake's own data location"
                    .to_string(),
            });
        }

        let target_data = target_data_location(&source, from, to, &to_location, data)?;
        let (target, reopen) = open_target(to, &to_location)?;

        let publish = target.storage().begin_publish()?;
        if let Some(store) = target.as_catalog() {
            store.set_data_location(&target_data)?;
            if let Some(lake_id) = source.lake_id() {
                store.set_lake_id(lake_id)?;
            }
        }
        let mut bytes = 0u64;
        let objects = source.objects();
        for (rel, object) in objects {
            if !target.storage().put_new(rel, object)? {
                return Err(LakeError::AlreadyALake {
                    path: PathBuf::from(redact_location(to)),
                });
            }
            bytes += object.len() as u64;
        }
        publish.commit()?;

        verify_target(reopen, &target_data, objects)?;
        retire_source(&source, to)?;
        Ok(MigrateReport {
            objects: objects.len(),
            bytes,
            source_retired: true,
        })
    }
}

fn is_catalog_location(location: &Location) -> bool {
    matches!(location, Location::Sqlite(_) | Location::Postgres(_))
}

fn read_source(location: &str, parsed: &Location) -> Result<Source> {
    match parsed {
        Location::Sqlite(_) | Location::Postgres(_) => {
            let store = catalog_store::open_catalog(location, parsed, false)?;
            if store.moved_to()?.is_some() {
                return Err(catalog_moved(location, &*store)?);
            }
            let mut objects = Vec::new();
            for rel in store.list_recursive("")? {
                if !catalog_store::is_metadata_path(&rel) {
                    continue;
                }
                let context = format!("reading {location}#{rel}");
                if let Some(bytes) = store.read_optional(&rel, &context)? {
                    objects.push((rel, bytes));
                }
            }
            if !objects.iter().any(|(rel, _)| rel == CATALOG_FILE) {
                return Err(LakeError::NotALake {
                    path: PathBuf::from(redact_location(location)),
                });
            }
            Ok(Source::Sql {
                lake_id: store.lake_id()?,
                data_location: store.data_location()?,
                store,
                objects,
            })
        }
        Location::Local(_) | Location::S3 { .. } => {
            let storage = crate::storage_for(parsed.clone())?;
            let Some(catalog) = storage.read_optional(
                CATALOG_FILE,
                &format!("reading {}", storage.abs(CATALOG_FILE).display()),
            )?
            else {
                return Err(missing_catalog_error(&*storage, &storage.abs(""))?);
            };
            let mut objects = vec![(CATALOG_FILE.to_string(), catalog)];
            for rel in [LAKE_FILE, HIGHEST_FILE] {
                if let Some(bytes) = storage
                    .read_optional(rel, &format!("reading {}", storage.abs(rel).display()))?
                {
                    objects.push((rel.to_string(), bytes));
                }
            }
            for family in ["snapshots", "commits", "replication"] {
                for tail in storage.list_recursive(family)? {
                    let rel = format!("{family}/{tail}");
                    if let Some(bytes) = storage
                        .read_optional(&rel, &format!("reading {}", storage.abs(&rel).display()))?
                    {
                        objects.push((rel, bytes));
                    }
                }
            }
            let lake_id = source_lake_id(&*storage, &objects)?;
            Ok(Source::Object {
                storage,
                lake_id,
                objects,
            })
        }
    }
}

/// The identity of an object-store lake without opening it: a plaintext
/// `lake.json`, or the authenticated `(lake_id, version)` binding of a sealed
/// `catalog.json` (no key needed).
fn source_lake_id(storage: &dyn Storage, objects: &[(String, Vec<u8>)]) -> Result<Option<String>> {
    if let Some((_, bytes)) = objects.iter().find(|(rel, _)| rel == LAKE_FILE)
        && bytes.starts_with(b"{")
    {
        #[derive(Deserialize)]
        struct Identity {
            lake_id: String,
        }
        if let Ok(identity) = serde_json::from_slice::<Identity>(bytes)
            && is_lake_id(&identity.lake_id)
        {
            return Ok(Some(identity.lake_id));
        }
    }
    if let Some((_, bytes)) = objects.iter().find(|(rel, _)| rel == CATALOG_FILE)
        && crate::crypto::is_container(bytes)
    {
        let binding = crate::crypto::binding_of_container(&storage.abs(CATALOG_FILE), bytes)?;
        if let Some((lake_id, _)) = parse_manifest_binding(&binding) {
            return Ok(Some(lake_id.to_string()));
        }
    }
    Ok(None)
}

fn target_data_location(
    source: &Source,
    from: &str,
    to: &str,
    to_location: &Location,
    data: Option<&str>,
) -> Result<String> {
    match (source.is_sql(), is_catalog_location(to_location)) {
        (false, true) => {
            if let Some(data) = data
                && data != from
            {
                return Err(LakeError::BadLocation {
                    location: redact_location(from),
                    message: "the data files stay at the source location; omit `data` or pass \
                              the source location"
                        .to_string(),
                });
            }
            Ok(from.to_string())
        }
        (false, false) => Err(LakeError::BadLocation {
            location: redact_location(to),
            message: "object store → object store is not a migration; move the directory or \
                      prefix instead"
                .to_string(),
        }),
        (true, false) => {
            let recorded = source.data_location().ok_or_else(|| LakeError::Corrupt {
                path: PathBuf::from(redact_location(from)),
                message: "the source catalog has no pink_lake_meta.data_location; pass one with \
                          `data <location>`"
                    .to_string(),
            })?;
            let chosen = data.unwrap_or(recorded);
            if chosen != to {
                return Err(LakeError::BadLocation {
                    location: redact_location(to),
                    message: format!(
                        "SQL → object store writes the metadata next to the data; the target \
                         must be the lake's data location ({})",
                        redact_location(recorded)
                    ),
                });
            }
            Ok(chosen.to_string())
        }
        (true, true) => {
            if let Some(data) = data {
                return Ok(data.to_string());
            }
            source
                .data_location()
                .map(str::to_string)
                .ok_or_else(|| LakeError::Corrupt {
                    path: PathBuf::from(redact_location(from)),
                    message: "the source catalog has no pink_lake_meta.data_location; pass one \
                              with `data <location>`"
                        .to_string(),
                })
        }
    }
}

/// A closure that reopens the target for verification after the copy.
type TargetReopen = Box<dyn FnOnce() -> Result<Target>>;

/// Open the target and prove it is empty (or, for an object-store target, that
/// it holds no metadata yet — its data files are meant to be there). Returns
/// the target plus a closure that reopens it for verification.
fn open_target(to: &str, parsed: &Location) -> Result<(Target, TargetReopen)> {
    match parsed {
        Location::Sqlite(_) | Location::Postgres(_) => {
            let store = match catalog_store::open_catalog(to, parsed, false) {
                Ok(store) => {
                    if store.moved_to()?.is_some() {
                        return Err(catalog_moved(to, &*store)?);
                    }
                    if store.exists(CATALOG_FILE)? || !store.list_recursive("")?.is_empty() {
                        return Err(LakeError::AlreadyALake {
                            path: PathBuf::from(redact_location(to)),
                        });
                    }
                    store
                }
                Err(LakeError::NotALake { .. }) => catalog_store::open_catalog(to, parsed, true)?,
                Err(e) => return Err(e),
            };
            let location = parsed.clone();
            let reopen_to = to.to_string();
            Ok((
                Target::Sql(store),
                Box::new(move || {
                    catalog_store::open_catalog(&reopen_to, &location, false)
                        .map(Target::Sql)
                        .map_err(|e| annotate_verify(e, &reopen_to))
                }),
            ))
        }
        Location::Local(_) | Location::S3 { .. } => {
            let storage = crate::storage_for(parsed.clone())?;
            if storage.exists(CATALOG_FILE)? || storage.exists(LAKE_FILE)? {
                return Err(LakeError::AlreadyALake {
                    path: PathBuf::from(redact_location(to)),
                });
            }
            let location = parsed.clone();
            let reopen_to = to.to_string();
            Ok((
                Target::Object(storage),
                Box::new(move || {
                    crate::storage_for(location)
                        .map(Target::Object)
                        .map_err(|e| annotate_verify(e, &reopen_to))
                }),
            ))
        }
    }
}

fn verify_target(
    reopen: Box<dyn FnOnce() -> Result<Target>>,
    target_data: &str,
    objects: &[(String, Vec<u8>)],
) -> Result<()> {
    let target = reopen()?;
    if let Some(store) = target.as_catalog()
        && store.data_location()?.as_deref() != Some(target_data)
    {
        return Err(LakeError::Internal {
            message: "the migrated catalog did not record the expected data location".to_string(),
        });
    }
    for (rel, expected) in objects {
        let context = format!("verifying the migrated {rel}");
        let Some(actual) = target.storage().read_optional(rel, &context)? else {
            return Err(LakeError::Internal {
                message: format!("the migrated catalog is missing {rel}"),
            });
        };
        if &actual != expected {
            return Err(LakeError::Internal {
                message: format!("the migrated {rel} is not byte-identical to the source"),
            });
        }
    }
    Ok(())
}

fn retire_source(source: &Source, to: &str) -> Result<()> {
    let target = redact_location(to);
    match source {
        Source::Sql { store, .. } => {
            let publish = store.begin_publish()?;
            store.set_moved_to(&target)?;
            publish.commit()
        }
        Source::Object {
            storage, lake_id, ..
        } => {
            let marker = MovedMarker {
                moved_to: target,
                lake_id: lake_id.clone(),
                moved_at: time::iso8601_utc(SystemTime::now()),
            };
            let mut bytes =
                serde_json::to_vec_pretty(&marker).map_err(|e| LakeError::Internal {
                    message: format!("serializing moved.json: {e}"),
                })?;
            bytes.push(b'\n');
            let context = format!("writing {}", storage.abs(MOVED_FILE).display());
            storage
                .put(MOVED_FILE, &bytes)
                .map_err(|e| annotate(e, &context))?;
            // Catalog last: until this succeeds the source still opens
            // normally, and snapshots remain for a hand recovery.
            storage.delete(CATALOG_FILE)?;
            Ok(())
        }
    }
}

fn catalog_moved(location: &str, store: &dyn CatalogStore) -> Result<LakeError> {
    let moved_to = store
        .moved_to()?
        .unwrap_or_else(|| "another catalog".to_string());
    Ok(LakeError::CatalogMoved {
        path: PathBuf::from(redact_location(location)),
        moved_to,
    })
}

fn annotate(error: LakeError, location: &str) -> LakeError {
    match error {
        LakeError::Io { context, source } => LakeError::Io {
            context: format!("{context} ({})", redact_location(location)),
            source,
        },
        other => other,
    }
}

fn annotate_verify(error: LakeError, location: &str) -> LakeError {
    annotate(error, location)
}

/// The missing-catalog error: a retired object-store location carries
/// `moved.json`, so report the move instead of pretending the lake vanished.
/// Called only when `catalog.json` is already known to be missing.
pub(crate) fn missing_catalog_error(storage: &dyn Storage, root: &Path) -> Result<LakeError> {
    let context = format!("reading {}", storage.abs(MOVED_FILE).display());
    match storage.read_moved_optional(&context) {
        Ok(Some(bytes)) => {
            if let Ok(marker) = serde_json::from_slice::<MovedMarker>(&bytes) {
                return Ok(LakeError::CatalogMoved {
                    path: root.to_path_buf(),
                    moved_to: marker.moved_to,
                });
            }
        }
        // A moved marker is a best-effort hint; a malformed or unreadable one
        // must never mask the clear `NotALake`.
        Ok(None) => {}
        Err(_) => {}
    }
    Ok(LakeError::NotALake {
        path: root.to_path_buf(),
    })
}
