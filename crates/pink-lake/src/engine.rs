//! The engine lease (wave 96b): one engine per lake catalog.
//!
//! A stateless server's engine state is its in-process store; the catalog and
//! the data files are the durable pair. Phase 1 therefore allows exactly one
//! server per catalog — a second one would run a divergent copy of the engine
//! and silently overwrite the first one's metadata, since there is no cache
//! invalidation yet (phase 2). The lease is what refuses it.
//!
//! The lease is a row in the catalog database, not a local lock file: it must
//! hold when the two servers run on different machines. It is an opaque
//! [`crate::metadata::MetadataStore`] object (`engine/lease`), carrying the
//! owner and its heartbeat time:
//!
//! * a server takes the lease at startup and deletes it at shutdown;
//! * a heartbeat thread refreshes it every [`HEARTBEAT_INTERVAL`], so an
//!   *idle* server keeps its lease;
//! * a lease whose heartbeat is older than [`LEASE_TIMEOUT`] is stale — the
//!   owner crashed — and the next server takes it over;
//! * the owner verifies the row still names it before every commit
//!   ([`EngineLease::verify`]), so a server that was declared stale cannot
//!   keep publishing.
//!
//! The remaining race is inherent to a lock with no compare-and-swap: two
//! servers that start within the heartbeat window of a crashed one can both
//! decide to take over. Phase 2 replaces the lease with a real lease record
//! (fencing token + invalidation), which is why the object is versioned.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::metadata::MetadataStore;
use crate::{Lake, LakeError};

/// Where the lease lives among the catalog's opaque objects.
pub const LEASE_PATH: &str = "engine/lease";

/// How often a running server refreshes its heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How long a heartbeat may be missing before the lease counts as stale.
/// Three missed beats, so a briefly stalled server is not stolen from.
pub const LEASE_TIMEOUT: Duration = Duration::from_secs(6);

/// The lease could not be taken, or was lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseError {
    /// Another server holds the catalog's lease.
    Held {
        /// The holder's owner string (host and process).
        owner: String,
        /// Milliseconds since its last heartbeat.
        age_ms: u64,
    },
    /// This server's lease is gone: another server took it over (this server
    /// stalled past the timeout) or the row was deleted out of band.
    Lost {
        /// The owner the catalog now names, if any.
        owner: Option<String>,
    },
    /// The catalog could not be read or written.
    Catalog(String),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Held { owner, age_ms } => write!(
                f,
                "the lake is already served by {owner} (heartbeat {:.1}s ago): a lake has \
                 one writer at a time; attach it READ_ONLY to read it",
                *age_ms as f64 / 1000.0
            ),
            LeaseError::Lost { owner: Some(owner) } => write!(
                f,
                "this server lost the catalog lease to {owner}; its engine state is \
                 no longer authoritative — stop it and start a new one"
            ),
            LeaseError::Lost { owner: None } => write!(
                f,
                "this server lost the catalog lease (the lease row is gone); its \
                 engine state is no longer authoritative"
            ),
            LeaseError::Catalog(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for LeaseError {}

impl From<LakeError> for LeaseError {
    fn from(error: LakeError) -> Self {
        LeaseError::Catalog(error.to_string())
    }
}

/// The lease on one catalog's engine, held for the lifetime of the value.
#[derive(Debug)]
pub struct EngineLease {
    owner: String,
    store: Arc<dyn MetadataStore>,
}

impl EngineLease {
    /// Take the lease on `lake`'s catalog for `owner` (a `host:pid` string),
    /// or report the live holder.
    pub fn acquire(lake: &Lake, owner: &str) -> Result<EngineLease, LeaseError> {
        if owner.contains('\t') || owner.contains('\n') {
            return Err(LeaseError::Catalog(format!(
                "invalid lease owner {owner:?}: it must not contain tabs or newlines"
            )));
        }
        let store = lake.store_handle();
        let lease = EngineLease {
            owner: owner.to_string(),
            store,
        };
        match lease.read()? {
            Some(record) if record.owner != lease.owner => {
                let age = now_ms().saturating_sub(record.heartbeat_ms);
                if age < LEASE_TIMEOUT.as_millis() as u64 {
                    return Err(LeaseError::Held {
                        owner: record.owner,
                        age_ms: age,
                    });
                }
                // Stale: the previous owner crashed. Take it over.
                lease.write()?;
            }
            Some(_) => lease.write()?,
            None => lease.write()?,
        }
        Ok(lease)
    }

    /// The owner string this lease writes.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Refresh the heartbeat. Called by the server's heartbeat thread; a
    /// refresh that finds the row taken over reports [`LeaseError::Lost`] and
    /// the thread stops.
    pub fn heartbeat(&self) -> Result<(), LeaseError> {
        match self.read()? {
            Some(record) if record.owner == self.owner => self.write(),
            Some(record) => Err(LeaseError::Lost {
                owner: Some(record.owner),
            }),
            None => Err(LeaseError::Lost { owner: None }),
        }
    }

    /// Confirm this server still owns the catalog (the commit path does this
    /// before publishing, so a stolen lease cannot keep committing).
    pub fn verify(&self) -> Result<(), LeaseError> {
        match self.read()? {
            Some(record) if record.owner == self.owner => Ok(()),
            Some(record) => Err(LeaseError::Lost {
                owner: Some(record.owner),
            }),
            None => Err(LeaseError::Lost { owner: None }),
        }
    }

    fn read(&self) -> Result<Option<LeaseRecord>, LeaseError> {
        let Some(bytes) = self.store.get_object(LEASE_PATH)? else {
            return Ok(None);
        };
        let text = String::from_utf8(bytes)
            .map_err(|_| LeaseError::Catalog("the lease row is not UTF-8".to_string()))?;
        Ok(LeaseRecord::decode(&text))
    }

    fn write(&self) -> Result<(), LeaseError> {
        let record = LeaseRecord {
            owner: self.owner.clone(),
            heartbeat_ms: now_ms(),
        };
        self.store
            .replace_object(LEASE_PATH, record.encode().as_bytes())?;
        Ok(())
    }
}

impl Drop for EngineLease {
    /// Release the lease on a clean shutdown — but only while this server still
    /// holds it: a late drop must not delete the new owner's row.
    fn drop(&mut self) {
        if matches!(self.verify(), Ok(())) {
            let _ = self.store.delete_object(LEASE_PATH);
        }
    }
}

/// The lease row's decoded form.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseRecord {
    owner: String,
    heartbeat_ms: u64,
}

impl LeaseRecord {
    fn encode(&self) -> String {
        format!("{}\t{}", self.owner, self.heartbeat_ms)
    }

    fn decode(text: &str) -> Option<LeaseRecord> {
        let (owner, heartbeat) = text.trim_end_matches('\n').split_once('\t')?;
        if owner.is_empty() {
            return None;
        }
        Some(LeaseRecord {
            owner: owner.to_string(),
            heartbeat_ms: heartbeat.parse().ok()?,
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_record_round_trips() {
        let record = LeaseRecord {
            owner: "box:4711".to_string(),
            heartbeat_ms: 1_700_000_000_000,
        };
        assert_eq!(LeaseRecord::decode(&record.encode()), Some(record));
        assert_eq!(LeaseRecord::decode("no-tab"), None);
        assert_eq!(LeaseRecord::decode("\t12"), None);
        assert_eq!(LeaseRecord::decode("box:x"), None);
    }

    #[test]
    fn the_held_message_names_the_owner_and_its_heartbeat() {
        let error = LeaseError::Held {
            owner: "box:4711".to_string(),
            age_ms: 1_500,
        };
        let message = error.to_string();
        assert!(message.contains("already served by box:4711"), "{message}");
        assert!(message.contains("heartbeat 1.5s ago"), "{message}");
    }
}
