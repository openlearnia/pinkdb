//! The exclusive write lease: one holder freezes the warehouse, runs its
//! transform outside this process, and hands the warehouse back
//! (`docs/wave-138-plan.md`, decision 2).
//!
//! The state machine is the Go service's (`quacklab-vps-duckdb/runtime.go`):
//! `acquire` stops plain admission, drains in-flight requests, closes the
//! warehouse and arms a TTL watchdog; `release` and the watchdog race for the
//! single `ending` flag, drain the holder's SQL, reopen, and clear the lease;
//! an abandoned drain clears the lease without having closed anything. The
//! watchdog carries the expiry it was armed for, so a timer that has already
//! fired cannot expire a lease a concurrent renew extended.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::server::constant_time_eq;

/// `GRAIN_WRITE_LEASE_DRAIN_TIMEOUT` when it is unset or invalid.
pub(crate) const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// A lease with no TTL asked for lasts an hour.
pub(crate) const DEFAULT_WRITE_LEASE_TTL: Duration = Duration::from_secs(60 * 60);
/// The clamp's floor: a shorter lease would race its own acquire.
pub(crate) const MIN_WRITE_LEASE_TTL: Duration = Duration::from_secs(30);
/// The clamp's ceiling.
pub(crate) const MAX_WRITE_LEASE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// What a lease does to the warehouse: close it for the duration, open it
/// again at the end. The local warehouse has nothing to do either way; 138c
/// gives the lake one that attaches and detaches its catalog.
pub trait Warehouse: Send + Sync {
    fn close(&self) -> Result<(), String>;
    fn open(&self) -> Result<(), String>;
}

/// The adapter's own warehouse: a [`pinkdb::Database`] that never closes.
pub(crate) struct LocalWarehouse;

impl Warehouse for LocalWarehouse {
    fn close(&self) -> Result<(), String> {
        Ok(())
    }

    fn open(&self) -> Result<(), String> {
        Ok(())
    }
}

/// One active lease.
struct Lease {
    id: String,
    owner: String,
    issued_at: Instant,
    expires_at: SystemTime,
    ending: bool,
}

struct Inner {
    lease: Option<Lease>,
    in_flight: usize,
}

/// The lease state plus the admission count it drains.
pub(crate) struct Leases {
    inner: Mutex<Inner>,
    /// The in-flight count after every change, so a drain waits on a value
    /// rather than on a channel that could be closed before it subscribes.
    drained: watch::Sender<usize>,
}

impl Leases {
    pub(crate) fn new() -> Self {
        let (drained, _) = watch::channel(0);
        Self {
            inner: Mutex::new(Inner {
                lease: None,
                in_flight: 0,
            }),
            drained,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic in one request must not poison the warehouse for the rest.
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn in_flight(&self) -> usize {
        self.lock().in_flight
    }

    /// Admit one plain request, or refuse while a lease (ending or not) is
    /// installed. The guard releases the count when the request answers.
    pub(crate) fn try_admit(self: &Arc<Self>) -> Option<Admit> {
        let mut inner = self.lock();
        if inner.lease.is_some() {
            return None;
        }
        inner.in_flight += 1;
        let permit = inner.in_flight;
        drop(inner);
        self.drained.send_replace(permit);
        Some(Admit {
            leases: Arc::clone(self),
        })
    }

    /// Admit the lease holder's own SQL: only the exact active id, and never
    /// while the lease is ending.
    pub(crate) fn try_admit_holder(self: &Arc<Self>, id: &str) -> Option<Admit> {
        let mut inner = self.lock();
        let lease = inner.lease.as_ref()?;
        if lease.ending || !constant_time_eq(lease.id.as_bytes(), id.as_bytes()) {
            return None;
        }
        inner.in_flight += 1;
        let permit = inner.in_flight;
        drop(inner);
        self.drained.send_replace(permit);
        Some(Admit {
            leases: Arc::clone(self),
        })
    }

    pub(crate) fn active_id(&self) -> Option<String> {
        self.lock().lease.as_ref().map(|lease| lease.id.clone())
    }

    async fn wait_drained(&self) {
        let mut drained = self.drained.subscribe();
        let _ = drained.wait_for(|in_flight| *in_flight == 0).await;
    }

    /// Go's `acquireWriteLease`: install the lease before anything else (plain
    /// admission stops at once), drain, close, then arm the watchdog.
    pub(crate) async fn acquire(
        self: &Arc<Self>,
        warehouse: Arc<dyn Warehouse>,
        owner: String,
        ttl: Duration,
        drain_timeout: Duration,
    ) -> Result<Grant, AcquireError> {
        let started = Instant::now();
        let id = new_lease_id()
            .map_err(|message| AcquireError::Failed(format!("create a lease id: {message}")))?;
        {
            let mut inner = self.lock();
            if inner.lease.is_some() {
                return Err(AcquireError::Held);
            }
            inner.lease = Some(Lease {
                id: id.clone(),
                owner: owner.clone(),
                issued_at: started,
                expires_at: SystemTime::now(),
                ending: false,
            });
        }
        // A cancelled request (client gone) must not leave the warehouse
        // frozen with a lease nobody holds: the drop abandons it, as Go's
        // `ctx.Done()` path does.
        let mut pending = Pending {
            leases: self,
            id: id.clone(),
            armed: true,
        };
        if self.in_flight() > 0
            && tokio::time::timeout(drain_timeout, self.wait_drained())
                .await
                .is_err()
        {
            return Err(AcquireError::Failed(format!(
                "drain in-flight sql: still busy after {}s",
                drain_timeout.as_secs()
            )));
        }
        let drain_ms = started.elapsed().as_millis() as u64;
        close_warehouse(Arc::clone(&warehouse))
            .await
            .map_err(AcquireError::Failed)?;

        let expires_at = SystemTime::now() + ttl;
        self.arm(id.clone(), expires_at, ttl, Arc::clone(&warehouse));
        {
            let mut inner = self.lock();
            match inner.lease.as_mut() {
                Some(lease) if lease.id == id && !lease.ending => {
                    lease.expires_at = expires_at;
                }
                // `/recover` force-ended the lease while the drain ran.
                _ => {
                    return Err(AcquireError::Failed(
                        "drain in-flight sql: the lease ended during the drain".to_string(),
                    ));
                }
            }
        }
        pending.armed = false;
        let acquire_ms = started.elapsed().as_millis() as u64;
        eprintln!(
            "write-lease acquired owner={} ttl={}s drainMs={drain_ms} acquireMs={acquire_ms}",
            safe_lease_owner(&owner),
            ttl.as_secs()
        );
        Ok(Grant {
            id,
            owner,
            ttl_seconds: ttl.as_secs() as i64,
            expires_at,
            drain_ms,
            acquire_ms,
        })
    }

    /// Extend the exact active lease; the watchdog is re-armed with the new
    /// expiry before the old one is forgotten.
    pub(crate) fn renew(
        self: &Arc<Self>,
        warehouse: Arc<dyn Warehouse>,
        id: &str,
        ttl: Duration,
    ) -> Result<Grant, EndError> {
        let mut inner = self.lock();
        let Some(lease) = inner
            .lease
            .as_mut()
            .filter(|lease| !lease.ending && constant_time_eq(lease.id.as_bytes(), id.as_bytes()))
        else {
            return Err(EndError::Unknown);
        };
        let expires_at = SystemTime::now() + ttl;
        lease.expires_at = expires_at;
        self.arm(lease.id.clone(), expires_at, ttl, warehouse);
        Ok(Grant {
            id: lease.id.clone(),
            owner: lease.owner.clone(),
            ttl_seconds: ttl.as_secs() as i64,
            expires_at,
            drain_ms: 0,
            acquire_ms: 0,
        })
    }

    /// Go's `endWriteLease`: one winner between release and the TTL watchdog,
    /// then the holder's SQL drains, then the warehouse reopens.
    pub(crate) async fn end(
        &self,
        id: &str,
        expected_expiry: Option<SystemTime>,
        reason: &str,
        warehouse: &Arc<dyn Warehouse>,
    ) -> Result<Released, EndError> {
        let started = Instant::now();
        let (issued_at, owner) = {
            let mut inner = self.lock();
            let Some(lease) = inner.lease.as_mut() else {
                return Err(EndError::Unknown);
            };
            if lease.ending || !constant_time_eq(lease.id.as_bytes(), id.as_bytes()) {
                return Err(EndError::Unknown);
            }
            if let Some(expected) = expected_expiry
                && lease.expires_at != expected
            {
                return Err(EndError::Unknown);
            }
            lease.ending = true;
            (lease.issued_at, lease.owner.clone())
        };
        // Silence is the point: while `ending`, holder admission is closed
        // and plain admission still sees the lease.
        self.wait_drained().await;
        let opened = open_warehouse(Arc::clone(warehouse)).await;
        self.lock().lease = None;
        match opened {
            Ok(()) => {
                let held_ms = issued_at.elapsed().as_millis() as u64;
                let reopen_ms = started.elapsed().as_millis() as u64;
                eprintln!(
                    "write-lease {reason} owner={} heldMs={held_ms} reopenMs={reopen_ms}",
                    safe_lease_owner(&owner)
                );
                Ok(Released { held_ms, reopen_ms })
            }
            Err(message) => {
                // The lease is gone so admission retries the open itself; the
                // releaser still hears about the failure.
                eprintln!("write-lease {reason}: reopen warehouse failed: {message}");
                Err(EndError::Reopen(message))
            }
        }
    }

    /// `end`, but for `/recover`: a lease another end already took is fine.
    pub(crate) async fn end_active(
        &self,
        reason: &str,
        warehouse: &Arc<dyn Warehouse>,
    ) -> Result<(), EndError> {
        let Some(id) = self.active_id() else {
            return Ok(());
        };
        match self.end(&id, None, reason, warehouse).await {
            Ok(_) | Err(EndError::Unknown) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Undo a lease whose drain never finished: the warehouse was not closed,
    /// so plain admission resumes immediately.
    fn abandon(&self, id: &str) {
        let mut inner = self.lock();
        if inner
            .lease
            .as_ref()
            .is_some_and(|lease| lease.id == id && !lease.ending)
        {
            inner.lease = None;
        }
    }

    /// The TTL watchdog: `expires_at` is the expiry this timer was armed for,
    /// so it can only end the lease it was created for. The wait itself is on
    /// the monotonic clock, as Go's `time.AfterFunc` is.
    ///
    /// The timer is never cancelled: a renew leaves the old one to wake up and
    /// be refused by that expiry check, because aborting the timer that is
    /// currently ending the lease would stop the end half-way.
    fn arm(
        self: &Arc<Self>,
        id: String,
        expires_at: SystemTime,
        ttl: Duration,
        warehouse: Arc<dyn Warehouse>,
    ) {
        let leases = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            if let Err(error) = leases
                .end(&id, Some(expires_at), "expired", &warehouse)
                .await
            {
                match error {
                    EndError::Unknown => {}
                    EndError::Reopen(message) => {
                        eprintln!("write-lease expiry: reopen warehouse failed: {message}")
                    }
                }
            }
        });
    }
}

/// One admitted request: dropping it returns the request to the drain count.
pub(crate) struct Admit {
    leases: Arc<Leases>,
}

impl Drop for Admit {
    fn drop(&mut self) {
        let mut inner = self.leases.lock();
        inner.in_flight -= 1;
        let in_flight = inner.in_flight;
        drop(inner);
        self.leases.drained.send_replace(in_flight);
    }
}

/// A lease whose acquire has not finished: any exit (timeout, cancellation,
/// an error) abandons it.
struct Pending<'a> {
    leases: &'a Leases,
    id: String,
    armed: bool,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.leases.abandon(&self.id);
        }
    }
}

/// An acquire response.
pub(crate) struct Grant {
    pub(crate) id: String,
    pub(crate) owner: String,
    pub(crate) ttl_seconds: i64,
    pub(crate) expires_at: SystemTime,
    pub(crate) drain_ms: u64,
    pub(crate) acquire_ms: u64,
}

/// A release response.
pub(crate) struct Released {
    pub(crate) held_ms: u64,
    pub(crate) reopen_ms: u64,
}

/// Why an acquire failed.
pub(crate) enum AcquireError {
    /// Another lease is active: `409`.
    Held,
    /// The drain, close or id failed: `503 lease_drain_failed`.
    Failed(String),
}

/// Why renew, release or expiry did nothing (or could not reopen).
pub(crate) enum EndError {
    /// No such active, non-ending lease: `404 unknown_lease`.
    Unknown,
    /// The lease ended but the warehouse would not reopen.
    Reopen(String),
}

async fn close_warehouse(warehouse: Arc<dyn Warehouse>) -> Result<(), String> {
    tokio::task::spawn_blocking(move || warehouse.close())
        .await
        .map_err(|join| format!("the warehouse worker failed: {join}"))?
}

async fn open_warehouse(warehouse: Arc<dyn Warehouse>) -> Result<(), String> {
    tokio::task::spawn_blocking(move || warehouse.open())
        .await
        .map_err(|join| format!("the warehouse worker failed: {join}"))?
}

/// 32 lowercase hex chars from the OS RNG, like the Go service's `crypto/rand`
/// id: a lease id is a capability, not a counter.
fn new_lease_id() -> Result<String, String> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes)
        .map_err(|error| format!("the OS random source is unavailable: {error}"))?;
    let mut id = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(id, "{byte:02x}");
    }
    Ok(id)
}

/// Go's `safeLeaseOwner`: a caller's label kept out of the log's structure.
pub(crate) fn safe_lease_owner(owner: &str) -> String {
    let mut safe = String::new();
    for character in owner.trim().chars() {
        if safe.len() >= 32 {
            break;
        }
        if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
            safe.push(character);
        }
    }
    if safe.is_empty() {
        "unknown".to_string()
    } else {
        safe
    }
}

/// Go's `clampWriteLeaseTTL`. `floor` is the 30 s minimum; a test lowers it so
/// a lease can expire without a 30 s sleep.
pub(crate) fn clamp_ttl(seconds: i64, floor: Duration) -> Duration {
    if seconds <= 0 {
        return DEFAULT_WRITE_LEASE_TTL;
    }
    Duration::from_secs(seconds as u64)
        .max(floor)
        .min(MAX_WRITE_LEASE_TTL)
}

/// Go's `time.ParseDuration`: a signed sequence of decimal numbers with a unit
/// (`ns`, `us`/`µs`, `ms`, `s`, `m`, `h`), e.g. `1h30m` or `1.5h`. A value that
/// is not positive is `None`, which is what the drain timeout needs anyway.
pub(crate) fn parse_go_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    let rest = raw.strip_prefix('+').unwrap_or(raw);
    if rest == "0" {
        return None;
    }
    let mut seconds = 0.0f64;
    let mut rest = rest;
    while !rest.is_empty() {
        let number_len = rest
            .find(|character: char| !(character.is_ascii_digit() || character == '.'))
            .unwrap_or(rest.len());
        let number: f64 = rest[..number_len].parse().ok()?;
        rest = &rest[number_len..];
        let unit_len = rest
            .find(|character: char| character.is_ascii_digit() || character == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_len];
        rest = &rest[unit_len..];
        seconds += number
            * match unit {
                "ns" => 1e-9,
                "us" | "µs" | "μs" => 1e-6,
                "ms" => 1e-3,
                "s" => 1.0,
                "m" => 60.0,
                "h" => 3600.0,
                _ => return None,
            };
    }
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(seconds))
}

/// RFC 3339 in UTC, the way Go's `time.Time` marshals: fractional seconds only
/// when there are any, trailing zeros trimmed.
pub(crate) fn rfc3339(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = since_epoch.as_secs() as i64;
    let nanos = since_epoch.subsec_nanos();
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let second_of_day = seconds.rem_euclid(86_400);
    let mut out = format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60
    );
    if nanos > 0 {
        let fraction = format!(".{nanos:09}");
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Days since 1970-01-01 to a civil date (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    (
        if month <= 2 { year + 1 } else { year },
        month as u32,
        day as u32,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ttl_clamps_between_thirty_seconds_and_six_hours() {
        assert_eq!(clamp_ttl(0, MIN_WRITE_LEASE_TTL), DEFAULT_WRITE_LEASE_TTL);
        assert_eq!(clamp_ttl(-5, MIN_WRITE_LEASE_TTL), DEFAULT_WRITE_LEASE_TTL);
        assert_eq!(clamp_ttl(5, MIN_WRITE_LEASE_TTL), MIN_WRITE_LEASE_TTL);
        assert_eq!(
            clamp_ttl(120, MIN_WRITE_LEASE_TTL),
            Duration::from_secs(120)
        );
        assert_eq!(clamp_ttl(999_999, MIN_WRITE_LEASE_TTL), MAX_WRITE_LEASE_TTL);
        // The test hook lowers only the floor.
        assert_eq!(
            clamp_ttl(1, Duration::from_millis(200)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn owners_are_sanitized_for_the_log() {
        assert_eq!(safe_lease_owner("dbt-run"), "dbt-run");
        assert_eq!(safe_lease_owner("  "), "unknown");
        assert_eq!(safe_lease_owner("a b\nc"), "abc");
        assert_eq!(safe_lease_owner("../../etc"), "etc");
        assert_eq!(safe_lease_owner(&"x".repeat(40)), "x".repeat(32));
        assert_eq!(safe_lease_owner("ünicode"), "nicode");
    }

    #[test]
    fn ids_are_thirty_two_hex_chars() {
        let id = new_lease_id().expect("the OS random source");
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|character| character.is_ascii_hexdigit()));
        assert_ne!(id, new_lease_id().unwrap());
    }

    #[test]
    fn go_durations_parse() {
        assert_eq!(parse_go_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_go_duration(" 30s "), Some(Duration::from_secs(30)));
        assert_eq!(parse_go_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("1.5h"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("300ms"), Some(Duration::from_millis(300)));
        assert_eq!(parse_go_duration("250µs"), Some(Duration::from_micros(250)));
        assert_eq!(parse_go_duration("1h2m3s"), Some(Duration::from_secs(3723)));
        for bad in ["", "0", "-5s", "5", "m", "5x", "1h30", "5 s"] {
            assert_eq!(parse_go_duration(bad), None, "{bad}");
        }
    }

    #[test]
    fn rfc3339_matches_go() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_millis(1_700_000_000_500)),
            "2023-11-14T22:13:20.5Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_nanos(1_700_000_000_000_000_001)),
            "2023-11-14T22:13:20.000000001Z"
        );
    }
}
