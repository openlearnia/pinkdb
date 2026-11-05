//! The process-wide memory budget (wave 136a), after DuckDB's
//! `BufferManager`/`memory_limit`: one limit, one atomic `used` count and a
//! peak, and RAII reservations that release on drop.
//!
//! The budget is *approximate*: an operator reserves the bytes of the chunks
//! and tables it holds (computed the way [`crate::exec`]'s
//! `chunk_approx_bytes` does), not the bytes its allocator returned. It is a
//! limit on how much state a statement may accumulate, not an allocator, so a
//! statement that stays below the limit behaves exactly as it did before the
//! pool existed.
//!
//! `SET memory_limit` ([`set_limit`]) replaces the limit for the whole
//! process, like DuckDB's global setting; [`reset_limit`] returns to the
//! default. The default is 80% of the memory the process may use: physical RAM,
//! or the tighter cgroup limit a container (Cloud Run included) imposed.
//! wasm32 has no way to read either, so its default is no limit.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::types::{PinkDbError, Result};

/// No limit: DuckDB's `SET memory_limit='-1'`. `used` never reaches it.
pub const UNLIMITED: usize = usize::MAX;

static USED: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
/// The last explicit `SET memory_limit`. Only read while [`EXPLICIT`] is set.
static LIMIT: AtomicUsize = AtomicUsize::new(UNLIMITED);
static EXPLICIT: AtomicBool = AtomicBool::new(false);
static DEFAULT: OnceLock<usize> = OnceLock::new();

/// The cache's eviction hook: frees up to the requested bytes of decoded
/// chunks and returns how much the pool got back. With `wait` false it gives
/// up (frees nothing) when another thread is already evicting; an operator's
/// reservation waits, so a busy evictor never fails it.
pub type Evictor = fn(usize, bool) -> usize;

static EVICTOR: OnceLock<Evictor> = OnceLock::new();

/// Register the callback a reservation that would pass the limit calls before
/// it fails (the storage layer's decoded-chunk cache). The first call wins:
/// there is one hook per process, and it is stateless.
pub fn set_evictor(evict: Evictor) {
    let _ = EVICTOR.set(evict);
}

/// Reserve `bytes` from the pool without an [`Account`]: a storage chunk's
/// charge, which follows the chunk's own lifetime rather than a statement's.
pub fn reserve(bytes: usize) -> Result<Reservation> {
    acquire(Pool::Operator, bytes)?;
    Ok(Reservation {
        bytes,
        account: None,
        pool: Pool::Operator,
    })
}

/// The current process-wide limit, in bytes ([`UNLIMITED`] for none).
pub fn limit() -> usize {
    if EXPLICIT.load(Ordering::Relaxed) {
        LIMIT.load(Ordering::Relaxed)
    } else {
        *DEFAULT.get_or_init(default_limit)
    }
}

/// `SET memory_limit`: applies to every connection of the process, like
/// DuckDB's global `max_memory`.
pub fn set_limit(bytes: usize) {
    LIMIT.store(bytes, Ordering::Relaxed);
    EXPLICIT.store(true, Ordering::Relaxed);
    trim_cache();
}

/// `RESET memory_limit`: back to 80% of the memory the process may use.
pub fn reset_limit() {
    EXPLICIT.store(false, Ordering::Relaxed);
    trim_cache();
}

/// Bytes currently reserved across the process.
pub fn used() -> usize {
    USED.load(Ordering::Relaxed)
}

/// The highest [`used`] value since the process started.
pub fn peak() -> usize {
    PEAK.load(Ordering::Relaxed)
}

/// Bytes the decoded cache holds: clean decoded chunks, charged to their own
/// budget so a scan's cache can never take an operator's room (wave 136c-2).
static CACHE_USED: AtomicUsize = AtomicUsize::new(0);
static CACHE_PEAK: AtomicUsize = AtomicUsize::new(0);
/// The last explicit `SET decoded_cache_limit`; `CACHE_EXPLICIT` says
/// whether it applies.
static CACHE_LIMIT: AtomicUsize = AtomicUsize::new(0);
static CACHE_EXPLICIT: AtomicBool = AtomicBool::new(false);

/// Bytes held by the decoded cache across the process.
pub fn cache_used() -> usize {
    CACHE_USED.load(Ordering::Relaxed)
}

/// The highest [`cache_used`] value since the process started.
pub fn cache_peak() -> usize {
    CACHE_PEAK.load(Ordering::Relaxed)
}

/// The decoded cache's budget: the explicit setting, else no budget of its
/// own — clean chunks may use whatever `memory_limit` leaves free (see
/// `try_acquire`), the way DuckDB's buffer pool lives inside `max_memory`.
pub fn cache_limit() -> usize {
    if CACHE_EXPLICIT.load(Ordering::Relaxed) {
        CACHE_LIMIT.load(Ordering::Relaxed)
    } else {
        limit()
    }
}

/// `SET decoded_cache_limit`: cap clean decoded chunks below the free room.
pub fn set_cache_limit(bytes: usize) {
    CACHE_LIMIT.store(bytes, Ordering::Relaxed);
    CACHE_EXPLICIT.store(true, Ordering::Relaxed);
    trim_cache();
}

/// `RESET decoded_cache_limit`: back to the free room under `memory_limit`.
pub fn reset_cache_limit() {
    CACHE_EXPLICIT.store(false, Ordering::Relaxed);
    trim_cache();
}

/// Drop cache entries until the budget holds again, after it shrank.
fn trim_cache() {
    let used = cache_used();
    let limit = cache_limit();
    if used <= limit {
        return;
    }
    if let Some(evict) = EVICTOR.get() {
        evict(used - limit, true);
    }
}

/// Charge `bytes` to the decoded cache, evicting clean chunks to make room.
/// `None` when the budget is still full: the caller then decodes without
/// caching, so a query never fails because the cache is full.
pub fn reserve_decoded(bytes: usize) -> Option<Reservation> {
    reserve_in(Pool::Cache, bytes)
}

/// Charge `bytes` to the decoded cache only if its budget has room as it is:
/// never evicts. A chunk's first decode admits this way, so a one-pass scan
/// over a full cache neither flushes it nor queues on its evictor (wave 136c-4).
pub fn try_reserve_decoded(bytes: usize) -> Option<Reservation> {
    // `then`, not `then_some`: a reservation built eagerly for a failed
    // acquire would release bytes it never took when it drops.
    try_acquire(Pool::Cache, bytes).then(|| Reservation {
        bytes,
        account: None,
        pool: Pool::Cache,
    })
}

/// Which budget a reservation charges: operators (and the chunks a write
/// decodes, which are the truth until saved) are never evicted, so a scan's
/// cache yields to them first; clean decoded chunks are evictable and live in
/// whatever room is left. [`used`] stays what spill reads.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Pool {
    Operator,
    Cache,
}

/// DuckDB's `StringUtil::BytesToHumanReadableString`: binary units, one
/// truncated fractional digit (`1.5 GiB`, `488.2 MiB`, `0 bytes`).
pub fn human_bytes(bytes: usize) -> String {
    const UNITS: [&str; 6] = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut parts = [0usize; 6];
    parts[0] = bytes;
    for i in 1..parts.len() {
        parts[i] = parts[i - 1] / 1024;
        parts[i - 1] %= 1024;
    }
    for i in (1..parts.len()).rev() {
        if parts[i] != 0 {
            let fraction = parts[i - 1] * 10 / 1024;
            return format!("{}.{} {}", parts[i], fraction, UNITS[i]);
        }
    }
    format!(
        "{}{}",
        parts[0],
        if bytes == 1 { " byte" } else { " bytes" }
    )
}

/// `SET memory_limit`'s value syntax, after DuckDB's `ParseMemoryLimit`:
/// a number with an optional binary or decimal unit (`1.5GiB`, `512MB`,
/// `1024` is *not* accepted — DuckDB requires a unit), a percentage of the
/// available memory (`80%`), or `-1`/`none`/`null` for no limit.
pub fn parse_limit(input: &str) -> Result<usize> {
    let text = input.trim();
    if text.starts_with('-')
        || text.eq_ignore_ascii_case("none")
        || text.eq_ignore_ascii_case("null")
    {
        return Ok(UNLIMITED);
    }
    let end = text
        .char_indices()
        .take_while(|(_, c)| c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E' | '-'))
        .map(|(i, c)| i + c.len_utf8())
        .last()
        .unwrap_or(0);
    if end == 0 {
        return Err(PinkDbError::Execution(
            "Memory limit must have a number (e.g. SET memory_limit=1GB".to_string(),
        ));
    }
    let number: f64 = text[..end].parse().map_err(|_| {
        PinkDbError::Execution(format!("Memory limit has an invalid number: '{input}'"))
    })?;
    if number < 0.0 {
        return Ok(UNLIMITED);
    }
    // A bare `0` is the only unitless size: "no cache" for the decoded cache.
    let unit = text[end..].trim();
    if unit.is_empty() && number == 0.0 {
        return Ok(0);
    }
    if unit == "%" {
        if !(0.0..=100.0).contains(&number) {
            return Err(PinkDbError::Execution(format!(
                "Unable to parse valid percentage (input: {number}%)"
            )));
        }
        let Some(available) = available_memory() else {
            return Err(PinkDbError::Execution(
                "cannot read the available memory size; set an absolute memory_limit instead"
                    .to_string(),
            ));
        };
        return Ok((available as f64 * number / 100.0) as usize);
    }
    let multiplier: u64 = match unit.to_ascii_lowercase().as_str() {
        "byte" | "bytes" | "b" => 1,
        "kilobyte" | "kilobytes" | "kb" | "k" => 1_000,
        "megabyte" | "megabytes" | "mb" | "m" => 1_000_000,
        "gigabyte" | "gigabytes" | "gb" | "g" => 1_000_000_000,
        "terabyte" | "terabytes" | "tb" | "t" => 1_000_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        _ => {
            return Err(PinkDbError::Execution(format!(
                "Unknown unit for memory: '{}' (expected: KB, MB, GB, TB for 1000^i units or \
                 KiB, MiB, GiB, TiB for 1024^i units)",
                unit.to_ascii_lowercase()
            )));
        }
    };
    if !number.is_finite() {
        return Err(PinkDbError::Execution(format!(
            "Memory limit has an invalid number: '{input}'"
        )));
    }
    Ok((number * multiplier as f64) as usize)
}

/// The memory this process may use: physical RAM, capped by a cgroup limit
/// when the container sets one. `None` when neither can be read (wasm32).
pub fn available_memory() -> Option<usize> {
    available_memory_from(
        Path::new("/sys/fs/cgroup/memory.max"),
        Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
        physical_memory(),
    )
}

/// [`available_memory`] with its inputs injected, so a test can hand it fake
/// cgroup files and a fake RAM size.
pub(crate) fn available_memory_from(
    cgroup_v2: &Path,
    cgroup_v1: &Path,
    physical: Option<usize>,
) -> Option<usize> {
    // A cgroup "no limit" is either the literal `max` (v2, unparseable here)
    // or a number near the address-space size (v1); take the smaller of the
    // two candidates so such a value cannot lift the limit above RAM.
    let cgroup = read_cgroup(cgroup_v2).or_else(|| read_cgroup(cgroup_v1));
    match (physical, cgroup) {
        (Some(ram), Some(cgroup)) => Some(ram.min(cgroup)),
        (Some(ram), None) => Some(ram),
        (None, Some(cgroup)) => Some(cgroup),
        (None, None) => None,
    }
}

fn read_cgroup(path: &Path) -> Option<usize> {
    let text = std::fs::read_to_string(path).ok()?;
    let value: u64 = text.trim().parse().ok()?;
    // v1 writes ~`u64::MAX` for "unlimited"; `max` (v2) is not a number at all.
    (value < (1u64 << 60)).then(|| value.min(usize::MAX as u64) as usize)
}

/// 80% of [`available_memory`], DuckDB's default; no limit when the size is
/// unknown.
pub fn default_limit() -> usize {
    default_from(available_memory())
}

fn default_from(available: Option<usize>) -> usize {
    // `* 8` first, like DuckDB, without overflowing on a huge cgroup value.
    available.map_or(UNLIMITED, |memory| (memory as u128 * 8 / 10) as usize)
}

#[cfg(target_os = "linux")]
fn physical_memory() -> Option<usize> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: usize = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

#[cfg(target_os = "macos")]
fn physical_memory() -> Option<usize> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    String::from_utf8(out.stdout).ok()?.trim().parse().ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn physical_memory() -> Option<usize> {
    None
}

/// One statement's live reservations, so its profile can report the peak
/// without reading the process-wide high-water mark.
#[derive(Clone, Default)]
pub struct Account(Option<Arc<AccountState>>);

#[derive(Default)]
struct AccountState {
    held: AtomicUsize,
    peak: AtomicUsize,
}

impl Account {
    pub fn new() -> Account {
        Account(Some(Arc::new(AccountState::default())))
    }

    /// Reserve `bytes` from the pool and from this account, or fail with the
    /// out-of-memory error without reserving anything.
    pub fn reserve(&self, bytes: usize) -> Result<Reservation> {
        acquire(Pool::Operator, bytes)?;
        let reservation = Reservation {
            bytes,
            account: self.0.clone(),
            pool: Pool::Operator,
        };
        reservation.charge_account(bytes);
        Ok(reservation)
    }

    /// A zero-byte reservation that grows into this account: an operator
    /// state that starts empty (wave 136a).
    pub fn empty_reservation(&self) -> Reservation {
        Reservation {
            bytes: 0,
            account: self.0.clone(),
            pool: Pool::Operator,
        }
    }

    /// The largest number of bytes this account held at once.
    pub fn peak(&self) -> usize {
        self.0
            .as_ref()
            .map_or(0, |state| state.peak.load(Ordering::Relaxed))
    }
}

/// Bytes reserved from the pool, released when the reservation drops. A
/// reservation can grow or shrink as the state it stands for changes; a
/// failed grow leaves it — and the pool — unchanged.
pub struct Reservation {
    bytes: usize,
    account: Option<Arc<AccountState>>,
    pool: Pool,
}

impl Reservation {
    /// A reservation outside any account: holds nothing.
    pub fn empty() -> Reservation {
        Reservation {
            bytes: 0,
            account: None,
            pool: Pool::Operator,
        }
    }

    /// Whether this charges the decoded cache rather than the operator pool
    /// (a test hook: per-entry accounting, which tests running alongside
    /// cannot move).
    #[cfg(test)]
    pub(crate) fn is_cache(&self) -> bool {
        matches!(self.pool, Pool::Cache)
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Resize to `bytes`, growing or shrinking by the difference: what an
    /// operator calls after each chunk or partial it adds to its state.
    pub fn resize(&mut self, bytes: usize) -> Result<()> {
        match bytes.cmp(&self.bytes) {
            std::cmp::Ordering::Greater => self.grow(bytes - self.bytes),
            std::cmp::Ordering::Less => {
                self.shrink(self.bytes - bytes);
                Ok(())
            }
            std::cmp::Ordering::Equal => Ok(()),
        }
    }

    /// Reserve `add` more bytes, failing (and keeping the current size) when
    /// the pool would pass its limit.
    pub fn grow(&mut self, add: usize) -> Result<()> {
        acquire(self.pool, add)?;
        self.bytes = self.bytes.saturating_add(add);
        self.charge_account(add);
        Ok(())
    }

    /// Give `sub` bytes back (never more than held).
    pub fn shrink(&mut self, sub: usize) {
        let sub = sub.min(self.bytes);
        release(self.pool, sub);
        self.bytes -= sub;
        if let Some(state) = &self.account {
            uncharge(state, sub);
        }
    }

    fn charge_account(&self, add: usize) {
        if let Some(state) = &self.account {
            let held = state.held.fetch_add(add, Ordering::Relaxed) + add;
            state.peak.fetch_max(held, Ordering::Relaxed);
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        release(self.pool, self.bytes);
        if let Some(state) = &self.account {
            uncharge(state, self.bytes);
        }
    }
}

fn acquire(pool: Pool, bytes: usize) -> Result<()> {
    if try_acquire(pool, bytes) {
        return Ok(());
    }
    // Over the limit. The storage layer caches decoded chunks it can drop and
    // decode again, so keep asking it to free room until the request fits or
    // it finds nothing left to drop: a reservation fails only when everything
    // charged is still in use. Asking for twice the need leaves the retry room
    // that a concurrent decoder would otherwise take, and the loop stops on
    // the evictor's own progress rather than a round count, so a pool holding
    // evictable chunks always answers a request that fits under the limit.
    // The evictor only releases reservations, so it never re-enters here.
    let Some(evict) = EVICTOR.get() else {
        return Err(out_of_memory(bytes));
    };
    while evict(bytes.saturating_mul(2), true) > 0 {
        if try_acquire(pool, bytes) {
            return Ok(());
        }
    }
    Err(out_of_memory(bytes))
}

/// [`reserve_decoded`]'s body: the cache budget, with the same
/// evict-then-retry shape as an operator reservation, but a `None` answer
/// instead of an out-of-memory error.
fn reserve_in(pool: Pool, bytes: usize) -> Option<Reservation> {
    if try_acquire(pool, bytes) {
        return Some(Reservation {
            bytes,
            account: None,
            pool,
        });
    }
    // A cache admission never waits for another thread's eviction pass: when
    // the evictor is busy it frees nothing, and the chunk is decoded for its
    // reader alone (wave 136c-4).
    let evict = EVICTOR.get()?;
    while evict(bytes.saturating_mul(2), false) > 0 {
        if try_acquire(pool, bytes) {
            return Some(Reservation {
                bytes,
                account: None,
                pool,
            });
        }
    }
    None
}

/// Charge `bytes` if the budget has room; `false` when they would pass it.
///
/// Both pools share `memory_limit`, the way DuckDB's buffer pool shares
/// `max_memory`: the cache may only take what the operators leave free, so an
/// operator reservation that does not fit evicts clean chunks (`acquire`)
/// before it fails. An explicit `decoded_cache_limit` caps the cache below
/// that free room; `-1` in the cache setting means the whole pool.
fn try_acquire(pool: Pool, bytes: usize) -> bool {
    let limit = limit();
    let counter = match pool {
        Pool::Operator => &USED,
        Pool::Cache => &CACHE_USED,
    };
    let mut used = counter.load(Ordering::Relaxed);
    loop {
        let next = used.saturating_add(bytes);
        let (operators, cache) = if pool == Pool::Operator {
            (next, CACHE_USED.load(Ordering::Relaxed))
        } else {
            (USED.load(Ordering::Relaxed), next)
        };
        let fits = operators.saturating_add(cache) <= limit
            && (pool == Pool::Operator
                || !CACHE_EXPLICIT.load(Ordering::Relaxed)
                || next <= CACHE_LIMIT.load(Ordering::Relaxed));
        if !fits {
            return false;
        }
        match counter.compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => {
                let peak = match pool {
                    Pool::Operator => &PEAK,
                    Pool::Cache => &CACHE_PEAK,
                };
                peak.fetch_max(next, Ordering::Relaxed);
                return true;
            }
            Err(current) => used = current,
        }
    }
}

fn release(pool: Pool, bytes: usize) {
    if bytes > 0 {
        let counter = match pool {
            Pool::Operator => &USED,
            Pool::Cache => &CACHE_USED,
        };
        let before = counter.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(before >= bytes, "memory pool released more than it holds");
    }
}

fn uncharge(state: &AccountState, bytes: usize) {
    if bytes > 0 {
        state.held.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// DuckDB's `OutOfMemoryException` wording for a failed allocation of
/// `requested` bytes, with the pool's current numbers.
fn out_of_memory(requested: usize) -> PinkDbError {
    PinkDbError::Execution(format!(
        "Out of Memory Error: failed to allocate data of size {} ({}/{} used)",
        human_bytes(requested),
        human_bytes(used()),
        human_bytes(limit())
    ))
}

/// One lock for tests that change the process-wide limit: other tests of the
/// same binary must not run with a tiny limit.
#[doc(hidden)]
pub fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// True for the setting name `SET`/`RESET`/`current_setting` treat as the
/// process-wide memory limit (DuckDB's `max_memory`, aliased `memory_limit`).
/// The decoded cache's own budget (wave 136c-2): like `memory_limit` a
/// process-wide knob, not session state.
pub fn is_cache_limit_setting(name: &str) -> bool {
    name.eq_ignore_ascii_case("decoded_cache_limit")
}

pub fn is_limit_setting(name: &str) -> bool {
    name.eq_ignore_ascii_case("memory_limit") || name.eq_ignore_ascii_case("max_memory")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duckdb_units() {
        assert_eq!(parse_limit("1KB").unwrap(), 1000);
        assert_eq!(parse_limit("1.5GiB").unwrap(), 3 * 1024 * 1024 * 1024 / 2);
        assert_eq!(parse_limit("512MB").unwrap(), 512 * 1000 * 1000);
        assert_eq!(parse_limit(" 2 GB ").unwrap(), 2 * 1000 * 1000 * 1000);
        assert_eq!(parse_limit("1 TiB").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_limit("1t").unwrap(), 1000 * 1000 * 1000 * 1000);
        assert_eq!(parse_limit("3 bytes").unwrap(), 3);
        assert_eq!(parse_limit("1 byte").unwrap(), 1);
    }

    #[test]
    fn parses_none_and_negative_as_unlimited() {
        assert_eq!(parse_limit("-1").unwrap(), UNLIMITED);
        assert_eq!(parse_limit("none").unwrap(), UNLIMITED);
        assert_eq!(parse_limit("NULL").unwrap(), UNLIMITED);
    }

    #[test]
    fn rejects_invalid_forms_with_duckdb_wording() {
        let err = parse_limit("1GBX").unwrap_err().to_string();
        assert!(err.contains("Unknown unit for memory: 'gbx'"), "{err}");
        assert!(err.contains("expected: KB, MB, GB, TB"), "{err}");
        let err = parse_limit("GB").unwrap_err().to_string();
        assert!(err.contains("Memory limit must have a number"), "{err}");
        // DuckDB takes no bare byte count: a number needs a unit.
        let err = parse_limit("1024").unwrap_err().to_string();
        assert!(err.contains("Unknown unit for memory: ''"), "{err}");
        let err = parse_limit("2e").unwrap_err().to_string();
        assert!(err.contains("invalid number"), "{err}");
    }

    #[test]
    fn percentages_scale_the_available_memory() {
        let dir = std::env::temp_dir().join(format!("pinkdb-mem-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let v2 = dir.join("memory.max");
        let v1 = dir.join("memory.limit_in_bytes");
        std::fs::write(&v2, "max").unwrap();
        std::fs::write(&v1, "9223372036854771712").unwrap();
        let available = available_memory_from(&v2, &v1, Some(16 * 1024 * 1024 * 1024));
        assert_eq!(available, Some(16 * 1024 * 1024 * 1024));
        // A cgroup limit below RAM wins.
        std::fs::write(&v2, format!("{}", 4u64 * 1024 * 1024 * 1024)).unwrap();
        assert_eq!(
            available_memory_from(&v2, &v1, Some(16 * 1024 * 1024 * 1024)),
            Some(4 * 1024 * 1024 * 1024)
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn default_is_eighty_percent_of_available_memory() {
        let dir = std::env::temp_dir().join(format!("pinkdb-mem-default-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let v2 = dir.join("memory.max");
        let v1 = dir.join("memory.limit_in_bytes");
        std::fs::write(&v2, format!("{}", 1024 * 1024 * 1024)).unwrap();
        let available = available_memory_from(&v2, &v1, Some(16 * 1024 * 1024 * 1024)).unwrap();
        assert_eq!(available, 1024 * 1024 * 1024);
        assert_eq!(default_from(Some(available)), 858_993_459);
        assert_eq!(default_from(None), UNLIMITED);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn formats_like_duckdb() {
        assert_eq!(human_bytes(0), "0 bytes");
        assert_eq!(human_bytes(1), "1 byte");
        assert_eq!(human_bytes(1023), "1023 bytes");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(512 * 1000 * 1000), "488.2 MiB");
        assert_eq!(
            human_bytes(1024 * 1024 * 1024 + 512 * 1024 * 1024),
            "1.5 GiB"
        );
        assert_eq!(human_bytes(2 * 1000 * 1000 * 1000), "1.8 GiB");
        assert_eq!(human_bytes(UNLIMITED), "16383.9 PiB");
    }
}
