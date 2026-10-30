//! Console login sessions (wave 121).
//!
//! A login creates a server-side session: a 256-bit random id (the cookie),
//! a separate 256-bit CSRF token, and the identity statements run under. The
//! password is verified once, through the engine, and never kept: later
//! statements adopt the user with [`pinkdb::Connection::impersonate`].
//!
//! Sessions end on logout, after [`crate::UiOptions::session_idle`] without
//! a request, after [`crate::UiOptions::session_lifetime`] in any case, and
//! when the store's user is dropped. The table is capped
//! ([`crate::UiOptions::max_sessions`]); the least recently used session
//! makes room for a new login.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use pinkdb::{CancelFlag, Connection, Database};

use crate::auth::constant_time_eq;
use crate::security::random_token;

/// Whose privileges a session's statements run under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Identity {
    /// The trusted operator session: the no-auth console of a store without
    /// users, or the bearer-token console (the token is the operator's own
    /// secret, as in wave 78).
    Operator,
    /// A store user, verified at login (HTTP Basic mode): every statement
    /// runs under that user's grants.
    User(String),
}

/// Where a session was started from (shown in the session list).
#[derive(Clone, Debug, Default)]
pub struct Origin {
    pub ip: Option<IpAddr>,
    pub user_agent: String,
}

/// One logged-in browser.
pub struct UiSession {
    /// The cookie value.
    id: String,
    /// The id the session list shows (not the cookie: listing sessions must
    /// not hand out credentials).
    pub public_id: String,
    /// Where the sign-in came from.
    pub origin: Origin,
    /// When it signed in (wall clock, for the session list).
    pub started: SystemTime,
    /// A TOTP secret being enrolled, until the user confirms a code.
    pub pending_totp: Mutex<Option<String>>,
    /// The CSRF token every `POST` must echo.
    csrf: String,
    /// Whose privileges the statements run under.
    pub identity: Identity,
    /// `current_user()` of the session: the owner of its worksheets.
    pub user: String,
    created: Instant,
    last_seen: Mutex<Instant>,
    /// The running worksheet queries, by the client's run id.
    runs: Mutex<HashMap<String, CancelFlag>>,
    /// Whether an ask is running.
    asking: Mutex<bool>,
}

impl UiSession {
    fn new(identity: Identity, user: String, origin: Origin) -> Self {
        let now = Instant::now();
        UiSession {
            id: random_token(),
            public_id: random_token().chars().take(12).collect(),
            origin,
            started: SystemTime::now(),
            pending_totp: Mutex::new(None),
            csrf: random_token(),
            identity,
            user,
            created: now,
            last_seen: Mutex::new(now),
            runs: Mutex::new(HashMap::new()),
            asking: Mutex::new(false),
        }
    }

    /// A one-request session for a personal access token: not in the
    /// table, no cookie, and its CSRF token is never sent anywhere.
    pub fn transient(identity: Identity, user: String) -> Arc<Self> {
        Arc::new(Self::new(identity, user, Origin::default()))
    }

    /// The CSRF token (sent to the browser once, at login and on
    /// `GET /api/session`).
    pub fn csrf(&self) -> &str {
        &self.csrf
    }

    /// Does `token` match the session's CSRF token (constant time)?
    pub fn csrf_matches(&self, token: &str) -> bool {
        constant_time_eq(self.csrf.as_bytes(), token.as_bytes())
    }

    /// A connection that runs statements as this session's identity.
    pub fn connect(&self, db: &Database) -> Result<Connection, String> {
        let mut conn = db.connect();
        if let Identity::User(user) = &self.identity {
            conn.impersonate(user)
                .map_err(|_| "the session's user no longer exists".to_string())?;
            conn.require_login(true);
        }
        Ok(conn)
    }

    /// Register a running query; refused over `limit` or for a run id that
    /// is already running.
    pub fn begin_run(&self, run_id: &str, limit: usize) -> Result<CancelFlag, RunRefused> {
        let mut runs = self.runs.lock().unwrap_or_else(|p| p.into_inner());
        if runs.contains_key(run_id) {
            return Err(RunRefused::Duplicate);
        }
        if runs.len() >= limit.max(1) {
            return Err(RunRefused::Busy);
        }
        let flag = CancelFlag::new();
        runs.insert(run_id.to_string(), flag.clone());
        Ok(flag)
    }

    /// Forget a finished query.
    pub fn end_run(&self, run_id: &str) {
        self.runs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(run_id);
    }

    /// Cancel one of this session's queries. `false` when it is not running.
    pub fn cancel(&self, run_id: &str) -> bool {
        match self
            .runs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(run_id)
        {
            Some(flag) => {
                flag.cancel();
                true
            }
            None => false,
        }
    }

    /// Cancel every running query (logout).
    fn cancel_all(&self) {
        for flag in self.runs.lock().unwrap_or_else(|p| p.into_inner()).values() {
            flag.cancel();
        }
    }

    /// Claim the session's ask slot (one ask at a time per session).
    pub fn begin_ask(&self) -> bool {
        let mut asking = self.asking.lock().unwrap_or_else(|p| p.into_inner());
        if *asking {
            false
        } else {
            *asking = true;
            true
        }
    }

    /// Release the ask slot.
    pub fn end_ask(&self) {
        *self.asking.lock().unwrap_or_else(|p| p.into_inner()) = false;
    }
}

/// Why a query was not started.
#[derive(Debug, PartialEq, Eq)]
pub enum RunRefused {
    /// The session runs as many queries as it may.
    Busy,
    /// The run id is already running.
    Duplicate,
}

/// The session table.
pub struct Sessions {
    map: Mutex<HashMap<String, Arc<UiSession>>>,
    idle: Duration,
    lifetime: Duration,
    max: usize,
}

impl Sessions {
    /// An empty table with the given limits.
    pub fn new(idle: Duration, lifetime: Duration, max: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            idle,
            lifetime,
            max: max.max(1),
        }
    }

    /// Start a session. Returns it (its id is the cookie value).
    pub fn create(&self, identity: Identity, user: String, origin: Origin) -> Arc<UiSession> {
        let now = Instant::now();
        let session = Arc::new(UiSession::new(identity, user, origin));
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        map.retain(|_, s| !self.expired(s, now));
        while map.len() >= self.max {
            // Evict the least recently used session.
            let Some(oldest) = map
                .iter()
                .min_by_key(|(_, s)| *s.last_seen.lock().unwrap_or_else(|p| p.into_inner()))
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            if let Some(evicted) = map.remove(&oldest) {
                evicted.cancel_all();
            }
        }
        map.insert(session.id.clone(), Arc::clone(&session));
        session
    }

    /// The live session for a cookie value, refreshing its idle clock.
    pub fn get(&self, id: &str) -> Option<Arc<UiSession>> {
        let now = Instant::now();
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        let session = map.get(id).cloned()?;
        // The lookup is by the full random id; a timing difference between a
        // hit and a miss reveals nothing about any other id.
        if self.expired(&session, now) {
            map.remove(id);
            session.cancel_all();
            return None;
        }
        *session.last_seen.lock().unwrap_or_else(|p| p.into_inner()) = now;
        Some(session)
    }

    /// End a session (logout).
    pub fn remove(&self, id: &str) {
        let removed = self
            .map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id);
        if let Some(session) = removed {
            session.cancel_all();
        }
    }

    /// The id of a session (for logout).
    pub fn id_of(session: &UiSession) -> &str {
        &session.id
    }

    /// The live sessions of `user` (case-insensitive), newest first.
    pub fn of_user(&self, user: &str) -> Vec<Arc<UiSession>> {
        let now = Instant::now();
        let map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<Arc<UiSession>> = map
            .values()
            .filter(|s| s.user.eq_ignore_ascii_case(user) && !self.expired(s, now))
            .cloned()
            .collect();
        out.sort_by_key(|s| std::cmp::Reverse(s.started));
        out
    }

    /// End `user`'s sessions, except the one whose cookie is `keep`.
    /// Returns how many ended.
    pub fn end_user(&self, user: &str, keep: Option<&str>) -> usize {
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        let ids: Vec<String> = map
            .iter()
            .filter(|(id, s)| s.user.eq_ignore_ascii_case(user) && Some(id.as_str()) != keep)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            if let Some(session) = map.remove(id) {
                session.cancel_all();
            }
        }
        ids.len()
    }

    /// End one of `user`'s sessions by its public id. `false` when there is
    /// no such session of that user.
    pub fn end_public(&self, user: &str, public_id: &str) -> bool {
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        let id = map
            .iter()
            .find(|(_, s)| {
                s.user.eq_ignore_ascii_case(user)
                    && constant_time_eq(s.public_id.as_bytes(), public_id.as_bytes())
            })
            .map(|(id, _)| id.clone());
        match id.and_then(|id| map.remove(&id)) {
            Some(session) => {
                session.cancel_all();
                true
            }
            None => false,
        }
    }

    /// When a session was last used.
    pub fn last_seen(session: &UiSession) -> Instant {
        *session.last_seen.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn expired(&self, session: &UiSession, now: Instant) -> bool {
        let last = *session.last_seen.lock().unwrap_or_else(|p| p.into_inner());
        now.duration_since(last) >= self.idle
            || now.duration_since(session.created) >= self.lifetime
    }

    /// How many sessions are live (tests).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_expire_when_idle() {
        let sessions = Sessions::new(Duration::from_millis(30), Duration::from_secs(60), 8);
        let s = sessions.create(Identity::Operator, "pinkdb".into(), Origin::default());
        let id = Sessions::id_of(&s).to_string();
        assert!(sessions.get(&id).is_some());
        std::thread::sleep(Duration::from_millis(60));
        assert!(sessions.get(&id).is_none());
    }

    #[test]
    fn the_table_is_capped() {
        let sessions = Sessions::new(Duration::from_secs(60), Duration::from_secs(60), 2);
        let first = sessions.create(Identity::Operator, "a".into(), Origin::default());
        let first_id = Sessions::id_of(&first).to_string();
        std::thread::sleep(Duration::from_millis(5));
        sessions.create(Identity::Operator, "b".into(), Origin::default());
        std::thread::sleep(Duration::from_millis(5));
        sessions.create(Identity::Operator, "c".into(), Origin::default());
        assert_eq!(sessions.len(), 2);
        assert!(sessions.get(&first_id).is_none(), "the oldest was evicted");
    }

    #[test]
    fn runs_are_limited_and_cancellable() {
        let sessions = Sessions::new(Duration::from_secs(60), Duration::from_secs(60), 2);
        let s = sessions.create(Identity::Operator, "a".into(), Origin::default());
        let flag = s.begin_run("r1", 1).unwrap();
        assert_eq!(s.begin_run("r2", 1).unwrap_err(), RunRefused::Busy);
        assert_eq!(s.begin_run("r1", 2).unwrap_err(), RunRefused::Duplicate);
        assert!(s.cancel("r1"));
        assert!(flag.is_cancelled());
        s.end_run("r1");
        assert!(!s.cancel("r1"));
        assert!(s.csrf_matches(s.csrf()));
        assert!(!s.csrf_matches("nope"));
    }

    #[test]
    fn a_users_sessions_are_listed_and_ended() {
        let sessions = Sessions::new(Duration::from_secs(60), Duration::from_secs(60), 8);
        let user = |name: &str| Identity::User(name.to_string());
        let a1 = sessions.create(user("ana"), "ana".into(), Origin::default());
        let a2 = sessions.create(user("ana"), "ana".into(), Origin::default());
        let b = sessions.create(user("bob"), "bob".into(), Origin::default());
        assert_eq!(sessions.of_user("ANA").len(), 2);
        assert!(
            !sessions.end_public("bob", &a1.public_id),
            "not bob's session"
        );
        assert!(sessions.end_public("ana", &a1.public_id));
        assert_eq!(sessions.of_user("ana").len(), 1);
        sessions.create(user("ana"), "ana".into(), Origin::default());
        assert_eq!(sessions.end_user("ana", Some(Sessions::id_of(&a2))), 1);
        assert!(sessions.get(Sessions::id_of(&a2)).is_some(), "kept");
        assert!(sessions.get(Sessions::id_of(&b)).is_some());
        assert_eq!(sessions.end_user("ana", None), 1);
        assert!(sessions.of_user("ana").is_empty());
    }
}
