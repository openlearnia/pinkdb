//! Role-based access control (wave 48): users, roles, privileges, and the
//! per-statement check an authenticated session runs under.
//!
//! # Model
//!
//! A store carries **principals** — *users* (they have an Argon2id password
//! hash and may log in) and *roles* (they never log in; they exist to be
//! granted to users and to other roles) — plus three kinds of grants:
//!
//! | Scope | Privilege | Meaning |
//! |---|---|---|
//! | table | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | data access to one `db.table` |
//! | database | `CREATE` | schema DDL in that database: `CREATE/DROP/ALTER TABLE`, views, indexes, procedures, materialized views |
//! | global | `ADMIN` | manage users/roles/grants, create/drop databases, `CHECKPOINT`, `LOAD EXTENSION`, `CALL` — and every other privilege check is passed |
//!
//! Role membership (`GRANT role TO principal`) is transitive: a principal
//! holds every privilege of every role it is a member of, directly or
//! through other roles. Cycles are refused at `GRANT` time. A membership
//! never implies `ADMIN` unless the role itself was granted `ADMIN`.
//!
//! There are no object owners: `CREATE` on a database lets a session change
//! its schema, but the creator is not automatically granted any table
//! privilege — grant `SELECT`/`INSERT`/… explicitly.
//!
//! # Enforcement
//!
//! Privilege checks happen **only for authenticated sessions** — a
//! [`crate::Connection`] whose [`crate::Connection::login`] succeeded. The
//! embedded API (a plain [`crate::Database::connect`], the FFI, wasm) carries
//! no principal and stays trusted: every statement runs exactly as before
//! this wave. The network server authenticates a connection when the store
//! has any user (see the server crate's docs) and then each statement runs
//! under [`check_statement`] with that connection's principal.
//!
//! `check_statement` is exact for the statement's own plan: it derives the
//! tables the statement reads (subqueries, joins, CTEs and `EXPLAIN` bodies
//! included) and writes, and requires the matching privileges. Statements
//! that write *nothing* the plan can name — `CALL` (whose body binds only at
//! call time) and the process/store-level statements — require `ADMIN`.
//! Metadata statements (`SHOW …, DESCRIBE`) need no privilege: they report
//! names and schemas, not data.
//!
//! # Passwords
//!
//! Passwords are hashed with Argon2id (the same OWASP-recommended
//! configuration the at-rest encryption uses: 19 MiB, t=2, p=1, 16-byte
//! salt) and stored as the PHC string
//! (`$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`). The plaintext exists
//! only in the parsed statement and in the process memory of the statement
//! execution; the `.pink` catalog and the WAL only ever carry the PHC hash
//! (the WAL logs the rewritten `… PASSWORD_HASH '<phc>'` form — see
//! [`wal_statement_text`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use crate::plan::LogicalPlan;
use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result};

/// The salt length of a fresh password hash, in bytes (the at-rest
/// encryption's `DEFAULT_SALT_LEN`).
const SALT_LEN: usize = 16;

/// One privilege. Table privileges apply to a `(database, table)` pair,
/// [`Privilege::Create`] to a database, and `ADMIN` is global (it is not a
/// [`Privilege`] variant: `GRANT ADMIN` has its own statement form with no
/// `ON` clause).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    /// Database-level DDL: create, drop or alter schema objects in that
    /// database. Grants no data access.
    Create,
}

impl Privilege {
    /// Every table privilege, in the canonical order `GRANT ALL ON TABLE`
    /// expands to.
    pub const TABLE: [Privilege; 4] = [
        Privilege::Select,
        Privilege::Insert,
        Privilege::Update,
        Privilege::Delete,
    ];

    /// Every database privilege (`GRANT ALL ON DATABASE`).
    pub const DATABASE: [Privilege; 1] = [Privilege::Create];

    /// The SQL word the privilege is written with.
    pub fn keyword(self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Create => "CREATE",
        }
    }

    /// The scope the privilege is granted at: `TABLE` or `DATABASE`.
    pub fn scope(self) -> &'static str {
        match self {
            Privilege::Create => "DATABASE",
            _ => "TABLE",
        }
    }

    /// Parse a privilege word (`SELECT`, `INSERT`, `UPDATE`, `DELETE`,
    /// `CREATE`), case-insensitively.
    pub fn parse(word: &str) -> Option<Privilege> {
        Some(match word.to_ascii_uppercase().as_str() {
            "SELECT" => Privilege::Select,
            "INSERT" => Privilege::Insert,
            "UPDATE" => Privilege::Update,
            "DELETE" => Privilege::Delete,
            "CREATE" => Privilege::Create,
            _ => return None,
        })
    }

    /// The persisted tag (`FLAG_RBAC` sections of the `.pink` format).
    pub(crate) fn tag(self) -> u8 {
        match self {
            Privilege::Select => 0,
            Privilege::Insert => 1,
            Privilege::Update => 2,
            Privilege::Delete => 3,
            Privilege::Create => 4,
        }
    }

    /// Decode a persisted tag; unknown tags are a corrupt-file error.
    pub(crate) fn from_tag(tag: u8) -> Result<Privilege> {
        Ok(match tag {
            0 => Privilege::Select,
            1 => Privilege::Insert,
            2 => Privilege::Update,
            3 => Privilege::Delete,
            4 => Privilege::Create,
            other => {
                return Err(PinkDbError::Execution(format!(
                    "corrupt file: unknown privilege tag {other}"
                )));
            }
        })
    }

    /// `SELECT, INSERT`: the list form the grant descriptions use.
    pub(crate) fn list(privs: &[Privilege]) -> String {
        privs
            .iter()
            .map(|p| p.keyword())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The object a grant applies to, in **canonical key form** (lowercased
/// database and table names): the map keys of the store's grant table and
/// the form [`check_statement`] checks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum GrantObject {
    /// The global `ADMIN` privilege.
    Admin,
    /// Membership in a role (a `GRANT role TO principal`).
    Role { role: String },
    /// A table in one database.
    Table { db: String, table: String },
    /// A whole database (the `CREATE` privilege).
    Database { db: String },
}

impl GrantObject {
    /// `ADMIN` / `ROLE analyst` / `TABLE main.t` / `DATABASE main`: the
    /// label the grant descriptions use.
    pub fn label(&self) -> String {
        match self {
            GrantObject::Admin => "ADMIN".to_string(),
            GrantObject::Role { role } => format!("ROLE {role}"),
            GrantObject::Table { db, table } => format!("TABLE {db}.{table}"),
            GrantObject::Database { db } => format!("DATABASE {db}"),
        }
    }
}

/// The resolved target of a `GRANT`/`REVOKE` in the AST: unlike
/// [`GrantSpec`] the database qualifier is not resolved yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantTarget {
    /// `GRANT ADMIN TO <principal>` / `REVOKE ADMIN FROM <principal>`.
    Admin { principal: String },
    /// `GRANT <role> TO <principal>` / `REVOKE <role> FROM <principal>`:
    /// role membership.
    Role { role: String, principal: String },
    /// `GRANT <priv>[, …] ON TABLE [db.]t TO <principal>`.
    Table {
        privs: GrantPrivs,
        db: Option<String>,
        table: String,
        principal: String,
    },
    /// `GRANT <priv>[, …] ON DATABASE db TO <principal>`.
    Database {
        privs: GrantPrivs,
        db: String,
        principal: String,
    },
}

/// A parsed `GRANT`/`REVOKE` privilege list: `ALL [PRIVILEGES]`, or an
/// explicit list. `ALL`'s expansion depends on the `ON` scope, so it is
/// expanded (and validated) at bind time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantPrivs {
    /// `ALL [PRIVILEGES]` was written.
    pub all: bool,
    /// The explicitly written privileges, in written order.
    pub privs: Vec<Privilege>,
}

impl GrantPrivs {
    /// `ALL [PRIVILEGES]`.
    pub fn all() -> GrantPrivs {
        GrantPrivs {
            all: true,
            privs: Vec::new(),
        }
    }

    /// An explicit privilege list.
    pub fn list(privs: Vec<Privilege>) -> GrantPrivs {
        GrantPrivs { all: false, privs }
    }

    /// Expand for a table grant: `ALL` means `SELECT, INSERT, UPDATE,
    /// DELETE`.
    pub fn expand_table(&self) -> Vec<Privilege> {
        if self.all {
            Privilege::TABLE.to_vec()
        } else {
            self.privs.clone()
        }
    }

    /// Expand for a database grant: `ALL` means `CREATE`.
    pub fn expand_database(&self) -> Vec<Privilege> {
        if self.all {
            Privilege::DATABASE.to_vec()
        } else {
            self.privs.clone()
        }
    }
}

/// A bound `GRANT`/`REVOKE`: canonical keys plus the display form the result
/// reports.
///
/// The binder resolves and validates every name (the principal, the table or
/// database) against the catalog, so the executor only applies the change;
/// it re-validates existence for the multi-writer paths, where a concurrent
/// commit could have removed the object between bind and execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantSpec {
    /// The privileges to grant (canonical order, no duplicates). Empty for a
    /// role membership or `ADMIN` grant.
    pub privs: Vec<Privilege>,
    /// The grant's object, in canonical (lowercased) key form.
    pub object: GrantObject,
    /// The receiving principal, in canonical key form.
    pub principal: String,
    /// `SELECT ON TABLE main.t TO alice` — the human-readable form of the
    /// grant, built at bind time from the as-written names, reported in the
    /// statement's result.
    pub description: String,
}

/// A password as written in `CREATE USER … PASSWORD …`.
///
/// The statement is bound several times on the multi-writer paths (the
/// read-lock bind, the rebind under the exclusive lock, the rebind after a
/// lease refresh), but an Argon2id derivation is deliberately expensive, so
/// the parsed AST carries the hash's memo: the first bind derives it, every
/// later bind reuses it, and the plan stored in the catalog gets exactly the
/// string the WAL logs.
#[derive(Clone)]
pub struct PasswordSource {
    /// The plaintext as written; redacted by this type's `Debug`.
    plain: Option<String>,
    /// A pre-hashed PHC string (`PASSWORD_HASH '<phc>'`, the form WAL replay
    /// logs and re-parses).
    prehashed: Option<String>,
    /// The derived PHC hash (filled on the first bind).
    derived: OnceLock<String>,
}

impl PasswordSource {
    /// A password as the user wrote it (unhashed).
    pub fn plaintext(plain: String) -> PasswordSource {
        PasswordSource {
            plain: Some(plain),
            prehashed: None,
            derived: OnceLock::new(),
        }
    }

    /// An already-hashed password (`PASSWORD_HASH '<phc>'`).
    pub fn prehashed(hash: String) -> PasswordSource {
        PasswordSource {
            plain: None,
            prehashed: Some(hash),
            derived: OnceLock::new(),
        }
    }

    /// The PHC hash for this password: derived once per statement (a
    /// pre-hashed source is returned as-is).
    pub fn hashed(&self) -> Result<&str> {
        if let Some(h) = &self.prehashed {
            return Ok(h);
        }
        if let Some(h) = self.derived.get() {
            return Ok(h.as_str());
        }
        let Some(plain) = &self.plain else {
            return Err(PinkDbError::Execution(
                "internal error: password source holds neither text nor hash".to_string(),
            ));
        };
        let derived = hash_password(plain)?;
        Ok(self.derived.get_or_init(|| derived).as_str())
    }

    /// The pre-hashed form, when this source was written as
    /// `PASSWORD_HASH '<phc>'` (the binder validates it up front).
    pub fn prehashed_hash(&self) -> Option<&str> {
        self.prehashed.as_deref()
    }

    /// Was this written as the empty plaintext (`PASSWORD ''`)?
    pub fn is_empty_plaintext(&self) -> bool {
        self.plain.as_deref() == Some("")
    }
}

/// `Debug` must never print the plaintext (plans and ASTs are printed on
/// test failure and in engine diagnostics).
impl std::fmt::Debug for PasswordSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PasswordSource(<redacted>)")
    }
}

impl PartialEq for PasswordSource {
    /// Equality is by *source text*: two statements are equal whether or not
    /// their hashes have been derived yet.
    fn eq(&self, other: &Self) -> bool {
        self.plain == other.plain && self.prehashed == other.prehashed
    }
}

/// Hash `password` with Argon2id (19 MiB, t=2, p=1, a fresh 16-byte salt)
/// and return the PHC string. The parameters match the at-rest encryption's
/// defaults, so both features use one Argon2id configuration.
pub fn hash_password(password: &str) -> Result<String> {
    let salt_bytes = random_salt()?;
    let salt = argon2::password_hash::SaltString::encode_b64(&salt_bytes)
        .map_err(|e| PinkDbError::Execution(format!("password salt encoding failed: {e}")))?;
    let hasher = argon2::Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        argon2_params()?,
    );
    let hash =
        argon2::password_hash::PasswordHasher::hash_password(&hasher, password.as_bytes(), &salt)
            .map_err(|e| PinkDbError::Execution(format!("password hashing failed: {e}")))?;
    Ok(hash.to_string())
}

/// The Argon2id work factors: the same OWASP-recommended configuration the
/// at-rest encryption uses (see `crate::crypto`).
fn argon2_params() -> Result<argon2::Params> {
    argon2::Params::new(
        crate::crypto::ARGON2_M_COST_KIB,
        crate::crypto::ARGON2_T_COST,
        crate::crypto::ARGON2_P_COST,
        None,
    )
    .map_err(|e| PinkDbError::Execution(format!("invalid Argon2id parameters: {e}")))
}

/// 16 random bytes from the OS for one password salt.
#[cfg(not(target_arch = "wasm32"))]
fn random_salt() -> Result<[u8; SALT_LEN]> {
    let mut salt = [0u8; SALT_LEN];
    getrandom::getrandom(&mut salt).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot obtain OS randomness for a password salt: {e}"
        ))
    })?;
    Ok(salt)
}

/// wasm32 has no OS randomness in the default build (no `getrandom` backend
/// is linked), so setting a password from plaintext is refused there with a
/// clear error. Verification and `PASSWORD_HASH` need no randomness and keep
/// working.
#[cfg(target_arch = "wasm32")]
fn random_salt() -> Result<[u8; SALT_LEN]> {
    Err(PinkDbError::Execution(
        "setting a password from plaintext is not supported on wasm targets (no OS \
         randomness); use PASSWORD_HASH '<phc string>' instead"
            .to_string(),
    ))
}

/// Verify `password` against a stored PHC hash. `false` for a wrong password
/// **and** for a hash that does not parse (a corrupt catalog row must not
/// authenticate anyone).
///
/// The derivation parameters are the ones embedded in `hash` (that is the
/// `password-hash` verifier contract), which is why
/// [`validate_password_hash`] bounds them before a hash can enter the
/// catalog.
pub fn verify_password(hash: &str, password: &str) -> bool {
    let Ok(parsed) = argon2::PasswordHash::new(hash) else {
        return false;
    };
    argon2::password_hash::PasswordVerifier::verify_password(
        &argon2::Argon2::default(),
        password.as_bytes(),
        &parsed,
    )
    .is_ok()
}

// ---------- password work-factor policy ----------
//
// A `PASSWORD_HASH` row is verified with the parameters *inside* the PHC
// string, so accepting an arbitrary string would let a user choose their own
// (possibly crackable) work factor — or, in the other direction, make every
// login allocate gigabytes. The policy floor is the engine's default
// (`m=19456 KiB, t=2, p=1`, the same OWASP-recommended configuration as the
// at-rest KDF); the ceiling is the at-rest container's accepted envelope
// (`m<=77824 KiB`, `t<=8`, `p<=4`, `m*t<=311296`), so a password hash can
// never cost more than the worst encryption passphrase the format itself
// accepts. At the ceiling one verification allocates ~76 MiB and costs
// roughly 0.1 s in release on the development machine — with the server's
// four-verification in-flight cap that is ~304 MiB transient, not the
// multiple GiB an unvalidated `m=4000000` would allocate.

/// The policy floor / default `m_cost` (KiB): 19 MiB.
pub const PASSWORD_HASH_MIN_M_COST_KIB: u32 = crate::crypto::ARGON2_M_COST_KIB;
/// The policy floor / default `t_cost` (iterations).
pub const PASSWORD_HASH_MIN_T_COST: u32 = crate::crypto::ARGON2_T_COST;
/// The policy floor / default `p_cost` (lanes).
pub const PASSWORD_HASH_MIN_P_COST: u32 = crate::crypto::ARGON2_P_COST;
/// The policy ceiling for `m_cost` (KiB): 4× the default, the at-rest KDF's
/// own accepted maximum (`crypto::ARGON2_MAX_M_COST_KIB`). 76 MiB per
/// verification is the most a login may allocate.
pub const PASSWORD_HASH_MAX_M_COST_KIB: u32 = 4 * PASSWORD_HASH_MIN_M_COST_KIB;
/// The policy ceiling for `t_cost`: 4× the default (the at-rest envelope).
pub const PASSWORD_HASH_MAX_T_COST: u32 = 4 * PASSWORD_HASH_MIN_T_COST;
/// The policy ceiling for `p_cost`: 4 lanes (the at-rest envelope).
pub const PASSWORD_HASH_MAX_P_COST: u32 = 4 * PASSWORD_HASH_MIN_P_COST;
/// The policy ceiling for `m_cost * t_cost` (KiB·iterations): 8× the
/// default work, matching `crypto::ARGON2_MAX_WORK_KIB`.
pub const PASSWORD_HASH_MAX_WORK_KIB: u64 =
    8 * (PASSWORD_HASH_MIN_M_COST_KIB as u64) * (PASSWORD_HASH_MIN_T_COST as u64);

/// A fixed PHC hash used for the **dummy verification** of an unknown user
/// (M-2): the login path must pay the same Argon2id cost whether or not the
/// name exists. It is never a valid credential (its hash is 32 zero bytes),
/// and its parameters are exactly the policy floor. Keep it in sync with
/// [`PASSWORD_HASH_MIN_M_COST_KIB`] and friends; the unit tests pin it.
const DUMMY_PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$\
     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// Run the dummy verification for an unknown user, discarding the result.
fn verify_dummy(password: &str) {
    #[cfg(test)]
    DUMMY_VERIFICATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _ = verify_password(DUMMY_PASSWORD_HASH, password);
}

/// The fixed dummy hash, exposed to the crate's tests that pin the policy.
#[cfg(test)]
pub(crate) fn dummy_password_hash() -> &'static str {
    DUMMY_PASSWORD_HASH
}

/// Check a `PASSWORD_HASH '<phc>'` argument at bind time: the string must be
/// a valid Argon2id PHC hash **inside the work-factor policy**, so a typo or
/// a deliberately weak/expensive hash fails the statement instead of
/// producing a user nobody should log in as (M-8).
pub fn validate_password_hash(hash: &str) -> Result<()> {
    let parsed = argon2::PasswordHash::new(hash)
        .map_err(|e| PinkDbError::Catalog(format!("PASSWORD_HASH is not a valid PHC hash: {e}")))?;
    if parsed.algorithm.as_str() != "argon2id" {
        return Err(PinkDbError::Catalog(format!(
            "PASSWORD_HASH must be an argon2id hash, got \"{}\"",
            parsed.algorithm
        )));
    }
    // Argon2 v0x13 (PHC `v=19`) is the only version new credentials use.
    let version = parsed.version.ok_or_else(|| {
        PinkDbError::Catalog(
            "PASSWORD_HASH must name its Argon2 version (v=19), got a hash without one".to_string(),
        )
    })?;
    if version != 19 {
        return Err(PinkDbError::Catalog(format!(
            "PASSWORD_HASH must use Argon2 version 19 (v=19), got v={version}"
        )));
    }
    let params = &parsed.params;
    if params.is_empty() {
        return Err(PinkDbError::Catalog(format!(
            "PASSWORD_HASH is missing its Argon2 parameters (need m>={}, t>={}, p>={})",
            PASSWORD_HASH_MIN_M_COST_KIB, PASSWORD_HASH_MIN_T_COST, PASSWORD_HASH_MIN_P_COST
        )));
    }
    let decimal = |name: &str, what: &str| -> Result<u32> {
        params.get_decimal(name).ok_or_else(|| {
            PinkDbError::Catalog(format!(
                "PASSWORD_HASH is missing its Argon2 {what} parameter \"{name}\""
            ))
        })
    };
    let m = decimal("m", "memory")?;
    let t = decimal("t", "iteration")?;
    let p = decimal("p", "parallelism")?;
    if m < PASSWORD_HASH_MIN_M_COST_KIB
        || t < PASSWORD_HASH_MIN_T_COST
        || p < PASSWORD_HASH_MIN_P_COST
    {
        return Err(PinkDbError::Catalog(format!(
            "PASSWORD_HASH work factor is too low (m={m}, t={t}, p={p}); the policy floor is \
             m>={}, t>={}, p>={}",
            PASSWORD_HASH_MIN_M_COST_KIB, PASSWORD_HASH_MIN_T_COST, PASSWORD_HASH_MIN_P_COST
        )));
    }
    if m > PASSWORD_HASH_MAX_M_COST_KIB
        || t > PASSWORD_HASH_MAX_T_COST
        || p > PASSWORD_HASH_MAX_P_COST
        || (m as u64) * (t as u64) > PASSWORD_HASH_MAX_WORK_KIB
    {
        return Err(PinkDbError::Catalog(format!(
            "PASSWORD_HASH work factor is too high (m={m}, t={t}, p={p}); the policy ceiling is \
             m<={}, t<={}, p<={}, m*t<={}",
            PASSWORD_HASH_MAX_M_COST_KIB,
            PASSWORD_HASH_MAX_T_COST,
            PASSWORD_HASH_MAX_P_COST,
            PASSWORD_HASH_MAX_WORK_KIB
        )));
    }
    if parsed.salt.is_none() || parsed.hash.is_none() {
        return Err(PinkDbError::Catalog(
            "PASSWORD_HASH is missing its salt or hash segment".to_string(),
        ));
    }
    Ok(())
}

/// Test-only counter: how many times [`verify_dummy`] ran. The deterministic
/// M-2 test proves the unknown-user path performs work; it is compiled out of
/// normal builds (the timing test covers the observable behavior).
#[cfg(test)]
pub(crate) static DUMMY_VERIFICATIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// One user: the display name as created plus the Argon2id PHC hash. The
/// plaintext never reaches the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDef {
    /// Display name as created (catalog keys are lowercased).
    pub name: String,
    /// The PHC string (`$argon2id$v=19$…`).
    pub password_hash: String,
}

/// The store's user/role/grant catalog. One per store (users and roles are
/// store-global: a table privilege names its database).
///
/// Keys are lowercased principal names and lowercased `(database, table)`
/// pairs; [`RbacCatalog::display_of`] recovers the as-created spelling for
/// diagnostics.
#[derive(Debug, Clone, Default)]
pub(crate) struct RbacCatalog {
    /// Users by lowercased name.
    pub(crate) users: BTreeMap<String, UserDef>,
    /// Roles by lowercased name, holding the display name as created.
    pub(crate) roles: BTreeMap<String, String>,
    /// Role membership: member (a user or role, lowercased) → the roles it
    /// is a direct member of (lowercased).
    pub(crate) memberships: BTreeMap<String, BTreeSet<String>>,
    /// Table grants: `(db, table)` → principal → privileges.
    pub(crate) table_grants: BTreeMap<(String, String), BTreeMap<String, BTreeSet<Privilege>>>,
    /// Database grants: `db` → principal → privileges.
    pub(crate) database_grants: BTreeMap<String, BTreeMap<String, BTreeSet<Privilege>>>,
    /// Principals (users and roles) holding the global `ADMIN` privilege.
    pub(crate) admins: BTreeSet<String>,
}

impl RbacCatalog {
    /// Is any user defined? The server's authentication switch: `false`
    /// means every connection is trusted (the documented empty-catalog
    /// policy).
    pub(crate) fn has_users(&self) -> bool {
        !self.users.is_empty()
    }

    /// Is this a defined user or role?
    pub(crate) fn principal_exists(&self, key: &str) -> bool {
        self.users.contains_key(key) || self.roles.contains_key(key)
    }

    /// Every role the principal is a member of, directly or transitively.
    /// Cycle-safe (a cycle cannot be created through `GRANT`, but a
    /// hand-edited catalog must not loop).
    pub(crate) fn roles_of(&self, principal: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let mut queue = vec![principal.to_ascii_lowercase()];
        while let Some(member) = queue.pop() {
            let Some(roles) = self.memberships.get(&member) else {
                continue;
            };
            for role in roles {
                if out.insert(role.clone()) {
                    queue.push(role.clone());
                }
            }
        }
        out
    }

    /// Does `principal` hold `priv` on `object`? Direct grants, grants to
    /// any role the principal is a member of, and `ADMIN` (which passes
    /// every check, including one granted to a role the principal belongs
    /// to) all count.
    pub(crate) fn allowed(
        &self,
        principal: &str,
        object: &GrantObject,
        privilege: Privilege,
    ) -> bool {
        let principal = principal.to_ascii_lowercase();
        if self.is_admin_resolved(&principal) {
            return true;
        }
        let mut holders = vec![principal];
        holders.extend(self.roles_of(&holders[0]));
        match object {
            // `ADMIN` is not a privilege variant; membership is never a
            // privilege check target.
            GrantObject::Admin | GrantObject::Role { .. } => false,
            GrantObject::Table { db, table } => {
                let Some(by_principal) = self
                    .table_grants
                    .get(&(db.to_ascii_lowercase(), table.to_ascii_lowercase()))
                else {
                    return false;
                };
                holders.iter().any(|p| {
                    by_principal
                        .get(p)
                        .is_some_and(|set| set.contains(&privilege))
                })
            }
            GrantObject::Database { db } => {
                let Some(by_principal) = self.database_grants.get(&db.to_ascii_lowercase()) else {
                    return false;
                };
                holders.iter().any(|p| {
                    by_principal
                        .get(p)
                        .is_some_and(|set| set.contains(&privilege))
                })
            }
        }
    }

    /// Is `principal` a global admin?
    pub(crate) fn is_admin(&self, principal: &str) -> bool {
        self.admins.contains(&principal.to_ascii_lowercase())
    }

    /// Is `principal` a global admin directly **or through a role it is a
    /// member of**? This is the documented model: a role's `ADMIN` grant
    /// flows to every member, and the enforcement path uses this resolution
    /// (not the direct-membership [`RbacCatalog::is_admin`]).
    pub(crate) fn is_admin_resolved(&self, principal: &str) -> bool {
        let principal = principal.to_ascii_lowercase();
        self.is_admin(&principal)
            || self
                .roles_of(&principal)
                .iter()
                .any(|role| self.is_admin(role))
    }

    /// Every privilege `principal` holds on `object` through a **grant**
    /// (directly or through a role), in canonical order. The `ADMIN` bypass
    /// is deliberately not included: this reports the grant table, and an
    /// admin passes every check without holding an explicit privilege.
    pub(crate) fn effective_privileges(
        &self,
        principal: &str,
        object: &GrantObject,
    ) -> Vec<Privilege> {
        let mut holders = vec![principal.to_ascii_lowercase()];
        holders.extend(self.roles_of(&holders[0]));
        let mut out = BTreeSet::new();
        for holder in holders {
            match object {
                GrantObject::Admin | GrantObject::Role { .. } => {}
                GrantObject::Table { db, table } => {
                    if let Some(set) = self
                        .table_grants
                        .get(&(db.to_ascii_lowercase(), table.to_ascii_lowercase()))
                        .and_then(|by_principal| by_principal.get(&holder))
                    {
                        out.extend(set.iter().copied());
                    }
                }
                GrantObject::Database { db } => {
                    if let Some(set) = self
                        .database_grants
                        .get(&db.to_ascii_lowercase())
                        .and_then(|by_principal| by_principal.get(&holder))
                    {
                        out.extend(set.iter().copied());
                    }
                }
            }
        }
        out.into_iter().collect()
    }

    /// Drop every grant and membership naming `key` (a dropped user or
    /// role takes its privileges with it).
    pub(crate) fn remove_principal(&mut self, key: &str) {
        let key = key.to_ascii_lowercase();
        self.memberships.remove(&key);
        for roles in self.memberships.values_mut() {
            roles.remove(&key);
        }
        self.admins.remove(&key);
        for by_principal in self.table_grants.values_mut() {
            by_principal.remove(&key);
        }
        self.table_grants
            .retain(|_, by_principal| !by_principal.is_empty());
        for by_principal in self.database_grants.values_mut() {
            by_principal.remove(&key);
        }
        self.database_grants
            .retain(|_, by_principal| !by_principal.is_empty());
    }

    /// Grant `privs` to `principal` on `object` (idempotent). Returns
    /// `false` when every privilege was already held.
    pub(crate) fn grant(
        &mut self,
        object: &GrantObject,
        principal: &str,
        privs: &[Privilege],
    ) -> bool {
        let principal = principal.to_ascii_lowercase();
        match object {
            // Role membership is applied by [`RbacCatalog::add_membership`];
            // a privilege grant never targets a role.
            GrantObject::Role { .. } => false,
            GrantObject::Admin => self.admins.insert(principal),
            GrantObject::Table { db, table } => {
                let set = self
                    .table_grants
                    .entry((db.to_ascii_lowercase(), table.to_ascii_lowercase()))
                    .or_default()
                    .entry(principal)
                    .or_default();
                // A plain loop on purpose: every privilege must be
                // inserted even after the first success (`any`/`find` would
                // short-circuit the side effects away).
                let mut changed = false;
                for p in privs {
                    changed |= set.insert(*p);
                }
                changed
            }
            GrantObject::Database { db } => {
                let set = self
                    .database_grants
                    .entry(db.to_ascii_lowercase())
                    .or_default()
                    .entry(principal)
                    .or_default();
                // A plain loop on purpose: every privilege must be
                // inserted even after the first success (`any`/`find` would
                // short-circuit the side effects away).
                let mut changed = false;
                for p in privs {
                    changed |= set.insert(*p);
                }
                changed
            }
        }
    }

    /// Revoke `privs` from `principal` on `object` (idempotent; emptying a
    /// table's grant map removes the key). Returns `false` when nothing was
    /// held.
    pub(crate) fn revoke(
        &mut self,
        object: &GrantObject,
        principal: &str,
        privs: &[Privilege],
    ) -> bool {
        let principal = principal.to_ascii_lowercase();
        match object {
            GrantObject::Role { .. } => false,
            GrantObject::Admin => self.admins.remove(&principal),
            GrantObject::Table { db, table } => {
                let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
                let Some(by_principal) = self.table_grants.get_mut(&key) else {
                    return false;
                };
                let changed = match by_principal.get_mut(&principal) {
                    Some(set) => {
                        // Every privilege must be removed even after the
                        // first success (see `grant`).
                        let mut changed = false;
                        for p in privs {
                            changed |= set.remove(p);
                        }
                        changed
                    }
                    None => false,
                };
                if by_principal.get(&principal).is_some_and(BTreeSet::is_empty) {
                    by_principal.remove(&principal);
                }
                if self.table_grants.get(&key).is_some_and(BTreeMap::is_empty) {
                    self.table_grants.remove(&key);
                }
                changed
            }
            GrantObject::Database { db } => {
                let key = db.to_ascii_lowercase();
                let Some(by_principal) = self.database_grants.get_mut(&key) else {
                    return false;
                };
                let changed = match by_principal.get_mut(&principal) {
                    Some(set) => {
                        // Every privilege must be removed even after the
                        // first success (see `grant`).
                        let mut changed = false;
                        for p in privs {
                            changed |= set.remove(p);
                        }
                        changed
                    }
                    None => false,
                };
                if by_principal.get(&principal).is_some_and(BTreeSet::is_empty) {
                    by_principal.remove(&principal);
                }
                if self
                    .database_grants
                    .get(&key)
                    .is_some_and(BTreeMap::is_empty)
                {
                    self.database_grants.remove(&key);
                }
                changed
            }
        }
    }

    /// The as-created spelling of a principal (for diagnostics), else the
    /// key itself.
    pub(crate) fn display_of(&self, key: &str) -> String {
        let key = key.to_ascii_lowercase();
        if let Some(user) = self.users.get(&key) {
            return user.name.clone();
        }
        if let Some(role) = self.roles.get(&key) {
            return role.clone();
        }
        key
    }

    /// Add `member → role` (idempotent). Returns `false` when the membership
    /// already existed. The caller has already refused cycles.
    pub(crate) fn add_membership(&mut self, member: &str, role: &str) -> bool {
        self.memberships
            .entry(member.to_ascii_lowercase())
            .or_default()
            .insert(role.to_ascii_lowercase())
    }

    /// Remove `member → role` (idempotent; an emptied entry is dropped).
    pub(crate) fn remove_membership(&mut self, member: &str, role: &str) -> bool {
        let member = member.to_ascii_lowercase();
        let Some(roles) = self.memberships.get_mut(&member) else {
            return false;
        };
        let removed = roles.remove(&role.to_ascii_lowercase());
        if roles.is_empty() {
            self.memberships.remove(&member);
        }
        removed
    }

    /// Drop every grant on one table (a `DROP TABLE` must not leave
    /// privileges behind for a future table of the same name).
    pub(crate) fn remove_table(&mut self, db: &str, table: &str) {
        self.table_grants
            .remove(&(db.to_ascii_lowercase(), table.to_ascii_lowercase()));
    }

    /// Move one table's grants to a new name (`ALTER TABLE … RENAME TO` keeps
    /// a table's privileges with the table). A collision with an existing
    /// entry merges the two (the rename cannot create one: names are unique).
    pub(crate) fn rename_table(&mut self, db: &str, from: &str, to: &str) {
        let db = db.to_ascii_lowercase();
        let from = (db.clone(), from.to_ascii_lowercase());
        let to = (db, to.to_ascii_lowercase());
        if from == to {
            return;
        }
        let Some(grants) = self.table_grants.remove(&from) else {
            return;
        };
        let entry = self.table_grants.entry(to).or_default();
        for (principal, privs) in grants {
            entry.entry(principal).or_default().extend(privs);
        }
    }

    /// Drop every grant in one database, tables included (a `DROP DATABASE`
    /// removes the whole namespace).
    pub(crate) fn remove_database(&mut self, db: &str) {
        let db = db.to_ascii_lowercase();
        self.database_grants.remove(&db);
        self.table_grants.retain(|(gdb, _), _| *gdb != db);
    }

    /// Make the shared store's grants for one table equal to `snapshot`'s: a
    /// transaction's `DROP TABLE`/`ALTER TABLE … RENAME` ran against the
    /// private snapshot, and COMMIT merges tables — the grants must follow
    /// the same way, or a rolled-forward drop would leave privileges behind
    /// for a future table of the same name.
    pub(crate) fn sync_table_grants(&mut self, snapshot: &RbacCatalog, db: &str, table: &str) {
        let key = (db.to_ascii_lowercase(), table.to_ascii_lowercase());
        match snapshot.table_grants.get(&key) {
            Some(grants) => {
                self.table_grants.insert(key, grants.clone());
            }
            None => {
                self.table_grants.remove(&key);
            }
        }
    }
}

/// Apply one bound `GRANT`/`REVOKE` to the store (the executor's arm).
///
/// Every name is re-validated here: bind and execution are separate lock
/// acquisitions on the multi-writer paths, so a concurrent commit could have
/// dropped the table or the principal in between (and a store refreshed from
/// the WAL after a conflict rebinds, but a `REVOKE` of a vanished object must
/// still fail cleanly rather than silently succeed). Idempotent: granting a
/// privilege twice, or revoking one that is not held, is a no-op.
pub(crate) fn apply_grant(store: &mut PinkDb, spec: &GrantSpec, revoke: bool) -> Result<()> {
    // Validation first: the apply step needs the catalog mutably.
    let rbac = store.rbac();
    if !rbac.principal_exists(&spec.principal) {
        return Err(PinkDbError::Catalog(format!(
            "user or role \"{}\" not found",
            rbac.display_of(&spec.principal)
        )));
    }
    match &spec.object {
        GrantObject::Admin => {}
        GrantObject::Role { role } => {
            if !rbac.roles.contains_key(role) {
                return Err(PinkDbError::Catalog(format!(
                    "role \"{}\" not found",
                    rbac.display_of(role)
                )));
            }
            if !revoke {
                if *role == spec.principal {
                    return Err(PinkDbError::Catalog(format!(
                        "a principal cannot be a member of itself (\"{}\")",
                        rbac.display_of(role)
                    )));
                }
                if rbac.roles_of(role).contains(&spec.principal) {
                    return Err(PinkDbError::Catalog(format!(
                        "granting role \"{}\" to \"{}\" would create a membership cycle",
                        rbac.display_of(role),
                        rbac.display_of(&spec.principal)
                    )));
                }
            }
        }
        GrantObject::Table { db, table } => {
            if store.table_in(db, table).is_err() {
                return Err(PinkDbError::Catalog(format!(
                    "table \"{table}\" not found in database \"{db}\""
                )));
            }
        }
        GrantObject::Database { db } => {
            if !store.database_exists(db) {
                return Err(PinkDbError::Catalog(format!("database \"{db}\" not found")));
            }
        }
    }
    let rbac = store.rbac_mut();
    match (&spec.object, revoke) {
        (GrantObject::Admin, false) => {
            rbac.admins.insert(spec.principal.clone());
        }
        (GrantObject::Admin, true) => {
            rbac.admins.remove(&spec.principal);
        }
        (GrantObject::Role { role }, false) => {
            rbac.add_membership(&spec.principal, role);
        }
        (GrantObject::Role { role }, true) => {
            rbac.remove_membership(&spec.principal, role);
        }
        (object, false) => {
            rbac.grant(object, &spec.principal, &spec.privs);
        }
        (object, true) => {
            rbac.revoke(object, &spec.principal, &spec.privs);
        }
    }
    Ok(())
}

/// The store's RBAC catalog (users, roles, grants).
impl PinkDb {
    /// The RBAC catalog (immutable).
    pub(crate) fn rbac(&self) -> &RbacCatalog {
        &self.rbac
    }

    /// The RBAC catalog (mutable; the executor's grant/revoke/DDL path).
    pub(crate) fn rbac_mut(&mut self) -> &mut RbacCatalog {
        &mut self.rbac
    }

    /// Is any user defined? A store with no users is the documented
    /// empty-catalog state: the network server lets every connection in
    /// **trusted** (embedded semantics) until the first user exists.
    pub fn has_users(&self) -> bool {
        self.rbac.has_users()
    }

    /// A user's stored Argon2id PHC password hash (`None` for a role or an
    /// unknown name). The hash is not secret, but it is the only form of a
    /// password this store ever holds.
    pub fn user_password_hash(&self, user: &str) -> Option<&str> {
        self.rbac
            .users
            .get(&user.to_ascii_lowercase())
            .map(|u| u.password_hash.as_str())
    }

    /// Is this principal (a user or a role) a global admin? (Roles pass their
    /// `ADMIN` down to every member, like any other grant.)
    pub fn principal_is_admin(&self, principal: &str) -> bool {
        self.rbac.is_admin_resolved(principal)
    }

    /// The privileges `principal` effectively holds on one table: its direct
    /// grants plus every grant held by any role it is a member of
    /// (transitively), in canonical order.
    pub fn effective_table_privileges(
        &self,
        principal: &str,
        db: &str,
        table: &str,
    ) -> Vec<Privilege> {
        self.rbac.effective_privileges(
            principal,
            &GrantObject::Table {
                db: db.to_ascii_lowercase(),
                table: table.to_ascii_lowercase(),
            },
        )
    }

    /// The privileges `principal` effectively holds on one database (see
    /// [`PinkDb::effective_table_privileges`]).
    pub fn effective_database_privileges(&self, principal: &str, db: &str) -> Vec<Privilege> {
        self.rbac.effective_privileges(
            principal,
            &GrantObject::Database {
                db: db.to_ascii_lowercase(),
            },
        )
    }

    /// The user record a login needs, copied out so the caller can release
    /// the store lock **before** the expensive Argon2id verification (H-6):
    /// `Some((display name, PHC hash))`, or `None` for an unknown user.
    pub(crate) fn login_record(&self, user: &str) -> Option<(String, String)> {
        self.rbac
            .users
            .get(&user.to_ascii_lowercase())
            .map(|def| (def.name.clone(), def.password_hash.clone()))
    }

    /// Verify a password against a **copied** user record (see
    /// [`PinkDb::login_record`]). This is the pure-computation half of a
    /// login: it touches no store state, so callers must not hold the store
    /// lock while it runs. An unknown user still pays one dummy Argon2id
    /// verification (M-2), so the two failure paths cost the same.
    pub(crate) fn verify_login_record(
        record: Option<(String, String)>,
        password: &str,
    ) -> Result<String> {
        match record {
            Some((name, hash)) => {
                if verify_password(&hash, password) {
                    Ok(name)
                } else {
                    Err(auth_failed())
                }
            }
            None => {
                verify_dummy(password);
                Err(auth_failed())
            }
        }
    }

    /// Verify a login against the store. Returns the user's display name on
    /// success; `authentication failed` (with the same text for an unknown
    /// user and a wrong password, so a client cannot probe which names
    /// exist) otherwise.
    ///
    /// An unknown user performs the same dummy Argon2id verification as a
    /// wrong password (M-2). Callers that hold the store read lock should
    /// prefer copying [`PinkDb::login_record`] out and calling
    /// [`PinkDb::verify_login_record`] after releasing it (H-6); this method
    /// is the convenience API for embedders that already have exclusive
    /// access.
    pub fn authenticate(&self, user: &str, password: &str) -> Result<String> {
        let record = self.login_record(user);
        // Re-borrow the record's hash is unnecessary: the verification only
        // reads the copied PHC string. `self` stays borrowed because of the
        // method signature, but the expensive derivation is identical.
        Self::verify_login_record(record, password)
    }
}

pub(crate) fn auth_failed() -> PinkDbError {
    PinkDbError::Execution("authentication failed: unknown user or wrong password".to_string())
}

/// The error a privilege check reports.
fn denied(principal: &str, what: &str, hint: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "permission denied: user \"{principal}\" needs {what} ({hint})"
    ))
}

/// The database a (possibly unqualified) plan target resolves to.
fn target_db(qdb: &Option<String>, default_db: &str) -> String {
    qdb.clone()
        .unwrap_or_else(|| default_db.to_string())
        .to_ascii_lowercase()
}

/// The statement kinds `check_statement` applies the read pass to: every
/// query and DML shape (and the DDL that reads its input — `CREATE TABLE AS`,
/// `CREATE MATERIALIZED VIEW`), plus anything a future version adds
/// (fail-closed: an unknown plan gets read-checked, never skipped).
///
/// The kinds that return `false` here are exactly the ones handled by
/// statement kind above: the `ADMIN`-only set, the schema DDL set (checked
/// for `CREATE` on their database), and those that name no object.
fn read_checked(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        // ADMIN-only statements (see `check_statement`).
        LogicalPlan::CreateUser { .. }
            | LogicalPlan::AlterUser { .. }
            | LogicalPlan::DropUser { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::CreateDatabase { .. }
            | LogicalPlan::DropDatabase { .. }
            | LogicalPlan::Checkpoint
            | LogicalPlan::LoadExtension { .. }
            | LogicalPlan::Call { .. }
            // Schema DDL (checked for `CREATE` on the target database).
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreateProcedure { .. }
            | LogicalPlan::DropProcedure { .. }
            | LogicalPlan::CreateMaterializedView { .. }
            | LogicalPlan::RefreshMaterializedView { .. }
            | LogicalPlan::RefreshMaterializedViews { .. }
            | LogicalPlan::DropMaterializedView { .. }
            // Metadata and session control: no object access.
            | LogicalPlan::ShowTables { .. }
            | LogicalPlan::ShowDatabases
            | LogicalPlan::ShowMaterializedViews { .. }
            | LogicalPlan::Describe { .. }
            | LogicalPlan::Use { .. }
            | LogicalPlan::Begin
            | LogicalPlan::Commit
            | LogicalPlan::Rollback
    )
}

/// Check one bound statement against `principal`'s privileges.
///
/// Called by [`crate::Connection`] for every statement of an authenticated
/// session, after binding and before execution. Returns a
/// `permission denied: …` error naming the missing privilege and the
/// statement that needs it. An `ADMIN` principal (directly, or through a
/// role it is a member of) passes every check. An authenticated principal is
/// **always** checked: the "empty catalog ⇒ trusted" policy belongs to the
/// trusted-session path ([`crate::Connection`] with no principal), never to
/// a session that authenticated earlier, so emptying the catalog simply
/// removes every grant and denies.
pub(crate) fn check_statement(
    store: &PinkDb,
    principal: &str,
    plan: &LogicalPlan,
    default_db: &str,
) -> Result<()> {
    let rbac = store.rbac();
    if rbac.is_admin_resolved(principal) {
        return Ok(());
    }
    // M-4: a table-function scan (`read_parquet`, `read_arrow`, the
    // replacement scan `FROM 'file.parquet'`) reads external files with a
    // client-controlled path. It has no catalog object to grant on, so it is
    // ADMIN-only in an authenticated session. The walk covers the whole
    // statement — CTEs, subqueries, `UNION` arms, materialized-view
    // definitions and `EXPLAIN` — not just the top node. A plain view's
    // defining query is unbound until the view is read, and the expanded
    // plan is checked then.

    if crate::plan::contains_table_function(plan) {
        let admin_only = |what: &str| -> Result<()> {
            Err(denied(
                principal,
                "the ADMIN privilege",
                &format!(
                    "GRANT ADMIN TO {} (this statement is: {what})",
                    rbac.display_of(principal)
                ),
            ))
        };
        return admin_only("a file/table-function scan");
    }
    // 1. Statement-kind privileges.
    let create_on_db = |db: String| -> Result<()> {
        if rbac.allowed(
            principal,
            &GrantObject::Database { db: db.clone() },
            Privilege::Create,
        ) {
            return Ok(());
        }
        Err(denied(
            principal,
            &format!("the CREATE privilege on database \"{db}\""),
            &format!(
                "GRANT CREATE ON DATABASE {db} TO {}",
                rbac.display_of(principal)
            ),
        ))
    };
    let admin_only = |what: &str| -> Result<()> {
        Err(denied(
            principal,
            "the ADMIN privilege",
            &format!(
                "GRANT ADMIN TO {} (this statement is: {what})",
                rbac.display_of(principal)
            ),
        ))
    };
    match plan {
        LogicalPlan::CreateUser { .. }
        | LogicalPlan::AlterUser { .. }
        | LogicalPlan::DropUser { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. } => return admin_only("user/role management"),
        LogicalPlan::CreateDatabase { .. } | LogicalPlan::DropDatabase { .. } => {
            return admin_only("database management");
        }
        LogicalPlan::Checkpoint => return admin_only("CHECKPOINT"),
        LogicalPlan::LoadExtension { .. } => return admin_only("LOAD EXTENSION"),
        // M-3: writing a file on the server host is never implied by SELECT.
        // A dedicated filesystem-write privilege would need a new grant
        // keyword and `.pink`/WAL persistence surface, so this pass reuses
        // ADMIN. The server's own `--allow-copy-to/--copy-root` opt-in is a
        // second, independent gate.
        LogicalPlan::CopyTo { .. } => return admin_only("COPY TO"),
        // A procedure body binds only when it runs (its statements may read
        // or write anything the definer can reach), so running one requires
        // ADMIN. Defining one only needs CREATE on the database.
        LogicalPlan::Call { .. } => return admin_only("CALL"),
        LogicalPlan::CreateTable { db, .. }
        | LogicalPlan::AlterTable { db, .. }
        | LogicalPlan::DropTable { db, .. }
        | LogicalPlan::CreateView { db, .. }
        | LogicalPlan::DropView { db, .. }
        | LogicalPlan::CreateIndex { db, .. }
        | LogicalPlan::DropIndex { db, .. }
        | LogicalPlan::CreateProcedure { db, .. }
        | LogicalPlan::DropProcedure { db, .. }
        // Wave 77: a macro is database metadata like a procedure: defining or
        // dropping one needs CREATE on the database. Invoking one needs no
        // macro privilege — its expansion runs under the invoker's ordinary
        // table privileges (see `check_statement`'s read pass).
        | LogicalPlan::CreateMacro { db, .. }
        | LogicalPlan::DropMacro { db, .. }
        | LogicalPlan::CreateTableAs { db, .. }
        | LogicalPlan::CreateMaterializedView { db, .. }
        | LogicalPlan::RefreshMaterializedView { db, .. }
        | LogicalPlan::DropMaterializedView { db, .. } => {
            create_on_db(target_db(db, default_db))?;
        }
        LogicalPlan::RefreshMaterializedViews { db } => {
            create_on_db(target_db(db, default_db))?;
        }
        _ => {}
    }
    // 2. Table-level privileges (writes).
    match plan {
        LogicalPlan::Insert { db, table, .. } | LogicalPlan::InsertSelect { db, table, .. } => {
            check_table(store, principal, db, table, default_db, Privilege::Insert)?;
        }
        LogicalPlan::Update { db, table, .. } => {
            check_table(store, principal, db, table, default_db, Privilege::Update)?;
        }
        LogicalPlan::Delete { db, table, .. } => {
            check_table(store, principal, db, table, default_db, Privilege::Delete)?;
        }
        _ => {}
    }
    // 3. Read pass: every table the statement reads needs SELECT. Includes
    //    the target of UPDATE/DELETE (both read what they write) and the
    //    source of INSERT..SELECT / CREATE TABLE AS.
    if read_checked(plan) {
        let mut reads = std::collections::HashSet::new();
        crate::plan::collect_tables_read(plan, default_db, &mut reads);
        let mut reads: Vec<_> = reads.into_iter().collect();
        reads.sort();
        for (db, table) in reads {
            check_table(
                store,
                principal,
                &Some(db),
                &table,
                default_db,
                Privilege::Select,
            )?;
        }
    }
    Ok(())
}

/// Require one table privilege, with the statement-shaped hint.
fn check_table(
    store: &PinkDb,
    principal: &str,
    qdb: &Option<String>,
    table: &str,
    default_db: &str,
    privilege: Privilege,
) -> Result<()> {
    let db = target_db(qdb, default_db);
    let object = GrantObject::Table {
        db: db.clone(),
        table: table.to_ascii_lowercase(),
    };
    if store.rbac().allowed(principal, &object, privilege) {
        return Ok(());
    }
    Err(denied(
        principal,
        &format!(
            "the {} privilege on table \"{db}.{table}\"",
            privilege.keyword()
        ),
        &format!(
            "GRANT {} ON TABLE {db}.{table} TO {}",
            privilege.keyword(),
            store.rbac().display_of(principal)
        ),
    ))
}

/// The statement text a committed statement is logged with. `CREATE/ALTER
/// USER` is rewritten to its `PASSWORD_HASH '<phc>'` form, so the WAL (and
/// everything replayed from it) never carries the plaintext. Index DDL is
/// rewritten to a canonical, fully-qualified, identifier-quoted statement:
/// `.pink` has no index section and replay re-executes the text, so it must
/// re-bind exactly regardless of how the session spelled it. Every other
/// statement logs its own source text unchanged.
pub(crate) fn wal_statement_text(plan: &LogicalPlan, sql: &str) -> String {
    match plan {
        LogicalPlan::CreateUser {
            name,
            password_hash,
            if_not_exists,
        } => format!(
            "CREATE USER {}{} PASSWORD_HASH '{}'",
            if *if_not_exists { "IF NOT EXISTS " } else { "" },
            quote_ident(name),
            password_hash
        ),
        LogicalPlan::AlterUser {
            name,
            password_hash,
        } => format!(
            "ALTER USER {} PASSWORD_HASH '{}'",
            quote_ident(name),
            password_hash
        ),
        LogicalPlan::CreateIndex {
            db,
            name,
            table,
            column_name,
            if_not_exists,
            ..
        } => format!(
            "CREATE INDEX {}{} ON {}({})",
            if *if_not_exists { "IF NOT EXISTS " } else { "" },
            quote_ident(name),
            qualified_ident(db, table),
            quote_ident(column_name)
        ),
        LogicalPlan::DropIndex {
            db,
            name,
            if_exists,
            ..
        } => format!(
            "DROP INDEX {}{}",
            if *if_exists { "IF EXISTS " } else { "" },
            qualified_ident(db, name)
        ),
        _ => sql.to_string(),
    }
}

fn qualified_ident(db: &Option<String>, name: &str) -> String {
    match db {
        Some(db) => format!("{}.{}", quote_ident(db), quote_ident(name)),
        None => quote_ident(name),
    }
}

/// Quote an identifier for a rewritten WAL statement when the as-written
/// name would not re-parse as a bare identifier (it holds spaces, a keyword,
/// punctuation, …). Doubling the quotes is the grammar's escape.
pub(crate) fn quote_ident(name: &str) -> String {
    let bare = !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()));
    if bare && crate::sql::keyword::Keyword::lookup(name).is_none() {
        return name.to_string();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privilege_words_parse_case_insensitively() {
        assert_eq!(Privilege::parse("select"), Some(Privilege::Select));
        assert_eq!(Privilege::parse("INSERT"), Some(Privilege::Insert));
        assert_eq!(Privilege::parse("Update"), Some(Privilege::Update));
        assert_eq!(Privilege::parse("DELETE"), Some(Privilege::Delete));
        assert_eq!(Privilege::parse("create"), Some(Privilege::Create));
        assert_eq!(Privilege::parse("all"), None);
        assert_eq!(Privilege::parse("truncate"), None);
        // The tag round-trip is what the `.pink` section uses.
        for p in Privilege::TABLE.iter().chain(Privilege::DATABASE.iter()) {
            assert_eq!(Privilege::from_tag(p.tag()).unwrap(), *p);
        }
        assert!(Privilege::from_tag(9).is_err());
        // Scope and labels.
        assert_eq!(Privilege::Select.scope(), "TABLE");
        assert_eq!(Privilege::Create.scope(), "DATABASE");
        assert_eq!(
            Privilege::list(&Privilege::TABLE),
            "SELECT, INSERT, UPDATE, DELETE"
        );
        assert_eq!(
            GrantObject::Database { db: "main".into() }.label(),
            "DATABASE main"
        );
        assert_eq!(GrantObject::Role { role: "r".into() }.label(), "ROLE r");
    }

    #[test]
    fn grant_privs_expand_per_scope() {
        assert_eq!(GrantPrivs::all().expand_table(), Privilege::TABLE.to_vec());
        assert_eq!(
            GrantPrivs::all().expand_database(),
            Privilege::DATABASE.to_vec()
        );
        let list = GrantPrivs::list(vec![Privilege::Insert, Privilege::Select]);
        assert_eq!(
            list.expand_table(),
            vec![Privilege::Insert, Privilege::Select]
        );
        assert_eq!(list.expand_database().len(), 2);
    }

    #[test]
    fn password_sources_hash_once_and_redact_their_debug() {
        let source = PasswordSource::plaintext("hunter2".to_string());
        let first = source.hashed().unwrap().to_string();
        assert!(
            first.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"),
            "{first}"
        );
        // The memo returns the very same hash (one derivation per statement).
        assert_eq!(source.hashed().unwrap(), first);
        assert!(verify_password(&first, "hunter2"));
        assert!(!verify_password(&first, "Hunter2"));
        // Debug never prints the plaintext.
        assert_eq!(format!("{source:?}"), "PasswordSource(<redacted>)");
        assert!(!format!("{source:?}").contains("hunter2"));
        // A pre-hashed source is returned as-is (no derivation).
        let pre = PasswordSource::prehashed("$argon2id$whatever".to_string());
        assert_eq!(pre.hashed().unwrap(), "$argon2id$whatever");
        assert_eq!(pre.prehashed_hash(), Some("$argon2id$whatever"));
        // A hash that does not parse, or has no policy parameters, never
        // authenticates anyone and is refused at bind time (M-8).
        assert!(!verify_password("$argon2id$whatever", "whatever"));
        assert!(validate_password_hash("$argon2id$whatever").is_err());
        assert!(validate_password_hash("nonsense").is_err());
        // The engine's own default hash is inside the policy.
        assert!(validate_password_hash(&first).is_ok());
        // The M-2 dummy hash is a valid, floor-parameter PHC hash.
        assert!(validate_password_hash(dummy_password_hash()).is_ok());
    }

    #[test]
    fn unknown_user_login_performs_the_dummy_verification() {
        use std::sync::atomic::Ordering;
        let db = PinkDb::new();
        let before = DUMMY_VERIFICATIONS.load(Ordering::Relaxed);
        let err = db.authenticate("nobody", "x").unwrap_err().to_string();
        assert!(err.starts_with("authentication failed"), "{err}");
        assert_eq!(
            DUMMY_VERIFICATIONS.load(Ordering::Relaxed),
            before + 1,
            "an unknown user must still run the dummy Argon2id verification"
        );
        // A known user with a wrong password does not take the dummy path.
        let mut db = PinkDb::new();
        db.execute("CREATE USER alice PASSWORD 'right';").unwrap();
        let before = DUMMY_VERIFICATIONS.load(Ordering::Relaxed);
        assert!(db.authenticate("alice", "wrong").is_err());
        assert_eq!(DUMMY_VERIFICATIONS.load(Ordering::Relaxed), before);
    }

    #[test]
    fn password_hash_policy_floor_and_ceiling() {
        let phc = |m: u32, t: u32, p: u32| {
            format!(
                "$argon2id$v=19$m={m},t={t},p={p}$AAAAAAAAAAAAAAAAAAAAAA$\
                 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
        };
        // The floor and the ceiling are accepted.
        assert!(validate_password_hash(&phc(19456, 2, 1)).is_ok());
        assert!(validate_password_hash(&phc(77824, 4, 1)).is_ok());
        assert!(validate_password_hash(&phc(19456, 8, 4)).is_ok());
        // Below the floor.
        for (m, t, p) in [(19455, 2, 1), (19456, 1, 1), (19456, 2, 0), (8, 1, 1)] {
            let e = validate_password_hash(&phc(m, t, p))
                .expect_err(&format!("m={m},t={t},p={p} must be refused"))
                .to_string();
            assert!(e.contains("too low"), "{e}");
            assert!(e.contains("m>=19456"), "{e}");
        }
        // Above the ceiling (individual dimensions and the work product).
        for (m, t, p) in [
            (77825, 2, 1),
            (19456, 9, 1),
            (19456, 2, 5),
            (77824, 8, 1), // m*t = 622592 > 311296
            (4000000, 2, 1),
        ] {
            let e = validate_password_hash(&phc(m, t, p))
                .expect_err(&format!("m={m},t={t},p={p} must be refused"))
                .to_string();
            assert!(e.contains("too high"), "{e}");
        }
        // Version, missing parameters and malformed strings.
        let v16 = "$argon2id$v=16$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$\
                   AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert!(validate_password_hash(v16).is_err());
        assert!(
            validate_password_hash(
                "$argon2id$v=19$m=19456$AAAAAAAAAAAAAAAAAAAAAA$\
                 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
            .is_err()
        );
        assert!(validate_password_hash("$argon2id$v=19$m=19456,t=2,p=1$AAAA$AAAA").is_err());
    }

    #[test]
    fn wal_statement_text_rewrites_only_password_statements() {
        use crate::plan::LogicalPlan;
        // Every other statement logs its own text, byte for byte.
        let plan = LogicalPlan::CreateRole {
            name: "r".to_string(),
            if_not_exists: false,
        };
        assert_eq!(
            wal_statement_text(&plan, "CREATE ROLE r;"),
            "CREATE ROLE r;"
        );
        let plan = LogicalPlan::CreateUser {
            name: "alice".to_string(),
            password_hash: "$argon2id$hash".to_string(),
            if_not_exists: true,
        };
        assert_eq!(
            wal_statement_text(&plan, "CREATE USER IF NOT EXISTS alice PASSWORD 'secret';"),
            "CREATE USER IF NOT EXISTS alice PASSWORD_HASH '$argon2id$hash'"
        );
        assert!(!wal_statement_text(&plan, "x").contains("secret"));
        let plan = LogicalPlan::AlterUser {
            name: "select".to_string(),
            password_hash: "$argon2id$hash".to_string(),
        };
        assert_eq!(
            wal_statement_text(&plan, "ALTER USER select PASSWORD 's';"),
            "ALTER USER \"select\" PASSWORD_HASH '$argon2id$hash'"
        );
    }

    #[test]
    fn wal_statement_text_canonicalizes_index_ddl() {
        use crate::plan::LogicalPlan;
        // Index DDL is re-parsed on WAL replay, so the logged text is fully
        // qualified and every identifier is quoted exactly: keywords, spaces,
        // unicode and an embedded quote included.
        let create = LogicalPlan::CreateIndex {
            db: Some("d ä".to_string()),
            name: "select".to_string(),
            table: "t x".to_string(),
            column: 0,
            column_name: "a\"b".to_string(),
            if_not_exists: true,
        };
        assert_eq!(
            wal_statement_text(&create, "CREATE INDEX IF NOT EXISTS select ON t(a);"),
            "CREATE INDEX IF NOT EXISTS \"select\" ON \"d ä\".\"t x\"(\"a\"\"b\")"
        );
        let drop = LogicalPlan::DropIndex {
            db: Some("d ä".to_string()),
            name: "ix".to_string(),
            table: Some("t x".to_string()),
            if_exists: true,
        };
        assert_eq!(
            wal_statement_text(&drop, "DROP INDEX ix;"),
            "DROP INDEX IF EXISTS \"d ä\".ix"
        );
    }

    #[test]
    fn an_empty_catalog_still_checks_an_authenticated_principal() {
        // M-1: the "no RBAC data ⇒ skip" shortcut applies only to a trusted
        // session (principal == None), never to one that authenticated
        // earlier. With an empty catalog there are no grants, so a
        // privileged statement is denied instead of silently promoted.
        let db = PinkDb::new();
        assert!(!db.has_users());
        let e = check_statement(&db, "alice", &LogicalPlan::Checkpoint, "main")
            .expect_err("an authenticated principal keeps being checked");
        assert!(e.to_string().contains("ADMIN privilege"), "{e}");
    }

    #[test]
    fn role_granted_admin_is_resolved_by_the_enforcement_path() {
        let mut db = PinkDb::new();
        db.execute("CREATE USER alice PASSWORD 'alice-secret';")
            .unwrap();
        db.execute("CREATE ROLE ops;").unwrap();
        db.execute("GRANT ADMIN TO ops;").unwrap();
        db.execute("GRANT ops TO alice;").unwrap();
        assert!(db.principal_is_admin("alice"));
        // The same plan that is admin-only is accepted only because of the
        // role-held ADMIN: `check_statement` uses the resolved check.
        assert!(check_statement(&db, "alice", &LogicalPlan::Checkpoint, "main").is_ok());
        db.execute("REVOKE ADMIN FROM ops;").unwrap();
        assert!(!db.principal_is_admin("alice"));
        assert!(check_statement(&db, "alice", &LogicalPlan::Checkpoint, "main").is_err());
    }
}
