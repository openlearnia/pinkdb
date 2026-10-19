//! pinkdb's core **math** extension (wave 54): the DuckDB math surface that
//! the engine does not already provide — the exponential/logarithm family,
//! the trigonometric family, `degrees`/`radians`/`pi` and the volatile
//! `random()` — authored with the Rust extension SDK.
//!
//! One [`pinkdb_extension!`] declaration defines every function; the same
//! declaration publishes the linked registration the CLI loads at startup
//! (`pinkdb_math::ensure_registered()` + `load_linked_extension("math")`).
//! This crate is **linked-only**: it is an rlib the CLI links in, so it
//! deliberately emits no native ABI exports (a native cdylib would call
//! `pinkdb_abi_exports!()`; two linked extension rlibs carrying those
//! `#[no_mangle]` globals are a `duplicate symbol: pinkdb_extension_init`
//! link error on Linux).
//!
//! # What this extension provides, and what the engine already owns
//!
//! Extension functions cannot shadow a built-in, so the names the engine
//! already provides stay built-ins and are **not** re-declared here:
//! `sqrt`, `pow`/`power`, `floor`, `ceil`/`ceiling`, `sign`, `greatest` and
//! `least` are core scalar functions with their own DuckDB-pinned tests (and
//! `duckdiff` probes). They keep working unchanged — this crate's SQL tests
//! pin them alongside the extension's functions so the whole math surface is
//! covered in one place.
//!
//! | function | signature | notes |
//! |---|---|---|
//! | `exp` | `(DOUBLE) → DOUBLE` | `e^x` |
//! | `ln` | `(DOUBLE) → DOUBLE` | natural log |
//! | `log` | `(DOUBLE[, DOUBLE]) → DOUBLE` | **1-arg: log₁₀; 2-arg: `log(b, x)` = log base `b` of `x`**, i.e. `log10(x)/log10(b)` — DuckDB's overloads |
//! | `log2` / `log10` | `(DOUBLE) → DOUBLE` | base-2 / base-10 log |
//! | `sin`, `cos`, `tan`, `cot` | `(DOUBLE) → DOUBLE` | `cot` = `1/tan` |
//! | `asin`, `acos`, `atan` | `(DOUBLE) → DOUBLE` | `\|x\| > 1` → NaN |
//! | `atan2` | `(DOUBLE, DOUBLE) → DOUBLE` | `atan2(y, x)` (y first, like DuckDB) |
//! | `degrees` / `radians` | `(DOUBLE) → DOUBLE` | π-based conversions |
//! | `pi` | `() → DOUBLE` | π |
//! | `random` | `() → DOUBLE` | uniform `[0, 1)`; **volatile** |
//!
//! # Semantics, pinned to DuckDB `v2.0.0-alpha46057`
//!
//! Every deterministic function is the corresponding IEEE-754 `f64`
//! operation, so NaN/±infinity propagate exactly like DuckDB (which uses the
//! same libm operations):
//!
//! * `exp(0)` = 1, `exp(1)` = 2.718281828459045, `exp(1000)` = inf;
//!   `exp(-inf)` = 0.
//! * `ln(0)` = -inf (probed), `ln(-1)` = NaN, `ln(inf)` = inf.
//! * `log(100)` = 2 (log₁₀), `log(2, 8)` = 3, `log(8, 2)` =
//!   0.33333333333333337 (base = the *first* argument — probed against the
//!   parameter names `[b, x]`); the two-argument form composes as
//!   `log10(x)/log10(b)`, DuckDB's own formula (the natural-log ratio differs
//!   by an ulp for some inputs, e.g. `log(10, 0.5)`); `log(0)` = -inf,
//!   `log(-1)` = NaN, `log(1, 2)` = inf, `log(0, 2)` = -0.0,
//!   `log(2, 0)` = -inf.
//! * `sin(0)` = 0, `cos(0)` = 1, `tan(0)` = 0, `cot(0)` = inf,
//!   `asin(2)`/`acos(2)` = NaN, `atan2(0, 0)` = 0, `atan2(1, 0)` = π/2,
//!   `atan2(0, -1)` = π.
//! * `degrees(pi())` = 180, `radians(180)` = π.
//! * Every function is NULL-propagating (`ln(NULL)` is NULL) and a NULL in
//!   *any* position of `log`/`atan2` yields NULL — `log(2, NULL)` is NULL,
//!   never the one-argument form.
//! * `random()` returns a DOUBLE in `[0, 1)`. It is declared **volatile**:
//!   the engine refuses it in any statement it WAL-logs (`INSERT`,
//!   `UPDATE`, `DELETE`, CTAS, ...) while a read-only `SELECT random()`
//!   works — replaying a logged statement could otherwise produce a
//!   different value.
//!
//! # Divergences from DuckDB (documented)
//!
//! * **`sign` returns INTEGER here** (DuckDB's is TINYINT — the engine has no
//!   TINYINT); the value is identical. `sign` is a built-in, listed here for
//!   completeness because DuckDB's math surface groups it.
//! * **A VARCHAR argument is refused, never parsed**: `exp('1')` errors in
//!   pinkdb ("must be DOUBLE, got VARCHAR (use CAST)") while DuckDB casts the
//!   string. Numeric arguments (INTEGER, BIGINT, DECIMAL) *do* widen into a
//!   DOUBLE parameter automatically — the engine inserts the cast DuckDB
//!   applies implicitly, so `exp(1)`, `log(100)`, `sin(0)` and integer or
//!   DECIMAL columns all work.
//! * **`sqrt`/`pow`/`floor`/`ceil`/`sign`/`greatest`/`least` are engine
//!   built-ins**, not extension functions (an extension cannot shadow one):
//!   their behavior is pinned by the engine's tests and the `numeric-fns` /
//!   `greatest-least` differential probes, and this crate's SQL tests cover
//!   them end to end (`sqrt(4)`, `pow(2, 10)`, integer/DOUBLE columns,
//!   NaN/`-inf` edge cases).
//! * **`random()` cannot be WAL-logged** (by design); DuckDB has no WAL and
//!   allows it anywhere.
//! * No `gamma`/`lgamma`/`factorial`/`even`/`odd`/`gcd`/`lcm` in v1 — the
//!   brief's surface plus `log2`/`log10`/`cot`/`degrees`/`radians`/`pi`.
//!
//! # Activation
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! pinkdb_math::ensure_registered()?;              // host bootstrap, once
//! let mut db = pinkdb::PinkDb::new();
//! db.load_linked_extension("math")?;              // or: LOAD EXTENSION 'math'
//! # Ok(())
//! # }
//! ```
//!
//! The CLI's default `math` feature does both at startup for every store the
//! session creates or opens, so `exp`/`log`/`random` work without a statement
//! (`LOAD EXTENSION 'math'` is then the documented duplicate-load error);
//! `--no-default-features` leaves it out. A durable store replays math calls
//! through the registry (each touching record is prefixed with
//! `LOAD EXTENSION 'math'`), so the extension must be registered before a
//! database is opened — exactly like every linked extension.
//!
//! The declaration uses the SDK's **`optional(1)`** arity for `log`
//! (1 or 2 arguments). That is a ranged arity, which only the linked loading
//! model registers — and the linked model is how this crate is loaded
//! (there is no cdylib build; the native ABI v1 has no ranged-arity
//! descriptor). For the same reason the crate is linked-only and emits no
//! native ABI exports: it is an rlib in the host binary, not a shared
//! library.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb_ext_sdk::{Optional, pinkdb_extension};

/// The registry name: `LOAD EXTENSION 'math'`.
pub const EXTENSION_NAME: &str = "math";

// ---------- the volatile function's random source ----------

/// The next SplitMix64 output. One global stream, advanced with a
/// compare-and-exchange so concurrent callers each get a distinct value.
///
/// The state cell is *initialized* with the seed (not sentinel-zero): a
/// compare-and-exchange against a cell nobody ever writes would spin
/// forever, and `splitmix64(0)` is a perfectly good first step.
fn next_u64() -> u64 {
    fn state_cell() -> &'static AtomicU64 {
        static CELL: OnceLock<AtomicU64> = OnceLock::new();
        CELL.get_or_init(|| AtomicU64::new(seed_from_entropy()))
    }

    let state = state_cell();
    let mut current = state.load(Ordering::Relaxed);
    loop {
        let next = splitmix64(current);
        match state.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

/// A nonzero seed from the platform's `RandomState` (OS entropy on native
/// targets; a fixed fallback where the target has none — wasm builds are
/// linked, and a `random()` stream with a platform seed is a documented
/// limit, not a correctness issue: no statement may depend on the value).
fn seed_from_entropy() -> u64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    static SALT: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let salt = SALT.fetch_add(1, Ordering::Relaxed);
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(salt);
    match hasher.finish() {
        0 => 0x2545_F491_4F6C_DD1D,
        seed => seed,
    }
}

/// The SplitMix64 mixer (Steele et al.); a full-period permutation of `u64`.
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ---------- the declaration ----------
//
// The Rust fn names are the SQL names (the SDK registers `stringify!(fname)`),
// so the bodies are the functions themselves. `ext_math_*` names would make
// the extension unusable from DuckDB-compatible SQL; the crate-internal
// helpers above carry the only non-SQL names.

pinkdb_extension! {
    name = "math",

    functions = [
        /// `exp(x)` — `e` raised to `x`. `exp(0)` = 1, `exp(1000)` = inf.
        fn exp(x: Option<f64>) -> Option<f64> {
            x.map(f64::exp)
        },

        /// `ln(x)` — the natural logarithm; `ln(0)` = -inf, `ln(-1)` = NaN.
        fn ln(x: Option<f64>) -> Option<f64> {
            x.map(f64::ln)
        },

        /// `log(x)` (base 10) or `log(b, x)` (base `b`).
        ///
        /// The SDK's `optional(1)` arity: the call may stop after the first
        /// argument, and [`Optional`] keeps "not passed" distinct from NULL.
        /// The two-argument composition is DuckDB's: `log10(x)/log10(b)`.
        optional(1) fn log(b: Option<f64>, x: Optional<f64>) -> Option<f64> {
            let b = b?;
            Some(match x {
                Optional::Absent => b.log10(),
                Optional::Null => return None,
                Optional::Value(x) => x.log10() / b.log10(),
            })
        },

        /// `log2(x)` — the base-2 logarithm.
        fn log2(x: Option<f64>) -> Option<f64> {
            x.map(f64::log2)
        },

        /// `log10(x)` — the base-10 logarithm.
        fn log10(x: Option<f64>) -> Option<f64> {
            x.map(f64::log10)
        },

        /// `sin(x)`.
        fn sin(x: Option<f64>) -> Option<f64> {
            x.map(f64::sin)
        },

        /// `cos(x)`.
        fn cos(x: Option<f64>) -> Option<f64> {
            x.map(f64::cos)
        },

        /// `tan(x)`.
        fn tan(x: Option<f64>) -> Option<f64> {
            x.map(f64::tan)
        },

        /// `cot(x)` — the cotangent, DuckDB's `1/tan(x)`; `cot(0)` = inf.
        fn cot(x: Option<f64>) -> Option<f64> {
            x.map(|x| 1.0 / x.tan())
        },

        /// `asin(x)` — NaN outside `[-1, 1]` (DuckDB).
        fn asin(x: Option<f64>) -> Option<f64> {
            x.map(f64::asin)
        },

        /// `acos(x)` — NaN outside `[-1, 1]` (DuckDB).
        fn acos(x: Option<f64>) -> Option<f64> {
            x.map(f64::acos)
        },

        /// `atan(x)` — `±π/2` at `±inf`.
        fn atan(x: Option<f64>) -> Option<f64> {
            x.map(f64::atan)
        },

        /// `atan2(y, x)` — the angle of the point `(x, y)`, `y` first
        /// (DuckDB's parameter order).
        fn atan2(y: Option<f64>, x: Option<f64>) -> Option<f64> {
            Some(y?.atan2(x?))
        },

        /// `degrees(x)` — radians to degrees (`degrees(pi())` = 180).
        fn degrees(x: Option<f64>) -> Option<f64> {
            x.map(f64::to_degrees)
        },

        /// `radians(x)` — degrees to radians (`radians(180)` = π).
        fn radians(x: Option<f64>) -> Option<f64> {
            x.map(f64::to_radians)
        },

        /// `pi()` — the ratio of a circle's circumference to its diameter.
        fn pi() -> f64 {
            std::f64::consts::PI
        },
    ],

    volatile = [
        /// `random()` — a uniform DOUBLE in `[0, 1)`, from a process-global
        /// SplitMix64 stream; refused in any statement the engine WAL-logs.
        fn random() -> f64 {
            // 53 bits of mantissa: `next >> 11` is uniform in [0, 2^53), and
            // 2^53 * 2^-53 covers [0, 1) exactly (DuckDB's range).
            (next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
        },
    ],
}

/// The extension as the engine registers it: the declared functions, exactly
/// as [`pinkdb_extension!`] generated them.
pub fn extension() -> Result<pinkdb_ext_sdk::LinkedExtension, pinkdb_ext_sdk::ExtError> {
    pinkdb_linked_extension()
}

/// Publish the extension in the process-global linked registry. Strict: fails
/// if the name is already taken, whatever published it.
pub fn register() -> Result<(), pinkdb_ext_sdk::ExtError> {
    pinkdb_ext_sdk::register_linked_extension(extension()?)
}

/// Publish the extension unless it is already published *with these exact
/// functions* — the idempotent bootstrap for hosts (and tests) that may run
/// more than once in one process. A name owned by a different extension is
/// still an error.
pub fn ensure_registered() -> Result<(), pinkdb_ext_sdk::ExtError> {
    match register() {
        Ok(()) => Ok(()),
        Err(error) => match pinkdb_ext_sdk::linked::get(EXTENSION_NAME) {
            Some(existing) if same_shape(&existing) => Ok(()),
            _ => Err(error),
        },
    }
}

/// Whether `existing` declares exactly our function names (used to tell a
/// second bootstrap of *this* extension from a foreign owner of the name).
fn same_shape(existing: &pinkdb_ext_sdk::LinkedExtension) -> bool {
    let Ok(ours) = extension() else {
        return false;
    };
    let mut a: Vec<&str> = ours.functions().iter().map(|f| f.name()).collect();
    let mut b: Vec<&str> = existing.functions().iter().map(|f| f.name()).collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb_ext_sdk::{ExtArg, ExtType, ExtValue};

    /// The exact `f64` bits of an `Option<f64>`, for NaN/±0 comparisons.
    fn bits(x: Option<f64>) -> Option<u64> {
        x.map(f64::to_bits)
    }

    /// Pinned against DuckDB α46057 (the logged `duckdiff` probes cover the
    /// same values through SQL).
    #[test]
    fn exponential_and_logarithm_family_matches_duckdb() {
        assert_eq!(exp(Some(0.0)), Some(1.0));
        assert_eq!(exp(Some(1.0)), Some(std::f64::consts::E));
        assert_eq!(exp(Some(1000.0)), Some(f64::INFINITY));
        assert_eq!(exp(Some(f64::NEG_INFINITY)), Some(0.0));
        assert!(exp(Some(f64::NAN)).unwrap().is_nan());
        assert_eq!(exp(None), None);

        assert_eq!(ln(Some(1.0)), Some(0.0));
        assert_eq!(ln(Some(std::f64::consts::E)), Some(1.0));
        assert_eq!(ln(Some(f64::INFINITY)), Some(f64::INFINITY));
        // ln(0) is -inf and ln(-1) is NaN (DuckDB prints `-inf` and `nan`).
        assert_eq!(ln(Some(0.0)), Some(f64::NEG_INFINITY));
        assert!(ln(Some(-1.0)).unwrap().is_nan());
        assert_eq!(ln(None), None);

        assert_eq!(log2(Some(8.0)), Some(3.0));
        assert_eq!(log2(Some(0.0)), Some(f64::NEG_INFINITY));
        assert_eq!(log10(Some(1000.0)), Some(3.0));
        assert_eq!(log10(Some(1.0)), Some(0.0));
        assert!(log10(Some(-2.0)).unwrap().is_nan());
    }

    #[test]
    fn log_has_ducks_two_overloads_and_keeps_absent_distinct_from_null() {
        // One argument: base 10 (DuckDB's `log(x)`).
        assert_eq!(log(Some(100.0), Optional::Absent), Some(2.0));
        assert_eq!(log(Some(0.0), Optional::Absent), Some(f64::NEG_INFINITY));
        assert!(log(Some(-1.0), Optional::Absent).unwrap().is_nan());
        // Two arguments: the first is the base (DuckDB's `[b, x]` order).
        assert_eq!(log(Some(2.0), Optional::Value(8.0)), Some(3.0));
        assert_eq!(
            bits(log(Some(8.0), Optional::Value(2.0))),
            bits(Some(0.33333333333333337))
        );
        // DuckDB composes the two-argument form from log10 (the natural-log
        // ratio is one ulp off here).
        assert_eq!(
            bits(log(Some(10.0), Optional::Value(0.5))),
            bits(Some(-std::f64::consts::LOG10_2))
        );
        assert_eq!(
            bits(log(Some(5.0), Optional::Value(2.0))),
            bits(Some(0.430676558073393))
        );
        // DuckDB's probed corners for the two-argument form.
        assert_eq!(log(Some(1.0), Optional::Value(2.0)), Some(f64::INFINITY));
        assert_eq!(bits(log(Some(0.0), Optional::Value(2.0))), bits(Some(-0.0)));
        assert_eq!(
            log(Some(2.0), Optional::Value(0.0)),
            Some(f64::NEG_INFINITY)
        );
        assert_eq!(log(Some(2.0), Optional::Value(1.0)), Some(0.0));
        assert!(log(Some(2.0), Optional::Value(-1.0)).unwrap().is_nan());
        assert!(log(Some(-2.0), Optional::Value(8.0)).unwrap().is_nan());
        // NULL wins in every position — never the one-argument form.
        assert_eq!(log(None, Optional::Value(2.0)), None);
        assert_eq!(log(Some(2.0), Optional::Null), None);
        assert_eq!(log(None, Optional::Absent), None);
    }

    #[test]
    fn trigonometric_family_matches_duckdb() {
        assert_eq!(sin(Some(0.0)), Some(0.0));
        assert_eq!(cos(Some(0.0)), Some(1.0));
        assert_eq!(tan(Some(0.0)), Some(0.0));
        assert_eq!(sin(Some(std::f64::consts::FRAC_PI_2)), Some(1.0));
        assert_eq!(cot(Some(0.0)), Some(f64::INFINITY));
        // 1/tan(pi/4) is 1 + 1 ulp (tan's own rounding), like DuckDB's libm.
        assert!((cot(Some(std::f64::consts::FRAC_PI_4)).unwrap() - 1.0).abs() < 1e-15);
        assert!(cot(Some(0.0)).unwrap().is_infinite());
        // Out-of-domain angles are NaN, like DuckDB.
        assert!(asin(Some(2.0)).unwrap().is_nan());
        assert!(acos(Some(2.0)).unwrap().is_nan());
        assert_eq!(asin(Some(1.0)), Some(std::f64::consts::FRAC_PI_2));
        assert_eq!(acos(Some(1.0)), Some(0.0));
        assert_eq!(atan(Some(f64::INFINITY)), Some(std::f64::consts::FRAC_PI_2));
        // atan2 takes (y, x); DuckDB's probed corners.
        assert_eq!(atan2(Some(0.0), Some(0.0)), Some(0.0));
        assert_eq!(
            atan2(Some(1.0), Some(0.0)),
            Some(std::f64::consts::FRAC_PI_2)
        );
        assert_eq!(atan2(Some(0.0), Some(-1.0)), Some(std::f64::consts::PI));
        assert_eq!(
            atan2(Some(-1.0), Some(0.0)),
            Some(-std::f64::consts::FRAC_PI_2)
        );
        // NaN/∞ propagation is the platform's `f64` behavior in both engines.
        assert!(tan(Some(f64::NAN)).unwrap().is_nan());
        assert!(sin(Some(f64::INFINITY)).unwrap().is_nan());
    }

    #[test]
    fn conversions_and_pi_match_duckdb() {
        assert_eq!(degrees(Some(pi())), Some(180.0));
        assert_eq!(radians(Some(180.0)), Some(pi()));
        assert_eq!(pi(), std::f64::consts::PI);
        assert_eq!(degrees(Some(0.0)), Some(0.0));
        assert_eq!(radians(Some(0.0)), Some(0.0));
        assert_eq!(degrees(None), None);
        assert_eq!(radians(None), None);
    }

    #[test]
    fn null_propagation_is_strict_everywhere() {
        assert_eq!(exp(None), None);
        assert_eq!(ln(None), None);
        assert_eq!(log2(None), None);
        assert_eq!(log10(None), None);
        assert_eq!(sin(None), None);
        assert_eq!(cos(None), None);
        assert_eq!(tan(None), None);
        assert_eq!(cot(None), None);
        assert_eq!(asin(None), None);
        assert_eq!(acos(None), None);
        assert_eq!(atan(None), None);
        assert_eq!(atan2(None, Some(1.0)), None);
        assert_eq!(atan2(Some(1.0), None), None);
    }

    #[test]
    fn random_is_uniform_double_in_the_unit_interval() {
        let first = random();
        assert!((0.0..1.0).contains(&first), "{first}");
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..1000 {
            let value = random();
            assert!((0.0..1.0).contains(&value), "{value}");
            distinct.insert(value.to_bits());
        }
        // A stream that never advances (or a broken seed) would fail here.
        assert!(distinct.len() > 900, "{} distinct values", distinct.len());
        // Concurrency: the exchange loop must not hand out duplicates.
        let mut handles = Vec::new();
        for _ in 0..4 {
            handles.push(std::thread::spawn(|| {
                (0..250).map(|_| random().to_bits()).collect::<Vec<_>>()
            }));
        }
        let mut all = std::collections::HashSet::new();
        for handle in handles {
            for bits in handle.join().unwrap() {
                all.insert(bits);
            }
        }
        assert!(all.len() > 900, "{} distinct across threads", all.len());
    }

    /// The declaration, through the SDK: the SQL names are DuckDB's, the
    /// arities are pinned, and `random` is the volatile one.
    #[test]
    fn declaration_metadata_is_pinned() {
        let ext = extension().unwrap();
        assert_eq!(ext.name(), "math");
        let names: Vec<&str> = ext.functions().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                "exp", "ln", "log", "log2", "log10", "sin", "cos", "tan", "cot", "asin", "acos",
                "atan", "atan2", "degrees", "radians", "pi", "random",
            ]
        );
        let by_name = |name: &str| {
            ext.functions()
                .iter()
                .find(|f| f.name() == name)
                .unwrap()
                .clone()
        };
        assert_eq!(by_name("exp").min_args(), 1);
        assert_eq!(by_name("exp").max_args(), 1);
        assert_eq!(by_name("exp").ret_type(), ExtType::Double);
        assert_eq!(by_name("exp").arg_type_at(0), ExtType::Double);
        let log = by_name("log");
        assert_eq!((log.min_args(), log.max_args()), (1, 2));
        assert!(!log.is_variadic());
        assert_eq!(log.arg_type_at(1), ExtType::Double);
        assert_eq!(by_name("pi").min_args(), 0);
        assert_eq!(by_name("pi").max_args(), 0);
        assert!(by_name("random").is_volatile());
        assert!(!by_name("exp").is_volatile());
        // None of the names shadows an engine built-in (the engine refuses
        // the whole load otherwise): the overlapping DuckDB math surface stays
        // built-in. This list is the extension's registered set, verbatim.
        for builtin in [
            "sqrt", "pow", "power", "floor", "ceil", "sign", "greatest", "least",
        ] {
            assert!(
                !names.contains(&builtin),
                "{builtin} is an engine built-in, not part of this extension"
            );
        }
    }

    /// The generated callbacks marshal exactly like direct calls, including
    /// the ranged `log` and every NULL path.
    #[test]
    fn callbacks_marshal_arguments() {
        let ext = extension().unwrap();
        let call = |name: &str, args: &[ExtArg<'_>]| {
            ext.functions()
                .iter()
                .find(|f| f.name() == name)
                .unwrap()
                .call(args)
                .unwrap()
        };
        assert_eq!(call("exp", &[ExtArg::Double(0.0)]), ExtValue::Double(1.0));
        assert_eq!(
            call("exp", &[ExtArg::Null(ExtType::Double)]),
            ExtValue::Null
        );
        // The ranged arity: a short call decodes as `Absent`.
        assert_eq!(call("log", &[ExtArg::Double(100.0)]), ExtValue::Double(2.0));
        assert_eq!(
            call("log", &[ExtArg::Double(2.0), ExtArg::Double(8.0)]),
            ExtValue::Double(3.0)
        );
        assert_eq!(
            call("log", &[ExtArg::Double(2.0), ExtArg::Null(ExtType::Double)]),
            ExtValue::Null
        );
        assert_eq!(
            call("atan2", &[ExtArg::Double(1.0), ExtArg::Double(0.0)]),
            ExtValue::Double(std::f64::consts::FRAC_PI_2)
        );
        assert_eq!(call("pi", &[]), ExtValue::Double(std::f64::consts::PI));
        // A NULL into a plain `Option<f64>` parameter is fine (it maps to
        // None); a NULL into the *required* position of `log` is NULL too.
        assert_eq!(
            call("sin", &[ExtArg::Null(ExtType::Double)]),
            ExtValue::Null
        );
    }
}
