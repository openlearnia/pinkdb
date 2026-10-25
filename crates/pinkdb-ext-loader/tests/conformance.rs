//! ABI conformance: one fixture, four SDKs, the same assertions.
//!
//! `ext-sdk/conformance/README.md` is the contract. Each SDK implements the
//! fixture once (`ext-sdk/c/conformance`, `ext-sdk/cpp/conformance`,
//! `ext-sdk/go/conformance`, `ext-sdk/rust/conformance`); this test builds all
//! four and drives them through the loader's public API, so an SDK is "ABI
//! complete" only when its fixture passes the same matrix as the others:
//! every value tag in both directions, NULL and zero-length results,
//! host-allocated VARCHAR results, error codes, panic containment, the
//! volatile flag, foreign-thread callbacks and the `struct_size`
//! negotiation.
//!
//! The allocation-counter half of the matrix lives in
//! `tests/conformance_ownership.rs`: the counters are process-global, so it
//! gets its own process.
//!
//! Missing toolchains (Go, or a C/C++ compiler) skip their fixture and say
//! so. `PINKDB_CONFORMANCE_STRICT=1` turns a skip into a failure — CI's
//! `sdk-smokes` job sets it, because that job installs every toolchain.

mod common;

use std::ffi::{c_char, c_void};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use common::conformance::{Fixture, build_dir, fixtures, path_string, repo_root};
use pinkdb_ext_loader::{
    DynamicFunction, ExtArg, ExtError, ExtType, ExtValue, LoadedExtension,
    PINKDB_EXT_API_MINOR2_SIZE, PINKDB_EXT_API_V1_SIZE, PinkdbExtInitFn, PinkdbExtScalarFn,
    PinkdbHostApi, load,
};

/// The functions the contract requires, in registration order.
const REQUIRED: [&str; 16] = [
    "cf_echo",
    "cf_bool",
    "cf_int",
    "cf_bigint",
    "cf_double",
    "cf_date",
    "cf_timestamp",
    "cf_interval",
    "cf_variant",
    "cf_all",
    "cf_empty",
    "cf_fill",
    "cf_fail",
    "cf_unwind",
    "cf_volatile",
    "cf_thread",
];

fn load_fixture(fixture: &Fixture) -> LoadedExtension {
    load(&path_string(&fixture.library)).unwrap_or_else(|e| {
        panic!(
            "the {} conformance fixture failed to load: {e}",
            fixture.sdk
        )
    })
}

fn function<'a>(ext: &'a LoadedExtension, name: &str) -> &'a DynamicFunction {
    ext.functions()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("the fixture registered no {name}()"))
}

fn call(ext: &LoadedExtension, name: &str, args: &[ExtArg<'_>]) -> ExtValue {
    function(ext, name)
        .call(args)
        .unwrap_or_else(|e| panic!("{name}() failed: {e}"))
}

fn text(value: ExtValue) -> Vec<u8> {
    match value {
        ExtValue::Varchar(bytes) => bytes,
        other => panic!("expected a VARCHAR, got {other:?}"),
    }
}

/// `pinkdb_conformance_caps` and friends: introspection symbols, opened
/// directly, the way `tests/common` reads the C fixtures' probes.
struct Counters {
    _library: libloading::Library,
    calls: unsafe extern "C" fn() -> i64,
    foreign_threads: unsafe extern "C" fn() -> i64,
    caps: unsafe extern "C" fn() -> i64,
}

impl Counters {
    fn open(fixture: &Fixture) -> Counters {
        // SAFETY: opening the fixture again returns the already-mapped image
        // (dlopen refcounts); the symbols are the fixture's own, exported with
        // C linkage.
        unsafe {
            let library = libloading::Library::new(&fixture.library)
                .unwrap_or_else(|e| panic!("cannot reopen the {} fixture: {e}", fixture.sdk));
            let calls = *library
                .get::<unsafe extern "C" fn() -> i64>(b"pinkdb_conformance_calls\0")
                .expect("pinkdb_conformance_calls");
            let foreign_threads = *library
                .get::<unsafe extern "C" fn() -> i64>(b"pinkdb_conformance_foreign_threads\0")
                .expect("pinkdb_conformance_foreign_threads");
            let caps = *library
                .get::<unsafe extern "C" fn() -> i64>(b"pinkdb_conformance_caps\0")
                .expect("pinkdb_conformance_caps");
            Counters {
                _library: library,
                calls,
                foreign_threads,
                caps,
            }
        }
    }
}

#[test]
fn every_fixture_loads_and_registers_the_contract() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        assert_eq!(ext.name(), "conformance_ext", "{}", fixture.sdk);
        let names: Vec<&str> = ext.functions().iter().map(|f| f.name()).collect();
        assert_eq!(
            names.len(),
            REQUIRED.len(),
            "{} registered {names:?}",
            fixture.sdk
        );
        for required in REQUIRED {
            assert!(
                names.contains(&required),
                "{} did not register {required}() (has {names:?})",
                fixture.sdk
            );
        }
        for f in ext.functions() {
            let expected = f.name() == "cf_volatile";
            assert_eq!(
                f.is_volatile(),
                expected,
                "{}: {}() is_volatile",
                fixture.sdk,
                f.name()
            );
        }
        // The caps word is part of the contract: bit 0 = cf_fail allocates
        // before failing, bit 1 = cf_unwind really unwinds.
        // SAFETY: a plain i64 load of an atomic inside the fixture.
        let caps = unsafe { (Counters::open(fixture).caps)() };
        let expected_caps =
            i64::from(fixture.fail_allocates) | (i64::from(fixture.unwind_is_real) << 1);
        assert_eq!(caps, expected_caps, "{} caps word", fixture.sdk);
    }
}

#[test]
fn values_round_trip_in_every_tag() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let sdk = fixture.sdk;

        // VARCHAR: bytes verbatim, including embedded NULs and multibyte.
        for bytes in [
            b"hello".as_slice(),
            b"".as_slice(),
            b"a\0b".as_slice(),
            "héllo ✓".as_bytes(),
        ] {
            assert_eq!(
                text(call(&ext, "cf_echo", &[ExtArg::Varchar(bytes)])),
                bytes,
                "{sdk} cf_echo"
            );
        }
        assert_eq!(
            call(&ext, "cf_echo", &[ExtArg::Null(ExtType::Varchar)]),
            ExtValue::Null,
            "{sdk}"
        );

        assert_eq!(
            call(&ext, "cf_bool", &[ExtArg::Boolean(true)]),
            ExtValue::Boolean(true),
            "{sdk}"
        );
        assert_eq!(
            call(&ext, "cf_bool", &[ExtArg::Boolean(false)]),
            ExtValue::Boolean(false),
            "{sdk}"
        );
        assert_eq!(
            call(&ext, "cf_bool", &[ExtArg::Null(ExtType::Boolean)]),
            ExtValue::Null,
            "{sdk}"
        );

        for value in [0, -1, i32::MIN, i32::MAX] {
            assert_eq!(
                call(&ext, "cf_int", &[ExtArg::Integer(value)]),
                ExtValue::Integer(value),
                "{sdk} cf_int({value})"
            );
        }
        assert_eq!(
            call(&ext, "cf_int", &[ExtArg::Null(ExtType::Integer)]),
            ExtValue::Null,
            "{sdk}"
        );

        for value in [0, -1, i64::MIN, i64::MAX] {
            assert_eq!(
                call(&ext, "cf_bigint", &[ExtArg::Bigint(value)]),
                ExtValue::Bigint(value),
                "{sdk} cf_bigint({value})"
            );
        }
        assert_eq!(
            call(&ext, "cf_bigint", &[ExtArg::Null(ExtType::Bigint)]),
            ExtValue::Null,
            "{sdk}"
        );

        // DOUBLE is compared bit-for-bit: NaN payloads and ±0 must survive
        // four languages' marshalling untouched.
        for value in [
            4.5,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            f64::MIN,
        ] {
            let got = call(&ext, "cf_double", &[ExtArg::Double(value)]);
            let ExtValue::Double(got) = got else {
                panic!("{sdk} cf_double returned {got:?}")
            };
            assert_eq!(
                got.to_bits(),
                value.to_bits(),
                "{sdk} cf_double bits for {value}"
            );
        }
        assert_eq!(
            call(&ext, "cf_double", &[ExtArg::Null(ExtType::Double)]),
            ExtValue::Null,
            "{sdk}"
        );

        for value in [0, 1, -1, i32::MIN, i32::MAX] {
            assert_eq!(
                call(&ext, "cf_date", &[ExtArg::Date(value)]),
                ExtValue::Date(value),
                "{sdk} cf_date({value})"
            );
        }
        assert_eq!(
            call(&ext, "cf_date", &[ExtArg::Null(ExtType::Date)]),
            ExtValue::Null,
            "{sdk}"
        );

        for value in [0, 1, -1, i64::MIN, i64::MAX] {
            assert_eq!(
                call(&ext, "cf_timestamp", &[ExtArg::Timestamp(value)]),
                ExtValue::Timestamp(value),
                "{sdk} cf_timestamp({value})"
            );
        }
        assert_eq!(
            call(&ext, "cf_timestamp", &[ExtArg::Null(ExtType::Timestamp)]),
            ExtValue::Null,
            "{sdk}"
        );

        // INTERVAL: micros in the int64, months/days packed into the bits of
        // the double. The extremes catch a sign-extension or endianness bug.
        for (months, days, micros) in [
            (0, 0, 0),
            (1, 2, 3),
            (-1, -2, -3),
            (i32::MIN, i32::MAX, i64::MIN),
            (i32::MAX, i32::MIN, i64::MAX),
        ] {
            assert_eq!(
                call(
                    &ext,
                    "cf_interval",
                    &[ExtArg::Interval {
                        months,
                        days,
                        micros
                    }]
                ),
                ExtValue::Interval {
                    months,
                    days,
                    micros
                },
                "{sdk} cf_interval({months}, {days}, {micros})"
            );
        }
        assert_eq!(
            call(&ext, "cf_interval", &[ExtArg::Null(ExtType::Interval)]),
            ExtValue::Null,
            "{sdk}"
        );

        // VARIANT: the payload is opaque bytes on the text channel, so a
        // fixture that is not the engine cannot interpret it — it must come
        // back byte for byte, empty and embedded-NUL payloads included.
        for bytes in [
            [].as_slice(),
            [0x00].as_slice(),
            [0x18, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00].as_slice(),
            [0x11, 0x02, 0x00, 0x01, 0x02, 0x61, 0x62].as_slice(),
        ] {
            assert_eq!(
                call(&ext, "cf_variant", &[ExtArg::Variant(bytes)]),
                ExtValue::Variant(bytes.to_vec()),
                "{sdk} cf_variant({bytes:?})"
            );
        }
        assert_eq!(
            call(&ext, "cf_variant", &[ExtArg::Null(ExtType::Variant)]),
            ExtValue::Null,
            "{sdk}"
        );
    }
}

#[test]
fn cf_all_renders_the_canonical_summary() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let sdk = fixture.sdk;

        let row = text(call(
            &ext,
            "cf_all",
            &[
                ExtArg::Boolean(true),
                ExtArg::Integer(-2),
                ExtArg::Bigint(3),
                ExtArg::Double(4.5),
                ExtArg::Varchar(b"xy"),
                ExtArg::Date(7),
                ExtArg::Timestamp(8),
                ExtArg::Interval {
                    months: 1,
                    days: 2,
                    micros: 3,
                },
            ],
        ));
        assert_eq!(
            String::from_utf8(row).unwrap(),
            "b=1,i=-2,big=3,d=0x4012000000000000,v=xy,date=7,ts=8,iv=1/2/3",
            "{sdk} cf_all"
        );

        let nulls = text(call(
            &ext,
            "cf_all",
            &[
                ExtArg::Null(ExtType::Boolean),
                ExtArg::Null(ExtType::Integer),
                ExtArg::Null(ExtType::Bigint),
                ExtArg::Null(ExtType::Double),
                ExtArg::Null(ExtType::Varchar),
                ExtArg::Null(ExtType::Date),
                ExtArg::Null(ExtType::Timestamp),
                ExtArg::Null(ExtType::Interval),
            ],
        ));
        assert_eq!(
            String::from_utf8(nulls).unwrap(),
            "b=null,i=null,big=null,d=null,v=null,date=null,ts=null,iv=null",
            "{sdk} cf_all with NULLs"
        );
    }
}

#[test]
fn varchar_edge_cases() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let sdk = fixture.sdk;

        // A zero-length, non-NULL result: `text_len = 0`, and the bytes are
        // empty whether or not the SDK allocated a pointer.
        assert_eq!(text(call(&ext, "cf_empty", &[])), Vec::<u8>::new(), "{sdk}");

        assert_eq!(
            text(call(&ext, "cf_fill", &[ExtArg::Bigint(0)])),
            Vec::<u8>::new(),
            "{sdk}"
        );
        assert_eq!(
            text(call(&ext, "cf_fill", &[ExtArg::Bigint(5)])),
            b"xxxxx".to_vec(),
            "{sdk}"
        );
        assert_eq!(
            text(call(&ext, "cf_fill", &[ExtArg::Bigint(1)])),
            b"x".to_vec(),
            "{sdk}"
        );
        assert_eq!(
            call(&ext, "cf_fill", &[ExtArg::Bigint(-1)]),
            ExtValue::Null,
            "{sdk}"
        );

        // Over the host's 64 MiB limit: the SDK may refuse it itself (an
        // error code) or hand it back, in which case the *host* must refuse to
        // read or free it. Either way no bytes reach the engine.
        let oversized = i64::from(64 * 1024 * 1024) + 1;
        match function(&ext, "cf_fill").call(&[ExtArg::Bigint(oversized)]) {
            Err(ExtError::Callback { code, .. }) => assert_ne!(code, 0, "{sdk}"),
            Err(ExtError::BadResult { message, .. }) => assert!(
                message.contains("over the"),
                "{sdk} refused the oversized VARCHAR for the wrong reason: {message}"
            ),
            other => panic!("{sdk} cf_fill({oversized}) must not succeed: {other:?}"),
        }
    }
}

#[test]
fn error_paths_report_callback_codes() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let error = function(&ext, "cf_fail")
            .call(&[ExtArg::Bigint(1)])
            .expect_err("cf_fail always fails");
        match error {
            ExtError::Callback { function, code } => {
                assert_eq!(function, "cf_fail", "{}", fixture.sdk);
                assert_eq!(code, 1, "{}", fixture.sdk);
            }
            other => panic!("{} cf_fail gave {other:?}", fixture.sdk),
        }
        // The failure is a value-level error, not a wedged extension.
        assert_eq!(
            call(&ext, "cf_bigint", &[ExtArg::Bigint(9)]),
            ExtValue::Bigint(9),
            "{} still works after cf_fail",
            fixture.sdk
        );
    }
}

#[test]
fn unwinding_is_contained_at_the_boundary() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let error = function(&ext, "cf_unwind")
            .call(&[])
            .expect_err("cf_unwind never returns a value");
        match error {
            ExtError::Callback { function, code } => {
                assert_eq!(function, "cf_unwind", "{}", fixture.sdk);
                assert_eq!(code, fixture.unwind_code, "{}", fixture.sdk);
            }
            other => panic!(
                "{} cf_unwind escaped the SDK's guard instead of returning a code: {other:?}",
                fixture.sdk
            ),
        }
        // The panic did not poison the extension or the loader.
        assert_eq!(
            text(call(&ext, "cf_echo", &[ExtArg::Varchar(b"after")])),
            b"after".to_vec(),
            "{} still answers after an unwind",
            fixture.sdk
        );
    }
}

#[test]
fn volatile_functions_are_flagged() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let f = function(&ext, "cf_volatile");
        assert!(f.is_volatile(), "{}", fixture.sdk);
        assert_eq!(
            f.call(&[]).unwrap(),
            ExtValue::Bigint(42),
            "{}",
            fixture.sdk
        );
    }
}

#[test]
fn callbacks_run_on_foreign_threads() {
    let _serial = serialized();
    for fixture in fixtures() {
        let ext = load_fixture(fixture);
        let counters = Counters::open(fixture);
        // SAFETY: the counters are plain i64 loads of atomics inside the
        // fixture; no arguments, no unwinding.
        let (before_calls, before_foreign) =
            unsafe { ((counters.calls)(), (counters.foreign_threads)()) };

        // The loader promises callbacks may arrive on any thread, including
        // concurrently: drive one from each of several spawned threads.
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let f = function(&ext, "cf_thread");
                scope.spawn(move || {
                    for _ in 0..25 {
                        assert_eq!(f.call(&[]).unwrap(), ExtValue::Bigint(1));
                    }
                });
            }
        });

        // SAFETY: as above.
        let (after_calls, after_foreign) =
            unsafe { ((counters.calls)(), (counters.foreign_threads)()) };
        assert!(
            after_calls - before_calls >= 100,
            "{} counted {} cf_thread calls",
            fixture.sdk,
            after_calls - before_calls
        );
        assert!(
            after_foreign > before_foreign,
            "{} saw no callback on a foreign thread (calls delta {})",
            fixture.sdk,
            after_calls - before_calls
        );
    }
}

/// One registration a fixture attempted, as the stub host saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Attempt {
    name: String,
    volatile: bool,
}

/// Registrations attempted during the current init. The stub host is
/// process-global state (C callbacks), so the test serializes on it.
static ATTEMPTS: Mutex<Vec<Attempt>> = Mutex::new(Vec::new());

/// Serializes the tests in this file.
///
/// The fixtures are process-global singletons, as the ABI intends (an
/// extension is initialized once per process): a C++ fixture's `g_host` or a
/// callback counter is shared by every handle, so two tests loading the same
/// library at the same time would see each other's registrations.
fn serialized() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn record(name: *const c_char, volatile: bool) -> i32 {
    let name = if name.is_null() {
        String::from("<NULL>")
    } else {
        // SAFETY: the ABI requires a NUL-terminated name, valid for the call.
        unsafe { std::ffi::CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned()
    };
    ATTEMPTS
        .lock()
        .expect("the attempts lock")
        .push(Attempt { name, volatile });
    0 // accept everything: this host is only observing
}

extern "C" fn stub_register(
    _ctx: *mut c_void,
    name: *const c_char,
    _n_args: u32,
    _arg_types: *const i32,
    _ret_type: i32,
    _callback: Option<PinkdbExtScalarFn>,
    _user_data: *mut c_void,
) -> i32 {
    record(name, false)
}

extern "C" fn stub_register_flags(
    _ctx: *mut c_void,
    name: *const c_char,
    _n_args: u32,
    _arg_types: *const i32,
    _ret_type: i32,
    flags: u32,
    _callback: Option<PinkdbExtScalarFn>,
    _user_data: *mut c_void,
) -> i32 {
    record(name, flags & 0x1 != 0)
}

extern "C" fn stub_execute(
    _ctx: *mut c_void,
    _sql: *const c_char,
    _sql_len: usize,
    _out_text: *mut *mut c_char,
    _out_len: *mut usize,
) -> i32 {
    2 // no live session
}

extern "C" fn stub_alloc(size: usize) -> *mut c_void {
    // SAFETY: `malloc(size)` is the host's pair for `free`.
    unsafe { malloc(size) }
}

extern "C" fn stub_free(ptr: *mut c_void) {
    // SAFETY: the host frees only pointers `stub_alloc` returned.
    unsafe { free(ptr) }
}

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
}

/// A five-field prefix of `PinkdbHostApi`: exactly the ABI v1 surface, which
/// is what `PINKDB_EXT_API_V1_SIZE` measures.
#[repr(C)]
struct LegacyHost {
    abi_version: u32,
    struct_size: u32,
    register_scalar: extern "C" fn(
        *mut c_void,
        *const c_char,
        u32,
        *const i32,
        i32,
        Option<PinkdbExtScalarFn>,
        *mut c_void,
    ) -> i32,
    alloc: extern "C" fn(usize) -> *mut c_void,
    free: extern "C" fn(*mut c_void),
}

fn legacy_host(abi_version: u32) -> LegacyHost {
    assert_eq!(
        std::mem::size_of::<LegacyHost>() as u32,
        PINKDB_EXT_API_V1_SIZE,
        "the legacy host must be exactly the ABI v1 surface"
    );
    LegacyHost {
        abi_version,
        struct_size: std::mem::size_of::<LegacyHost>() as u32,
        register_scalar: stub_register,
        alloc: stub_alloc,
        free: stub_free,
    }
}

/// A host that publishes the whole current struct.
fn full_host(abi_version: u32) -> PinkdbHostApi {
    PinkdbHostApi {
        abi_version,
        struct_size: PINKDB_EXT_API_MINOR2_SIZE,
        register_scalar: stub_register,
        alloc: stub_alloc,
        free: stub_free,
        register_scalar_flags: stub_register_flags,
        execute_statement: stub_execute,
    }
}

/// Run one fixture's `pinkdb_extension_init` against `host`, returning its
/// code and the registrations it attempted.
///
/// Only called in the child process (`init_in_a_child_process`): the fixtures
/// keep process-global state from their one legitimate init, so an extra init
/// in the test process itself would perturb the loads under test.
fn init_code(fixture: &Fixture, host: *const c_void) -> (i32, Vec<Attempt>) {
    ATTEMPTS.lock().expect("the attempts lock").clear();
    // SAFETY: opening the fixture again is refcounted by the dynamic loader;
    // the init signature is the ABI's, and every host built above is a valid
    // prefix of `PinkdbHostApi` with a truthful `struct_size`.
    let code = unsafe {
        let library = libloading::Library::new(&fixture.library)
            .unwrap_or_else(|e| panic!("cannot reopen the {} fixture: {e}", fixture.sdk));
        let init = library
            .get::<PinkdbExtInitFn>(b"pinkdb_extension_init\0")
            .expect("pinkdb_extension_init");
        let code = init(host.cast::<PinkdbHostApi>(), std::ptr::null_mut());
        // An extension is never unloaded (the header says so, and a Go
        // c-shared library cannot be): dropping the handle would `dlclose` it.
        std::mem::forget(library);
        code
    };
    let registered = ATTEMPTS.lock().expect("the attempts lock").clone();
    (code, registered)
}

/// The mode a [`init_in_a_child_process`] child runs: the last part of the
/// host struct's compatibility matrix.
fn child_host(mode: &str) -> Box<PinkdbHostApi> {
    match mode {
        "legacy" | "abi999" => {
            // The ABI major is the only variation: a host that stops at the v1
            // surface, either with our version or a foreign one.
            let abi = if mode == "legacy" { 1 } else { 999 };
            let host = legacy_host(abi);
            let mut boxed = Box::new(full_host(1));
            // SAFETY: `LegacyHost` is a prefix of `PinkdbHostApi` and
            // `struct_size` says so, which is exactly what the fixture reads.
            boxed.struct_size = host.struct_size;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (&host as *const LegacyHost).cast::<u8>(),
                    (&mut *boxed as *mut PinkdbHostApi).cast::<u8>(),
                    std::mem::size_of::<LegacyHost>(),
                );
            }
            boxed
        }
        "full" => Box::new(full_host(1)),
        other => panic!("unknown negotiation mode {other}"),
    }
}

/// Init one fixture in a fresh process and report what it did.
///
/// A child process per fixture is the only honest way to drive
/// `pinkdb_extension_init` with a host stub: the ABI calls init exactly once
/// per process, and every fixture keeps process-global state afterwards.
#[test]
fn init_in_a_child_process() {
    let Ok(mode) = std::env::var("PINKDB_CONFORMANCE_INIT") else {
        return; // not a child invocation: nothing to do
    };
    let path = std::env::var("PINKDB_CONFORMANCE_LIB").expect("PINKDB_CONFORMANCE_LIB");
    let fixture = Fixture {
        sdk: "child",
        library: PathBuf::from(path),
        unwind_code: 0,
        unwind_is_real: false,
        fail_allocates: false,
    };
    let host = child_host(&mode);
    let (code, attempts) = init_code(&fixture, (&*host as *const PinkdbHostApi).cast::<c_void>());
    let report = std::env::var("PINKDB_CONFORMANCE_REPORT").expect("PINKDB_CONFORMANCE_REPORT");
    let mut text = format!("code {code}\n");
    for attempt in attempts {
        text.push_str(&format!(
            "registered {} {}\n",
            if attempt.volatile { 1 } else { 0 },
            attempt.name
        ));
    }
    std::fs::write(report, text).expect("write the negotiation report");
}

/// Run [`init_in_a_child_process`] for one fixture and mode.
fn init_in_child(fixture: &Fixture, mode: &str) -> (i32, Vec<Attempt>) {
    // The report goes to a file: the test harness owns stdout.
    let report = build_dir().join(format!("negotiation-{}-{mode}.txt", fixture.sdk));
    let _ = std::fs::remove_file(&report);
    let output = Command::new(std::env::current_exe().expect("the test binary path"))
        .args(["--exact", "init_in_a_child_process", "--test-threads=1"])
        .env("PINKDB_CONFORMANCE_INIT", mode)
        .env("PINKDB_CONFORMANCE_LIB", &fixture.library)
        .env("PINKDB_CONFORMANCE_REPORT", &report)
        .output()
        .expect("spawn the negotiation child");
    assert!(
        output.status.success(),
        "the {} {mode} negotiation child failed ({})\n{}",
        fixture.sdk,
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let report = std::fs::read_to_string(&report).unwrap_or_else(|e| {
        panic!(
            "the {} {mode} negotiation child reported nothing ({e})\n{}",
            fixture.sdk,
            String::from_utf8_lossy(&output.stderr)
        )
    });
    let mut code = None;
    let mut attempts = Vec::new();
    for line in report.lines() {
        if let Some(value) = line.strip_prefix("code ") {
            code = Some(value.trim().parse::<i32>().expect("a code"));
        } else if let Some(value) = line.strip_prefix("registered ") {
            let (volatile, name) = value.split_once(' ').expect("registered <0|1> <name>");
            attempts.push(Attempt {
                name: name.to_string(),
                volatile: volatile == "1",
            });
        }
    }
    (
        code.unwrap_or_else(|| panic!("the child reported no code:\n{report}")),
        attempts,
    )
}

#[test]
fn struct_size_and_abi_version_are_negotiated() {
    for fixture in fixtures() {
        // A host that stops before the ABI 1.2 fields. Every fixture declares a
        // volatile function, and a volatile declaration must never degrade to
        // a deterministic one, so all four refuse the load. The code is each
        // SDK's own (C 3, C++ 1, Go 10, Rust 4): the contract is that it is
        // non-zero and that nothing was registered as deterministic.
        let (code, registered) = init_in_child(fixture, "legacy");
        assert_ne!(
            code, 0,
            "{} accepted a host without register_scalar_flags",
            fixture.sdk
        );
        assert!(
            !registered
                .iter()
                .any(|a| a.name.eq_ignore_ascii_case("cf_volatile") && !a.volatile),
            "{} registered cf_volatile() as a deterministic function on a \
             flags-less host (attempts: {registered:?})",
            fixture.sdk
        );

        // The same fixture against a complete host: every registration is
        // accepted, so init succeeds and cf_volatile goes through the flagged
        // entry point.
        let (code, registered) = init_in_child(fixture, "full");
        assert_eq!(code, 0, "{} refused a complete host", fixture.sdk);
        assert_eq!(
            registered.len(),
            REQUIRED.len(),
            "{} registered {registered:?}",
            fixture.sdk
        );
        assert_eq!(
            registered
                .iter()
                .filter(|a| a.volatile)
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>(),
            ["cf_volatile"],
            "{} volatile registrations",
            fixture.sdk
        );

        // An extension built for another ABI major refuses first, before it
        // reads anything else out of the struct.
        let (code, registered) = init_in_child(fixture, "abi999");
        assert_ne!(code, 0, "{} accepted ABI version 999", fixture.sdk);
        assert!(
            registered.is_empty(),
            "{} registered {registered:?} against a foreign ABI version",
            fixture.sdk
        );
    }
}

#[test]
fn the_loader_header_and_the_sdk_copies_are_the_same_file() {
    // The conformance matrix is the behavioural half of header sync; this is
    // the cheap structural check that keeps the four copies comparable at all
    // (tests/header_sync.rs is the byte-exact one).
    let root = repo_root();
    for sdk in ["c", "cpp", "go", "rust"] {
        let path = root
            .join("ext-sdk")
            .join(sdk)
            .join("include")
            .join("pinkdb_extension.h");
        assert!(path.is_file(), "missing vendored header {}", path.display());
        let text = std::fs::read_to_string(&path).expect("the ABI header is UTF-8");
        assert!(
            text.contains("PINKDB_EXT_HOST_HAS_FLAGS") && text.contains("PINKDB_EXT_API_V1_SIZE"),
            "{} predates the struct_size negotiation macros",
            path.display()
        );
    }
}
