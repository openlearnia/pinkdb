//! C ABI v1 types, mirrored 1:1 from `include/pinkdb_extension.h`.
//!
//! The value model ([`ExtType`], [`ExtArg`], [`ExtValue`], the type tags and
//! the arity limit) is shared by both loading models and builds on every
//! target. The by-value C structs, callback signatures and host API are the
//! `dlopen` model's wire format and are therefore native-only ([`crate::
//! linked`] passes the Rust enums directly).
//!
//! Nothing here is unsafe: the structs are plain `repr(C)` data and the enums
//! are host-side conveniences. All pointer handling that needs `unsafe` lives
//! in [`crate::function`] and [`crate::host`].

#[cfg(not(target_arch = "wasm32"))]
use std::ffi::{c_char, c_void};
use std::fmt;

/// ABI **major** version implemented by this loader and required from
/// extensions. Wave 105 bumped the minor version only: a v1 extension (which
/// reports `1` here) still loads, because every addition after v1 is detected
/// at runtime — a bigger [`PinkdbHostApi`] `struct_size`, or the presence of
/// the optional `pinkdb_ext_serve` symbol.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_ABI_VERSION: u32 = 1;

/// ABI **minor** version implemented by this loader (major 1, minor 5).
///
/// Minor 2 adds [`PinkdbHostApi::execute_statement`] and the optional
/// `pinkdb_ext_serve` export; minor 3 adds [`ExtType::Variant`]
/// ([`PINKDB_EXT_TYPE_VARIANT`]); minor 4 adds the optional
/// `pinkdb_ext_command` export and [`PinkdbHostApi::execute_internal`];
/// minor 5 adds the fixed-size array types [`ExtType::ArrayDouble`] and
/// [`ExtType::ArrayFloat`] (wave 107a). All are additive, so minor 1 and
/// minor 0 extensions are unchanged. The wire exposes the minor only
/// indirectly (`struct_size`, symbol presence), which is why the major check
/// stayed an equality check — and why a new *type tag* needs no
/// `struct_size` landmark: a host only ever sends the types an extension
/// declared, and an older host rejects an unknown tag at registration.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_ABI_MINOR: u32 = 5;

/// Maximum argument count accepted by [`crate::PinkdbHostApi::register_scalar`]
/// and by [`crate::LinkedFunction::new`]. Kept identical for both loading
/// models so one extension's arity limit does not change with the target.
pub const PINKDB_EXT_MAX_ARGS: u32 = 64;

/// Registration flag: the function is volatile (nondeterministic) — see the
/// header's "Deterministic vs volatile functions" section. [`crate::abi::
/// PinkdbHostApi::register_scalar_flags`] is the only way to set it;
/// `register_scalar` is the same call with `flags == 0` (deterministic).
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_FLAG_VOLATILE: u32 = 0x1;

/// Every registration flag this loader knows. A registration carrying any
/// other bit is rejected: silently ignoring a flag is exactly how a declared
/// volatile function would become deterministic again.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_FLAG_KNOWN: u32 = PINKDB_EXT_FLAG_VOLATILE;

/// The `struct_size` of the ABI v1 surface: the fields every host has, up to
/// but not including the appended `register_scalar_flags`.
///
/// This mirrors the header's `PINKDB_EXT_API_V1_SIZE`; an extension that finds
/// a smaller `struct_size` must refuse the load rather than read a field the
/// host never published.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_API_V1_SIZE: u32 =
    std::mem::offset_of!(PinkdbHostApi, register_scalar_flags) as u32;

/// The `struct_size` of the ABI minor 1 surface (`PINKDB_EXT_API_FLAGS_SIZE`):
/// everything up to but not including `execute_statement`.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_API_FLAGS_SIZE: u32 =
    std::mem::offset_of!(PinkdbHostApi, execute_statement) as u32;

/// The `struct_size` of the ABI minor-2 host API
/// (`PINKDB_EXT_API_MINOR2_SIZE`): everything up to but not including
/// `execute_internal`. That is exactly `size_of` on a minor-2 or minor-3 host,
/// so the landmark keeps its meaning on both.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_API_MINOR2_SIZE: u32 =
    std::mem::offset_of!(PinkdbHostApi, execute_internal) as u32;

/// The `struct_size` of the current (minor 4) host API: the whole struct.
#[cfg(not(target_arch = "wasm32"))]
pub const PINKDB_EXT_API_MINOR4_SIZE: u32 = std::mem::size_of::<PinkdbHostApi>() as u32;

// Value / argument type tags (same numbering as the pinkdb C ABI).
pub const PINKDB_EXT_TYPE_NULL: i32 = 0;
pub const PINKDB_EXT_TYPE_BOOLEAN: i32 = 1;
pub const PINKDB_EXT_TYPE_INTEGER: i32 = 2;
pub const PINKDB_EXT_TYPE_BIGINT: i32 = 3;
pub const PINKDB_EXT_TYPE_DOUBLE: i32 = 4;
pub const PINKDB_EXT_TYPE_VARCHAR: i32 = 5;
pub const PINKDB_EXT_TYPE_DATE: i32 = 6;
pub const PINKDB_EXT_TYPE_TIMESTAMP: i32 = 7;
/// Calendar interval, three DuckDB fields (`months`, `days`, `micros`). The
/// tag number matches `pinkdb.h`'s `PINKDB_TYPE_INTERVAL`; 8 is that header's
/// DECIMAL, which extension ABI v1 deliberately does not support. The
/// by-value payload packing is [`pack_interval`] / [`unpack_interval`].
pub const PINKDB_EXT_TYPE_INTERVAL: i32 = 9;
/// Semi-structured value. The payload rides the VARCHAR channel
/// (`text_ptr` / `text_len`, exact byte count, no NUL): bytes in pinkdb's
/// VARIANT encoding, the same bytes `pinkdb::variant::encode` writes and
/// `pinkdb::variant::decode` reads. The tag number matches `pinkdb.h`'s
/// `PINKDB_TYPE_VARIANT` (that header renders the value as JSON text).
pub const PINKDB_EXT_TYPE_VARIANT: i32 = 10;
/// Fixed-size array of DOUBLE (FLOAT) elements, a pointer-and-length view of
/// `len` contiguous `double`s. The type is parametric, and the length has to
/// travel wherever a type tag travels — an argument or return *declaration*,
/// not just a value — so it is packed above the kind byte
/// ([`pack_array_len`] / [`unpack_array_len`]). The low byte alone is
/// [`PINKDB_EXT_TYPE_ARRAY_DOUBLE`], which is what an older host rejects as
/// an unknown tag at registration. INTEGER and BIGINT arrays have no ABI
/// type: [`ExtType::ArrayDouble`] is the only one.
pub const PINKDB_EXT_TYPE_ARRAY_DOUBLE: i32 = 11;

/// Fixed-size array of FLOAT (`f32`) elements: the 4-byte counterpart of
/// [`PINKDB_EXT_TYPE_ARRAY_DOUBLE`], packed the same way
/// ([`pack_array_float_len`]). Embeddings are `f32` on the wire and in storage,
/// so a distance kernel can read a `FLOAT[N]` argument with no conversion.
pub const PINKDB_EXT_TYPE_ARRAY_FLOAT: i32 = 12;

/// Pack an array length into the ARRAY_DOUBLE tag: `(len << 8) | 11`.
pub const fn pack_array_len(len: u32) -> i32 {
    PINKDB_EXT_TYPE_ARRAY_DOUBLE | ((len as i32) << 8)
}

/// The array length packed by [`pack_array_len`] (0 for any other tag).
pub const fn unpack_array_len(tag: i32) -> u32 {
    (tag as u32) >> 8
}

/// Pack an array length into the ARRAY_FLOAT tag: `(len << 8) | 12`.
pub const fn pack_array_float_len(len: u32) -> i32 {
    PINKDB_EXT_TYPE_ARRAY_FLOAT | ((len as i32) << 8)
}

/// Pack an interval's three fields into the by-value ABI payload: `micros`
/// goes in the `int64` field, `months`/`days` ride in the bits of the
/// `double` field (high 32 / low 32). `double` carries no numeric meaning for
/// an INTERVAL value — use [`unpack_interval`], never the float itself.
pub const fn pack_interval(months: i32, days: i32, micros: i64) -> (i64, f64) {
    let bits = ((months as u32 as u64) << 32) | (days as u32 as u64);
    (micros, f64::from_bits(bits))
}

/// Unpack the payload written by [`pack_interval`] (or by an extension built
/// against the header's description of it) back into `(months, days, micros)`.
pub const fn unpack_interval(int64: i64, double: f64) -> (i32, i32, i64) {
    let bits = double.to_bits();
    ((bits >> 32) as u32 as i32, bits as u32 as i32, int64)
}

/// One tagged scalar, by value. Same layout family as `pinkdb-ffi`'s
/// `PinkValue`; for VARCHAR, `text_ptr`/`text_len` carry raw bytes (embedded
/// NULs allowed, no trailing NUL required).
#[cfg(not(target_arch = "wasm32"))]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PinkdbExtValue {
    pub type_tag: i32,
    pub is_null: i32,
    pub int64: i64,
    pub double: f64,
    pub text_ptr: *const c_char,
    pub text_len: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl PinkdbExtValue {
    /// A NULL value of declared tag `type_tag`.
    pub(crate) fn null(type_tag: i32) -> Self {
        Self {
            type_tag,
            is_null: 1,
            int64: 0,
            double: 0.0,
            text_ptr: std::ptr::null(),
            text_len: 0,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl fmt::Debug for PinkdbExtValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinkdbExtValue")
            .field("type_tag", &self.type_tag)
            .field("is_null", &self.is_null)
            .field("int64", &self.int64)
            .field("double", &self.double)
            .field("text_len", &self.text_len)
            .finish_non_exhaustive()
    }
}

// Compile-time layout pins for `pinkdb_ext_value` (mirrored in the C header):
// reordering a field or changing its type would silently reinterpret every
// value crossing the ABI. The pointer-width-dependent fields are pinned in the
// test below.
#[cfg(not(target_arch = "wasm32"))]
const _: () = {
    assert!(std::mem::offset_of!(PinkdbExtValue, type_tag) == 0);
    assert!(std::mem::offset_of!(PinkdbExtValue, is_null) == 4);
    assert!(std::mem::offset_of!(PinkdbExtValue, int64) == 8);
    assert!(std::mem::offset_of!(PinkdbExtValue, double) == 16);
};
#[cfg(all(not(target_arch = "wasm32"), target_pointer_width = "64"))]
const _: () = {
    assert!(std::mem::offset_of!(PinkdbExtValue, text_ptr) == 24);
    assert!(std::mem::offset_of!(PinkdbExtValue, text_len) == 32);
    assert!(std::mem::size_of::<PinkdbExtValue>() == 40);
};

/// An extension's scalar callback. Exported code is called through this
/// pointer; the callback must be thread-safe (see the header).
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtScalarFn = unsafe extern "C" fn(
    user_data: *mut c_void,
    args: *const PinkdbExtValue,
    n_args: usize,
    out: *mut PinkdbExtValue,
) -> i32;

/// `pinkdb_extension_abi_version() -> u32`.
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtAbiVersionFn = unsafe extern "C" fn() -> u32;
/// `pinkdb_extension_name() -> *const c_char`.
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtNameFn = unsafe extern "C" fn() -> *const c_char;
/// `pinkdb_extension_init(api, host_ctx) -> i32`.
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtInitFn =
    unsafe extern "C" fn(api: *const PinkdbHostApi, host_ctx: *mut c_void) -> i32;

/// The host's statement callback ([`PinkdbHostApi::execute_statement`]).
///
/// `sql` is `sql_len` UTF-8 bytes (no trailing NUL required). The host runs
/// them under its own [`crate::StatementHost`] policy and returns text it
/// allocated with [`PinkdbHostApi::alloc`]:
///
/// - return `0`: `*out_text`/`*out_len` hold the formatted result
///   (`pinkdb::agent` Markdown, truncation notice included);
/// - return `1`: the statement failed, and the text is the one-line
///   `pinkdb::agent` JSON error;
/// - any other code: no text (the pointers are NULL/0); the host could not
///   run a statement (no live session, closed store).
///
/// The extension owns the returned buffer and must release it with
/// [`PinkdbHostApi::free`].
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtExecuteStatementFn = extern "C" fn(
    ctx: *mut c_void,
    sql: *const c_char,
    sql_len: usize,
    out_text: *mut *mut c_char,
    out_len: *mut usize,
) -> i32;

/// `pinkdb_ext_serve(api, host_ctx, transport, options) -> i32` — the optional
/// serve entry point (ABI minor 2).
///
/// The host hands control to the extension (an MCP server, typically) and
/// blocks until the client disconnects or the host asks for shutdown; a
/// non-zero return is a server failure. `transport` is `stdio` or
/// `http:HOST:PORT`, `options` a host-defined string. During the call
/// [`PinkdbHostApi::execute_statement`] is live with the `host_ctx` the host
/// passed; in the API handed to init the field is a stub that answers
/// "no session" (code 2) and never dereferences `host_ctx`.
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtServeFn = unsafe extern "C" fn(
    api: *const PinkdbHostApi,
    host_ctx: *mut c_void,
    transport: *const c_char,
    options: *const c_char,
) -> i32;

/// `pinkdb_ext_command(api, host_ctx, args, sink, sink_ctx) -> i32` — the
/// optional command entry point (ABI minor 4).
///
/// The host calls it for one dot command (`.agent ask "…"`), with `args` the
/// text after the command name. The extension writes streamed output through
/// `sink` and returns 0 when it handled the command, non-zero on failure
/// (having already said why through the sink). During the call
/// [`PinkdbHostApi::execute_statement`] runs under the tool policy and
/// [`PinkdbHostApi::execute_internal`] under the internal one, both bound to
/// `host_ctx`; elsewhere both are stubs answering "no session" (code 2).
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtCommandFn = unsafe extern "C" fn(
    api: *const PinkdbHostApi,
    host_ctx: *mut c_void,
    args: *const c_char,
    sink: Option<PinkdbExtSinkFn>,
    sink_ctx: *mut c_void,
) -> i32;

/// One streamed chunk of a command's output: channel 0 is answer/progress
/// text (the host's stdout), 1 is a diagnostic (its stderr). `text` is exactly
/// `len` bytes, not NUL-terminated.
#[cfg(not(target_arch = "wasm32"))]
pub type PinkdbExtSinkFn =
    extern "C" fn(ctx: *mut c_void, channel: i32, text: *const c_char, len: usize);

/// Host-provided services handed to `pinkdb_extension_init`. Mirrors
/// `PinkdbHostApi` in the header field for field. A plain POD: extensions
/// copy it out of init (the header documents it as init-scoped), and the
/// fields are all function pointers and integers.
#[cfg(not(target_arch = "wasm32"))]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PinkdbHostApi {
    /// [`PINKDB_EXT_ABI_VERSION`].
    pub abi_version: u32,
    /// `size_of::<PinkdbHostApi>()` at build time.
    pub struct_size: u32,
    /// Register one scalar function; 0 = registered, non-zero = rejected.
    ///
    /// `func` is deliberately [`Option`]: the C signature takes a function
    /// pointer, which a buggy or hostile extension may pass as NULL. The
    /// option is ABI-identical (a nullable function pointer) and lets the
    /// host reject NULL with a registration error instead of storing a null
    /// pointer and jumping to address 0 on the first call.
    pub register_scalar: extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        n_args: u32,
        arg_types: *const i32,
        ret_type: i32,
        func: Option<PinkdbExtScalarFn>,
        user_data: *mut c_void,
    ) -> i32,
    /// Allocate result text (libc malloc); pairs with `free`.
    pub alloc: extern "C" fn(size: usize) -> *mut c_void,
    /// Free result text allocated by `alloc`.
    pub free: extern "C" fn(ptr: *mut c_void),
    /// [`Self::register_scalar`] plus a `PINKDB_EXT_FLAG_*` set
    /// ([`PINKDB_EXT_FLAG_VOLATILE`]). Appended after ABI v1 shipped, so an
    /// extension must compare `struct_size` before calling it (see the
    /// header); the field itself is ABI-identical to a nullable function
    /// pointer.
    pub register_scalar_flags: extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        n_args: u32,
        arg_types: *const i32,
        ret_type: i32,
        flags: u32,
        func: Option<PinkdbExtScalarFn>,
        user_data: *mut c_void,
    ) -> i32,
    /// Run one statement in the host (ABI minor 2), returning text out.
    ///
    /// Set only in the API handed to `pinkdb_ext_serve`; the init API carries
    /// a function that reports "no live session" and never dereferences
    /// `ctx`, so an extension cannot reach a statement outside a serve call.
    /// Appended after v1 shipped: an extension must compare `struct_size`
    /// before calling it.
    pub execute_statement: PinkdbExtExecuteStatementFn,
    /// Run one statement under the *internal* policy (ABI minor 4): writes are
    /// allowed only inside the database the host names for its extension, and
    /// the text is JSON (`[{"columns":[…],"rows":[[…]]}, …]`) instead of
    /// Markdown, because this channel carries an extension's own bookkeeping.
    ///
    /// Set only in the API handed to `pinkdb_ext_command`; init and serve
    /// carry a stub answering "no live session". Appended after `execute
    /// _statement`, so an extension must compare `struct_size` before using it.
    pub execute_internal: PinkdbExtExecuteStatementFn,
}

/// The ABI's type tags as a Rust enum (the host never uses the NULL tag for
/// declared argument / return types).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtType {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
    Date,
    Timestamp,
    /// Calendar interval (`months`, `days`, `micros`), carried by value as
    /// [`pack_interval`] describes.
    Interval,
    /// Semi-structured value, carried as pinkdb-encoded VARIANT bytes over
    /// the VARCHAR channel ([`PINKDB_EXT_TYPE_VARIANT`]).
    Variant,
    /// A fixed-size array of `len` doubles, carried as a pointer-and-length
    /// view over the value channel: `int64` is the element count, `text_ptr`
    /// points at `len` contiguous `double`s and `text_len` is `len * 8`.
    /// The engine passes no per-element boxing and no NULL elements — an
    /// ARRAY argument holding a NULL element is refused with an error
    /// naming the function.
    ArrayDouble {
        len: u32,
    },
    /// A fixed-size array of `len` floats (4-byte elements), carried the same
    /// way as [`ExtType::ArrayDouble`] with `text_len == len * 4`.
    ArrayFloat {
        len: u32,
    },
}

impl ExtType {
    /// The `PINKDB_EXT_TYPE_*` tag.
    pub const fn tag(self) -> i32 {
        match self {
            ExtType::Boolean => PINKDB_EXT_TYPE_BOOLEAN,
            ExtType::Integer => PINKDB_EXT_TYPE_INTEGER,
            ExtType::Bigint => PINKDB_EXT_TYPE_BIGINT,
            ExtType::Double => PINKDB_EXT_TYPE_DOUBLE,
            ExtType::Varchar => PINKDB_EXT_TYPE_VARCHAR,
            ExtType::Date => PINKDB_EXT_TYPE_DATE,
            ExtType::Timestamp => PINKDB_EXT_TYPE_TIMESTAMP,
            ExtType::Interval => PINKDB_EXT_TYPE_INTERVAL,
            ExtType::Variant => PINKDB_EXT_TYPE_VARIANT,
            ExtType::ArrayDouble { len } => pack_array_len(len),
            ExtType::ArrayFloat { len } => pack_array_float_len(len),
        }
    }

    /// Decode a tag; `None` for [`PINKDB_EXT_TYPE_NULL`] and unknown tags.
    pub const fn from_tag(tag: i32) -> Option<ExtType> {
        match tag {
            PINKDB_EXT_TYPE_BOOLEAN => Some(ExtType::Boolean),
            PINKDB_EXT_TYPE_INTEGER => Some(ExtType::Integer),
            PINKDB_EXT_TYPE_BIGINT => Some(ExtType::Bigint),
            PINKDB_EXT_TYPE_DOUBLE => Some(ExtType::Double),
            PINKDB_EXT_TYPE_VARCHAR => Some(ExtType::Varchar),
            PINKDB_EXT_TYPE_DATE => Some(ExtType::Date),
            PINKDB_EXT_TYPE_TIMESTAMP => Some(ExtType::Timestamp),
            PINKDB_EXT_TYPE_INTERVAL => Some(ExtType::Interval),
            PINKDB_EXT_TYPE_VARIANT => Some(ExtType::Variant),
            tag if tag & 0xff == PINKDB_EXT_TYPE_ARRAY_DOUBLE => Some(ExtType::ArrayDouble {
                len: unpack_array_len(tag),
            }),
            tag if tag & 0xff == PINKDB_EXT_TYPE_ARRAY_FLOAT => Some(ExtType::ArrayFloat {
                len: unpack_array_len(tag),
            }),
            _ => None,
        }
    }

    /// DuckDB-style display name (`DOUBLE[3]` carries its length).
    pub fn name(self) -> String {
        match self {
            ExtType::Boolean => "BOOLEAN".to_string(),
            ExtType::Integer => "INTEGER".to_string(),
            ExtType::Bigint => "BIGINT".to_string(),
            ExtType::Double => "DOUBLE".to_string(),
            ExtType::Varchar => "VARCHAR".to_string(),
            ExtType::Date => "DATE".to_string(),
            ExtType::Timestamp => "TIMESTAMP".to_string(),
            ExtType::Interval => "INTERVAL".to_string(),
            ExtType::Variant => "VARIANT".to_string(),
            ExtType::ArrayDouble { len } => format!("DOUBLE[{len}]"),
            ExtType::ArrayFloat { len } => format!("FLOAT[{len}]"),
        }
    }
}

impl fmt::Display for ExtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

/// One argument value for [`crate::DynamicFunction::call`]. VARCHAR bytes are
/// borrowed; NULL carries the argument's declared type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExtArg<'a> {
    Null(ExtType),
    Boolean(bool),
    Integer(i32),
    Bigint(i64),
    Double(f64),
    Varchar(&'a [u8]),
    Date(i32),
    Timestamp(i64),
    /// A calendar interval, by value.
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    /// pinkdb-encoded VARIANT bytes, borrowed for the duration of the call.
    Variant(&'a [u8]),
    /// `len` contiguous doubles, borrowed for the duration of the call. The
    /// engine's ARRAY vector already stores elements contiguously, so this is
    /// a slice of it — never a copy.
    ArrayDouble(&'a [f64]),
    /// `len` contiguous floats, borrowed like [`ExtArg::ArrayDouble`].
    ArrayFloat(&'a [f32]),
}

impl ExtArg<'_> {
    /// The declared ABI type of this argument.
    pub const fn ext_type(&self) -> ExtType {
        match self {
            ExtArg::Null(t) => *t,
            ExtArg::Boolean(_) => ExtType::Boolean,
            ExtArg::Integer(_) => ExtType::Integer,
            ExtArg::Bigint(_) => ExtType::Bigint,
            ExtArg::Double(_) => ExtType::Double,
            ExtArg::Varchar(_) => ExtType::Varchar,
            ExtArg::Date(_) => ExtType::Date,
            ExtArg::Timestamp(_) => ExtType::Timestamp,
            ExtArg::Interval { .. } => ExtType::Interval,
            ExtArg::Variant(_) => ExtType::Variant,
            ExtArg::ArrayDouble(values) => ExtType::ArrayDouble {
                len: values.len() as u32,
            },
            ExtArg::ArrayFloat(values) => ExtType::ArrayFloat {
                len: values.len() as u32,
            },
        }
    }

    /// Marshal into the by-value C struct (VARCHAR borrows the bytes).
    ///
    /// Native-only: the struct crosses the `dlopen` ABI; the linked model
    /// (`crate::linked`) passes [`ExtArg`] itself to the callback.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn to_abi(self) -> PinkdbExtValue {
        let (type_tag, is_null, int64, double, text_ptr, text_len) = match self {
            // A NULL argument keeps is_null set, carrying its declared type.
            ExtArg::Null(t) => (t.tag(), 1, 0, 0.0, std::ptr::null(), 0),
            ExtArg::Boolean(b) => (
                PINKDB_EXT_TYPE_BOOLEAN,
                0,
                b as i64,
                0.0,
                std::ptr::null(),
                0,
            ),
            ExtArg::Integer(i) => (
                PINKDB_EXT_TYPE_INTEGER,
                0,
                i as i64,
                0.0,
                std::ptr::null(),
                0,
            ),
            ExtArg::Bigint(i) => (PINKDB_EXT_TYPE_BIGINT, 0, i, 0.0, std::ptr::null(), 0),
            ExtArg::Double(x) => (PINKDB_EXT_TYPE_DOUBLE, 0, 0, x, std::ptr::null(), 0),
            ExtArg::Varchar(bytes) => (
                PINKDB_EXT_TYPE_VARCHAR,
                0,
                0,
                0.0,
                bytes.as_ptr() as *const c_char,
                bytes.len(),
            ),
            ExtArg::Variant(bytes) => (
                PINKDB_EXT_TYPE_VARIANT,
                0,
                0,
                0.0,
                bytes.as_ptr() as *const c_char,
                bytes.len(),
            ),
            ExtArg::Date(d) => (PINKDB_EXT_TYPE_DATE, 0, d as i64, 0.0, std::ptr::null(), 0),
            ExtArg::Timestamp(us) => (PINKDB_EXT_TYPE_TIMESTAMP, 0, us, 0.0, std::ptr::null(), 0),
            ExtArg::Interval {
                months,
                days,
                micros,
            } => {
                let (int64, double) = pack_interval(months, days, micros);
                (
                    PINKDB_EXT_TYPE_INTERVAL,
                    0,
                    int64,
                    double,
                    std::ptr::null(),
                    0,
                )
            }
            // The element count rides `int64` as well as the tag, so a
            // callback can read it without unpacking the type tag.
            ExtArg::ArrayDouble(values) => (
                pack_array_len(values.len() as u32),
                0,
                values.len() as i64,
                0.0,
                values.as_ptr() as *const c_char,
                std::mem::size_of_val(values),
            ),
            ExtArg::ArrayFloat(values) => (
                pack_array_float_len(values.len() as u32),
                0,
                values.len() as i64,
                0.0,
                values.as_ptr() as *const c_char,
                std::mem::size_of_val(values),
            ),
        };
        PinkdbExtValue {
            type_tag,
            is_null,
            int64,
            double,
            text_ptr,
            text_len,
        }
    }
}

/// One result value returned by a callback. VARCHAR bytes are copied out of
/// extension memory (and that memory is freed) before `call` returns.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtValue {
    Null,
    Boolean(bool),
    Integer(i32),
    Bigint(i64),
    Double(f64),
    Varchar(Vec<u8>),
    Date(i32),
    Timestamp(i64),
    /// A calendar interval, by value.
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    /// pinkdb-encoded VARIANT bytes, copied out of extension memory.
    Variant(Vec<u8>),
    /// Array elements copied out of extension memory; the length is the
    /// type's, and `decode_result` checks the two agree.
    ArrayDouble(Vec<f64>),
    /// Array elements copied out of extension memory (4-byte elements).
    ArrayFloat(Vec<f32>),
}

impl ExtValue {
    /// The ABI type of a non-NULL value.
    pub const fn ext_type(&self) -> Option<ExtType> {
        Some(match self {
            ExtValue::Null => return None,
            ExtValue::Boolean(_) => ExtType::Boolean,
            ExtValue::Integer(_) => ExtType::Integer,
            ExtValue::Bigint(_) => ExtType::Bigint,
            ExtValue::Double(_) => ExtType::Double,
            ExtValue::Varchar(_) => ExtType::Varchar,
            ExtValue::Date(_) => ExtType::Date,
            ExtValue::Timestamp(_) => ExtType::Timestamp,
            ExtValue::Interval { .. } => ExtType::Interval,
            ExtValue::Variant(_) => ExtType::Variant,
            ExtValue::ArrayDouble(values) => ExtType::ArrayDouble {
                len: values.len() as u32,
            },
            ExtValue::ArrayFloat(values) => ExtType::ArrayFloat {
                len: values.len() as u32,
            },
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn type_tags_round_trip_and_are_distinct() {
        let all = [
            ExtType::Boolean,
            ExtType::Integer,
            ExtType::Bigint,
            ExtType::Double,
            ExtType::Varchar,
            ExtType::Date,
            ExtType::Timestamp,
            ExtType::Interval,
            ExtType::Variant,
        ];
        let mut tags: Vec<i32> = all.iter().map(|t| t.tag()).collect();
        tags.sort_unstable();
        tags.dedup();
        assert_eq!(tags.len(), all.len(), "tags must be distinct");
        for t in all {
            assert_eq!(ExtType::from_tag(t.tag()), Some(t));
        }
        assert_eq!(ExtType::from_tag(PINKDB_EXT_TYPE_NULL), None);
        assert_eq!(ExtType::from_tag(99), None);
    }

    #[test]
    fn ext_args_marshal_with_null_and_text() {
        let null = ExtArg::Null(ExtType::Integer).to_abi();
        assert_eq!(null.type_tag, PINKDB_EXT_TYPE_INTEGER);
        assert_eq!(null.is_null, 1);
        assert!(null.text_ptr.is_null());

        let s = ExtArg::Varchar(b"hey");
        let abi = s.to_abi();
        assert_eq!(abi.type_tag, PINKDB_EXT_TYPE_VARCHAR);
        assert_eq!(abi.is_null, 0);
        assert_eq!(abi.text_len, 3);
        assert!(!abi.text_ptr.is_null());

        let d = ExtArg::Double(2.5).to_abi();
        assert_eq!(d.type_tag, PINKDB_EXT_TYPE_DOUBLE);
        assert_eq!(d.double, 2.5);
        assert_eq!(d.int64, 0);

        // A VARIANT borrows its bytes on the text channel, length included.
        let v = ExtArg::Variant(&[0x01, 0x02, 0x00]);
        let abi = v.to_abi();
        assert_eq!(abi.type_tag, PINKDB_EXT_TYPE_VARIANT);
        assert_eq!(abi.is_null, 0);
        assert_eq!(abi.text_len, 3);
        assert!(!abi.text_ptr.is_null());
    }

    /// The interval payload survives the by-value round trip in every field,
    /// including negative months/days/micros.
    #[test]
    fn interval_payload_round_trips() {
        for (months, days, micros) in [
            (0, 0, 0),
            (13, 17, 0),
            (-1, -1, -432_000_000),
            (i32::MAX, i32::MIN, i64::MIN),
            (i32::MIN, i32::MAX, i64::MAX),
        ] {
            let (int64, double) = pack_interval(months, days, micros);
            assert_eq!(unpack_interval(int64, double), (months, days, micros));

            let abi = ExtArg::Interval {
                months,
                days,
                micros,
            }
            .to_abi();
            assert_eq!(abi.type_tag, PINKDB_EXT_TYPE_INTERVAL);
            assert_eq!(abi.is_null, 0);
            assert_eq!(
                unpack_interval(abi.int64, abi.double),
                (months, days, micros)
            );
        }
        // A NULL interval carries the tag, not a payload.
        assert_eq!(
            ExtArg::Null(ExtType::Interval).to_abi().type_tag,
            PINKDB_EXT_TYPE_INTERVAL
        );
    }

    #[test]
    fn host_api_layout_starts_with_version_and_size() {
        // The C header documents abi_version + struct_size as the first two
        // fields; a layout change would silently break extensions.
        assert_eq!(std::mem::offset_of!(PinkdbHostApi, abi_version), 0);
        assert_eq!(std::mem::offset_of!(PinkdbHostApi, struct_size), 4);
        assert_eq!(std::mem::offset_of!(PinkdbHostApi, register_scalar), 8);
    }

    /// The volatile flag is an *additive* ABI v1 addition: appending
    /// `register_scalar_flags` after `free` changes no earlier field's
    /// offset, so an extension built against the pre-flag header keeps
    /// working byte-for-byte (it just never reads the new field), while an
    /// extension that uses it checks `struct_size` first.
    #[test]
    fn register_scalar_flags_is_appended_after_the_v1_fields() {
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::offset_of!(PinkdbHostApi, alloc), 16);
            assert_eq!(std::mem::offset_of!(PinkdbHostApi, free), 24);
            assert_eq!(
                std::mem::offset_of!(PinkdbHostApi, register_scalar_flags),
                32
            );
            // ABI minor 2: the statement callback is appended after the v1
            // fields, never inserted between them.
            assert_eq!(std::mem::offset_of!(PinkdbHostApi, execute_statement), 40);
            // ABI minor 4: the internal-channel callback follows it, so the
            // minor-2 landmark below is exactly the old `size_of`.
            assert_eq!(std::mem::offset_of!(PinkdbHostApi, execute_internal), 48);
            assert_eq!(std::mem::size_of::<PinkdbHostApi>(), 56);
        }
        // The check an extension makes before calling the new field: the
        // host's struct_size covers it exactly when it is the last field.
        const FIELD_END: usize = std::mem::offset_of!(PinkdbHostApi, execute_internal)
            + std::mem::size_of::<PinkdbExtExecuteStatementFn>();
        assert_eq!(FIELD_END, std::mem::size_of::<PinkdbHostApi>());
        // The published size ladder the header's `PINKDB_EXT_API_*_SIZE` and
        // `PINKDB_EXT_HOST_HAS_*` macros spell out, pinned here so an SDK or
        // fixture that hard-codes one fails a test instead of a load.
        assert_eq!(
            PINKDB_EXT_API_V1_SIZE as usize,
            std::mem::offset_of!(PinkdbHostApi, register_scalar_flags)
        );
        assert_eq!(
            PINKDB_EXT_API_FLAGS_SIZE as usize,
            std::mem::offset_of!(PinkdbHostApi, execute_statement)
        );
        // The minor-2 landmark is where `execute_statement` ends, which is
        // also where `execute_internal` begins.
        assert_eq!(
            PINKDB_EXT_API_MINOR2_SIZE as usize,
            std::mem::offset_of!(PinkdbHostApi, execute_statement)
                + std::mem::size_of::<PinkdbExtExecuteStatementFn>()
        );
        assert_eq!(
            PINKDB_EXT_API_MINOR2_SIZE as usize,
            std::mem::offset_of!(PinkdbHostApi, execute_internal)
        );
        assert_eq!(
            PINKDB_EXT_API_MINOR4_SIZE as usize,
            std::mem::offset_of!(PinkdbHostApi, execute_internal)
                + std::mem::size_of::<PinkdbExtExecuteStatementFn>()
        );
        assert_eq!(
            PINKDB_EXT_API_MINOR4_SIZE as usize,
            std::mem::size_of::<PinkdbHostApi>()
        );
    }

    #[test]
    fn ext_value_layout_matches_the_c_header() {
        // The by-value struct is passed across the ABI; its layout is pinned
        // by the const assertions above plus these pointer-width-dependent
        // offsets (64-bit hosts).
        assert_eq!(std::mem::offset_of!(PinkdbExtValue, type_tag), 0);
        assert_eq!(std::mem::offset_of!(PinkdbExtValue, is_null), 4);
        assert_eq!(std::mem::offset_of!(PinkdbExtValue, int64), 8);
        assert_eq!(std::mem::offset_of!(PinkdbExtValue, double), 16);
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::offset_of!(PinkdbExtValue, text_ptr), 24);
            assert_eq!(std::mem::offset_of!(PinkdbExtValue, text_len), 32);
            assert_eq!(std::mem::size_of::<PinkdbExtValue>(), 40);
        }
    }
}
