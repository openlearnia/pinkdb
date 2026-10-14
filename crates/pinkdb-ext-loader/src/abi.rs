//! C ABI v1 types, mirrored 1:1 from `include/pinkdb_extension.h`.
//!
//! Nothing here is unsafe: the structs are plain `repr(C)` data and the enums
//! are host-side conveniences. All pointer handling that needs `unsafe` lives
//! in [`crate::function`] and [`crate::host`].

use std::ffi::{c_char, c_void};
use std::fmt;

/// ABI version implemented by this loader and required from extensions.
pub const PINKDB_EXT_ABI_VERSION: u32 = 1;

/// Maximum argument count accepted by [`crate::PinkdbHostApi::register_scalar`].
pub const PINKDB_EXT_MAX_ARGS: u32 = 64;

// Value / argument type tags (same numbering as the pinkdb C ABI).
pub const PINKDB_EXT_TYPE_NULL: i32 = 0;
pub const PINKDB_EXT_TYPE_BOOLEAN: i32 = 1;
pub const PINKDB_EXT_TYPE_INTEGER: i32 = 2;
pub const PINKDB_EXT_TYPE_BIGINT: i32 = 3;
pub const PINKDB_EXT_TYPE_DOUBLE: i32 = 4;
pub const PINKDB_EXT_TYPE_VARCHAR: i32 = 5;
pub const PINKDB_EXT_TYPE_DATE: i32 = 6;
pub const PINKDB_EXT_TYPE_TIMESTAMP: i32 = 7;

/// One tagged scalar, by value. Same layout family as `pinkdb-ffi`'s
/// `PinkValue`; for VARCHAR, `text_ptr`/`text_len` carry raw bytes (embedded
/// NULs allowed, no trailing NUL required).
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
const _: () = {
    assert!(std::mem::offset_of!(PinkdbExtValue, type_tag) == 0);
    assert!(std::mem::offset_of!(PinkdbExtValue, is_null) == 4);
    assert!(std::mem::offset_of!(PinkdbExtValue, int64) == 8);
    assert!(std::mem::offset_of!(PinkdbExtValue, double) == 16);
};
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(std::mem::offset_of!(PinkdbExtValue, text_ptr) == 24);
    assert!(std::mem::offset_of!(PinkdbExtValue, text_len) == 32);
    assert!(std::mem::size_of::<PinkdbExtValue>() == 40);
};

/// An extension's scalar callback. Exported code is called through this
/// pointer; the callback must be thread-safe (see the header).
pub type PinkdbExtScalarFn = unsafe extern "C" fn(
    user_data: *mut c_void,
    args: *const PinkdbExtValue,
    n_args: usize,
    out: *mut PinkdbExtValue,
) -> i32;

/// `pinkdb_extension_abi_version() -> u32`.
pub type PinkdbExtAbiVersionFn = unsafe extern "C" fn() -> u32;
/// `pinkdb_extension_name() -> *const c_char`.
pub type PinkdbExtNameFn = unsafe extern "C" fn() -> *const c_char;
/// `pinkdb_extension_init(api, host_ctx) -> i32`.
pub type PinkdbExtInitFn =
    unsafe extern "C" fn(api: *const PinkdbHostApi, host_ctx: *mut c_void) -> i32;

/// Host-provided services handed to `pinkdb_extension_init`. Mirrors
/// `PinkdbHostApi` in the header field for field.
#[repr(C)]
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
            _ => None,
        }
    }

    /// DuckDB-style display name.
    pub const fn name(self) -> &'static str {
        match self {
            ExtType::Boolean => "BOOLEAN",
            ExtType::Integer => "INTEGER",
            ExtType::Bigint => "BIGINT",
            ExtType::Double => "DOUBLE",
            ExtType::Varchar => "VARCHAR",
            ExtType::Date => "DATE",
            ExtType::Timestamp => "TIMESTAMP",
        }
    }
}

impl fmt::Display for ExtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
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
        }
    }

    /// Marshal into the by-value C struct (VARCHAR borrows the bytes).
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
            ExtArg::Date(d) => (PINKDB_EXT_TYPE_DATE, 0, d as i64, 0.0, std::ptr::null(), 0),
            ExtArg::Timestamp(us) => (PINKDB_EXT_TYPE_TIMESTAMP, 0, us, 0.0, std::ptr::null(), 0),
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
        })
    }
}

#[cfg(test)]
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
    }

    #[test]
    fn host_api_layout_starts_with_version_and_size() {
        // The C header documents abi_version + struct_size as the first two
        // fields; a layout change would silently break extensions.
        assert_eq!(std::mem::offset_of!(PinkdbHostApi, abi_version), 0);
        assert_eq!(std::mem::offset_of!(PinkdbHostApi, struct_size), 4);
        assert_eq!(std::mem::offset_of!(PinkdbHostApi, register_scalar), 8);
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
