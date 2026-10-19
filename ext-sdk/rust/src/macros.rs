//! The registration macro: one declaration generates the ABI v1 exports
//! (native) and the linked registration (every target), both driven by the
//! same [`ScalarSpec`](crate::ScalarSpec) list.
//!
//! The macro is the only thing an extension author has to write besides the
//! function bodies; see the crate docs for the accepted shapes and
//! `ext-sdk/rust/README.md` for the guided tour.
//!
//! # Arity
//!
//! A function is **fixed-arity** by default: it takes exactly the parameters
//! it declares. Two modifiers express the ranged arities the wave-52 loader
//! APIs support:
//!
//! - `optional(min)` — a bounded range: the declared parameters, but a call
//!   may stop after `min` of them. The optional positions must be written
//!   [`Optional<T>`](crate::Optional), whose three states are "not passed",
//!   "SQL NULL" and "a value" (a bare `Option<T>` conflates the first two and
//!   would make `log(10)` and `log(2, NULL)` indistinguishable).
//! - `variadic(min)` — a homogeneous tail: the declaration has exactly one
//!   parameter, `Vec<T>`, and a call passes any number of `T` arguments from
//!   `min` up to the ABI's limit (64). `T` is usually an `Option<_>` so NULL
//!   stays representable.
//!
//! Both shapes are decoded by the generated glue and registered through
//! [`LinkedFunction::new_optional`](pinkdb_ext_loader::LinkedFunction::new_optional)
//! / [`LinkedFunction::new_variadic`](pinkdb_ext_loader::LinkedFunction::new_variadic).
//! The native ABI v1 has no ranged-arity descriptor, so an extension that
//! declares either shape is **linked-only**: a native load refuses it with a
//! clear diagnostic instead of silently registering a fixed-arity function
//! (see the crate docs).

/// One function's generated typed item and its [`ScalarSpec`](crate::ScalarSpec)
/// expression, keyed by the declaration's arity shape.
///
/// The two `[...]` modifiers are the declaration's optional/variadic minimum
/// (empty when absent; the grammar rejects both at once); the parameter list
/// is forwarded as raw tokens so the variadic arm can destructure `Vec<T>`
/// into its element type. Not a stable API — use
/// [`pinkdb_extension!`](crate::pinkdb_extension).
#[doc(hidden)]
#[macro_export]
macro_rules! __pinkdb_spec {
    // ---- fixed arity: exactly the declared parameters ----
    ($volatile:literal, $fname:ident, [], [], ($($arg:ident : $ty:ty),* $(,)?), $ret:ty) => {
        $crate::ScalarSpec {
            name: ::core::stringify!($fname),
            args: &[ $( <$ty as $crate::ExtArgType>::EXT_TYPE ),* ],
            min_args: $crate::__private::arg_count(&[
                $( <$ty as $crate::ExtArgType>::EXT_TYPE ),*
            ]),
            variadic: false,
            ret: <$ret as $crate::IntoExtValue>::RET_TYPE,
            volatile: $volatile,
            call: |__args: &[$crate::ExtArg<'_>]| -> ::core::result::Result<
                $crate::ExtValue,
                ::std::string::String,
            > {
                #[allow(unused_mut, unused_variables)]
                let mut __i = 0usize;
                $(
                    let $arg = <$ty as $crate::FromExtArg>::from_ext_arg(&__args[__i])
                        .map_err(|e| ::std::format!("argument {}: {e}", __i + 1))?;
                    __i += 1;
                )*
                <$ret as $crate::IntoExtValue>::into_ext_value($fname($($arg),*))
            },
        }
    };

    // ---- optional arity: a call may stop after `min` arguments ----
    ($volatile:literal, $fname:ident, [$min:literal], [], ($($arg:ident : $ty:ty),* $(,)?), $ret:ty) => {
        $crate::ScalarSpec {
            name: ::core::stringify!($fname),
            args: &[ $( <$ty as $crate::ExtArgType>::EXT_TYPE ),* ],
            min_args: $min,
            variadic: false,
            ret: <$ret as $crate::IntoExtValue>::RET_TYPE,
            volatile: $volatile,
            call: |__args: &[$crate::ExtArg<'_>]| -> ::core::result::Result<
                $crate::ExtValue,
                ::std::string::String,
            > {
                #[allow(unused_mut, unused_variables)]
                let mut __i = 0usize;
                $(
                    let $arg = <$ty as $crate::FromExtArgAt>::from_ext_arg_at(__args, __i)
                        .map_err(|e| ::std::format!("argument {}: {e}", __i + 1))?;
                    __i += 1;
                )*
                <$ret as $crate::IntoExtValue>::into_ext_value($fname($($arg),*))
            },
        }
    };

    // ---- variadic arity: one `Vec<T>` tail, any number of arguments ----
    ($volatile:literal, $fname:ident, [], [$min:literal], ($values:ident : Vec<$elem:ty>), $ret:ty) => {
        $crate::ScalarSpec {
            name: ::core::stringify!($fname),
            args: &[ <$elem as $crate::ExtArgType>::EXT_TYPE ],
            min_args: $min,
            variadic: true,
            ret: <$ret as $crate::IntoExtValue>::RET_TYPE,
            volatile: $volatile,
            call: |__args: &[$crate::ExtArg<'_>]| -> ::core::result::Result<
                $crate::ExtValue,
                ::std::string::String,
            > {
                let $values = $crate::decode_arguments::<$elem>(__args)
                    .map_err(|e| ::std::format!("argument list: {e}"))?;
                <$ret as $crate::IntoExtValue>::into_ext_value($fname($values))
            },
        }
    };

    // ---- both modifiers at once: a declaration error, caught at compile time ----
    ($volatile:literal, $fname:ident, [$omin:literal], [$vmin:literal], ($($params:tt)*), $ret:ty) => {
        ::core::compile_error!(
            "a pinkdb extension function declares either `optional(min)` or `variadic(min)`, not both"
        )
    };
}

/// Declare one pinkdb extension: its registry name and its functions.
///
/// Every function is written as a normal Rust item with typed parameters and
/// a typed return value, and the macro generates:
///
/// - the three ABI v1 exports (`pinkdb_extension_abi_version`,
///   `pinkdb_extension_name`, `pinkdb_extension_init`) on native targets;
/// - `pinkdb_scalar_specs()` — the shared description both loading models
///   consume;
/// - `pinkdb_linked_extension()` / `pinkdb_register_linked()` — the wave-49
///   linked registration, which is also how a wasm build is published.
///
/// Accepted parameter/return types are the ABI v1 types (see the crate docs)
/// and `Option<T>` around them; NULL handling is explicit through
/// `Option<T>`.
///
/// `functions` lists the **deterministic** functions. A nondeterministic
/// function (one that reads a clock, a random source, ambient state, ...)
/// goes into the optional `volatile` list instead: the engine then refuses to
/// run it in any statement it WAL-logs. Deterministic is the default and
/// matches every pre-existing extension.
///
/// # Arity
///
/// By default a function takes exactly the parameters it declares. Two
/// leading modifiers express the ranged arities (see the module docs and the
/// crate docs' "Ranged arity" section):
///
/// ```no_run
/// # use pinkdb_ext_sdk::{Optional, pinkdb_extension};
/// pinkdb_extension! {
///     name = "my_extension",
///     functions = [
///         /// Fixed: exactly one argument.
///         fn my_halve(x: f64) -> f64 { x / 2.0 },
///
///         /// Optional: 1 or 2 arguments; the tail must be `Optional<T>`.
///         optional(1) fn my_log(b: Option<f64>, x: Optional<f64>) -> Option<f64> {
///             let b = b?;
///             Some(match x {
///                 Optional::Absent => b.log10(),
///                 Optional::Null => return None,
///                 Optional::Value(x) => x.ln() / b.ln(),
///             })
///         },
///
///         /// Variadic: one homogeneous `Vec<T>` tail, at least 1 argument.
///         variadic(1) fn my_sum(values: Vec<Option<i64>>) -> Option<i64> {
///             let mut total = 0i64;
///             for value in values.into_iter().flatten() {
///                 total += value;
///             }
///             Some(total)
///         },
///     ],
///     volatile = [
///         /// Nondeterministic: every call may differ.
///         fn my_ticks() -> i64 {
///             1
///         },
///     ],
/// }
/// ```
///
/// A ranged declaration (`optional`/`variadic`) is registered through the
/// linked model; the native ABI v1 cannot express an arity range, so a native
/// `LOAD EXTENSION` of an extension that declares one fails with a clear
/// diagnostic — publish it with `pinkdb_register_linked()` instead (wasm's
/// only model, and the native fallback).
///
/// The declaration may appear **once per crate** (it defines the extension's
/// exported symbols and a few plain functions); use one module per
/// extension, as the template does.
#[macro_export]
macro_rules! pinkdb_extension {
    (
        name = $name:literal,
        functions = [
            $(
                $(#[doc = $doc:literal])*
                $( optional ( $omin:literal ) )?
                $( variadic ( $vmin:literal ) )?
                fn $fname:ident($($params:tt)*) -> $ret:ty $body:block
                $(,)?
            )*
        ] $(,)?
        $(
            , volatile = [
                $(
                    $(#[doc = $vdoc:literal])*
                    $( optional ( $vomin:literal ) )?
                    $( variadic ( $vvmin:literal ) )?
                    fn $vname:ident($($vparams:tt)*) -> $vret:ty $vbody:block
                    $(,)?
                )*
            ] $(,)?
        )?
    ) => {
        $(
            $(#[doc = $doc])*
            fn $fname($($params)*) -> $ret $body
        )*
        $(
            $(
                $(#[doc = $vdoc])*
                fn $vname($($vparams)*) -> $vret $vbody
            )*
        )?

        /// The declared functions, as the shared list both loading models
        /// consume (generated by [`pinkdb_extension!`](crate::pinkdb_extension)).
        /// Deterministic functions first, then the volatile ones, each group
        /// in declaration order. The slice is `'static`: the callbacks'
        /// `user_data` points into it, so it must outlive every registered
        /// function.
        pub fn pinkdb_scalar_specs() -> &'static [$crate::ScalarSpec] {
            static SPECS: &[$crate::ScalarSpec] = &[
                $(
                    $crate::__pinkdb_spec!(
                        false,
                        $fname,
                        [ $($omin)? ],
                        [ $($vmin)? ],
                        ($($params)*),
                        $ret
                    ),
                )*
                $(
                    $(
                        $crate::__pinkdb_spec!(
                            true,
                            $vname,
                            [ $($vomin)? ],
                            [ $($vvmin)? ],
                            ($($vparams)*),
                            $vret
                        ),
                    )*
                )?
            ];
            SPECS
        }

        /// The extension's functions as a linked (wave-49) registration: the
        /// same declaration, usable on every target including wasm, where
        /// this is the *only* loading model. Publish it at host bootstrap
        /// with [`pinkdb_register_linked`](pinkdb_register_linked) (or
        /// `pinkdb_ext_loader::linked::register`).
        pub fn pinkdb_linked_extension() -> ::core::result::Result<
            $crate::LinkedExtension,
            $crate::ExtError,
        > {
            let mut functions = ::std::vec::Vec::new();
            for spec in pinkdb_scalar_specs() {
                functions.push(spec.linked_function()?);
            }
            $crate::LinkedExtension::new($name, functions)
        }

        /// Publish this extension in the process-global linked registry.
        /// Registration is one-shot per process (a host bootstrap step).
        pub fn pinkdb_register_linked() -> ::core::result::Result<(), $crate::ExtError> {
            $crate::register_linked_extension(pinkdb_linked_extension()?)
        }

        /// `pinkdb_extension_abi_version` (ABI v1): the loader compares this
        /// against its own version and refuses the library on a mismatch.
        #[cfg(not(target_arch = "wasm32"))]
        #[unsafe(no_mangle)]
        pub extern "C" fn pinkdb_extension_abi_version() -> u32 {
            $crate::__private::ABI_VERSION
        }

        /// `pinkdb_extension_name` (ABI v1): NUL-terminated UTF-8, copied by
        /// the host immediately.
        #[cfg(not(target_arch = "wasm32"))]
        #[unsafe(no_mangle)]
        pub extern "C" fn pinkdb_extension_name() -> *const ::core::ffi::c_char {
            // The literal is NUL-terminated here, so the pointer is a C string.
            ::core::concat!($name, "\0").as_ptr().cast::<::core::ffi::c_char>()
        }

        /// `pinkdb_extension_init` (ABI v1): register every declared
        /// function, volatile ones included. Called once by the loader; never
        /// unwinds (the SDK catches panics at the boundary).
        #[cfg(not(target_arch = "wasm32"))]
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn pinkdb_extension_init(
            api: *const $crate::__private::HostApi,
            host_ctx: *mut ::core::ffi::c_void,
        ) -> i32 {
            // SAFETY: the loader passes a valid, initialized host API and the
            // opaque context to hand back; `native_init` documents both.
            unsafe { $crate::__private::native_init(api, host_ctx, $name, pinkdb_scalar_specs()) }
        }
    };
}
