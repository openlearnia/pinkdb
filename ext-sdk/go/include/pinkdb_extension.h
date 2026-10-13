/*
 * pinkdb loadable-extension ABI v1 — https://github.com/openlearnia/pinkdb
 *
 * An extension is a shared library (`.dylib` / `.so` / `.dll`) loaded at
 * runtime by the `pinkdb-ext-loader` crate (host side: the SQL statement
 * `LOAD EXTENSION '<path>'`, or `PinkDb::load_extension` /
 * `pinkdb_ext_loader::load_extension`). This header is the complete
 * contract; it is plain C99, no pinkdb headers or libraries are needed to
 * build an extension.
 *
 * ---------------------------------------------------------------------------
 * Exported symbols (the loader looks each one up by name)
 * ---------------------------------------------------------------------------
 *   uint32_t    pinkdb_extension_abi_version(void);
 *       Must return PINKDB_EXT_ABI_VERSION (1). Any other value aborts the
 *       load with an error naming both versions.
 *
 *   const char *pinkdb_extension_name(void);
 *       NUL-terminated UTF-8, non-empty. The host copies it immediately, so
 *       a string literal (or any buffer that outlives the call) is fine.
 *
 *   int32_t     pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx);
 *       Called exactly once, on the loading thread, right after the version
 *       and name checks. Register functions through `api`; pass `host_ctx`
 *       back to `api->register_scalar` unchanged. Return 0 on success, any
 *       non-zero value aborts the load (the code is reported in the error).
 *       If any `register_scalar` call is rejected the load fails even when
 *       init returns 0 — never assume a rejected function exists.
 *       `api->abi_version` is PINKDB_EXT_ABI_VERSION; `api->struct_size` is
 *       the host's `sizeof(PinkdbHostApi)` (a larger value means a newer host
 *       with added fields — ignore what you do not know, or refuse).
 *       `pinkdb_extension_init` MUST NOT panic or unwind across the call:
 *       the host invokes it through an `extern "C"` boundary where unwinding
 *       is undefined behavior. Catch or avoid every error internally and
 *       return a non-zero code instead.
 *
 * ---------------------------------------------------------------------------
 * Value ABI: pinkdb_ext_value
 * ---------------------------------------------------------------------------
 * One tagged scalar (`PinkValue` layout family, same tag numbers):
 *
 *   type_tag  PINKDB_EXT_TYPE_* — the *declared* type of the value/argument;
 *   is_null   non-zero ⇒ the value is NULL (only type_tag is meaningful; for
 *             results the host ignores every other field, including text_ptr
 *             — but still frees a non-NULL text_ptr, see ownership below);
 *   int64     BOOLEAN (0/1), INTEGER, BIGINT, DATE (days since 1970-01-01),
 *             TIMESTAMP (microseconds since 1970-01-01 UTC);
 *   double_   DOUBLE (the Rust struct field is named `double`; `double_` is
 *             the C spelling — offsets and layout are identical);
 *   text_ptr  VARCHAR bytes; NULL when absent;
 *   text_len  byte length of text_ptr (embedded NUL bytes are allowed; a
 *             trailing NUL is neither required nor read).
 *
 * ---------------------------------------------------------------------------
 * Scalar callbacks
 * ---------------------------------------------------------------------------
 *   int32_t (*PinkdbExtScalarFn)(void *user_data,
 *                                const pinkdb_ext_value *args, size_t n_args,
 *                                pinkdb_ext_value *out);
 *
 * - `args` has exactly the registered arity, in declaration order, and is
 *   read-only. Read-only means "do not modify"; the array itself lives on the
 *   host's stack for the duration of the call.
 * - A registered argument type is fixed: no implicit casts are inserted by
 *   the binder, and a zero-argument function receives `args == NULL`.
 * - `out` points at host-owned, writable storage pre-filled with NULL. Set
 *   `out->type_tag` (and, for VARCHAR results, text_ptr/text_len), then
 *   return 0. If `is_null` stays non-zero the result is NULL.
 * - Return 0 for success. Any non-zero return code aborts the enclosing SQL
 *   statement with an error naming the function and the code; it must not be
 *   used for ordinary NULL results.
 * - NULL arguments are passed through: the host always calls the callback and
 *   sets `args[i].is_null`; the callback decides what NULL means (DuckDB
 *   calls this "special NULL handling" for every extension function).
 * - A VARCHAR argument's bytes are only valid for the duration of the call;
 *   copy them if you need to keep them.
 * - Callbacks MUST NOT panic or unwind across the call (undefined behavior
 *   through the host's `extern "C"` boundary) and MUST NOT call back into
 *   the engine. Return a non-zero code for errors.
 *
 * ---------------------------------------------------------------------------
 * Result text ownership (leak-free by construction)
 * ---------------------------------------------------------------------------
 * When a callback returns a non-NULL VARCHAR result it must allocate the
 * bytes with `api->alloc(size)` (never malloc/calloc/new directly — the host
 * frees with the matching `api->free`). The host copies exactly `text_len`
 * bytes out of `text_ptr` and then calls `api->free(text_ptr)` *immediately*
 * after the call returns. Rules:
 * - `text_ptr` must be NULL or a pointer returned by `api->alloc`;
 * - a non-NULL `text_ptr` is always freed by the host, even when
 *   `is_null` is set or the callback returns a non-zero code — never return
 *   a stack, static, or interior pointer there, and never keep it;
 * - the host never frees a NULL `text_ptr`;
 * - VARCHAR inputs (`args[i].text_ptr`) are host-owned: never free them.
 *
 * ---------------------------------------------------------------------------
 * THREAD SAFETY — read this
 * ---------------------------------------------------------------------------
 * Callbacks are invoked CONCURRENTLY from multiple host threads (the pinkdb
 * executor evaluates chunks in parallel on rayon worker threads). A callback
 * registered once may run on any host thread, including several at the same
 * time. Therefore:
 * - the callback and everything it touches (globals, statics, `user_data`)
 *   MUST be thread-safe: guard mutable state with atomics, locks, or
 *   thread-local storage;
 * - do not assume init and the callback run on the same thread;
 * - `api` may be called from any thread for the lifetime of the process
 *   (`alloc`/`free` are thread-safe); the `PinkdbHostApi` struct itself is
 *   only valid during `pinkdb_extension_init` — copy it (it is a small POD)
 *   if callbacks need it later;
 * - `register_scalar` may only be called from inside `pinkdb_extension_init`.
 * Extensions are never unloaded while the host process lives: the host keeps
 * the library mapped for every extension it accepted — including one that
 * registered no functions — for as long as the store holding the registration
 * exists. `dlclose` runs only at process exit.
 *
 * ---------------------------------------------------------------------------
 * Registration
 * ---------------------------------------------------------------------------
 *   int32_t register_scalar(void *ctx, const char *name, uint32_t n_args,
 *                           const int32_t *arg_types, int32_t ret_type,
 *                           PinkdbExtScalarFn fn, void *user_data);
 * - `ctx` is the `host_ctx` passed to init; pass it through unchanged.
 * - `name` is NUL-terminated UTF-8, non-empty and at most 256 bytes; matched
 *   case-insensitively. Names that collide with a built-in function or with
 *   an already-registered extension function are rejected.
 * - `fn` must NOT be NULL: the host rejects a NULL callback with a
 *   registration error (a stored NULL would be jumped to on the first call).
 * - `n_args` ≤ PINKDB_EXT_MAX_ARGS; `arg_types` may be NULL when n_args is 0
 *   and must otherwise point to `n_args` PINKDB_EXT_TYPE_* tags (only
 *   non-NULL value tags are accepted — no PINKDB_EXT_TYPE_NULL).
 * - `ret_type` is the PINKDB_EXT_TYPE_* the callback will produce. A
 *   non-NULL result whose `type_tag` differs is an error.
 * - `user_data` is opaque to the host: it is passed back to the callback
 *   unchanged on every call. Keep it thread-safe (see above).
 * - The host copies `name` and `arg_types` during the call.
 * - Returns 0 on success, non-zero when the function was rejected (nothing
 *   was registered; the load fails).
 */

#ifndef PINKDB_EXTENSION_H
#define PINKDB_EXTENSION_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ABI version of this header / the host loader. */
#define PINKDB_EXT_ABI_VERSION 1

/* Maximum number of function arguments the host accepts. */
#define PINKDB_EXT_MAX_ARGS 64

/* Value / argument type tags (same numbering as the pinkdb C ABI). */
enum {
    PINKDB_EXT_TYPE_NULL = 0,
    PINKDB_EXT_TYPE_BOOLEAN = 1,
    PINKDB_EXT_TYPE_INTEGER = 2,
    PINKDB_EXT_TYPE_BIGINT = 3,
    PINKDB_EXT_TYPE_DOUBLE = 4,
    PINKDB_EXT_TYPE_VARCHAR = 5,
    PINKDB_EXT_TYPE_DATE = 6,
    PINKDB_EXT_TYPE_TIMESTAMP = 7
};

/* One tagged scalar value; see the layout notes at the top of this header. */
typedef struct pinkdb_ext_value {
    int32_t type_tag;
    int32_t is_null;
    int64_t int64;
    double double_;
    const char *text_ptr;
    size_t text_len;
} pinkdb_ext_value;

struct PinkdbHostApi;

/* One scalar callback; ownership and threading rules are documented above. */
typedef int32_t (*PinkdbExtScalarFn)(void *user_data,
                                     const pinkdb_ext_value *args,
                                     size_t n_args,
                                     pinkdb_ext_value *out);

/* Host-provided services, valid during pinkdb_extension_init only. */
typedef struct PinkdbHostApi {
    /* PINKDB_EXT_ABI_VERSION and sizeof(PinkdbHostApi) as known to the host. */
    uint32_t abi_version;
    uint32_t struct_size;

    /* Register one scalar function; 0 = registered, non-zero = rejected. */
    int32_t (*register_scalar)(void *ctx,
                               const char *name,
                               uint32_t n_args,
                               const int32_t *arg_types,
                               int32_t ret_type,
                               PinkdbExtScalarFn fn,
                               void *user_data);

    /* Allocate result text. Pair with free(); both are thread-safe. */
    void *(*alloc)(size_t size);
    void (*free)(void *ptr);
} PinkdbHostApi;

#ifdef __cplusplus
}
#endif

#endif /* PINKDB_EXTENSION_H */
