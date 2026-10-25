/*
 * pinkdb loadable-extension ABI v1 (minor 4) — https://github.com/openlearnia/pinkdb
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
 *       load with an error naming both versions. The version is the major
 *       one: ABI 1 has grown additively (minor 2 added the optional
 *       `pinkdb_ext_serve` export and the `execute_statement` host field),
 *       so a v1 extension still loads unchanged, and an extension that does
 *       not export `pinkdb_ext_serve` is a functions-only extension.
 *
 *   const char *pinkdb_extension_name(void);
 *       NUL-terminated UTF-8, non-empty. The host copies it immediately, so
 *       a string literal (or any buffer that outlives the call) is fine.
 *
 *   int32_t     pinkdb_ext_serve(const PinkdbHostApi *api, void *host_ctx,
 *                                const char *transport, const char *options);
 *       OPTIONAL (ABI minor 2). Present when the extension can be handed a
 *       client. The host calls it in place of init's registrations-only
 *       lifetime and blocks until the extension returns; it returns 0 when
 *       the client disconnected or the host asked it to stop (SIGINT closes
 *       stdin, the HTTP loop sees its listener closed), non-zero on failure.
 *       `transport` is "stdio" or "http:HOST:PORT"; `options` is a
 *       host-defined string ("" when none). Both are NUL-terminated UTF-8,
 *       valid for the call only. The same rules as init apply: no unwinding
 *       across the call.
 *
 *       While a serve call runs the library is never unloaded and cannot be
 *       loaded a second time; the host enforces both.
 *
 *   int32_t     pinkdb_ext_command(const PinkdbHostApi *api, void *host_ctx,
 *                                  const char *args, PinkdbExtSinkFn sink,
 *                                  void *sink_ctx);
 *       OPTIONAL (ABI minor 4). Present when the extension implements host
 *       dot commands (the agent extension does). The host calls it once per
 *       command and blocks until it returns 0; `args` is the text after the
 *       command name, NUL-terminated UTF-8, valid for the call only. Text the
 *       command streams goes through `sink(sink_ctx, channel, text, len)`:
 *       channel 0 is the answer (the host's stdout), 1 a diagnostic (its
 *       stderr). The same no-unwinding and one-call-at-a-time rules as
 *       pinkdb_ext_serve apply, and the two share that one-call slot.
 *
 *   int32_t     pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx);
 *       Called exactly once, on the loading thread, right after the version
 *       and name checks. Register functions through `api`; pass `host_ctx`
 *       back to `api->register_scalar` / `api->register_scalar_flags`
 *       unchanged. Return 0 on success, any
 *       non-zero value aborts the load (the code is reported in the error).
 *       If any registration call is rejected the load fails even when
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
 *             and text_len, and never frees a pointer carried under a
 *             non-VARCHAR tag, see ownership below);
 *   int64     BOOLEAN (0/1), INTEGER, BIGINT, DATE (days since 1970-01-01),
 *             TIMESTAMP (microseconds since 1970-01-01 UTC), and INTERVAL
 *             (microseconds — see the INTERVAL note below);
 *   double_   DOUBLE (the Rust struct field is named `double`; `double_` is
 *             the C spelling — offsets and layout are identical); for
 *             INTERVAL it carries no numeric meaning and is read as a bit
 *             pattern (see below);
 *   text_ptr  VARCHAR bytes, or a VARIANT payload (see below); NULL when
 *             absent;
 *   text_len  byte length of text_ptr (embedded NUL bytes are allowed; a
 *             trailing NUL is neither required nor read). A VARCHAR or
 *             VARIANT result over 64 MiB is rejected without reading or
 *             freeing the buffer.
 *
 * INTERVAL (type tag 9, same number as the pinkdb C ABI's
 * PINKDB_TYPE_INTERVAL) is a calendar interval with DuckDB's three fields:
 * months, days and micros. The by-value struct cannot grow a field without
 * breaking ABI v1, so the payload rides in the two existing scalars:
 *
 *     int64      int64   = micros (can be any int64)
 *     double_    bits    = ((uint32)months << 32) | (uint32)days
 *
 * i.e. read/write `months` and `days` through the 64-bit pattern of
 * `double_` (C: memcpy the double to a uint64_t; C++: std::bit_cast; Rust:
 * `f64::to_bits` / `f64::from_bits`) — never through the float value itself.
 * The three fields round-trip exactly; a NULL interval ignores the payload.
 * Argument and result marshalling is identical. (Type tags 8 — the pinkdb C
 * ABI's DECIMAL — is deliberately not supported by extension ABI v1.)
 *
 * VARIANT (type tag 10, the pinkdb C ABI's PINKDB_TYPE_VARIANT number) is a
 * semi-structured value. The payload rides the text channel: `text_ptr` /
 * `text_len` hold exactly the bytes of pinkdb's VARIANT encoding — the same
 * opaque format the engine stores and re-reads (`pinkdb::variant::encode` /
 * `decode`). It is not JSON text: an extension that wants JSON must parse
 * the bytes itself (the C ABI's JSON rendering is a display surface, not this
 * one). The encoding is self-describing (nested objects and arrays included),
 * so an extension can hand any bytes back and the host decodes and validates
 * them; a payload that does not decode fails the statement with an error
 * naming the function. Reuse `api->alloc` for returned bytes exactly as for
 * VARCHAR results — the host copies `text_len` bytes and frees the buffer
 * immediately. Argument bytes are host-owned and valid only during the call.
 *
 * ARRAY (type tags 11 and 12) is a fixed-size array, passed as a pointer and
 * a length over contiguous elements — never a boxed array of values. Tag 11
 * carries 8-byte `double` elements and tag 12 (`PINKDB_EXT_TYPE_ARRAY_FLOAT`)
 * 4-byte `float` elements; both pack the length above the kind byte
 * (`PINKDB_EXT_ARRAY_TAG` / `PINKDB_EXT_ARRAY_FLOAT_TAG`, i.e. `(len << 8) |
 * tag`), and the element count also rides `int64`. For an argument,
 * `text_ptr` points at the engine's own element buffer (`len * 8` or
 * `len * 4` bytes, 8- or 4-byte aligned) and is host-owned, valid only during
 * the call. A NULL *array* arrives with `is_null` set and no payload. An ARRAY
 * argument that holds a NULL element is refused by the host: there is no
 * per-element NULL channel, and a substituted value would be a wrong answer.
 * A result allocates `len * 8` / `len * 4` bytes with `api->alloc`, 8-/4-byte
 * aligned, and sets `int64 = len`, `text_len = len * 8` / `len * 4` and the
 * packed tag; the host checks all three agree. Only FLOAT and DOUBLE arrays
 * have an ABI type (INTEGER/BIGINT arrays are refused by the binder).
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
 *   the binder, with one documented exception — a numeric argument
 *   (INTEGER, BIGINT, DECIMAL) widens into a `DOUBLE` parameter, which is
 *   DuckDB's implicit cast for the math functions (`exp(1)`, `sqrt(4)`);
 *   everything else needs an explicit `CAST`. A zero-argument function
 *   receives `args == NULL`.
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
 * - a non-NULL `text_ptr` on a **VARCHAR** result is always freed by the
 *   host, even when `is_null` is set or the callback returns a non-zero
 *   code — never return a stack, static, or interior pointer there, and
 *   never keep it;
 * - a `text_ptr` carried under any tag other than VARCHAR or VARIANT is
 *   ignored and **never** freed (it is not a result buffer), and a VARCHAR
 *   or VARIANT result whose
 *   `text_len` exceeds 64 MiB is rejected without reading or freeing the
 *   pointer;
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
 *   The host heap-allocates the context for the library's lifetime and sets
 *   an init-complete flag when init returns; a later call returns non-zero
 *   deterministically (it never writes into a dead frame).
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
 *   int32_t register_scalar_flags(void *ctx, const char *name, uint32_t n_args,
 *                                 const int32_t *arg_types, int32_t ret_type,
 *                                 uint32_t flags, PinkdbExtScalarFn fn,
 *                                 void *user_data);  (ABI v1 addition)
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
 * - `register_scalar` is exactly `register_scalar_flags` with `flags == 0`
 *   (deterministic); both share every other rule. `register_scalar_flags`
 *   lives at the END of `PinkdbHostApi`, so it is an *additive* ABI v1
 *   addition: extensions that never touch it behave exactly as before.
 *
 * ---------------------------------------------------------------------------
 * Deterministic vs volatile functions (ABI v1 addition)
 * ---------------------------------------------------------------------------
 * `flags` is a bit set; the only bit defined today is
 *
 *   PINKDB_EXT_FLAG_VOLATILE   the function is nondeterministic: it may read
 *                              the clock, a random source, ambient state, ...
 *
 * - **Default: deterministic** (`flags == 0`, i.e. every `register_scalar`
 *   call). A deterministic callback must be a pure function of its arguments
 *   (plus immutable state initialized in `pinkdb_extension_init`): the host
 *   may WAL-log a statement that calls it and re-execute that statement on
 *   recovery, and the replayed result must match the original. Returning
 *   different values for the same arguments on a *durable* store is a
 *   correctness bug in the extension.
 * - **PINKDB_EXT_FLAG_VOLATILE** declares the exception. The engine refuses
 *   to *execute* a volatile call in any statement it WAL-logs (INSERT,
 *   UPDATE, DELETE, DDL, CALL, ...): the statement fails with
 *   `extension function <name>() is volatile (nondeterministic) and cannot be
 *   called in a statement the engine WAL-logs: ...` before it runs, because
 *   replaying it later could produce a different value than this session saw.
 *   Read-only statements (a plain SELECT) never reach the WAL and may call
 *   volatile functions. Rationale: replay re-executes the logged SQL text;
 *   refusing at the only point where divergence could enter the WAL makes
 *   divergence impossible instead of merely unlikely.
 * - The declaration is the author's contract, not something the host can
 *   verify: the engine cannot tell a pure callback from one that reads the
 *   clock. Mark a function volatile whenever it is not a pure function of
 *   its arguments; marking a pure function volatile is safe but pointless
 *   (it only costs it the right to run in a WAL-logged statement).
 * - Unknown flag bits are rejected at registration (the load fails) — a
 *   future flag must never be silently ignored, which for this bit would
 *   turn a declared-volatile function back into a deterministic one.
 * - A host older than this addition has a smaller `struct_size` and no
 *   `register_scalar_flags` field. Compare before use:
 *
 *       if (PINKDB_EXT_HOST_HAS_FLAGS(api)) { ... }
 *
 *   (`PINKDB_EXT_HOST_HAS_FLAGS` / `PINKDB_EXT_HOST_HAS_EXECUTE` and the
 *   `PINKDB_EXT_API_*_SIZE` landmarks are defined below the struct.)
 *
 *   An extension that needs a volatile registration MUST fail
 *   `pinkdb_extension_init` (return non-zero) on such a host — never fall
 *   back to `register_scalar`, which would silently register it as
 *   deterministic and let a durable store replay a call whose value may
 *   change.
 */

#ifndef PINKDB_EXTENSION_H
#define PINKDB_EXTENSION_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ABI version of this header / the host loader. */
#define PINKDB_EXT_ABI_VERSION 1
/* Additive level of that major version: 2 = pinkdb_ext_serve and the
 * execute_statement host field; 3 = the VARIANT value type (tag 10);
 * 4 = pinkdb_ext_command and the execute_internal host field;
 * 5 = the ARRAY value types (tag 11, DOUBLE elements; tag 12, FLOAT
 *     elements).
 * Extensions that do not use them need not care; hosts compare
 * PINKDB_EXT_ABI_VERSION and probe the additions. A new type tag is safe in
 * both directions: a host only ever sends tags the extension declared, and a
 * minor-2 host rejects a VARIANT registration with a clear error. */
#define PINKDB_EXT_ABI_MINOR 5

/* Maximum number of function arguments the host accepts. */
#define PINKDB_EXT_MAX_ARGS 64

/* Function flags for register_scalar_flags (see "Deterministic vs volatile"
 * above). The only bit defined today; unknown bits are rejected. */
#define PINKDB_EXT_FLAG_VOLATILE 0x1u

/* Value / argument type tags (same numbering as the pinkdb C ABI). 8 is that
 * ABI's DECIMAL, which extension ABI v1 does not support; 9 is INTERVAL,
 * 10 is VARIANT, 11 is ARRAY of DOUBLE and 12 is ARRAY of FLOAT (see the
 * payload notes at the top of this header; an ARRAY tag carries its length
 * above the kind byte, so use PINKDB_EXT_ARRAY_TAG / PINKDB_EXT_ARRAY_FLOAT_TAG
 * rather than the bare 11 or 12). */
enum {
    PINKDB_EXT_TYPE_NULL = 0,
    PINKDB_EXT_TYPE_BOOLEAN = 1,
    PINKDB_EXT_TYPE_INTEGER = 2,
    PINKDB_EXT_TYPE_BIGINT = 3,
    PINKDB_EXT_TYPE_DOUBLE = 4,
    PINKDB_EXT_TYPE_VARCHAR = 5,
    PINKDB_EXT_TYPE_DATE = 6,
    PINKDB_EXT_TYPE_TIMESTAMP = 7,
    PINKDB_EXT_TYPE_INTERVAL = 9,
    PINKDB_EXT_TYPE_VARIANT = 10,
    PINKDB_EXT_TYPE_ARRAY_DOUBLE = 11,
    PINKDB_EXT_TYPE_ARRAY_FLOAT = 12
};

/* The type tag for a fixed-size DOUBLE array: `(len << 8) | 11`. The low byte
 * is the kind, so a host that predates the type rejects it as an unknown tag
 * at registration instead of misreading it. Lengths above 16,777,215 are not
 * expressible; the engine's own ARRAY types are far below that. */
#define PINKDB_EXT_ARRAY_TAG(len) \
    (PINKDB_EXT_TYPE_ARRAY_DOUBLE | ((int32_t)(len) << 8))

/* The type tag for a fixed-size FLOAT array (4-byte elements): `(len << 8) |
 * 12`. Same rules as PINKDB_EXT_ARRAY_TAG; the element count is in `int64` and
 * `text_len` must be `len * 4` for a result. */
#define PINKDB_EXT_ARRAY_FLOAT_TAG(len) \
    (PINKDB_EXT_TYPE_ARRAY_FLOAT | ((int32_t)(len) << 8))

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

/* SQL text the extension asks the host to run (see pinkdb_ext_serve). The
 * callback runs a statement under the host's own limits, read-only policy
 * and role checks and returns
 *   0  success, `*out_text` / `*out_len` carry the result (Markdown);
 *   1  the statement failed, the text is a JSON error object;
 *   2  no live session (the API this extension got did not come from serve).
 * `sql` need not be NUL-terminated: exactly `sql_len` bytes are read. The
 * returned text is `*out_len` bytes, allocated by the host and freed with
 * `free` below; it is not NUL-terminated. Either out pointer may be NULL
 * (NULL `*out_text` with code 0 means "no output", e.g. an empty result).
 * Statements the read-only policy refuses are code 1 with a JSON error. */
typedef int32_t (*PinkdbExtExecuteStatementFn)(void *ctx,
                                               const char *sql,
                                               size_t sql_len,
                                               char **out_text,
                                               size_t *out_len);

/* One chunk of command output (see pinkdb_ext_command). `text` need not be
 * NUL-terminated: exactly `len` bytes are read. The host copies them before
 * the call returns to the extension, so the buffer may be reused. */
typedef void (*PinkdbExtSinkFn)(void *ctx,
                                int32_t channel,
                                const char *text,
                                size_t len);

/* Host-provided services. Valid during pinkdb_extension_init, and for the
 * whole pinkdb_ext_serve or pinkdb_ext_command call (the host builds a fresh
 * struct for each and wires the statement channels to the live session). */
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

    /* Appended after ABI v1 shipped (additive; see "Deterministic vs
     * volatile" above): register_scalar plus a PINKDB_EXT_FLAG_* bit set.
     * Check `struct_size` before calling — an older host does not have this
     * field. */
    int32_t (*register_scalar_flags)(void *ctx,
                                     const char *name,
                                     uint32_t n_args,
                                     const int32_t *arg_types,
                                     int32_t ret_type,
                                     uint32_t flags,
                                     PinkdbExtScalarFn fn,
                                     void *user_data);

    /* Appended with ABI minor 2: run a statement in the host (see above).
     * Check `struct_size` before calling — an older host has no such field,
     * and calling it there reads past the struct. In the struct handed to
     * pinkdb_extension_init this is a stub that always answers 2. */
    PinkdbExtExecuteStatementFn execute_statement;

    /* Appended with ABI minor 4: run a statement for the extension's own
     * bookkeeping (the agent's session tables). Same signature and codes as
     * execute_statement, but the answer text is JSON — an array of
     * `{"columns":[...],"rows":[[...]]}` objects — and writes are confined to
     * the database the host keeps for this extension. Only wired during
     * pinkdb_ext_command; elsewhere it is the same "no session" stub. */
    PinkdbExtExecuteStatementFn execute_internal;
} PinkdbHostApi;

/* The command entry point (ABI minor 4). Returns 0 on success; a command that
 * failed reports why through the sink and returns non-zero. */
typedef int32_t (*PinkdbExtCommandFn)(const PinkdbHostApi *api,
                                      void *host_ctx,
                                      const char *args,
                                      PinkdbExtSinkFn sink,
                                      void *sink_ctx);

/* `struct_size` negotiation landmarks, in the order the fields were appended.
 * An extension requires PINKDB_EXT_API_V1_SIZE bytes (refuse to init when the
 * host reports less) and may use an appended field only when the host's
 * `struct_size` covers it. Comparing against a landmark — never against a
 * hand-written byte count — keeps an extension working on every host it is
 * compatible with. */
#define PINKDB_EXT_API_V1_SIZE ((uint32_t)offsetof(PinkdbHostApi, register_scalar_flags))
#define PINKDB_EXT_API_FLAGS_SIZE ((uint32_t)offsetof(PinkdbHostApi, execute_statement))
#define PINKDB_EXT_API_MINOR2_SIZE ((uint32_t)offsetof(PinkdbHostApi, execute_internal))
#define PINKDB_EXT_API_MINOR4_SIZE ((uint32_t)sizeof(PinkdbHostApi))

/* 1 when the host provides the register_scalar_flags field, else 0. */
#define PINKDB_EXT_HOST_HAS_FLAGS(api) ((api)->struct_size >= PINKDB_EXT_API_FLAGS_SIZE)
/* 1 when the host provides the execute_statement field, else 0. */
#define PINKDB_EXT_HOST_HAS_EXECUTE(api) ((api)->struct_size >= PINKDB_EXT_API_MINOR2_SIZE)
/* 1 when the host provides the execute_internal field, else 0. */
#define PINKDB_EXT_HOST_HAS_INTERNAL(api) ((api)->struct_size >= PINKDB_EXT_API_MINOR4_SIZE)

/* Copy the host API into an extension-owned struct.
 *
 * Keep the copy: the struct the host passes to pinkdb_extension_init is valid
 * only for that call. Copy exactly the `struct_size` bytes the host published
 * and zero the rest, so a header newer than the host never reads a field the
 * host never wrote. The negotiation macros above still gate every call to an
 * appended field. */
static inline void pinkdb_ext_copy_api(PinkdbHostApi *dst, const PinkdbHostApi *src) {
    size_t published = (size_t)src->struct_size;
    if (published > sizeof(*dst)) published = sizeof(*dst);
    memset(dst, 0, sizeof(*dst));
    memcpy(dst, src, published);
}

#ifdef __cplusplus
}
#endif

#endif /* PINKDB_EXTENSION_H */
