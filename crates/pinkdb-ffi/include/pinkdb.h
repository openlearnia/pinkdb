/*
 * pinkdb C ABI v1.2 — https://github.com/openlearnia/pinkdb
 *
 * Built as `libpinkdb_ffi.dylib` (cdylib) and `libpinkdb_ffi.a` (staticlib)
 * from the `pinkdb-ffi` crate. Link with `-lpinkdb_ffi`.
 *
 * Versioning: `PINKDB_ABI_VERSION` is `major << 16 | minor`. Adding a
 * function, a type tag or a struct field at the end bumps the minor; removing
 * or renaming a function, or changing an existing layout, bumps the major.
 * A caller may accept a newer library with the same major. Structs that cross
 * the boundary by value carry a `struct_size` field when a later minor may
 * grow them (`pink_value` predates that rule and keeps its v1 layout).
 *
 * Contract
 * --------
 * - Every pointer returned by this library is freed with the matching
 *   `pinkdb_*_free` function (`pinkdb_result_free`, `pinkdb_prepared_free`,
 *   `pinkdb_disconnect`, `pinkdb_close_database`, `pinkdb_destroy_config`
 *   and `pinkdb_close`). `errmsg` strings use `pinkdb_free`. Never free the
 *   same pointer twice.
 * - Text pointers (`pinkdb_result_column_name`, `pinkdb_result_name`,
 *   `pinkdb_result_profile_kind`, `pink_value.text_ptr`, `pinkdb_last_error`,
 *   `pinkdb_last_error_class`)
 *   borrow from their owning result/handle/thread and stay valid only until
 *   that owner is freed or the next FFI call on the same thread. Copy them if
 *   you need them longer. `text_ptr` is followed by a NUL byte but may contain
 *   embedded NULs — always trust `text_len`. `pinkdb_string_t.ptr` borrows
 *   from its chunk and is valid until `pinkdb_destroy_chunk`.
 * - v1 handles are NOT thread-safe: never call into one handle from two
 *   threads at once. A durable file may be opened by several processes;
 *   writes are serialized by a per-file write lease, and concurrent
 *   transactions use optimistic COMMIT validation (a conflict is a retryable
 *   error, never a silent lost update). In-memory stores are independent.
 *   The v1 errors are thread-local; `pinkdb_last_error` returns NULL when the
 *   last v1 call on the calling thread succeeded.
 * - Databases may be opened and shared across threads. A connection belongs
 *   to one thread at a time (create it on any thread, then hand it over), with
 *   one exception: `pinkdb_interrupt` may be called from another thread while
 *   a statement runs on that connection.
 * - SQL is passed as (pointer, length) and must be valid UTF-8. Passing a
 *   NULL pointer with a non-zero length, or invalid UTF-8, is an error.
 * - v1 error reporting: functions that return a pointer return NULL on
 *   failure; index/kind accessors return 0/-1 for out-of-range indices; call
 *   `pinkdb_last_error` for the message and `pinkdb_last_error_class` for its
 *   class.
 * - New API error reporting: `pinkdb_query` and `pinkdb_query_prepared`
 *   always return a result, successful or failed; read its error with
 *   `pinkdb_result_error`/`pinkdb_result_error_class` until
 *   `pinkdb_result_free`. Functions with an `errmsg` out-parameter set it to
 *   a string (free with `pinkdb_free`) on failure and NULL on success.
 */

#ifndef PINKDB_H
#define PINKDB_H

#include <stddef.h>
#include <stdint.h>

#define PINKDB_ABI_VERSION_MAJOR 1
#define PINKDB_ABI_VERSION_MINOR 2
#define PINKDB_ABI_VERSION ((PINKDB_ABI_VERSION_MAJOR << 16) | PINKDB_ABI_VERSION_MINOR)

/* Library (not ABI) version, from the Cargo package version. */
#define PINKDB_VERSION_MAJOR 0
#define PINKDB_VERSION_MINOR 1
#define PINKDB_VERSION_PATCH 0
#define PINKDB_VERSION "0.1.0"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque types. */
typedef struct pinkdb_handle pinkdb_handle;
typedef struct pinkdb_result pinkdb_result;
typedef struct pinkdb_prepared pinkdb_prepared;
typedef struct pinkdb_database pinkdb_database;
typedef struct pinkdb_connection pinkdb_connection;
typedef struct pinkdb_config pinkdb_config;
typedef struct pinkdb_data_chunk pinkdb_data_chunk;
typedef struct pinkdb_vector pinkdb_vector;

/* A VARCHAR/BLOB/VARIANT element in a chunk: `len` bytes at `ptr`, never
 * NUL-terminated. `ptr` is NULL and `len` 0 when the row is NULL. */
typedef struct pinkdb_string_t {
    const char *ptr;
    uint64_t len;
} pinkdb_string_t;

/* INTERVAL payload: months, days and microseconds, each added independently. */
typedef struct pinkdb_interval {
    int32_t months;
    int32_t days;
    int64_t micros;
} pinkdb_interval;

/* HUGEINT and DECIMAL: a signed 128-bit integer, two's complement. */
typedef struct pinkdb_hugeint {
    uint64_t lower;
    int64_t upper;
} pinkdb_hugeint;

/* UHUGEINT and UUID: an unsigned 128-bit integer, low limb first. A UUID is
 * the engine's 128-bit value; the v1 cell and the bindings render the
 * canonical text form. */
typedef struct pinkdb_uhugeint {
    uint64_t lower;
    uint64_t upper;
} pinkdb_uhugeint;

/* Statement kind tags (pinkdb_result_kind). */
enum {
    PINKDB_STMT_SELECTED = 0,
    PINKDB_STMT_CREATED = 1,
    PINKDB_STMT_INSERTED = 2,
    PINKDB_STMT_UPDATED = 3,
    PINKDB_STMT_DELETED = 4,
    PINKDB_STMT_BEGAN = 5,
    PINKDB_STMT_COMMITTED = 6,
    PINKDB_STMT_ROLLEDBACK = 7,
    PINKDB_STMT_CREATED_VIEW = 8,
    PINKDB_STMT_DROPPED_TABLE = 9,
    PINKDB_STMT_DROPPED_VIEW = 10,
    PINKDB_STMT_CREATED_DATABASE = 11,
    PINKDB_STMT_DROPPED_DATABASE = 12,
    PINKDB_STMT_USED = 13,
    /* LOAD EXTENSION '<path>': pinkdb_result_name returns the extension's
     * declared name. */
    PINKDB_STMT_LOADED_EXTENSION = 14,
    /* CHECKPOINT (a no-op for in-memory databases). */
    PINKDB_STMT_CHECKPOINTED = 15,
    /* ALTER TABLE: pinkdb_result_name returns the table's name after the
     * statement (a RENAME TO reports the new name). */
    PINKDB_STMT_ALTERED_TABLE = 16,
    /* Wave 48 RBAC statements. The *_USER / *_ROLE kinds return the user or
     * role name; GRANTED / REVOKED return the grant's description
     * ("SELECT ON TABLE main.main.t TO alice"). */
    PINKDB_STMT_CREATED_USER = 17,
    PINKDB_STMT_ALTERED_USER = 18,
    PINKDB_STMT_DROPPED_USER = 19,
    PINKDB_STMT_CREATED_ROLE = 20,
    PINKDB_STMT_DROPPED_ROLE = 21,
    PINKDB_STMT_GRANTED = 22,
    PINKDB_STMT_REVOKED = 23,
    /* SET VARIABLE (wave 95c): pinkdb_result_name returns the variable. */
    PINKDB_STMT_SET_VARIABLE = 24,
    /* Wave 131b schema DDL: pinkdb_result_name returns the schema name. */
    PINKDB_STMT_CREATED_SCHEMA = 25,
    PINKDB_STMT_DROPPED_SCHEMA = 26
};

/* Value / column type tags. */
enum {
    PINKDB_TYPE_NULL = 0,
    PINKDB_TYPE_BOOLEAN = 1,
    PINKDB_TYPE_INTEGER = 2,
    PINKDB_TYPE_BIGINT = 3,
    PINKDB_TYPE_DOUBLE = 4,
    PINKDB_TYPE_VARCHAR = 5,
    PINKDB_TYPE_DATE = 6,
    PINKDB_TYPE_TIMESTAMP = 7,
    /* v1 additive: DECIMAL/INTERVAL render into `text` (canonical DuckDB
       forms); `i`/`d` are zero. */
    PINKDB_TYPE_DECIMAL = 8,
    PINKDB_TYPE_INTERVAL = 9,
    /* v1 additive: VARIANT renders into `text` as its JSON value. Its DATE and
       TIMESTAMP kinds print as ISO strings (they come back as VARCHAR). */
    PINKDB_TYPE_VARIANT = 10,
    /* v1 additive: a DOUBLE[N] array. The low byte is this enumerator and the
       element count N travels in the tag itself — see PINKDB_ARRAY_TAG. */
    PINKDB_TYPE_ARRAY = 11,
    /* v1 additive: a FLOAT[N] array — the 4-byte counterpart of
       PINKDB_TYPE_ARRAY. See PINKDB_ARRAY_FLOAT_TAG. */
    PINKDB_TYPE_ARRAY_FLOAT = 12,
    /* v1 additive: 128-bit integers render into `text` as their exact decimal
       string; `i`/`d` are zero. Every SUM over an integer column now yields
       HUGEINT, so a host that predates these tags sees them as unknown. */
    PINKDB_TYPE_HUGEINT = 13,
    PINKDB_TYPE_UHUGEINT = 14,
    /* v1.2: UUID. The chunk API exposes 16 bytes; the v1 cell API still
       renders the canonical lowercase text. */
    PINKDB_TYPE_UUID = 15,
    /* v1.2: FLOAT, an array element type only (never a column's own tag). It
       is reported by pinkdb_result_column_array_element_type. */
    PINKDB_TYPE_FLOAT = 16
};

/* An ARRAY's type tag: the type in the low byte, the element count above it.
 * `PINKDB_ARRAY_LEN(tag)` recovers N. A host that predates ARRAY sees an
 * unknown low byte instead of misreading a length as a type. */
#define PINKDB_ARRAY_TAG(len) ((int32_t)(PINKDB_TYPE_ARRAY) | ((int32_t)(len) << 8))
#define PINKDB_ARRAY_FLOAT_TAG(len) \
    ((int32_t)(PINKDB_TYPE_ARRAY_FLOAT) | ((int32_t)(len) << 8))
#define PINKDB_ARRAY_LEN(tag) (((uint32_t)(tag)) >> 8)

/* One result value, passed by value.
 *
 * `int64` carries BOOLEAN (0/1), INTEGER, BIGINT, DATE (days since epoch) and
 * TIMESTAMP (microseconds since epoch). `double` carries DOUBLE. `text_ptr` /
 * `text_len` carry VARCHAR (with a trailing NUL at text_ptr[text_len]).
 *
 * An ARRAY value uses `type_tag = PINKDB_ARRAY_TAG(N)` (DOUBLE elements) or
 * `PINKDB_ARRAY_FLOAT_TAG(N)` (FLOAT elements), puts N in `int64`, and hands
 * over N * 8 (`f64`) or N * 4 (`f32`) little-endian bytes as a binary payload
 * in `text_ptr` / `text_len` (same trailing NUL). An ARRAY with a NULL
 * element is refused rather than silently defaulted — there is no per-element
 * NULL channel in this struct. INTEGER/BIGINT arrays are refused too; CAST the
 * column (e.g. `SELECT CAST(v AS VARCHAR)` or a COALESCE'd element access).
 * When `is_null` is non-zero only `type_tag` is meaningful. */
typedef struct pink_value {
    int32_t type_tag;
    int32_t is_null;
    int64_t int64;
    double double_;
    const char *text_ptr;
    size_t text_len;
} pink_value;

/* ABI version of the linked library, `major << 16 | minor` (1.2 is
 * 0x00010002). Compare it with PINKDB_ABI_VERSION; a same-major version is
 * forward compatible. */
uint32_t pinkdb_abi_version(void);

/* Open a durable database (checkpoint + WAL) at a filesystem path.
 * Returns NULL on error. */
pinkdb_handle *pinkdb_open(const char *path);

/* Open a fresh, empty in-memory database. Never returns NULL. */
pinkdb_handle *pinkdb_open_memory(void);

/* Close a handle. NULL-safe. */
void pinkdb_close(pinkdb_handle *handle);

/* Execute one or more `;`-separated SQL statements.
 * Returns NULL on error (see pinkdb_last_error); free with
 * pinkdb_result_free.
 *
 * SECURITY: this can load native code. The SQL statement
 * `LOAD EXTENSION '<path>'` makes the host process load and run a native
 * library (dlopen) from a path the SQL text names. It is DISABLED by
 * default on every handle: pinkdb_execute can only load native code after
 * pinkdb_set_allow_sql_load_extension(handle, 1) has enabled it. An
 * application that forwards untrusted input to pinkdb_execute inherits
 * arbitrary native code execution once the switch is on. */
pinkdb_result *pinkdb_execute(pinkdb_handle *handle, const char *sql,
                              size_t sql_len);

/* Enable (non-zero) or disable (zero) query profiling on this handle.
 * Profiling is OFF by default and is a per-handle (per-session) setting.
 * Returns 1 on success, 0 on failure (a NULL handle; see pinkdb_last_error).
 *
 * While enabled, the result returned by pinkdb_execute /
 * pinkdb_execute_prepared carries one profile per statement, read with the
 * pinkdb_result_profile_* accessors using the same statement index as the
 * other result accessors. Profiling only reads clocks: it never changes
 * results or WAL bytes, and when it is off both execute functions take the
 * plain engine path (zero profiling cost). */
int32_t pinkdb_set_profiling(pinkdb_handle *handle, int32_t enabled);

/* Allow (non-zero) or refuse (zero) the SQL `LOAD EXTENSION` statement on
 * this handle. Returns 1 on success, 0 on failure (a NULL handle; see
 * pinkdb_last_error).
 *
 * SECURITY: `LOAD EXTENSION '<path>'` makes the host process load and run a
 * native library from a path the SQL text names — arbitrary native code
 * execution for anyone who can influence the SQL. The statement is disabled
 * by default on every handle; enable it only for a handle whose SQL comes
 * from a trusted source. The loaded path must name a regular file (not a
 * symlink) inside a directory; a bare library name (which the dynamic
 * loader would resolve through its search path) is refused. */
int32_t pinkdb_set_allow_sql_load_extension(pinkdb_handle *handle,
                                            int32_t allow);

/* Free a result. NULL-safe. */
void pinkdb_result_free(pinkdb_result *result);

/* Prepared statements (`?` placeholders).
 *
 * `pinkdb_prepare` parses exactly one SQL statement; `?` in it is a
 * positional placeholder, numbered by textual order. Types are resolved when
 * the statement executes (the bind step): a comparison/arithmetic operator,
 * an INSERT/UPDATE target column, or `CAST(? AS type)` fixes the
 * placeholder's type, the bound value is converted to it, and an impossible
 * conversion or an untypable NULL is a bind error (pinkdb_last_error). The
 * prepared statement is reusable; every execution rebinds (fresh catalog
 * state) and, on a durable database, the WAL logs the fully substituted SQL.
 * Free with pinkdb_prepared_free. */
pinkdb_prepared *pinkdb_prepare(pinkdb_handle *handle, const char *sql,
                                size_t sql_len);

/* Number of `?` placeholders: how many values pinkdb_execute_prepared takes
 * (0 for NULL). */
size_t pinkdb_prepared_parameter_count(const pinkdb_prepared *prepared);

/* Free a prepared statement. NULL-safe. */
void pinkdb_prepared_free(pinkdb_prepared *prepared);

/* Execute a prepared statement with `value_count` values (in textual order).
 * Returns NULL on error (see pinkdb_last_error); free with
 * pinkdb_result_free. A wrong value count, a value of the wrong type, or a
 * bind/execution failure is a normal error; the handle stays usable. */
pinkdb_result *pinkdb_execute_prepared(pinkdb_handle *handle,
                                       const pinkdb_prepared *prepared,
                                       const pink_value *values,
                                       size_t value_count);

/* Number of statements in the result. */
size_t pinkdb_result_statement_count(const pinkdb_result *result);

/* Statement kind (PINKDB_STMT_*), -1 when out of range. */
int32_t pinkdb_result_kind(const pinkdb_result *result, size_t statement);

/* SELECT results: column count, else 0. */
size_t pinkdb_result_column_count(const pinkdb_result *result,
                                  size_t statement);

/* SELECT results: column name (NUL-terminated), NULL when out of range. */
const char *pinkdb_result_column_name(const pinkdb_result *result,
                                      size_t statement, size_t column);

/* SELECT results: column type tag (PINKDB_TYPE_*), -1 when out of range. */
int32_t pinkdb_result_column_type(const pinkdb_result *result,
                                  size_t statement, size_t column);

/* SELECT results: row count, else 0. */
size_t pinkdb_result_row_count(const pinkdb_result *result, size_t statement);

/* SELECT results: value at (statement, row, column); a NULL value when out of
 * range or when the statement is not a SELECT. */
pink_value pinkdb_result_value(const pinkdb_result *result, size_t statement,
                               size_t row, size_t column);

/*
 * Chunk API (ABI 1.2)
 * -------------------
 * A SELECT's rows are produced as columnar chunks of at most 2048 rows. The
 * chunk API exposes those chunks without converting them to cells:
 *
 *     pinkdb_data_chunk *chunk;
 *     while ((chunk = pinkdb_result_fetch_chunk(result, statement))) {
 *         size_t rows = pinkdb_chunk_size(chunk);
 *         const uint64_t *valid = pinkdb_vector_validity(
 *             pinkdb_chunk_column(chunk, 0));
 *         const int32_t *data = pinkdb_vector_data(
 *             pinkdb_chunk_column(chunk, 0));
 *         ...
 *         pinkdb_destroy_chunk(chunk);
 *     }
 *
 * Fetch order is statement-local: each call returns the next chunk and NULL
 * once the end is reached. Fetching from a failed result, a non-SELECT
 * statement, or a statement index out of range returns NULL. A fetch after
 * the end keeps returning NULL (it does not restart).
 *
 * Ownership: the caller owns each chunk and frees it with
 * pinkdb_destroy_chunk. A chunk holds its own reference to the underlying
 * column data, so it stays valid after pinkdb_result_free: a chunk may be
 * destroyed before or after its result. Vectors are borrowed from their
 * chunk and are invalid after pinkdb_destroy_chunk. A result may be freed
 * while chunks from it are still alive.
 */

/* DECIMAL precision, or -1 when the column is not DECIMAL. */
int32_t pinkdb_result_column_decimal_width(const pinkdb_result *result,
                                           size_t statement, size_t column);

/* DECIMAL scale (digits after the point), or -1 when the column is not
 * DECIMAL. */
int32_t pinkdb_result_column_decimal_scale(const pinkdb_result *result,
                                           size_t statement, size_t column);

/* ARRAY element count, or 0 when the column is not an ARRAY. */
uint64_t pinkdb_result_column_array_length(const pinkdb_result *result,
                                           size_t statement, size_t column);

/* An ARRAY's element type (PINKDB_TYPE_DOUBLE or PINKDB_TYPE_FLOAT), or -1
 * when the column is not an ARRAY. */
int32_t pinkdb_result_column_array_element_type(const pinkdb_result *result,
                                                size_t statement, size_t column);

/* The next chunk of a SELECT result, or NULL at the end / for a failed or
 * non-SELECT result. The caller frees it with pinkdb_destroy_chunk. */
pinkdb_data_chunk *pinkdb_result_fetch_chunk(pinkdb_result *result,
                                             size_t statement);

/* Free a chunk. NULL-safe. */
void pinkdb_destroy_chunk(pinkdb_data_chunk *chunk);

/* Rows in a chunk (0 for NULL). */
size_t pinkdb_chunk_size(const pinkdb_data_chunk *chunk);

/* A vector borrowed from `chunk`, or NULL when the column is out of range.
 * Valid until pinkdb_destroy_chunk(chunk). */
pinkdb_vector *pinkdb_chunk_column(const pinkdb_data_chunk *chunk,
                                   size_t column);

/* The vector's payload in its native layout:
 *
 *   PINKDB_TYPE_BOOLEAN      const uint8_t *   (0 or 1, one byte per row)
 *   PINKDB_TYPE_INTEGER      const int32_t *
 *   PINKDB_TYPE_BIGINT       const int64_t *
 *   PINKDB_TYPE_DOUBLE       const double *
 *   PINKDB_TYPE_DATE         const int32_t *   (days since 1970-01-01)
 *   PINKDB_TYPE_TIMESTAMP    const int64_t *   (microseconds since epoch, UTC)
 *   PINKDB_TYPE_INTERVAL     const pinkdb_interval *
 *   PINKDB_TYPE_HUGEINT      const pinkdb_hugeint *
 *   PINKDB_TYPE_DECIMAL      const pinkdb_hugeint * (unscaled; use
 *                                                  decimal_scale for the point)
 *   PINKDB_TYPE_UHUGEINT     const pinkdb_uhugeint *
 *   PINKDB_TYPE_UUID         const pinkdb_uhugeint * (the 128-bit value;
 *                                                    canonical text is upper
 *                                                    then lower, in hex)
 *   PINKDB_TYPE_VARCHAR      const pinkdb_string_t *
 *   PINKDB_TYPE_VARIANT      const pinkdb_string_t * (provisional: the JSON
 *                                                    text of each value)
 *
 * BOOLEAN, INTEGER, BIGINT, DOUBLE, DATE, TIMESTAMP, INTERVAL, HUGEINT,
 * UHUGEINT, UUID and DECIMAL are zero-copy on little-endian hosts: the
 * pointer is the engine's own column buffer. VARCHAR and VARIANT build one
 * string-element array per chunk without copying the bytes (VARIANT builds
 * its JSON text once per chunk). On a big-endian host the 128-bit types are
 * copied into a {lower, upper} array per chunk.
 *
 * Provisional: ARRAY has no data pointer in 1.2 (returns NULL); use the v1
 * cell API for ARRAY columns. 112c's Arrow export is the full nested path.
 * A NULL row's payload bytes are unspecified; check validity first. */
const void *pinkdb_vector_data(const pinkdb_vector *vector);

/* The vector's validity mask: u64 words, bit i set means row i is valid.
 * NULL when every row is valid. Bits at or beyond pinkdb_chunk_size are
 * unspecified. */
const uint64_t *pinkdb_vector_validity(const pinkdb_vector *vector);

/* 1 when `row` is valid, 0 otherwise. A NULL mask means every row is valid,
 * so this returns 1. */
int32_t pinkdb_validity_row_is_valid(const uint64_t *validity, size_t row);

/* INSERT/UPDATE/DELETE results: affected row count, else 0. */
size_t pinkdb_result_affected(const pinkdb_result *result, size_t statement);

/* CREATE/DROP VIEW/TABLE/DATABASE, USE and LOAD EXTENSION results: the
 * object/session/extension name (NUL-terminated), else NULL. */
const char *pinkdb_result_name(const pinkdb_result *result, size_t statement);

/* Query profiles (wave 84; opt-in with pinkdb_set_profiling).
 *
 * The profile of a statement exists only when profiling was enabled on the
 * handle for the execution that produced this result. "No profile" is
 * reported by pinkdb_result_profile_available returning 0, the numeric
 * accessors returning 0, and pinkdb_result_profile_kind returning NULL; it is
 * never an error and never crashes. A NULL result or an out-of-range
 * statement index is likewise reported as "no profile" (0/NULL), matching
 * the other index accessors. Call pinkdb_result_profile_available first when
 * a real zero (for example `log_ns` on a read) must be told apart from the
 * absence of a profile.
 *
 * Durations are nanoseconds (uint64_t); rows and columns are the profile's
 * own counts (SELECT rows/columns, or the affected count for
 * INSERT/UPDATE/DELETE). The kind string is NUL-terminated, borrowed from the
 * result, and valid until pinkdb_result_free (see the lifetime contract at
 * the top of this header). */
int32_t pinkdb_result_profile_available(const pinkdb_result *result,
                                        size_t statement);

/* Parsing time of the SQL text, in nanoseconds. */
uint64_t pinkdb_result_profile_parse_ns(const pinkdb_result *result,
                                        size_t statement);

/* Binding/planning time, in nanoseconds. */
uint64_t pinkdb_result_profile_bind_ns(const pinkdb_result *result,
                                       size_t statement);

/* Optimizer time, in nanoseconds. */
uint64_t pinkdb_result_profile_optimize_ns(const pinkdb_result *result,
                                           size_t statement);

/* Execution time, in nanoseconds. */
uint64_t pinkdb_result_profile_execute_ns(const pinkdb_result *result,
                                          size_t statement);

/* WAL logging/checkpoint time, in nanoseconds (0 for reads and in-memory
 * stores). */
uint64_t pinkdb_result_profile_log_ns(const pinkdb_result *result,
                                      size_t statement);

/* Total wall time of the statement, in nanoseconds. */
uint64_t pinkdb_result_profile_total_ns(const pinkdb_result *result,
                                        size_t statement);

/* Result rows (SELECT) or affected count (INSERT/UPDATE/DELETE). */
uint64_t pinkdb_result_profile_rows(const pinkdb_result *result,
                                    size_t statement);

/* Result columns (SELECT only). */
uint32_t pinkdb_result_profile_columns(const pinkdb_result *result,
                                       size_t statement);

/* Statement kind ("Selected", "Inserted", ...), NUL-terminated, owned by the
 * result; NULL when there is no profile. */
const char *pinkdb_result_profile_kind(const pinkdb_result *result,
                                       size_t statement);

/*
 * Database / connection API (ABI 1.1)
 * -----------------------------------
 * A database owns the store; connections own independent sessions. Open one
 * database, connect once per thread, and disconnect every connection before
 * closing the database.
 */

/* Create an empty configuration. Free with pinkdb_destroy_config. Never
 * returns NULL in practice; NULL on allocation failure. */
pinkdb_config *pinkdb_create_config(void);

/* Set one option, returning 1 on success and 0 on an unknown name or invalid
 * value. `name` and `value` are NUL-terminated UTF-8 and are read only for
 * this call. Names are case-insensitive:
 *
 *   read_only                 "true"/"false" (default false)
 *   access_mode               "automatic", "read_only", "read_write"
 *   memory_limit              DuckDB-style size ("1GB", "512MB", "-1" = none)
 *   threads                   positive integer
 *   wal_sync_method           "full", "fsync", "fdatasync"
 *   allow_sql_load_extension  "true"/"false" (default false)
 *
 * memory_limit is process-wide and applies when the database is opened.
 * read_only/access_mode refuse every non-query statement; they are enforced
 * by this library, not by file permissions.
 * On failure, pinkdb_config_error holds the message naming the option. */
int32_t pinkdb_config_set(pinkdb_config *config, const char *name,
                          const char *value);

/* Copy a 32-byte raw key into the configuration. The key is never logged or
 * echoed. Returns 1, or 0 for a NULL pointer or a length other than 32.
 * Keyed stores require a build with the `encryption` feature; opening one
 * without it fails with a clear error. In-memory stores cannot be keyed. */
int32_t pinkdb_config_set_encryption_key(pinkdb_config *config,
                                         const unsigned char *key,
                                         size_t key_len);

/* Last configuration failure, or NULL. Borrowed from the configuration and
 * valid until the next set call or pinkdb_destroy_config. */
const char *pinkdb_config_error(const pinkdb_config *config);

/* Destroy a configuration. NULL-safe. Databases opened with it keep their
 * copies of every setting. */
void pinkdb_destroy_config(pinkdb_config *config);

/* Open a database. `path` NULL or ":memory:" is in-memory; otherwise durable.
 * `config` may be NULL for defaults. Returns 1 on success, 0 on failure.
 * On failure *out_database is NULL and *errmsg is a string to free with
 * pinkdb_free; on success *errmsg is NULL. Both out parameters must be
 * non-NULL. The database may be used from several threads. */
int32_t pinkdb_open_ext(const char *path, const pinkdb_config *config,
                        pinkdb_database **out_database, char **errmsg);

/* Close the database. NULL-safe. Existing connections keep the store alive
 * until disconnected, but later queries on them fail with "invalid_argument".
 * Free with this call (not pinkdb_free). Do not call this concurrently with
 * any other function that uses the same database handle. */
void pinkdb_close_database(pinkdb_database *database);

/* Create an independent connection (session) on an open database. May be
 * called from any thread. Returns 1 and stores the connection, or 0 on a
 * NULL/closed database or allocation failure. Free with
 * pinkdb_disconnect. */
int32_t pinkdb_connect(pinkdb_database *database,
                       pinkdb_connection **out_connection);

/* Disconnect. NULL-safe. Do not use the pointer afterwards. */
void pinkdb_disconnect(pinkdb_connection *connection);

/* Stop the statement currently running on `connection`. Call from another
 * thread while that statement runs. The statement stops at the executor's
 * next cancellation checkpoint with the "interrupted" error class. A flag set
 * before the next query starts is cleared by that query. NULL-safe; no other
 * call on the connection may run concurrently. */
void pinkdb_interrupt(pinkdb_connection *connection);

/* Execute one or more SQL statements on a connection. Returns 1 when they
 * all ran, 0 when one failed; either way *out_result is a result owned by the
 * caller (free with pinkdb_result_free). Inspect a failed result with
 * pinkdb_result_error/class. *out_result must be non-NULL.
 *
 * SECURITY: the LOAD EXTENSION policy is fixed when the database is opened;
 * see allow_sql_load_extension above. */
int32_t pinkdb_query(pinkdb_connection *connection, const char *sql,
                     size_t sql_len, pinkdb_result **out_result);

/* Prepare one statement with `?` placeholders on a connection. Returns 1, or
 * 0 with *out_prepared NULL and *errmsg a string to free with pinkdb_free;
 * on success *errmsg is NULL. Both out parameters must be non-NULL. The
 * prepared statement may be executed on any connection of the same database
 * and is freed with pinkdb_prepared_free. */
int32_t pinkdb_prepare_ext(pinkdb_connection *connection, const char *sql,
                           size_t sql_len, pinkdb_prepared **out_prepared,
                           char **errmsg);

/* Execute a prepared statement with one pink_value per placeholder in
 * textual order. Returns and reports exactly like pinkdb_query. Values are
 * read only for the call (VARCHAR payloads are copied). */
int32_t pinkdb_query_prepared(pinkdb_connection *connection,
                              const pinkdb_prepared *prepared,
                              const pink_value *values, size_t value_count,
                              pinkdb_result **out_result);

/* Error carried by a result, or NULL when it succeeded. Owned by the result;
 * valid until pinkdb_result_free. */
const char *pinkdb_result_error(const pinkdb_result *result);

/* Stable error class of pinkdb_result_error, or NULL when the result
 * succeeded. Static string: never free. Same class names as
 * pinkdb_last_error_class, plus "permission" (read-only refusal) and
 * "interrupted" (pinkdb_interrupt). */
const char *pinkdb_result_error_class(const pinkdb_result *result);

/* Free an errmsg string from pinkdb_open_ext/pinkdb_prepare_ext. NULL-safe.
 * Not a general-purpose free: only pass strings this library returned. */
void pinkdb_free(void *ptr);

/* Last error on the calling thread (NUL-terminated), or NULL when the last
 * call on this thread succeeded. Valid until the next FFI call on this
 * thread. */
const char *pinkdb_last_error(void);

/* Class of the last error on the calling thread (NUL-terminated), or NULL
 * when the last call succeeded. One of:
 *
 *   "parse"            the SQL text did not parse
 *   "catalog"          a table/column/database/function does not exist
 *   "type"             a type error (conversion, arithmetic, ...)
 *   "parameter"        wrong bound-parameter count, untypable NULL, ...
 *   "execution"        the statement failed while running
 *   "constraint"       a constraint refused the statement
 *   "io"               a filesystem/WAL error
 *   "csv"              a CSV import/export error
 *   "invalid_argument" the caller passed a bad argument (NULL handle, invalid
 *                      UTF-8, a value that cannot be bound, ...)
 *   "permission"       the database is read-only and the statement writes
 *   "interrupted"      pinkdb_interrupt stopped the statement
 *   "internal"         a caught panic; the handle is still usable
 *
 * The string is static: never free it, and it stays valid across later calls
 * (unlike pinkdb_last_error). Drivers map these classes onto their own
 * exception hierarchies. New classes may be added in later ABI versions;
 * treat an unknown class as "internal". */
const char *pinkdb_last_error_class(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* PINKDB_H */
