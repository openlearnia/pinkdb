/*
 * pinkdb C ABI v1 — https://github.com/openlearnia/pinkdb
 *
 * Built as `libpinkdb_ffi.dylib` (cdylib) and `libpinkdb_ffi.a` (staticlib)
 * from the `pinkdb-ffi` crate. Link with `-lpinkdb_ffi`.
 *
 * Contract
 * --------
 * - Every pointer returned by this library is freed with the matching
 *   `pinkdb_*_free` function (`pinkdb_result_free`, `pinkdb_prepared_free`
 *   and `pinkdb_close`). Never free the same pointer twice.
 * - Text pointers (`pinkdb_result_column_name`, `pinkdb_result_name`,
 *   `pink_value.text_ptr`, `pinkdb_last_error`) borrow from their owning
 *   result/handle/thread and stay valid only until that owner is freed or the
 *   next FFI call on the same thread. Copy them if you need them longer.
 *   `text_ptr` is followed by a NUL byte but may contain embedded NULs —
 *   always trust `text_len`.
 * - Handles are NOT thread-safe: never call into one handle from two threads
 *   at once (serialize calls, e.g. with a mutex — the Go and Python bindings
 *   do). A durable file may be opened by several handles/processes; writes are
 *   serialized by a per-file write lease, and concurrent transactions use
 *   optimistic COMMIT validation (a conflict is a retryable error, never a
 *   silent lost update). In-memory handles are independent. Errors are
 *   thread-local; `pinkdb_last_error` returns NULL
 *   when the last call on the calling thread succeeded.
 * - SQL is passed as (pointer, length) and must be valid UTF-8. Passing a
 *   NULL pointer with a non-zero length, or invalid UTF-8, is an error.
 * - Error reporting: functions that return a pointer return NULL on failure;
 *   index/kind accessors return 0/-1 for out-of-range indices; call
 *   `pinkdb_last_error` for the message.
 */

#ifndef PINKDB_H
#define PINKDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque types. */
typedef struct pinkdb_handle pinkdb_handle;
typedef struct pinkdb_result pinkdb_result;
typedef struct pinkdb_prepared pinkdb_prepared;

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
     * ("SELECT ON TABLE main.t TO alice"). */
    PINKDB_STMT_CREATED_USER = 17,
    PINKDB_STMT_ALTERED_USER = 18,
    PINKDB_STMT_DROPPED_USER = 19,
    PINKDB_STMT_CREATED_ROLE = 20,
    PINKDB_STMT_DROPPED_ROLE = 21,
    PINKDB_STMT_GRANTED = 22,
    PINKDB_STMT_REVOKED = 23
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
    PINKDB_TYPE_INTERVAL = 9
};

/* One result value, passed by value.
 *
 * `int64` carries BOOLEAN (0/1), INTEGER, BIGINT, DATE (days since epoch) and
 * TIMESTAMP (microseconds since epoch). `double` carries DOUBLE. `text_ptr` /
 * `text_len` carry VARCHAR (with a trailing NUL at text_ptr[text_len]).
 * When `is_null` is non-zero only `type_tag` is meaningful. */
typedef struct pink_value {
    int32_t type_tag;
    int32_t is_null;
    int64_t int64;
    double double_;
    const char *text_ptr;
    size_t text_len;
} pink_value;

/* ABI version of the linked library (currently 1). */
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

/* INSERT/UPDATE/DELETE results: affected row count, else 0. */
size_t pinkdb_result_affected(const pinkdb_result *result, size_t statement);

/* CREATE/DROP VIEW/TABLE/DATABASE, USE and LOAD EXTENSION results: the
 * object/session/extension name (NUL-terminated), else NULL. */
const char *pinkdb_result_name(const pinkdb_result *result, size_t statement);

/* Last error on the calling thread (NUL-terminated), or NULL when the last
 * call on this thread succeeded. Valid until the next FFI call on this
 * thread. */
const char *pinkdb_last_error(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* PINKDB_H */
