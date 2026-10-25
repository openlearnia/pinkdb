/*
 * pinkdb extension skeleton (ABI 1.2) — copy this directory and rename.
 *
 * This file is a complete, buildable extension that registers two scalar
 * functions and shows the optional serve entry point:
 *
 *     my_ext_greet(name VARCHAR) -> VARCHAR      -- "Hello, <name>!"
 *     my_ext_tick()              -> BIGINT       -- a volatile counter
 *
 * Build it:
 *
 *     make                       # in this directory, uses ../include
 *     cmake -S . -B build && cmake --build build    # alternative
 *
 * Load it (from a pinkdb REPL, or any client):
 *
 *     LOAD EXTENSION 'my_extension.dylib';   -- .so on Linux
 *     SELECT my_ext_greet('world');          -- Hello, world!
 *
 * Loading is opt-in: start the CLI (or server) with `--allow-load-extension`
 * or set `PINKDB_ALLOW_LOAD_EXTENSION=1`, or the LOAD EXTENSION below is
 * refused. `--extension-root DIR` confines loads to DIR; a bare name, a `..`
 * component and a symlink are always refused. `--extension-pin PATH=SHA256`
 * (repeatable) pins the bytes when the directory is not fully trusted. A
 * local library is unsigned (Ed25519 covers `.install` packages only) and
 * runs with the loader's privileges — as the server's, on a server.
 *
 * The contract this file follows is documented in `../include/pinkdb_extension.h`
 * — read it once before writing real code. The rules that bite most:
 *
 *   1. VARCHAR results must be allocated with `api->alloc`, never malloc/new;
 *      the host copies `text_len` bytes and calls `api->free` immediately
 *      after the callback returns. A non-NULL `text_ptr` is always freed by
 *      the host (even on an error return), so never return a stack, static,
 *      or interior pointer.
 *   2. Callbacks run CONCURRENTLY on the host's rayon worker threads. Every
 *      piece of mutable state must be atomic/locked/thread-local. `alloc` and
 *      `free` are thread-safe; registration may only happen from
 *      `pinkdb_extension_init`.
 *   3. Never panic, throw, or unwind across the ABI boundary — the host calls
 *      you through an `extern "C"` edge where unwinding is undefined
 *      behavior. Catch/avoid every error internally and return a non-zero
 *      code (it aborts the statement with an error naming the function).
 *   4. Declare nondeterministic functions with PINKDB_EXT_FLAG_VOLATILE. The
 *      engine then refuses them in any statement it WAL-logs, because replay
 *      would recompute a different value. A host that predates the flags
 *      entry point must fail the load, never silently register the function
 *      as deterministic.
 */

#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------ */
/* Host API                                                                  */
/* ------------------------------------------------------------------------ */

/*
 * The `PinkdbHostApi` struct passed to `pinkdb_extension_init` is only valid
 * for the duration of that call (the header documents this). Callbacks run
 * later, on other threads, and need `alloc`/`free` — so copy the struct once,
 * during init, into a static. It is a small POD: the copy is a plain
 * assignment, and after init it is only read.
 */
static PinkdbHostApi g_api;

/* ------------------------------------------------------------------------ */
/* Scalar callbacks                                                          */
/* ------------------------------------------------------------------------ */

/*
 * my_ext_greet(name VARCHAR) -> VARCHAR
 *
 * Returns "Hello, <name>!" for a non-NULL input, NULL for NULL input.
 *
 * This callback demonstrates the full VARCHAR ownership dance:
 *   - `args[0].text_ptr` is HOST-owned input: read it, never free it, and
 *     treat it as valid only for the duration of this call (copy if kept);
 *   - the result buffer comes from `g_api.alloc`, is filled here, and is
 *     freed by the host right after this callback returns;
 *   - `text_len` is the exact byte count (no trailing NUL is required or
 *     read — we NUL-terminate anyway because it is free and aids debugging).
 *
 * Other tags carry their value in a different field of `pinkdb_ext_value`
 * (`int64` for BOOLEAN/INTEGER/BIGINT/DATE/TIMESTAMP, `double_` for DOUBLE,
 * both for INTERVAL); `type_tag` must match what init registered or the host
 * aborts the statement.
 */
static int32_t my_ext_greet(void *user_data, const pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out) {
    (void)user_data; /* the NULL `user_data` passed at registration */
    (void)n_args;    /* exactly the registered arity (1) */

    /* NULL in -> NULL out. The callback is always called; it decides. */
    if (args[0].is_null) {
        out->is_null = 1;
        return 0; /* `out` is pre-filled with NULL; nothing else to set */
    }

    /* "Hello, " (7) + name + "!" (1), plus one byte for a trailing NUL. */
    const size_t prefix = 7, suffix = 1;
    size_t len = args[0].text_len;
    size_t total = prefix + len + suffix;
    char *buf = (char *)g_api.alloc(total + 1);
    if (buf == NULL) {
        return 1; /* allocation failure: non-zero = statement error */
    }
    memcpy(buf, "Hello, ", prefix);
    memcpy(buf + prefix, args[0].text_ptr, len);
    buf[prefix + len] = '!';
    buf[total] = '\0';

    /* Declare the result: tag + non-NULL + borrowed-until-return bytes. */
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;   /* must be g_api.alloc memory (see rule 1) */
    out->text_len = total; /* bytes actually used; the NUL is not counted */
    return 0;              /* 0 = success */
}

/* The counter behind my_ext_tick(). */
static int64_t g_counter;

/*
 * my_ext_tick() -> BIGINT
 *
 * Volatile: every call returns a new value. The counter is atomic because
 * callbacks run concurrently. The engine permits this in read-only queries
 * and rejects it inside any statement it WAL-logs; that is the point of the
 * flag, so do not reach for it for values that are merely slow.
 */
static int32_t my_ext_tick(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args; /* arity 0: the host passes args == NULL */
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = (int64_t)__atomic_add_fetch(&g_counter, 1, __ATOMIC_RELAXED);
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Optional serve entry point (ABI minor 2)                                  */
/* ------------------------------------------------------------------------ */

/*
 * A host that runs this extension as an MCP server (`pinkdb-server
 * --mcp-extension`) looks this symbol up after init and calls it instead of
 * returning to SQL. Serve blocks until the client disconnects, then returns
 * 0. It must use `api->execute_statement` for every SQL statement — the
 * extension never sees the engine's session directly.
 *
 * This skeleton implements the `"stdio"` transport with one SQL statement per
 * line, which is enough to see the shape of it. Omit the function entirely if
 * your extension has nothing to serve; the symbol is optional.
 *
 * `execute_statement` returns:
 *   0  success — `*out_text` / `*out_len` hold a Markdown result (or are
 *      NULL/0 when the statement produced no output);
 *   1  the statement failed — the text is a JSON error object;
 *   2  there is no live session (during init, or after shutdown).
 * The returned buffer is malloc'd: copy or print it, then `free` it. It is
 * not NUL-terminated.
 */
int32_t pinkdb_ext_serve(const PinkdbHostApi *api, void *host_ctx,
                         const char *transport, const char *options) {
    (void)options;
    if (api == NULL || !PINKDB_EXT_HOST_HAS_EXECUTE(api)) {
        return 1; /* old host: no execute_statement */
    }
    if (transport == NULL || strcmp(transport, "stdio") != 0) {
        return 2; /* unknown transport */
    }

    char line[8192];
    while (fgets(line, sizeof line, stdin) != NULL) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        char *text = NULL;
        size_t text_len = 0;
        int32_t code = api->execute_statement(host_ctx, line, len, &text,
                                              &text_len);
        if (text != NULL) {
            fwrite(text, 1, text_len, stdout);
            fputc('\n', stdout);
            free(text);
        }
        fflush(stdout);
        if (code == 2) {
            return 0; /* the session went away: normal shutdown */
        }
    }
    return 0;
}

/* ------------------------------------------------------------------------ */
/* ABI entry points (the loader looks these up by name)                      */
/* ------------------------------------------------------------------------ */

/* Must return PINKDB_EXT_ABI_VERSION; any other value aborts the load. */
uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

/* NUL-terminated UTF-8, non-empty, at most 256 bytes; copied by the host. */
const char *pinkdb_extension_name(void) {
    return "my_extension";
}

/*
 * Called exactly once, on the loading thread, after the version/name checks.
 * Register every function here. Return 0 on success; any non-zero code aborts
 * the load with an error naming the code. A rejected registration also fails
 * the load (even if init returns 0), so check every return value.
 */
int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    /* Defensive checks: a newer/older host must be refused explicitly. */
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    /* `struct_size` is how a host announces which fields it has: never read
     * a field past it. PINKDB_EXT_API_V1_SIZE covers everything up to the
     * appended flags entry point — the oldest layout this SDK supports. */
    if (api->struct_size < PINKDB_EXT_API_V1_SIZE) {
        return 2; /* host predates this header's struct — too old */
    }

    /* Keep the services for the callbacks (see the comment on `g_api`). */
    pinkdb_ext_copy_api(&g_api, api);

    /*
     * One registered function. `arg_types` must point to `n_args` non-NULL
     * type tags and may be NULL when `n_args` is 0; the host copies both the
     * name and the tags during this call, so a local array is fine.
     * `host_ctx` is opaque: pass it back to `register_scalar` unchanged.
     */
    static const int32_t arg_types[1] = {PINKDB_EXT_TYPE_VARCHAR};
    if (api->register_scalar(host_ctx, "my_ext_greet", 1, arg_types,
                             PINKDB_EXT_TYPE_VARCHAR, my_ext_greet, NULL) != 0) {
        return 10; /* the name collides, or the registration was invalid */
    }

    /*
     * The flags entry point was appended after the v1 fields, so it exists
     * only when the host's struct is long enough (PINKDB_EXT_HOST_HAS_FLAGS).
     * A volatile function that cannot be declared must fail the load: falling
     * back to `register_scalar` would make a nondeterministic function look
     * deterministic to WAL replay.
     */
    if (!PINKDB_EXT_HOST_HAS_FLAGS(api)) {
        return 3;
    }
    if (api->register_scalar_flags(host_ctx, "my_ext_tick", 0, NULL,
                                   PINKDB_EXT_TYPE_BIGINT,
                                   PINKDB_EXT_FLAG_VOLATILE, my_ext_tick,
                                   NULL) != 0) {
        return 11;
    }

    return 0;
}
