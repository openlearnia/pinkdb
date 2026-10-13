/*
 * pinkdb extension skeleton (ABI v1) — copy this directory and rename.
 *
 * This file is a complete, buildable extension that registers one scalar
 * function:
 *
 *     my_ext_greet(name VARCHAR) -> VARCHAR      -- "Hello, <name>!"
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
 * The contract this file follows is documented in `../include/pinkdb_extension.h`
 * — read it once before writing real code. The three rules that bite most:
 *
 *   1. VARCHAR results must be allocated with `api->alloc`, never malloc/new;
 *      the host copies `text_len` bytes and calls `api->free` immediately
 *      after the callback returns. A non-NULL `text_ptr` is always freed by
 *      the host (even on an error return), so never return a stack, static,
 *      or interior pointer.
 *   2. Callbacks run CONCURRENTLY on the host's rayon worker threads. Every
 *      piece of mutable state must be atomic/locked/thread-local. `alloc` and
 *      `free` are thread-safe; `register_scalar` may only be called from
 *      `pinkdb_extension_init`.
 *   3. Never panic, throw, or unwind across the ABI boundary — the host calls
 *      you through an `extern "C"` edge where unwinding is undefined
 *      behavior. Catch/avoid every error internally and return a non-zero
 *      code (it aborts the statement with an error naming the function).
 */

#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>
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
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2; /* host predates this header's struct — too old */
    }

    /* Keep the services for the callbacks (see the comment on `g_api`). */
    g_api = *api;

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

    return 0;
}
