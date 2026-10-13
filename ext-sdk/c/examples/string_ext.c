/*
 * pinkdb extension example — string_ext (ABI v1, plain C99).
 *
 * Registers two VARCHAR-returning scalar functions:
 *
 *     ext_repeat(s VARCHAR, n INTEGER) -> VARCHAR   -- s repeated n times
 *     ext_title(s VARCHAR)             -> VARCHAR   -- ASCII title case
 *
 * Both demonstrate the ABI's result-text ownership: the bytes are allocated
 * with `api->alloc`, and the host copies `text_len` bytes and frees the
 * buffer with `api->free` immediately after the callback returns. VARCHAR
 * *inputs* are host-owned and must never be freed. NULL inputs yield NULL.
 *
 * Both callbacks are reentrant: the only shared state is the copied
 * `PinkdbHostApi` struct, written once in init and read-only afterwards;
 * `alloc` is thread-safe (callbacks run concurrently on host workers).
 */

#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* The header documents the API struct as valid only during init; callbacks
 * need `alloc`, so copy it once (see the template for the same pattern). */
static PinkdbHostApi g_api;

/* ASCII-only case helpers: locale-independent and UTF-8-byte-safe (bytes
 * outside [A-Za-z] pass through untouched). */
static char ascii_upper(char c) {
    return (c >= 'a' && c <= 'z') ? (char)(c - ('a' - 'A')) : c;
}

static char ascii_lower(char c) {
    return (c >= 'A' && c <= 'Z') ? (char)(c + ('a' - 'A')) : c;
}

static int is_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

/* ------------------------------------------------------------------------ */
/* Callbacks                                                                 */
/* ------------------------------------------------------------------------ */

/* ext_repeat(s, n): s concatenated n times. n <= 0 -> empty string. */
static int32_t ext_repeat(void *user_data, const pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;

    /* NULL in -> NULL out. (is_null is the only meaningful field then.) */
    if (args[0].is_null || args[1].is_null) {
        out->is_null = 1;
        return 0;
    }

    size_t len = args[0].text_len;      /* input bytes, valid for this call */
    int64_t n = args[1].int64;          /* INTEGER argument (i32 payload) */

    /* An empty result is still a non-NULL VARCHAR: allocate one byte for the
     * terminator so the host has a valid `alloc` pointer to free. */
    size_t total = 0;
    if (n > 0) {
        /* Guard against size_t overflow before allocating: a huge repeat
         * count is an error, not a crash. */
        if (len != 0 && (size_t)n > (SIZE_MAX - 1) / len) {
            return 2;
        }
        total = len * (size_t)n;
    }

    char *buf = (char *)g_api.alloc(total + 1);
    if (buf == NULL) {
        return 1; /* out of memory: statement error */
    }
    if (n > 0) {
        for (size_t i = 0; i < (size_t)n; i++) {
            memcpy(buf + i * len, args[0].text_ptr, len);
        }
    }
    buf[total] = '\0'; /* not required by the ABI, convenient for debugging */

    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;   /* host-owned from here on; freed via api->free */
    out->text_len = total; /* exact byte count; the NUL is not part of it */
    return 0;
}

/* ext_title(s): ASCII title case — first letter of each word upper, rest
 * lower; non-ASCII bytes pass through. */
static int32_t ext_title(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;

    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }

    /* Title case is a same-length transform: copy into alloc'd memory. */
    size_t len = args[0].text_len;
    char *buf = (char *)g_api.alloc(len + 1);
    if (buf == NULL) {
        return 1;
    }

    int at_word_start = 1;
    for (size_t i = 0; i < len; i++) {
        char c = args[0].text_ptr[i];
        if (is_space(c)) {
            at_word_start = 1;
            buf[i] = c;
        } else if (at_word_start) {
            buf[i] = ascii_upper(c);
            at_word_start = 0;
        } else {
            buf[i] = ascii_lower(c);
        }
    }
    buf[len] = '\0';

    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = len;
    return 0;
}

/* ------------------------------------------------------------------------ */
/* ABI entry points                                                          */
/* ------------------------------------------------------------------------ */

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "string_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2;
    }
    g_api = *api;

    static const int32_t t_repeat[2] = {PINKDB_EXT_TYPE_VARCHAR, PINKDB_EXT_TYPE_INTEGER};
    static const int32_t t_title[1] = {PINKDB_EXT_TYPE_VARCHAR};

    if (api->register_scalar(host_ctx, "ext_repeat", 2, t_repeat,
                             PINKDB_EXT_TYPE_VARCHAR, ext_repeat, NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "ext_title", 1, t_title,
                             PINKDB_EXT_TYPE_VARCHAR, ext_title, NULL) != 0) {
        return 11;
    }
    return 0;
}
