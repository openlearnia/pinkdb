/*
 * The ABI conformance fixture, C SDK edition.
 *
 * One contract (`ext-sdk/conformance/README.md`), implemented once per SDK and
 * driven by `crates/pinkdb-ext-loader/tests/conformance.rs`, so the four SDKs
 * are measured against the same behaviour instead of against their own
 * examples. Keep this file boring: it exists to prove the ABI, not to be read
 * as a tutorial (the template and the examples do that).
 *
 * Capability word = 1: `cf_fail` really allocates before it fails (bit 0).
 * Bit 1 is clear — C has no unwinding, so `cf_unwind` returns code 2 instead
 * of panicking.
 */

#include "pinkdb_extension.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Copied out of the init-scoped struct: callbacks run later, on other
 * threads, and need `alloc`/`free` (see the header). */
static PinkdbHostApi g_api;

/* Introspection counters (read by the loader tests, not part of the ABI). */
/* Atomic: callbacks arrive concurrently on several threads, and a plain
 * increment loses updates (the foreign-thread test counted 68 of 100). */
static _Atomic int64_t g_calls;
static _Atomic int64_t g_foreign_threads;
static pthread_t g_init_thread;

static void note_call(void) {
    atomic_fetch_add_explicit(&g_calls, 1, memory_order_relaxed);
    if (!pthread_equal(pthread_self(), g_init_thread)) {
        atomic_fetch_add_explicit(&g_foreign_threads, 1, memory_order_relaxed);
    }
}

/* ------------------------------------------------------------------------ */
/* Helpers                                                                   */
/* ------------------------------------------------------------------------ */

/* A small growable buffer writing into host-allocated memory. */
typedef struct {
    char *ptr;
    size_t len;
    size_t cap;
    int failed;
} sbuf;

static void sbuf_init(sbuf *b) {
    b->ptr = NULL;
    b->len = 0;
    b->cap = 0;
    b->failed = 0;
}

static void sbuf_reserve(sbuf *b, size_t extra) {
    if (b->failed) {
        return;
    }
    size_t need = b->len + extra + 1;
    if (need <= b->cap) {
        return;
    }
    size_t cap = b->cap ? b->cap : 64;
    while (cap < need) {
        cap *= 2;
    }
    char *grown = (char *)g_api.alloc(cap);
    if (grown == NULL) {
        b->failed = 1;
        return;
    }
    if (b->ptr != NULL) {
        memcpy(grown, b->ptr, b->len);
        g_api.free(b->ptr);
    }
    b->ptr = grown;
    b->cap = cap;
}

static void sbuf_bytes(sbuf *b, const char *bytes, size_t len) {
    sbuf_reserve(b, len);
    if (b->failed) {
        return;
    }
    memcpy(b->ptr + b->len, bytes, len);
    b->len += len;
    b->ptr[b->len] = '\0';
}

static void sbuf_str(sbuf *b, const char *text) {
    sbuf_bytes(b, text, strlen(text));
}

static void sbuf_i64(sbuf *b, int64_t value) {
    char digits[32];
    int n = snprintf(digits, sizeof digits, "%lld", (long long)value);
    if (n > 0) {
        sbuf_bytes(b, digits, (size_t)n);
    }
}

/* Hand a VARCHAR result to the host: it copies `text_len` bytes and then
 * frees the buffer with `api->free`. Ownership moves on success only. */
static int32_t publish_varchar(sbuf *b, pinkdb_ext_value *out) {
    if (b->failed) {
        if (b->ptr != NULL) {
            g_api.free(b->ptr);
        }
        return 1;
    }
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = b->ptr;
    out->text_len = b->len;
    return 0;
}

static uint64_t double_bits(double value) {
    uint64_t bits;
    memcpy(&bits, &value, sizeof bits);
    return bits;
}

/* ------------------------------------------------------------------------ */
/* Identity functions (one per tag)                                          */
/* ------------------------------------------------------------------------ */

#define IDENTITY(name, tag, payload)                                          \
    static int32_t name(void *user_data, const pinkdb_ext_value *args,        \
                        size_t n_args, pinkdb_ext_value *out) {                \
        (void)user_data;                                                      \
        (void)n_args;                                                         \
        note_call();                                                          \
        if (args[0].is_null) {                                                \
            return 0; /* host pre-filled `out` with NULL */                   \
        }                                                                     \
        out->type_tag = (tag);                                                \
        out->is_null = 0;                                                     \
        out->payload = args[0].payload;                                       \
        return 0;                                                             \
    }

IDENTITY(cf_bool, PINKDB_EXT_TYPE_BOOLEAN, int64)
IDENTITY(cf_int, PINKDB_EXT_TYPE_INTEGER, int64)
IDENTITY(cf_bigint, PINKDB_EXT_TYPE_BIGINT, int64)
IDENTITY(cf_double, PINKDB_EXT_TYPE_DOUBLE, double_)
IDENTITY(cf_date, PINKDB_EXT_TYPE_DATE, int64)
IDENTITY(cf_timestamp, PINKDB_EXT_TYPE_TIMESTAMP, int64)

/* INTERVAL moves two payloads: micros in `int64`, months/days in the bits of
 * `double_` (see the header). */
static int32_t cf_interval(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    note_call();
    if (args[0].is_null) {
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTERVAL;
    out->is_null = 0;
    out->int64 = args[0].int64;
    out->double_ = args[0].double_;
    return 0;
}

/* VARIANT rides the same text channel as VARCHAR: an opaque pinkdb-encoded
 * payload, copied into host memory because the input is valid only during
 * this call. */
static int32_t cf_variant(void *user_data, const pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    note_call();
    if (args[0].is_null) {
        return 0;
    }
    size_t len = args[0].text_len;
    char *buf = (char *)g_api.alloc(len == 0 ? 1 : len);
    if (buf == NULL) {
        return 1;
    }
    if (len != 0) {
        memcpy(buf, args[0].text_ptr, len);
    }
    out->type_tag = PINKDB_EXT_TYPE_VARIANT;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = len;
    return 0;
}

/* VARCHAR is the one tag whose payload is a pointer: copy the bytes into
 * host memory, because the input is valid only during this call. */
static int32_t cf_echo(void *user_data, const pinkdb_ext_value *args,
                       size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    note_call();
    if (args[0].is_null) {
        return 0;
    }
    size_t len = args[0].text_len;
    char *buf = (char *)g_api.alloc(len + 1);
    if (buf == NULL) {
        return 1;
    }
    if (len != 0) {
        memcpy(buf, args[0].text_ptr, len);
    }
    buf[len] = '\0';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = len;
    return 0;
}

/* ------------------------------------------------------------------------ */
/* The canonical summary                                                     */
/* ------------------------------------------------------------------------ */

static void field_bool(sbuf *b, const pinkdb_ext_value *v) {
    if (v->is_null) {
        sbuf_str(b, "null");
    } else {
        sbuf_str(b, v->int64 != 0 ? "1" : "0");
    }
}

static void field_int(sbuf *b, const pinkdb_ext_value *v) {
    if (v->is_null) {
        sbuf_str(b, "null");
    } else {
        sbuf_i64(b, v->int64);
    }
}

static void field_double(sbuf *b, const pinkdb_ext_value *v) {
    if (v->is_null) {
        sbuf_str(b, "null");
        return;
    }
    char hex[32];
    int n = snprintf(hex, sizeof hex, "0x%016llx",
                     (unsigned long long)double_bits(v->double_));
    if (n > 0) {
        sbuf_bytes(b, hex, (size_t)n);
    }
}

static void field_varchar(sbuf *b, const pinkdb_ext_value *v) {
    if (v->is_null) {
        sbuf_str(b, "null");
    } else {
        sbuf_bytes(b, v->text_ptr, v->text_len);
    }
}

static void field_interval(sbuf *b, const pinkdb_ext_value *v) {
    if (v->is_null) {
        sbuf_str(b, "null");
        return;
    }
    uint64_t bits = double_bits(v->double_);
    sbuf_i64(b, (int64_t)(int32_t)(uint32_t)(bits >> 32)); /* months */
    sbuf_str(b, "/");
    sbuf_i64(b, (int64_t)(int32_t)(uint32_t)bits); /* days */
    sbuf_str(b, "/");
    sbuf_i64(b, v->int64); /* micros */
}

static int32_t cf_all(void *user_data, const pinkdb_ext_value *args,
                      size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    note_call();

    sbuf b;
    sbuf_init(&b);
    sbuf_str(&b, "b=");
    field_bool(&b, &args[0]);
    sbuf_str(&b, ",i=");
    field_int(&b, &args[1]);
    sbuf_str(&b, ",big=");
    field_int(&b, &args[2]);
    sbuf_str(&b, ",d=");
    field_double(&b, &args[3]);
    sbuf_str(&b, ",v=");
    field_varchar(&b, &args[4]);
    sbuf_str(&b, ",date=");
    field_int(&b, &args[5]);
    sbuf_str(&b, ",ts=");
    field_int(&b, &args[6]);
    sbuf_str(&b, ",iv=");
    field_interval(&b, &args[7]);
    return publish_varchar(&b, out);
}

/* ------------------------------------------------------------------------ */
/* Edge cases                                                                */
/* ------------------------------------------------------------------------ */

/* A zero-length, non-NULL VARCHAR: `text_len = 0`, pointer still allocated so
 * the host has something valid to free. */
static int32_t cf_empty(void *user_data, const pinkdb_ext_value *args,
                        size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    (void)args; /* arity 0: the host passes args == NULL */
    note_call();
    char *buf = (char *)g_api.alloc(1);
    if (buf == NULL) {
        return 1;
    }
    buf[0] = '\0';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = 0;
    return 0;
}

/* n bytes of 'x'; negative -> NULL; past the host's 64 MiB VARCHAR limit is
 * an error (the host would reject it anyway, with a worse message). */
static int32_t cf_fill(void *user_data, const pinkdb_ext_value *args,
                       size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    note_call();
    if (args[0].is_null || args[0].int64 < 0) {
        return 0;
    }
    int64_t count = args[0].int64;
    if (count > 64 * 1024 * 1024) {
        return 1;
    }
    size_t len = (size_t)count;
    char *buf = (char *)g_api.alloc(len + 1);
    if (buf == NULL) {
        return 1;
    }
    memset(buf, 'x', len);
    buf[len] = '\0';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = len;
    return 0;
}

/* Publish allocated text, then fail: the host must free the buffer on the
 * error path. Capability bit 0. */
static int32_t cf_fail(void *user_data, const pinkdb_ext_value *args,
                       size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    (void)args;
    note_call();
    char *buf = (char *)g_api.alloc(4);
    if (buf == NULL) {
        return 1;
    }
    memcpy(buf, "oops", 4);
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = 4;
    return 1; /* the host frees the text, then aborts with code 1 */
}

/* No unwinding in C: the "SDK's worst case" is a plain error code. */
static int32_t cf_unwind(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    (void)out;
    note_call();
    return 2;
}

static int32_t cf_volatile(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    note_call();
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = 42;
    return 0;
}

static int32_t cf_thread(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    note_call(); /* also counts this call's thread */
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = 1;
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Introspection (opened by the loader tests, not part of the ABI)           */
/* ------------------------------------------------------------------------ */

int64_t pinkdb_conformance_calls(void) {
    return atomic_load_explicit(&g_calls, memory_order_relaxed);
}

int64_t pinkdb_conformance_foreign_threads(void) {
    return atomic_load_explicit(&g_foreign_threads, memory_order_relaxed);
}

int64_t pinkdb_conformance_caps(void) {
    return 1; /* bit 0: cf_fail allocates; bit 1 clear: no unwinding in C */
}

/* ------------------------------------------------------------------------ */
/* ABI entry points                                                          */
/* ------------------------------------------------------------------------ */

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "conformance_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    /* The smallest struct a 1.2 SDK can work with: everything up to (not
     * including) the appended fields. A host below this predates the ABI the
     * SDK was written against. */
    if (api->struct_size < PINKDB_EXT_API_V1_SIZE) {
        return 2;
    }
    pinkdb_ext_copy_api(&g_api, api);
    g_init_thread = pthread_self();

    /* `cf_volatile` needs the appended flags entry point. Reading it from a
     * shorter struct would read past the host's allocation, so refuse an old
     * host instead of silently registering the function as deterministic. */
    if (!PINKDB_EXT_HOST_HAS_FLAGS(api)) {
        return 3;
    }

    static const int32_t t_bool[1] = {PINKDB_EXT_TYPE_BOOLEAN};
    static const int32_t t_int[1] = {PINKDB_EXT_TYPE_INTEGER};
    static const int32_t t_bigint[1] = {PINKDB_EXT_TYPE_BIGINT};
    static const int32_t t_double[1] = {PINKDB_EXT_TYPE_DOUBLE};
    static const int32_t t_varchar[1] = {PINKDB_EXT_TYPE_VARCHAR};
    static const int32_t t_date[1] = {PINKDB_EXT_TYPE_DATE};
    static const int32_t t_timestamp[1] = {PINKDB_EXT_TYPE_TIMESTAMP};
    static const int32_t t_interval[1] = {PINKDB_EXT_TYPE_INTERVAL};
    static const int32_t t_variant[1] = {PINKDB_EXT_TYPE_VARIANT};
    static const int32_t t_all[8] = {
        PINKDB_EXT_TYPE_BOOLEAN, PINKDB_EXT_TYPE_INTEGER,
        PINKDB_EXT_TYPE_BIGINT,  PINKDB_EXT_TYPE_DOUBLE,
        PINKDB_EXT_TYPE_VARCHAR, PINKDB_EXT_TYPE_DATE,
        PINKDB_EXT_TYPE_TIMESTAMP, PINKDB_EXT_TYPE_INTERVAL,
    };

    if (api->register_scalar(host_ctx, "cf_echo", 1, t_varchar,
                             PINKDB_EXT_TYPE_VARCHAR, cf_echo, NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "cf_bool", 1, t_bool,
                             PINKDB_EXT_TYPE_BOOLEAN, cf_bool, NULL) != 0) {
        return 11;
    }
    if (api->register_scalar(host_ctx, "cf_int", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, cf_int, NULL) != 0) {
        return 12;
    }
    if (api->register_scalar(host_ctx, "cf_bigint", 1, t_bigint,
                             PINKDB_EXT_TYPE_BIGINT, cf_bigint, NULL) != 0) {
        return 13;
    }
    if (api->register_scalar(host_ctx, "cf_double", 1, t_double,
                             PINKDB_EXT_TYPE_DOUBLE, cf_double, NULL) != 0) {
        return 14;
    }
    if (api->register_scalar(host_ctx, "cf_date", 1, t_date,
                             PINKDB_EXT_TYPE_DATE, cf_date, NULL) != 0) {
        return 15;
    }
    if (api->register_scalar(host_ctx, "cf_timestamp", 1, t_timestamp,
                             PINKDB_EXT_TYPE_TIMESTAMP, cf_timestamp, NULL) != 0) {
        return 16;
    }
    if (api->register_scalar(host_ctx, "cf_interval", 1, t_interval,
                             PINKDB_EXT_TYPE_INTERVAL, cf_interval, NULL) != 0) {
        return 17;
    }
    if (api->register_scalar(host_ctx, "cf_variant", 1, t_variant,
                             PINKDB_EXT_TYPE_VARIANT, cf_variant, NULL) != 0) {
        return 23;
    }
    if (api->register_scalar(host_ctx, "cf_all", 8, t_all,
                             PINKDB_EXT_TYPE_VARCHAR, cf_all, NULL) != 0) {
        return 18;
    }
    if (api->register_scalar(host_ctx, "cf_empty", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, cf_empty, NULL) != 0) {
        return 19;
    }
    if (api->register_scalar(host_ctx, "cf_fill", 1, t_bigint,
                             PINKDB_EXT_TYPE_VARCHAR, cf_fill, NULL) != 0) {
        return 20;
    }
    if (api->register_scalar(host_ctx, "cf_fail", 1, t_bigint,
                             PINKDB_EXT_TYPE_BIGINT, cf_fail, NULL) != 0) {
        return 21;
    }
    if (api->register_scalar(host_ctx, "cf_unwind", 0, NULL,
                             PINKDB_EXT_TYPE_BIGINT, cf_unwind, NULL) != 0) {
        return 22;
    }
    if (api->register_scalar_flags(host_ctx, "cf_volatile", 0, NULL,
                                   PINKDB_EXT_TYPE_BIGINT,
                                   PINKDB_EXT_FLAG_VOLATILE, cf_volatile,
                                   NULL) != 0) {
        return 23;
    }
    if (api->register_scalar(host_ctx, "cf_thread", 0, NULL,
                             PINKDB_EXT_TYPE_BIGINT, cf_thread, NULL) != 0) {
        return 24;
    }
    return 0;
}
