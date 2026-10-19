/*
 * pinkdb extension ABI v1 — test extension for `pinkdb-ext-loader`.
 *
 * Compiled at test time with the system `cc` (no build dependency) against
 * `crates/pinkdb-ext-loader/include/pinkdb_extension.h`, loaded by the Rust
 * integration tests. Besides the three ABI entry points it exports a few
 * `pinkdb_test_*` introspection symbols the Rust side reads directly with
 * dlopen (`libloading`) to assert on call counts and thread use.
 *
 * The extension is deliberately thread-safe: counters use compiler atomic
 * builtins, the thread registry is guarded by an atomic spinlock, and every
 * callback is reentrant (see the THREAD SAFETY section of the header).
 * POSIX only (pthreads); the ABI itself is portable.
 */
#include "pinkdb_extension.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Copied out of init: the header documents the struct as init-scoped. */
static PinkdbHostApi g_api;

/* ------------------------------------------------------------------ */
/* Introspection state (not part of the ABI).                          */
/* ------------------------------------------------------------------ */

/* Thread identity: POSIX thread ids, captured at call time. (The address of
 * a thread-local is NOT usable here: in a dlopen'ed image on Darwin two live
 * threads can observe the same TLV address — measured, hence pthreads.) */
#define MAX_THREADS 64

static uintptr_t g_thread_ids[MAX_THREADS];
static int g_thread_count;
static int g_thread_lock;

static long long g_ticks;
static long long g_probe_calls;
static long long g_tap_calls;
static long long g_shout_allocs;
static long long g_fail_calls;

static void record_thread(void) {
    uintptr_t id = (uintptr_t)pthread_self();
    while (__atomic_exchange_n(&g_thread_lock, 1, __ATOMIC_ACQ_REL)) {
        /* spin: the critical section is a handful of instructions */
    }
    int found = 0;
    for (int i = 0; i < g_thread_count; i++) {
        if (g_thread_ids[i] == id) {
            found = 1;
            break;
        }
    }
    if (!found && g_thread_count < MAX_THREADS) {
        g_thread_ids[g_thread_count++] = id;
    }
    __atomic_store_n(&g_thread_lock, 0, __ATOMIC_RELEASE);
}

int32_t pinkdb_test_distinct_threads(void) {
    return __atomic_load_n(&g_thread_count, __ATOMIC_RELAXED);
}

int64_t pinkdb_test_probe_calls(void) {
    return __atomic_load_n(&g_probe_calls, __ATOMIC_RELAXED);
}

int64_t pinkdb_test_tap_calls(void) {
    return __atomic_load_n(&g_tap_calls, __ATOMIC_RELAXED);
}

int64_t pinkdb_test_ticks(void) {
    return __atomic_load_n(&g_ticks, __ATOMIC_RELAXED);
}

int64_t pinkdb_test_shout_allocs(void) {
    return __atomic_load_n(&g_shout_allocs, __ATOMIC_RELAXED);
}

int64_t pinkdb_test_fail_calls(void) {
    return __atomic_load_n(&g_fail_calls, __ATOMIC_RELAXED);
}

/* ------------------------------------------------------------------ */
/* Callbacks.                                                          */
/* ------------------------------------------------------------------ */

/* ext_double_it(a INTEGER) -> INTEGER */
static int32_t ext_double_it(void *user_data, const pinkdb_ext_value *args,
                             size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = args[0].int64 * 2;
    return 0;
}

/* ext_shout(s VARCHAR) -> VARCHAR: ASCII uppercase, host-owned text. */
static int32_t ext_shout(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    size_t len = args[0].text_len;
    char *buf = (char *)g_api.alloc(len + 1);
    if (buf == NULL) {
        return 1;
    }
    __atomic_add_fetch(&g_shout_allocs, 1, __ATOMIC_RELAXED);
    for (size_t i = 0; i < len; i++) {
        char c = args[0].text_ptr[i];
        buf[i] = (c >= 'a' && c <= 'z') ? (char)(c - ('a' - 'A')) : c;
    }
    buf[len] = '\0';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = len;
    return 0;
}

/* ext_concat2(a VARCHAR, b VARCHAR) -> VARCHAR */
static int32_t ext_concat2(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null || args[1].is_null) {
        out->is_null = 1;
        return 0;
    }
    size_t la = args[0].text_len;
    size_t lb = args[1].text_len;
    char *buf = (char *)g_api.alloc(la + lb + 1);
    if (buf == NULL) {
        return 1;
    }
    memcpy(buf, args[0].text_ptr, la);
    memcpy(buf + la, args[1].text_ptr, lb);
    buf[la + lb] = '\0';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = la + lb;
    return 0;
}

/* ext_tick() -> INTEGER: 1, 2, 3, ... — a fresh value per call, so a
 * constant-folded call would show up as repeated values in a projection.
 * Also pins the ABI promise that a zero-argument callback gets a NULL args
 * array. */
static int32_t ext_tick(void *user_data, const pinkdb_ext_value *args,
                        size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    if (n_args != 0 || args != NULL) {
        return 1;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = __atomic_add_fetch(&g_ticks, 1, __ATOMIC_RELAXED);
    return 0;
}

/* ext_probe(a INTEGER) -> INTEGER: identity; records thread + call count. */
static int32_t ext_probe(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    record_thread();
    __atomic_add_fetch(&g_probe_calls, 1, __ATOMIC_RELAXED);
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = args[0].int64;
    return 0;
}

/* ext_tap(a INTEGER) -> INTEGER: identity; records thread + call count.
 * Separate from ext_probe so tests can assert exact call deltas without
 * interfering with each other. */
static int32_t ext_tap(void *user_data, const pinkdb_ext_value *args,
                       size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    record_thread();
    __atomic_add_fetch(&g_tap_calls, 1, __ATOMIC_RELAXED);
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = args[0].int64;
    return 0;
}

/* ext_fail_on_negative(a INTEGER) -> INTEGER: non-zero return codes must
 * surface as SQL errors (never as NULL). Counts calls so tests can pin that
 * an erroring constant call is evaluated exactly once. */
static int32_t ext_fail_on_negative(void *user_data,
                                    const pinkdb_ext_value *args, size_t n_args,
                                    pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    __atomic_add_fetch(&g_fail_calls, 1, __ATOMIC_RELAXED);
    if (!args[0].is_null && args[0].int64 < 0) {
        return 7;
    }
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = args[0].int64;
    return 0;
}

/* ext_is_even(a INTEGER) -> BOOLEAN */
static int32_t ext_is_even(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_BOOLEAN;
    out->is_null = 0;
    out->int64 = (args[0].int64 % 2) == 0;
    return 0;
}

/* ext_halve(a DOUBLE) -> DOUBLE */
static int32_t ext_halve(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_DOUBLE;
    out->is_null = 0;
    out->double_ = args[0].double_ / 2.0;
    return 0;
}

/* ext_plus_day(d DATE) -> DATE */
static int32_t ext_plus_day(void *user_data, const pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_DATE;
    out->is_null = 0;
    out->int64 = args[0].int64 + 1;
    return 0;
}

/* ext_micros(t TIMESTAMP) -> BIGINT: raw microseconds, identity. */
static int32_t ext_micros(void *user_data, const pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = args[0].int64;
    return 0;
}

/* ext_interval_pack(months INTEGER, days INTEGER, micros BIGINT) -> INTERVAL.
 *
 * The header's INTERVAL payload: micros in `int64`, months/days in the bits
 * of `double_`. This is exactly how a third-party C extension is meant to
 * build the value. */
static int32_t ext_interval_pack(void *user_data,
                                 const pinkdb_ext_value *args, size_t n_args,
                                 pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null || args[1].is_null || args[2].is_null) {
        out->is_null = 1;
        return 0;
    }
    uint64_t bits = ((uint64_t)(uint32_t)args[0].int64 << 32) |
                    (uint32_t)args[1].int64;
    double packed;
    memcpy(&packed, &bits, sizeof(bits));
    out->type_tag = PINKDB_EXT_TYPE_INTERVAL;
    out->is_null = 0;
    out->int64 = args[2].int64;
    out->double_ = packed;
    return 0;
}

/* ext_interval_fields(iv INTERVAL) -> VARCHAR "months|days|micros". */
static int32_t ext_interval_fields(void *user_data,
                                   const pinkdb_ext_value *args, size_t n_args,
                                   pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    uint64_t bits;
    memcpy(&bits, &args[0].double_, sizeof(bits));
    long long months = (int32_t)(uint32_t)(bits >> 32);
    long long days = (int32_t)(uint32_t)bits;
    long long micros = args[0].int64;
    char buf[96];
    int len = snprintf(buf, sizeof(buf), "%lld|%lld|%lld", months, days, micros);
    if (len < 0 || (size_t)len >= sizeof(buf)) {
        return 1;
    }
    char *text = (char *)g_api.alloc((size_t)len);
    if (text == NULL) {
        return 1;
    }
    memcpy(text, buf, (size_t)len);
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = text;
    out->text_len = (size_t)len;
    return 0;
}

/* ------------------------------------------------------------------ */
/* ABI entry points.                                                   */
/* ------------------------------------------------------------------ */

static const char EXT_NAME[] = "test_ext";

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return EXT_NAME;
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2;
    }
    g_api = *api;

    const int32_t t_int[1] = {PINKDB_EXT_TYPE_INTEGER};
    const int32_t t_str[1] = {PINKDB_EXT_TYPE_VARCHAR};
    const int32_t t_str2[2] = {PINKDB_EXT_TYPE_VARCHAR, PINKDB_EXT_TYPE_VARCHAR};
    const int32_t t_double[1] = {PINKDB_EXT_TYPE_DOUBLE};
    const int32_t t_date[1] = {PINKDB_EXT_TYPE_DATE};
    const int32_t t_ts[1] = {PINKDB_EXT_TYPE_TIMESTAMP};
    const int32_t t_pack[3] = {PINKDB_EXT_TYPE_INTEGER, PINKDB_EXT_TYPE_INTEGER,
                               PINKDB_EXT_TYPE_BIGINT};
    const int32_t t_iv[1] = {PINKDB_EXT_TYPE_INTERVAL};

    if (api->register_scalar(host_ctx, "ext_double_it", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext_double_it, NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "ext_shout", 1, t_str,
                             PINKDB_EXT_TYPE_VARCHAR, ext_shout, NULL) != 0) {
        return 11;
    }
    if (api->register_scalar(host_ctx, "ext_concat2", 2, t_str2,
                             PINKDB_EXT_TYPE_VARCHAR, ext_concat2, NULL) != 0) {
        return 12;
    }
    if (api->register_scalar(host_ctx, "ext_tick", 0, NULL,
                             PINKDB_EXT_TYPE_INTEGER, ext_tick, NULL) != 0) {
        return 13;
    }
    if (api->register_scalar(host_ctx, "ext_probe", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext_probe, NULL) != 0) {
        return 14;
    }
    if (api->register_scalar(host_ctx, "ext_tap", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext_tap, NULL) != 0) {
        return 20;
    }
    if (api->register_scalar(host_ctx, "ext_fail_on_negative", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext_fail_on_negative,
                             NULL) != 0) {
        return 15;
    }
    if (api->register_scalar(host_ctx, "ext_is_even", 1, t_int,
                             PINKDB_EXT_TYPE_BOOLEAN, ext_is_even, NULL) != 0) {
        return 16;
    }
    if (api->register_scalar(host_ctx, "ext_halve", 1, t_double,
                             PINKDB_EXT_TYPE_DOUBLE, ext_halve, NULL) != 0) {
        return 17;
    }
    if (api->register_scalar(host_ctx, "ext_plus_day", 1, t_date,
                             PINKDB_EXT_TYPE_DATE, ext_plus_day, NULL) != 0) {
        return 18;
    }
    if (api->register_scalar(host_ctx, "ext_micros", 1, t_ts,
                             PINKDB_EXT_TYPE_BIGINT, ext_micros, NULL) != 0) {
        return 19;
    }
    if (api->register_scalar(host_ctx, "ext_interval_pack", 3, t_pack,
                             PINKDB_EXT_TYPE_INTERVAL, ext_interval_pack,
                             NULL) != 0) {
        return 21;
    }
    if (api->register_scalar(host_ctx, "ext_interval_fields", 1, t_iv,
                             PINKDB_EXT_TYPE_VARCHAR, ext_interval_fields,
                             NULL) != 0) {
        return 22;
    }
    return 0;
}
