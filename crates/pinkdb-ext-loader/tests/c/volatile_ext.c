/*
 * pinkdb extension ABI v1 — volatile-flag test extension.
 *
 * Compiled at test time with the system `cc` against the loader's ABI header
 * and loaded by `tests/volatile_functions.rs`. It exercises the wave-51
 * additive registration entry point `api->register_scalar_flags`:
 *
 *   ext_vol_tick()     0 args -> BIGINT  PINKDB_EXT_FLAG_VOLATILE
 *                      A clock-like counter: every call returns a fresh value
 *                      (the extension's analogue of `time()`), so a statement
 *                      that called it could never replay to the same rows.
 *   ext_vol_stable(i)  INTEGER -> INTEGER, registered through the SAME entry
 *                      point with flags == 0 — the documented deterministic
 *                      default, byte-for-byte what `register_scalar` does.
 *
 * Also exports `pinkdb_test_vol_ticks()`, a non-ABI introspection symbol the
 * Rust side reads with dlopen to observe how often the volatile callback ran.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

/* Copied out of init: the header documents the struct as init-scoped. */
static PinkdbHostApi g_api;

/* Monotonic tick source (not part of the ABI): atomics because callbacks run
 * concurrently on the host's rayon workers. */
static long long g_ticks;

int32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "volatile_ext";
}

/* Volatile: each call returns the previous tick count, so consecutive calls
 * (and any replay) observe different values. */
static int32_t ext_vol_tick(void *user_data,
                            const pinkdb_ext_value *args,
                            size_t n_args,
                            pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    long long tick = __atomic_fetch_add(&g_ticks, 1, __ATOMIC_RELAXED);
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = tick;
    return 0;
}

/* Deterministic: a pure function of its argument (registered with flags 0
 * through register_scalar_flags). */
static int32_t ext_vol_stable(void *user_data,
                              const pinkdb_ext_value *args,
                              size_t n_args,
                              pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args == NULL || args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = (int64_t)args[0].int64 * 2;
    return 0;
}

/* ABI-addition introspection (not part of the ABI). */
long long pinkdb_test_vol_ticks(void) {
    return __atomic_load_n(&g_ticks, __ATOMIC_RELAXED);
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    g_api = *api;
    /* The documented check for the additive entry point: an older host has a
     * smaller struct and no `register_scalar_flags` field. This extension
     * needs it (it registers a volatile function), so it must fail the load
     * instead of silently registering as deterministic. */
    if (api->struct_size
        < offsetof(PinkdbHostApi, register_scalar_flags)
              + sizeof(api->register_scalar_flags)) {
        return 7;
    }
    if (api->register_scalar_flags(host_ctx, "ext_vol_tick", 0, NULL,
                                   PINKDB_EXT_TYPE_BIGINT,
                                   PINKDB_EXT_FLAG_VOLATILE, ext_vol_tick,
                                   NULL)
        != 0) {
        return 10;
    }
    /* flags == 0: exactly the deterministic default of register_scalar. */
    const int32_t t_int = PINKDB_EXT_TYPE_INTEGER;
    if (api->register_scalar_flags(host_ctx, "ext_vol_stable", 1, &t_int,
                                   PINKDB_EXT_TYPE_INTEGER, 0, ext_vol_stable,
                                   NULL)
        != 0) {
        return 11;
    }
    /* An extension may mix both entry points: register_scalar is the same
     * call with flags 0. `ext_vol_stable` already covers flags 0 through the
     * new entry point; this third function proves the old entry point still
     * works from the same init. */
    if (api->register_scalar(host_ctx, "ext_vol_stable_legacy", 1, &t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext_vol_stable, NULL)
        != 0) {
        return 12;
    }
    return 0;
}
