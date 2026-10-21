/*
 * L-8 test extension: keeps the host context after init and tries to register
 * a function later. The host must reject the late registration
 * deterministically (and must not write into a dead stack frame).
 */
#include "pinkdb_extension.h"

#include <stddef.h>

static PinkdbHostApi g_api;
static void *g_ctx;

static int32_t ext_ok(void *user_data, const pinkdb_ext_value *args,
                      size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = 7;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) { return PINKDB_EXT_ABI_VERSION; }

const char *pinkdb_extension_name(void) { return "late-register-test"; }

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *ctx) {
    g_api = *api;
    g_ctx = ctx;
    return api->register_scalar(ctx, "late_base", 0, NULL,
                                PINKDB_EXT_TYPE_INTEGER, ext_ok, NULL);
}

/* Called by the Rust test after init returned. Must return non-zero. */
int32_t pinkdb_test_late_register(void) {
    return g_api.register_scalar(g_ctx, "late_after_init", 0, NULL,
                                 PINKDB_EXT_TYPE_INTEGER, ext_ok, NULL);
}
