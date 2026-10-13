/*
 * An extension whose init attempts invalid registrations. Each must be
 * rejected (a non-zero return from register_scalar); the extension then
 * returns 0 from init anyway, and the load must still fail — a rejected
 * registration may never look successful.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

static int32_t never_called(void *user_data, const pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->is_null = 1;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "bad_register_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    const int32_t t_int[1] = {PINKDB_EXT_TYPE_INTEGER};

    /* Invalid return type tag 99. */
    if (api->register_scalar(host_ctx, "bad_ret", 1, t_int, 99, never_called,
                             NULL) == 0) {
        return 1;
    }
    /* NULL arg_types with one argument. */
    if (api->register_scalar(host_ctx, "bad_arg", 1, NULL,
                             PINKDB_EXT_TYPE_INTEGER, never_called,
                             NULL) == 0) {
        return 2;
    }
    /* More arguments than the ABI allows. */
    if (api->register_scalar(host_ctx, "too_many",
                             (uint32_t)(PINKDB_EXT_MAX_ARGS + 1), t_int,
                             PINKDB_EXT_TYPE_INTEGER, never_called,
                             NULL) == 0) {
        return 3;
    }
    /* A VALID registration after the failed ones — the host must still fail
     * the whole load (nothing from this extension is registered). */
    if (api->register_scalar(host_ctx, "innocent", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, never_called, NULL) != 0) {
        return 4;
    }
    return 0;
}
