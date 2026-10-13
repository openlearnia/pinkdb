/*
 * A pinkdb extension that registers a function with a NULL callback. The C
 * signature takes a function pointer, which the host must reject before
 * storing it — a stored NULL would be jumped to on the first call. The
 * extension returns 0 from init either way: the *host* is what must fail the
 * load.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "null_callback_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    const int32_t t_int[1] = {PINKDB_EXT_TYPE_INTEGER};
    /* Must be rejected; a 0 return here would mean the host stored it. */
    if (api->register_scalar(host_ctx, "null_cb", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, NULL, NULL) == 0) {
        return 2;
    }
    return 0;
}
