/*
 * pinkdb extension ABI v1 — unknown-flag rejection test extension.
 *
 * `register_scalar_flags` must reject a flag bit the host does not know
 * (here `0x8`, beyond `PINKDB_EXT_FLAG_VOLATILE`): silently dropping it would
 * turn a declared-volatile function back into a deterministic one, which is
 * exactly the mistake the flag exists to prevent. The registration is
 * rejected, so the load fails even though init returns 0.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

static int32_t never_called(void *user_data,
                            const pinkdb_ext_value *args,
                            size_t n_args,
                            pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    (void)out;
    return 1;
}

int32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "bad_flags_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    /* 0x8 is not a flag this host defines. */
    int32_t rc = api->register_scalar_flags(host_ctx, "bad_flags", 0, NULL,
                                            PINKDB_EXT_TYPE_BIGINT,
                                            PINKDB_EXT_FLAG_VOLATILE | 0x8u,
                                            never_called, NULL);
    if (rc == 0) {
        return 3; /* the host must have rejected it */
    }
    return 0; /* the rejection already failed the load; report it cleanly */
}
