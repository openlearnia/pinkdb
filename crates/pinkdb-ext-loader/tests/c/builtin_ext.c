/*
 * An extension that tries to shadow a built-in function name ("upper"). The
 * host must reject the registration (a built-in always wins at bind time, so
 * a shadowing extension function would be unreachable — failing loudly beats
 * a silently dead function).
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

static int32_t fake_upper(void *user_data, const pinkdb_ext_value *args,
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
    return "builtin_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    const int32_t t_str[1] = {PINKDB_EXT_TYPE_VARCHAR};
    /* The loader accepts this (it does not know the engine's built-ins); the
     * engine refuses it when the extension is registered, so the load must
     * fail and leave the registry untouched. */
    if (api->register_scalar(host_ctx, "upper", 1, t_str,
                             PINKDB_EXT_TYPE_VARCHAR, fake_upper, NULL) != 0) {
        return 1;
    }
    return 0;
}
