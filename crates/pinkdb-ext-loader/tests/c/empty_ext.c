/*
 * A pinkdb extension that registers no functions at all. The load must still
 * succeed (an empty extension is legal) and the host must keep the library
 * mapped — the zero-function case used to unload the code immediately — and
 * pin the name so a second load is refused.
 */
#include "pinkdb_extension.h"

#include <stdint.h>

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "empty_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    (void)host_ctx;
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    return 0;
}
