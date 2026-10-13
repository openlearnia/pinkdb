/*
 * An extension built against a (hypothetical) future ABI version: the loader
 * must refuse it with an error naming both versions, before touching init.
 */
#include <stddef.h>
#include <stdint.h>

uint32_t pinkdb_extension_abi_version(void) {
    return 2;
}

const char *pinkdb_extension_name(void) {
    return "wrong_abi_ext";
}

int32_t pinkdb_extension_init(const void *api, void *host_ctx) {
    (void)api;
    (void)host_ctx;
    return 0;
}
