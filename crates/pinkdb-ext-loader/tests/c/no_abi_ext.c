/*
 * A library that is not a pinkdb extension: it exports a plausible name
 * symbol but no ABI version, so the loader must report the missing symbol
 * (not crash, not half-load).
 */
#include <stddef.h>
#include <stdint.h>

const char *pinkdb_extension_name(void) {
    return "no_abi_ext";
}

/* Present to prove the loader checks the ABI symbol first: init is never
 * reached because the version symbol is missing. */
int32_t pinkdb_extension_init(const void *api, void *host_ctx) {
    (void)api;
    (void)host_ctx;
    return 0;
}
