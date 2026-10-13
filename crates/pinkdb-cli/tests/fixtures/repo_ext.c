/*
 * Test fixture for the CLI repository tests (`crates/pinkdb-cli/tests/repo.rs`):
 * a minimal ABI v1 extension named `repo_ext` registering one BIGINT scalar.
 *
 *     ext_repo_triple(v BIGINT) -> BIGINT   (NULL in -> NULL out)
 *
 * Compiled with the system `cc` at test time and served by the in-test
 * fixture HTTP server, then installed and loaded through the real CLI.
 */

#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

static int32_t ext_repo_triple(void *user_data, const pinkdb_ext_value *args,
                               size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = args[0].int64 * 3;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "repo_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    static const int32_t t_bigint[1] = {PINKDB_EXT_TYPE_BIGINT};
    if (api->register_scalar(host_ctx, "ext_repo_triple", 1, t_bigint,
                             PINKDB_EXT_TYPE_BIGINT, ext_repo_triple, NULL) != 0) {
        return 10;
    }
    return 0;
}
