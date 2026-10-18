/*
 * Test fixture for the CLI procedure tests
 * (`crates/pinkdb-cli/tests/procedures.rs`): a minimal ABI v1 extension named
 * `proc_ext` registering one BIGINT scalar.
 *
 *     ext_proc_identity(v BIGINT) -> BIGINT   (NULL in -> NULL out)
 *
 * The point of the fixture is that an extension callback is *opaque* (it may
 * be impure, nondeterministic or fail), so a procedure body that calls one is
 * refused: a durable store replays its logged `CALL` by re-executing it, and
 * the callback's result is not reproducible in general.
 */

#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

static int32_t ext_proc_identity(void *user_data, const pinkdb_ext_value *args,
                                 size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = args[0].int64;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "proc_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    static const int32_t identity_arg[1] = {PINKDB_EXT_TYPE_BIGINT};
    if (api->register_scalar(host_ctx, "ext_proc_identity", 1, identity_arg,
                             PINKDB_EXT_TYPE_BIGINT, ext_proc_identity,
                             NULL) != 0) {
        return 10;
    }
    return 0;
}
