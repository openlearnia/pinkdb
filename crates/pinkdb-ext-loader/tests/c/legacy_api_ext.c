/*
 * pinkdb extension ABI v1 — pre-flag (legacy) extension, additivity proof.
 *
 * A C extension built against the ABI v1 header *before* the volatile-flag
 * addition sees a `PinkdbHostApi` with five fields; the current host's struct
 * is that struct plus one appended function pointer. This fixture reproduces
 * the pre-flag view exactly (a byte-for-byte prefix of the current struct —
 * pinned by the layout tests in `src/abi.rs`): it declares the legacy struct
 * itself, casts the host pointer to it, and registers through
 * `register_scalar` as an old binary would.
 *
 * Loading it into the current host must succeed and the registered function
 * must be deterministic (the pre-flag default), so an existing extension
 * binary needs no rebuild. `tests/volatile_functions.rs` asserts both.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

/* The pre-wave-51 `PinkdbHostApi`, field for field: version, size,
 * register_scalar, alloc, free. Nothing else existed. */
typedef struct LegacyHostApi {
    uint32_t abi_version;
    uint32_t struct_size;
    int32_t (*register_scalar)(void *ctx,
                               const char *name,
                               uint32_t n_args,
                               const int32_t *arg_types,
                               int32_t ret_type,
                               PinkdbExtScalarFn fn,
                               void *user_data);
    void *(*alloc)(size_t size);
    void (*free)(void *ptr);
} LegacyHostApi;

static int32_t legacy_add_one(void *user_data,
                              const pinkdb_ext_value *args,
                              size_t n_args,
                              pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args == NULL || args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = args[0].int64 + 1;
    return 0;
}

int32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "legacy_api_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    /* What an old binary compiled against the old header does: it reads only
     * the fields it knows (the legacy struct is a prefix of the current one,
     * so the cast is ABI-safe by construction). */
    const LegacyHostApi *legacy = (const LegacyHostApi *)api;
    const int32_t t_int = PINKDB_EXT_TYPE_INTEGER;
    if (legacy->register_scalar(host_ctx, "ext_legacy_add_one", 1, &t_int,
                                PINKDB_EXT_TYPE_INTEGER, legacy_add_one, NULL)
        != 0) {
        return 5;
    }
    return 0;
}
