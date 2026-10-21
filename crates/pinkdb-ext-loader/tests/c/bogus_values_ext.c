/*
 * L-9 test extension: returns deliberately malformed VARCHAR results.
 *
 * - `bogus_len` returns a VARCHAR whose `text_len` is SIZE_MAX while the
 *   pointer references a one-byte static buffer. The host must reject it
 *   before reading (or freeing) anything.
 * - `bogus_tag` returns INTEGER with a non-NULL `text_ptr`/`text_len` that
 *   aliases a static buffer. The host must not free a pointer that is not a
 *   VARCHAR result.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

static const char g_tiny[] = "x";

static int32_t ext_bogus_len(void *user_data, const pinkdb_ext_value *args,
                             size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = g_tiny;
    out->text_len = (size_t)-1;
    return 0;
}

static int32_t ext_bogus_tag(void *user_data, const pinkdb_ext_value *args,
                             size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = 42;
    /* A non-VARCHAR tag with text fields set: must never be freed. */
    out->text_ptr = g_tiny;
    out->text_len = 1;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) { return PINKDB_EXT_ABI_VERSION; }

const char *pinkdb_extension_name(void) { return "bogus-values-test"; }

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *ctx) {
    if (api->register_scalar(ctx, "bogus_len", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, ext_bogus_len, NULL) != 0) {
        return 1;
    }
    return api->register_scalar(ctx, "bogus_tag", 0, NULL,
                                PINKDB_EXT_TYPE_INTEGER, ext_bogus_tag, NULL);
}
