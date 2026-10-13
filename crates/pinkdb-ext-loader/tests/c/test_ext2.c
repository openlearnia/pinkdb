/*
 * A second, independent pinkdb test extension: same ABI shape as `test_ext`
 * but a distinct declared name and functions, used by the WAL tests that must
 * distinguish "extension A" from "extension B" (e.g. A loaded before a
 * checkpoint and its file deleted, B loaded after).
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

/* Copied out of init: the header documents the struct as init-scoped. */
static PinkdbHostApi g_api;

/* ext2_triple(a INTEGER) -> INTEGER */
static int32_t ext2_triple(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = args[0].int64 * 3;
    return 0;
}

/* ext2_shout(s VARCHAR) -> VARCHAR: distinct name for WAL prefix tests. */
static int32_t ext2_shout(void *user_data, const pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;
    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }
    size_t len = args[0].text_len;
    char *buf = (char *)g_api.alloc(len + 1);
    if (buf == NULL) {
        return 1;
    }
    for (size_t i = 0; i < len; i++) {
        char c = args[0].text_ptr[i];
        buf[i] = (c >= 'a' && c <= 'z') ? (char)(c - ('a' - 'A')) : c;
    }
    buf[len] = '\0';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = len;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "test_ext2";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    g_api = *api;
    const int32_t t_int[1] = {PINKDB_EXT_TYPE_INTEGER};
    const int32_t t_str[1] = {PINKDB_EXT_TYPE_VARCHAR};
    if (api->register_scalar(host_ctx, "ext2_triple", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext2_triple, NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "ext2_shout", 1, t_str,
                             PINKDB_EXT_TYPE_VARCHAR, ext2_shout, NULL) != 0) {
        return 11;
    }
    return 0;
}
