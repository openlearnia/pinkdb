/*
 * A pinkdb extension whose callbacks exercise the VARCHAR result-ownership
 * error paths: an error return with an allocated buffer, a NULL result with
 * an allocated buffer, invalid UTF-8, a NULL text pointer with a non-zero
 * length, and a zero-length result. The host must copy/free exactly per the
 * header — every allocated buffer is released, on every path — and report
 * the ABI violations as errors.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

/* Copied out of init: the header documents the struct as init-scoped. */
static PinkdbHostApi g_api;

/* How many result buffers this extension allocated (test introspection). */
static long long g_ownership_allocs;

/* Allocate `len` bytes of result text via the host allocator. */
static char *alloc_text(size_t len) {
    char *buf = (char *)g_api.alloc(len);
    if (buf != NULL) {
        __atomic_add_fetch(&g_ownership_allocs, 1, __ATOMIC_RELAXED);
    }
    return buf;
}

int64_t pinkdb_test_ownership_allocs(void) {
    return __atomic_load_n(&g_ownership_allocs, __ATOMIC_RELAXED);
}

/* own_err_with_buf() -> VARCHAR: allocates a buffer, then returns a non-zero
 * code. The host must still free the buffer (and report the code). */
static int32_t own_err_with_buf(void *user_data, const pinkdb_ext_value *args,
                                size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    char *buf = alloc_text(4);
    if (buf == NULL) {
        return 1;
    }
    buf[0] = 'o';
    buf[1] = 'o';
    buf[2] = 'p';
    buf[3] = 's';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = 4;
    return 9;
}

/* own_null_with_buf() -> VARCHAR: sets is_null and a buffer; the result is
 * NULL, but the buffer was allocated and must be freed by the host. */
static int32_t own_null_with_buf(void *user_data, const pinkdb_ext_value *args,
                                 size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    char *buf = alloc_text(3);
    if (buf == NULL) {
        return 1;
    }
    buf[0] = 'n';
    buf[1] = 'u';
    buf[2] = 'l';
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 1;
    out->text_ptr = buf;
    out->text_len = 3;
    return 0;
}

/* own_bad_utf8() -> VARCHAR: two bytes that are not valid UTF-8. The engine
 * rejects the value; the buffer is freed by the host. */
static int32_t own_bad_utf8(void *user_data, const pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    char *buf = alloc_text(2);
    if (buf == NULL) {
        return 1;
    }
    buf[0] = (char)0xff;
    buf[1] = (char)0xfe;
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = buf;
    out->text_len = 2;
    return 0;
}

/* own_null_ptr_with_len() -> VARCHAR: NULL text_ptr with text_len 5 is an
 * ABI violation the host must report (never read). */
static int32_t own_null_ptr_with_len(void *user_data,
                                     const pinkdb_ext_value *args,
                                     size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = NULL;
    out->text_len = 5;
    return 0;
}

/* own_zero_len() -> VARCHAR: a valid empty string (NULL pointer, length 0). */
static int32_t own_zero_len(void *user_data, const pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_VARCHAR;
    out->is_null = 0;
    out->text_ptr = NULL;
    out->text_len = 0;
    return 0;
}

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "ownership_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    g_api = *api;

    if (api->register_scalar(host_ctx, "own_err_with_buf", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, own_err_with_buf,
                             NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "own_null_with_buf", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, own_null_with_buf,
                             NULL) != 0) {
        return 11;
    }
    if (api->register_scalar(host_ctx, "own_bad_utf8", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, own_bad_utf8, NULL) != 0) {
        return 12;
    }
    if (api->register_scalar(host_ctx, "own_null_ptr_with_len", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, own_null_ptr_with_len,
                             NULL) != 0) {
        return 13;
    }
    if (api->register_scalar(host_ctx, "own_zero_len", 0, NULL,
                             PINKDB_EXT_TYPE_VARCHAR, own_zero_len, NULL) != 0) {
        return 14;
    }
    return 0;
}
